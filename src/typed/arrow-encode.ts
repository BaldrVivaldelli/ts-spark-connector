/**
 * Codificación de filas locales — convierte objetos fila de JavaScript en un
 * stream Arrow IPC para `LocalRelation` (el camino inverso de `arrow-rows`).
 *
 * Las representaciones de valor son exactamente las que `collect()` devuelve,
 * de modo que `createDataFrame(await df.collect(), schema)` es un roundtrip
 * sin pérdida: `long` es `bigint`, `date` es `"YYYY-MM-DD"`, `timestamp` es un
 * string ISO-8601 y los números son `number`.
 */

import type { Data, DataType } from "apache-arrow";
import type { SchemaDef, TypeToken } from "../schema/schema";

let arrowModule: typeof import("apache-arrow") | undefined;

function arrow(): typeof import("apache-arrow") {
    // Arrow is comparatively large; load it only when local data is encoded.
    return arrowModule ??= require("apache-arrow") as typeof import("apache-arrow");
}

const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

interface ColumnPlan {
    readonly name: string;
    readonly token: TypeToken;
    readonly nullable: boolean;
}

function columnPlan(def: SchemaDef): ColumnPlan[] {
    const entries = Object.entries(def);
    if (entries.length === 0) {
        throw new TypeError("createDataFrame() requires a schema with at least one column.");
    }
    return entries.map(([name, spec]) => {
        if (typeof spec !== "string") {
            throw new TypeError(
                `createDataFrame() does not support ${spec.kind} columns yet; ` +
                `column ${JSON.stringify(name)} declares one. Build the column with sql() instead.`,
            );
        }
        const nullable = spec.endsWith("?");
        const token = (nullable ? spec.slice(0, -1) : spec) as TypeToken;
        return { name, token, nullable };
    });
}

function fail(column: string, row: number, expected: string, value: unknown): never {
    throw new TypeError(
        `createDataFrame() column ${JSON.stringify(column)} row ${row}: expected ${expected}, ` +
        `received ${typeof value === "bigint" ? `${value}n` : JSON.stringify(value)}.`,
    );
}

/** Converts one validated JS value into what the Arrow builder for the token expects. */
function builderValue(plan: ColumnPlan, value: unknown, row: number): unknown {
    const { name, token } = plan;
    switch (token) {
        case "int": {
            if (typeof value !== "number" || !Number.isInteger(value) || value < INT32_MIN || value > INT32_MAX) {
                fail(name, row, "a 32-bit integer number", value);
            }
            return value;
        }
        case "long": {
            if (typeof value === "bigint") return value;
            if (typeof value === "number" && Number.isSafeInteger(value)) return BigInt(value);
            fail(name, row, "a bigint (or safe-integer number)", value);
            break;
        }
        case "double":
        case "float": {
            if (typeof value !== "number") fail(name, row, "a number", value);
            return value;
        }
        case "string": {
            if (typeof value !== "string") fail(name, row, "a string", value);
            return value;
        }
        case "boolean": {
            if (typeof value !== "boolean") fail(name, row, "a boolean", value);
            return value;
        }
        case "date": {
            if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
                fail(name, row, 'a "YYYY-MM-DD" date string', value);
            }
            const ms = Date.parse(`${value}T00:00:00Z`);
            // V8 rolls impossible days over ("2026-02-30" becomes March 2nd);
            // round-tripping the parse rejects both NaN and silent rollovers.
            if (Number.isNaN(ms) || new Date(ms).toISOString().slice(0, 10) !== value) {
                fail(name, row, "a valid calendar date", value);
            }
            return ms;
        }
        case "timestamp": {
            if (typeof value !== "string") fail(name, row, "an ISO-8601 timestamp string", value);
            const ms = Date.parse(value);
            if (Number.isNaN(ms)) fail(name, row, "a parseable ISO-8601 timestamp", value);
            return ms;
        }
    }
}

function arrowType(token: TypeToken): DataType {
    const A = arrow();
    switch (token) {
        case "int": return new A.Int32();
        case "long": return new A.Int64();
        case "double": return new A.Float64();
        case "float": return new A.Float32();
        case "string": return new A.Utf8();
        case "boolean": return new A.Bool();
        case "date": return new A.DateDay();
        case "timestamp": return new A.TimestampMicrosecond("UTC");
    }
}

/**
 * Serializa filas locales como un stream Arrow IPC cuyo schema (tipos y
 * nullabilidad por campo) proviene del schema declarado. Los valores se
 * validan fila por fila: un null en una columna no declarada nullable, o un
 * valor cuyo tipo JS no corresponde al token, fallan con la columna y la fila
 * en el mensaje.
 */
export function encodeLocalRelation(
    rows: ReadonlyArray<Record<string, unknown>>,
    def: SchemaDef,
): Uint8Array {
    const A = arrow();
    const plans = columnPlan(def);

    rows.forEach((rawRow, index) => {
        if (rawRow === null || typeof rawRow !== "object") {
            throw new TypeError(`createDataFrame() row ${index} is not an object.`);
        }
    });

    // A builder flushed once yields exactly one Data chunk per column, whatever
    // the row count, so every column lands in a single aligned RecordBatch.
    const children: Data[] = plans.map(plan => {
        const builder = A.makeBuilder({ type: arrowType(plan.token), nullValues: [null] });
        rows.forEach((rawRow, index) => {
            const value = rawRow[plan.name];
            if (value === null || value === undefined) {
                if (!plan.nullable) fail(plan.name, index, "a non-null value (column is not nullable)", value);
                builder.append(null);
                return;
            }
            builder.append(builderValue(plan, value, index));
        });
        return builder.finish().flush();
    });

    const fields = plans.map((plan, index) => new A.Field(plan.name, children[index]!.type, plan.nullable));
    const schema = new A.Schema(fields);
    const structData = A.makeData({
        type: new A.Struct(fields),
        length: rows.length,
        nullCount: 0,
        children,
    });
    const table = new A.Table(schema, new A.RecordBatch(schema, structData));
    return A.tableToIPC(table, "stream");
}
