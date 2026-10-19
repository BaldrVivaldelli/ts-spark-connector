/**
 * Codificación de filas locales — convierte objetos fila de JavaScript en un
 * stream Arrow IPC para `LocalRelation` (el camino inverso de `arrow-rows`).
 *
 * Las representaciones de valor son exactamente las que `collect()` devuelve,
 * de modo que `createDataFrame(await df.collect(), schema)` es un roundtrip
 * sin pérdida: `long` es `bigint`, `date` es `"YYYY-MM-DD"`, `timestamp` es un
 * string ISO-8601, `decimal` es un string, `array` es un array, `map` es un
 * `Map` y `struct` es un objeto. Los valores anidados admiten `null` (el DDL
 * de Spark no distingue nulabilidad anidada); solo las columnas de primer
 * nivel declaradas sin `?` la rechazan.
 */

import type { Data, DataType } from "apache-arrow";
import type { DecimalSpec, FieldSpec, SchemaDef, TypeToken } from "../schema/schema";

let arrowModule: typeof import("apache-arrow") | undefined;

function arrow(): typeof import("apache-arrow") {
    // Arrow is comparatively large; load it only when local data is encoded.
    return arrowModule ??= require("apache-arrow") as typeof import("apache-arrow");
}

const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

/** How one declared field is encoded: its Arrow type and the JS → builder value conversion. */
interface FieldPlan {
    readonly type: DataType;
    /** Validated JS value (never null) → what the Arrow builder for `type` accepts. */
    readonly convert: (value: unknown, at: string) => unknown;
}

interface ColumnPlan extends FieldPlan {
    readonly name: string;
    readonly nullable: boolean;
}

function render(value: unknown): string {
    if (typeof value === "bigint") return `${value}n`;
    if (value instanceof Map) return `Map(${value.size})`;
    return JSON.stringify(value) ?? String(value);
}

function fail(at: string, expected: string, value: unknown): never {
    throw new TypeError(`createDataFrame() ${at}: expected ${expected}, received ${render(value)}.`);
}

function isNullableSpec(spec: FieldSpec): boolean {
    return typeof spec === "string" ? spec.endsWith("?") : spec.nullable === true;
}

/** Converts one validated JS value into what the Arrow builder for the token expects. */
function primitiveValue(token: TypeToken, value: unknown, at: string): unknown {
    switch (token) {
        case "int": {
            if (typeof value !== "number" || !Number.isInteger(value) || value < INT32_MIN || value > INT32_MAX) {
                fail(at, "a 32-bit integer number", value);
            }
            return value;
        }
        case "long": {
            if (typeof value === "bigint") return value;
            if (typeof value === "number" && Number.isSafeInteger(value)) return BigInt(value);
            fail(at, "a bigint (or safe-integer number)", value);
            break;
        }
        case "double":
        case "float": {
            if (typeof value !== "number") fail(at, "a number", value);
            return value;
        }
        case "string": {
            if (typeof value !== "string") fail(at, "a string", value);
            return value;
        }
        case "boolean": {
            if (typeof value !== "boolean") fail(at, "a boolean", value);
            return value;
        }
        case "date": {
            if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
                fail(at, 'a "YYYY-MM-DD" date string', value);
            }
            const ms = Date.parse(`${value}T00:00:00Z`);
            // V8 rolls impossible days over ("2026-02-30" becomes March 2nd);
            // round-tripping the parse rejects both NaN and silent rollovers.
            if (Number.isNaN(ms) || new Date(ms).toISOString().slice(0, 10) !== value) {
                fail(at, "a valid calendar date", value);
            }
            return ms;
        }
        case "timestamp": {
            if (typeof value !== "string") fail(at, "an ISO-8601 timestamp string", value);
            const ms = Date.parse(value);
            if (Number.isNaN(ms)) fail(at, "a parseable ISO-8601 timestamp", value);
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

/** Two's-complement little-endian words of a 128-bit decimal, as Arrow's builder stores them. */
function decimalWords(unscaled: bigint): Uint32Array {
    const twos = unscaled < 0n ? (1n << 128n) + unscaled : unscaled;
    const words = new Uint32Array(4);
    for (let i = 0; i < 4; i += 1) {
        words[i] = Number((twos >> BigInt(32 * i)) & 0xffffffffn);
    }
    return words;
}

function decimalValue(spec: DecimalSpec, value: unknown, at: string): Uint32Array {
    const expected = `a DECIMAL(${spec.precision},${spec.scale}) string (or bigint)`;
    const text = typeof value === "bigint" ? value.toString() : typeof value === "string" ? value.trim() : undefined;
    const match = text === undefined ? null : /^([+-])?(\d*)(?:\.(\d*))?$/.exec(text);
    if (!match || (match[2] === "" && (match[3] ?? "") === "")) fail(at, expected, value);
    const [, sign, whole, fraction = ""] = match;
    if (fraction.length > spec.scale) {
        fail(at, `${expected} with at most ${spec.scale} decimals`, value);
    }
    const digits = `${whole}${fraction.padEnd(spec.scale, "0")}`.replace(/^0+(?=\d)/, "");
    if (digits.length > spec.precision) {
        fail(at, `${expected} within ${spec.precision} digits`, value);
    }
    const unscaled = BigInt(digits);
    return decimalWords(sign === "-" ? -unscaled : unscaled);
}

function fieldPlan(spec: FieldSpec): FieldPlan {
    const A = arrow();
    if (typeof spec === "string") {
        const token = (spec.endsWith("?") ? spec.slice(0, -1) : spec) as TypeToken;
        return { type: arrowType(token), convert: (value, at) => primitiveValue(token, value, at) };
    }
    switch (spec.kind) {
        case "decimal":
            return {
                type: new A.Decimal(spec.scale, spec.precision, 128),
                convert: (value, at) => decimalValue(spec, value, at),
            };
        case "array": {
            const element = fieldPlan(spec.element);
            return {
                type: new A.List(new A.Field("element", element.type, true)),
                convert: (value, at) => {
                    if (!Array.isArray(value)) fail(at, "an array", value);
                    return value.map((item, index) =>
                        item === null || item === undefined ? null : element.convert(item, `${at}[${index}]`));
                },
            };
        }
        case "map": {
            const key = fieldPlan(spec.key);
            const item = fieldPlan(spec.value);
            const stringKeys = spec.key === "string";
            const entries = new A.Field(
                "entries",
                new A.Struct([new A.Field("key", key.type, false), new A.Field("value", item.type, true)]),
                false,
            );
            return {
                type: new A.Map_(entries, false),
                convert: (value, at) => {
                    const pairs = value instanceof Map
                        ? [...value.entries()]
                        : stringKeys && value !== null && typeof value === "object" && !Array.isArray(value)
                            ? Object.entries(value)
                            : fail(at, stringKeys ? "a Map or a plain object" : "a Map", value);
                    return new Map(pairs.map(([k, v]) => {
                        if (k === null || k === undefined) fail(`${at} key`, "a non-null map key", k);
                        const encodedKey = key.convert(k, `${at} key ${render(k)}`);
                        const encodedValue = v === null || v === undefined
                            ? null
                            : item.convert(v, `${at}[${render(k)}]`);
                        return [encodedKey, encodedValue] as const;
                    }));
                },
            };
        }
        case "struct": {
            const fields = Object.entries(spec.fields).map(([name, child]) => [name, fieldPlan(child)] as const);
            return {
                type: new A.Struct(fields.map(([name, plan]) => new A.Field(name, plan.type, true))),
                convert: (value, at) => {
                    if (value === null || typeof value !== "object" || Array.isArray(value) || value instanceof Map) {
                        fail(at, "an object with the struct's fields", value);
                    }
                    const source = value as Record<string, unknown>;
                    return Object.fromEntries(fields.map(([name, plan]) => {
                        const item = source[name];
                        return [name, item === null || item === undefined ? null : plan.convert(item, `${at}.${name}`)];
                    }));
                },
            };
        }
    }
    throw new TypeError(`createDataFrame() does not support ${(spec as { kind: string }).kind} columns.`);
}

function columnPlans(def: SchemaDef): ColumnPlan[] {
    const entries = Object.entries(def);
    if (entries.length === 0) {
        throw new TypeError("createDataFrame() requires a schema with at least one column.");
    }
    return entries.map(([name, spec]) => ({ name, nullable: isNullableSpec(spec), ...fieldPlan(spec) }));
}

/**
 * Serializa filas locales como un stream Arrow IPC cuyo schema (tipos y
 * nullabilidad por campo) proviene del schema declarado. Los valores se
 * validan fila por fila: un null en una columna no declarada nullable, o un
 * valor cuyo tipo JS no corresponde al token, fallan con la columna y la fila
 * en el mensaje (y la ruta anidada para arrays, maps y structs).
 */
export function encodeLocalRelation(
    rows: ReadonlyArray<Record<string, unknown>>,
    def: SchemaDef,
): Uint8Array {
    const A = arrow();
    const plans = columnPlans(def);

    rows.forEach((rawRow, index) => {
        if (rawRow === null || typeof rawRow !== "object") {
            throw new TypeError(`createDataFrame() row ${index} is not an object.`);
        }
    });

    // A builder flushed once yields exactly one Data chunk per column, whatever
    // the row count, so every column lands in a single aligned RecordBatch.
    const children: Data[] = plans.map(plan => {
        const builder = A.makeBuilder({ type: plan.type, nullValues: [null] });
        rows.forEach((rawRow, index) => {
            const value = rawRow[plan.name];
            const at = `column ${JSON.stringify(plan.name)} row ${index}`;
            if (value === null || value === undefined) {
                if (!plan.nullable) fail(at, "a non-null value (column is not nullable)", value);
                builder.append(null);
                return;
            }
            builder.append(plan.convert(value, at));
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
