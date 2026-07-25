/**
 * Decodificación de filas tipadas — convierte los batches Arrow de Spark
 * Connect en objetos fila planos.
 *
 * Forma parte de la API pública unificada del DataFrame con schema tipado.
 *
 * La API no tipada solo dispone de `printArrowResults` (display) y `collectRaw`
 * (respuestas crudas). Para que `collect()` en el camino tipado devuelva filas
 * conformes al schema actual (`Row<S>[]`) necesitamos materializar los buffers
 * Arrow IPC en objetos `{ columna: valor }`.
 *
 * Los valores `BigInt` (longs de Spark) se conservan siempre como `bigint`, de
 * modo que el contrato no cambie según el valor ni pierda precisión.
 */

import type { DataType, Field } from "apache-arrow";
import type { FieldSpec, SchemaDef, TypeToken } from "../schema/schema";

let arrowModule: typeof import("apache-arrow") | undefined;

function arrow(): typeof import("apache-arrow") {
    // Arrow is comparatively large; keep the normal plan-building import path
    // light and load it only when rows are actually decoded.
    return arrowModule ??= require("apache-arrow") as typeof import("apache-arrow");
}

function requiredAt<T>(values: readonly T[], index: number, context: string): T {
    const value = values[index];
    if (value === undefined) {
        throw new TypeError(`Malformed Arrow schema: missing ${context} at index ${index}.`);
    }
    return value;
}

/** Forma de una respuesta de Spark Connect que transporta un batch Arrow. */
type ArrowBatchResponse = { arrow_batch?: { data?: Buffer } };

/**
 * Extrae los buffers Arrow IPC de las respuestas crudas de `ExecutePlan` de
 * Spark Connect, descartando las respuestas que no contienen un batch Arrow.
 */
export function arrowBuffersFromResponses(responses: unknown[]): Buffer[] {
    return responses
        .filter((r): r is ArrowBatchResponse => !!(r as ArrowBatchResponse)?.arrow_batch?.data)
        .map(r => (r as ArrowBatchResponse).arrow_batch!.data as Buffer);
}

/**
 * Decodifica buffers Arrow IPC en un arreglo de objetos fila. Cada fila es un
 * objeto plano indexado por nombre de columna. Los valores `BigInt` (longs de
 * Spark) se conservan siempre como `bigint`.
 */
export function rowsFromArrowBuffers<Row>(buffers: Buffer[], expectedSchema?: SchemaDef): Row[] {
    const rows: Row[] = [];

    for (const buf of buffers) {
        const table = arrow().tableFromIPC(buf);
        const fields = table.schema.fields as Field<DataType>[];
        assertUniqueFieldNames(fields);
        if (expectedSchema) assertExpectedSchema(fields, expectedSchema);
        if (table.numRows === 0) continue;

        const names = fields.map(f => f.name);
        const vectors = fields.map((_field, index) => table.getChildAt(index)!);

        for (let r = 0; r < table.numRows; r++) {
            const row: Record<string, unknown> = {};
            for (let c = 0; c < names.length; c++) {
                const name = requiredAt(names, c, "field name");
                const vector = requiredAt(vectors, c, "column vector");
                const field = requiredAt(fields, c, "field");
                row[name] = normalizeValue(vector.get(r), field.type);
            }
            rows.push(row as Row);
        }
    }

    return rows;
}

/**
 * Normaliza un valor decodificado de Arrow. Los `BigInt` se conservan siempre;
 * el resto de los valores se convierte según el tipo Arrow declarado.
 */
function normalizeValue(value: unknown, type: DataType): unknown {
    if (value == null) return value;
    const { DataType } = arrow();

    // Int64 remains bigint at every magnitude: the public `long` contract is
    // precision-safe and therefore never changes type based on the value.
    if (typeof value === "bigint") return value;

    if (DataType.isDate(type)) {
        return new Date(Number(value)).toISOString().slice(0, 10);
    }
    if (DataType.isTimestamp(type)) {
        return new Date(Number(value)).toISOString();
    }
    if (DataType.isDecimal(type)) {
        return decimalString(String(value), type.scale);
    }
    if (DataType.isList(type) || DataType.isFixedSizeList(type)) {
        const childType = requiredAt(type.children, 0, "list child").type;
        return Array.from(value as Iterable<unknown>, item => normalizeValue(item, childType));
    }
    if (DataType.isStruct(type)) {
        const source = value as Record<string, unknown>;
        return Object.fromEntries(type.children.map(field => [
            field.name,
            normalizeValue(source[field.name], field.type),
        ]));
    }
    if (DataType.isMap(type)) {
        const entry = requiredAt(type.children, 0, "map entry").type;
        const keyType = requiredAt(entry.children, 0, "map key").type;
        const valueType = requiredAt(entry.children, 1, "map value").type;
        return new Map(Array.from(
            value as Iterable<[unknown, unknown]>,
            ([key, item]) => [normalizeValue(key, keyType), normalizeValue(item, valueType)],
        ));
    }
    return value;
}

function assertUniqueFieldNames(fields: readonly Field<DataType>[]): void {
    const seen = new Set<string>();
    for (const field of fields) {
        if (seen.has(field.name)) {
            throw new TypeError(
                `Spark returned duplicate column name ${JSON.stringify(field.name)}; ` +
                "row objects cannot represent duplicate columns. Rename them before collect()/toRows()."
            );
        }
        seen.add(field.name);
    }
}

function assertExpectedSchema(fields: readonly Field<DataType>[], expected: SchemaDef): void {
    const entries = Object.entries(expected);
    const actualNames = fields.map(field => field.name);
    const expectedNames = entries.map(([name]) => name);
    if (actualNames.length !== expectedNames.length ||
        actualNames.some((name, index) => name !== expectedNames[index])) {
        throw new TypeError(
            `Spark schema mismatch: expected columns [${expectedNames.join(", ")}], ` +
            `received [${actualNames.join(", ")}].`
        );
    }

    entries.forEach(([name, spec], index) => {
        const field = requiredAt(fields, index, "expected field");
        if (!isNullableSpec(spec) && field.nullable) {
            throw new TypeError(
                `Spark schema mismatch at ${name}: TypeScript expects a non-null column, ` +
                "but Spark reports it as nullable."
            );
        }
        assertFieldType(field.type, spec, name);
    });
}

function assertFieldType(actualInput: DataType, spec: FieldSpec, path: string): void {
    const DataTypeRuntime = arrow().DataType;
    const actual = DataTypeRuntime.isDictionary(actualInput)
        ? actualInput.dictionary
        : actualInput;

    if (typeof spec === "string") {
        const token = (spec.endsWith("?") ? spec.slice(0, -1) : spec) as TypeToken;
        const matches = token === "int"
            ? DataTypeRuntime.isInt(actual) && actual.isSigned && actual.bitWidth === 32
            : token === "long"
              ? DataTypeRuntime.isInt(actual) && actual.isSigned && actual.bitWidth === 64
              : token === "float"
                ? DataTypeRuntime.isFloat(actual) && actual.precision === 1
                : token === "double"
                  ? DataTypeRuntime.isFloat(actual) && actual.precision === 2
                  : token === "string"
                    ? DataTypeRuntime.isUtf8(actual) || DataTypeRuntime.isLargeUtf8(actual)
                    : token === "boolean"
                      ? DataTypeRuntime.isBool(actual)
                      : token === "date"
                        ? DataTypeRuntime.isDate(actual)
                        : token === "timestamp"
                          ? DataTypeRuntime.isTimestamp(actual)
                          : false;
        if (!matches) schemaTypeMismatch(path, token, actual);
        return;
    }

    switch (spec.kind) {
        case "decimal":
            if (!DataTypeRuntime.isDecimal(actual) ||
                actual.precision !== spec.precision || actual.scale !== spec.scale) {
                schemaTypeMismatch(path, `DECIMAL(${spec.precision},${spec.scale})`, actual);
            }
            return;
        case "array": {
            if (!(DataTypeRuntime.isList(actual) || DataTypeRuntime.isFixedSizeList(actual))) {
                schemaTypeMismatch(path, "ARRAY", actual);
            }
            assertFieldType(requiredAt(actual.children, 0, `${path} array child`).type, spec.element, `${path}[]`);
            return;
        }
        case "map": {
            if (!DataTypeRuntime.isMap(actual)) schemaTypeMismatch(path, "MAP", actual);
            assertFieldType(actual.keyType, spec.key, `${path}.key`);
            assertFieldType(actual.valueType, spec.value, `${path}.value`);
            return;
        }
        case "struct": {
            if (!DataTypeRuntime.isStruct(actual)) schemaTypeMismatch(path, "STRUCT", actual);
            const expectedFields = Object.entries(spec.fields);
            if (actual.children.length !== expectedFields.length ||
                actual.children.some((field, index) => field.name !== expectedFields[index]?.[0])) {
                throw new TypeError(`Spark schema mismatch at ${path}: nested struct fields differ.`);
            }
            expectedFields.forEach(([name, child], index) => {
                const actualChild = requiredAt(actual.children, index, `${path} struct child`);
                assertFieldType(actualChild.type, child, `${path}.${name}`);
            });
            return;
        }
    }
}

function schemaTypeMismatch(path: string, expected: string, actual: DataType): never {
    throw new TypeError(
        `Spark schema mismatch at ${path}: expected ${expected}, received ${actual.toString()}.`
    );
}

function isNullableSpec(spec: FieldSpec): boolean {
    return typeof spec === "string" ? spec.endsWith("?") : spec.nullable === true;
}

function decimalString(unscaledInput: string, scale: number): string {
    if (scale <= 0) return unscaledInput;
    const negative = unscaledInput.startsWith("-");
    const digits = negative ? unscaledInput.slice(1) : unscaledInput;
    const padded = digits.padStart(scale + 1, "0");
    const split = padded.length - scale;
    return `${negative ? "-" : ""}${padded.slice(0, split)}.${padded.slice(split)}`;
}
