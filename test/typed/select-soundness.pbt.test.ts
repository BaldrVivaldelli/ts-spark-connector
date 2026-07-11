import { describe, it, expect } from "vitest";
import fc from "fast-check";

/**
 * Property 3: Soundness de `select`.
 *
 * **Validates: Requirements 5.1, 5.2**
 *
 * El tipo de nivel de tipos `Selected<S, K> = { [P in K]: S[P] }`
 * (`src/schema/schema-transforms.ts`) no puede ejecutarse directamente: vive
 * solo en el sistema de tipos. Para validar su *soundness* con `fast-check`
 * modelamos la proyección como una función de runtime sobre un registro
 * (name → type-tag) que refleja exactamente la semántica de `Selected`, y
 * comprobamos las postcondiciones a nivel de conjuntos.
 *
 * Postcondiciones validadas (sobre cualquier schema S y cualquier K ⊆ keys(S)):
 *  - (5.1) keys(select(S, K)) === set(K): el resultado contiene *exactamente*
 *    las claves seleccionadas, ni más ni menos.
 *  - (5.1) cada clave seleccionada conserva su type-tag original: select(S,K)[k]
 *    === S[k].
 *  - (5.1/5.2) el resultado nunca contiene una clave fuera de keys(S).
 *  - (5.2) seleccionar una clave que no pertenece a keys(S) está prohibido por
 *    el modelo (espejo del error de compilación que produce `K extends keyof S`).
 */

// ---------------------------------------------------------------------------
// Runtime model
// ---------------------------------------------------------------------------

/**
 * Un "type-tag" representa el tipo de valor original de una columna (lo que en
 * el sistema de tipos sería `S[P]`), incluyendo nullabilidad. Conservar el tag
 * intacto en la proyección equivale a conservar el tipo original.
 */
type TypeTag =
    | "string"
    | "number"
    | "boolean"
    | "string|null"
    | "number|null"
    | "boolean|null";

const TYPE_TAGS: readonly TypeTag[] = [
    "string",
    "number",
    "boolean",
    "string|null",
    "number|null",
    "boolean|null",
];

/** Un schema modelado en runtime: nombre de columna → type-tag. */
type SchemaModel = Record<string, TypeTag>;

/**
 * Modelo de runtime de `Selected<S, K>`: proyecta `schema` sobre el subconjunto
 * de claves `keys`. Refleja `{ [P in K]: S[P] }`.
 *
 * Espejo de la precondición de tipos `K extends keyof S` (Requirement 5.2):
 * intentar seleccionar una clave que no está en el schema es un error, igual
 * que el error de compilación que produciría el camino tipado.
 */
function select(schema: SchemaModel, keys: readonly string[]): SchemaModel {
    const out: SchemaModel = {};
    for (const k of keys) {
        if (!Object.prototype.hasOwnProperty.call(schema, k)) {
            throw new Error(`select: key "${k}" is not in keys(S)`);
        }
        out[k] = schema[k];
    }
    return out;
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

/** Genera un schema (registro nombre → type-tag) con claves únicas. */
const schemaArb: fc.Arbitrary<SchemaModel> = fc.dictionary(
    fc.string({ minLength: 1, maxLength: 6 }),
    fc.constantFrom(...TYPE_TAGS),
    { maxKeys: 8 },
);

/**
 * Genera un par (schema, K) donde K ⊆ keys(S). `fc.subarray` elige un
 * subconjunto (sin duplicados, preservando el orden) de las claves del schema.
 */
const schemaAndSubsetArb: fc.Arbitrary<[SchemaModel, string[]]> = schemaArb.chain(
    (schema) =>
        fc.tuple(
            fc.constant(schema),
            fc.subarray(Object.keys(schema)),
        ),
);

/**
 * Genera un par (schema, badKey) donde `badKey` NO pertenece a keys(S).
 * Sirve para validar el rechazo de claves fuera del schema (Requirement 5.2).
 */
const schemaAndOutsideKeyArb: fc.Arbitrary<[SchemaModel, string]> = schemaArb.chain(
    (schema) =>
        fc.tuple(
            fc.constant(schema),
            fc
                .string({ minLength: 1, maxLength: 8 })
                .filter((k) => !Object.prototype.hasOwnProperty.call(schema, k)),
        ),
);

const setOf = (keys: readonly string[]): Set<string> => new Set(keys);

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

describe("Property 3: soundness de select (runtime model of Selected<S, K>)", () => {
    it("produce exactamente las claves seleccionadas (keys(select(S,K)) === set(K))", () => {
        fc.assert(
            fc.property(schemaAndSubsetArb, ([schema, keys]) => {
                const result = select(schema, keys);
                const resultKeys = setOf(Object.keys(result));
                const selectedKeys = setOf(keys);

                // mismo tamaño y misma membresía => conjuntos iguales
                expect(resultKeys.size).toBe(selectedKeys.size);
                for (const k of selectedKeys) {
                    expect(resultKeys.has(k)).toBe(true);
                }
                for (const k of resultKeys) {
                    expect(selectedKeys.has(k)).toBe(true);
                }
            }),
        );
    });

    it("conserva el type-tag original de cada clave seleccionada", () => {
        fc.assert(
            fc.property(schemaAndSubsetArb, ([schema, keys]) => {
                const result = select(schema, keys);
                for (const k of keys) {
                    expect(result[k]).toBe(schema[k]);
                }
            }),
        );
    });

    it("nunca incluye una clave fuera de keys(S)", () => {
        fc.assert(
            fc.property(schemaAndSubsetArb, ([schema, keys]) => {
                const result = select(schema, keys);
                for (const k of Object.keys(result)) {
                    expect(Object.prototype.hasOwnProperty.call(schema, k)).toBe(true);
                }
            }),
        );
    });

    it("rechaza seleccionar una clave que no pertenece a keys(S) (espejo del error de compilación)", () => {
        fc.assert(
            fc.property(schemaAndOutsideKeyArb, ([schema, badKey]) => {
                expect(() => select(schema, [badKey])).toThrow();
                // y combinada con claves válidas, también falla
                const validKeys = Object.keys(schema);
                expect(() => select(schema, [...validKeys, badKey])).toThrow();
            }),
        );
    });

    it("seleccionar todas las claves es la identidad sobre el schema", () => {
        fc.assert(
            fc.property(schemaArb, (schema) => {
                const result = select(schema, Object.keys(schema));
                expect(result).toEqual(schema);
            }),
        );
    });
});
