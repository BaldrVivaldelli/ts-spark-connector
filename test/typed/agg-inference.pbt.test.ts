import { describe, it, expect } from "vitest";
import fc from "fast-check";

/**
 * Property 6: Inferencia de agregación.
 *
 * **Validates: Requirements 5.6**
 *
 * El tipo de nivel de tipos
 * `Aggregated<S, K, A> = Prettify<Pick<S, K> & UnionToIntersection<A[number]["__out"]>>`
 * (`src/schema/schema-transforms.ts`) vive solo en el sistema de tipos y no
 * puede ejecutarse directamente. Para validar su corrección con `fast-check`
 * modelamos en runtime el algoritmo `inferAggSchema(S, keys, aggs)` del diseño
 * (sección "Especificación formal de la inferencia de agregación"), que refleja
 * exactamente la semántica de `Aggregated`, y comprobamos sus postcondiciones a
 * nivel de conjuntos.
 *
 * Postcondiciones del diseño (para todo schema S, keys ⊆ keys(S) y aggs con
 * alias únicos disjuntos de keys):
 *  - para todo k en keys:        S'[k]          === S[k]          (las claves de
 *    agrupación conservan su tipo original)
 *  - para toda agg en aggs:      S'[agg.alias]  === agg.outputType (cada alias
 *    mapea a su tipo de salida)
 *  - dom(S') = set(keys) ∪ set(aliases)
 *  - el conjunto de claves de S' NO depende del orden de las claves de
 *    agrupación (ni del orden de las agregaciones).
 */

// ---------------------------------------------------------------------------
// Runtime model
// ---------------------------------------------------------------------------

/**
 * Un "type-tag" representa el tipo de valor de una columna (lo que en el sistema
 * de tipos sería `S[P]` o el `Out` de una agregación), incluyendo nullabilidad.
 * Conservar el tag intacto equivale a conservar el tipo.
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

/** Una agregación nombrada: alias de salida + type-tag de salida. */
interface AggModel {
    readonly alias: string;
    readonly outputType: TypeTag;
}

const hasOwn = (o: object, k: string): boolean =>
    Object.prototype.hasOwnProperty.call(o, k);

/**
 * Modelo de runtime de `Aggregated<S, K, A>`: computa el schema resultante de
 * `groupBy(...keys).agg(...aggs)`. Espejo directo de `inferAggSchema` del
 * diseño:
 *
 *   FOR each k IN keys DO S'[k] ← S[k]
 *   FOR each agg IN aggs DO
 *       ASSERT agg.alias NOT IN keys(S')   -- alias único
 *       S'[agg.alias] ← agg.outputType
 *
 * Los `throw` reflejan las precondiciones de tipos (`K extends keyof S` y la
 * unicidad de alias): violarlas en el camino tipado sería un error de
 * compilación.
 */
function inferAggSchema(
    schema: SchemaModel,
    keys: readonly string[],
    aggs: readonly AggModel[],
): SchemaModel {
    const out: SchemaModel = {};
    for (const k of keys) {
        if (!hasOwn(schema, k)) {
            throw new Error(`inferAggSchema: grouping key "${k}" is not in keys(S)`);
        }
        out[k] = schema[k];
    }
    for (const agg of aggs) {
        if (hasOwn(out, agg.alias)) {
            throw new Error(`inferAggSchema: alias "${agg.alias}" is not unique`);
        }
        out[agg.alias] = agg.outputType;
    }
    return out;
}

const setOf = (keys: readonly string[]): Set<string> => new Set(keys);

const sameSet = (a: Set<string>, b: Set<string>): boolean => {
    if (a.size !== b.size) return false;
    for (const x of a) if (!b.has(x)) return false;
    return true;
};

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

/** Genera un schema (registro nombre → type-tag) con claves únicas. */
const schemaArb: fc.Arbitrary<SchemaModel> = fc.dictionary(
    fc.string({ minLength: 1, maxLength: 6 }),
    fc.constantFrom(...TYPE_TAGS),
    { minKeys: 1, maxKeys: 8 },
);

/**
 * Caso de prueba completo: un schema, un subconjunto de claves de agrupación
 * (K ⊆ keys(S)), una permutación de esas claves, y una lista de agregaciones
 * con alias únicos disjuntos de keys(S) (precondición "alias único" del diseño).
 */
interface AggCase {
    readonly schema: SchemaModel;
    readonly groupKeys: string[];
    readonly permutedGroupKeys: string[];
    readonly aggs: AggModel[];
}

const aggCaseArb: fc.Arbitrary<AggCase> = schemaArb.chain((schema) => {
    const allKeys = Object.keys(schema);
    const forbidden = new Set(allKeys);

    // Alias único, disjunto de keys(S) para honrar el ASSERT del algoritmo
    // (los alias no colisionan con las claves de agrupación ni entre sí).
    const aggArb: fc.Arbitrary<AggModel> = fc.record({
        alias: fc
            .string({ minLength: 1, maxLength: 6 })
            .filter((s) => !forbidden.has(s)),
        outputType: fc.constantFrom(...TYPE_TAGS),
    });

    return fc
        .tuple(
            fc.subarray(allKeys),
            fc.uniqueArray(aggArb, {
                selector: (a) => a.alias,
                maxLength: 6,
            }),
        )
        .chain(([groupKeys, aggs]) =>
            fc
                .shuffledSubarray(groupKeys, {
                    minLength: groupKeys.length,
                    maxLength: groupKeys.length,
                })
                .map((permutedGroupKeys) => ({
                    schema,
                    groupKeys,
                    permutedGroupKeys,
                    aggs,
                })),
        );
});

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

describe("Property 6: inferencia de agregación (runtime model of Aggregated<S, K, A>)", () => {
    it("el conjunto de claves del resultado no depende del orden de las claves de agrupación", () => {
        fc.assert(
            fc.property(aggCaseArb, ({ schema, groupKeys, permutedGroupKeys, aggs }) => {
                const a = inferAggSchema(schema, groupKeys, aggs);
                const b = inferAggSchema(schema, permutedGroupKeys, aggs);

                // El SET de claves es invariante ante la permutación...
                expect(sameSet(setOf(Object.keys(a)), setOf(Object.keys(b)))).toBe(true);
                // ...y de hecho el mapa completo (clave → tipo) coincide, ya que
                // un schema es un registro y no depende del orden de inserción.
                expect(a).toEqual(b);
            }),
        );
    });

    it("dom(resultado) === set(keys) ∪ set(aliases)", () => {
        fc.assert(
            fc.property(aggCaseArb, ({ schema, groupKeys, aggs }) => {
                const result = inferAggSchema(schema, groupKeys, aggs);
                const expected = setOf([
                    ...groupKeys,
                    ...aggs.map((agg) => agg.alias),
                ]);
                expect(sameSet(setOf(Object.keys(result)), expected)).toBe(true);
            }),
        );
    });

    it("las claves de agrupación conservan su tipo original (S'[k] === S[k])", () => {
        fc.assert(
            fc.property(aggCaseArb, ({ schema, groupKeys, aggs }) => {
                const result = inferAggSchema(schema, groupKeys, aggs);
                for (const k of groupKeys) {
                    expect(result[k]).toBe(schema[k]);
                }
            }),
        );
    });

    it("cada alias de agregación mapea a su tipo de salida (S'[alias] === agg.outputType)", () => {
        fc.assert(
            fc.property(aggCaseArb, ({ schema, groupKeys, aggs }) => {
                const result = inferAggSchema(schema, groupKeys, aggs);
                for (const agg of aggs) {
                    expect(result[agg.alias]).toBe(agg.outputType);
                }
            }),
        );
    });
});
