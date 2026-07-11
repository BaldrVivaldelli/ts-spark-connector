/**
 * Property-based tests for join schema inference (task 2.5).
 *
 * Validates the type-level `Joined<L, R>` / `AmbiguousColumn` policy from
 * `src/schema/schema-transforms.ts` through an executable *runtime model* of
 * `inferJoinSchema(L, R)`, mirroring the design pseudocode
 * (design.md, "Pseudocódigo de la inferencia de schema en join").
 *
 * Properties under test:
 *   - **Property 4: Join disjunto sin pérdida** — if keys(L) ∩ keys(R) = ∅ then
 *     keys(Joined) = keys(L) ∪ keys(R) and NO column is marked ambiguous.
 *   - **Property 5: Join con colisión es seguro** — for any n ∈ keys(L) ∩ keys(R),
 *     Joined[n] is the ambiguous marker, and disjoint columns keep their types.
 *
 * **Validates: Requirements 6.1, 6.2, 6.3**
 */
import { describe, expect, it } from "vitest";
import fc from "fast-check";

// ---------------------------------------------------------------------------
// Runtime model of the schema and of inferJoinSchema (mirrors the design)
// ---------------------------------------------------------------------------

/**
 * Runtime tag for a column value type. This is the runtime mirror of the
 * type-level `ColumnType = string | number | boolean | null` from
 * `src/schema/schema-model.ts`.
 */
type ColTag = "string" | "number" | "boolean" | "null";

/** Runtime mirror of `Schema = Record<string, ColumnType>`. */
type RuntimeSchema = Record<string, ColTag>;

/**
 * Runtime mirror of the type-level `AmbiguousColumn<Name>` marker. A column
 * present in both sides of a join is tagged with this instead of choosing a
 * side, so any later use of it can be rejected.
 */
interface AmbiguousMarker {
    readonly __ambiguous: string;
}

/** A joined column is either a concrete type tag or the ambiguous marker. */
type JoinedColumn = ColTag | AmbiguousMarker;

/** Runtime mirror of `Joined<L, R>`. */
type JoinedSchema = Record<string, JoinedColumn>;

const COL_TAGS: readonly ColTag[] = ["string", "number", "boolean", "null"];

function has(obj: RuntimeSchema, key: string): boolean {
    return Object.prototype.hasOwnProperty.call(obj, key);
}

function isAmbiguous(c: JoinedColumn): c is AmbiguousMarker {
    return typeof c === "object" && c !== null && "__ambiguous" in c;
}

/**
 * Executable model of the type-level `Joined<L, R>`, following the design
 * pseudocode exactly:
 *   - n only in L  -> S'[n] = L[n]
 *   - n only in R  -> S'[n] = R[n]
 *   - n in L ∩ R   -> S'[n] = AmbiguousColumn(n)
 */
function inferJoinSchema(L: RuntimeSchema, R: RuntimeSchema): JoinedSchema {
    const out: JoinedSchema = Object.create(null) as JoinedSchema;
    for (const n of Object.keys(L)) {
        if (!has(R, n)) {
            out[n] = L[n];
        } else {
            out[n] = { __ambiguous: n };
        }
    }
    for (const n of Object.keys(R)) {
        if (!has(L, n)) {
            out[n] = R[n];
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// Generators: two schemas with controllable key overlap
// ---------------------------------------------------------------------------

const arbColTag: fc.Arbitrary<ColTag> = fc.constantFrom(...COL_TAGS);
const arbKey: fc.Arbitrary<string> = fc.string({ minLength: 1, maxLength: 6 });

/**
 * Generates two schemas with **disjoint** keys: every key is assigned to
 * exactly one side, so keys(L) ∩ keys(R) = ∅ holds by construction.
 */
const disjointSchemas: fc.Arbitrary<{ L: RuntimeSchema; R: RuntimeSchema }> =
    fc
        .uniqueArray(arbKey, { maxLength: 12 })
        .chain((keys) =>
            fc
                .array(fc.tuple(arbColTag, fc.boolean()), {
                    minLength: keys.length,
                    maxLength: keys.length,
                })
                .map((assigns) => {
                    const L: RuntimeSchema = Object.create(null);
                    const R: RuntimeSchema = Object.create(null);
                    keys.forEach((k, i) => {
                        const [tag, toLeft] = assigns[i];
                        if (toLeft) L[k] = tag;
                        else R[k] = tag;
                    });
                    return { L, R };
                }),
        );

/**
 * Generates two schemas with **controllable overlap**: each unique key lands in
 * one of three buckets — left-only, right-only, or shared (collision). The
 * `shared` bucket is biased to make collisions common. Returns the schemas plus
 * the bookkeeping needed to assert the postconditions.
 */
const overlappingSchemas: fc.Arbitrary<{
    L: RuntimeSchema;
    R: RuntimeSchema;
    shared: string[];
    leftOnly: Record<string, ColTag>;
    rightOnly: Record<string, ColTag>;
}> = fc
    .uniqueArray(arbKey, { minLength: 1, maxLength: 12 })
    .chain((keys) =>
        fc
            .array(
                // [leftType, rightType, bucket]; bucket 2 (shared) is weighted.
                fc.tuple(arbColTag, arbColTag, fc.constantFrom(0, 1, 2, 2)),
                { minLength: keys.length, maxLength: keys.length },
            )
            .map((assigns) => {
                const L: RuntimeSchema = Object.create(null);
                const R: RuntimeSchema = Object.create(null);
                const shared: string[] = [];
                const leftOnly: Record<string, ColTag> = Object.create(null);
                const rightOnly: Record<string, ColTag> = Object.create(null);
                keys.forEach((k, i) => {
                    const [lt, rt, bucket] = assigns[i];
                    if (bucket === 0) {
                        L[k] = lt;
                        leftOnly[k] = lt;
                    } else if (bucket === 1) {
                        R[k] = rt;
                        rightOnly[k] = rt;
                    } else {
                        L[k] = lt;
                        R[k] = rt;
                        shared.push(k);
                    }
                });
                return { L, R, shared, leftOnly, rightOnly };
            }),
    );

// ---------------------------------------------------------------------------
// Property 4: Join disjunto sin pérdida
// ---------------------------------------------------------------------------

describe("inferJoinSchema — Property 4: join disjunto sin pérdida", () => {
    it("keys(Joined) = keys(L) ∪ keys(R) and no column is ambiguous", () => {
        fc.assert(
            fc.property(disjointSchemas, ({ L, R }) => {
                const joined = inferJoinSchema(L, R);

                const joinedKeys = new Set(Object.keys(joined));
                const union = new Set([...Object.keys(L), ...Object.keys(R)]);

                // keys(Joined) = keys(L) ∪ keys(R)
                expect(joinedKeys).toEqual(union);

                // No column is marked ambiguous and every type is preserved.
                for (const n of Object.keys(joined)) {
                    const col = joined[n];
                    expect(isAmbiguous(col)).toBe(false);
                    const original = has(L, n) ? L[n] : R[n];
                    expect(col).toBe(original);
                }
            }),
        );
    });
});

// ---------------------------------------------------------------------------
// Property 5: Join con colisión es seguro
// ---------------------------------------------------------------------------

describe("inferJoinSchema — Property 5: join con colisión es seguro", () => {
    it("collided columns become the ambiguous marker; disjoint columns keep their types", () => {
        fc.assert(
            fc.property(overlappingSchemas, ({ L, R, shared, leftOnly, rightOnly }) => {
                const joined = inferJoinSchema(L, R);

                // Domain is still the full union of keys.
                const joinedKeys = new Set(Object.keys(joined));
                const union = new Set([...Object.keys(L), ...Object.keys(R)]);
                expect(joinedKeys).toEqual(union);

                // Every n in keys(L) ∩ keys(R) is the ambiguous marker for n.
                for (const n of shared) {
                    const col = joined[n];
                    expect(isAmbiguous(col)).toBe(true);
                    expect((col as AmbiguousMarker).__ambiguous).toBe(n);
                }

                // Disjoint columns keep their original types (no ambiguity).
                for (const n of Object.keys(leftOnly)) {
                    expect(joined[n]).toBe(leftOnly[n]);
                }
                for (const n of Object.keys(rightOnly)) {
                    expect(joined[n]).toBe(rightOnly[n]);
                }
            }),
        );
    });

    it("a column is ambiguous in the result iff it exists on both sides", () => {
        fc.assert(
            fc.property(overlappingSchemas, ({ L, R }) => {
                const joined = inferJoinSchema(L, R);
                for (const n of Object.keys(joined)) {
                    const inBoth = has(L, n) && has(R, n);
                    expect(isAmbiguous(joined[n])).toBe(inBoth);
                }
            }),
        );
    });
});
