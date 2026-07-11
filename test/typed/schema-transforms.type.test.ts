/**
 * Type-level tests for the schema transforms (`src/schema/schema-transforms.ts`).
 *
 * These tests are verified by `tsc -p tsconfig.test.json` (npm script
 * `typecheck:test`), NOT by the runtime test runner. They assert two things:
 *
 *  1. Positive inference — each transform produces the expected output schema,
 *     checked with an `Expect<Equal<Actual, Expected>>` type-equality helper.
 *  2. Negative cases — invalid usage does NOT compile, guarded by
 *     `// @ts-expect-error`. If any of those lines stopped erroring, tsc would
 *     report an "unused" directive and fail the build, so the safety net is
 *     genuinely enforced.
 *
 * A trivial vitest wrapper is included so the file is also a valid (no-op) test
 * when collected by the runtime runner; the real verification is the typecheck.
 *
 * _Requirements: 11.2_
 */

import { describe, expect, it } from "vitest";
import type { ColumnType, Prettify } from "../../src/schema/schema-model";
import type {
    Aggregated,
    Aggregation,
    AmbiguousColumn,
    Dropped,
    Joined,
    Renamed,
    Selected,
    WithColumn,
} from "../../src/schema/schema-transforms";

// ---------------------------------------------------------------------------
// Type-equality assertion helpers (type-challenges style).
// ---------------------------------------------------------------------------

/**
 * Strict, invariant type equality. `Equal<A, B>` is `true` only when `A` and
 * `B` are mutually identical (not merely mutually assignable), so it catches
 * widening, missing keys and nullability drift.
 */
type Equal<X, Y> =
    (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2
        ? true
        : false;

/** Compiles only when `T` is exactly `true`; used to assert `Equal<...>`. */
type Expect<T extends true> = T;

// ---------------------------------------------------------------------------
// Sample schemas used across the assertions.
// ---------------------------------------------------------------------------

type People = {
    id: number;
    name: string;
    age: number | null;
    active: boolean;
};

type Purchases = {
    user_id: number;
    product: string;
    amount: number;
    // `id` deliberately collides with `People.id` to exercise the join policy.
    id: number;
};

// ===========================================================================
// Positive inference
// ===========================================================================

// --- Selected: projects exactly the chosen keys with their original types ---
type _SelectedSingle = Expect<Equal<Selected<People, "name">, { name: string }>>;
type _SelectedMany = Expect<
    Equal<Selected<People, "id" | "name">, { id: number; name: string }>
>;
// Original (nullable) type is preserved through the projection.
type _SelectedNullable = Expect<
    Equal<Selected<People, "age">, { age: number | null }>
>;

// --- Renamed: drops the source key, adds the target with the same type ------
type _Renamed = Expect<
    Equal<
        Renamed<People, "name", "fullName">,
        Prettify<{ id: number; age: number | null; active: boolean; fullName: string }>
    >
>;

// --- WithColumn: adds a new column, or replaces an existing one's type ------
type _WithColumnAdd = Expect<
    Equal<
        WithColumn<People, "score", number>,
        Prettify<{
            id: number;
            name: string;
            age: number | null;
            active: boolean;
            score: number;
        }>
    >
>;
type _WithColumnReplace = Expect<
    Equal<
        WithColumn<People, "age", string>,
        Prettify<{ id: number; name: string; age: string; active: boolean }>
    >
>;

// --- Dropped: removes the named keys ----------------------------------------
type _Dropped = Expect<
    Equal<
        Dropped<People, "age">,
        Prettify<{ id: number; name: string; active: boolean }>
    >
>;
type _DroppedMany = Expect<
    Equal<Dropped<People, "id" | "age">, Prettify<{ name: string; active: boolean }>>
>;

// --- Aggregated: grouping keys (original types) + named aggregate outputs ---
type CountAgg = Aggregation<"n", number>;
type TotalAgg = Aggregation<"total", number | null>;

type _Aggregated = Expect<
    Equal<
        Aggregated<People, "active", [CountAgg, TotalAgg]>,
        Prettify<{ active: boolean; n: number; total: number | null }>
    >
>;
// The result key set does not depend on the order of the grouping keys.
type _AggregatedKeyOrder = Expect<
    Equal<
        Aggregated<People, "id" | "active", [CountAgg]>,
        Aggregated<People, "active" | "id", [CountAgg]>
    >
>;

// --- Joined: disjoint columns merge; collisions become AmbiguousColumn ------
type _Joined = Expect<
    Equal<
        Joined<People, Purchases>,
        Prettify<{
            // left-only columns keep their type
            name: string;
            age: number | null;
            active: boolean;
            // right-only columns keep their type
            user_id: number;
            product: string;
            amount: number;
            // colliding column is marked ambiguous, not silently merged
            id: AmbiguousColumn<"id">;
        }>
    >
>;

// ===========================================================================
// Negative cases — these MUST NOT compile.
// ===========================================================================

// --- Selecting a nonexistent column name ------------------------------------
// @ts-expect-error - "height" is not a key of People
type _SelectMissing = Selected<People, "height">;

// --- Using a dropped column afterwards --------------------------------------
type WithoutId = Dropped<People, "id">;
// @ts-expect-error - "id" was dropped, it is no longer a key of the schema
type _DroppedKeyIndex = WithoutId["id"];
// @ts-expect-error - "id" was dropped, selecting it afterwards is invalid
type _SelectDropped = Selected<WithoutId, "id">;

// --- Using an ambiguous (post-join) column as a valid column ----------------
type JoinedPeoplePurchases = Joined<People, Purchases>;

/** Compiles only when `T` is a usable column type. */
type AssertColumnType<T extends ColumnType> = T;

// @ts-expect-error - merged `id` is AmbiguousColumn<"id">, not a usable ColumnType
type _AmbiguousMisuse = AssertColumnType<JoinedPeoplePurchases["id"]>;

// ---------------------------------------------------------------------------
// Minimal runtime wrapper so the file is a valid (no-op) vitest test too.
// The real verification happens at `tsc -p tsconfig.test.json` time.
// ---------------------------------------------------------------------------

describe("schema transforms (type-level)", () => {
    it("is verified statically by typecheck:test", () => {
        // Reference the positive assertions so they are not flagged as unused.
        const positiveAssertions: true[] = [
            true satisfies _SelectedSingle,
            true satisfies _SelectedMany,
            true satisfies _SelectedNullable,
            true satisfies _Renamed,
            true satisfies _WithColumnAdd,
            true satisfies _WithColumnReplace,
            true satisfies _Dropped,
            true satisfies _DroppedMany,
            true satisfies _Aggregated,
            true satisfies _AggregatedKeyOrder,
            true satisfies _Joined,
        ];
        expect(positiveAssertions.every((value) => value)).toBe(true);
    });
});
