/**
 * Type-level tests for the typed builders:
 *  - columns        (`src/typed/typed-column.ts`: TypedColumn / NumericColumn / Condition)
 *  - scalar fns     (`src/typed/functions.ts`: length / abs / when().otherwise())
 *  - aggregations   (`src/typed/aggregations.ts`: makeAggFactory -> count / sum / ...)
 *
 * Like `schema-transforms.type.test.ts`, these are verified by
 * `tsc -p tsconfig.test.json` (npm script `typecheck:test`), NOT by the runtime
 * runner. They assert two things:
 *
 *  1. Positive inference — each builder produces the expected value type
 *     (including nullability), checked with `Expect<Equal<Actual, Expected>>`.
 *  2. Negative cases — incompatible-type arguments do NOT compile, guarded by
 *     `// @ts-expect-error`. If any of those lines stopped erroring, tsc would
 *     report an "unused" directive (TS2578) and fail the build, so the safety
 *     net is genuinely enforced.
 *
 * The builders are generic over the interpreter algebra `E`; the concrete `E`
 * is irrelevant to these type checks, so we pin it to a placeholder (`unknown`)
 * and construct the operand columns via `declare const` (type-only, erased at
 * runtime). All statements that touch those erased operands live inside
 * never-called functions, so tsc type-checks them while the runtime runner
 * never executes them. The real verification is the typecheck; the vitest
 * wrapper is a no-op that just keeps the file a valid test file.
 *
 * _Requirements: 8.3, 11.2_
 */

import { describe, expect, it } from "vitest";
import { abs, length, when } from "../../src/typed/functions";
import { makeAggFactory } from "../../src/typed/aggregations";
import type { Condition, NumericColumn, TypedColumn } from "../../src/typed/typed-column";

// ---------------------------------------------------------------------------
// Type-equality assertion helpers (type-challenges style).
// ---------------------------------------------------------------------------

/**
 * Strict, invariant type equality. `Equal<A, B>` is `true` only when `A` and
 * `B` are mutually identical (not merely mutually assignable), so it catches
 * widening and nullability drift.
 */
type Equal<X, Y> =
    (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2
        ? true
        : false;

/** Compiles only when `T` is exactly `true`; used to assert `Equal<...>`. */
type Expect<T extends true> = T;

/** Extracts the value type `T` carried by a `TypedColumn<T, E>` (or subclass). */
type ColValue<C> = C extends TypedColumn<infer T, infer _E> ? T : never;

/** Extracts the `{ [Alias]: Out }` phantom shape carried by an `Aggregation`. */
type AggOut<A> = A extends { __out: infer O } ? O : never;

// ---------------------------------------------------------------------------
// Sample schema + placeholder operand columns over a fixed algebra `E`.
//
// `declare const` emits no runtime code, so referencing these only ever happens
// inside the never-called functions below (never at import time).
// ---------------------------------------------------------------------------

type People = {
    id: number;
    name: string;
    age: number | null;
    amount: number;
};

// `unknown` is an arbitrary placeholder for the interpreter algebra `E`.
declare const age: NumericColumn<number, unknown>;
declare const ageOpt: NumericColumn<number | null, unknown>;
declare const longOpt: NumericColumn<bigint | null, unknown>;
declare const name: TypedColumn<string, unknown>;
declare const nameOpt: TypedColumn<string | null, unknown>;
declare const cond: Condition<unknown>;

// ===========================================================================
// Positive inference — the builders infer the expected value type.
//
// Housed in a never-called function: tsc fully type-checks the body (so each
// `Expect<Equal<...>>` constraint is evaluated), but the runtime runner never
// executes it, so the erased `declare const` operands are never dereferenced.
// ===========================================================================

function _positiveInference(): void {
    // --- abs preserves the nullability of its input column ------------------
    const absNullable = abs(ageOpt); // NumericColumn<number | null, unknown>
    type _AbsNullable = Expect<Equal<ColValue<typeof absNullable>, number | null>>;

    const absNonNull = abs(age); // NumericColumn<number, unknown>
    type _AbsNonNull = Expect<Equal<ColValue<typeof absNonNull>, number>>;

    // --- length always returns a numeric, nullable column -------------------
    const lengthResult = length(nameOpt); // NumericColumn<number | null, unknown>
    type _LengthNumericNullable = Expect<Equal<ColValue<typeof lengthResult>, number | null>>;

    // --- when().when().otherwise() yields the (nullable) branch value type --
    const caseResult = when(cond, "high").when(cond, "mid").otherwise("low");
    type _CaseBranches = Expect<Equal<ColValue<typeof caseResult>, string | null>>;

    // Spark `/` always returns floating point, while mixed long/double
    // arithmetic widens and integral long arithmetic stays bigint.
    const dividedLong = longOpt.div(2n);
    type _DividedLong = Expect<Equal<ColValue<typeof dividedLong>, number | null>>;
    const integralLong = longOpt.plus(2n);
    type _IntegralLong = Expect<Equal<ColValue<typeof integralLong>, bigint | null>>;
    const widenedLong = longOpt.plus(1.5);
    type _WidenedLong = Expect<Equal<ColValue<typeof widenedLong>, number | null>>;

    // A nullable fallback cannot remove nullability from coalesce.
    const stillNullable = ageOpt.coalesce(ageOpt);
    type _StillNullable = Expect<Equal<ColValue<typeof stillNullable>, number | null>>;
    const madeNonNull = ageOpt.coalesce(age);
    type _MadeNonNull = Expect<Equal<ColValue<typeof madeNonNull>, number>>;

    // --- Spark COUNT is BIGINT; SUM(INT) may widen number to bigint --------
    const countAgg = makeAggFactory<People, unknown>().count().as("n");
    type _CountOut = Expect<Equal<AggOut<typeof countAgg>, { n: bigint }>>;

    const sumAgg = makeAggFactory<People, unknown>().sum("amount").as("total");
    type _SumOut = Expect<
        Equal<AggOut<typeof sumAgg>, { total: number | bigint | null }>
    >;
}

// ===========================================================================
// Negative cases — these MUST NOT compile (Requirement 8.3).
//
// Same never-called-function trick: the offending statements are type-checked
// by tsc (each `@ts-expect-error` must suppress a real error) but never run.
// ===========================================================================

function _mustNotCompile(): void {
    // Comparing a numeric column against a string is a type error.
    // @ts-expect-error - age is numeric; "old" is not a number
    age.gt("old");

    // Arithmetic on a numeric column with a string argument is a type error.
    // @ts-expect-error - plus expects a number or numeric column, not a string
    age.plus("x");

    // length() applied to a numeric column is a type error.
    // @ts-expect-error - length expects a string column, not a numeric one
    length(age);

    // abs() applied to a string column is a type error.
    // @ts-expect-error - abs expects a numeric column, not a string one
    abs(name);

    // when()/otherwise() with mismatched branch value types is a type error.
    // @ts-expect-error - the first branch is a string; 1 is a number
    when(cond, "high").when(cond, 1).otherwise("low");

    // Aggregating a column name that is not in the schema is a type error.
    // @ts-expect-error - "missing" is not a key of People
    makeAggFactory<People, unknown>().sum("missing");
}

// ---------------------------------------------------------------------------
// Minimal runtime wrapper so the file is a valid (no-op) vitest test too.
// The real verification happens at `tsc -p tsconfig.test.json` time.
// ---------------------------------------------------------------------------

describe("typed builders (type-level)", () => {
    it("is verified statically by typecheck:test", () => {
        // Reference the fixtures so they are retained; they are never invoked.
        expect(typeof _positiveInference).toBe("function");
        expect(typeof _mustNotCompile).toBe("function");
    });
});
