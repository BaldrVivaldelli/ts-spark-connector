/**
 * EXPERIMENTAL — typed scalar functions and caseWhen for the typed DataFrame.
 *
 * Each function declares its input column type(s) and output type, so the type
 * flows through `withColumn`/`select`. At runtime they build the same proto
 * expressions the untyped API uses via `ProtoExprAlg`.
 */

import { ProtoExprAlg } from "../engine/compilerRead";
import { Condition, TypedColumn, NumericColumn, ScalarType, ColumnType } from "./typed-dataframe";

type ProtoExpr = unknown;

function exprOf(c: TypedColumn<ScalarType | null>): ProtoExpr {
    return (c as unknown as { expr: ProtoExpr }).expr;
}

// ---- String functions (string -> ...) -------------------------------------

/** `length(col)` — string column -> numeric length. */
export function length(col: TypedColumn<string | null>): NumericColumn<number | null> {
    return new NumericColumn<number | null>(ProtoExprAlg.call("length", [exprOf(col)] as never[]));
}

/** `upper(col)` — uppercases a string column. Nullability is preserved. */
export function upper<T extends string | null>(col: TypedColumn<T>): TypedColumn<T> {
    return new TypedColumn<T>(ProtoExprAlg.call("upper", [exprOf(col)] as never[]));
}

/** `lower(col)` — lowercases a string column. */
export function lower<T extends string | null>(col: TypedColumn<T>): TypedColumn<T> {
    return new TypedColumn<T>(ProtoExprAlg.call("lower", [exprOf(col)] as never[]));
}

/** `concat(a, b, ...)` — concatenates string columns/literals into a string. */
export function concat(...parts: Array<TypedColumn<string | null> | string>): TypedColumn<string | null> {
    const args = parts.map(p =>
        typeof p === "string" ? ProtoExprAlg.lit(p) : exprOf(p)
    );
    return new TypedColumn<string | null>(ProtoExprAlg.call("concat", args as never[]));
}

// ---- Numeric functions (number -> number) ----------------------------------

/** `abs(col)` — absolute value, preserving nullability. */
export function abs<T extends number | null>(col: TypedColumn<T>): NumericColumn<T> {
    return new NumericColumn<T>(ProtoExprAlg.call("abs", [exprOf(col)] as never[]));
}

/** `round(col, scale)` — rounds a numeric column. */
export function round<T extends number | null>(col: TypedColumn<T>, scale = 0): NumericColumn<T> {
    return new NumericColumn<T>(
        ProtoExprAlg.call("round", [exprOf(col), ProtoExprAlg.lit(scale)] as never[])
    );
}

// ---- caseWhen --------------------------------------------------------------

/**
 * Typed `when(...).then(...)...otherwise(...)`. Every branch result and the
 * `otherwise` value must share the same scalar type `T`, so the resulting
 * column is `TypedColumn<T>`.
 *
 * `T` is widened from the first value's literal to its base scalar type (so
 * `when(cond, "high")` yields a `string` chain, not a `"high"` chain), which
 * lets later branches use other values of the same base type.
 */
export function when<V extends ScalarType>(
    cond: Condition,
    value: V | TypedColumn<Widen<V>>
): CaseChain<Widen<V>> {
    return new CaseChain<Widen<V>>([{ when: condExpr(cond), then: valueExpr(value) }]);
}

/** Widens a scalar literal to its base type (`"high"` -> `string`, `1` -> `number`). */
type Widen<V> = V extends string ? string : V extends number ? number : V extends boolean ? boolean : V;

class CaseChain<T extends ScalarType> {
    /** @internal */ constructor(
        private readonly branches: Array<{ when: ProtoExpr; then: ProtoExpr }>
    ) {}

    when(cond: Condition, value: T | TypedColumn<T>): CaseChain<T> {
        return new CaseChain<T>([...this.branches, { when: condExpr(cond), then: valueExpr(value) }]);
    }

    /** Closes the chain. The result is nullable (the else branch may be null). */
    otherwise(value: T | TypedColumn<T> | null): TypedColumn<T | null> {
        const elze = value === null ? ProtoExprAlg.lit(null as never) : valueExpr(value);
        return new TypedColumn<T | null>(ProtoExprAlg.caseWhen(this.branches, elze));
    }
}

function condExpr(cond: Condition): ProtoExpr {
    return (cond as unknown as { expr: ProtoExpr }).expr;
}

function valueExpr(value: ScalarType | TypedColumn<ColumnType>): ProtoExpr {
    return value instanceof TypedColumn
        ? (value as unknown as { expr: ProtoExpr }).expr
        : ProtoExprAlg.lit(value as ScalarType);
}
