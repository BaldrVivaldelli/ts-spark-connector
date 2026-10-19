import { NullsOrder, SortDirection, WindowSpec } from "../../types";

/** @internal */
export type LiteralValue = string | number | boolean | bigint | null;

/**
 * How a cast behaves on values that do not fit the target type: Spark's
 * session default (unspecified), `legacy`, `ansi` (fail), or `try` (null).
 * @public
 */
export type CastEvalMode = "legacy" | "ansi" | "try";

/** @internal */
export interface ExprAlg<E> {
    col(name: string, planId?: number): E;
    lit(v: LiteralValue): E;
    bin(op: string, left: E, right: E): E;
    logical(op: "AND" | "OR", left: E, right: E): E;
    alias(input: E, ...names: string[]): E;
    call(name: string, args: E[]): E;
    /** `name(DISTINCT args...)`, e.g. `count(DISTINCT x)`. */
    callDistinct(name: string, args: E[]): E;
    /** A Spark SQL expression parsed by the server, e.g. `expr("a + 1")`. */
    exprString(sql: string): E;
    /** Casts to a Spark DDL type string such as `int` or `array<string>`. */
    cast(input: E, type: string, evalMode?: CastEvalMode): E;
    /** A lambda for higher-order functions; `params` are the variable names. */
    lambda(params: string[], body: E): E;
    /** A reference to a lambda parameter bound by an enclosing `lambda`. */
    lambdaVar(name: string): E;
    /** Adds or replaces (`value` given) or drops (`value` omitted) a struct field. */
    updateFields(struct: E, field: string, value?: E): E;
    /** `child[extraction]`: array index (0-based), map key or struct field. */
    extractValue(child: E, extraction: E): E;
    /** Every column whose name matches `regex` (`df.colRegex`), optionally scoped to a plan. */
    colRegex(regex: string, planId?: number): E;
    /** A metadata column such as `_metadata` (`df.metadataColumn`). */
    metadataCol(name: string, planId?: number): E;
    sortKey(input: E, direction: SortDirection, nulls?: NullsOrder): E;
    star(): E;
    caseWhen(branches: Array<{ when: E; then: E }>, elze?: E): E;
    win(func: E, spec: WindowSpec<E>): E;
    isNull(input: E): E;
    isNotNull(input: E): E;
    coalesce(args: E[]): E;
    explode(input: E): E;
    posexplode(input: E): E;
    getField(input: E, field: string): E;
    map_keys(input: E): E;
    map_values(input: E): E;
    elementAt(map: E, key: E): E;
    getItem(collectionExpr: E, key: E | string | number): E;
    split(input: E, delimiter: E | string): E;
    from_json(jsonExpr: E, schema: string): E;
    to_json(expr: E): E;
}
