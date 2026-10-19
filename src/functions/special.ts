// Functions whose arguments need more than the catalog's coercions:
// higher-order functions taking JS lambdas, overloads and PySpark special cases.

import { LiteralValue } from "../algebra/read";
import { EBuilder, SortKeyBuilder, col as columnRef, exprBuilder } from "../engine/column";
import { Arg, c, cn, ColumnOrName, ColumnOrNumber, invoke, invokeDistinct, l, maybe } from "./args";

export { col, expr, lit, when } from "../engine/column";

/**
 * A column by name; same as `col`.
 * @public
 */
export const column = (name: string): EBuilder => columnRef(name);

// Aggregates ----------------------------------------------------------------

/**
 * Number of rows of the group (`count("*")`) or of non-null values of a column.
 *
 * Spark SQL: `count`.
 * @public
 */
export function count(col: ColumnOrName): EBuilder {
    // Spark only rewrites `count(*)` to `count(1)` in its SQL parser; a star
    // argument sent over Connect would expand to every column instead.
    return invoke("count", [col === "*" ? l(1) : c(col)]);
}

/**
 * Number of distinct non-null combinations of the columns.
 *
 * Spark SQL: `count(DISTINCT ...)`.
 * @public
 */
export function count_distinct(col: ColumnOrName, ...cols: ColumnOrName[]): EBuilder {
    return invokeDistinct("count", [col, ...cols].map(c));
}

/**
 * Sum of the distinct values of the group.
 *
 * Spark SQL: `sum(DISTINCT ...)`.
 * @public
 */
export function sum_distinct(col: ColumnOrName): EBuilder {
    return invokeDistinct("sum", [c(col)]);
}

// Math ----------------------------------------------------------------------

/**
 * The two forms of `log`.
 * @public
 */
export type LogFunction = {
    /** Natural logarithm. */
    (col: ColumnOrName): EBuilder;
    /** Logarithm of `col` in `base`. */
    (base: ColumnOrNumber, col: ColumnOrName): EBuilder;
};

/**
 * `log(col)` is the natural logarithm; `log(base, col)` the logarithm in `base`.
 *
 * Spark SQL: `ln` / `log`.
 * @public
 */
export const log: LogFunction = (baseOrCol: ColumnOrNumber, col?: ColumnOrName): EBuilder => {
    if (col !== undefined) return invoke("log", [cn(baseOrCol), c(col)]);
    if (typeof baseOrCol === "number") {
        throw new TypeError("log(base) needs the column: use log(base, col), or log(col) for ln.");
    }
    return invoke("ln", [c(baseOrCol)]);
};

// Trimming --------------------------------------------------------------------

// Spark's two-argument trim/ltrim/rtrim take the characters to remove FIRST
// (`trim(trimStr, str)`), unlike `btrim(str, trimStr)`; these keep PySpark's
// `(col, trim)` order and swap the arguments on the wire.
const trimFunction = (name: string) => (col: ColumnOrName, trim?: string | EBuilder): EBuilder =>
    trim === undefined ? invoke(name, [c(col)]) : invoke(name, [l(trim), c(col)]);

/**
 * Removes leading and trailing `trim` characters (spaces by default).
 *
 * Spark SQL: `trim`.
 * @public
 */
export const trim: (col: ColumnOrName, trim?: string | EBuilder) => EBuilder = trimFunction("trim");

/**
 * Removes leading `trim` characters (spaces by default).
 *
 * Spark SQL: `ltrim`.
 * @public
 */
export const ltrim: (col: ColumnOrName, trim?: string | EBuilder) => EBuilder = trimFunction("ltrim");

/**
 * Removes trailing `trim` characters (spaces by default).
 *
 * Spark SQL: `rtrim`.
 * @public
 */
export const rtrim: (col: ColumnOrName, trim?: string | EBuilder) => EBuilder = trimFunction("rtrim");

// Time windows ----------------------------------------------------------------

/**
 * Buckets rows into tumbling (`slideDuration` omitted) or sliding time windows
 * of `windowDuration`, e.g. `window("ts", "10 minutes", "5 minutes")`. The
 * result is a `struct<start, end>` column.
 *
 * Spark SQL: `window`.
 * @public
 */
export function window(
    timeColumn: ColumnOrName,
    windowDuration: string,
    slideDuration?: string,
    startTime?: string,
): EBuilder {
    // Spark needs the slide when a start offset is given; PySpark defaults it
    // to the window duration (a tumbling window).
    const slide = slideDuration ?? (startTime === undefined ? undefined : windowDuration);
    return invoke("window", [c(timeColumn), l(windowDuration)], [maybe(slide, l), maybe(startTime, l)]);
}

// Higher-order functions ------------------------------------------------------

let lambdaVariables = 0;

const isBuilder = (x: unknown): x is EBuilder =>
    typeof x === "object" && x !== null && "build" in x;

/**
 * Turns a JS callback into a Spark lambda. Each parameter gets a fresh name
 * so nested lambdas never capture each other's variables.
 */
function lambda(
    fn: (...params: EBuilder[]) => EBuilder,
    names: readonly string[],
): Arg {
    const variables = names.map(name => `${name}_${++lambdaVariables}`);
    const body = fn(...variables.map(variable => exprBuilder(EX => EX.lambdaVar(variable))));
    if (!isBuilder(body)) {
        throw new TypeError("A higher-order function callback must return a column expression.");
    }
    return EX => EX.lambda(variables, body.build(EX));
}

/** Element callbacks may declare a second parameter to receive the index. */
const elementLambda = (fn: (x: EBuilder, i: EBuilder) => EBuilder): Arg =>
    lambda(fn as (...params: EBuilder[]) => EBuilder, fn.length >= 2 ? ["x", "i"] : ["x"]);

/**
 * Applies `fn` to every element of an array. Declare a second parameter to
 * receive the 0-based index: `transform("xs", (x, i) => x.plus(i))`.
 *
 * Spark SQL: `transform`.
 * @public
 */
export function transform(col: ColumnOrName, fn: (x: EBuilder, i: EBuilder) => EBuilder): EBuilder {
    return invoke("transform", [c(col), elementLambda(fn)]);
}

/**
 * The elements of an array for which `fn` is `true`; `fn` may take the
 * 0-based index as its second parameter.
 *
 * Spark SQL: `filter`.
 * @public
 */
export function filter(col: ColumnOrName, fn: (x: EBuilder, i: EBuilder) => EBuilder): EBuilder {
    return invoke("filter", [c(col), elementLambda(fn)]);
}

/**
 * `true` when `fn` is `true` for some element of the array.
 *
 * Spark SQL: `exists`.
 * @public
 */
export function exists(col: ColumnOrName, fn: (x: EBuilder) => EBuilder): EBuilder {
    return invoke("exists", [c(col), lambda(fn, ["x"])]);
}

/**
 * `true` when `fn` is `true` for every element of the array.
 *
 * Spark SQL: `forall`.
 * @public
 */
export function forall(col: ColumnOrName, fn: (x: EBuilder) => EBuilder): EBuilder {
    return invoke("forall", [c(col), lambda(fn, ["x"])]);
}

/**
 * Folds an array: starts from `initialValue`, combines it with each element
 * through `merge` and maps the result through `finish`. The accumulator type
 * must stay the same (cast `initialValue` when the elements are wider).
 *
 * Spark SQL: `aggregate`.
 * @public
 */
export function aggregate(
    col: ColumnOrName,
    initialValue: LiteralValue | EBuilder,
    merge: (acc: EBuilder, x: EBuilder) => EBuilder,
    finish?: (acc: EBuilder) => EBuilder,
): EBuilder {
    return fold("aggregate", col, initialValue, merge, finish);
}

/**
 * Same as `aggregate`.
 *
 * Spark SQL: `reduce`.
 * @public
 */
export function reduce(
    col: ColumnOrName,
    initialValue: LiteralValue | EBuilder,
    merge: (acc: EBuilder, x: EBuilder) => EBuilder,
    finish?: (acc: EBuilder) => EBuilder,
): EBuilder {
    return fold("reduce", col, initialValue, merge, finish);
}

function fold(
    name: string,
    col: ColumnOrName,
    initialValue: LiteralValue | EBuilder,
    merge: (acc: EBuilder, x: EBuilder) => EBuilder,
    finish?: (acc: EBuilder) => EBuilder,
): EBuilder {
    return invoke(
        name,
        [c(col), l(initialValue), lambda(merge, ["acc", "x"])],
        [maybe(finish, fn => lambda(fn, ["acc"]))],
    );
}

/**
 * Merges two arrays element-wise with `fn`; the shorter one is padded with nulls.
 *
 * Spark SQL: `zip_with`.
 * @public
 */
export function zip_with(
    left: ColumnOrName,
    right: ColumnOrName,
    fn: (x: EBuilder, y: EBuilder) => EBuilder,
): EBuilder {
    return invoke("zip_with", [c(left), c(right), lambda(fn, ["x", "y"])]);
}

/**
 * Sorts an array ascending (nulls last), or with a comparator returning a
 * negative, zero or positive integer: `array_sort("xs", (l, r) => ...)`.
 *
 * Spark SQL: `array_sort`.
 * @public
 */
export function array_sort(
    col: ColumnOrName,
    comparator?: (left: EBuilder, right: EBuilder) => EBuilder,
): EBuilder {
    return invoke("array_sort", [c(col)], [maybe(comparator, fn => lambda(fn, ["l", "r"]))]);
}

/**
 * The entries of a map for which `fn(key, value)` is `true`.
 *
 * Spark SQL: `map_filter`.
 * @public
 */
export function map_filter(col: ColumnOrName, fn: (key: EBuilder, value: EBuilder) => EBuilder): EBuilder {
    return invoke("map_filter", [c(col), lambda(fn, ["k", "v"])]);
}

/**
 * A map whose keys are `fn(key, value)`.
 *
 * Spark SQL: `transform_keys`.
 * @public
 */
export function transform_keys(col: ColumnOrName, fn: (key: EBuilder, value: EBuilder) => EBuilder): EBuilder {
    return invoke("transform_keys", [c(col), lambda(fn, ["k", "v"])]);
}

/**
 * A map whose values are `fn(key, value)`.
 *
 * Spark SQL: `transform_values`.
 * @public
 */
export function transform_values(col: ColumnOrName, fn: (key: EBuilder, value: EBuilder) => EBuilder): EBuilder {
    return invoke("transform_values", [c(col), lambda(fn, ["k", "v"])]);
}

/**
 * Merges two maps key by key with `fn(key, value1, value2)`; a key missing
 * from one map gives `null` for its value.
 *
 * Spark SQL: `map_zip_with`.
 * @public
 */
export function map_zip_with(
    col1: ColumnOrName,
    col2: ColumnOrName,
    fn: (key: EBuilder, value1: EBuilder, value2: EBuilder) => EBuilder,
): EBuilder {
    return invoke("map_zip_with", [c(col1), c(col2), lambda(fn, ["k", "v1", "v2"])]);
}

// Sort keys -------------------------------------------------------------------

const sortKey = (direction: "asc" | "desc", nulls?: "nullsFirst" | "nullsLast") =>
    (col: ColumnOrName): SortKeyBuilder =>
        EX => ({ expr: c(col)(EX), direction, nulls });

/** Ascending sort key for `orderBy`/`sort`. @public */
export const asc: (col: ColumnOrName) => SortKeyBuilder = sortKey("asc");
/** Ascending sort key with nulls first. @public */
export const asc_nulls_first: (col: ColumnOrName) => SortKeyBuilder = sortKey("asc", "nullsFirst");
/** Ascending sort key with nulls last. @public */
export const asc_nulls_last: (col: ColumnOrName) => SortKeyBuilder = sortKey("asc", "nullsLast");
/** Descending sort key for `orderBy`/`sort`. @public */
export const desc: (col: ColumnOrName) => SortKeyBuilder = sortKey("desc");
/** Descending sort key with nulls first. @public */
export const desc_nulls_first: (col: ColumnOrName) => SortKeyBuilder = sortKey("desc", "nullsFirst");
/** Descending sort key with nulls last. @public */
export const desc_nulls_last: (col: ColumnOrName) => SortKeyBuilder = sortKey("desc", "nullsLast");
