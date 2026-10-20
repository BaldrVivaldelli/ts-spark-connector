// Argument coercion shared by the generated and hand-written `functions`.

import { ExprAlg, LiteralValue } from "../algebra/read";
import { EBuilder, exprBuilder } from "../engine/column";

/**
 * A column given as an expression or by name. As in PySpark, a bare string
 * passed where a column is expected is a column name, never a literal.
 * @public
 */
export type ColumnOrName = EBuilder | string;

/**
 * A column, a column name, or a number literal (PySpark's
 * `ColumnOrName | float` arguments such as `pow(col, 2)`).
 * @public
 */
export type ColumnOrNumber = ColumnOrName | number;

/** @internal An argument still waiting for the interpreter. */
export type Arg = <E>(EX: ExprAlg<E>) => E;

const isBuilder = (x: unknown): x is EBuilder =>
    typeof x === "object" && x !== null && "build" in x;

/** @internal Column argument: a name or an expression. */
export const c = (column: ColumnOrName): Arg => EX =>
    typeof column === "string" ? EX.col(column) : column.build(EX);

/** @internal Literal argument: a JS value or an expression computing it. */
export const l = (value: LiteralValue | EBuilder): Arg => EX =>
    isBuilder(value) ? value.build(EX) : EX.lit(value);

/** @internal Column, column name, or number literal. */
export const cn = (value: ColumnOrNumber): Arg =>
    typeof value === "number" ? l(value) : c(value);

/** @internal Coerces an optional argument, keeping `undefined` as "omitted". */
export const maybe = <T>(value: T | undefined, coerce: (value: T) => Arg): Arg | undefined =>
    value === undefined ? undefined : coerce(value);

/**
 * @internal Data source options as Spark expects them in functions such as
 * `from_json`: a `map(key1, value1, ...)` of string literals.
 */
export const opts = (options: Readonly<Record<string, string>> | undefined): Arg | undefined => {
    if (options === undefined) return undefined;
    const entries = Object.entries(options);
    return EX => EX.call("map", entries.flatMap(([key, value]) => [EX.lit(key), EX.lit(String(value))]));
};

/**
 * @internal Builds `name(args..., optional...)`. Optional arguments are
 * trailing; an omitted one followed by a provided one would shift every later
 * argument into the wrong position, so it is rejected.
 */
export function invoke(
    name: string,
    args: readonly Arg[],
    optional: ReadonlyArray<Arg | undefined> = [],
): EBuilder {
    let end = optional.length;
    while (end > 0 && optional[end - 1] === undefined) end -= 1;
    const trailing = optional.slice(0, end);
    if (trailing.some(arg => arg === undefined)) {
        throw new TypeError(
            `${name}(): an optional argument was omitted before one that was given.`,
        );
    }
    const all = [...args, ...(trailing as Arg[])];
    return exprBuilder(EX => EX.call(name, all.map(arg => arg(EX))));
}

/** @internal Like `invoke`, for `name(DISTINCT args...)`. */
export function invokeDistinct(name: string, args: readonly Arg[]): EBuilder {
    return exprBuilder(EX => EX.callDistinct(name, args.map(arg => arg(EX))));
}
