// src/engine/column.ts  — Tagless Final builders (sin AST público)



import {ExprAlg, LiteralValue} from "../algebra/read";
import {FrameBoundary, NullsOrder, SortDirection, WindowSpec} from "../types";

// `FrameBoundary` used to be declared twice (here and in ../types/window). The
// window frame boundary that `WindowSpec` uses is the single declaration now.
export type { FrameBoundary };

/**
 * Right-hand operand of a column method: another expression, or a JS value
 * that is sent as a literal (a bare string is a literal here, as in PySpark).
 * @public
 */
export type ColumnOperand = EBuilder | LiteralValue;

/** @public */
export type EBuilder = {
    build<E>(EX: ExprAlg<E>): E;

    // helpers encadenables tipo PySpark
    alias(name: string): EBuilder;
    /** Same as `alias`, matching PySpark's `Column.name`. */
    name(alias: string): EBuilder;
    eq(x: EBuilder | string | number | boolean): EBuilder;
    /** Inequality (`!=`). Comparing against `null` yields `null`, as in SQL. */
    neq(x: ColumnOperand): EBuilder;
    /** Null-safe equality (`<=>`): `null <=> null` is `true`. */
    eqNullSafe(x: ColumnOperand): EBuilder;
    gt(x: EBuilder | number): EBuilder;
    gte(x: EBuilder | number): EBuilder;
    lt(x: EBuilder | number): EBuilder;
    lte(x: EBuilder | number): EBuilder;
    and(x: EBuilder): EBuilder;
    or(x: EBuilder): EBuilder;
    /** Logical negation (`NOT`). */
    not(): EBuilder;

    /** Addition (`+`). */
    plus(x: ColumnOperand): EBuilder;
    /** Subtraction (`-`). */
    minus(x: ColumnOperand): EBuilder;
    /** Multiplication (`*`). */
    multiply(x: ColumnOperand): EBuilder;
    /** Division (`/`); always fractional in Spark. */
    divide(x: ColumnOperand): EBuilder;
    /** Remainder (`%`). */
    mod(x: ColumnOperand): EBuilder;
    /** Bitwise AND (`&`). */
    bitwiseAND(x: ColumnOperand): EBuilder;
    /** Bitwise OR (`|`). */
    bitwiseOR(x: ColumnOperand): EBuilder;
    /** Bitwise XOR (`^`). */
    bitwiseXOR(x: ColumnOperand): EBuilder;

    /** `true` when the value equals any of `values` (SQL `IN`). */
    isin(...values: Array<ColumnOperand | readonly ColumnOperand[]>): EBuilder;
    /** Inclusive range check: `lower <= this AND this <= upper`. */
    between(lower: ColumnOperand, upper: ColumnOperand): EBuilder;
    /** SQL `LIKE` pattern match (`_` and `%` wildcards). */
    like(pattern: string | EBuilder): EBuilder;
    /** Case-insensitive `LIKE`. */
    ilike(pattern: string | EBuilder): EBuilder;
    /** Java regular-expression match. */
    rlike(pattern: string | EBuilder): EBuilder;
    /** String prefix test. */
    startsWith(prefix: string | EBuilder): EBuilder;
    /** String suffix test. */
    endsWith(suffix: string | EBuilder): EBuilder;
    /** Substring test. */
    contains(other: string | EBuilder): EBuilder;
    /** Substring starting at the 1-based `start`, `length` characters long. */
    substr(start: number | EBuilder, length: number | EBuilder): EBuilder;
    /** `true` when the value is NaN. */
    isNaN(): EBuilder;

    /** Casts to a Spark DDL type such as `"int"`, `"decimal(10,2)"` or `"array<string>"`. */
    cast(type: string): EBuilder;
    /** Like `cast`, but yields `null` instead of failing on invalid input. */
    tryCast(type: string): EBuilder;

    /**
     * `this[key]`, following PySpark's `Column.getItem`: a 0-based array
     * index, a map key, or a struct field name.
     */
    getItem(key: string | number | EBuilder): EBuilder;
    /** A struct field, following PySpark's `Column.getField`. */
    getField(name: string): EBuilder;
    /** Adds or replaces a field of this struct. */
    withField(name: string, value: EBuilder): EBuilder;
    /** Drops fields of this struct (missing fields are an error in Spark). */
    dropFields(...names: string[]): EBuilder;

    // order keys
    asc(nulls?: NullsOrder): SortKeyBuilder;
    desc(nulls?: NullsOrder): SortKeyBuilder;
    ascNullsFirst(): SortKeyBuilder;
    ascNullsLast():  SortKeyBuilder;
    descNullsFirst():SortKeyBuilder;
    descNullsLast(): SortKeyBuilder;

    isNull(): EBuilder;
    isNotNull(): EBuilder;
    /**
     * Evaluates this window or aggregate function over a window. Without a
     * specification the window is the whole result (Spark's empty `OVER ()`).
     */
    over(spec?: (EX: ExprAlg<any>) => WindowSpec<any>): EBuilder

    from_json(schema: string): EBuilder;
    to_json(): EBuilder;
};

const isBuilder = (x: unknown): x is EBuilder =>
    typeof x === "object" && x !== null && "build" in x;

// Coerces a standalone argument: a bare string is treated as a COLUMN reference.
// Used by top-level helpers like call/coalesce/explode/isNull.
const toE =
    (x: EBuilder | string | number | boolean) =>
        <E>(EX: ExprAlg<E>) =>
            isBuilder(x)
                ? x.build(EX)
                : typeof x === "string"
                    ? EX.col(x)
                    : EX.lit(x);

// Coerces the right-hand side of a column method: a bare string is treated as a
// LITERAL (PySpark semantics, e.g. col("a").eq("x") compares against the value "x").
const toOperand = (x: ColumnOperand) =>
    <E>(EX: ExprAlg<E>) => isBuilder(x) ? x.build(EX) : EX.lit(x);

/** @public */
export type SortKeyBuilder = <E>(EX: ExprAlg<E>) => { expr: E; direction: SortDirection; nulls?: NullsOrder };

const EMPTY_WINDOW = <E>(): WindowSpec<E> => ({ partitionBy: [], orderBy: [] });

const EB = (f: <E>(EX: ExprAlg<E>) => E): EBuilder => {
    const binary = (op: string) => (x: ColumnOperand) =>
        EB(EX => EX.bin(op, f(EX), toOperand(x)(EX)));
    const fn = (name: string) => (x: ColumnOperand) =>
        EB(EX => EX.call(name, [f(EX), toOperand(x)(EX)]));

    return {
        build: f,

        alias: (name) => EB(EX => EX.alias(f(EX), name)),
        name: (alias) => EB(EX => EX.alias(f(EX), alias)),

        eq: binary("="),
        // `!=` is SQL syntax, not a function Spark Connect can resolve.
        neq: (x) => EB(EX => EX.call("not", [EX.bin("=", f(EX), toOperand(x)(EX))])),
        eqNullSafe: binary("<=>"),
        gt: binary(">"),
        gte: binary(">="),
        lt: binary("<"),
        lte: binary("<="),

        and: (x) => EB(EX => EX.logical("AND", f(EX), x.build(EX))),
        or: (x) => EB(EX => EX.logical("OR", f(EX), x.build(EX))),
        not: () => EB(EX => EX.call("not", [f(EX)])),

        plus: binary("+"),
        minus: binary("-"),
        multiply: binary("*"),
        divide: binary("/"),
        mod: binary("%"),
        bitwiseAND: binary("&"),
        bitwiseOR: binary("|"),
        bitwiseXOR: binary("^"),

        isin: (...values) => {
            const flat = values.flatMap(value => Array.isArray(value) ? value : [value as ColumnOperand]);
            return EB(EX => EX.call("in", [f(EX), ...flat.map(value => toOperand(value)(EX))]));
        },
        between: (lower, upper) => EB(EX => EX.logical(
            "AND",
            EX.bin(">=", f(EX), toOperand(lower)(EX)),
            EX.bin("<=", f(EX), toOperand(upper)(EX)),
        )),
        like: fn("like"),
        ilike: fn("ilike"),
        rlike: fn("rlike"),
        startsWith: fn("startswith"),
        endsWith: fn("endswith"),
        contains: fn("contains"),
        substr: (start, length) =>
            EB(EX => EX.call("substr", [f(EX), toOperand(start)(EX), toOperand(length)(EX)])),
        isNaN: () => EB(EX => EX.call("isnan", [f(EX)])),

        cast: (type) => EB(EX => EX.cast(f(EX), type)),
        tryCast: (type) => EB(EX => EX.cast(f(EX), type, "try")),

        getItem: (key) => EB(EX => EX.extractValue(f(EX), toOperand(key)(EX))),
        getField: (name) => EB(EX => EX.extractValue(f(EX), EX.lit(name))),
        withField: (name, value) => EB(EX => EX.updateFields(f(EX), name, value.build(EX))),
        dropFields: (...names) => EB(EX =>
            names.reduce((struct, name) => EX.updateFields(struct, name), f(EX))
        ),

        asc: (nulls) => (EX) => ({expr: f(EX), direction: "asc" as const, nulls}),
        desc: (nulls) => (EX) => ({expr: f(EX), direction: "desc" as const, nulls}),
        ascNullsFirst: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "asc", nulls: "nullsFirst"}),
        ascNullsLast: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "asc", nulls: "nullsLast"}),
        descNullsFirst: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "desc", nulls: "nullsFirst"}),
        descNullsLast: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "desc", nulls: "nullsLast"}),
        isNull:    () => EB(EX => EX.isNull(f(EX))),
        isNotNull: () => EB(EX => EX.isNotNull(f(EX))),
        over: (specB) => EB(EX => EX.win(f(EX), specB ? specB(EX) : EMPTY_WINDOW())),
        from_json: (schema: string) => EB(EX => EX.from_json(f(EX), schema)),
        to_json: () => EB(EX => EX.to_json(f(EX))),
    };
};

/**
 * Wraps an algebra thunk as a chainable column expression. The function
 * library (`functions`) builds every expression through it.
 * @internal
 */
export const exprBuilder = (f: <E>(EX: ExprAlg<E>) => E): EBuilder => EB(f);

/** @public */
export function call(name: string, args: (EBuilder | string | number | boolean)[]): EBuilder {
    return EB(EX => EX.call(name, args.map(arg => toE(arg)(EX))));
}

/**
 * A Spark SQL expression, parsed and resolved by the server: `expr("amount * 2")`,
 * `expr("upper(name) AS shout")`.
 * @public
 */
export function expr(sql: string): EBuilder {
    if (typeof sql !== "string" || !sql.trim()) {
        throw new TypeError("expr() requires a non-empty SQL expression.");
    }
    return EB(EX => EX.exprString(sql));
}

/** @public */
export const map_keys = (mapCol: EBuilder): EBuilder =>
    call("map_keys", [mapCol]);

/** @public */
export const map_values = (mapCol: EBuilder): EBuilder =>
    call("map_values", [mapCol]);

/** @public */
export const elementAt = (mapCol: EBuilder, key: EBuilder | string | number): EBuilder =>
    call("element_at", [mapCol, key]);
/** @public */
export function split(input: EBuilder, delimiter: string | EBuilder): EBuilder {
    return call("split", [input, delimiter]);
}

/** @public */
export function getField(structCol: EBuilder, field: string): EBuilder {
    return EB(EX => EX.getField(structCol.build(EX), field));
}

/** @public */
export function to_json(expr: EBuilder): EBuilder {
    return EB(EX => EX.to_json(expr.build(EX)));
}

/** @public */
export function from_json(jsonExpr: EBuilder, schema: string): EBuilder {
    return EB(EX => EX.from_json(jsonExpr.build(EX), schema));
}
/** @public */
export function struct(...fields: EBuilder[]): EBuilder {
    return call("struct", fields);
}

/** @public */
export function getItem(collection: EBuilder, key: EBuilder | string | number): EBuilder {
    return EB(EX =>
        EX.getItem(
            collection.build(EX),
            isBuilder(key)
                ? key.build(EX)
                : EX.lit(key)
        )
    );
}

/** @public */
export function explode(col: EBuilder | string): EBuilder {
    return EB(EX => EX.explode(toE(col)(EX)));
}

/** @public */
export function posexplode(col: EBuilder | string): EBuilder {
    return EB(EX => EX.posexplode(toE(col)(EX)));
}

/** @public */
export function isNull(x: EBuilder | string): EBuilder {
    return EB(EX => EX.isNull(toE(x)(EX)));
}

/** @public */
export function isNotNull(x: EBuilder | string): EBuilder {
    return EB(EX => EX.isNotNull(toE(x)(EX)));
}
/** @public */
export function coalesce(...xs: Array<EBuilder | string | number | boolean>): EBuilder {
    return EB(EX => EX.coalesce(xs.map(x => toE(x)(EX))));
}

/** @public */
export const col = (name: string): EBuilder => EB(EX => EX.col(name));
/** @public */
export const lit = (v: LiteralValue): EBuilder => EB(EX => EX.lit(v));

/**
 * Untyped `when(...).when(...).otherwise(...)` chain returned by `when()`.
 * Exported as `ExpressionCaseChain`; it is distinct from `TypedCaseChain`, the
 * schema-aware chain produced by `typedWhen()`.
 * @public
 */
export type CaseChain = {
    when(cond: EBuilder, val: EBuilder | string | number | boolean): CaseChain;
    otherwise(val: EBuilder | string | number | boolean): EBuilder;
};

/** @public */
export function when(cond: EBuilder, val: EBuilder | string | number | boolean): CaseChain {
    // acumulamos ramas en forma inmutable
    const branches: Array<{ when: EBuilder; then: EBuilder }> = [
        { when: cond, then: asE(val) }
    ];
    const make = (br: typeof branches): CaseChain => ({
        when(nextCond, nextVal) {
            return make([...br, { when: nextCond, then: asE(nextVal) }]);
        },
        otherwise(elseVal) {
            const elseB = asE(elseVal);
            return EB(EX => EX.caseWhen(
                br.map(b => ({ when: b.when.build(EX), then: b.then.build(EX) })),
                elseB.build(EX)
            ));
        }
    });
    return make(branches);
}

/**
 * A window frame boundary: a `FrameBoundary`, or a PySpark-style offset where
 * negative numbers precede the current row, `0` is the current row, positive
 * numbers follow it and `-Infinity` / `Infinity` are unbounded.
 * @public
 */
export type FrameBoundaryInput = FrameBoundary | number;

/** A column to partition by: a name or an expression. @public */
export type WindowPartitionInput = string | EBuilder;
/** A column to order a window by: a name, an expression or a sort key. @public */
export type WindowOrderInput = string | EBuilder | SortKeyBuilder;

/**
 * A window specification for `EBuilder.over`. It is itself the spec function
 * `over` expects, and refines into a new specification with each method, so
 * `Window.partitionBy("k")`, `Window.orderBy("t")` and
 * `Window.partitionBy("k").orderBy("t").rowsBetween(-1, 0)` are all usable.
 * @public
 */
export type WindowSpecBuilder = (<E>(EX: ExprAlg<E>) => WindowSpec<E>) & {
    partitionBy(...keys: WindowPartitionInput[]): WindowSpecBuilder;
    orderBy(...ords: WindowOrderInput[]): WindowSpecBuilder;
    /** A frame counted in rows relative to the current row. */
    rowsBetween(start: FrameBoundaryInput, end: FrameBoundaryInput): WindowSpecBuilder;
    /** A frame over the values of the (single, numeric) order-by expression. */
    rangeBetween(start: FrameBoundaryInput, end: FrameBoundaryInput): WindowSpecBuilder;
};

type WindowParts = {
    partitionBy: readonly WindowPartitionInput[];
    orderBy: readonly WindowOrderInput[];
    frame?: { type: "rows" | "range"; start: FrameBoundary; end: FrameBoundary };
};

function toFrameBoundary(input: FrameBoundaryInput): FrameBoundary {
    if (typeof input !== "number") return input;
    if (Number.isNaN(input)) {
        throw new RangeError("Window frame boundaries must be numbers, not NaN.");
    }
    if (input === Number.NEGATIVE_INFINITY) return { type: "UnboundedPreceding" };
    if (input === Number.POSITIVE_INFINITY) return { type: "UnboundedFollowing" };
    if (input === 0) return { type: "CurrentRow" };
    return input < 0
        ? { type: "ValuePreceding", value: -input }
        : { type: "ValueFollowing", value: input };
}

function windowSpec(parts: WindowParts): WindowSpecBuilder {
    const build = <E>(EX: ExprAlg<E>): WindowSpec<E> => ({
        partitionBy: parts.partitionBy.map(k => typeof k === "string" ? EX.col(k) : k.build(EX)),
        orderBy: parts.orderBy.map(o => {
            if (typeof o === "string") return { input: EX.col(o), direction: "asc" as const };
            if (typeof o === "function") {
                const so = o(EX);
                return { input: so.expr, direction: so.direction, nulls: so.nulls };
            }
            return { input: o.build(EX), direction: "asc" as const };
        }),
        ...(parts.frame ? { frame: parts.frame } : {}),
    });
    const frame = (type: "rows" | "range") => (start: FrameBoundaryInput, end: FrameBoundaryInput) =>
        windowSpec({ ...parts, frame: { type, start: toFrameBoundary(start), end: toFrameBoundary(end) } });
    return Object.assign(build, {
        partitionBy: (...keys: WindowPartitionInput[]) => windowSpec({ ...parts, partitionBy: keys }),
        orderBy: (...ords: WindowOrderInput[]) => windowSpec({ ...parts, orderBy: ords }),
        rowsBetween: frame("rows"),
        rangeBetween: frame("range"),
    });
}

const EMPTY_WINDOW_SPEC = windowSpec({ partitionBy: [], orderBy: [] });

/** @public */
export const Window = {
    partitionBy: (...keys: WindowPartitionInput[]): WindowSpecBuilder => EMPTY_WINDOW_SPEC.partitionBy(...keys),
    orderBy: (...ords: WindowOrderInput[]): WindowSpecBuilder => EMPTY_WINDOW_SPEC.orderBy(...ords),
    rowsBetween: (start: FrameBoundaryInput, end: FrameBoundaryInput): WindowSpecBuilder =>
        EMPTY_WINDOW_SPEC.rowsBetween(start, end),
    rangeBetween: (start: FrameBoundaryInput, end: FrameBoundaryInput): WindowSpecBuilder =>
        EMPTY_WINDOW_SPEC.rangeBetween(start, end),
    /** Frame start at the first row of the partition. */
    unboundedPreceding: { type: "UnboundedPreceding" } as FrameBoundary,
    /** Frame end at the last row of the partition. */
    unboundedFollowing: { type: "UnboundedFollowing" } as FrameBoundary,
    /** The current row. */
    currentRow: { type: "CurrentRow" } as FrameBoundary,
};


// util
const asE = (x: EBuilder | string | number | boolean): EBuilder =>
    isBuilder(x) ? x : lit(x);
