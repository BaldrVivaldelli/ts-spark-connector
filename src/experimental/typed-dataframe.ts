/**
 * EXPERIMENTAL — schema-typed DataFrame prototype.
 *
 * Goal: validate the ergonomics of carrying the column schema in the type, so
 * that `select("missing_col")` and `filter(c => c.age.gt("x"))` fail at COMPILE
 * time instead of at runtime on the Spark server.
 *
 * Design principle being tested: this is a *pure type layer*. It does not add a
 * new runtime engine — it reuses the existing proto interpreters
 * (`ProtoDFAlg` / `ProtoExprAlg`), so the plan it produces is identical to what
 * the untyped `ReadChainedDataFrame` would produce. Only the surface types
 * change.
 *
 * Scope of the prototype: relation + select + filter + withColumn +
 * withColumnRenamed + join, plus execution (collect/show/explain), end to end.
 * This is intentionally NOT wired into the public API yet.
 */

import { ProtoDFAlg, ProtoExprAlg, ProtoExec } from "../engine/compilerRead";
import { DeclaredSchema, InferSchema, SchemaDef } from "./schema";
import { JoinTypeInput, ExplainModeInput } from "../engine/sparkConnectEnums";
import { SortOrder, SortDirection, NullsOrder } from "../types";
import { SparkSession } from "../client/session";
import { printArrowResults } from "../utils/arrowPrinter";
import { arrowBuffersFromResponses, rowsFromArrowBuffers } from "./arrow-rows";
import { Aggregation, AggFactory, makeAggFactory } from "./aggregations";

/** TS types we allow as column value types in the prototype. */
export type ScalarType = string | number | boolean;
/** A column value type, possibly nullable. */
export type ColumnType = ScalarType | null;

/** A schema maps column name -> its (TS) value type. */
export type Schema = Record<string, ColumnType>;

/** Strips `| null` to get the underlying scalar type of a column. */
type NonNull<T> = T extends null ? never : T;

// The proto interpreters are typed over `any` representations internally, so we
// treat the produced nodes as opaque here.
type ProtoExpr = unknown;
type ProtoRel = unknown;

/** A boolean expression usable in `filter`, composable with and/or. */
export class Condition {
    /** @internal */ constructor(readonly expr: ProtoExpr) {}

    and(other: Condition): Condition {
        return new Condition(ProtoExprAlg.logical("AND", this.expr, other.expr));
    }

    or(other: Condition): Condition {
        return new Condition(ProtoExprAlg.logical("OR", this.expr, other.expr));
    }
}

/**
 * A typed reference to a column of value type `T` (which may include `null`).
 * Comparison helpers accept the column's non-null scalar type (or another column
 * of a compatible type), so `c.age.gt("x")` is rejected when `age` is numeric.
 */
export class TypedColumn<T extends ColumnType> {
    /** @internal */ constructor(readonly expr: ProtoExpr) {}

    private cmp(op: string, other: NonNull<T> | TypedColumn<ColumnType>): Condition {
        const rhs = other instanceof TypedColumn ? other.expr : ProtoExprAlg.lit(other as ScalarType);
        return new Condition(ProtoExprAlg.bin(op, this.expr, rhs));
    }

    eq(other: NonNull<T> | TypedColumn<T>): Condition { return this.cmp("=", other); }
    gt(other: NonNull<T> | TypedColumn<T>): Condition { return this.cmp(">", other); }
    gte(other: NonNull<T> | TypedColumn<T>): Condition { return this.cmp(">=", other); }
    lt(other: NonNull<T> | TypedColumn<T>): Condition { return this.cmp("<", other); }
    lte(other: NonNull<T> | TypedColumn<T>): Condition { return this.cmp("<=", other); }

    /** Null checks — available on every column, nullable or not. */
    isNull(): Condition {
        return new Condition(ProtoExprAlg.isNull(this.expr));
    }
    isNotNull(): Condition {
        return new Condition(ProtoExprAlg.isNotNull(this.expr));
    }

    /**
     * Replaces null with a fallback, producing a NON-nullable column. The
     * fallback must be the column's non-null scalar type (or another column).
     */
    coalesce(fallback: NonNull<T> | TypedColumn<T>): TypedColumn<NonNull<T>> {
        const fb = fallback instanceof TypedColumn ? fallback.expr : ProtoExprAlg.lit(fallback as ScalarType);
        return new TypedColumn<NonNull<T>>(ProtoExprAlg.coalesce([this.expr, fb]));
    }

    /** Aliases the column (used inside `select`/`withColumn` expression builders). */
    as(name: string): TypedColumn<T> {
        return new TypedColumn<T>(ProtoExprAlg.alias(this.expr, name));
    }

    /** Ascending sort key for this column (optionally choosing null ordering). */
    asc(nulls?: NullsOrder): SortKey {
        return new SortKey(this.expr, "asc", nulls);
    }

    /** Descending sort key for this column (optionally choosing null ordering). */
    desc(nulls?: NullsOrder): SortKey {
        return new SortKey(this.expr, "desc", nulls);
    }
}

/**
 * A numeric typed column adds arithmetic. The result of arithmetic between two
 * possibly-null operands is itself nullable (null propagates in Spark).
 */
export class NumericColumn<T extends number | null> extends TypedColumn<T> {
    private arith(op: string, other: number | TypedColumn<number | null>): NumericColumn<number | null> {
        const rhs = other instanceof TypedColumn ? other.expr : ProtoExprAlg.lit(other);
        return new NumericColumn<number | null>(ProtoExprAlg.bin(op, this.expr, rhs));
    }

    plus(other: number | TypedColumn<number | null>): NumericColumn<number | null> { return this.arith("+", other); }
    minus(other: number | TypedColumn<number | null>): NumericColumn<number | null> { return this.arith("-", other); }
    times(other: number | TypedColumn<number | null>): NumericColumn<number | null> { return this.arith("*", other); }
    div(other: number | TypedColumn<number | null>): NumericColumn<number | null> { return this.arith("/", other); }
}

/** A sort key produced from a typed column, used by `orderBy`. */
export class SortKey {
    /** @internal */ constructor(
        readonly expr: ProtoExpr,
        readonly direction: SortDirection,
        readonly nulls?: NullsOrder
    ) {}
}

/** Accessor object exposing one typed column per schema field. Numeric columns
 * (including nullable numerics) expose arithmetic via `NumericColumn`. */
export type Columns<S extends Schema> = {
    readonly [K in keyof S]: [NonNull<S[K]>] extends [number]
        ? NumericColumn<S[K] & (number | null)>
        : TypedColumn<S[K]>;
};

/** Result of selecting a subset of keys `K` from schema `S`. */
type Selected<S extends Schema, K extends keyof S> = { [P in K]: S[P] };

/** Flattens an intersection of object types into a single readable object type. */
type Prettify<T> = { [K in keyof T]: T[K] } & {};

/**
 * Schema after renaming column `From` to `To`, preserving its value type. The
 * old key is dropped and the new key carries the same type. Other columns are
 * unchanged.
 */
type Renamed<S extends Schema, From extends keyof S, To extends string> =
    Prettify<Omit<S, From> & { [P in To]: S[From] }>;

/**
 * Schema produced by a join. In the clean case (disjoint column names) this is
 * the union of both sides' columns. When a name exists on both sides the right
 * side wins in the type (the same ambiguity Spark resolves at runtime); callers
 * should rename colliding columns before joining. See JOIN-LIMITATIONS below.
 */
type Joined<L extends Schema, R extends Schema> = Prettify<Omit<L, keyof R> & R>;

/** Schema after dropping the columns named in `K`. */
type Dropped<S extends Schema, K extends keyof S> = Prettify<Omit<S, K>>;

/** Merges the `__out` shapes of an aggregation tuple into one object type. */
type AggOutputs<A extends readonly Aggregation<string, ColumnType>[]> =
    Prettify<UnionToIntersection<A[number]["__out"]>>;

/** Standard union-to-intersection helper. */
type UnionToIntersection<U> =
    (U extends unknown ? (k: U) => void : never) extends (k: infer I) => void ? I : never;

/** Result schema of groupBy(...keys).agg(...aggs): the keys plus the agg outputs. */
type Aggregated<
    S extends Schema,
    K extends keyof S,
    A extends readonly Aggregation<string, ColumnType>[]
> = Prettify<Pick<S, K> & AggOutputs<A>>;

/**
 * A typed column accessor for one named side of a join. Same shape as
 * `Columns<S>` but kept distinct for readability in join predicates.
 */
type SideColumns<S extends Schema> = Columns<S>;

/**
 * A DataFrame whose column schema `S` is tracked in the type. Every
 * transformation returns a new `TypedDataFrame` with the schema updated
 * accordingly.
 */
export class TypedDataFrame<S extends Schema> {
    /** @internal */ private constructor(
        private readonly plan: ProtoRel,
        private readonly session: SparkSession
    ) {}

    /**
     * Entry point. The caller declares the schema explicitly (a real
     * implementation would infer it from the read options / catalog).
     */
    static read<S extends Schema>(
        session: SparkSession,
        format: string,
        path: string,
        options?: Record<string, string>
    ): TypedDataFrame<S> {
        return new TypedDataFrame<S>(ProtoDFAlg.relation(format, path, options), session);
    }

    /**
     * Entry point (Option B): the schema is declared once and used for BOTH the
     * compile-time type AND the runtime `data_source.schema` sent to Spark, so
     * Spark applies exactly this schema instead of inferring it. No drift.
     */
    static readWith<D extends SchemaDef>(
        session: SparkSession,
        declared: DeclaredSchema<D>,
        format: string,
        path: string,
        options?: Record<string, string>
    ): TypedDataFrame<InferSchema<D>> {
        const base = ProtoDFAlg.relation(format, path, options) as {
            read: { data_source: Record<string, unknown> };
        };
        // Inject the DDL schema into the proto so the server uses it verbatim.
        const plan = {
            read: {
                ...base.read,
                data_source: {
                    ...base.read.data_source,
                    schema: declared.toDDL(),
                },
            },
        };
        return new TypedDataFrame<InferSchema<D>>(plan, session);
    }

    /** Projects a subset of existing columns. Unknown names fail to compile. */
    select<K extends keyof S & string>(...cols: K[]): TypedDataFrame<Selected<S, K>> {
        const exprs = cols.map(name => ProtoExprAlg.col(name));
        return new TypedDataFrame<Selected<S, K>>(ProtoDFAlg.select(this.plan, exprs), this.session);
    }

    /** Filters rows. The predicate gets a typed column accessor. */
    filter(predicate: (c: Columns<S>) => Condition): TypedDataFrame<S> {
        const condition = predicate(makeColumns<S>());
        return new TypedDataFrame<S>(ProtoDFAlg.filter(this.plan, condition.expr), this.session);
    }

    /** Adds (or replaces) a column, extending the schema with `Name: T`. */
    withColumn<Name extends string, T extends ColumnType>(
        name: Name,
        build: (c: Columns<S>) => TypedColumn<T>
    ): TypedDataFrame<S & { [P in Name]: T }> {
        const column = build(makeColumns<S>());
        return new TypedDataFrame<S & { [P in Name]: T }>(
            ProtoDFAlg.withColumn(this.plan, name, column.expr),
            this.session
        );
    }

    /**
     * Renames a column. The source name must exist in the schema (unknown names
     * fail to compile); the result schema drops the old key and adds the new one
     * with the same value type.
     */
    withColumnRenamed<From extends keyof S & string, To extends string>(
        from: From,
        to: To
    ): TypedDataFrame<Renamed<S, From, To>> {
        return new TypedDataFrame<Renamed<S, From, To>>(
            ProtoDFAlg.withColumnRenamed(this.plan, from, to),
            this.session
        );
    }

    /**
     * Joins with another typed DataFrame. The condition receives two typed
     * accessors — `l` for this frame's columns and `r` for the right frame's —
     * which removes the ambiguity of which side a column refers to.
     *
     * The resulting schema is the merge of both sides. Disjoint names is the
     * clean case; on a name collision the right side wins in the type and the
     * column is ambiguous at runtime (rename before joining). See
     * JOIN-LIMITATIONS in the experimental README.
     */
    join<R extends Schema>(
        right: TypedDataFrame<R>,
        on: (l: SideColumns<S>, r: SideColumns<R>) => Condition,
        joinType: JoinTypeInput = "INNER"
    ): TypedDataFrame<Joined<S, R>> {
        const condition = on(makeColumns<S>(), makeColumns<R>());
        return new TypedDataFrame<Joined<S, R>>(
            ProtoDFAlg.join(this.plan, right.plan, condition.expr, joinType),
            this.session
        );
    }

    /**
     * Orders the rows by one or more columns. Each key is built from a typed
     * column accessor (e.g. `c => c.age.desc()` or just `c => c.name.asc()`).
     * Schema is unchanged.
     */
    orderBy(...keys: Array<(c: Columns<S>) => SortKey | TypedColumn<ColumnType>>): TypedDataFrame<S> {
        const cols = makeColumns<S>();
        const orders: SortOrder<ProtoExpr>[] = keys.map(k => {
            const key = k(cols);
            return key instanceof SortKey
                ? { expr: key.expr, direction: key.direction, nulls: key.nulls }
                : { expr: key.expr, direction: "asc" as SortDirection };
        });
        return new TypedDataFrame<S>(ProtoDFAlg.orderBy(this.plan, orders), this.session);
    }

    /** Limits the number of rows. Schema is unchanged. */
    limit(n: number): TypedDataFrame<S> {
        return new TypedDataFrame<S>(ProtoDFAlg.limit(this.plan, n), this.session);
    }

    /** Removes duplicate rows. Schema is unchanged. */
    distinct(): TypedDataFrame<S> {
        return new TypedDataFrame<S>(ProtoDFAlg.distinct(this.plan), this.session);
    }

    /** Drops the named columns. Unknown names fail to compile. */
    drop<K extends keyof S & string>(...cols: K[]): TypedDataFrame<Dropped<S, K>> {
        return new TypedDataFrame<Dropped<S, K>>(ProtoDFAlg.drop(this.plan, cols), this.session);
    }

    /**
     * Set union with another frame of the SAME schema. Requiring identical
     * schemas keeps the result well-typed and matches positional UNION ALL.
     */
    union(other: TypedDataFrame<S>): TypedDataFrame<S> {
        return new TypedDataFrame<S>(ProtoDFAlg.union(this.plan, other.plan), this.session);
    }

    /** Set intersection with another frame of the same schema. */
    intersect(other: TypedDataFrame<S>): TypedDataFrame<S> {
        return new TypedDataFrame<S>(ProtoDFAlg.intersect(this.plan, other.plan), this.session);
    }

    /** Set difference (this minus other) for frames of the same schema. */
    except(other: TypedDataFrame<S>): TypedDataFrame<S> {
        return new TypedDataFrame<S>(ProtoDFAlg.except(this.plan, other.plan), this.session);
    }

    /**
     * Groups by one or more columns. The returned `GroupedData` carries both the
     * source schema and the grouping keys, so `.agg(...)` can compute the result
     * schema as (grouping keys) + (aggregation outputs).
     */
    groupBy<K extends keyof S & string>(...keys: K[]): GroupedData<S, K> {
        const keyExprs = keys.map(name => ProtoExprAlg.col(name));
        return new GroupedData<S, K>(this.plan, this.session, keyExprs);
    }

    /**
     * Executes the plan and returns the rows, typed as the current schema `S`.
     * This is what closes the loop: declare the schema once, get typed rows back.
     */
    async collect(): Promise<S[]> {
        const responses = await ProtoExec.collect(this.plan, this.session);
        const buffers = arrowBuffersFromResponses(responses);
        return rowsFromArrowBuffers<S>(buffers);
    }

    /** Executes the plan and pretty-prints the rows to the console. */
    async show(): Promise<void> {
        const responses = await ProtoExec.collect(this.plan, this.session);
        printArrowResults(arrowBuffersFromResponses(responses));
    }

    /** Returns the Spark plan explanation for the given mode. */
    explain(mode: ExplainModeInput = "simple"): Promise<string> {
        return ProtoExec.explain(this.plan, this.session, mode);
    }

    /** Serializes the underlying Spark Connect proto plan (for inspection). */
    toProtoJSON(): string {
        return JSON.stringify(this.plan, null, 2);
    }

    /**
     * @internal Builds a typed frame from a raw plan + session. Used by
     * GroupedData to return the aggregation result; not part of the public API.
     */
    static __fromPlan<S extends Schema>(plan: ProtoRel, session: SparkSession): TypedDataFrame<S> {
        return new TypedDataFrame<S>(plan, session);
    }
}

/**
 * Intermediate result of `groupBy`. Holds the grouping keys; `.agg(...)`
 * produces a `TypedDataFrame` whose schema is the keys plus the aggregation
 * outputs. Aggregations are built via a typed factory (`a => a.count().as("n")`).
 */
export class GroupedData<S extends Schema, K extends keyof S & string> {
    /** @internal */ constructor(
        private readonly plan: ProtoRel,
        private readonly session: SparkSession,
        private readonly keyExprs: ProtoExpr[]
    ) {}

    /**
     * Applies one or more aggregations. Each is built from the typed factory and
     * named with `.as(...)`. The result schema is `Pick<S, K> & {aggregations}`.
     */
    agg<A extends readonly Aggregation<string, ColumnType>[]>(
        build: (a: AggFactory<S>) => readonly [...A]
    ): TypedDataFrame<Aggregated<S, K, A>> {
        const aggregations = build(makeAggFactory<S>(makeColumns<S>()));
        const aggMap: Record<string, ProtoExpr> = {};
        for (const agg of aggregations) {
            aggMap[agg.alias] = agg.expr;
        }
        const group = ProtoDFAlg.groupBy(this.plan, this.keyExprs);
        const plan = ProtoDFAlg.agg(group, aggMap);
        return TypedDataFrame.__fromPlan<Aggregated<S, K, A>>(plan, this.session);
    }
}

/**
 * Builds the typed column accessor. At runtime it's a Proxy that lazily
 * produces a `NumericColumn` for any accessed name; the compiler restricts
 * access to the declared schema keys via the `Columns<S>` type, and narrows
 * each to `NumericColumn` or `TypedColumn` per the field's type.
 *
 * We always instantiate `NumericColumn` at runtime because it is a superset of
 * `TypedColumn` (all comparison/null methods plus arithmetic). For non-numeric
 * columns the arithmetic methods are simply unreachable through the types.
 */
function makeColumns<S extends Schema>(): Columns<S> {
    return new Proxy(
        {},
        {
            get: (_target, prop) => new NumericColumn(ProtoExprAlg.col(String(prop))),
        }
    ) as Columns<S>;
}
