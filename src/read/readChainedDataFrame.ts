import { SparkDFAlg, SparkExprAlg } from "./readDataFrameInterpreter";
import { LogicalPlan } from "../engine/logicalPlan";
import {
    ProtoDFAlg,
    ProtoExec,
    ProtoExprAlg,
    applyPendingAnalyzeActions,
} from "../engine/compilerRead";
import { SparkSession } from "../client/session";
import {
    DEFAULT_JOIN_TYPE,
    ExplainModeInput,
    JoinHintName,
    JoinTypeInput,
} from "../engine/sparkConnectEnums";
import { printArrowResults } from "../utils/arrowPrinter";
import {DataFrameWriterTF} from "../write/dataFrameWriterTF";
import { toJSON, toMermaid } from "../trace/traceSerializers";
import { TraceDFAlg, TraceExprAlg } from "../trace/trace";
import { NullsOrder, SortOrder } from "../types";
import {DFAlg, DFProgram, EventTimeWatermarkCap, ExprAlg, LiteralValue, StreamingMark, StreamingReadCap} from "../algebra/read";
import {CacheCap, HintCap, RepartitionCap, SamplingCap, SqlCap} from "../algebra/read/batch-capabilities";
import {BatchWProgram, StreamWProgram, WBatch, WStream} from "../algebra/write";
import {BatchWriterAlg, StreamWriterAlg} from "../algebra/write/dataframe";
import {
    ColumnType,
    Schema,
    SchemaShape,
    UnknownSchema,
    UsableColumnKey,
} from "../schema/schema-model";
import type { FieldSpec, SchemaDef } from "../schema/schema";
import {
    Aggregated,
    Dropped,
    JoinedFor,
    Renamed,
    Selected,
    WithColumn,
} from "../schema/schema-transforms";
import { arrowBuffersFromResponses, rowsFromArrowBuffers } from "../typed/arrow-rows";
import {
    Columns,
    Condition,
    SortKey,
    TypedColumn,
    makeColumns,
} from "../typed/typed-column";
import {
    AggFactory,
    Aggregation as TypedAggregation,
    makeAggFactory,
} from "../typed/aggregations";

export type EBuilder = { build<E>(EX: ExprAlg<E>): E };
export type SortKeyInput =
    | string
    | EBuilder
    | ((EX: ExprAlg<any>) => SortOrder<any>);

type ColumnName<S> = [S] extends [UnknownSchema]
    ? string
    : S extends SchemaShape
      ? UsableColumnKey<S>
      : string;

type SelectedSchema<S, K extends PropertyKey> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? Selected<S, Extract<K, keyof S>>
      : UnknownSchema;

type DroppedSchema<S, K extends PropertyKey> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? Dropped<S, Extract<K, keyof S>>
      : UnknownSchema;

type RenamedSchema<S, From extends PropertyKey, To extends string> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? Renamed<S, Extract<From, keyof S>, To>
      : UnknownSchema;

type AddedColumnSchema<S, Name extends string, T extends ColumnType> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? WithColumn<S, Name, T>
      : UnknownSchema;

type RenamedColumnsSchema<S, M extends Record<string, string>> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? { [K in keyof S as K extends keyof M ? Extract<M[K], string> : K]: S[K] }
      : UnknownSchema;

type DuplicateRenameTargets<M extends Record<string, string>> = {
    [K in keyof M]: {
        [P in Exclude<keyof M, K>]: M[P] extends M[K]
            ? M[K] extends M[P]
                ? M[K]
                : never
            : never;
    }[Exclude<keyof M, K>];
}[keyof M];

type ValidRenameMap<S, M extends Record<string, string>> =
    DuplicateRenameTargets<M> extends never
        ? [S] extends [UnknownSchema]
            ? M
            : Exclude<keyof M, keyof Extract<S, SchemaShape>> extends never
              ? Extract<
                    M[keyof M],
                    Exclude<keyof Extract<S, SchemaShape>, keyof M>
                > extends never
                  ? M
                  : never
              : never
        : never;

type ValidRenameTarget<S, From extends PropertyKey, To extends string> =
    [S] extends [UnknownSchema]
        ? To
        : To extends Extract<Exclude<keyof Extract<S, SchemaShape>, From>, string>
          ? never
          : To;

type StatisticsSchema<S, K extends PropertyKey> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? { summary: string } & { [P in Extract<K, keyof S>]: string | null }
      : UnknownSchema;

type JoinSchema<S, RS extends SchemaShape, JT extends JoinTypeInput> = S extends SchemaShape
    ? JoinedFor<S, RS, JT>
    : UnknownSchema;

type TypedPredicate<S, E> = S extends SchemaShape
    ? (columns: Columns<S, E>) => Condition<E>
    : never;

type TypedColumnFactory<S, T extends ColumnType, E> = S extends SchemaShape
    ? (columns: Columns<S, E>) => TypedColumn<T, E>
    : never;

type TypedJoinPredicate<S, RS extends SchemaShape, E> = S extends SchemaShape
    ? (left: Columns<S, E>, right: Columns<RS, E>) => Condition<E>
    : never;

type TypedOrderFactory<S, E> = S extends SchemaShape
    ? (columns: Columns<S, E>) => SortKey<E> | TypedColumn<ColumnType, E>
    : never;

type TypedAggBuilder<
    S,
    E,
    A extends readonly TypedAggregation<string, ColumnType, E>[],
> = S extends SchemaShape
    ? (factory: AggFactory<S, E>) => readonly [...A] & UniqueAggregationTuple<A>
    : never;

type UniqueAggregationTuple<
    A extends readonly { readonly alias: string }[],
    Seen extends string = never,
> = A extends readonly [infer Head, ...infer Tail]
    ? Head extends { readonly alias: infer Alias extends string }
        ? Alias extends Seen
            ? never
            : Tail extends readonly { readonly alias: string }[]
              ? UniqueAggregationTuple<Tail, Seen | Alias>
              : unknown
        : unknown
    : unknown;

type AggregatedSchema<
    S,
    K extends PropertyKey,
    A extends readonly { readonly __out: object }[],
> = S extends SchemaShape
    ? Aggregated<S, Extract<K, keyof S>, A>
    : UnknownSchema;

/**
 * Row shape returned by `collect()`. A collided join column is deliberately
 * exposed as `unknown`: the marker is a compile-time diagnostic for column
 * expressions, not a value that Spark places in Arrow rows.
 */
export type RowOf<S> = [S] extends [UnknownSchema]
    ? Record<string, unknown>
    : S extends SchemaShape
      ? { [K in keyof S]: S[K] extends ColumnType ? S[K] : unknown }
      : Record<string, unknown>;

function schemaDefsEqual(left?: SchemaDef, right?: SchemaDef): boolean {
    if (!left || !right) return left === right;
    return JSON.stringify(left) === JSON.stringify(right);
}

function nullableFieldSpec(spec: FieldSpec): FieldSpec {
    if (typeof spec === "string") {
        return spec.endsWith("?") ? spec : `${spec}?` as FieldSpec;
    }
    return { ...spec, nullable: true } as FieldSpec;
}

function joinRuntimeSchema(
    left: SchemaDef | undefined,
    right: SchemaDef | undefined,
    joinType: JoinTypeInput,
): SchemaDef | undefined {
    if (!left || !right) return undefined;
    const normalized = String(joinType).toUpperCase();
    if (normalized === "LEFT_SEMI" || normalized === "LEFT_ANTI") return left;

    // A JavaScript row object cannot represent two output fields with the same
    // name. Keep the descriptor absent here; the Arrow boundary independently
    // rejects the duplicate fields with a precise error.
    if (Object.keys(left).some(name => Object.prototype.hasOwnProperty.call(right, name))) {
        return undefined;
    }

    const leftNullable = normalized === "RIGHT" || normalized === "RIGHT_OUTER" ||
        normalized === "OUTER" || normalized === "FULL" || normalized === "FULL_OUTER";
    const rightNullable = normalized === "LEFT" || normalized === "LEFT_OUTER" ||
        normalized === "OUTER" || normalized === "FULL" || normalized === "FULL_OUTER";

    return {
        ...Object.fromEntries(Object.entries(left).map(([name, spec]) => [
            name,
            leftNullable ? nullableFieldSpec(spec) : spec,
        ])),
        ...Object.fromEntries(Object.entries(right).map(([name, spec]) => [
            name,
            rightNullable ? nullableFieldSpec(spec) : spec,
        ])),
    };
}

function selectRuntimeSchema(
    schema: SchemaDef | undefined,
    names: readonly string[],
): SchemaDef | undefined {
    if (!schema) return undefined;
    const selected: Record<string, FieldSpec> = {};
    for (const name of names) {
        if (!Object.prototype.hasOwnProperty.call(schema, name) ||
            Object.prototype.hasOwnProperty.call(selected, name)) {
            return undefined;
        }
        selected[name] = schema[name];
    }
    return selected;
}

function dropRuntimeSchema(
    schema: SchemaDef | undefined,
    names: readonly string[],
): SchemaDef | undefined {
    if (!schema) return undefined;
    const removed = new Set(names);
    return Object.fromEntries(Object.entries(schema).filter(([name]) => !removed.has(name)));
}

function statisticsRuntimeSchema(
    schema: SchemaDef | undefined,
    names: readonly string[],
): SchemaDef | undefined {
    if (!selectRuntimeSchema(schema, names)) return undefined;
    return {
        summary: "string",
        ...Object.fromEntries(names.map(name => [name, "string?"] as const)),
    };
}

function assertRenameMapping(mapping: Record<string, string>): void {
    const targets = new Set<string>();
    for (const [source, target] of Object.entries(mapping)) {
        assertNonEmptyString("rename source", source);
        assertNonEmptyString("rename target", target);
        if (targets.has(target)) {
            throw new TypeError(`Multiple columns cannot be renamed to ${JSON.stringify(target)}.`);
        }
        targets.add(target);
    }
}

function assertUniqueColumnNames(label: string, names: readonly string[]): void {
    const seen = new Set<string>();
    for (const name of names) {
        if (seen.has(name)) {
            throw new TypeError(`${label} does not allow duplicate column ${JSON.stringify(name)}.`);
        }
        seen.add(name);
    }
}

function renameRuntimeSchema(
    schema: SchemaDef | undefined,
    mapping: Record<string, string>,
): SchemaDef | undefined {
    if (!schema) return undefined;
    for (const source of Object.keys(mapping)) {
        if (!Object.prototype.hasOwnProperty.call(schema, source)) {
            throw new TypeError(`Cannot rename missing column ${JSON.stringify(source)}.`);
        }
    }
    const renamed: Record<string, FieldSpec> = {};
    for (const [name, spec] of Object.entries(schema)) {
        const target = mapping[name] ?? name;
        if (Object.prototype.hasOwnProperty.call(renamed, target)) {
            throw new TypeError(
                `Rename would create duplicate column ${JSON.stringify(target)}.`
            );
        }
        renamed[target] = spec;
    }
    return renamed;
}

function resolveOrderInput<E>(input: unknown, EX: ExprAlg<E>): SortOrder<E> {
    if (typeof input === "string") {
        return { expr: EX.col(input), direction: "asc" };
    }
    if (typeof input !== "function") {
        return { expr: (input as EBuilder).build(EX), direction: "asc" };
    }

    // Typed order callbacks and legacy SortKeyBuilder are both functions. Try
    // the typed accessor first; legacy builders require ExprAlg and therefore
    // fall back to the existing invocation below.
    try {
        const typed = (input as TypedOrderFactory<Schema, E>)(makeColumns<Schema, E>());
        if (typed instanceof SortKey) return typed.toSortOrder(EX);
        if (typed instanceof TypedColumn) {
            return { expr: typed.build(EX), direction: "asc" };
        }
    } catch {
        // Legacy builder: evaluate it with the real expression algebra.
    }
    return (input as (algebra: ExprAlg<E>) => SortOrder<E>)(EX);
}

const PROTO_INT32_MAX = 2_147_483_647;

function assertInteger(name: string, value: number, minimum: number): void {
    if (!Number.isSafeInteger(value) || value < minimum || value > PROTO_INT32_MAX) {
        throw new RangeError(
            `${name} must be an integer between ${minimum} and ${PROTO_INT32_MAX}.`
        );
    }
}

function assertOptionalSeed(name: string, seed?: number): void {
    if (seed !== undefined && !Number.isSafeInteger(seed)) {
        throw new RangeError(`${name} seed must be a safe integer.`);
    }
}

function assertNonEmptyString(name: string, value: string): void {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${name} must be a non-empty string.`);
    }
}

function freshSparkSeed(): number {
    // Spark Connect accepts int64 here, but a positive int32 is exactly
    // representable by JavaScript, protobufjs and Spark on every supported
    // runtime. Generate it when the lazy DataFrame is created so repeated
    // interpretations of the same DataFrame remain immutable.
    return Math.floor(Math.random() * 2_147_483_647);
}

let nextRelationPlanId = 1;
function freshRelationPlanId(): number {
    const planId = nextRelationPlanId;
    nextRelationPlanId = nextRelationPlanId >= 2_147_483_647 ? 1 : nextRelationPlanId + 1;
    return planId;
}

export const col = (name: string): EBuilder => ({ build: EX => EX.col(name) });
export const lit = (v: LiteralValue): EBuilder => ({ build: EX => EX.lit(v) });
export const eq = (l: EBuilder, r: EBuilder | string | number | boolean): EBuilder => ({
    build: EX => EX.bin("=", l.build(EX), typeof r === "object" ? (r as EBuilder).build(EX) : EX.lit(r as any))
});
export const asc = (e: EBuilder, nulls?: NullsOrder) =>
    (EX: ExprAlg<any>): SortOrder<any> => ({ expr: e.build(EX), direction: "asc", nulls });
export const desc = (e: EBuilder, nulls?: NullsOrder) =>
    (EX: ExprAlg<any>): SortOrder<any> => ({ expr: e.build(EX), direction: "desc", nulls });

/**
 * Superficie pública encadenable, ahora parametrizada por el schema `S`.
 *
 * `S` es el **primer** parámetro de tipo y su valor por defecto es
 * `UnknownSchema` (Requirement 1.1). Cuando `S = UnknownSchema` (el caso del
 * usuario no tipado), la clase se comporta exactamente como la API actual: las
 * firmas laxas (`string`/`EBuilder`) y el mismo `DFProgram` tagless-final, sin
 * cambios de runtime ni de plan (Requirements 1.5, 2.1, 2.3, 3.3).
 *
 * Los parámetros `R/E/G/CDF/CEX` y la semántica de capabilities se conservan tal
 * cual (Requirement 1.4); `S` es **ortogonal** a ellos y puramente a nivel de
 * tipos. Las sobrecargas tipadas por operación (select/drop/filter/…) y la
 * inferencia de schema resultante se agregan en tareas posteriores (5.2–5.4,
 * 7.1); esta tarea solo establece la firma de la clase y la propagación de `S`
 * por `chain()`.
 */
export class ReadChainedDataFrame<S = UnknownSchema, R = unknown, E = unknown, G = unknown, CDF = {}, CEX = {}> {
    /** @internal Mantiene `S` invariante para que las set ops exijan el mismo schema. */
    declare private readonly __schemaInvariant: (schema: S) => S;
    private readonly prog: DFProgram<R, E, G, CDF, CEX>;

    constructor(
        p: DFProgram<R, E, G, CDF, CEX>,
        private readonly session: SparkSession,
        private readonly streaming = false,
        private readonly runtimeSchema?: SchemaDef,
    ) {
        this.prog = p;
    }

    static fromCSV<R, E, G, CDF = {}, CEX = {}>(
        path: string | string[],
        session: SparkSession,
        options?: Record<string, string>
    ) {
        const p: DFProgram<R, E, G, CDF, CEX> = (DF) => DF.relation("csv", path, options);
        return new ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>(p, session);
    }


    /**
     * Agrega un paso al programa y devuelve una nueva instancia.
     *
     * Propaga `S` sin cambios (el camino laxo preserva el schema actual; para el
     * usuario no tipado `S` permanece `UnknownSchema`). Las operaciones que
     * transforman el schema en el camino tipado lo harán mediante sobrecargas
     * dedicadas en tareas posteriores. El runtime es idéntico al actual: solo se
     * compone el `DFProgram`.
     */
    private chain<CDF2 = unknown, CEX2 = unknown>(
        step: (df: R, EX: ExprAlg<E> & CEX & CEX2, DF: DFAlg<R, E, G, CDF & CDF2>) => R
    ): ReadChainedDataFrame<S, R, E, G, CDF & CDF2, CEX & CEX2> {
        return this.chainAs<S, CDF2, CEX2>(step);
    }

    /** Compone el mismo programa cambiando únicamente el phantom schema. */
    private chainAs<NS, CDF2 = unknown, CEX2 = unknown>(
        step: (df: R, EX: ExprAlg<E> & CEX & CEX2, DF: DFAlg<R, E, G, CDF & CDF2>) => R,
        schemaTransform: (schema: SchemaDef | undefined) => SchemaDef | undefined = schema => schema,
    ): ReadChainedDataFrame<NS, R, E, G, CDF & CDF2, CEX & CEX2> {
        const next: DFProgram<R, E, G, CDF & CDF2, CEX & CEX2> = (DF, EX) =>
            step(this.prog(DF, EX), EX, DF);
        return new ReadChainedDataFrame<NS, R, E, G, CDF & CDF2, CEX & CEX2>(
            next,
            this.session,
            this.streaming,
            schemaTransform(this.runtimeSchema),
        );
    }

    select<K extends ColumnName<S>>(
        ...cols: K[]
    ): ReadChainedDataFrame<SelectedSchema<S, K>, R, E, G, CDF, CEX>;
    select(...cols: EBuilder[]): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>;
    select(...cols: (string | EBuilder)[]): ReadChainedDataFrame<any, R, E, G, CDF, CEX> {
        if (cols.every((column): column is string => typeof column === "string")) {
            assertUniqueColumnNames("select()", cols);
        }
        return this.chainAs<any>(
            (df, EX, DF) =>
                DF.select(df, cols.map(c => typeof c === "string" ? EX.col(c) : c.build(EX))),
            schema => cols.every((column): column is string => typeof column === "string")
                ? selectRuntimeSchema(schema, cols)
                : undefined,
        );
    }

    filter(predicate: TypedPredicate<S, E>): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    filter(cond: EBuilder): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    filter(cond: EBuilder | TypedPredicate<S, E>): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) => {
            const condition = typeof cond === "function"
                ? (cond as (columns: Columns<Extract<S, SchemaShape>, E>) => Condition<E>)(
                    makeColumns<Extract<S, SchemaShape>, E>()
                )
                : cond;
            return DF.filter(df, condition.build(EX));
        });
    }

    withColumn<Name extends string, T extends ColumnType>(
        name: Name,
        build: TypedColumnFactory<S, T, E>,
    ): ReadChainedDataFrame<AddedColumnSchema<S, Name, T>, R, E, G, CDF, CEX>;
    withColumn(name: string, e: EBuilder): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>;
    withColumn<Name extends string, T extends ColumnType>(
        name: Name,
        input: EBuilder | TypedColumnFactory<S, T, E>,
    ): ReadChainedDataFrame<any, R, E, G, CDF, CEX> {
        return this.chainAs<any>(
            (df, EX, DF) => {
                const column = typeof input === "function"
                    ? (input as (columns: Columns<Extract<S, SchemaShape>, E>) => TypedColumn<T, E>)(
                        makeColumns<Extract<S, SchemaShape>, E>()
                    )
                    : input;
                return DF.withColumn(df, name, column.build(EX));
            },
            () => undefined,
        );
    }

    join<RS extends SchemaShape, JT extends JoinTypeInput = "INNER", RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
        on: TypedJoinPredicate<S, RS, E>,
        jt?: JT,
    ): ReadChainedDataFrame<JoinSchema<S, RS, JT>, R, E, G, CDF & RCDF, CEX & RCEX>;
    join<RS = unknown, RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
        on: EBuilder,
        jt?: JoinTypeInput,
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF & RCDF, CEX & RCEX>;
    join<RS, RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
        on: EBuilder | TypedJoinPredicate<S, Extract<RS, SchemaShape>, E>,
        jt: JoinTypeInput = DEFAULT_JOIN_TYPE
    ): ReadChainedDataFrame<any, R, E, G, CDF & RCDF, CEX & RCEX> {
        const typedJoin = typeof on === "function";
        const leftPlanId = typedJoin ? freshRelationPlanId() : undefined;
        const rightPlanId = typedJoin ? freshRelationPlanId() : undefined;
        return this.chainAs<any, RCDF, RCEX>((_, EX, DF) => {
            const rawLeftPlan = this.prog(DF, EX);
            // DF tiene al menos CDF & RCDF; EX tiene al menos CEX & RCEX.
            const rawRightPlan = right.getProgram()(
                DF as DFAlg<R, E, G, RCDF>,
                EX as ExprAlg<E> & RCEX,
            );
            const leftPlan = typedJoin && leftPlanId !== undefined && DF.withPlanId
                ? DF.withPlanId(rawLeftPlan, leftPlanId)
                : rawLeftPlan;
            const rightPlan = typedJoin && rightPlanId !== undefined && DF.withPlanId
                ? DF.withPlanId(rawRightPlan, rightPlanId)
                : rawRightPlan;
            const condition = typedJoin
                ? (on as (
                    left: Columns<Extract<S, SchemaShape>, E>,
                    right: Columns<Extract<RS, SchemaShape>, E>,
                ) => Condition<E>)(
                    makeColumns<Extract<S, SchemaShape>, E>(leftPlanId),
                    makeColumns<Extract<RS, SchemaShape>, E>(rightPlanId),
                )
                : on;
            return DF.join(leftPlan, rightPlan, condition.build(EX), jt);
        }, schema => joinRuntimeSchema(schema, right.runtimeSchema, jt));
    }


    groupBy<K extends ColumnName<S>>(
        ...by: K[]
    ): GroupedDataFrameTF<S, K, R, E, G, CDF, CEX>;
    groupBy(...by: EBuilder[]): GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX>;
    groupBy(
        ...by: (string | EBuilder)[]
    ): GroupedDataFrameTF<any, string, R, E, G, CDF, CEX> {
        if (by.every((column): column is string => typeof column === "string")) {
            assertUniqueColumnNames("groupBy()", by);
        }
        return new GroupedDataFrameTF(this, by);
    }

    orderBy(...colsOrKeys: Array<ColumnName<S> | EBuilder | TypedOrderFactory<S, E>>): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    orderBy(...colsOrKeys: Array<ColumnName<S> | EBuilder | ((EX: ExprAlg<any>) => SortOrder<any>)>): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    orderBy(...colsOrKeys: unknown[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) => {
            const orders = colsOrKeys.map(k => resolveOrderInput(k, EX));
            return DF.orderBy(df, orders);
        });
    }

    sort(...colsOrKeys: Array<ColumnName<S> | EBuilder | TypedOrderFactory<S, E>>): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    sort(...colsOrKeys: Array<ColumnName<S> | EBuilder | ((EX: ExprAlg<any>) => SortOrder<any>)>): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    sort(...colsOrKeys: unknown[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) => {
            const orders = colsOrKeys.map(k => resolveOrderInput(k, EX));
            return DF.sort(df, orders);
        });
    }

    limit(n: number) {
        assertInteger("limit()", n, 0);
        return this.chain((df, _EX, DF) => DF.limit(df, n));
    }

    distinct() {
        return this.chain((df, _EX, DF) => DF.distinct(df));
    }

    dropDuplicates<K extends ColumnName<S>>(...cols: K[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    dropDuplicates(...cols: EBuilder[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    dropDuplicates(...cols: (string | EBuilder)[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) => {
            const exprs =
                cols.length === 0
                    ? undefined
                    : cols.map(c => (typeof c === "string" ? EX.col(c) : c.build(EX)));
            return DF.dropDuplicates(df, exprs);
        });
    }

    union(right: ReadChainedDataFrame<S, R, E, G, CDF, CEX>): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        const next: DFProgram<R, E, G, CDF, CEX> = (DF, EX) => {
            const leftPlan = this.prog(DF, EX);
            const rightPlan = right.prog(DF, EX);
            return DF.union(leftPlan, rightPlan);
        };
        return new ReadChainedDataFrame<S, R, E, G, CDF, CEX>(
            next,
            this.session,
            this.streaming || right.streaming,
            schemaDefsEqual(this.runtimeSchema, right.runtimeSchema)
                ? this.runtimeSchema
                : undefined,
        );
    }

    unionByName<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>,
        allowMissingColumns?: false,
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX>;
    unionByName<RS, RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
        allowMissingColumns: true,
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF & RCDF, CEX & RCEX>;
    unionByName<RS, RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
        allowMissingColumns = false,
    ): ReadChainedDataFrame<any, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<any, RCDF, RCEX>(
            (_, EX, DF) =>
                DF.union(
                    this.prog(DF, EX),
                    right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                    { byName: true, allowMissingColumns }
                ),
            schema => !allowMissingColumns && schemaDefsEqual(schema, right.runtimeSchema)
                ? schema
                : undefined,
        );
    }

    intersect<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (_, EX, DF) => DF.intersect(
                this.prog(DF, EX),
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX)
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
        );
    }

    intersectAll<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (_, EX, DF) => DF.intersect(
                this.prog(DF, EX),
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                { all: true }
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
        );
    }

    except<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (_, EX, DF) => DF.except(
                this.prog(DF, EX),
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX)
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
        );
    }

    exceptAll<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (_, EX, DF) => DF.except(
                this.prog(DF, EX),
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                { all: true }
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
        );
    }

    withColumnRenamed<From extends ColumnName<S>, const To extends string>(
        oldName: From,
        newName: ValidRenameTarget<S, From, To>,
    ): ReadChainedDataFrame<RenamedSchema<S, From, To>, R, E, G, CDF, CEX> {
        assertRenameMapping({ [oldName]: newName });
        return this.chainAs<RenamedSchema<S, From, To>>(
            (df, _EX, DF) => DF.withColumnRenamed(df, oldName, newName),
            schema => renameRuntimeSchema(schema, { [oldName]: newName }),
        );
    }

    withColumnsRenamed<const M extends Record<string, string>>(
        mapping: ValidRenameMap<S, M>,
    ): ReadChainedDataFrame<RenamedColumnsSchema<S, M>, R, E, G, CDF, CEX> {
        assertRenameMapping(mapping);
        return this.chainAs<RenamedColumnsSchema<S, M>>(
            (df, _EX, DF) => DF.withColumnsRenamed(df, mapping),
            schema => renameRuntimeSchema(schema, mapping),
        );
    }

    coalesce(numPartitions: number): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionCap<R>, CEX>;
    coalesce(name: string, ...exprs: Array<string | EBuilder | number | boolean>): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>;
    coalesce(
        nameOrPartitions: string | number,
        ...exprs: Array<string | EBuilder | number | boolean>
    ): ReadChainedDataFrame<any, R, E, G, any, CEX> {
        if (typeof nameOrPartitions === "number") {
            assertInteger("coalesce() numPartitions", nameOrPartitions, 1);
            return this.chain<RepartitionCap<R>>(
                (df, _EX, DF) => DF.coalesce(df, nameOrPartitions)
            );
        }
        assertNonEmptyString("coalesce() output column", nameOrPartitions);
        if (exprs.length === 0) {
            throw new TypeError("coalesce() requires at least one expression.");
        }
        return this.chainAs<UnknownSchema>(
            (df, EX, DF) => {
                const toE = (x: string | EBuilder | number | boolean) =>
                    typeof x === "string"
                        ? EX.col(x)
                        : typeof x === "object" && x !== null && "build" in x
                            ? (x as EBuilder).build(EX)
                            : EX.lit(x as any);

                const coalesced = EX.coalesce(exprs.map(toE));
                return DF.withColumn(df, nameOrPartitions, coalesced);
            },
            () => undefined,
        );
    }

    describe<K extends ColumnName<S>>(
        colNames: readonly K[],
    ): ReadChainedDataFrame<StatisticsSchema<S, K>, R, E, G, CDF, CEX> {
        if ((colNames as readonly string[]).includes("summary")) {
            throw new TypeError("describe() cannot describe a column named 'summary'.");
        }
        return this.chainAs<StatisticsSchema<S, K>>((df, EX, DF) => {
            const toString = (e: any) => EX.call("concat", [EX.lit(""), e]);
            const stats = ["count", "mean", "stddev", "min", "max"] as const;
            const NULL_D = EX.call("nullif", [EX.lit(1.0), EX.lit(1.0)]);
            const NUMERIC_RX = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$";
            const toDoubleIfNumeric = (name: string) =>
                EX.caseWhen(
                    [{
                        when: EX.call("rlike", [
                            EX.call("concat", [EX.lit(""), EX.col(name)]),
                            EX.lit(NUMERIC_RX),
                        ]),
                        then: EX.bin("*", EX.lit(1.0), EX.col(name)),
                    }],
                    NULL_D
                );

            const pruned = DF.select(df, colNames.map(n => EX.col(n)));

            const numExpr: Record<string, any> = Object.fromEntries(
                colNames.map(n => [n, toDoubleIfNumeric(n)])
            );

            const measures = Object.fromEntries(
                colNames.flatMap(n => ([
                    [`__${n}_count`, EX.call("count", [EX.col(n)])],
                    [`__${n}_mean`,   EX.call("avg", [numExpr[n]])],
                    [`__${n}_stddev`, EX.call("stddev_samp", [numExpr[n]])],
                    [`__${n}_min`, EX.call("min", [EX.col(n)])],
                    [`__${n}_max`, EX.call("max", [EX.col(n)])],
                ]))
            );

            const aggregated = DF.agg(DF.groupBy(pruned, [] as E[]), measures);

            const projectFor = (stat: typeof stats[number]) =>
                DF.select(aggregated, [
                    EX.alias(EX.lit(stat), "summary"),
                    ...colNames.map(n =>
                        EX.alias(toString(EX.col(`__${n}_${stat}`)), n)
                    ),
                ]);

            return stats.slice(1).reduce(
                (acc, s) => DF.union(acc, projectFor(s), { byName: true }),
                projectFor(stats[0])
            );
        }, schema => statisticsRuntimeSchema(schema, colNames as readonly string[]));
    }

    summary<K extends ColumnName<S>>(
        metrics: readonly string[] | undefined,
        colNames: readonly K[],
    ): ReadChainedDataFrame<StatisticsSchema<S, K>, R, E, G, CDF, CEX> {
        if ((colNames as readonly string[]).includes("summary")) {
            throw new TypeError("summary() cannot summarize a column named 'summary'.");
        }
        return this.chainAs<StatisticsSchema<S, K>>((df, EX, DF) => {
            const DEFAULTS = ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"] as const;
            const req = (metrics?.length ? metrics : DEFAULTS).map(m => m.toLowerCase());

            type Parsed =
                | { kind: "builtin"; name: "count" | "mean" | "stddev" | "min" | "max"; label: string; suffix: string }
                | { kind: "pct"; p: number; label: string; suffix: string };

            const norm = (m: string): Parsed => {
                if (m === "median") m = "50%";
                if (/%$/.test(m)) {
                    const p = parseFloat(m) / 100;
                    if (!(p >= 0 && p <= 1)) throw new Error(`summary(): invalid percentile '${m}'`);
                    const pct = Math.round(p * 100);
                    return { kind: "pct", p, label: `${pct}%`, suffix: `p${pct}` };
                }
                if (m === "std") m = "stddev";
                const ok = ["count", "mean", "stddev", "min", "max"] as const;
                if ((ok as readonly string[]).includes(m)) {
                    return { kind: "builtin", name: m as any, label: m, suffix: (m === "stddev" ? "stddev" : m) };
                }
                throw new Error(`summary(): unsupported metric '${m}'`);
            };
            const parsed: Parsed[] = req.map(norm);

            const toString = (e: any) => EX.call("concat", [EX.lit(""), e]);
            const NULL_D = EX.call("nullif", [EX.lit(1.0), EX.lit(1.0)]);
            const NUMERIC_RX = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$";
            const toDoubleIfNumeric = (name: string) =>
                EX.caseWhen(
                    [{
                        when: EX.call("rlike", [
                            EX.call("concat", [EX.lit(""), EX.col(name)]),
                            EX.lit(NUMERIC_RX),
                        ]),
                        then: EX.bin("*", EX.lit(1.0), EX.col(name)),
                    }],
                    NULL_D
                );

            const pruned = DF.select(df, colNames.map(n => EX.col(n)));

            const pairs: [string, any][] = [];
            for (const n of colNames) {
                const numArg = toDoubleIfNumeric(n);
                for (const m of parsed) {
                    if (m.kind === "builtin") {
                        if (m.name === "count") pairs.push([`__${n}_count`, EX.call("count", [EX.col(n)])]);
                        if (m.name === "mean") pairs.push([`__${n}_mean`, EX.call("avg", [numArg])]);
                        if (m.name === "stddev") pairs.push([`__${n}_stddev`, EX.call("stddev_samp", [numArg])]);
                        if (m.name === "min") pairs.push([`__${n}_min`, EX.call("min", [EX.col(n)])]);
                        if (m.name === "max") pairs.push([`__${n}_max`, EX.call("max", [EX.col(n)])]);
                    } else {
                        pairs.push([`__${n}_${m.suffix}`, EX.call("percentile_approx", [numArg, EX.lit(m.p)])]);
                    }
                }
            }
            const measures = Object.fromEntries(pairs);
            const aggregated = DF.agg(DF.groupBy(pruned, [] as E[]), measures);

            const projectFor = (m: Parsed) =>
                DF.select(aggregated, [
                    EX.alias(EX.lit(m.label), "summary"),
                    ...colNames.map(n =>
                        EX.alias(toString(EX.col(`__${n}_${m.suffix}`)), n)
                    ),
                ]);

            const rows = parsed.map(projectFor);
            return rows.slice(1).reduce(
                (acc, r) => DF.union(acc, r, { byName: true }),
                rows[0]
            );
        }, schema => statisticsRuntimeSchema(schema, colNames as readonly string[]));
    }

    repartition(numPartitions: number, shuffle = true): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionCap<R>, CEX> {
        assertInteger("repartition() numPartitions", numPartitions, 1);
        return this.chain<RepartitionCap<R>>((df, _EX, DF) => DF.repartition(df, numPartitions, shuffle));
    }

    hint(name: JoinHintName | string, ...params: any[]) {
        return this.chain<HintCap<R>>((df, _EX, DF) => DF.hint(df, name, params));
    }

    broadcast() {
        return this.hint("broadcast");
    }

    mergeHint() {
        return this.hint("merge");
    }

    shuffleHashHint() {
        return this.hint("shuffle_hash");
    }

    shuffleReplicateNLHint() {
        return this.hint("shuffle_replicate_nl");
    }

    coalescePartitions(numPartitions: number): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionCap<R>, CEX> {
        assertInteger("coalescePartitions() numPartitions", numPartitions, 1);
        return this.chain<RepartitionCap<R>>((df, _EX, DF) => DF.coalesce(df, numPartitions));
    }

    /** @deprecated Prefer `session.sql(query)`; SQL is session-scoped and does not consume this DataFrame. */
    sql(query: string): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF & SqlCap<R>, CEX> {
        assertNonEmptyString("sql() query", query);
        return this.chainAs<UnknownSchema, SqlCap<R>>((_df, _EX, DF) => DF.sql(query));
    }

    cache(): ReadChainedDataFrame<S, R, E, G, CDF & CacheCap<R>, CEX> {
        return this.chain<CacheCap<R>>((df, _EX, DF) => DF.cache(df));
    }

    persist(level?: string): ReadChainedDataFrame<S, R, E, G, CDF & CacheCap<R>, CEX> {
        return this.chain<CacheCap<R>>((df, _EX, DF) => DF.persist(df, level ?? "MEMORY_AND_DISK"));
    }

    unpersist(blocking?: boolean): ReadChainedDataFrame<S, R, E, G, CDF & CacheCap<R>, CEX> {
        return this.chain<CacheCap<R>>((df, _EX, DF) => DF.unpersist(df, blocking));
    }

    runWith(DF: DFAlg<R, E, G, CDF>, EX: ExprAlg<E> & CEX): R {
        return this.prog(DF, EX);
    }

    getProgram(): DFProgram<R, E, G, CDF, CEX> {
        return this.prog;
    }

    getSession(): SparkSession {
        return this.session;
    }

    /** @internal Runtime flavor marker used to reject crossed batch/stream writers. */
    isStreamingDataFrame(): boolean {
        return this.streaming;
    }

    /**
     * Afirma un schema para una fuente dinámica. Es una promesa no verificada:
     * no agrega DDL ni consulta a Spark. Cuando el schema puede declararse, se
     * recomienda `session.read.readWith(schema(...), ...)`.
     */
    as<T extends Schema>(): ReadChainedDataFrame<T, R, E, G, CDF, CEX> {
        return new ReadChainedDataFrame<T, R, E, G, CDF, CEX>(
            this.prog,
            this.session,
            this.streaming,
            undefined,
        );
    }

    /**
     * Runs the program against a concrete interpreter pair.
     *
     * `prog` is written generically over the algebra (it works for any R/E/G),
     * but this class fixes R/E/G as type parameters. Each concrete interpreter
     * (Spark logical plan, proto, trace) carries its own representation types,
     * so feeding one in requires a single cast. Confining it here keeps the rest
     * of the class free of `as any` and documents why the cast is sound: the
     * program never inspects R/E/G, it only forwards algebra calls.
     */
    private interpretWith<Out>(DF: unknown, EX: unknown): Out {
        return (this.prog as unknown as (df: unknown, ex: unknown) => Out)(DF, EX);
    }

    private compileToSparkPlan(): LogicalPlan {
        return this.interpretWith<LogicalPlan>(SparkDFAlg, SparkExprAlg);
    }

    async collectRaw(): Promise<unknown[]> {
        const root = this.interpretWith<unknown>(ProtoDFAlg, ProtoExprAlg);
        return ProtoExec.collect(root, this.session);
    }

    /** Streams raw Spark Connect ExecutePlan responses with bounded buffering. */
    async *streamRaw(): AsyncGenerator<Record<string, unknown>, void, void> {
        const root = this.interpretWith<LogicalPlan>(ProtoDFAlg, ProtoExprAlg);
        const executor = await applyPendingAnalyzeActions(root, this.session);
        for await (const response of executor.stream(root)) {
            yield response;
        }
    }

    /** Streams Arrow IPC batches as they arrive from Spark. */
    async *toArrowBatches(): AsyncGenerator<Buffer, void, void> {
        for await (const response of this.streamRaw()) {
            for (const buffer of arrowBuffersFromResponses([response])) yield buffer;
        }
    }

    /** Streams decoded rows without collecting the entire DataFrame in memory. */
    async *toRows(): AsyncGenerator<RowOf<S>, void, void> {
        for await (const buffer of this.toArrowBatches()) {
            for (const row of rowsFromArrowBuffers<RowOf<S>>([buffer], this.runtimeSchema)) yield row;
        }
    }

    /** Executes the plan and decodes Spark's Arrow batches into row objects. */
    async collect(): Promise<Array<RowOf<S>>> {
        const responses = await this.collectRaw();
        return rowsFromArrowBuffers<RowOf<S>>(
            arrowBuffersFromResponses(responses),
            this.runtimeSchema,
        );
    }

    /**
     * Prints at most `n` rows, truncating cells to `truncate` characters.
     * Unlike the previous implementation this never collects the full DataFrame.
     */
    async show(n = 20, truncate = 20): Promise<void> {
        if (!Number.isInteger(n) || n < 0) {
            throw new RangeError("show(): n must be a non-negative integer.");
        }
        if (!Number.isInteger(truncate) || truncate < 0) {
            throw new RangeError("show(): truncate must be a non-negative integer.");
        }

        const result = await this.limit(n).collectRaw();
        printArrowResults(arrowBuffersFromResponses(result), { maxRows: n, truncate });
    }

    explain(mode: ExplainModeInput = "simple"): Promise<string> {
        const root = this.interpretWith<unknown>(ProtoDFAlg, ProtoExprAlg);
        return ProtoExec.explain(root, this.session, mode);
    }

    // dentro de ReadChainedDataFrame<...>
    write(
        this: ReadChainedDataFrame<S, R, E, G, Exclude<CDF, StreamingMark<R>>, CEX>
    ): DataFrameWriterTF<
        R, E, G,
        WBatch,
        Exclude<CDF, StreamingMark<R>>,
        CEX,
        BatchWriterAlg<R>
    > {
        if (this.streaming) {
            throw new Error("Cannot use .write (batch) on a streaming DataFrame. Use .writeStream() instead.");
        }
        type CDFBatch = Exclude<CDF, StreamingMark<R>>;

        const prog: BatchWProgram<R, E, G, CDFBatch, CEX> =
            (WR, DF, EX) => {
                const root = this.getProgram()(DF as any, EX as any);
                return (WR as any).fromChild(root);
            };

        const wProgram = (WR: BatchWriterAlg<R>,
                          DF: DFAlg<R, E, G, CDFBatch>,
                          EX: ExprAlg<E> & CEX) =>
            prog(WR, DF, EX);

        return DataFrameWriterTF.fromParts<
            R, E, G,
            WBatch,
            CDFBatch,
            CEX,
            BatchWriterAlg<R>
        >({
            session: this.getSession(),
            dfProgram: this.getProgram() as unknown as DFProgram<R, E, G, CDFBatch, CEX>,
            wProgram,
        });
    }

    private compileTrace(): any {
        return this.interpretWith<any>(TraceDFAlg, TraceExprAlg);
    }

    toClientASTJSON(): string {
        const root = this.compileTrace();
        return toJSON(root);
    }

    toClientASTMermaid(): string {
        const root = this.compileTrace();
        return toMermaid(root);
    }

    toSparkLogicalPlanJSON(): string {
        const lp = this.compileToSparkPlan();
        return JSON.stringify(lp, null, 2);
    }

    toProtoJSON(): string {
        const protoRoot = this.interpretWith<unknown>(ProtoDFAlg, ProtoExprAlg);
        return JSON.stringify(protoRoot, null, 2);
    }

    sample(fraction: number, withReplacement = false, seed?: number, deterministicOrder = false) {
        if (!Number.isFinite(fraction) || fraction < 0) {
            throw new RangeError("sample(): fraction must be a finite number >= 0");
        }
        if (!withReplacement && fraction > 1) {
            throw new Error("sample(): fraction must be in [0,1] when withReplacement=false");
        }
        assertOptionalSeed("sample()", seed);
        const resolvedSeed = seed ?? freshSparkSeed();
        const lb = 0.0, ub = fraction;
        return this.chain<SamplingCap<R>>((df, _EX, DF) => DF.sample(df, lb, ub, withReplacement, resolvedSeed, deterministicOrder));
    }

    drop<K extends ColumnName<S>>(
        ...columnNames: K[]
    ): ReadChainedDataFrame<DroppedSchema<S, K>, R, E, G, CDF, CEX> {
        return this.chainAs<DroppedSchema<S, K>>(
            (df, _EX, DF) => DF.drop(df, columnNames),
            schema => dropRuntimeSchema(schema, columnNames),
        );
    }

    randomSplit(weights: number[], seed?: number): ReadChainedDataFrame<S, R, E, G, CDF & SamplingCap<R>, CEX>[] {
        if (!weights?.length) throw new Error("randomSplit(): weights must not be empty");
        if (weights.some(weight => !Number.isFinite(weight) || weight < 0)) {
            throw new RangeError("randomSplit(): weights must be finite non-negative numbers");
        }
        const sum = weights.reduce((a, b) => a + b, 0);
        if (!Number.isFinite(sum)) {
            throw new RangeError("randomSplit(): sum of weights must be finite");
        }
        if (sum <= 0) throw new Error("randomSplit(): sum of weights must be > 0");
        assertOptionalSeed("randomSplit()", seed);
        const resolvedSeed = seed ?? freshSparkSeed();

        const bounds: Array<[number, number]> = [];
        let acc = 0;
        for (const w of weights) {
            const start = acc / sum;
            acc += w;
            const end = acc / sum;
            bounds.push([start, end]);
        }

        // Spark Connect exposes the same lower/upper-bound Sample relation
        // used by Spark's randomSplit implementation. This avoids overwriting
        // and then dropping a real user column named `__rand_split__`.
        return bounds.map(([lowerBound, upperBound]) =>
            this.chain<SamplingCap<R>>((df, _EX, DF) =>
                DF.sample(df, lowerBound, upperBound, false, resolvedSeed, true)
            )
        );
    }

    static readStream<R, E, G, CDF = unknown, CEX = unknown>(
        format: string,
        session: SparkSession,
        options?: Record<string, string>
    ) {
        assertNonEmptyString("readStream() format", format);
        type Need = StreamingReadCap<R>  & StreamingMark<R>;
        const p: DFProgram<R, E, G, CDF & Need, CEX> =
            (DF: DFAlg<R, E, G, CDF & Need>) => DF.readStream(format, options);
        return new ReadChainedDataFrame<UnknownSchema, R, E, G, CDF & Need, CEX>(p, session, true);
    }

    withWatermark(eventTimeCol: EBuilder, delay: string) {
        assertNonEmptyString("withWatermark() delay", delay);
        type Need = EventTimeWatermarkCap<R, E>;
        return this.chain<Need>((df, EX, DF) =>
            (DF as DFAlg<R, E, G, CDF & Need>).withWatermark(df, eventTimeCol.build(EX), delay)
        );
    }

    writeStream(
        this: ReadChainedDataFrame<S, R, E, G, CDF & StreamingMark<R>, CEX>
    ): DataFrameWriterTF<R, E, G, WStream, CDF & StreamingMark<R>, CEX, StreamWriterAlg<R>> {
        if (!this.streaming) {
            throw new Error("Cannot use .writeStream() on a batch DataFrame. Use .write() instead.");
        }
        const prog: StreamWProgram<R, E, G, CDF & StreamingMark<R>, CEX> =
            (WR, DF, EX) => WR.writeStream(this.getProgram()(DF as any, EX as any));
        return DataFrameWriterTF.fromParts({
            session: this.getSession(),
            dfProgram: this.getProgram() as any,
            wProgram: (WR, DF, EX) => prog(WR as any, DF, EX),
        });
    }
}

/**
 * Resultado intermedio de `groupBy`. Conserva el programa tagless-final y las
 * claves en el tipo; `agg` materializa recién al interpretar, de modo que los
 * builders tipados siguen siendo agnósticos de proto/trace/logical-plan.
 */
export class GroupedDataFrameTF<
    S,
    K extends PropertyKey,
    R,
    E,
    G,
    CDF,
    CEX,
> {
    constructor(
        private readonly source: ReadChainedDataFrame<S, R, E, G, CDF, CEX>,
        private readonly grouping: ReadonlyArray<string | EBuilder>,
    ) {}

    agg<const A extends readonly TypedAggregation<string, ColumnType, E>[]>(
        build: TypedAggBuilder<S, E, A>,
    ): ReadChainedDataFrame<AggregatedSchema<S, K, A>, R, E, G, CDF, CEX>;
    agg(
        aggregations: Record<string, EBuilder | string>,
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>;
    agg<const A extends readonly TypedAggregation<string, ColumnType, E>[]>(
        input: TypedAggBuilder<S, E, A> | Record<string, EBuilder | string>,
    ): ReadChainedDataFrame<any, R, E, G, CDF, CEX> {
        const parseAggregation = (value: string) => {
            const match = value.match(/^\s*([A-Za-z_]\w*)\s*\(\s*([^)]+)\s*\)\s*$/);
            if (!match) throw new Error(`Invalid aggregation: ${value}`);
            return { fn: match[1], arg: match[2] };
        };

        const next: DFProgram<R, E, G, CDF, CEX> = (DF, EX) => {
            const keys = this.grouping.map(key =>
                typeof key === "string" ? EX.col(key) : key.build(EX)
            );
            const grouped = DF.groupBy(this.source.getProgram()(DF, EX), keys);

            if (typeof input === "function") {
                const built = (input as (
                    factory: AggFactory<Extract<S, SchemaShape>, E>
                ) => readonly [...A])(
                    makeAggFactory<Extract<S, SchemaShape>, E>()
                );
                const aliases = built.map(aggregation => aggregation.alias);
                assertUniqueColumnNames("agg()", aliases);
                for (const alias of aliases) assertNonEmptyString("agg() alias", alias);
                const groupingNames = this.grouping.filter(
                    (key): key is string => typeof key === "string"
                );
                const collision = aliases.find(alias => groupingNames.includes(alias));
                if (collision) {
                    throw new TypeError(
                        `agg() alias ${JSON.stringify(collision)} collides with a grouping column.`
                    );
                }
                const expressions = Object.fromEntries(
                    built.map(aggregation => [aggregation.alias, aggregation.build(EX)])
                );
                return DF.agg(grouped, expressions);
            }

            const expressions = Object.fromEntries(
                Object.entries(input).map(([alias, value]) => {
                    if (typeof value !== "string") return [alias, value.build(EX)];
                    const { fn, arg } = parseAggregation(value);
                    return [alias, EX.call(fn, [EX.col(arg)])];
                })
            );
            return DF.agg(grouped, expressions);
        };

        return new ReadChainedDataFrame(
            next,
            this.source.getSession(),
            this.source.isStreamingDataFrame(),
        );
    }
}
