import { SparkDFAlg, SparkExprAlg } from "./readDataFrameInterpreter";
import { LogicalPlan } from "../engine/logicalPlan";
import {
    ProtoDFAlg,
    ProtoExec,
    ProtoExprAlg,
    ProtoRel,
    applyPendingAnalyzeActions,
} from "../engine/compilerRead";
import { SparkSession } from "../client/session";
import type { ExecutionOptions } from "../client/executionOptions";
import {
    DEFAULT_JOIN_TYPE,
    ExplainModeInput,
    GroupTypeInput,
    JoinHintName,
    JoinTypeInput,
} from "../engine/sparkConnectEnums";
import { printArrowResults } from "../utils/arrowPrinter";
import {DataFrameWriterTF} from "../write/dataFrameWriterTF";
import { toJSON, toMermaid } from "../trace/traceSerializers";
import { TraceDFAlg, TraceExprAlg, TraceNode } from "../trace/trace";
import { NullsOrder, SortOrder } from "../types";
import {DFAlg, DFProgram, EventTimeWatermarkCap, ExprAlg, LiteralValue, StreamingMark, StreamingReadCap} from "../algebra/read";
import {
    AsOfDirection,
    CacheCap,
    HintCap,
    RepartitionByExpressionCap,
    RepartitionCap,
    SamplingCap,
    SqlCap,
} from "../algebra/read/batch-capabilities";
import { SparkConnectExecutor, type AnalyzeField } from "../client/sparkConnectExecutor";
import { storageLevelFromProto, toProtoStorageLevel, type StorageLevel } from "../client/storageLevel";
import type { ResolvedExecution } from "../client/executionOptions";
import type { EBuilder as ColumnExpression, SortKeyBuilder } from "../engine/column";
import { DataFrameWriterV2 } from "../write/dataFrameWriterV2";
import { MergeIntoWriter } from "../write/mergeIntoWriter";
import {
    DataFrameNaFunctions,
    DataFrameStatFunctions,
    NAFillValue,
    NAHow,
    NAReplaceValue,
} from "./dataFrameFunctions";
import { SparkStructType, sparkStructType } from "./dataTypes";
import { Observation } from "./observation";
import {
    assertQuantileArguments,
    assertSupport,
    columnList,
    dropNaMinimum,
    fillSpec,
    replaceSpec,
    sampleFractions,
} from "./naStat";
import {BatchWProgram, StreamWProgram, WBatch, WStream} from "../algebra/write";
import {BatchWriterAlg, StreamWriterAlg} from "../algebra/write/dataframe";
import {
    ColumnType,
    Prettify,
    Schema,
    SchemaShape,
    UnknownSchema,
    UsableColumnKey,
} from "../schema/schema-model";
import { DeclaredSchema, schema as declareSchema, type FieldSpec, type InferSchema, type SchemaDef } from "../schema/schema";
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
import {
    assertInteger,
    assertNonEmptyString,
    assertOptionalSeed,
    freshRelationPlanId,
    freshSparkSeed,
} from "./dataframeValidation";
import { resolveOrderInput } from "./orderResolver";
import {
    assertStatisticsColumns,
    buildDescribePlan,
    buildSummaryPlan,
} from "./statistics";
import {
    assertRenameMapping,
    assertUniqueColumnNames,
    dropRuntimeSchema,
    joinRuntimeSchema,
    renameRuntimeSchema,
    schemaDefsEqual,
    selectRuntimeSchema,
    statisticsRuntimeSchema,
} from "./runtimeSchema";

/**
 * Minimal structural contract accepted by the lax (untyped) DataFrame
 * overloads: any value that can build an expression against an `ExprAlg`.
 *
 * @deprecated Annotate with `EBuilder` from the root entry point instead; every
 * expression helper (`col`, `lit`, `when`, ...) returns an `EBuilder`, which
 * satisfies this shape. Exported as `DataFrameExpressionBuilder`, this alias
 * will be removed from the root entry point in the next major release.
 * @public
 */
export type EBuilder = { build<E>(EX: ExprAlg<E>): E };
export type SortKeyInput =
    | string
    | EBuilder
    | ((EX: ExprAlg<any>) => SortOrder<any>);

/** @public */
export type ColumnName<S> = [S] extends [UnknownSchema]
    ? string
    : S extends SchemaShape
      ? UsableColumnKey<S>
      : string;

/** @internal */
export type SelectedSchema<S, K extends PropertyKey> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? Selected<S, Extract<K, keyof S>>
      : UnknownSchema;

/** @internal */
export type DroppedSchema<S, K extends PropertyKey> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? Dropped<S, Extract<K, keyof S>>
      : UnknownSchema;

/** @internal */
export type RenamedSchema<S, From extends PropertyKey, To extends string> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? Renamed<S, Extract<From, keyof S>, To>
      : UnknownSchema;

/** @internal */
export type AddedColumnSchema<S, Name extends string, T extends ColumnType> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? WithColumn<S, Name, T>
      : UnknownSchema;

/** @internal */
export type RenamedColumnsSchema<S, M extends Record<string, string>> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? { [K in keyof S as K extends keyof M ? Extract<M[K], string> : K]: S[K] }
      : UnknownSchema;

/** @internal */
export type DuplicateRenameTargets<M extends Record<string, string>> = {
    [K in keyof M]: {
        [P in Exclude<keyof M, K>]: M[P] extends M[K]
            ? M[K] extends M[P]
                ? M[K]
                : never
            : never;
    }[Exclude<keyof M, K>];
}[keyof M];

/** @internal */
export type ValidRenameMap<S, M extends Record<string, string>> =
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

/** @internal */
export type ValidRenameTarget<S, From extends PropertyKey, To extends string> =
    [S] extends [UnknownSchema]
        ? To
        : To extends Extract<Exclude<keyof Extract<S, SchemaShape>, From>, string>
          ? never
          : To;

/** @internal */
export type StatisticsSchema<S, K extends PropertyKey> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? { summary: string } & { [P in Extract<K, keyof S>]: string | null }
      : UnknownSchema;

/** @internal */
export type JoinSchema<S, RS extends SchemaShape, JT extends JoinTypeInput> = S extends SchemaShape
    ? JoinedFor<S, RS, JT>
    : UnknownSchema;

/** @public */
export type TypedPredicate<S, E> = S extends SchemaShape
    ? (columns: Columns<S, E>) => Condition<E>
    : never;

/** @public */
export type TypedColumnFactory<S, T extends ColumnType, E> = S extends SchemaShape
    ? (columns: Columns<S, E>) => TypedColumn<T, E>
    : never;

/** @public */
export type TypedJoinPredicate<S, RS extends SchemaShape, E> = S extends SchemaShape
    ? (left: Columns<S, E>, right: Columns<RS, E>) => Condition<E>
    : never;

/** @public */
export type TypedOrderFactory<S, E> = S extends SchemaShape
    ? (columns: Columns<S, E>) => SortKey<E> | TypedColumn<ColumnType, E>
    : never;

/** @public */
export type TypedAggBuilder<
    S,
    E,
    A extends readonly TypedAggregation<string, ColumnType, E>[],
> = S extends SchemaShape
    ? (factory: AggFactory<S, E>) => readonly [...A] & UniqueAggregationTuple<A>
    : never;

/** @internal */
export type UniqueAggregationTuple<
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

/** @internal */
export type AggregatedSchema<
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
 * @public
 */
export type RowOf<S> = [S] extends [UnknownSchema]
    ? Record<string, unknown>
    : S extends SchemaShape
      ? { [K in keyof S]: S[K] extends ColumnType ? S[K] : unknown }
      : Record<string, unknown>;

/**
 * Typed `withColumns` callback: receives the column accessor and returns the
 * new columns by name.
 * @public
 */
export type TypedColumnsFactory<S, M, E> = S extends SchemaShape
    ? (columns: Columns<S, E>) => M
    : never;

/** @internal Schema after adding or replacing every column of `M`. */
export type AddedColumnsSchema<S, M> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : S extends SchemaShape
      ? Prettify<Omit<S, keyof M> & {
          -readonly [K in keyof M]: M[K] extends TypedColumn<infer T, any> ? T : never;
      }>
      : UnknownSchema;

/** @internal Schema of a cross join; unknown when either side is. */
export type CrossJoinSchema<S, RS> = [S] extends [UnknownSchema]
    ? UnknownSchema
    : [RS] extends [UnknownSchema]
      ? UnknownSchema
      : S extends SchemaShape
        ? RS extends SchemaShape
          ? JoinedFor<S, RS, "CROSS">
          : UnknownSchema
        : UnknownSchema;

type ColumnInput = string | EBuilder;

function toColumnInputs(input: ColumnInput | ReadonlyArray<ColumnInput>): ColumnInput[] {
    const inputs = typeof input === "string" || !Array.isArray(input)
        ? [input as ColumnInput]
        : [...input];
    for (const value of inputs) {
        if (typeof value === "string") assertNonEmptyString("column", value);
    }
    return inputs;
}

function columnExpression<E>(input: ColumnInput, EX: ExprAlg<E>): E {
    return typeof input === "string" ? EX.col(input) : input.build(EX);
}

/** Partitioning inputs are column names, expressions or sort keys. */
function assertPartitionInputs(label: string, inputs: readonly unknown[]): void {
    for (const input of inputs) {
        if (typeof input === "string") {
            assertNonEmptyString(`${label} column`, input);
        } else if (typeof input !== "function" && !(input !== null && typeof input === "object" && "build" in input)) {
            throw new TypeError(`${label} columns must be names, column expressions or sort keys.`);
        }
    }
}

/**
 * Options of `asOfJoin`; the names follow PySpark's `_joinAsOf` and Scala's
 * `joinAsOf`.
 * @public
 */
export type AsOfJoinOptions = {
    /**
     * Largest distance between the as-of values that still matches, as a
     * number or an expression such as `expr("INTERVAL 1 HOUR")`.
     */
    tolerance?: ColumnExpression | number | bigint;
    /** Whether an equal as-of value counts as a match (default `true`). */
    allowExactMatches?: boolean;
    /** `backward` (default), `forward` or `nearest`. */
    direction?: AsOfDirection;
};

/**
 * Grouping variant carried from `rollup` / `cube` / `pivot` / `groupingSets`
 * to `agg`, with the inputs still unbuilt.
 * @internal
 */
export type GroupingInput = {
    groupType: GroupTypeInput;
    pivot?: { column: string | EBuilder; values?: LiteralValue[] };
    groupingSets?: Array<Array<string | EBuilder>>;
};

const LATERAL_JOIN_TYPES = new Set(["INNER", "CROSS", "LEFT", "LEFT_OUTER"]);
const AS_OF_DIRECTIONS = new Set<AsOfDirection>(["backward", "forward", "nearest"]);

/**
 * `to(schema)` reconciles a source whose columns Spark already reports as
 * nullable (every file and table source), and Spark refuses to cast those to
 * NOT NULL fields. The DDL sent is therefore nullable throughout, while the
 * declaration itself stays the runtime schema, which the Arrow decoder
 * enforces against the data.
 */
function nullableSpec(spec: FieldSpec): FieldSpec {
    if (typeof spec === "string") return (spec.endsWith("?") ? spec : `${spec}?`) as FieldSpec;
    switch (spec.kind) {
        case "array":
            return { ...spec, element: nullableSpec(spec.element), nullable: true };
        case "map":
            return { ...spec, value: nullableSpec(spec.value), nullable: true };
        case "struct":
            return { ...spec, fields: nullableFields(spec.fields), nullable: true };
        default:
            return { ...spec, nullable: true };
    }
}

function nullableFields(def: SchemaDef): SchemaDef {
    return Object.fromEntries(Object.entries(def).map(([name, spec]) => [name, nullableSpec(spec)]));
}

/** The relation id a CheckpointCommand reports, if any. */
function checkpointRelationId(responses: readonly unknown[]): string | undefined {
    for (const response of responses) {
        const result = (response as {
            checkpoint_command_result?: { relation?: { relation_id?: unknown } };
        } | null)?.checkpoint_command_result;
        const relationId = result?.relation?.relation_id;
        if (typeof relationId === "string" && relationId) return relationId;
    }
    return undefined;
}

export const col = (name: string): EBuilder => ({ build: EX => EX.col(name) });
export const lit = (v: LiteralValue): EBuilder => ({ build: EX => EX.lit(v) });
export const eq = (l: EBuilder, r: EBuilder | string | number | boolean): EBuilder => ({
    build: EX => EX.bin("=", l.build(EX), typeof r === "object" ? r.build(EX) : EX.lit(r))
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
 * @public
 */
export class ReadChainedDataFrame<S = UnknownSchema, R = unknown, E = unknown, G = unknown, CDF = {}, CEX = {}> {
    /** @internal Mantiene `S` invariante para que las set ops exijan el mismo schema. */
    declare private readonly __schemaInvariant: (schema: S) => S;
    /**
     * @internal Mantiene invariante únicamente la marca de streaming de `CDF`.
     * Sin esto `CDF` solo aparece en posición contravariante y los guards
     * `this:` de `write()` / `writeStream()` aceptan cualquier sabor; con la
     * marca invariante un DataFrame streaming no es asignable al `this` batch
     * ni viceversa, mientras que las capacidades restantes siguen siendo libres.
     */
    declare private readonly __streamingInvariant: (
        mark: Extract<CDF, StreamingMark<R>>,
    ) => Extract<CDF, StreamingMark<R>>;
    private readonly prog: DFProgram<R, E, G, CDF, CEX>;

    constructor(
        p: DFProgram<R, E, G, CDF, CEX>,
        private readonly session: SparkSession,
        private readonly streaming = false,
        private readonly runtimeSchema?: SchemaDef,
        /** Observations registered with `observe()` anywhere in this plan. */
        private readonly observations: readonly Observation[] = [],
        /** Set on the DataFrame `checkpoint()` returns; `releaseCheckpoint()` needs it. */
        private readonly checkpointRelationId?: string,
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

    /**
     * Compone el mismo programa cambiando únicamente el phantom schema.
     *
     * `step` recibe el plan izquierdo ya interpretado. Las operaciones binarias
     * deben usar ese valor en lugar de volver a ejecutar `this.prog`: cada
     * re-ejecución interpreta el subárbol izquierdo dos veces, y una cadena de
     * N joins o unions pasa a costar 2^N interpretaciones.
     *
     * `streaming` permite que las binarias marquen el resultado como streaming
     * cuando cualquiera de los dos lados lo es.
     */
    private chainAs<NS, CDF2 = unknown, CEX2 = unknown>(
        step: (df: R, EX: ExprAlg<E> & CEX & CEX2, DF: DFAlg<R, E, G, CDF & CDF2>) => R,
        schemaTransform: (schema: SchemaDef | undefined) => SchemaDef | undefined = schema => schema,
        streaming: boolean = this.streaming,
        observations: readonly Observation[] = this.observations,
    ): ReadChainedDataFrame<NS, R, E, G, CDF & CDF2, CEX & CEX2> {
        const next: DFProgram<R, E, G, CDF & CDF2, CEX & CEX2> = (DF, EX) =>
            step(this.prog(DF, EX), EX, DF);
        return new ReadChainedDataFrame<NS, R, E, G, CDF & CDF2, CEX & CEX2>(
            next,
            this.session,
            streaming,
            schemaTransform(this.runtimeSchema),
            observations,
        );
    }

    /** Per-execution controls for an action, including this plan's observations. */
    private execution(options?: ExecutionOptions): ResolvedExecution {
        const resolved = this.session.resolveExecution(options);
        return this.observations.length === 0 ? resolved : { ...resolved, observations: this.observations };
    }

    /** @internal Observations registered anywhere in this plan. */
    getObservations(): readonly Observation[] {
        return this.observations;
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

    /**
     * Projects Spark SQL expressions, e.g. `selectExpr("id", "amount * 2 AS doubled")`.
     * The server parses each string, so the result schema is not known statically.
     */
    selectExpr(...exprs: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        if (exprs.length === 0) {
            throw new TypeError("selectExpr() requires at least one expression.");
        }
        for (const sql of exprs) assertNonEmptyString("selectExpr() expression", sql);
        return this.chainAs<UnknownSchema>(
            (df, EX, DF) => DF.select(df, exprs.map(sql => EX.exprString(sql))),
            () => undefined,
        );
    }

    filter(predicate: TypedPredicate<S, E>): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    filter(cond: EBuilder): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    /** Filters with a Spark SQL predicate, e.g. `filter("age > 21 AND country = 'AR'")`. */
    filter(condition: string): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    filter(cond: EBuilder | TypedPredicate<S, E> | string): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        if (typeof cond === "string") assertNonEmptyString("filter() condition", cond);
        return this.chain((df, EX, DF) => {
            if (typeof cond === "string") return DF.filter(df, EX.exprString(cond));
            const condition = typeof cond === "function"
                ? (cond as (columns: Columns<Extract<S, SchemaShape>, E>) => Condition<E>)(
                    makeColumns<Extract<S, SchemaShape>, E>()
                )
                : cond;
            return DF.filter(df, condition.build(EX));
        });
    }

    /** Alias of `filter`, matching PySpark's `DataFrame.where`. */
    where(predicate: TypedPredicate<S, E>): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    where(cond: EBuilder): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    where(condition: string): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    where(cond: EBuilder | TypedPredicate<S, E> | string): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.filter(cond as EBuilder);
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
        return this.chainAs<any, RCDF, RCEX>((rawLeftPlan, EX, DF) => {
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
        }, schema => joinRuntimeSchema(schema, right.runtimeSchema, jt), this.streaming || right.streaming,
            [...this.observations, ...right.observations]);
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

    union<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>,
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (df, EX, DF) => DF.union(
                df,
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
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
            (df, EX, DF) =>
                DF.union(
                    df,
                    right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                    { byName: true, allowMissingColumns }
                ),
            schema => !allowMissingColumns && schemaDefsEqual(schema, right.runtimeSchema)
                ? schema
                : undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
        );
    }

    intersect<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (df, EX, DF) => DF.intersect(
                df,
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX)
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
        );
    }

    intersectAll<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (df, EX, DF) => DF.intersect(
                df,
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                { all: true }
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
        );
    }

    except<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (df, EX, DF) => DF.except(
                df,
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX)
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
        );
    }

    exceptAll<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<S, R, E, G, RCDF, RCEX>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<S, RCDF, RCEX>(
            (df, EX, DF) => DF.except(
                df,
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                { all: true }
            ),
            schema => schemaDefsEqual(schema, right.runtimeSchema) ? schema : undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
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
                            : EX.lit(x);

                const coalesced = EX.coalesce(exprs.map(toE));
                return DF.withColumn(df, nameOrPartitions, coalesced);
            },
            () => undefined,
        );
    }

    describe<K extends ColumnName<S>>(
        colNames: readonly K[],
    ): ReadChainedDataFrame<StatisticsSchema<S, K>, R, E, G, CDF, CEX> {
        const names = colNames as readonly string[];
        assertStatisticsColumns("describe", names);
        return this.chainAs<StatisticsSchema<S, K>>(
            (df, EX, DF) => buildDescribePlan(DF, EX, df, names),
            schema => statisticsRuntimeSchema(schema, names),
        );
    }

    summary<K extends ColumnName<S>>(
        metrics: readonly string[] | undefined,
        colNames: readonly K[],
    ): ReadChainedDataFrame<StatisticsSchema<S, K>, R, E, G, CDF, CEX> {
        const names = colNames as readonly string[];
        assertStatisticsColumns("summary", names);
        return this.chainAs<StatisticsSchema<S, K>>(
            (df, EX, DF) => buildSummaryPlan(DF, EX, df, metrics, names),
            schema => statisticsRuntimeSchema(schema, names),
        );
    }

    /**
     * `repartition(n)` round-robins into `n` partitions (`shuffle = false`
     * coalesces instead). Given columns, Spark hash-partitions by them,
     * into `n` partitions when a count comes first.
     */
    repartition(numPartitions: number, shuffle?: boolean): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionCap<R>, CEX>;
    repartition(
        numPartitions: number,
        ...cols: [ColumnName<S> | EBuilder, ...Array<ColumnName<S> | EBuilder>]
    ): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionByExpressionCap<R, E>, CEX>;
    repartition(
        ...cols: [ColumnName<S> | EBuilder, ...Array<ColumnName<S> | EBuilder>]
    ): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionByExpressionCap<R, E>, CEX>;
    repartition(
        ...args: Array<number | boolean | ColumnName<S> | EBuilder | undefined>
    ): ReadChainedDataFrame<S, R, E, G, any, CEX> {
        const [first, ...rest] = args;
        if (typeof first === "number" && (rest[0] === undefined || typeof rest[0] === "boolean")) {
            if (rest.length > 1) throw new TypeError("repartition(numPartitions, shuffle) takes no columns.");
            const shuffle = rest[0] !== false;
            assertInteger("repartition() numPartitions", first, 1);
            return this.chain<RepartitionCap<R>>((df, _EX, DF) => DF.repartition(df, first, shuffle));
        }
        const numPartitions = typeof first === "number" ? first : undefined;
        const cols = (numPartitions === undefined ? args : rest) as unknown[];
        if (numPartitions !== undefined) assertInteger("repartition() numPartitions", numPartitions, 1);
        if (cols.length === 0) throw new TypeError("repartition() requires a partition count or columns.");
        if (cols.some(input => typeof input === "number" || typeof input === "boolean" || typeof input === "function")) {
            throw new TypeError("repartition() columns must be names or column expressions.");
        }
        assertPartitionInputs("repartition()", cols);
        return this.chain<RepartitionByExpressionCap<R, E>>((df, EX, DF) =>
            DF.repartitionByExpression(
                df,
                (cols as ColumnInput[]).map(input => columnExpression(input, EX)),
                numPartitions,
                false,
            )
        );
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

    /**
     * Applies `func` to this DataFrame and returns its result, so reusable
     * pipeline steps chain like PySpark's `DataFrame.transform`:
     * `df.transform(withTotals).transform(onlyActive)`.
     */
    transform<Out>(func: (df: this) => Out): Out {
        if (typeof func !== "function") throw new TypeError("transform() takes a function.");
        return func(this);
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
            this.observations,
            this.checkpointRelationId,
        );
    }

    /**
     * Runs the program against a concrete interpreter pair.
     *
     * `prog` is written generically over the algebra (it works for any R/E/G),
     * but this class fixes R/E/G as type parameters. Each concrete interpreter
     * (Spark logical plan, proto, trace) carries its own representation types,
     * so feeding one in requires a single representation-boundary cast. Keeping
     * it here documents why the cast is sound: the
     * program never inspects R/E/G, it only forwards algebra calls.
     */
    private interpretWith<Out>(DF: unknown, EX: unknown): Out {
        return (this.prog as unknown as (df: unknown, ex: unknown) => Out)(DF, EX);
    }

    private compileToSparkPlan(): LogicalPlan {
        return this.interpretWith<LogicalPlan>(SparkDFAlg, SparkExprAlg);
    }

    /**
     * Executes the plan and returns the raw ExecutePlan responses. `options`
     * cancels or tags this execution only; see `ExecutionOptions`.
     */
    async collectRaw(options?: ExecutionOptions): Promise<unknown[]> {
        const execution = this.execution(options);
        const root = this.interpretWith<ProtoRel>(ProtoDFAlg, ProtoExprAlg);
        return ProtoExec.collect(root, this.session, execution);
    }

    /**
     * Streams raw Spark Connect ExecutePlan responses with bounded buffering.
     * Session tags are read when iteration starts.
     */
    async *streamRaw(options?: ExecutionOptions): AsyncGenerator<Record<string, unknown>, void, void> {
        const execution = this.execution(options);
        const root = this.interpretWith<ProtoRel>(ProtoDFAlg, ProtoExprAlg);
        const executor = await applyPendingAnalyzeActions(root, this.session);
        for await (const response of executor.stream(root, execution)) {
            yield response;
        }
    }

    /** Streams Arrow IPC batches as they arrive from Spark. */
    async *toArrowBatches(options?: ExecutionOptions): AsyncGenerator<Buffer, void, void> {
        for await (const response of this.streamRaw(options)) {
            for (const buffer of arrowBuffersFromResponses([response])) yield buffer;
        }
    }

    /** Streams decoded rows without collecting the entire DataFrame in memory. */
    async *toRows(options?: ExecutionOptions): AsyncGenerator<RowOf<S>, void, void> {
        for await (const buffer of this.toArrowBatches(options)) {
            for (const row of rowsFromArrowBuffers<RowOf<S>>([buffer], this.runtimeSchema)) yield row;
        }
    }

    /**
     * Executes the plan and decodes Spark's Arrow batches into row objects.
     * `options` cancels or tags this execution only; see `ExecutionOptions`.
     */
    async collect(options?: ExecutionOptions): Promise<Array<RowOf<S>>> {
        const responses = await this.collectRaw(options);
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
        const root = this.interpretWith<ProtoRel>(ProtoDFAlg, ProtoExprAlg);
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

        const dfProgram = this.getProgram() as unknown as DFProgram<R, E, G, CDFBatch, CEX>;
        const prog: BatchWProgram<R, E, G, CDFBatch, CEX> =
            (WR, DF, EX) => WR.fromChild(dfProgram(DF, EX));

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
            dfProgram,
            wProgram,
            observations: this.observations,
        });
    }

    private compileTrace(): TraceNode {
        return this.interpretWith<TraceNode>(TraceDFAlg, TraceExprAlg);
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
        // The logical-plan AST keeps bigint literals as-is; JSON has no bigint.
        return JSON.stringify(lp, (_key, value) => (typeof value === "bigint" ? value.toString() : value), 2);
    }

    toProtoJSON(): string {
        const protoRoot = this.interpretWith<unknown>(ProtoDFAlg, ProtoExprAlg);
        // LocalRelation carries its rows as Arrow IPC bytes; summarise them
        // instead of flooding the inspection output with a byte-per-line dump.
        return JSON.stringify(
            protoRoot,
            (_key, value) => (value instanceof Uint8Array ? `<${value.byteLength} bytes of Arrow IPC data>` : value),
            2,
        );
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

    // ---------------------------------------------------------------------
    // Null handling and statistics (PySpark `df.na` / `df.stat`)
    // ---------------------------------------------------------------------

    /** Null-value functions, mirroring PySpark's `df.na`. */
    get na(): DataFrameNaFunctions<S, R, E, G, CDF, CEX> {
        return new DataFrameNaFunctions(this);
    }

    /** Statistic functions, mirroring PySpark's `df.stat`. */
    get stat(): DataFrameStatFunctions<S, R, E, G, CDF, CEX> {
        return new DataFrameStatFunctions(this);
    }

    /**
     * Replaces nulls. A single value applies to every column of a compatible
     * type (or to `subset`); a mapping fills each named column with its own
     * value. The schema is unchanged.
     */
    fillna(
        value: NAFillValue | Readonly<Record<string, NAFillValue>>,
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        const { columns, values } = fillSpec(value, subset);
        return this.chain((df, _EX, DF) => DF.fillNa(df, columns, values));
    }

    /**
     * Drops rows with nulls (or NaN). `how = "any"` (default) drops a row with
     * any null in the considered columns, `"all"` only rows where all of them
     * are null; `thresh` keeps rows with at least that many non-null values
     * and takes precedence over `how`.
     */
    dropna(
        how?: NAHow,
        thresh?: number,
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        const minNonNulls = dropNaMinimum(how, thresh);
        const columns = columnList("dropna()", subset);
        return this.chain((df, _EX, DF) => DF.dropNa(df, columns, minNonNulls));
    }

    /**
     * Replaces values: `replace(old, new)`, `replace([olds], [news] | new)` or
     * `replace(mapping)`, optionally restricted to `subset`. Numbers compare as
     * doubles; value kinds cannot be mixed.
     */
    replace(
        toReplace: NAReplaceValue | readonly NAReplaceValue[],
        value: NAReplaceValue | readonly NAReplaceValue[],
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    replace(
        mapping: Readonly<Record<string, NAReplaceValue>> | ReadonlyMap<NAReplaceValue, NAReplaceValue>,
        subset?: ColumnName<S> | readonly ColumnName<S>[],
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    replace(...args: unknown[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        const { columns, replacements } = replaceSpec(args);
        return this.chain((df, _EX, DF) => DF.replaceNa(df, columns, replacements));
    }

    /** Pearson correlation of two numeric columns. */
    async corr(col1: ColumnName<S>, col2: ColumnName<S>, method: "pearson" = "pearson"): Promise<number> {
        if (method !== "pearson") {
            throw new RangeError(`corr() supports only the "pearson" method; received ${String(method)}.`);
        }
        assertNonEmptyString("corr() col1", col1);
        assertNonEmptyString("corr() col2", col2);
        return this.scalarDouble("corr()", (df, DF) => DF.corr(df, col1, col2, method));
    }

    /** Sample covariance of two numeric columns. */
    async cov(col1: ColumnName<S>, col2: ColumnName<S>): Promise<number> {
        assertNonEmptyString("cov() col1", col1);
        assertNonEmptyString("cov() col2", col2);
        return this.scalarDouble("cov()", (df, DF) => DF.cov(df, col1, col2));
    }

    /** Pair-wise frequency table: one row per `col1` value, one column per `col2` value. */
    crosstab(col1: ColumnName<S>, col2: ColumnName<S>): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        assertNonEmptyString("crosstab() col1", col1);
        assertNonEmptyString("crosstab() col2", col2);
        return this.chainAs<UnknownSchema>((df, _EX, DF) => DF.crosstab(df, col1, col2), () => undefined);
    }

    /** Frequent items per column (one row of `<column>_freqItems` arrays). */
    freqItems(cols: readonly ColumnName<S>[], support?: number): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        const columns = columnList("freqItems()", cols);
        if (columns.length === 0) throw new TypeError("freqItems() requires at least one column.");
        assertSupport(support);
        return this.chainAs<UnknownSchema>((df, _EX, DF) => DF.freqItems(df, columns, support), () => undefined);
    }

    /**
     * Approximate quantiles (Greenwald-Khanna). One column returns one value
     * per probability; several columns return one such list per column.
     */
    async approxQuantile(col: ColumnName<S>, probabilities: readonly number[], relativeError: number): Promise<number[]>;
    async approxQuantile(
        cols: readonly ColumnName<S>[],
        probabilities: readonly number[],
        relativeError: number,
    ): Promise<number[][]>;
    async approxQuantile(
        cols: ColumnName<S> | readonly ColumnName<S>[],
        probabilities: readonly number[],
        relativeError: number,
    ): Promise<number[] | number[][]> {
        const columns = columnList("approxQuantile()", cols);
        if (columns.length === 0) throw new TypeError("approxQuantile() requires at least one column.");
        assertQuantileArguments(probabilities, relativeError);
        const values = await this.scalarValue(
            "approxQuantile()",
            (df, DF) => DF.approxQuantile(df, columns, [...probabilities], relativeError),
        );
        if (!Array.isArray(values) || values.some(row => !Array.isArray(row))) {
            throw new TypeError("approxQuantile() expected a list of quantile lists from Spark.");
        }
        const quantiles = (values as unknown[][]).map(row => row.map(Number));
        return typeof cols === "string" ? quantiles[0]! : quantiles;
    }

    /**
     * Stratified sample without replacement. `fractions` maps each stratum of
     * `col` to the fraction of its rows to keep; missing strata keep none.
     */
    sampleBy(
        col: ColumnName<S> | EBuilder,
        fractions: Readonly<Record<string, number>> | ReadonlyMap<LiteralValue, number>,
        seed?: number,
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        if (typeof col === "string") assertNonEmptyString("sampleBy() column", col);
        if (!(fractions instanceof Map) && typeof col === "string") {
            // Object keys are strings, and Spark matches strata by value and
            // type, so a numeric column would silently sample nothing.
            const spec = this.runtimeSchema?.[col];
            if (spec !== undefined && spec !== "string" && spec !== "string?") {
                throw new TypeError(
                    `sampleBy() strata for the non-string column ${JSON.stringify(col)} must be given as a Map.`
                );
            }
        }
        const strata = sampleFractions(fractions);
        assertOptionalSeed("sampleBy()", seed);
        const resolvedSeed = seed ?? freshSparkSeed();
        return this.chain((df, EX, DF) =>
            DF.sampleBy(df, typeof col === "string" ? EX.col(col) : col.build(EX), strata, resolvedSeed)
        );
    }

    // ---------------------------------------------------------------------
    // Reshaping and row-position relations
    // ---------------------------------------------------------------------

    /**
     * Unpivots `values` (every non-id column when omitted) into
     * `variableColumnName` / `valueColumnName` rows, keeping `ids`.
     */
    unpivot(
        ids: ColumnName<S> | EBuilder | ReadonlyArray<ColumnName<S> | EBuilder>,
        values: ColumnName<S> | EBuilder | ReadonlyArray<ColumnName<S> | EBuilder> | undefined,
        variableColumnName: string,
        valueColumnName: string,
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        assertNonEmptyString("unpivot() variableColumnName", variableColumnName);
        assertNonEmptyString("unpivot() valueColumnName", valueColumnName);
        const idList = toColumnInputs(ids);
        const valueList = values === undefined ? undefined : toColumnInputs(values);
        if (valueList?.length === 0) throw new TypeError("unpivot() values must not be empty; omit them instead.");
        return this.chainAs<UnknownSchema>(
            (df, EX, DF) => DF.unpivot(
                df,
                idList.map(input => columnExpression(input, EX)),
                valueList?.map(input => columnExpression(input, EX)),
                variableColumnName,
                valueColumnName,
            ),
            () => undefined,
        );
    }

    /** Alias of `unpivot`, as in PySpark. */
    melt(
        ids: ColumnName<S> | EBuilder | ReadonlyArray<ColumnName<S> | EBuilder>,
        values: ColumnName<S> | EBuilder | ReadonlyArray<ColumnName<S> | EBuilder> | undefined,
        variableColumnName: string,
        valueColumnName: string,
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        return this.unpivot(ids, values, variableColumnName, valueColumnName);
    }

    /** Renames every column positionally; Spark requires one name per column. */
    toDF(...names: string[]): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        for (const name of names) assertNonEmptyString("toDF() column", name);
        assertUniqueColumnNames("toDF()", names);
        return this.chainAs<UnknownSchema>((df, _EX, DF) => DF.toDF(df, names), () => undefined);
    }

    /**
     * Names this DataFrame for qualified references, e.g. a self-join on
     * `col("a.id").eq(col("b.id"))`. The schema is unchanged.
     */
    alias(name: string): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        assertNonEmptyString("alias()", name);
        return this.chain((df, _EX, DF) => DF.subqueryAlias(df, name));
    }

    /** Skips the first `n` rows. */
    offset(n: number): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        assertInteger("offset()", n, 0);
        return this.chain((df, _EX, DF) => DF.offset(df, n));
    }

    /**
     * Transposes rows and columns around `indexColumn` (the first column when
     * omitted). Every other column must share a type.
     */
    transpose(indexColumn?: ColumnName<S> | EBuilder): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX> {
        if (typeof indexColumn === "string") assertNonEmptyString("transpose() indexColumn", indexColumn);
        return this.chainAs<UnknownSchema>(
            (df, EX, DF) => DF.transpose(
                df,
                indexColumn === undefined ? [] : [columnExpression(indexColumn, EX)],
            ),
            () => undefined,
        );
    }

    /** Adds or replaces several columns in one projection. */
    withColumns<const M extends Readonly<Record<string, TypedColumn<ColumnType, E>>>>(
        build: TypedColumnsFactory<S, M, E>,
    ): ReadChainedDataFrame<AddedColumnsSchema<S, M>, R, E, G, CDF, CEX>;
    withColumns(columns: Readonly<Record<string, EBuilder>>): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>;
    withColumns(
        input: Readonly<Record<string, EBuilder>> | TypedColumnsFactory<S, Readonly<Record<string, TypedColumn<ColumnType, E>>>, E>,
    ): ReadChainedDataFrame<any, R, E, G, CDF, CEX> {
        const eager = typeof input === "function" ? undefined : Object.keys(input);
        if (eager) {
            if (eager.length === 0) throw new TypeError("withColumns() requires at least one column.");
            for (const name of eager) assertNonEmptyString("withColumns() column", name);
        }
        return this.chainAs<any>(
            (df, EX, DF) => {
                const columns = typeof input === "function"
                    ? (input as (columns: Columns<Extract<S, SchemaShape>, E>) => Record<string, EBuilder>)(
                        makeColumns<Extract<S, SchemaShape>, E>()
                    )
                    : input;
                const entries = Object.entries(columns);
                if (entries.length === 0) throw new TypeError("withColumns() requires at least one column.");
                return DF.withColumns(df, entries.map(([name, column]) => {
                    assertNonEmptyString("withColumns() column", name);
                    return { name, column: column.build(EX) };
                }));
            },
            () => undefined,
        );
    }

    /** Cartesian product with `right`. */
    crossJoin<RS, RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
    ): ReadChainedDataFrame<CrossJoinSchema<S, RS>, R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chainAs<CrossJoinSchema<S, RS>, RCDF, RCEX>(
            (df, EX, DF) => DF.crossJoin(
                df,
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
            ),
            schema => joinRuntimeSchema(schema, right.runtimeSchema, "CROSS"),
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
        );
    }

    /**
     * Range-partitions by `cols` (ascending unless given as sort keys such as
     * `col("x").desc()`), optionally into `numPartitions` partitions.
     */
    repartitionByRange(
        numPartitions: number,
        ...cols: Array<ColumnName<S> | EBuilder | SortKeyBuilder>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionByExpressionCap<R, E>, CEX>;
    repartitionByRange(
        ...cols: Array<ColumnName<S> | EBuilder | SortKeyBuilder>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionByExpressionCap<R, E>, CEX>;
    repartitionByRange(
        ...args: Array<number | ColumnName<S> | EBuilder | SortKeyBuilder>
    ): ReadChainedDataFrame<S, R, E, G, CDF & RepartitionByExpressionCap<R, E>, CEX> {
        const numPartitions = typeof args[0] === "number" ? args[0] : undefined;
        const cols = (numPartitions === undefined ? args : args.slice(1)) as unknown[];
        if (numPartitions !== undefined) assertInteger("repartitionByRange() numPartitions", numPartitions, 1);
        if (cols.length === 0) throw new TypeError("repartitionByRange() requires at least one column.");
        assertPartitionInputs("repartitionByRange()", cols);
        return this.chain<RepartitionByExpressionCap<R, E>>((df, EX, DF) =>
            DF.repartitionByExpression(
                df,
                cols.map(input => {
                    const order = resolveOrderInput(input, EX);
                    return EX.sortKey(order.expr, order.direction, order.nulls);
                }),
                numPartitions,
                true,
            )
        );
    }

    // ---------------------------------------------------------------------
    // Grouping variants, partition-local sorting, watermarks, joins, metrics
    // ---------------------------------------------------------------------

    /**
     * Groups by every prefix of `by` (subtotals and a grand total). The key
     * columns become nullable, so the result is untyped.
     */
    rollup(...by: Array<ColumnName<S> | EBuilder>): GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX> {
        return this.grouped("rollup()", by, { groupType: "rollup" });
    }

    /** Groups by every combination of `by`. The result is untyped, like `rollup`. */
    cube(...by: Array<ColumnName<S> | EBuilder>): GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX> {
        return this.grouped("cube()", by, { groupType: "cube" });
    }

    /**
     * Groups by the given grouping sets (`GROUP BY GROUPING SETS`). `by` lists
     * the columns the sets draw from, in output order.
     */
    groupingSets(
        sets: ReadonlyArray<ReadonlyArray<ColumnName<S> | EBuilder>>,
        ...by: Array<ColumnName<S> | EBuilder>
    ): GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX> {
        if (!Array.isArray(sets) || sets.length === 0) {
            throw new TypeError("groupingSets() requires at least one grouping set.");
        }
        for (const set of sets) {
            if (!Array.isArray(set)) throw new TypeError("groupingSets() sets must be arrays of columns.");
            for (const column of set) {
                if (typeof column === "string") assertNonEmptyString("groupingSets() column", column);
            }
        }
        return this.grouped("groupingSets()", by, {
            groupType: "grouping_sets",
            groupingSets: sets.map(set => [...set]),
        });
    }

    private grouped(
        label: string,
        by: ReadonlyArray<string | EBuilder>,
        spec: GroupingInput,
    ): GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX> {
        if (by.every((column): column is string => typeof column === "string")) {
            assertUniqueColumnNames(label, by);
        }
        return new GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX>(
            this as unknown as ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>,
            by,
            spec,
        );
    }

    /** Sorts each partition on its own, without a global shuffle. */
    sortWithinPartitions(
        ...colsOrKeys: Array<ColumnName<S> | EBuilder | TypedOrderFactory<S, E> | ((EX: ExprAlg<any>) => SortOrder<any>)>
    ): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        if (colsOrKeys.length === 0) throw new TypeError("sortWithinPartitions() requires at least one column.");
        return this.chain((df, EX, DF) =>
            DF.sortWithinPartitions(df, colsOrKeys.map(k => resolveOrderInput(k, EX)))
        );
    }

    /**
     * `dropDuplicates` for streaming DataFrames with a watermark: duplicates
     * that arrive within the watermark delay are dropped as well.
     */
    dropDuplicatesWithinWatermark<K extends ColumnName<S>>(...cols: K[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    dropDuplicatesWithinWatermark(...cols: EBuilder[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX>;
    dropDuplicatesWithinWatermark(...cols: (string | EBuilder)[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) =>
            DF.dropDuplicatesWithinWatermark(
                df,
                cols.length === 0 ? undefined : cols.map(c => (typeof c === "string" ? EX.col(c) : c.build(EX))),
            )
        );
    }

    /**
     * Every column whose name matches `regex`, for `select`. Spark expects the
     * regex between backticks: `` df.colRegex("`(id|name)`") ``.
     */
    colRegex(regex: string): EBuilder {
        assertNonEmptyString("colRegex() regex", regex);
        return { build: EX => EX.colRegex(regex) };
    }

    /** A metadata column of a file source, such as `_metadata`. */
    metadataColumn(name: string): EBuilder {
        assertNonEmptyString("metadataColumn() name", name);
        return { build: EX => EX.metadataCol(name) };
    }

    /**
     * Lateral join: `right` may reference this DataFrame's columns. Spark
     * supports inner, cross and left outer lateral joins.
     */
    lateralJoin<RS, RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
        on?: EBuilder,
        how: JoinTypeInput = "INNER",
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF & RCDF, CEX & RCEX> {
        if (!LATERAL_JOIN_TYPES.has(String(how).toUpperCase())) {
            throw new RangeError(`lateralJoin() supports inner, cross and left outer joins; received ${String(how)}.`);
        }
        return this.chainAs<UnknownSchema, RCDF, RCEX>(
            (df, EX, DF) => DF.lateralJoin(
                df,
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                on?.build(EX),
                how,
            ),
            () => undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
        );
    }

    /**
     * As-of join (PySpark `_joinAsOf`, Scala `joinAsOf`): every left row joins
     * the right row whose `rightAsOfColumn` is closest to its
     * `leftAsOfColumn`, looking `backward` by default. `on` is a column name,
     * a list of names shared by both sides, or a join expression.
     */
    asOfJoin<RS, RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>,
        leftAsOfColumn: string | EBuilder,
        rightAsOfColumn: string | EBuilder,
        on?: string | readonly string[] | EBuilder,
        how = "inner",
        options: AsOfJoinOptions = {},
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF & RCDF, CEX & RCEX> {
        if (typeof leftAsOfColumn === "string") assertNonEmptyString("asOfJoin() leftAsOfColumn", leftAsOfColumn);
        if (typeof rightAsOfColumn === "string") assertNonEmptyString("asOfJoin() rightAsOfColumn", rightAsOfColumn);
        assertNonEmptyString("asOfJoin() how", how);
        const direction = options.direction ?? "backward";
        if (!AS_OF_DIRECTIONS.has(direction)) {
            throw new RangeError(`asOfJoin() direction must be backward, forward or nearest; received ${String(direction)}.`);
        }
        const allowExactMatches = options.allowExactMatches ?? true;
        if (typeof allowExactMatches !== "boolean") {
            throw new TypeError("asOfJoin() allowExactMatches must be a boolean.");
        }
        const usingColumns = typeof on === "string"
            ? [on]
            : Array.isArray(on) ? [...(on as readonly string[])] : undefined;
        if (usingColumns !== undefined) {
            if (usingColumns.length === 0) throw new TypeError("asOfJoin() on must name at least one column.");
            for (const column of usingColumns) assertNonEmptyString("asOfJoin() on column", column);
        }
        const tolerance = options.tolerance;
        if (typeof tolerance === "number" && !Number.isFinite(tolerance)) {
            throw new RangeError("asOfJoin() tolerance must be finite.");
        }
        return this.chainAs<UnknownSchema, RCDF, RCEX>(
            (df, EX, DF) => {
                const toExpr = (column: string | EBuilder) =>
                    typeof column === "string" ? EX.col(column) : column.build(EX);
                return DF.asOfJoin(df, right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX), {
                    leftAsOf: toExpr(leftAsOfColumn),
                    rightAsOf: toExpr(rightAsOfColumn),
                    ...(usingColumns !== undefined
                        ? { usingColumns }
                        : on === undefined ? {} : { on: (on as EBuilder).build(EX) }),
                    joinType: how,
                    ...(tolerance === undefined
                        ? {}
                        : { tolerance: typeof tolerance === "object" ? tolerance.build(EX) : EX.lit(tolerance) }),
                    allowExactMatches,
                    direction,
                });
            },
            () => undefined,
            this.streaming || right.streaming,
            [...this.observations, ...right.observations],
        );
    }

    /**
     * Collects named aggregate metrics while an action runs, without a second
     * pass over the data. With an `Observation`, the values are available
     * from `observation.get()` after any action on this DataFrame or one
     * derived from it (`collect`, `toRows`, `save`, `writeTo`, `mergeInto`,
     * ...); with a plain name they only reach query listeners and the UI, as
     * in PySpark. Streaming DataFrames report metrics to listeners only.
     */
    observe(observation: Observation | string, ...metrics: EBuilder[]): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        if (this.streaming) {
            throw new TypeError(
                "observe() is not available on a streaming DataFrame: Spark reports its metrics to streaming query listeners only."
            );
        }
        if (metrics.length === 0) throw new TypeError("observe() requires at least one metric expression.");
        const name = typeof observation === "string" ? observation : observation.name;
        assertNonEmptyString("observe() name", name);
        return this.chainAs<S>(
            (df, EX, DF) => DF.observe(df, name, metrics.map(metric => metric.build(EX))),
            schema => schema,
            this.streaming,
            typeof observation === "string" ? this.observations : [...this.observations, observation],
        );
    }

    /**
     * Reconciles every row to `schema`: columns are reordered and cast,
     * missing nullable columns are filled with null. A declared schema types
     * the result; a DDL string such as `"id INT, name STRING"` leaves it untyped.
     */
    to<D extends SchemaDef>(declared: DeclaredSchema<D>): ReadChainedDataFrame<InferSchema<D>, R, E, G, CDF, CEX>;
    to(ddl: string): ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>;
    to(target: DeclaredSchema<SchemaDef> | string): ReadChainedDataFrame<any, R, E, G, CDF, CEX> {
        if (typeof target === "string") {
            assertNonEmptyString("to() schema", target);
            return this.chainAs<UnknownSchema>((df, _EX, DF) => DF.toSchema(df, target), () => undefined);
        }
        if (!(target instanceof DeclaredSchema)) {
            throw new TypeError("to() requires a DDL string or a schema declared with schema({...}).");
        }
        const ddl = declareSchema(nullableFields(target.def)).toDDL();
        return this.chainAs<any>((df, _EX, DF) => DF.toSchema(df, ddl), () => target.def);
    }

    /** Attaches column metadata (a JSON object) to `column`, keeping its values. */
    withMetadata(column: ColumnName<S>, metadata: Readonly<Record<string, unknown>> | string): ReadChainedDataFrame<S, R, E, G, CDF, CEX> {
        assertNonEmptyString("withMetadata() column", column);
        const json = typeof metadata === "string" ? metadata : JSON.stringify(metadata);
        let parsed: unknown;
        try {
            parsed = JSON.parse(json);
        } catch {
            throw new TypeError("withMetadata() metadata must be a JSON object.");
        }
        if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
            throw new TypeError("withMetadata() metadata must be a JSON object.");
        }
        return this.chain((df, EX, DF) => DF.withMetadata(df, column, EX.col(column), json));
    }

    /** One `value` string column holding each row as a JSON document. */
    toJSON(): ReadChainedDataFrame<{ value: string }, R, E, G, CDF, CEX> {
        return this.chainAs<{ value: string }>(
            (df, EX, DF) => DF.select(df, [EX.alias(EX.call("to_json", [EX.call("struct", [EX.star()])]), "value")]),
            () => ({ value: "string" }),
        );
    }

    // ---------------------------------------------------------------------
    // Actions
    // ---------------------------------------------------------------------

    /** Number of rows, as Spark's `long` (a `bigint`, like every long column). */
    async count(): Promise<bigint> {
        const rows = await this.chainAs<UnknownSchema>(
            (df, EX, DF) => DF.agg(DF.groupBy(df, []), { count: EX.call("count", [EX.lit(1)]) }),
            () => undefined,
        ).collect();
        const value = rows[0]?.count;
        if (typeof value !== "bigint") {
            throw new TypeError(`count() expected a long result from Spark but received ${typeof value}.`);
        }
        return value;
    }

    /** First row, or `undefined` when the DataFrame is empty. */
    async first(): Promise<RowOf<S> | undefined> {
        return (await this.take(1))[0];
    }

    /** `head()` is `first()`; `head(n)` is `take(n)`. */
    async head(): Promise<RowOf<S> | undefined>;
    async head(n: number): Promise<Array<RowOf<S>>>;
    async head(n?: number): Promise<RowOf<S> | undefined | Array<RowOf<S>>> {
        return n === undefined ? this.first() : this.take(n);
    }

    /** At most the first `n` rows. */
    async take(n: number): Promise<Array<RowOf<S>>> {
        return this.limit(n).collect();
    }

    /** At most the last `n` rows, in order. */
    async tail(n: number): Promise<Array<RowOf<S>>> {
        assertInteger("tail()", n, 0);
        return this.chain((df, _EX, DF) => DF.tail(df, n)).collect();
    }

    /** Whether the DataFrame has no rows; only one row is fetched. */
    async isEmpty(): Promise<boolean> {
        return (await this.take(1)).length === 0;
    }

    // ---------------------------------------------------------------------
    // Server-side introspection (AnalyzePlan)
    // ---------------------------------------------------------------------

    /** The resolved schema as reported by Spark. */
    async schema(): Promise<SparkStructType> {
        return sparkStructType((await this.analyze("schema")).schema);
    }

    /** Column names, in order. */
    async columns(): Promise<string[]> {
        return (await this.schema()).fields.map(field => field.name);
    }

    /** `[name, type]` pairs, with Spark's simple type strings. */
    async dtypes(): Promise<Array<[string, string]>> {
        return (await this.schema()).fields.map(field => [field.name, field.dataType]);
    }

    /** Spark's schema tree (`root\n |-- ...`), optionally limited to `level` nesting levels. */
    async treeString(level?: number): Promise<string> {
        if (level !== undefined) assertInteger("treeString() level", level, 1);
        const result = await this.analyze("tree_string", level === undefined ? {} : { level });
        return String(result.tree_string);
    }

    /** Prints the schema tree to stdout. */
    async printSchema(level?: number): Promise<void> {
        console.log(await this.treeString(level));
    }

    /** Whether `collect()` can run locally without executors. */
    async isLocal(): Promise<boolean> {
        return (await this.analyze("is_local")).is_local === true;
    }

    /** Whether the plan reads from a streaming source. */
    async isStreaming(): Promise<boolean> {
        return (await this.analyze("is_streaming")).is_streaming === true;
    }

    /** Best-effort list of the files that make up this DataFrame. */
    async inputFiles(): Promise<string[]> {
        const files = (await this.analyze("input_files")).files;
        return Array.isArray(files) ? files.map(String) : [];
    }

    /** Whether both logical plans compute the same result. */
    async sameSemantics<RS, RCDF, RCEX>(other: ReadChainedDataFrame<RS, R, E, G, RCDF, RCEX>): Promise<boolean> {
        const result = await this.analyzeWith("same_semantics", root => ({
            target_plan: { root },
            other_plan: { root: other.interpretWith<ProtoRel>(ProtoDFAlg, ProtoExprAlg) },
        }));
        return result.result === true;
    }

    /** Hash of the logical plan; equal for semantically equal plans. */
    async semanticHash(): Promise<number> {
        return Number((await this.analyze("semantic_hash")).result);
    }

    /** The storage level the plan is cached with (`NONE` when it is not). */
    async storageLevel(): Promise<StorageLevel> {
        const result = await this.analyzeWith("get_storage_level", root => ({ relation: root }));
        return storageLevelFromProto(result.storage_level);
    }

    // ---------------------------------------------------------------------
    // Checkpoints and views
    // ---------------------------------------------------------------------

    /**
     * Materializes the plan on the server (a reliable checkpoint, written to
     * the checkpoint directory) and returns a DataFrame that reads it back.
     * Spark keeps the data until the session closes, which releases it.
     */
    checkpoint(eager = true): Promise<ReadChainedDataFrame<S, R, E, G, CDF, CEX>> {
        return this.checkpointWith("checkpoint()", false, eager);
    }

    /**
     * Like `checkpoint` but stored with the executors (`storageLevel`
     * defaults to Spark's `MEMORY_AND_DISK`): faster and not fault tolerant.
     */
    localCheckpoint(eager = true, storageLevel?: string): Promise<ReadChainedDataFrame<S, R, E, G, CDF, CEX>> {
        return this.checkpointWith("localCheckpoint()", true, eager, storageLevel);
    }

    private async checkpointWith(
        label: string,
        local: boolean,
        eager: boolean,
        storageLevel?: string,
    ): Promise<ReadChainedDataFrame<S, R, E, G, CDF, CEX>> {
        if (this.streaming) throw new Error(`${label} is not available on a streaming DataFrame.`);
        if (typeof eager !== "boolean") throw new TypeError(`${label} eager must be a boolean.`);
        const level = storageLevel === undefined ? undefined : toProtoStorageLevel(storageLevel);
        const root = this.toProtoRelation();
        const executor = await applyPendingAnalyzeActions(root, this.session);
        const responses = await executor.runCommand({
            checkpoint_command: {
                relation: root,
                local,
                eager,
                ...(level === undefined ? {} : { storage_level: level }),
            },
        });
        const relationId = checkpointRelationId(responses);
        if (relationId === undefined) {
            throw new Error("Invalid CheckpointCommand response: missing the cached relation id.");
        }
        this.session.trackCachedRelation(relationId);
        return new ReadChainedDataFrame<S, R, E, G, CDF, CEX>(
            DF => DF.cachedRemoteRelation(relationId),
            this.session,
            false,
            this.runtimeSchema,
            [],
            relationId,
        );
    }

    /**
     * Frees the server-side data behind the DataFrame `checkpoint()` or
     * `localCheckpoint()` returned. Idempotent; `close()` releases whatever
     * was not released explicitly.
     */
    async releaseCheckpoint(): Promise<void> {
        const relationId = this.checkpointRelationId;
        if (relationId === undefined) {
            throw new TypeError("releaseCheckpoint() applies to the DataFrame returned by checkpoint() or localCheckpoint().");
        }
        if (!this.session.untrackCachedRelation(relationId)) return;
        try {
            await SparkConnectExecutor.for(this.session).runCommand({
                remove_cached_remote_relation_command: { relation: { relation_id: relationId } },
            });
        } catch (error) {
            this.session.trackCachedRelation(relationId);
            throw error;
        }
    }

    /**
     * `write()` for a DataFrame the `this:` guards above already proved to be
     * batch; the cast keeps the nested `Exclude` out of the public signatures.
     */
    private batchWriter(): DataFrameWriterTF<R, E, G, WBatch, any, CEX, BatchWriterAlg<R>> {
        return (this as unknown as ReadChainedDataFrame<S, R, E, G, any, CEX>).write();
    }

    /** Registers this DataFrame as a session temp view; fails if the name exists. */
    createTempView(this: ReadChainedDataFrame<S, R, E, G, Exclude<CDF, StreamingMark<R>>, CEX>, name: string): Promise<void> {
        return this.batchWriter().createTempView(name);
    }

    /** Registers or replaces this DataFrame as a session temp view. */
    createOrReplaceTempView(this: ReadChainedDataFrame<S, R, E, G, Exclude<CDF, StreamingMark<R>>, CEX>, name: string): Promise<void> {
        return this.batchWriter().createOrReplaceTempView(name);
    }

    /** Registers this DataFrame as `global_temp.<name>`, visible to every session; fails if it exists. */
    createGlobalTempView(this: ReadChainedDataFrame<S, R, E, G, Exclude<CDF, StreamingMark<R>>, CEX>, name: string): Promise<void> {
        return this.batchWriter().createGlobalTempView(name);
    }

    /** Registers or replaces `global_temp.<name>`. */
    createOrReplaceGlobalTempView(this: ReadChainedDataFrame<S, R, E, G, Exclude<CDF, StreamingMark<R>>, CEX>, name: string): Promise<void> {
        return this.batchWriter().createOrReplaceGlobalTempView(name);
    }

    // ---------------------------------------------------------------------
    // Table writes (DataFrameWriterV2 and MERGE INTO)
    // ---------------------------------------------------------------------

    /** Writes to a catalog table through Spark's DataFrameWriterV2. */
    writeTo(
        this: ReadChainedDataFrame<S, R, E, G, Exclude<CDF, StreamingMark<R>>, CEX>,
        table: string,
    ): DataFrameWriterV2 {
        if (this.streaming) {
            throw new Error("Cannot use writeTo() on a streaming DataFrame. Use writeStream() instead.");
        }
        return new DataFrameWriterV2(this.session, () => this.toProtoRelation(), table, this.observations);
    }

    /**
     * Merges this DataFrame (the source) into `table` on `condition`. Add
     * `whenMatched` / `whenNotMatched` / `whenNotMatchedBySource` clauses and
     * finish with `merge()`.
     */
    mergeInto(
        this: ReadChainedDataFrame<S, R, E, G, Exclude<CDF, StreamingMark<R>>, CEX>,
        table: string,
        condition: ColumnExpression,
    ): MergeIntoWriter {
        if (this.streaming) {
            throw new Error("Cannot use mergeInto() on a streaming DataFrame.");
        }
        return new MergeIntoWriter(this.session, () => this.toProtoRelation(), table, condition, this.observations);
    }

    private toProtoRelation(): ProtoRel {
        return this.interpretWith<ProtoRel>(ProtoDFAlg, ProtoExprAlg);
    }

    private analyze(field: AnalyzeField, extra: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
        return this.analyzeWith(field, root => ({ plan: { root }, ...extra }));
    }

    private async analyzeWith(
        field: AnalyzeField,
        payload: (root: ProtoRel) => Record<string, unknown>,
    ): Promise<Record<string, unknown>> {
        const root = this.toProtoRelation();
        const executor = await applyPendingAnalyzeActions(root, this.session);
        return executor.analyzePlan(field, payload(root));
    }

    /** Executes a single-row statistic relation and returns its only value. */
    private async scalarValue(
        label: string,
        step: (df: R, DF: DFAlg<R, E, G, CDF>) => R,
    ): Promise<unknown> {
        const rows = await this.chainAs<UnknownSchema>((df, _EX, DF) => step(df, DF), () => undefined).collect();
        const first = rows[0];
        if (first === undefined) throw new TypeError(`${label} expected one result row from Spark.`);
        return Object.values(first)[0];
    }

    private async scalarDouble(label: string, step: (df: R, DF: DFAlg<R, E, G, CDF>) => R): Promise<number> {
        const value = await this.scalarValue(label, step);
        if (typeof value !== "number") {
            throw new TypeError(`${label} expected a double result from Spark but received ${typeof value}.`);
        }
        return value;
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
        type StreamingCDF = CDF & StreamingMark<R>;
        const dfProgram = this.getProgram() as unknown as DFProgram<R, E, G, StreamingCDF, CEX>;
        const prog: StreamWProgram<R, E, G, StreamingCDF, CEX> =
            (WR, DF, EX) => WR.writeStream(dfProgram(DF, EX));
        return DataFrameWriterTF.fromParts<R, E, G, WStream, StreamingCDF, CEX, StreamWriterAlg<R>>({
            session: this.getSession(),
            dfProgram,
            wProgram: prog,
        });
    }
}

/**
 * Resultado intermedio de `groupBy`. Conserva el programa tagless-final y las
 * claves en el tipo; `agg` materializa recién al interpretar, de modo que los
 * builders tipados siguen siendo agnósticos de proto/trace/logical-plan.
 * @public
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
        private readonly spec?: GroupingInput,
    ) {}

    /**
     * Pivots `column` into one output column per value. Without `values`
     * Spark first runs a job to collect the distinct values; the result is
     * untyped because its columns depend on the data.
     */
    pivot(
        column: string | EBuilder,
        values?: readonly LiteralValue[],
    ): GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX> {
        if (this.spec !== undefined) {
            throw new TypeError("pivot() applies to a plain groupBy(), not to rollup, cube or grouping sets.");
        }
        if (typeof column === "string") assertNonEmptyString("pivot() column", column);
        if (values !== undefined && (!Array.isArray(values) || values.length === 0)) {
            throw new TypeError("pivot() values must be a non-empty array when given.");
        }
        return new GroupedDataFrameTF<UnknownSchema, string, R, E, G, CDF, CEX>(
            this.source as unknown as ReadChainedDataFrame<UnknownSchema, R, E, G, CDF, CEX>,
            this.grouping,
            { groupType: "pivot", pivot: { column, ...(values === undefined ? {} : { values: [...values] }) } },
        );
    }

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
            return { fn: match[1]!, arg: match[2]! };
        };

        const next: DFProgram<R, E, G, CDF, CEX> = (DF, EX) => {
            const keys = this.grouping.map(key =>
                typeof key === "string" ? EX.col(key) : key.build(EX)
            );
            const toExpr = (column: string | EBuilder) =>
                typeof column === "string" ? EX.col(column) : column.build(EX);
            const spec = this.spec === undefined
                ? undefined
                : {
                    groupType: this.spec.groupType,
                    ...(this.spec.pivot === undefined
                        ? {}
                        : {
                            pivot: {
                                column: toExpr(this.spec.pivot.column),
                                ...(this.spec.pivot.values === undefined ? {} : { values: this.spec.pivot.values }),
                            },
                        }),
                    ...(this.spec.groupingSets === undefined
                        ? {}
                        : { groupingSets: this.spec.groupingSets.map(set => set.map(toExpr)) }),
                };
            const grouped = DF.groupBy(this.source.getProgram()(DF, EX), keys, spec);

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
                return DF.agg(grouped, expressions, spec?.groupType);
            }

            const expressions = Object.fromEntries(
                Object.entries(input).map(([alias, value]) => {
                    if (typeof value !== "string") return [alias, value.build(EX)];
                    const { fn, arg } = parseAggregation(value);
                    return [alias, EX.call(fn, [EX.col(arg)])];
                })
            );
            return DF.agg(grouped, expressions, spec?.groupType);
        };

        return new ReadChainedDataFrame(
            next,
            this.source.getSession(),
            this.source.isStreamingDataFrame(),
            undefined,
            this.source.getObservations(),
        );
    }
}
