import { JoinHintName } from "../../engine/sparkConnectEnums";
import type { LiteralValue } from "./expressions";

// Gestión de cache/persist
/** @internal */
export interface CacheCap<R> {
    cache(plan: R): R;
    persist(plan: R, level?: string): R;
    unpersist(plan: R, blocking?: boolean): R;
}

// Particionamiento
/** @internal */
export interface RepartitionCap<R> {
    repartition(plan: R, numPartitions: number, shuffle: boolean): R;
    coalesce(plan: R, numPartitions: number): R;
}

// SQL ad-hoc (nota: en tu API original no recibe plan)
/** @internal */
export interface SqlCap<R> {
    sql(query: string): R;
}

// Hints
/** @internal */
export interface HintCap<R> {
    hint(plan: R, name: JoinHintName | string, params?: unknown[]): R;
}

// Muestreo
/** @internal */
export interface SamplingCap<R> {
    sample(
        plan: R,
        lowerBound: number,
        upperBound: number,
        withReplacement?: boolean,
        seed?: number,
        deterministicOrder?: boolean
    ): R;
}

// Estadísticos (si querés que el backend los implemente)
// Si preferís desacoplarlos, podés mover describe/summary a helpers de alto nivel.
/** @internal */
export interface DescribeCap<R, E> {
    describe(plan: R, columns: E[]): R;
}

/** @internal */
export interface SummaryCap<R, E> {
    summary(plan: R, metrics: E[], columns: E[]): R;
}

/** @internal One `na.replace` pair: values equal to `from` become `to`. */
export type NAReplacement = { from: LiteralValue; to: LiteralValue };

/**
 * Null handling (`df.na`). An empty column list means "every compatible
 * column", matching Spark's `DataFrameNaFunctions`.
 * @internal
 */
export interface NullHandlingCap<R> {
    fillNa(plan: R, columns: string[], values: LiteralValue[]): R;
    dropNa(plan: R, columns: string[], minNonNulls?: number): R;
    replaceNa(plan: R, columns: string[], replacements: NAReplacement[]): R;
}

/** @internal One `sampleBy` stratum and the fraction of its rows to keep. */
export type SampleFraction = { stratum: LiteralValue; fraction: number };

/** @internal Statistic relations (`df.stat`). */
export interface StatCap<R, E> {
    crosstab(plan: R, col1: string, col2: string): R;
    cov(plan: R, col1: string, col2: string): R;
    corr(plan: R, col1: string, col2: string, method?: string): R;
    approxQuantile(plan: R, columns: string[], probabilities: number[], relativeError: number): R;
    freqItems(plan: R, columns: string[], support?: number): R;
    sampleBy(plan: R, column: E, fractions: SampleFraction[], seed: number): R;
}

/** @internal Relations that reshape or re-label the input. */
export interface ReshapeCap<R, E> {
    unpivot(plan: R, ids: E[], values: E[] | undefined, variableColumnName: string, valueColumnName: string): R;
    toDF(plan: R, columnNames: string[]): R;
    transpose(plan: R, indexColumns: E[]): R;
    withColumns(plan: R, columns: Array<{ name: string; column: E }>): R;
    subqueryAlias(plan: R, alias: string): R;
    crossJoin(left: R, right: R): R;
}

/** @internal Row-position relations. */
export interface OffsetCap<R> {
    offset(plan: R, n: number): R;
    tail(plan: R, n: number): R;
}

/**
 * Expression partitioning. With `range` the expressions become ascending sort
 * orders unless they already are sort keys, which Spark turns into range
 * partitioning; otherwise Spark hash-partitions on them.
 * @internal
 */
export interface RepartitionByExpressionCap<R, E> {
    repartitionByExpression(plan: R, partitionExprs: E[], numPartitions: number | undefined, range: boolean): R;
}
