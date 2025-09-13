import { SortOrder } from "../types";
import { JoinTypeInput, JoinHintName } from "../engine/sparkConnectEnums";


export interface DFAlg<R, E, G = unknown> {
    relation(format: string, path: string | string[], options?: Record<string, string>): R;
    select(plan: R, columns: E[]): R;
    filter(plan: R, condition: E): R;
    withColumn(plan: R, name: string, column: E): R;
    join(left: R, right: R, on: E, joinType?: JoinTypeInput): R;
    groupBy(plan: R, cols: E[]): G;
    agg(group: G, aggregations: Record<string, E>, groupType?: any): R;
    orderBy(plan: R, orders: SortOrder<E>[]): R;
    sort(plan: R, orders: SortOrder<E>[]): R;
    limit(plan: R, n: number): R;
    distinct(plan: R): R;
    dropDuplicates(plan: R, cols?: E[]): R;
    union(left: R, right: R, opts?: { byName?: boolean; allowMissingColumns?: boolean }): R;
    withColumnRenamed(plan: R, oldName: string, newName: string): R;
    withColumnsRenamed(plan: R, mapping: Record<string, string>): R;
    describe(plan: R, columns: E[]): R;
    summary(plan: R, metrics: E[], columns: E[]): R;
    cache(plan: R): R;
    persist(plan: R, level?: string): R;
    unpersist(plan: R, blocking?: boolean): R;
    repartition(plan: R, numPartitions: number, shuffle: boolean): R;
    coalesce(plan: R, numPartitions: number): R;
    sql(query: string): R;
    hint(plan: R, name: JoinHintName | string, params?: any[]): R;
    sample(plan: R, lowerBound: number, upperBound: number, withReplacement?: boolean, seed?: number, deterministicOrder?: boolean): R;
    drop(plan: R, columnNames: string[]): R;
}