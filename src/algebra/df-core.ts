import { SortOrder } from "../types";
import { JoinTypeInput } from "../engine/sparkConnectEnums";

export interface DFCore<R, E, G = unknown> {
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

    drop(plan: R, columnNames: string[]): R;
}
