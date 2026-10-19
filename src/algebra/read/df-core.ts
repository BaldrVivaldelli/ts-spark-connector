import { SortOrder } from "../../types";
import { CatalogOperation } from "../../types/catalog";
import { GroupTypeInput, JoinTypeInput } from "../../engine/sparkConnectEnums";
import type { LiteralValue } from "./expressions";

/**
 * Grouping beyond a plain `groupBy`: `rollup`, `cube`, `pivot` (with or
 * without explicit values; without them Spark first collects the distinct
 * values) and `groupingSets`. `agg`'s own `groupType` argument wins over
 * `groupType` here when both are given.
 * @internal
 */
export type GroupingSpec<E> = {
    groupType: GroupTypeInput;
    pivot?: { column: E; values?: LiteralValue[] };
    groupingSets?: E[][];
};

/** @internal */
export interface DFCore<R, E, G = unknown> {
    relation(
        format: string,
        path: string | string[],
        options?: Record<string, string>,
        schema?: string,
        /** JDBC only: one partition per WHERE clause (`DataSource.predicates`). */
        predicates?: readonly string[]
    ): R;

    /** Roots the plan at a server-side catalog metadata operation. */
    catalog(op: CatalogOperation): R;

    /**
     * Roots the plan at client-provided local data: a DDL schema plus, when
     * the relation is not empty, rows already serialized as an Arrow IPC
     * stream. `rowCount` feeds plan inspection; the bytes never reach it.
     */
    localRelation(ddl: string, data: Uint8Array | undefined, rowCount: number): R;

    /**
     * Roots the plan at Spark's `range` relation: a single `id` column of
     * longs from `start` (0 when omitted) up to, excluding, `end`, in steps
     * of `step`, optionally split into `numPartitions` partitions.
     */
    range(start: bigint | undefined, end: bigint, step: bigint, numPartitions: number | undefined): R;

    /** Binds a relation root to the id used by qualified unresolved attributes. */
    withPlanId?(plan: R, planId: number): R;

    select(plan: R, columns: E[]): R;
    filter(plan: R, condition: E): R;
    withColumn(plan: R, name: string, column: E): R;

    join(left: R, right: R, on: E, joinType?: JoinTypeInput): R;

    groupBy(plan: R, cols: E[], spec?: GroupingSpec<E>): G;
    agg(group: G, aggregations: Record<string, E>, groupType?: GroupTypeInput): R;

    orderBy(plan: R, orders: SortOrder<E>[]): R;
    sort(plan: R, orders: SortOrder<E>[]): R;

    limit(plan: R, n: number): R;
    distinct(plan: R): R;
    dropDuplicates(plan: R, cols?: E[]): R;
    union(left: R, right: R, opts?: { byName?: boolean; allowMissingColumns?: boolean }): R;
    intersect(left: R, right: R, opts?: { all?: boolean }): R;
    except(left: R, right: R, opts?: { all?: boolean }): R;

    withColumnRenamed(plan: R, oldName: string, newName: string): R;
    withColumnsRenamed(plan: R, mapping: Record<string, string>): R;

    drop(plan: R, columnNames: string[]): R;
}
