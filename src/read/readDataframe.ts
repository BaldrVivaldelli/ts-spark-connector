import {JoinTypeInput} from "../engine/sparkConnectEnums";
import {SparkSession} from "../client/session";

export type SortDirection = "asc" | "desc";
export type NullsOrder = "nullsFirst" | "nullsLast";
export type FrameType = "rows" | "range";

export type FrameBoundary =
    | { type: "UnboundedPreceding" }
    | { type: "UnboundedFollowing" }
    | { type: "CurrentRow" }
    | { type: "ValuePreceding"; value: number }
    | { type: "ValueFollowing"; value: number };

export type SortOrder<E> = {
    expr: E;
    direction: SortDirection;
    nulls?: NullsOrder;
};

export type WindowSpec<E> = {
    partitionBy: E[];
    orderBy: Array<{ input: E; direction: SortDirection; nulls?: NullsOrder }>;
    frame?: { type: FrameType; start: FrameBoundary; end: FrameBoundary };
};

export interface ExprAlg<E> {
    col(name: string): E;

    lit(v: string | number | boolean): E;

    bin(op: string, left: E, right: E): E;

    logical(op: "AND" | "OR", left: E, right: E): E;

    alias(input: E, ...names: string[]): E;

    call(name: string, args: E[]): E;

    sortKey(input: E, direction: SortDirection, nulls?: NullsOrder): E;

    star(): E;

    caseWhen(branches: Array<{ when: E; then: E }>, elze?: E): E;

    win(func: E, spec: WindowSpec<E>): E;

    isNull(input: E): E;
    isNotNull(input: E): E;
    coalesce(args: E[]): E;
    sortKey(input: E, direction: "asc"|"desc", nulls?: "nullsFirst"|"nullsLast"): E;
    explode(input: E): E;
    posexplode(input: E): E;
    getField(input: E, field: string): E;
    map_keys(input: E): E;
    map_values(input: E): E;
    elementAt(map: E, key: E): E;
    getItem(collectionExpr: E, key: E | string | number): E;
    split(input: E, delimiter: E | string): E;
    from_json(jsonExpr: E, schema: string): E;
    to_json(expr: E): E;
}

export interface DFAlg<R, E, G = unknown> {
    relation(format: string, path: string | string[], options?: Record<string, string>): R;

    select(plan: R, columns: E[]): R;

    filter(plan: R, condition: E): R;

    withColumn(plan: R, name: string, column: E): R;

    join(left: R, right: R, on: E, joinType?: JoinTypeInput): R;

    groupBy(plan: R, cols: E[]): G;

    // ⬇⬇⬇ AQUÍ el cambio clave
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

}

export interface DFExec<R> {
    collect(root: R, session: SparkSession): Promise<any[]>;
    explain(root: R, session: SparkSession, ): Promise<any[]>;

}
export type DFProgram<R, E, G = unknown> =
    (DF: DFAlg<R, E, G>, EX: ExprAlg<E>) => R;
