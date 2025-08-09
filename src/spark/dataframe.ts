import { Column } from "../engine/column";


export interface GroupedDSL<F> {
    agg(aggregations: Record<string, string>): F
}

export interface DataFrameDSLFactory<F> {
    select(plan: F, columns: (string | Column)[]): F;

    filter(plan: F, condition: Column): F;

    withColumn(plan: F, name: string, column: Column): F;

    collect(plan: F): Promise<any[]>;

    show(plan: F): void | Promise<void>;

    join(left: F, right: F, on: Column): F;

    groupBy(plan: F, cols: (string | Column)[]): GroupedDSL<F>;

    orderBy(plan: F, cols: Array<string | Column>): F;
    sort(plan: F, cols: Array<string | Column>): F;

    limit(plan: F, n: number): F;

    distinct(plan: F): F;

    dropDuplicates(plan: F, cols?: (string | Column)[]): F;

    union(left: F, right: F, opts?: { byName?: boolean; allowMissingColumns?: boolean }): F;

    withColumnRenamed(plan: F, oldName: string, newName: string): F;

    withColumnsRenamed(plan: F, mapping: Record<string, string>): F;
}