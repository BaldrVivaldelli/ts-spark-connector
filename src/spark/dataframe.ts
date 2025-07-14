import { Column } from "../engine/column";


/*export interface DataFrameDSL<F> {
    select(columns: (string | Column)[]): F;
    filter(condition: Column): F
    /!*join(other: F, on: string): F
    groupBy(columns: string[]): GroupedDSL<F>*!/
    withColumn(name: string, column: Column): F
    show(): void,
    collect(): Promise<any[]>;
    }*/
export interface GroupedDSL<F> {
    agg(aggregations: Record<string, string>): F
}

export interface DataFrameDSLFactory<F> {
    select(plan: F, columns: (string | Column)[]): F;
    filter(plan: F, condition: Column): F;
    withColumn(plan: F, name: string, column: Column): F;
    collect(plan: F): Promise<any[]>;
    show(plan: F): void | Promise<void>;
}