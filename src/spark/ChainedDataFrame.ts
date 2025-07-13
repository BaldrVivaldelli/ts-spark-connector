import {DataFrame, DataFrameDSL} from "./dataframe";
import {dataframeInterpreter} from "./dataFrameInterpreter";
import {Column} from "../engine/column";

export class ChainedDataFrame {
    constructor(private df: DataFrame) {}

    private get dsl(): DataFrameDSL<DataFrame> {
        return dataframeInterpreter(this.df);
    }

    select(...cols: (string | Column)[]): ChainedDataFrame {
        const df = this.dsl.select(cols);
        return new ChainedDataFrame(df);
    }

    filter(condition: Column): ChainedDataFrame {
        const next = this.dsl.filter(condition);
        return new ChainedDataFrame(next);
    }

/*    groupBy(...columns: string[]) {
        const base = this;
        return {
            agg(aggs: Record<string, string>) {
                const grouped = base.dsl.groupBy(columns).agg(aggs);
                return new ChainedDataFrame(grouped);
            },
        };
    }*/

    withColumn(name: string, column: Column): ChainedDataFrame {
        const next = this.dsl.withColumn(name, column);
        return new ChainedDataFrame(next);
    }

    show(){
        return this.df.show();
    }

    col(name: string): Column {
        return this.df.col(name); // usás la función global internamente
    }
    collect() {
        return this.df.collect();
    }

    // Si querés exponer lógica interna
    getPlan() {
        return this.df["plan"];
    }
}
