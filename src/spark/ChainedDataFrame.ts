import { DataFrameDSLFactory} from "./dataframe";
import {Column} from "../engine/column";
import {LogicalPlan} from "../engine/logicalPlan";
import {dataframeInterpreter} from "./dataFrameInterpreter";
export class ChainedDataFrame {
    constructor(private plan: LogicalPlan) {}

    private get dsl(): DataFrameDSLFactory<LogicalPlan> {
        return dataframeInterpreter(this.getPlan());
    }


    select(...cols: (string | Column)[]): ChainedDataFrame {
        const nextPlan = this.dsl.select(this.getPlan(),cols);
        return new ChainedDataFrame(nextPlan); // ⚠️ no pasamos DataFrame, sino el nuevo LogicalPlan
    }


    join(right: ChainedDataFrame, condition: Column): ChainedDataFrame {
        const joinedPlan = this.dsl.join(this.plan, right.plan, condition);
        return new ChainedDataFrame(joinedPlan);
    }

    filter(condition: Column): ChainedDataFrame {
        const nextPlan = this.dsl.filter(this.getPlan(),condition);
        return new ChainedDataFrame(nextPlan);
    }

    withColumn(name: string, column: Column): ChainedDataFrame {
        const nextPlan = this.dsl.withColumn(this.getPlan(),name, column);
        return new ChainedDataFrame(nextPlan);
    }

    show() {
        this.dsl.show(this.getPlan());
    }

    async collect(){
        return await this.dsl.collect(this.getPlan());
    }

    getPlan() {
        return this.plan;
    }
}
