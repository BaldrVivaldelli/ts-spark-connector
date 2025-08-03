import { DataFrameDSLFactory} from "./dataframe";
import {Column} from "../engine/column";
import {LogicalPlan} from "../engine/logicalPlan";
import {dataframeInterpreter} from "./dataFrameInterpreter";
import {SparkSession} from "./session";
export class ChainedDataFrame {
    constructor(
        private plan: LogicalPlan,
        private session: SparkSession
    ) {}

    private get dsl(): DataFrameDSLFactory<LogicalPlan> {
        return dataframeInterpreter(this.getPlan(), this.getSession());
    }
    private wrap(plan: LogicalPlan,sparkSession:SparkSession): ChainedDataFrame {
        console.debug("[WRAP] Plan", JSON.stringify(plan, null, 2));
        return new ChainedDataFrame(plan, sparkSession);
    }
    select(...cols: (string | Column)[]): ChainedDataFrame {
        const nextPlan = this.dsl.select(this.getPlan(),cols);
        return this.wrap(nextPlan,this.getSession());
    }


    join(right: ChainedDataFrame, condition: Column): ChainedDataFrame {
        const nextPlan = this.dsl.join(this.plan, right.plan, condition);
        return this.wrap(nextPlan,this.getSession());
    }

    filter(condition: Column): ChainedDataFrame {
        const nextPlan = this.dsl.filter(this.getPlan(),condition);
        return this.wrap(nextPlan,this.getSession());
    }

    withColumn(name: string, column: Column): ChainedDataFrame {
        const nextPlan = this.dsl.withColumn(this.getPlan(),name, column);
        return this.wrap(nextPlan,this.getSession());
    }

    show() {
        this.dsl.show(this.getPlan());
    }

    async collect(){
        return await this.dsl.collect(this.getPlan());
    }

    getSession(){
        return this.session;
    }
    getPlan() {
        return this.plan;
    }
}
