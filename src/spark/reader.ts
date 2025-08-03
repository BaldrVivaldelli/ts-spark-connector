import { LogicalPlan } from "../engine/logicalPlan";
import {ChainedDataFrame} from "./ChainedDataFrame";
import {SparkSession} from "./session";

export class DataFrameReader {
    constructor(private session: SparkSession) {}

    csv(path: string): ChainedDataFrame {
        const plan: LogicalPlan = {
            type: "Relation",
            format: "csv",
            path,
            options: this.options,
        };

        return new ChainedDataFrame(plan,this.session);
    }

    option(key: string, value: string): this {
        this.options[key] = value;
        return this;
    }

    private options: Record<string, string> = {};
}
