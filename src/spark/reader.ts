import { LogicalPlan } from "../engine/logicalPlan";
import {ChainedDataFrame} from "./ChainedDataFrame";

export class DataFrameReader {
    private options: Record<string, string> = {};

    option(key: string, value: string): DataFrameReader {
        this.options[key] = value;
        return this;
    }
    csv(path: string): ChainedDataFrame {
        const plan: LogicalPlan = {
            type: "Relation",
            format: "csv",
            options: this.options,
            path,
        };
        return new ChainedDataFrame(plan);
    }
}
