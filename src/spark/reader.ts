import { DataFrame } from "./dataframe";
import { LogicalPlan } from "../engine/logicalPlan";

export class DataFrameReader {
    private options: Record<string, string> = {};

    option(key: string, value: string): DataFrameReader {
        this.options[key] = value;
        return this;
    }

    csv(path: string): DataFrame {
        const plan: LogicalPlan = {
            type: "Relation",
            format: "csv",
            options: this.options,
            path
        };
        return new DataFrame(plan);
    }
}
