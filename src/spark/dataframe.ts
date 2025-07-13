import { LogicalPlan } from "../engine/logicalPlan";
import { Column, col } from "../engine/column";
import { sparkGrpcClient } from "../client/sparkClient";
import {compileToProtobuf} from "../engine/compiler";
import {printArrowResults} from "../utils/arrowPrinter";


export interface DataFrameDSL<F> {
    select(columns: (string | Column)[]): F;
    filter(condition: Column): F
    /*join(other: F, on: string): F
    groupBy(columns: string[]): GroupedDSL<F>*/
    withColumn(name: string, column: Column): F
}
export interface GroupedDSL<F> {
    agg(aggregations: Record<string, string>): F
}

export class DataFrame {
    constructor(public plan: LogicalPlan) {}


    col(name: string): Column {
        return col(name);
    }

    async collect() {
        const logicalPlan = compileToProtobuf(this.plan);

        const request = {
            session_id: crypto.randomUUID(),
            user_context: {},
            plan: logicalPlan.plan
        };

        return await sparkGrpcClient.executePlan(request);
    }

    async show() {
        const result = await this.collect();
        const arrowBuffers = result
            .filter(r => r.arrow_batch?.data)
            .map(r => r.arrow_batch.data as Buffer);

        printArrowResults(arrowBuffers);
    }
}
