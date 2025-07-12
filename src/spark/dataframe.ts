import { LogicalPlan } from "../engine/logicalPlan";
import { Column, col } from "../engine/column";
import { sparkGrpcClient } from "../client/sparkClient";
import {compileToProtobuf} from "../engine/compiler";
import {printArrowResults} from "../utils/arrowPrinter";

export class DataFrame {
    constructor(private plan: LogicalPlan) {}

    filter(condition: Column): DataFrame {
        return new DataFrame({ type: "Filter", input: this.plan, condition: condition.expr });
    }

    select(...cols: string[]): DataFrame {
        return new DataFrame({
            type: "Project",
            input: this.plan,
            columns: cols.map(name => col(name).expr)
        });
    }

    col(name: string): Column {
        return col(name);
    }


    async collect() {
        const logicalPlan = compileToProtobuf(this.plan);

        const request = {
            session_id: "00112233-4455-6677-8899-aabbccddeeff",
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
