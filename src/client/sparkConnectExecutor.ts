import {SparkSession} from "./session";
import {LogicalPlan} from "../engine/logicalPlan";
import {sparkGrpcClient} from "./sparkClient";
import {ProtoWriteRoot, protoWriteRootToPlan} from "../write/compilerWrite";


export interface SparkPlanInterpreter<F> {
    execute(plan: LogicalPlan): F;
    runWrite(plan: ProtoWriteRoot): Promise<any[]>;
}

export class SparkConnectExecutor implements SparkPlanInterpreter<Promise<any>> {
    constructor(private readonly session: SparkSession) {
    }



    static for(session: SparkSession): SparkConnectExecutor {
        return new SparkConnectExecutor(session);
    }
    execute(plan: any): Promise<any> {
        const request = this.buildBaseRequest(plan, this.session);
        return sparkGrpcClient.executePlan(request);
    }

    async runWrite(root: ProtoWriteRoot): Promise<any[]> {
        const plan = protoWriteRootToPlan(root);
        return await sparkGrpcClient.executePlan({
            plan,
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
        })
    }

    private buildBaseRequest(root: any, session: SparkSession) {
        return {
            session_id: session.getSessionId(),
            user_context: session.getUserContext(),
            plan: {root},
        };
    }

}