import {SparkSession} from "./session";
import {LogicalPlan} from "../engine/logicalPlan";
import {sparkGrpcClient, StreamingQueryHandle} from "./sparkClient";
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
        console.log("Enviando comando GRPC:", JSON.stringify({ plan }, null, 2));
        const request = this.buildBaseRequest(plan, this.session);
        return sparkGrpcClient.executePlan(request);
    }

    async runWrite(root: ProtoWriteRoot): Promise<any[]> {
        const plan = protoWriteRootToPlan(root);

        console.log(JSON.stringify(plan, null, 2));
        return await sparkGrpcClient.executePlan({
            plan,
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
        })
    }
    async runStream(root: ProtoWriteRoot): Promise<StreamingQueryHandle> {
        const plan = protoWriteRootToPlan(root);
        const orderedPlan = Array.isArray(plan) ? this.reorderStreamingCommands(plan) : [plan];

        const session_id = this.session.getSessionId();
        const user_context = this.session.getUserContext();

        // Ejecutar todos menos el último de forma sincrónica
        const nonStreamingCommands = orderedPlan.slice(0, -1);
        for (const cmd of nonStreamingCommands) {
            console.log(JSON.stringify(cmd, null, 2));
            await sparkGrpcClient.executePlan({ plan: cmd, session_id, user_context });
        }

        // Ejecutar el último como plan de streaming
        const finalPlan = orderedPlan[orderedPlan.length - 1];
        console.log(JSON.stringify(finalPlan, null, 2));
        return await sparkGrpcClient.executePlanStreaming({ plan: finalPlan, session_id, user_context });
    }

    private buildBaseRequest(root: any, session: SparkSession) {
        return {
            session_id: session.getSessionId(),
            user_context: session.getUserContext(),
            plan: {root},
        };
    }

    private reorderStreamingCommands(plan: any[]): any[] {
        return [
            ...plan.filter(p => p.command?.create_dataframe_view),      // primero: definiciones de vistas
            ...plan.filter(p => p.command?.write_stream_operation),     // último: escrituras de stream
        ];
    }
}