import crypto from "crypto";
import { SparkSession } from "./session";
import { LogicalPlan } from "../engine/logicalPlan";
import { sparkGrpcClient, StreamingQueryHandle } from "./sparkClient";
import { ProtoPlan, ProtoWriteRoot, protoWriteRootToPlan } from "../write/compilerWrite";
import { ExplainModeInput, toProtoExplainMode } from "../engine/sparkConnectEnums";

type ExecutePlanResponse = Record<string, unknown>;
type UserContext = ReturnType<SparkSession["getUserContext"]>;

type ExecutePlanRequest = {
    session_id: string;
    user_context: UserContext;
    client_type: "ts-spark-connector";
    plan: {
        root: LogicalPlan | ProtoPlan;
    };
};

type AnalyzePlanRequest = {
    session_id: string;
    user_context: UserContext;
    client_type: "ts-spark-connector";
    explain: {
        plan: {
            root: LogicalPlan | ProtoPlan;
        };
        explain_mode: number;
    };
};

type StreamingExecuteRequest = {
    session_id: string;
    user_context: UserContext;
    client_type: "ts-spark-connector";
    plan: ProtoPlan;
    operation_id: string;
};

type OrderedStreamingPlan = ProtoPlan & {
    command?: {
        create_dataframe_view?: unknown;
        write_stream_operation_start?: unknown;
    };
};

export interface SparkPlanInterpreter<F> {
    execute(plan: LogicalPlan): F;
    runWrite(plan: ProtoWriteRoot): Promise<ExecutePlanResponse[]>;
}

function debugEnabled(): boolean {
    return process.env.SPARK_CONNECT_DEBUG === "1" || process.env.SPARK_CONNECT_DEBUG === "true";
}

function debugLog(payload: unknown) {
    if (!debugEnabled()) return;
    // eslint-disable-next-line no-console
    console.log(typeof payload === "string" ? payload : JSON.stringify(payload, null, 2));
}

export class SparkConnectExecutor implements SparkPlanInterpreter<Promise<ExecutePlanResponse[]>> {
    constructor(private readonly session: SparkSession) {}

    static for(session: SparkSession): SparkConnectExecutor {
        return new SparkConnectExecutor(session);
    }

    execute(plan: LogicalPlan): Promise<ExecutePlanResponse[]> {
        const request = this.buildExecuteRequest(plan);
        debugLog({ executePlan: request.plan });
        return sparkGrpcClient.executePlan(request, this.session.getConnectionConfig());
    }

    explain(plan: LogicalPlan, mode: ExplainModeInput): Promise<string> {
        const request = this.buildAnalyzeRequest(plan, mode);
        debugLog({ analyzePlan: request.explain });
        return sparkGrpcClient.explain(request, this.session.getConnectionConfig());
    }

    async runWrite(root: ProtoWriteRoot): Promise<ExecutePlanResponse[]> {
        const plan = protoWriteRootToPlan(root);
        const request = {
            plan,
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector" as const,
        };
        debugLog({ executeWrite: plan });
        return sparkGrpcClient.executePlan(request, this.session.getConnectionConfig());
    }

    async runStream(root: ProtoWriteRoot): Promise<StreamingQueryHandle> {
        const plan = protoWriteRootToPlan(root);
        const orderedPlan = Array.isArray(plan) ? this.reorderStreamingCommands(plan) : [plan];
        const sessionId = this.session.getSessionId();
        const userContext = this.session.getUserContext();
        const connectionConfig = this.session.getConnectionConfig();

        const nonStreamingCommands = orderedPlan.slice(0, -1);
        for (const command of nonStreamingCommands) {
            debugLog({ executeStreamingPrerequisite: command });
            await sparkGrpcClient.executePlan({
                plan: command,
                session_id: sessionId,
                user_context: userContext,
                client_type: "ts-spark-connector",
            }, connectionConfig);
        }

        const finalPlan = orderedPlan[orderedPlan.length - 1];
        const operationId = crypto.randomUUID();
        debugLog({ executeStreamingPlan: finalPlan, operation_id: operationId });
        const handle = await sparkGrpcClient.executePlanStreaming({
            plan: finalPlan,
            session_id: sessionId,
            user_context: userContext,
            operation_id: operationId,
            client_type: "ts-spark-connector",
        }, connectionConfig);

        if (root.awaitTermination) {
            await handle.awaitTermination();
        }

        return handle;
    }

    private buildExecuteRequest(root: LogicalPlan): ExecutePlanRequest {
        return {
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector",
            plan: { root },
        };
    }

    private buildAnalyzeRequest(root: LogicalPlan, mode: ExplainModeInput): AnalyzePlanRequest {
        return {
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector",
            explain: {
                plan: { root },
                explain_mode: toProtoExplainMode(mode),
            },
        };
    }

    private reorderStreamingCommands(plan: ProtoPlan[]): ProtoPlan[] {
        const ordered = plan as OrderedStreamingPlan[];
        return [
            ...ordered.filter(command => command.command?.create_dataframe_view),
            ...ordered.filter(command => command.command?.write_stream_operation_start),
        ];
    }
}
