import crypto from "crypto";
import { SparkConnectionConfig, SparkSession } from "./session";
import { combineSignals, ResolvedExecution } from "./executionOptions";
import type { ProtoRel } from "../engine/compilerRead";
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
        root: ProtoRel;
    };
    operation_id: string;
    tags?: string[];
    client_observed_server_side_session_id?: string;
};

type AnalyzePlanRequest = {
    session_id: string;
    user_context: UserContext;
    client_type: "ts-spark-connector";
    explain: {
        plan: {
            root: ProtoRel;
        };
        explain_mode: number;
    };
    client_observed_server_side_session_id?: string;
};

type OrderedStreamingPlan = ProtoPlan & {
    command?: {
        create_dataframe_view?: unknown;
        write_stream_operation_start?: unknown;
    };
};

/** AnalyzePlan request kinds whose result is read back by field name. */
export type AnalyzeField =
    | "schema"
    | "tree_string"
    | "is_local"
    | "is_streaming"
    | "input_files"
    | "same_semantics"
    | "semantic_hash";

type AnalyzeAction =
    | { kind: "persist"; relation: unknown; level: string }
    | { kind: "unpersist"; relation: unknown; blocking?: boolean };

type StorageLevelProto = {
    use_disk: boolean;
    use_memory: boolean;
    use_off_heap: boolean;
    deserialized: boolean;
    replication: number;
};

const STORAGE_LEVELS: Record<string, StorageLevelProto> = {
    NONE: { use_disk: false, use_memory: false, use_off_heap: false, deserialized: false, replication: 1 },
    DISK_ONLY: { use_disk: true, use_memory: false, use_off_heap: false, deserialized: false, replication: 1 },
    MEMORY_ONLY: { use_disk: false, use_memory: true, use_off_heap: false, deserialized: true, replication: 1 },
    MEMORY_AND_DISK: { use_disk: true, use_memory: true, use_off_heap: false, deserialized: true, replication: 1 },
    MEMORY_ONLY_SER: { use_disk: false, use_memory: true, use_off_heap: false, deserialized: false, replication: 1 },
    MEMORY_AND_DISK_SER: { use_disk: true, use_memory: true, use_off_heap: false, deserialized: false, replication: 1 },
    OFF_HEAP: { use_disk: true, use_memory: true, use_off_heap: true, deserialized: false, replication: 1 },
};

/**
 * Runs already-compiled Spark Connect relations. `ProtoDFAlg` produces the
 * `ProtoRel` directly; the client-side `LogicalPlan` AST in ../engine/logicalPlan
 * is a debugging serializer and never reaches this boundary.
 */
export interface SparkPlanInterpreter<F> {
    execute(plan: ProtoRel): F;
    runWrite(plan: ProtoWriteRoot): Promise<ExecutePlanResponse[]>;
}

export class SparkConnectExecutor implements SparkPlanInterpreter<Promise<ExecutePlanResponse[]>> {
    constructor(private readonly session: SparkSession) {}

    static for(session: SparkSession): SparkConnectExecutor {
        return new SparkConnectExecutor(session);
    }

    async execute(
        plan: ProtoRel,
        execution: ResolvedExecution = this.session.resolveExecution(),
    ): Promise<ExecutePlanResponse[]> {
        await this.prepareSession();
        const request = this.buildExecuteRequest(plan, execution);
        const responses = await this.remote(() =>
            sparkGrpcClient.executePlan(request, this.connectionConfigFor(execution))
        );
        responses.forEach(response => this.session.observeServerSideSessionId(response));
        return responses;
    }

    /** Streams ExecutePlan responses without materializing the complete result. */
    async *stream(
        plan: ProtoRel,
        execution: ResolvedExecution = this.session.resolveExecution(),
    ): AsyncGenerator<ExecutePlanResponse, void, void> {
        await this.prepareSession();
        const request = this.buildExecuteRequest(plan, execution);
        try {
            for await (const response of sparkGrpcClient.executePlanStream(
                request,
                this.connectionConfigFor(execution)
            )) {
                this.session.observeServerSideSessionId(response);
                yield response;
            }
        } catch (error) {
            this.session.observeRemoteError(error);
            throw error;
        }
    }

    async explain(plan: ProtoRel, mode: ExplainModeInput): Promise<string> {
        await this.prepareSession();
        const request = this.buildAnalyzeRequest(plan, mode);
        const result = await this.remote(() =>
            sparkGrpcClient.explainWithResponse(request, this.session.getConnectionConfig())
        );
        this.session.observeServerSideSessionId(result.response);
        return result.explainString;
    }

    /** Applies deferred cache/persist/unpersist operations via AnalyzePlan RPC. */
    async runAnalyzeAction(action: AnalyzeAction): Promise<void> {
        await this.prepareSession();
        const requestBase = {
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector",
            ...this.observedServerSessionField(),
        };
        const request = action.kind === "persist"
            ? {
                ...requestBase,
                persist: {
                    relation: action.relation,
                    storage_level: this.storageLevel(action.level),
                },
            }
            : {
                ...requestBase,
                unpersist: {
                    relation: action.relation,
                    ...(action.blocking === undefined ? {} : { blocking: action.blocking }),
                },
            };
        const response = await this.remote(() =>
            sparkGrpcClient.analyze(request, this.session.getConnectionConfig())
        );
        this.session.observeServerSideSessionId(response);
    }

    /**
     * Runs one AnalyzePlan request (`schema`, `tree_string`, `is_local`, ...)
     * and returns the response's sub-message of the same name.
     */
    async analyzePlan(field: AnalyzeField, payload: Record<string, unknown>): Promise<Record<string, unknown>> {
        await this.prepareSession();
        const request = {
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector",
            ...this.observedServerSessionField(),
            [field]: payload,
        };
        const response = await this.remote(() =>
            sparkGrpcClient.analyze(request, this.session.getConnectionConfig())
        );
        this.session.observeServerSideSessionId(response);
        const result = response[field];
        if (result === null || typeof result !== "object") {
            throw new Error(`Invalid AnalyzePlan response: missing ${field} result.`);
        }
        return result as Record<string, unknown>;
    }

    /**
     * Executes a Spark Connect `Command` (WriteOperationV2, MergeIntoTable,
     * ...). Like `runWrite`, recovery reattaches with the stable operation id
     * and never replays the command.
     */
    async runCommand(command: Record<string, unknown>): Promise<ExecutePlanResponse[]> {
        await this.prepareSession();
        const request = {
            plan: { command },
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector" as const,
            operation_id: crypto.randomUUID(),
            ...this.observedServerSessionField(),
        };
        const config = this.session.getConnectionConfig();
        const responses = await this.remote(() => sparkGrpcClient.executePlan(request, config));
        responses.forEach(response => this.session.observeServerSideSessionId(response));
        return responses;
    }

    async runWrite(
        root: ProtoWriteRoot,
        execution: ResolvedExecution = this.session.resolveExecution(),
    ): Promise<ExecutePlanResponse[]> {
        await this.prepareSession();
        const plan = protoWriteRootToPlan(root);
        const request = {
            plan,
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector" as const,
            operation_id: execution.operationId,
            ...(execution.tags.length > 0 ? { tags: execution.tags } : {}),
            ...this.observedServerSessionField(),
        };
        const config = this.connectionConfigFor(execution);
        // ExecutePlan recovery uses ReattachExecute with this stable operation id;
        // the original mutating command is never replayed.
        const responses = await this.remote(() => sparkGrpcClient.executePlan(request, config));
        responses.forEach(response => this.session.observeServerSideSessionId(response));
        return responses;
    }

    async runStream(root: ProtoWriteRoot): Promise<StreamingQueryHandle> {
        await this.prepareSession();
        const plan = protoWriteRootToPlan(root);
        const orderedPlan = Array.isArray(plan) ? this.reorderStreamingCommands(plan) : [plan];
        const sessionId = this.session.getSessionId();
        const userContext = this.session.getUserContext();
        const connectionConfig = this.session.getConnectionConfig();

        const nonStreamingCommands = orderedPlan.slice(0, -1);
        for (const command of nonStreamingCommands) {
            const responses = await this.remote(() => sparkGrpcClient.executePlan({
                plan: command,
                session_id: sessionId,
                user_context: userContext,
                client_type: "ts-spark-connector",
                operation_id: crypto.randomUUID(),
                ...this.observedServerSessionField(),
            }, connectionConfig));
            responses.forEach(response => this.session.observeServerSideSessionId(response));
        }

        const finalPlan = orderedPlan[orderedPlan.length - 1];
        const operationId = crypto.randomUUID();
        const rawHandle = await this.remote(() => sparkGrpcClient.executePlanStreaming({
            plan: finalPlan,
            session_id: sessionId,
            user_context: userContext,
            operation_id: operationId,
            client_type: "ts-spark-connector",
            ...this.observedServerSessionField(),
        }, connectionConfig));
        this.syncStreamingIdentity(rawHandle);
        const handle = this.wrapStreamingHandle(rawHandle);

        if (root.awaitTermination) {
            await handle.awaitTermination();
        }

        return handle;
    }

    private buildExecuteRequest(root: ProtoRel, execution: ResolvedExecution): ExecutePlanRequest {
        return {
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector",
            plan: { root },
            operation_id: execution.operationId,
            ...(execution.tags.length > 0 ? { tags: execution.tags } : {}),
            ...this.observedServerSessionField(),
        };
    }

    /** Session connection config, cancelled by the execution's own signal too. */
    private connectionConfigFor(execution: ResolvedExecution): SparkConnectionConfig {
        const config = this.session.getConnectionConfig();
        const signal = combineSignals(config.signal, execution.signal);
        return signal === config.signal ? config : { ...config, signal };
    }

    private buildAnalyzeRequest(root: ProtoRel, mode: ExplainModeInput): AnalyzePlanRequest {
        return {
            session_id: this.session.getSessionId(),
            user_context: this.session.getUserContext(),
            client_type: "ts-spark-connector",
            ...this.observedServerSessionField(),
            explain: {
                plan: { root },
                explain_mode: toProtoExplainMode(mode),
            },
        };
    }

    private async prepareSession(): Promise<void> {
        await this.session.ensureRemoteConfigApplied();
        this.session.markRemoteTouched();
    }

    private async remote<T>(operation: () => Promise<T>): Promise<T> {
        try {
            return await operation();
        } catch (error) {
            this.session.observeRemoteError(error);
            throw error;
        }
    }

    private syncStreamingIdentity(handle: StreamingQueryHandle): void {
        if (!handle.serverSideSessionId) return;
        this.session.observeServerSideSessionId({
            session_id: this.session.getSessionId(),
            server_side_session_id: handle.serverSideSessionId,
        });
    }

    private wrapStreamingHandle(handle: StreamingQueryHandle): StreamingQueryHandle {
        const awaitTermination = (async (timeoutMs?: number): Promise<void | boolean> => {
            const result = timeoutMs === undefined
                ? await this.remote<void>(() => handle.awaitTermination())
                : await this.remote<boolean>(() => handle.awaitTermination(timeoutMs));
            this.syncStreamingIdentity(handle);
            return result;
        }) as StreamingQueryHandle["awaitTermination"];

        return {
            name: handle.name,
            get serverSideSessionId() { return handle.serverSideSessionId; },
            awaitTermination,
            stop: async () => {
                const result = await this.remote(() => handle.stop());
                this.syncStreamingIdentity(handle);
                return result;
            },
        };
    }

    private observedServerSessionField(): { client_observed_server_side_session_id?: string } {
        const id = this.session.getServerSideSessionId();
        return id ? { client_observed_server_side_session_id: id } : {};
    }

    private storageLevel(level: string): StorageLevelProto {
        const normalized = level.toUpperCase();
        const storage = STORAGE_LEVELS[normalized];
        if (!storage) {
            throw new RangeError(
                `Unsupported storage level "${level}". Expected one of: ${Object.keys(STORAGE_LEVELS).join(", ")}.`
            );
        }
        return storage;
    }

    private reorderStreamingCommands(plan: ProtoPlan[]): ProtoPlan[] {
        const ordered = plan as OrderedStreamingPlan[];
        return [
            ...ordered.filter(command => command.command?.create_dataframe_view),
            ...ordered.filter(command => command.command?.write_stream_operation_start),
        ];
    }
}
