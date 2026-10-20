import crypto from "node:crypto";
import type { ReattachableResponse, RpcMessage } from "./reattachableExecution";

type StreamingQueryInstanceId = {
    id?: string;
    run_id?: string;
    runId?: string;
};

/** @internal The `{ id, run_id }` pair Spark uses to address one query run. */
export type NormalizedStreamingQueryInstanceId = {
    id: string;
    run_id: string;
};

type QueryIdCarrier = {
    query_id?: StreamingQueryInstanceId;
    queryId?: StreamingQueryInstanceId;
};

export type StreamStartResult = QueryIdCarrier & {
    name?: string;
    query_name?: string;
    queryName?: string;
    query_started_event_json?: string;
    queryStartedEventJson?: string;
};

type StatusResult = {
    status_message?: string;
    statusMessage?: string;
    is_data_available?: boolean;
    isDataAvailable?: boolean;
    is_trigger_active?: boolean;
    isTriggerActive?: boolean;
    is_active?: boolean;
    isActive?: boolean;
};

type RecentProgressResult = {
    recent_progress_json?: string[];
    recentProgressJson?: string[];
};

export type StreamingQueryCommandResult = QueryIdCarrier & {
    status?: StatusResult;
    recent_progress?: RecentProgressResult;
    recentProgress?: RecentProgressResult;
    explain?: { result?: string };
    await_termination?: {
        terminated?: boolean;
    };
    awaitTermination?: {
        terminated?: boolean;
    };
    exception?: {
        exception_message?: string;
        exceptionMessage?: string;
        error_class?: string;
        errorClass?: string;
        stack_trace?: string;
        stackTrace?: string;
    };
};

type StreamingQueryInstance = {
    id?: StreamingQueryInstanceId;
    name?: string;
};

export type StreamingQueryManagerCommandResult = {
    active?: { active_queries?: StreamingQueryInstance[]; activeQueries?: StreamingQueryInstance[] };
    query?: StreamingQueryInstance;
    await_any_termination?: { terminated?: boolean };
    awaitAnyTermination?: { terminated?: boolean };
    reset_terminated?: boolean;
    resetTerminated?: boolean;
    add_listener?: boolean;
    remove_listener?: boolean;
};

export type StreamingQueryListenerEventMessage = {
    event_json?: string;
    eventJson?: string;
    event_type?: string | number;
    eventType?: string | number;
};

export type StreamingQueryListenerEventsResult = {
    events?: StreamingQueryListenerEventMessage[];
    listener_bus_listener_added?: boolean;
    listenerBusListenerAdded?: boolean;
};

export type ExecutePlanResponse = ReattachableResponse & {
    write_stream_operation_start_result?: StreamStartResult;
    writeStreamOperationStartResult?: StreamStartResult;
    streaming_query_command_result?: StreamingQueryCommandResult;
    streamingQueryCommandResult?: StreamingQueryCommandResult;
    streaming_query_manager_command_result?: StreamingQueryManagerCommandResult;
    streamingQueryManagerCommandResult?: StreamingQueryManagerCommandResult;
    streaming_query_listener_events_result?: StreamingQueryListenerEventsResult;
    streamingQueryListenerEventsResult?: StreamingQueryListenerEventsResult;
};

/**
 * Snapshot of a streaming query's state, as `StreamingQuery.status()` reports
 * it in Spark.
 * @public
 */
export type StreamingQueryStatus = {
    /** Human-readable description of what the query is doing. */
    message: string;
    /** Whether new data is available to be processed. */
    isDataAvailable: boolean;
    /** Whether a trigger is currently processing data. */
    isTriggerActive: boolean;
    /** Whether the query is running. */
    isActive: boolean;
};

/**
 * One progress report of a streaming query. Spark serialises it as JSON; the
 * listed fields are the stable ones, everything else is kept as reported.
 * @public
 */
export type StreamingQueryProgress = {
    id: string;
    runId: string;
    name: string | null;
    timestamp: string;
    batchId: number;
    numInputRows?: number;
    inputRowsPerSecond?: number;
    processedRowsPerSecond?: number;
    durationMs?: Record<string, number>;
    sources?: Array<Record<string, unknown>>;
    sink?: Record<string, unknown>;
    stateOperators?: Array<Record<string, unknown>>;
    observedMetrics?: Record<string, unknown>;
} & Record<string, unknown>;

/**
 * Failure reported by a streaming query, with Spark's error class and remote
 * stack trace when the server provides them.
 * @public
 */
export class StreamingQueryError extends Error {
    readonly errorClass?: string;
    readonly remoteStack?: string;

    constructor(message: string, options: { errorClass?: string; remoteStack?: string } = {}) {
        super(message);
        this.name = "StreamingQueryError";
        this.errorClass = options.errorClass;
        this.remoteStack = options.remoteStack;
    }
}

export function extractStreamStart(
    response: ExecutePlanResponse,
): StreamStartResult | undefined {
    return response.write_stream_operation_start_result ?? response.writeStreamOperationStartResult;
}

export function extractStreamingQueryCommandResult(
    response: ExecutePlanResponse,
): StreamingQueryCommandResult | undefined {
    return response.streaming_query_command_result ?? response.streamingQueryCommandResult;
}

export function extractStreamingQueryManagerResult(
    response: ExecutePlanResponse,
): StreamingQueryManagerCommandResult | undefined {
    return response.streaming_query_manager_command_result ?? response.streamingQueryManagerCommandResult;
}

export function extractListenerEventsResult(
    response: ExecutePlanResponse,
): StreamingQueryListenerEventsResult | undefined {
    return response.streaming_query_listener_events_result ?? response.streamingQueryListenerEventsResult;
}

function normalizeStreamingQueryId(
    queryId?: StreamingQueryInstanceId,
): NormalizedStreamingQueryInstanceId | undefined {
    if (!queryId) return undefined;
    const id = typeof queryId.id === "string" && queryId.id.trim()
        ? queryId.id
        : undefined;
    const runId = typeof queryId.run_id === "string" && queryId.run_id.trim()
        ? queryId.run_id
        : (typeof queryId.runId === "string" && queryId.runId.trim()
            ? queryId.runId
            : undefined);
    return id && runId ? { id, run_id: runId } : undefined;
}

export function extractStreamingQueryId(
    carrier?: QueryIdCarrier,
): NormalizedStreamingQueryInstanceId | undefined {
    return normalizeStreamingQueryId(carrier?.query_id ?? carrier?.queryId);
}

/** Reads the `{ id, name }` of a query instance listed by the query manager. */
export function extractStreamingQueryInstance(
    instance?: StreamingQueryInstance,
): { queryId: NormalizedStreamingQueryInstanceId; name: string } | undefined {
    const queryId = normalizeStreamingQueryId(instance?.id);
    return queryId ? { queryId, name: instance?.name ?? "" } : undefined;
}

export function extractAwaitTerminationResult(
    result?: StreamingQueryCommandResult,
): boolean | undefined {
    const awaitTermination = result?.await_termination ?? result?.awaitTermination;
    return typeof awaitTermination?.terminated === "boolean"
        ? awaitTermination.terminated
        : undefined;
}

export function extractStreamingException(
    result?: StreamingQueryCommandResult,
): NonNullable<StreamingQueryCommandResult["exception"]> | undefined {
    return result?.exception;
}

/** Builds the public error from an `ExceptionResult`, or `undefined` when the query reported none. */
export function streamingExceptionToError(
    exception: StreamingQueryCommandResult["exception"],
): StreamingQueryError | undefined {
    const message = exception?.exception_message ?? exception?.exceptionMessage;
    if (!message) return undefined;
    return new StreamingQueryError(message, {
        errorClass: exception?.error_class ?? exception?.errorClass,
        remoteStack: exception?.stack_trace ?? exception?.stackTrace,
    });
}

export function extractStreamingStatus(
    result?: StreamingQueryCommandResult,
): StreamingQueryStatus | undefined {
    const status = result?.status;
    if (!status) return undefined;
    return {
        message: status.status_message ?? status.statusMessage ?? "",
        isDataAvailable: !!(status.is_data_available ?? status.isDataAvailable),
        isTriggerActive: !!(status.is_trigger_active ?? status.isTriggerActive),
        isActive: !!(status.is_active ?? status.isActive),
    };
}

/** Parses one progress report; Spark always serialises them as JSON objects. */
export function parseStreamingProgress(json: string): StreamingQueryProgress {
    let parsed: unknown;
    try {
        parsed = JSON.parse(json);
    } catch (error) {
        throw new Error(`Invalid streaming progress report: ${(error as Error).message}`, { cause: error });
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("Invalid streaming progress report: expected a JSON object.");
    }
    return parsed as StreamingQueryProgress;
}

export function extractRecentProgress(
    result?: StreamingQueryCommandResult,
): StreamingQueryProgress[] | undefined {
    const progress = result?.recent_progress ?? result?.recentProgress;
    if (!progress) return undefined;
    const reports = progress.recent_progress_json ?? progress.recentProgressJson ?? [];
    return reports.map(parseStreamingProgress);
}

export function validateTerminationTimeout(timeoutMs: number): void {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) {
        throw new RangeError("Streaming query timeoutMs must be a non-negative safe integer.");
    }
}

/** Copies the session identity of `request` into a fresh command request. */
export function buildCommandRequest(request: RpcMessage, command: RpcMessage): RpcMessage {
    const commandRequest: RpcMessage = {
        session_id: request.session_id,
        user_context: request.user_context,
        operation_id: crypto.randomUUID(),
        ...(request.client_observed_server_side_session_id
            ? {
                client_observed_server_side_session_id:
                    request.client_observed_server_side_session_id,
            }
            : {}),
        plan: { command },
    };

    const clientType = request.client_type;
    if (typeof clientType === "string" && clientType) {
        commandRequest.client_type = clientType;
    }
    return commandRequest;
}

export function buildStreamingQueryCommandRequest(
    request: RpcMessage,
    queryId: NormalizedStreamingQueryInstanceId,
    command: RpcMessage,
): RpcMessage {
    return buildCommandRequest(request, {
        streaming_query_command: {
            query_id: queryId,
            ...command,
        },
    });
}
