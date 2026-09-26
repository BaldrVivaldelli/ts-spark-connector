import crypto from "node:crypto";
import type { ReattachableResponse, RpcMessage } from "./reattachableExecution";

type StreamingQueryInstanceId = {
    id?: string;
    run_id?: string;
    runId?: string;
};

type NormalizedStreamingQueryInstanceId = {
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
};

export type StreamingQueryCommandResult = QueryIdCarrier & {
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

export type ExecutePlanResponse = ReattachableResponse & {
    write_stream_operation_start_result?: StreamStartResult;
    writeStreamOperationStartResult?: StreamStartResult;
    streaming_query_command_result?: StreamingQueryCommandResult;
    streamingQueryCommandResult?: StreamingQueryCommandResult;
};

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

export function validateTerminationTimeout(timeoutMs: number): void {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) {
        throw new RangeError("Streaming query timeoutMs must be a non-negative safe integer.");
    }
}

export function buildStreamingQueryCommandRequest(
    request: RpcMessage,
    queryId: NormalizedStreamingQueryInstanceId,
    command: RpcMessage,
): RpcMessage {
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
        plan: {
            command: {
                streaming_query_command: {
                    query_id: queryId,
                    ...command,
                },
            },
        },
    };

    const clientType = request.client_type;
    if (typeof clientType === "string" && clientType) {
        commandRequest.client_type = clientType;
    }
    return commandRequest;
}
