import type * as Grpc from "@grpc/grpc-js";
import { asSparkConnectError, attachSparkErrorDetails, SparkConnectError } from "./errors";
import { resolveRetryConfig, withRetry } from "./retry";
import type { RpcMessage } from "./reattachableExecution";
import type { SparkConnectionConfig } from "./session";
import { emitTelemetry } from "./telemetry";

type UnaryCallback<T> = (error: Error | null, response: T) => void;
export type UnaryMethod<T> = (
    request: RpcMessage,
    metadata: Grpc.Metadata,
    options: Grpc.CallOptions,
    callback: UnaryCallback<T>,
) => Grpc.ClientUnaryCall;

type ErrorDetailsClient = {
    fetchErrorDetails: UnaryMethod<RpcMessage>;
};

export function buildCallOptions(config?: SparkConnectionConfig): Grpc.CallOptions {
    return config?.rpcTimeoutMs === undefined
        ? {}
        : { deadline: Date.now() + config.rpcTimeoutMs };
}

function createAbortError(reason?: unknown): Error {
    const error = new Error(
        reason === undefined
            ? "The Spark Connect operation was aborted."
            : String(reason),
    );
    error.name = "AbortError";
    return error;
}

function sessionIdentityError(operation: string, message: string): SparkConnectError {
    const error = new SparkConnectError(`${operation} failed: ${message}`, {
        operation,
        errorClass: "INVALID_HANDLE.SESSION_CHANGED",
    });
    (error as SparkConnectError & { __noRetry?: boolean }).__noRetry = true;
    return error;
}

/** @internal Rejects cross-session or stale-server responses before callers consume them. */
export function assertRpcResponseSessionIntegrity(
    request: RpcMessage,
    response: unknown,
    operation: string,
): void {
    const carrier = response as {
        session_id?: unknown;
        sessionId?: unknown;
        server_side_session_id?: unknown;
        serverSideSessionId?: unknown;
    } | null;
    const expectedSessionId = request.session_id;
    const actualSessionId = carrier?.session_id ?? carrier?.sessionId;
    if (
        actualSessionId !== undefined
        && typeof expectedSessionId === "string"
        && actualSessionId !== expectedSessionId
    ) {
        throw sessionIdentityError(
            operation,
            `response session_id ${String(actualSessionId)} does not match ${expectedSessionId}`,
        );
    }

    const expectedServerId = request.client_observed_server_side_session_id;
    const actualServerId = carrier?.server_side_session_id ?? carrier?.serverSideSessionId;
    if (
        typeof expectedServerId === "string"
        && expectedServerId
        && typeof actualServerId === "string"
        && actualServerId
        && actualServerId !== expectedServerId
    ) {
        throw sessionIdentityError(
            operation,
            `server_side_session_id changed from ${expectedServerId} to ${actualServerId}`,
        );
    }
}

/** @internal Emits a distinct warning when only post-completion cleanup failed. */
export function emitExecuteCleanupTelemetry(
    config: SparkConnectionConfig | undefined,
    operationId: unknown,
    cleanupError: unknown,
): void {
    emitTelemetry(config, "spark.execute.cleanup_error", "warn", {
        operationId,
        code: (cleanupError as { code?: unknown } | null)?.code,
        errorClass: (cleanupError as { errorClass?: unknown } | null)?.errorClass,
    });
}

export function callUnary<TResponse>(
    method: UnaryMethod<TResponse>,
    request: RpcMessage,
    metadata: Grpc.Metadata,
    config?: SparkConnectionConfig,
    operation = "Spark Connect unary RPC",
): Promise<TResponse> {
    return new Promise((resolve, reject) => {
        const startedAt = Date.now();
        emitTelemetry(config, "spark.rpc.start", "debug", { rpc: operation });
        const signal = config?.signal;
        if (signal?.aborted) {
            emitTelemetry(config, "spark.rpc.cancelled", "info", {
                rpc: operation,
                durationMs: 0,
            });
            reject(createAbortError(signal.reason));
            return;
        }

        let settled = false;
        let call: Grpc.ClientUnaryCall | undefined;
        const cleanup = () => signal?.removeEventListener("abort", onAbort);
        const onAbort = () => {
            if (settled) return;
            settled = true;
            call?.cancel();
            cleanup();
            emitTelemetry(config, "spark.rpc.cancelled", "info", {
                rpc: operation,
                durationMs: Date.now() - startedAt,
            });
            reject(createAbortError(signal?.reason));
        };

        signal?.addEventListener("abort", onAbort, { once: true });
        call = method(request, metadata, buildCallOptions(config), (error, response) => {
            if (settled) return;
            settled = true;
            cleanup();
            if (error) {
                emitTelemetry(config, "spark.rpc.error", "error", {
                    rpc: operation,
                    durationMs: Date.now() - startedAt,
                    code: (error as Error & { code?: number }).code,
                });
                reject(asSparkConnectError(error, operation));
                return;
            }
            try {
                assertRpcResponseSessionIntegrity(request, response, operation);
            } catch (integrityError) {
                emitTelemetry(config, "spark.rpc.error", "error", {
                    rpc: operation,
                    durationMs: Date.now() - startedAt,
                    errorClass: "INVALID_HANDLE.SESSION_CHANGED",
                });
                reject(integrityError);
                return;
            }
            emitTelemetry(config, "spark.rpc.end", "debug", {
                rpc: operation,
                durationMs: Date.now() - startedAt,
            });
            resolve(response);
        });
    });
}

export async function enrichSparkConnectError(
    error: unknown,
    operation: string,
    request: RpcMessage,
    client: ErrorDetailsClient,
    metadata: Grpc.Metadata,
    config?: SparkConnectionConfig,
): Promise<Error> {
    const wrapped = asSparkConnectError(error, operation);
    if (!(wrapped instanceof SparkConnectError) || !wrapped.errorId) return wrapped;

    const detailRequest: RpcMessage = {
        session_id: request.session_id,
        user_context: request.user_context,
        error_id: wrapped.errorId,
        client_type: request.client_type,
        ...(request.client_observed_server_side_session_id
            ? {
                client_observed_server_side_session_id:
                    request.client_observed_server_side_session_id,
            }
            : {}),
    };
    const cleanupConfig = config ? { ...config, signal: undefined } : undefined;
    try {
        const details = await withRetry(
            () => callUnary(
                client.fetchErrorDetails.bind(client),
                detailRequest,
                metadata,
                cleanupConfig,
                "FetchErrorDetails",
            ),
            resolveRetryConfig(cleanupConfig),
        );
        return attachSparkErrorDetails(wrapped, details);
    } catch {
        // Error enrichment must never hide the original RPC failure.
        return wrapped;
    }
}

export async function callUnaryWithRetry<TResponse>(
    client: ErrorDetailsClient,
    method: UnaryMethod<TResponse>,
    request: RpcMessage,
    metadata: Grpc.Metadata,
    config: SparkConnectionConfig | undefined,
    operation: string,
): Promise<TResponse> {
    try {
        return await withRetry(
            () => callUnary(method, request, metadata, config, operation),
            resolveRetryConfig(config),
            { signal: config?.signal },
        );
    } catch (error) {
        throw await enrichSparkConnectError(
            error,
            operation,
            request,
            client,
            metadata,
            config,
        );
    }
}
