import type { RetryConfig } from "./session";
import { abortError, throwIfAborted, waitBeforeRetry } from "./retry";

export type RpcMessage = Record<string, unknown>;

export interface RpcReadable<T> {
    cancel(): void;
    pause(): void;
    resume(): void;
    on(event: "data", listener: (response: T) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (error: Error) => void): this;
}

export type ReattachableResponse = RpcMessage & {
    operation_id?: string;
    operationId?: string;
    response_id?: string;
    responseId?: string;
    server_side_session_id?: string;
    serverSideSessionId?: string;
    result_complete?: unknown;
    resultComplete?: unknown;
};

export interface ReattachableTransport<TResponse extends ReattachableResponse> {
    execute(request: RpcMessage): RpcReadable<TResponse>;
    reattach(request: RpcMessage): RpcReadable<TResponse>;
    release(request: RpcMessage): Promise<unknown>;
}

export type ReattachableExecutionOptions = {
    retry: Required<RetryConfig>;
    signal?: AbortSignal;
    /** Test hook; production uses an abortable timer. */
    sleep?: (ms: number) => Promise<void>;
    /** Test hook; production uses full jitter. */
    jitter?: (backoff: number) => number;
    /** Guards against a broken server repeatedly ending empty continuation streams. */
    maxEmptyReattachments?: number;
    /** Best-effort observer for ReleaseExecute failures after confirmed completion. */
    onCleanupError?: (error: unknown) => void;
};

const DEFAULT_MAX_EMPTY_REATTACHMENTS = 8;
const HIGH_WATER_MARK = 16;
const LOW_WATER_MARK = 8;

function requiredString(request: RpcMessage, key: string): string {
    const value = request[key];
    if (typeof value !== "string" || !value.trim()) {
        throw new Error(`Reattachable ExecutePlan requires a non-empty ${key}.`);
    }
    return value;
}

function optionalString(value: unknown): string | undefined {
    return typeof value === "string" && value ? value : undefined;
}

function responseOperationId(response: ReattachableResponse): string | undefined {
    return optionalString(response.operation_id) ?? optionalString(response.operationId);
}

function responseId(response: ReattachableResponse): string | undefined {
    return optionalString(response.response_id) ?? optionalString(response.responseId);
}

function serverSideSessionId(response: ReattachableResponse): string | undefined {
    return optionalString(response.server_side_session_id)
        ?? optionalString(response.serverSideSessionId);
}

function invalidSessionError(message: string): Error & { errorClass: string; __noRetry: true } {
    return Object.assign(new Error(message), {
        name: "SparkSessionIdentityError",
        errorClass: "INVALID_HANDLE.SESSION_CHANGED",
        __noRetry: true as const,
    });
}

function isResultComplete(response: ReattachableResponse): boolean {
    return response.result_complete != null || response.resultComplete != null;
}

function withReattachableOption(request: RpcMessage): RpcMessage {
    const inputOptions = Array.isArray(request.request_options)
        ? request.request_options as RpcMessage[]
        : [];
    let found = false;
    const requestOptions = inputOptions.map(option => {
        if (option.reattach_options == null && option.reattachOptions == null) return option;
        found = true;
        return { ...option, reattach_options: { reattachable: true } };
    });
    if (!found) requestOptions.push({ reattach_options: { reattachable: true } });
    return { ...request, request_options: requestOptions };
}

async function* readBoundedStream<T>(
    call: RpcReadable<T>,
    signal?: AbortSignal
): AsyncGenerator<T, void, void> {
    throwIfAborted(signal);
    const queue: T[] = [];
    let paused = false;
    let ended = false;
    let failure: Error | undefined;
    let wake: (() => void) | undefined;
    const notify = () => {
        const listener = wake;
        wake = undefined;
        listener?.();
    };
    const onAbort = () => {
        failure = abortError(signal?.reason);
        call.cancel();
        notify();
    };
    signal?.addEventListener("abort", onAbort, { once: true });

    call.on("data", response => {
        queue.push(response);
        if (!paused && queue.length >= HIGH_WATER_MARK) {
            paused = true;
            call.pause();
        }
        notify();
    });
    call.on("end", () => {
        ended = true;
        notify();
    });
    call.on("error", error => {
        if (failure?.name !== "AbortError") failure = error;
        notify();
    });

    try {
        for (;;) {
            if (failure?.name === "AbortError") throw failure;
            if (queue.length > 0) {
                const response = queue.shift()!;
                if (paused && queue.length <= LOW_WATER_MARK) {
                    paused = false;
                    call.resume();
                }
                yield response;
                continue;
            }
            if (failure) throw failure;
            if (ended) return;
            await new Promise<void>(resolve => { wake = resolve; });
        }
    } finally {
        signal?.removeEventListener("abort", onAbort);
        if (!ended) call.cancel();
    }
}

/**
 * Executes exactly one initial ExecutePlan and recovers only through
 * ReattachExecute. The operation is always released, including when the
 * consumer stops iterating early.
 */
export async function* executeReattachable<TResponse extends ReattachableResponse>(
    request: RpcMessage,
    transport: ReattachableTransport<TResponse>,
    options: ReattachableExecutionOptions
): AsyncGenerator<TResponse, void, void> {
    const operationId = requiredString(request, "operation_id");
    const sessionId = requiredString(request, "session_id");
    const userContext = request.user_context;
    if (userContext == null || typeof userContext !== "object") {
        throw new Error("Reattachable ExecutePlan requires user_context.");
    }

    const executeRequest = withReattachableOption(request);
    const maxEmptyReattachments = options.maxEmptyReattachments
        ?? DEFAULT_MAX_EMPTY_REATTACHMENTS;
    if (!Number.isSafeInteger(maxEmptyReattachments) || maxEmptyReattachments < 1) {
        throw new RangeError("maxEmptyReattachments must be a positive safe integer.");
    }

    let initialSent = false;
    let shouldReattach = false;
    let completed = false;
    let lastResponseId: string | undefined;
    let observedServerSessionId = optionalString(
        request.client_observed_server_side_session_id
    );
    let consecutiveFailures = 0;
    let emptyReattachments = 0;

    const reattachRequest = (): RpcMessage => ({
        session_id: sessionId,
        user_context: userContext,
        operation_id: operationId,
        ...(typeof request.client_type === "string" && request.client_type
            ? { client_type: request.client_type }
            : {}),
        ...(observedServerSessionId
            ? { client_observed_server_side_session_id: observedServerSessionId }
            : {}),
        ...(lastResponseId ? { last_response_id: lastResponseId } : {}),
    });

    try {
        while (!completed) {
            throwIfAborted(options.signal);
            let madeProgress = false;
            try {
                let call: RpcReadable<TResponse>;
                if (shouldReattach) {
                    call = transport.reattach(reattachRequest());
                } else {
                    // Mark before opening the call: even a synchronous transport
                    // failure is ambiguous, so the original plan must not replay.
                    initialSent = true;
                    shouldReattach = true;
                    call = transport.execute(executeRequest);
                }

                for await (const response of readBoundedStream(call, options.signal)) {
                    const returnedSessionId = response.session_id ?? response.sessionId;
                    if (returnedSessionId !== undefined && returnedSessionId !== sessionId) {
                        throw invalidSessionError(
                            `ExecutePlan returned session_id ${String(returnedSessionId)}; expected ${sessionId}.`
                        );
                    }
                    const returnedOperationId = responseOperationId(response);
                    if (returnedOperationId !== operationId) {
                        throw new Error(
                            `ExecutePlan returned operation_id ${String(returnedOperationId)}; ` +
                            `expected ${operationId}.`
                        );
                    }
                    const nextServerSessionId = serverSideSessionId(response);
                    if (observedServerSessionId && nextServerSessionId
                        && observedServerSessionId !== nextServerSessionId) {
                        throw invalidSessionError(
                            "ExecutePlan returned a different server_side_session_id " +
                            `(${nextServerSessionId}); expected ${observedServerSessionId}.`
                        );
                    }
                    observedServerSessionId = nextServerSessionId ?? observedServerSessionId;
                    const nextResponseId = responseId(response);
                    if (!nextResponseId) {
                        throw new Error("ExecutePlan returned a response without response_id.");
                    }
                    if (nextResponseId === lastResponseId) {
                        // Defensive at-most-once delivery if a server repeats the
                        // last acknowledged item while reattaching.
                        continue;
                    }
                    madeProgress = true;
                    consecutiveFailures = 0;
                    emptyReattachments = 0;
                    lastResponseId = nextResponseId;
                    completed = isResultComplete(response);
                    yield response;
                    if (completed) break;
                }

                if (completed) break;
                if (!madeProgress) {
                    emptyReattachments += 1;
                    if (emptyReattachments > maxEmptyReattachments) {
                        throw new Error(
                            `ReattachExecute ended ${emptyReattachments} times without a response.`
                        );
                    }
                }
            } catch (error) {
                shouldReattach = initialSent;
                consecutiveFailures += 1;
                await waitBeforeRetry(error, consecutiveFailures, options.retry, {
                    signal: options.signal,
                    sleep: options.sleep,
                    jitter: options.jitter,
                });
            }
        }
    } finally {
        if (initialSent) {
            try {
                await transport.release({
                    session_id: sessionId,
                    user_context: userContext,
                    operation_id: operationId,
                    ...(typeof request.client_type === "string" && request.client_type
                        ? { client_type: request.client_type }
                        : {}),
                    ...(observedServerSessionId
                        ? { client_observed_server_side_session_id: observedServerSessionId }
                        : {}),
                    release_all: {},
                });
            } catch (cleanupError) {
                if (completed) {
                    try {
                        options.onCleanupError?.(cleanupError);
                    } catch {
                        // A cleanup observer is diagnostic only and can never
                        // turn a confirmed execution into a failed one.
                    }
                }
            }
        }
    }
}
