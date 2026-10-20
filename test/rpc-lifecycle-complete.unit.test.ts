import { EventEmitter } from "node:events";
import * as grpc from "@grpc/grpc-js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { asSparkConnectError, SparkConnectError } from "../src/client/errors";
import type { RpcMessage } from "../src/client/reattachableExecution";
import {
    assertRpcResponseSessionIntegrity,
    buildCallOptions,
    buildStreamCallOptions,
    callUnary,
    callUnaryWithRetry,
    CLEANUP_RPC_TIMEOUT_MS,
    cleanupConnectionConfig,
    enrichSparkConnectError,
    reportExpiredDeadline,
    resolveExecuteDeadline,
    type UnaryMethod,
    withDeadlineStatus,
} from "../src/client/rpcLifecycle";
import type { SparkConnectionConfig } from "../src/client/session";

const metadata = new grpc.Metadata();
const request: RpcMessage = {
    session_id: "session-1",
    user_context: { user_id: "user-1" },
    client_type: "test-client",
};

type MutableCall = {
    cancel: ReturnType<typeof vi.fn>;
};

function clientCall(): MutableCall & grpc.ClientUnaryCall {
    return {
        cancel: vi.fn(),
    } as unknown as MutableCall & grpc.ClientUnaryCall;
}

function responding<T>(
    response: T,
    error: Error | null = null,
): UnaryMethod<T> {
    return (_request, _metadata, _options, callback) => {
        callback(error, response);
        return clientCall();
    };
}

describe("RPC lifecycle completion", () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("builds empty and deadline call options", () => {
        expect(buildCallOptions()).toEqual({});
        vi.spyOn(Date, "now").mockReturnValue(1_000);
        expect(buildCallOptions({ rpcTimeoutMs: 250 })).toEqual({ deadline: 1_250 });
    });

    it("accepts absent and matching session identities in both naming styles", () => {
        expect(() => assertRpcResponseSessionIntegrity(request, null, "Config")).not.toThrow();
        expect(() => assertRpcResponseSessionIntegrity(request, {}, "Config")).not.toThrow();
        expect(() => assertRpcResponseSessionIntegrity(request, {
            session_id: "session-1",
        }, "Config")).not.toThrow();
        expect(() => assertRpcResponseSessionIntegrity(request, {
            sessionId: "session-1",
        }, "Config")).not.toThrow();
        // FetchErrorDetailsResponse never sets session_id; proto-loader materializes
        // the proto3 default "" and that must read as "absent", not as a foreign session.
        expect(() => assertRpcResponseSessionIntegrity(request, {
            session_id: "",
        }, "FetchErrorDetails")).not.toThrow();
        expect(() => assertRpcResponseSessionIntegrity({
            ...request,
            session_id: 7,
        }, {
            session_id: "other",
        }, "Config")).not.toThrow();

        const withServer = {
            ...request,
            client_observed_server_side_session_id: "server-1",
        };
        expect(() => assertRpcResponseSessionIntegrity(withServer, {
            server_side_session_id: "server-1",
        }, "Config")).not.toThrow();
        expect(() => assertRpcResponseSessionIntegrity(withServer, {
            serverSideSessionId: "server-1",
        }, "Config")).not.toThrow();
        expect(() => assertRpcResponseSessionIntegrity(withServer, {
            server_side_session_id: "",
        }, "Config")).not.toThrow();
    });

    it("marks client and server identity mismatches as non-retryable", () => {
        for (const [checkedRequest, response] of [
            [request, { session_id: "other-session" }],
            [{
                ...request,
                client_observed_server_side_session_id: "server-1",
            }, {
                server_side_session_id: "server-2",
            }],
        ] as const) {
            try {
                assertRpcResponseSessionIntegrity(checkedRequest, response, "ExecutePlan");
                throw new Error("expected identity validation to fail");
            } catch (error) {
                expect(error).toMatchObject({
                    name: "SparkConnectError",
                    errorClass: "INVALID_HANDLE.SESSION_CHANGED",
                    __noRetry: true,
                });
            }
        }
    });

    it("resolves successful unary calls and emits telemetry", async () => {
        const logger = vi.fn();
        await expect(callUnary(
            responding({ session_id: "session-1", value: 42 }),
            request,
            metadata,
            { logger },
            "Config",
        )).resolves.toEqual({ session_id: "session-1", value: 42 });
        expect(logger).toHaveBeenCalledWith(expect.objectContaining({
            name: "spark.rpc.start",
        }));
        expect(logger).toHaveBeenCalledWith(expect.objectContaining({
            name: "spark.rpc.end",
        }));
    });

    it("wraps unary transport failures and identity failures", async () => {
        const transport = Object.assign(new Error("offline"), {
            code: grpc.status.UNAVAILABLE,
            details: "server offline",
        });
        await expect(callUnary(
            responding({}, transport),
            request,
            metadata,
            undefined,
            "AnalyzePlan",
        )).rejects.toMatchObject({
            name: "SparkConnectError",
            operation: "AnalyzePlan",
            code: grpc.status.UNAVAILABLE,
        });
        await expect(callUnary(
            responding({ session_id: "wrong" }),
            request,
            metadata,
            undefined,
            "Config",
        )).rejects.toMatchObject({
            errorClass: "INVALID_HANDLE.SESSION_CHANGED",
        });
    });

    it("rejects a signal that was aborted before dispatch", async () => {
        const method = vi.fn(responding({}));
        await expect(callUnary(method, request, metadata, {
            signal: {
                aborted: true,
                reason: undefined,
            } as unknown as AbortSignal,
        }, "Config")).rejects.toMatchObject({
            name: "AbortError",
            message: "The Spark Connect operation was aborted.",
        });
        expect(method).not.toHaveBeenCalled();
    });

    it("cancels an in-flight call and ignores its late callback", async () => {
        const controller = new AbortController();
        const call = clientCall();
        let respond: ((error: Error | null, response: RpcMessage) => void) | undefined;
        const method: UnaryMethod<RpcMessage> = (
            _request,
            _metadata,
            _options,
            callback,
        ) => {
            respond = callback;
            return call;
        };
        const pending = callUnary(method, request, metadata, {
            signal: controller.signal,
        }, "ExecutePlan");
        controller.abort("cancelled by caller");
        respond?.(null, { session_id: "session-1" });

        await expect(pending).rejects.toMatchObject({
            name: "AbortError",
            message: "cancelled by caller",
        });
        expect(call.cancel).toHaveBeenCalledOnce();
    });

    it("handles cancellation fired synchronously before the call is returned", async () => {
        const controller = new AbortController();
        const method: UnaryMethod<RpcMessage> = () => {
            controller.abort("synchronous cancellation");
            return clientCall();
        };
        await expect(callUnary(method, request, metadata, {
            signal: controller.signal,
        })).rejects.toMatchObject({
            name: "AbortError",
            message: "synchronous cancellation",
        });
    });

    it("removes cancellation after settlement and ignores duplicate callbacks", async () => {
        const controller = new AbortController();
        const call = clientCall();
        const method: UnaryMethod<RpcMessage> = (
            _request,
            _metadata,
            _options,
            callback,
        ) => {
            callback(null, { session_id: "session-1" });
            callback(new Error("too late"), {});
            return call;
        };
        await expect(callUnary(method, request, metadata, {
            signal: controller.signal,
        })).resolves.toMatchObject({ session_id: "session-1" });
        controller.abort();
        expect(call.cancel).not.toHaveBeenCalled();
    });

    it("ignores an abort callback retained by a nonconforming signal after settlement", async () => {
        let retainedAbort: (() => void) | undefined;
        const signal = {
            aborted: false,
            addEventListener(_event: string, listener: () => void) {
                retainedAbort = listener;
            },
            removeEventListener() {
                // Deliberately retain it to exercise the settled guard.
            },
        } as unknown as AbortSignal;
        await expect(callUnary(
            responding({ session_id: "session-1" }),
            request,
            metadata,
            { signal },
        )).resolves.toMatchObject({ session_id: "session-1" });
        expect(() => retainedAbort?.()).not.toThrow();
    });

    it("returns aborts and errors without IDs without fetching details", async () => {
        const fetchErrorDetails = vi.fn(responding({}));
        const client = { fetchErrorDetails };
        const aborted = Object.assign(new Error("cancelled"), { name: "AbortError" });
        await expect(enrichSparkConnectError(
            aborted,
            "Config",
            request,
            client,
            metadata,
        )).resolves.toBe(aborted);
        const ordinary = new Error("failed");
        await expect(enrichSparkConnectError(
            ordinary,
            "Config",
            request,
            client,
            metadata,
        )).resolves.toMatchObject({ cause: ordinary, errorId: undefined });
        expect(fetchErrorDetails).not.toHaveBeenCalled();
    });

    it("fetches structured details without inheriting operation cancellation", async () => {
        const controller = new AbortController();
        controller.abort("original operation stopped");
        const fetchErrorDetails = vi.fn(responding({
            session_id: "session-1",
            detail: "remote analysis",
        }));
        const original = Object.assign(new Error("analysis failed"), {
            errorId: "error-1",
        });
        const config: SparkConnectionConfig = {
            signal: controller.signal,
            retry: { maxRetries: 0 },
        };
        const enriched = await enrichSparkConnectError(
            original,
            "AnalyzePlan",
            {
                ...request,
                client_observed_server_side_session_id: "server-1",
            },
            { fetchErrorDetails },
            metadata,
            config,
        );

        expect(enriched).toMatchObject({
            errorId: "error-1",
            remoteDetails: {
                session_id: "session-1",
                detail: "remote analysis",
            },
        });
        expect(fetchErrorDetails.mock.calls[0]?.[0]).toMatchObject({
            session_id: "session-1",
            error_id: "error-1",
            client_observed_server_side_session_id: "server-1",
        });
    });

    it("keeps the original error if detail fetching fails", async () => {
        const original = Object.assign(new Error("analysis failed"), {
            errorId: "error-2",
        });
        const fetchErrorDetails = responding(
            {},
            Object.assign(new Error("detail RPC failed"), {
                code: grpc.status.INTERNAL,
            }),
        );
        const enriched = await enrichSparkConnectError(
            original,
            "AnalyzePlan",
            request,
            { fetchErrorDetails },
            metadata,
        );
        expect(enriched).toMatchObject({
            errorId: "error-2",
            remoteDetails: undefined,
            cause: original,
        });
    });

    it("retries unary failures and enriches the final error", async () => {
        let attempts = 0;
        const method: UnaryMethod<RpcMessage> = (
            _request,
            _metadata,
            _options,
            callback,
        ) => {
            attempts += 1;
            if (attempts === 1) {
                callback(Object.assign(new Error("transient"), {
                    code: grpc.status.UNAVAILABLE,
                }), {});
            } else {
                callback(null, { session_id: "session-1", ok: true });
            }
            return clientCall();
        };
        await expect(callUnaryWithRetry(
            { fetchErrorDetails: responding({}) },
            method,
            request,
            metadata,
            {
                retry: {
                    maxRetries: 1,
                    initialBackoffMs: 0,
                    maxBackoffMs: 0,
                },
            },
            "Config",
        )).resolves.toMatchObject({ ok: true });
        expect(attempts).toBe(2);

        const fetchErrorDetails = responding({ detail: true });
        await expect(callUnaryWithRetry(
            { fetchErrorDetails },
            responding({}, Object.assign(new Error("failed"), {
                errorId: "final-id",
            })),
            request,
            metadata,
            undefined,
            "Config",
        )).rejects.toMatchObject({
            errorId: "final-id",
            remoteDetails: { detail: true },
        });
    });
});

describe("cleanup and execution deadlines", () => {
    it("strips the caller signal and caps cleanup RPC deadlines", () => {
        const signal = new AbortController().signal;
        expect(cleanupConnectionConfig()).toEqual({ signal: undefined, rpcTimeoutMs: CLEANUP_RPC_TIMEOUT_MS });
        expect(cleanupConnectionConfig({ address: "sc://a:1", signal, rpcTimeoutMs: 250 }))
            .toEqual({ address: "sc://a:1", signal: undefined, rpcTimeoutMs: 250 });
        expect(cleanupConnectionConfig({ rpcTimeoutMs: 600_000 }).rpcTimeoutMs).toBe(CLEANUP_RPC_TIMEOUT_MS);
    });

    it("resolves the execution deadline once from executeTimeoutMs", () => {
        expect(resolveExecuteDeadline(undefined, 1_000)).toBeUndefined();
        expect(resolveExecuteDeadline({ rpcTimeoutMs: 5 }, 1_000)).toBeUndefined();
        expect(resolveExecuteDeadline({ executeTimeoutMs: 500 }, 1_000)).toBe(1_500);
        expect(resolveExecuteDeadline({ executeTimeoutMs: Number.POSITIVE_INFINITY }, 1_000))
            .toBe(Number.POSITIVE_INFINITY);
    });

    it("derives stream call options from the execution deadline", () => {
        const now = Date.now();
        expect(buildStreamCallOptions(undefined, undefined)).toEqual({});
        expect(buildStreamCallOptions({ rpcTimeoutMs: 1_000 }, undefined).deadline as number)
            .toBeGreaterThanOrEqual(now + 1_000);
        expect(buildStreamCallOptions({ rpcTimeoutMs: 1_000 }, 42)).toEqual({ deadline: 42 });
        expect(buildStreamCallOptions({ rpcTimeoutMs: 1_000 }, Number.POSITIVE_INFINITY)).toEqual({});
    });
});

describe("expired client deadlines", () => {
    const cancelled = (details?: string) => Object.assign(
        new Error(`1 CANCELLED: ${details ?? ""}`),
        { code: grpc.status.CANCELLED, ...(details === undefined ? {} : { details }) },
    );

    it("leaves failures alone without an expired deadline of this client", () => {
        const aborted = new AbortController();
        aborted.abort();
        const error = cancelled("Call cancelled");
        expect(reportExpiredDeadline(error, undefined, undefined, 2_000)).toBe(error);
        expect(reportExpiredDeadline(error, Number.POSITIVE_INFINITY, undefined, 2_000)).toBe(error);
        expect(reportExpiredDeadline(error, 3_000, undefined, 2_000)).toBe(error);
        expect(reportExpiredDeadline(error, 1_000, aborted.signal, 2_000)).toBe(error);
        expect(error).toMatchObject({ code: grpc.status.CANCELLED, details: "Call cancelled" });
    });

    it("leaves non-objects and other status codes alone", () => {
        const unavailable = Object.assign(new Error("14 UNAVAILABLE"), { code: grpc.status.UNAVAILABLE });
        expect(reportExpiredDeadline("offline", 1_000, undefined, 2_000)).toBe("offline");
        expect(reportExpiredDeadline(null, 1_000, undefined, 2_000)).toBeNull();
        expect(reportExpiredDeadline(unavailable, 1_000, undefined, 2_000).code).toBe(grpc.status.UNAVAILABLE);
    });

    it("reports a CANCELLED failure after the deadline as DEADLINE_EXCEEDED", () => {
        const error = cancelled("Call cancelled");
        expect(reportExpiredDeadline(error, 1_000, undefined, 1_000)).toBe(error);
        expect(error).toMatchObject({
            code: grpc.status.DEADLINE_EXCEEDED,
            details: "Deadline exceeded (the transport reported: Call cancelled)",
            message: "4 DEADLINE_EXCEEDED: Deadline exceeded (the transport reported: Call cancelled)",
        });
        expect(asSparkConnectError(error, "ExecutePlan").message)
            .toBe("ExecutePlan failed: Deadline exceeded (the transport reported: Call cancelled)");

        const withoutDetails = cancelled();
        reportExpiredDeadline(withoutDetails, new Date(1_000), new AbortController().signal, 5_000);
        expect(withoutDetails).toMatchObject({
            code: grpc.status.DEADLINE_EXCEEDED,
            details: "Deadline exceeded (the transport reported: Call cancelled)",
        });
        const emptyDetails = cancelled("");
        reportExpiredDeadline(emptyDetails, 1_000, undefined, 5_000);
        expect(emptyDetails.details).toBe("Deadline exceeded (the transport reported: Call cancelled)");
    });

    it("corrects stream errors before later listeners see them", () => {
        const call = new EventEmitter();
        expect(withDeadlineStatus(call, { deadline: Date.now() - 1 }, undefined)).toBe(call);
        const seen: number[] = [];
        call.on("error", (error: { code: number }) => seen.push(error.code));
        call.emit("error", cancelled("Call cancelled"));
        expect(seen).toEqual([grpc.status.DEADLINE_EXCEEDED]);

        const withoutDeadline = new EventEmitter();
        withDeadlineStatus(withoutDeadline, {}, undefined);
        expect(withoutDeadline.listenerCount("error")).toBe(0);

        const aborted = new AbortController();
        aborted.abort();
        const abortedCall = new EventEmitter();
        withDeadlineStatus(abortedCall, { deadline: Date.now() - 1 }, aborted.signal);
        const abortedCodes: number[] = [];
        abortedCall.on("error", (error: { code: number }) => abortedCodes.push(error.code));
        abortedCall.emit("error", cancelled("Call cancelled"));
        expect(abortedCodes).toEqual([grpc.status.CANCELLED]);
    });

    it("rejects a unary call cancelled after its rpcTimeoutMs deadline as DEADLINE_EXCEEDED", async () => {
        const logger = vi.fn();
        const late: UnaryMethod<RpcMessage> = (_request, _metadata, options, callback) => {
            const deadline = options.deadline as number;
            setTimeout(() => callback(cancelled("Call cancelled"), {}), Math.max(0, deadline - Date.now()) + 5);
            return clientCall();
        };
        await expect(callUnary(late, request, metadata, { rpcTimeoutMs: 1, logger }, "Config")).rejects.toMatchObject({
            name: "SparkConnectError",
            operation: "Config",
            code: grpc.status.DEADLINE_EXCEEDED,
        });
        expect(logger).toHaveBeenCalledWith(expect.objectContaining({
            name: "spark.rpc.error",
            attributes: expect.objectContaining({ code: grpc.status.DEADLINE_EXCEEDED }),
        }));
    });
});
