import os from "node:os";
import * as grpc from "@grpc/grpc-js";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
    asSparkConnectError,
    attachSparkErrorDetails,
    decodeGrpcStatusErrorInfo,
    SparkConnectError,
} from "../src/client/errors";
import {
    abortError,
    markNonRetryable,
    resolveRetryConfig,
    throwIfAborted,
    validateRetryConfig,
    waitBeforeRetry,
    withRetry,
} from "../src/client/retry";
import {
    cloneAuth,
    cloneSessionConfig,
    cloneTls,
    defaultUserContext,
    isEnabledConfigValue,
    isRemoteSparkConfig,
    normalizeConnectionConfig,
    syncAuthFromDraft,
    syncTlsFromDraft,
    validateSessionId,
} from "../src/client/sessionConfig";
import type { RetryConfig, SparkConnectionConfig } from "../src/client/session";
import {
    buildStreamingQueryCommandRequest,
    extractAwaitTerminationResult,
    extractStreamStart,
    extractStreamingException,
    extractStreamingQueryCommandResult,
    extractStreamingQueryId,
    validateTerminationTimeout,
} from "../src/client/streamingProtocol";
import { emitTelemetry, redactForTelemetry } from "../src/client/telemetry";

function transient(message = "unavailable"): Error & { code: number } {
    return Object.assign(new Error(message), { code: grpc.status.UNAVAILABLE });
}

function varint(value: number): Buffer {
    const bytes: number[] = [];
    let remaining = value;
    do {
        let byte = remaining & 0x7f;
        remaining = Math.floor(remaining / 128);
        if (remaining > 0) byte |= 0x80;
        bytes.push(byte);
    } while (remaining > 0);
    return Buffer.from(bytes);
}

function bytesField(number: number, value: Buffer): Buffer {
    return Buffer.concat([
        varint((number << 3) | 2),
        varint(value.length),
        value,
    ]);
}

function textField(number: number, value: string): Buffer {
    return bytesField(number, Buffer.from(value));
}

function encodedErrorInfo(errorId = "binary-id"): Buffer {
    const entry = Buffer.concat([textField(1, "errorId"), textField(2, errorId)]);
    const info = Buffer.concat([
        textField(1, "REMOTE"),
        textField(2, "spark"),
        bytesField(3, entry),
    ]);
    return bytesField(3, Buffer.concat([
        textField(1, "type.googleapis.com/google.rpc.ErrorInfo"),
        bytesField(2, info),
    ]));
}

describe("complete retry helper behavior", () => {
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it("validates observers and exercises default configuration hooks", () => {
        expect(() => validateRetryConfig({
            onRetry: "not-a-function",
        } as unknown as RetryConfig)).toThrow(TypeError);
        expect(() => resolveRetryConfig().onRetry({
            attempt: 1,
            delayMs: 0,
            error: new Error("ignored"),
        })).not.toThrow();
        expect(() => markNonRetryable(undefined)).not.toThrow();
        expect(() => markNonRetryable("primitive")).not.toThrow();
    });

    it("constructs abort errors and only throws for aborted signals", () => {
        expect(abortError()).toMatchObject({
            name: "AbortError",
            message: "The operation was aborted.",
        });
        expect(abortError(42)).toMatchObject({ name: "AbortError", message: "42" });
        expect(() => throwIfAborted()).not.toThrow();
        expect(() => throwIfAborted(new AbortController().signal)).not.toThrow();

        const controller = new AbortController();
        controller.abort("stopped");
        expect(() => throwIfAborted(controller.signal))
            .toThrow(expect.objectContaining({ name: "AbortError", message: "stopped" }));
    });

    it("rejects invalid, exhausted and non-retryable waits", async () => {
        const config = resolveRetryConfig({ retry: { maxRetries: 1 } });
        await expect(waitBeforeRetry(transient(), 0, config)).rejects.toThrow(RangeError);
        const exhausted = transient("exhausted");
        await expect(waitBeforeRetry(exhausted, 2, config)).rejects.toBe(exhausted);
        const permanent = Object.assign(new Error("permanent"), {
            code: grpc.status.INVALID_ARGUMENT,
        });
        await expect(waitBeforeRetry(permanent, 1, config)).rejects.toBe(permanent);
    });

    it("waits with custom hooks and rechecks cancellation afterwards", async () => {
        const controller = new AbortController();
        const onRetry = vi.fn();
        const sleep = vi.fn(async () => controller.abort("after sleep"));
        const config = resolveRetryConfig({
            retry: {
                maxRetries: 1,
                initialBackoffMs: 20,
                maxBackoffMs: 20,
                onRetry,
            },
        });

        await expect(waitBeforeRetry(transient(), 1, config, {
            signal: controller.signal,
            jitter: value => value / 2,
            sleep,
        })).rejects.toMatchObject({ name: "AbortError", message: "after sleep" });
        expect(sleep).toHaveBeenCalledWith(10);
        expect(onRetry).toHaveBeenCalledWith(expect.objectContaining({
            attempt: 1,
            delayMs: 10,
        }));
    });

    it("uses the default delay and jitter paths", async () => {
        vi.useFakeTimers();
        vi.spyOn(Math, "random").mockReturnValue(0.5);
        const config = resolveRetryConfig({
            retry: { maxRetries: 1, initialBackoffMs: 20, maxBackoffMs: 20 },
        });

        const waiting = waitBeforeRetry(transient(), 1, config);
        await vi.advanceTimersByTimeAsync(10);
        await expect(waiting).resolves.toBeUndefined();

        let calls = 0;
        const retried = withRetry(async () => {
            calls += 1;
            if (calls === 1) throw transient();
            return "ok";
        }, config);
        await vi.advanceTimersByTimeAsync(10);
        await expect(retried).resolves.toBe("ok");
    });

    it("aborts the default delay and custom withRetry sleep", async () => {
        const controller = new AbortController();
        const config = resolveRetryConfig({
            retry: { maxRetries: 1, initialBackoffMs: 100, maxBackoffMs: 100 },
        });
        const waiting = waitBeforeRetry(transient(), 1, config, {
            signal: controller.signal,
            jitter: value => value,
        });
        controller.abort("timer cancelled");
        await expect(waiting).rejects.toMatchObject({
            name: "AbortError",
            message: "timer cancelled",
        });

        const duringSleep = new AbortController();
        await expect(withRetry(async () => {
            throw transient();
        }, config, {
            signal: duringSleep.signal,
            jitter: value => value,
            sleep: async () => duringSleep.abort("custom sleep cancelled"),
        })).rejects.toMatchObject({
            name: "AbortError",
            message: "custom sleep cancelled",
        });
    });
});

describe("complete structured error decoding", () => {
    it("handles empty/non-matching details and every supported wire type", () => {
        expect(decodeGrpcStatusErrorInfo(Buffer.alloc(0))).toBeUndefined();
        const unrelated = Buffer.concat([
            Buffer.from([0x08, 0x01]),
            Buffer.from([0x11, 0, 0, 0, 0, 0, 0, 0, 0]),
            Buffer.from([0x1d, 0, 0, 0, 0]),
            bytesField(3, Buffer.concat([
                textField(1, "type.googleapis.com/example.Other"),
                bytesField(2, Buffer.alloc(0)),
            ])),
        ]);
        expect(decodeGrpcStatusErrorInfo(unrelated)).toBeUndefined();

        const sparseInfo = bytesField(3, Buffer.concat([
            textField(1, "type.googleapis.com/google.rpc.ErrorInfo"),
            bytesField(2, bytesField(3, textField(1, "key-without-value"))),
        ]));
        expect(decodeGrpcStatusErrorInfo(sparseInfo)).toEqual({
            reason: undefined,
            domain: undefined,
            metadata: {},
        });
    });

    it.each([
        ["field zero", Buffer.from([0x00])],
        ["unsupported wire type", Buffer.from([0x0b])],
        ["truncated fixed64", Buffer.from([0x09, 0])],
        ["truncated fixed32", Buffer.from([0x0d, 0])],
        ["truncated length", Buffer.from([0x0a, 0x02, 0])],
        ["unterminated varint", Buffer.from([0x80])],
    ])("rejects malformed protobuf: %s", (_name, payload) => {
        expect(() => decodeGrpcStatusErrorInfo(payload)).toThrow();
    });

    it("reads buffer, fallback and base64 metadata values", () => {
        const binary = encodedErrorInfo("base64-id");
        const fakeMetadata = {
            get(key: string): Array<string | Buffer> {
                if (key === "error-id") return [Buffer.from("buffer-id")];
                return [];
            },
        } as unknown as grpc.Metadata;
        const wrapped = asSparkConnectError(
            Object.assign(new Error("failed"), { metadata: fakeMetadata }),
            "ExecutePlan",
        ) as SparkConnectError;
        expect(wrapped.errorId).toBe("buffer-id");

        const fallbackMetadata = {
            get(key: string): Array<string | Buffer> {
                if (key === "error_id") return ["fallback-id"];
                return [];
            },
        } as unknown as grpc.Metadata;
        expect((asSparkConnectError(
            Object.assign(new Error("failed"), { metadata: fallbackMetadata }),
            "Config",
        ) as SparkConnectError).errorId).toBe("fallback-id");

        const base64Only = {
            get(key: string): Array<string | Buffer> {
                return key === "grpc-status-details-bin"
                    ? [binary.toString("base64")]
                    : [];
            },
        } as unknown as grpc.Metadata;
        expect((asSparkConnectError(
            Object.assign(new Error("failed"), { metadata: base64Only }),
            "AnalyzePlan",
        ) as SparkConnectError).errorId).toBe("base64-id");
    });

    it("wraps primitive causes, honors direct info and preserves no-retry markers", () => {
        expect(asSparkConnectError("plain failure", "Config")).toMatchObject({
            message: "Config failed: plain failure",
            cause: "plain failure",
        });

        const source = Object.assign(new Error("direct"), {
            errorInfo: {
                reason: "DIRECT_REASON",
                metadata: { errorId: "direct-id", errorClass: "DIRECT_CLASS" },
            },
            __noRetry: true,
        });
        const wrapped = asSparkConnectError(source, "ExecutePlan") as SparkConnectError;
        expect(wrapped).toMatchObject({
            errorId: "direct-id",
            errorClass: "DIRECT_CLASS",
            __noRetry: true,
        });
        expect(attachSparkErrorDetails(wrapped, { detail: true }))
            .toMatchObject({ remoteDetails: { detail: true }, __noRetry: true });
    });
});

describe("complete connection normalization", () => {
    const originalUrl = process.env.SPARK_CONNECT_URL;
    const originalUser = process.env.USER;
    const originalUsername = process.env.USERNAME;

    afterEach(() => {
        vi.restoreAllMocks();
        if (originalUrl === undefined) delete process.env.SPARK_CONNECT_URL;
        else process.env.SPARK_CONNECT_URL = originalUrl;
        if (originalUser === undefined) delete process.env.USER;
        else process.env.USER = originalUser;
        if (originalUsername === undefined) delete process.env.USERNAME;
        else process.env.USERNAME = originalUsername;
    });

    it("classifies connection-only, header and remote keys", () => {
        expect(isRemoteSparkConfig("spark.connect.url")).toBe(false);
        expect(isRemoteSparkConfig("spark.connect.header.request-id")).toBe(false);
        expect(isRemoteSparkConfig("spark.sql.shuffle.partitions")).toBe(true);
        expect(isEnabledConfigValue(true)).toBe(true);
        expect(isEnabledConfigValue("TRUE")).toBe(true);
        expect(isEnabledConfigValue(undefined)).toBe(false);
    });

    it("validates IDs, callback types, timeout and gRPC limits", () => {
        const id = "00112233-4455-4677-8899-aabbccddeeff";
        expect(validateSessionId(id)).toBe(id);
        expect(() => validateSessionId("not-a-uuid")).toThrow(TypeError);
        expect(() => normalizeConnectionConfig({
            logger: "bad",
        } as unknown as SparkConnectionConfig)).toThrow(TypeError);
        expect(() => normalizeConnectionConfig({
            metrics: "bad",
        } as unknown as SparkConnectionConfig)).toThrow(TypeError);
        for (const rpcTimeoutMs of [0, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
            expect(() => normalizeConnectionConfig({ rpcTimeoutMs })).toThrow(RangeError);
        }
        for (const value of [0, -1, 1.5, 2_147_483_648]) {
            expect(() => normalizeConnectionConfig({
                grpcMaxReceiveMessageBytes: value,
            })).toThrow(RangeError);
            expect(() => normalizeConnectionConfig({
                grpcMaxSendMessageBytes: value,
            })).toThrow(RangeError);
        }
    });

    it("chooses addresses in explicit, session, environment and default order", () => {
        expect(normalizeConnectionConfig({
            address: " sc://explicit:1 ",
            sessionConfig: { "spark.connect.url": "sc://ignored:2" },
        }).address).toBe("sc://explicit:1");
        expect(normalizeConnectionConfig({
            sessionConfig: {
                "spark.connect.url": " ",
                "spark.connect.address": " sc://session:3 ",
            },
        }).address).toBe("sc://session:3");

        process.env.SPARK_CONNECT_URL = " sc://environment:4 ";
        expect(normalizeConnectionConfig().address).toBe("sc://environment:4");
        delete process.env.SPARK_CONNECT_URL;
        expect(normalizeConnectionConfig().address).toBe("sc://localhost:15002");
    });

    it("migrates complete legacy auth and strips secrets from session config", () => {
        expect(normalizeConnectionConfig({
            sessionConfig: {
                "spark.auth.type": "token",
                "spark.auth.token": " legacy-token ",
                "spark.sql.answer": 42,
            },
        })).toMatchObject({
            auth: { type: "token", token: "legacy-token" },
            sessionConfig: { "spark.sql.answer": 42 },
        });
        expect(normalizeConnectionConfig({
            sessionConfig: {
                "spark.auth.type": "basic",
                "spark.auth.username": " user ",
                "spark.auth.password": " pass ",
            },
        }).auth).toEqual({ type: "basic", username: "user", password: "pass" });
        expect(normalizeConnectionConfig({
            sessionConfig: { "spark.auth.type": "token" },
        }).auth).toBeUndefined();
        expect(normalizeConnectionConfig({
            sessionConfig: {
                "spark.auth.type": "token",
                "spark.auth.token": " ",
            },
        }).auth).toBeUndefined();
        expect(normalizeConnectionConfig({
            sessionConfig: {
                "spark.auth.type": "basic",
                "spark.auth.username": "user",
            },
        }).auth).toBeUndefined();
        expect(normalizeConnectionConfig({
            auth: { type: "token", token: "explicit" },
            sessionConfig: {
                "spark.auth.type": "token",
                "spark.auth.token": "legacy",
            },
        }).auth).toEqual({ type: "token", token: "explicit" });
    });

    it("migrates enabled and material-based legacy TLS", () => {
        expect(normalizeConnectionConfig({
            sessionConfig: { "spark.ssl.enabled": true },
        }).tls).toEqual({});
        expect(normalizeConnectionConfig({
            sessionConfig: {
                "spark.connect.grpc.ssl.enabled": "true",
                "spark.ssl.certChain": " cert.pem ",
                "spark.ssl.privateKey": " key.pem ",
                "spark.ssl.serverNameOverride": " spark.internal ",
            },
        }).tls).toEqual({
            certChainPath: "cert.pem",
            privateKeyPath: "key.pem",
            serverNameOverride: "spark.internal",
        });
        expect(normalizeConnectionConfig({
            sessionConfig: { "spark.ssl.trustStore": " trust.p12 " },
        }).tls).toMatchObject({ trustStorePath: "trust.p12" });
        expect(normalizeConnectionConfig().tls).toBeUndefined();
        expect(normalizeConnectionConfig({
            tls: { serverNameOverride: "explicit" },
            sessionConfig: { "spark.ssl.serverNameOverride": "legacy" },
        }).tls).toEqual({ serverNameOverride: "explicit" });
    });

    it("clones optional values and synchronizes builder drafts", () => {
        const sessionConfig = { answer: 42 };
        const auth = { type: "token" as const, token: "secret" };
        const tls = { serverNameOverride: "spark" };
        expect(cloneSessionConfig()).toEqual({});
        expect(cloneSessionConfig(sessionConfig)).not.toBe(sessionConfig);
        expect(cloneAuth()).toBeUndefined();
        expect(cloneAuth(auth)).toEqual(auth);
        expect(cloneAuth(auth)).not.toBe(auth);
        expect(cloneTls()).toBeUndefined();
        expect(cloneTls(tls)).toEqual(tls);
        expect(cloneTls(tls)).not.toBe(tls);

        expect(syncAuthFromDraft({ type: "token", token: "secret" }))
            .toEqual({ type: "token", token: "secret" });
        expect(syncAuthFromDraft({ type: "token" })).toBeUndefined();
        expect(syncAuthFromDraft({
            type: "basic",
            username: "user",
            password: "pass",
        })).toEqual({ type: "basic", username: "user", password: "pass" });
        expect(syncAuthFromDraft({ type: "basic", username: "user" })).toBeUndefined();
        expect(syncAuthFromDraft({})).toBeUndefined();

        expect(syncTlsFromDraft({ enabled: true })).toEqual({});
        expect(syncTlsFromDraft({ certChainPath: "cert.pem" }))
            .toEqual({ certChainPath: "cert.pem" });
        expect(syncTlsFromDraft({ enabled: false })).toBeUndefined();
    });

    it("builds user context from OS and survives lookup failures", () => {
        vi.spyOn(os, "userInfo").mockReturnValue({
            username: "os-user",
            uid: 1,
            gid: 1,
            shell: null,
            homedir: "/tmp",
        });
        expect(defaultUserContext()).toEqual({
            user_id: "os-user",
            user_name: "os-user",
        });

        vi.mocked(os.userInfo).mockReturnValue({
            username: "",
            uid: 1,
            gid: 1,
            shell: null,
            homedir: "/tmp",
        });
        process.env.USER = "fallback-user";
        expect(defaultUserContext()).toEqual({
            user_id: "fallback-user",
            user_name: "fallback-user",
        });

        vi.mocked(os.userInfo).mockImplementation(() => {
            throw new Error("not supported");
        });
        delete process.env.USER;
        process.env.USERNAME = "environment-user";
        expect(defaultUserContext()).toEqual({
            user_id: "environment-user",
            user_name: "environment-user",
        });

        delete process.env.USER;
        delete process.env.USERNAME;
        expect(defaultUserContext()).toEqual({
            user_id: "ts-spark-connector",
            user_name: "ts-spark-connector",
        });
    });
});

describe("complete streaming protocol helpers", () => {
    it("supports snake_case and camelCase response variants", () => {
        const snakeStart = { query_id: { id: "query", run_id: "run" } };
        const camelStart = { queryId: { id: "query", runId: "run" } };
        expect(extractStreamStart({
            write_stream_operation_start_result: snakeStart,
        })).toBe(snakeStart);
        expect(extractStreamStart({
            writeStreamOperationStartResult: camelStart,
        })).toBe(camelStart);
        expect(extractStreamStart({})).toBeUndefined();

        const snakeResult = { await_termination: { terminated: false } };
        const camelResult = { awaitTermination: { terminated: true } };
        expect(extractStreamingQueryCommandResult({
            streaming_query_command_result: snakeResult,
        })).toBe(snakeResult);
        expect(extractStreamingQueryCommandResult({
            streamingQueryCommandResult: camelResult,
        })).toBe(camelResult);
        expect(extractStreamingQueryCommandResult({})).toBeUndefined();
        expect(extractAwaitTerminationResult(snakeResult)).toBe(false);
        expect(extractAwaitTerminationResult(camelResult)).toBe(true);
        expect(extractAwaitTerminationResult({})).toBeUndefined();
        expect(extractAwaitTerminationResult()).toBeUndefined();
    });

    it("normalizes complete IDs and rejects missing or blank parts", () => {
        expect(extractStreamingQueryId({
            query_id: { id: "query", run_id: "run" },
        })).toEqual({ id: "query", run_id: "run" });
        expect(extractStreamingQueryId({
            queryId: { id: "query", runId: "camel-run" },
        })).toEqual({ id: "query", run_id: "camel-run" });
        expect(extractStreamingQueryId()).toBeUndefined();
        expect(extractStreamingQueryId({})).toBeUndefined();
        expect(extractStreamingQueryId({ query_id: { id: "", run_id: "run" } }))
            .toBeUndefined();
        expect(extractStreamingQueryId({ query_id: { id: "query", run_id: " " } }))
            .toBeUndefined();
        expect(extractStreamingQueryId({
            query_id: { id: "query", run_id: "", runId: "fallback" },
        })).toEqual({ id: "query", run_id: "fallback" });
    });

    it("extracts exceptions, validates timeouts and builds both request forms", () => {
        const exception = { exceptionMessage: "failed" };
        expect(extractStreamingException({ exception })).toBe(exception);
        expect(extractStreamingException()).toBeUndefined();
        expect(() => validateTerminationTimeout(0)).not.toThrow();
        expect(() => validateTerminationTimeout(Number.MAX_SAFE_INTEGER)).not.toThrow();
        expect(() => validateTerminationTimeout(-1)).toThrow(RangeError);
        expect(() => validateTerminationTimeout(1.5)).toThrow(RangeError);

        const minimal = buildStreamingQueryCommandRequest(
            {
                session_id: "session",
                user_context: { user_id: "user" },
            },
            { id: "query", run_id: "run" },
            { stop: {} },
        );
        expect(minimal).toMatchObject({
            session_id: "session",
            user_context: { user_id: "user" },
            operation_id: expect.any(String),
            plan: {
                command: {
                    streaming_query_command: {
                        query_id: { id: "query", run_id: "run" },
                        stop: {},
                    },
                },
            },
        });
        expect(minimal).not.toHaveProperty("client_type");
        expect(minimal).not.toHaveProperty("client_observed_server_side_session_id");

        const full = buildStreamingQueryCommandRequest(
            {
                session_id: "session",
                user_context: { user_id: "user" },
                client_type: "client",
                client_observed_server_side_session_id: "server-session",
            },
            { id: "query", run_id: "run" },
            { status: {} },
        );
        expect(full).toMatchObject({
            client_type: "client",
            client_observed_server_side_session_id: "server-session",
        });
    });
});

describe("complete telemetry branches", () => {
    it("redacts primitives, buffers, arrays and circular structures", () => {
        const circular: { value: string; self?: unknown } = { value: "ok" };
        circular.self = circular;
        expect(redactForTelemetry(null)).toBeNull();
        expect(redactForTelemetry(42)).toBe(42);
        expect(redactForTelemetry(Buffer.from("secret"))).toBe("[Buffer 6 bytes]");
        expect(redactForTelemetry(["Bearer hidden", { password: "hidden" }]))
            .toEqual(["Bearer [REDACTED]", { password: "[REDACTED]" }]);
        expect(redactForTelemetry(circular)).toEqual({
            value: "ok",
            self: "[Circular]",
        });
    });

    it("isolates rejected observers and skips invalid duration histograms", async () => {
        const logger = vi.fn(() => Promise.reject(new Error("async logger failure")));
        const metrics = vi.fn(() => Promise.reject(new Error("async metrics failure")));
        emitTelemetry({ logger, metrics }, "spark.rpc.end", "debug", {
            durationMs: Number.NaN,
        });
        emitTelemetry({ metrics }, "spark.rpc.end", "debug", {
            durationMs: "12",
        });
        await Promise.resolve();
        await Promise.resolve();
        expect(logger).toHaveBeenCalledTimes(1);
        expect(metrics).toHaveBeenCalledTimes(2);
    });
});
