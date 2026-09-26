import type * as Grpc from "@grpc/grpc-js";
import fs from "node:fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
    assertSecureAuthTransport,
    buildChannelCredentials,
    buildChannelOptions,
    getClientReferenceCount,
    getClientCacheKey,
    sparkGrpcClient,
} from "../src/client/sparkClient";
import type {
    ReattachableResponse,
    RpcMessage,
    RpcReadable,
} from "../src/client/reattachableExecution";
import type { SparkConnectionConfig } from "../src/client/session";

type StreamEvent =
    | { kind: "data"; value: ReattachableResponse }
    | { kind: "end" }
    | { kind: "error"; error: Error };

class FakeStream implements RpcReadable<ReattachableResponse> {
    private readonly listeners = {
        data: [] as Array<(value: ReattachableResponse) => void>,
        end: [] as Array<() => void>,
        error: [] as Array<(error: Error) => void>,
    };
    readonly cancel = vi.fn();
    readonly pause = vi.fn();
    readonly resume = vi.fn();

    constructor(events: StreamEvent[]) {
        queueMicrotask(() => {
            for (const event of events) {
                if (event.kind === "data") {
                    this.listeners.data.forEach(listener => listener(event.value));
                } else if (event.kind === "end") {
                    this.listeners.end.forEach(listener => listener());
                } else {
                    this.listeners.error.forEach(listener => listener(event.error));
                }
            }
        });
    }

    on(event: "data", listener: (response: ReattachableResponse) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (error: Error) => void): this;
    on(
        event: "data" | "end" | "error",
        listener: ((response: ReattachableResponse) => void) | (() => void)
            | ((error: Error) => void),
    ): this {
        this.listeners[event].push(listener as never);
        return this;
    }
}

type UnaryName =
    | "releaseExecute"
    | "analyzePlan"
    | "config"
    | "interrupt"
    | "releaseSession"
    | "fetchErrorDetails";

type UnaryOutcome = RpcMessage | Error;

const state = {
    instances: [] as FakeService[],
    executeStreams: [] as StreamEvent[][],
    reattachStreams: [] as StreamEvent[][],
    unary: new Map<UnaryName, UnaryOutcome[]>(),
    calls: [] as Array<{
        name: string;
        request: RpcMessage;
        metadata: Grpc.Metadata;
        options: Grpc.CallOptions;
    }>,
};

class FakeService {
    readonly close = vi.fn();
    readonly address: string;

    constructor(
        address: string,
        _credentials: Grpc.ChannelCredentials,
        _options?: Grpc.ChannelOptions,
    ) {
        this.address = address;
        state.instances.push(this);
    }

    executePlan(
        request: RpcMessage,
        metadata: Grpc.Metadata,
        options: Grpc.CallOptions,
    ): RpcReadable<ReattachableResponse> {
        state.calls.push({ name: "executePlan", request, metadata, options });
        return new FakeStream(state.executeStreams.shift() ?? []);
    }

    reattachExecute(
        request: RpcMessage,
        metadata: Grpc.Metadata,
        options: Grpc.CallOptions,
    ): RpcReadable<ReattachableResponse> {
        state.calls.push({ name: "reattachExecute", request, metadata, options });
        return new FakeStream(state.reattachStreams.shift() ?? []);
    }

    releaseExecute = this.unary("releaseExecute");
    analyzePlan = this.unary("analyzePlan");
    config = this.unary("config");
    interrupt = this.unary("interrupt");
    releaseSession = this.unary("releaseSession");
    fetchErrorDetails = this.unary("fetchErrorDetails");

    private unary(name: UnaryName) {
        return (
            request: RpcMessage,
            metadata: Grpc.Metadata,
            options: Grpc.CallOptions,
            callback: (error: Error | null, response: RpcMessage) => void,
        ): Grpc.ClientUnaryCall => {
            state.calls.push({ name, request, metadata, options });
            const outcome = state.unary.get(name)?.shift() ?? {};
            if (outcome instanceof Error) callback(outcome, {});
            else callback(null, outcome);
            return { cancel: vi.fn() } as unknown as Grpc.ClientUnaryCall;
        };
    }
}

function setUnary(name: UnaryName, ...outcomes: UnaryOutcome[]): void {
    state.unary.set(name, outcomes);
}

let sequence = 0;
function connection(extra: SparkConnectionConfig = {}): SparkConnectionConfig {
    sequence += 1;
    return {
        address: `sc://fake-${sequence}:15002`,
        ...extra,
    };
}

function executeRequest(extra: RpcMessage = {}): RpcMessage {
    return {
        session_id: "session-1",
        user_context: { user_id: "user-1" },
        client_type: "test-client",
        operation_id: `operation-${sequence}`,
        ...extra,
    };
}

function complete(
    request: RpcMessage,
    extra: ReattachableResponse = {},
): StreamEvent[] {
    return [
        {
            kind: "data",
            value: {
                operation_id: request.operation_id as string,
                response_id: "response-complete",
                result_complete: {},
                ...extra,
            },
        },
        { kind: "end" },
    ];
}

describe("spark gRPC client complete behavior", () => {
    beforeEach(() => {
        const grpcRuntime = require("@grpc/grpc-js") as typeof import("@grpc/grpc-js");
        const protoLoader = require("@grpc/proto-loader") as typeof import("@grpc/proto-loader");
        vi.spyOn(protoLoader, "loadSync").mockReturnValue({});
        vi.spyOn(grpcRuntime, "loadPackageDefinition").mockReturnValue({
            spark: {
                connect: {
                    SparkConnectService: FakeService,
                },
            },
        } as unknown as Grpc.GrpcObject);
    });

    afterEach(() => {
        state.executeStreams.length = 0;
        state.reattachStreams.length = 0;
        state.unary.clear();
        state.calls.length = 0;
        vi.restoreAllMocks();
    });

    it("loads the service lazily, builds auth/header metadata and reuses a cached channel", async () => {
        const config = connection({
            allowInsecureAuth: true,
            auth: { type: "token", token: "secret" },
            sessionConfig: {
                "spark.connect.header.x-request-id": "request-1",
                "spark.connect.header.": "ignored",
            },
        });
        setUnary("config", { ok: true }, { ok: true });

        await expect(sparkGrpcClient.config({ operation: {} }, config))
            .resolves.toMatchObject({ ok: true });
        await expect(sparkGrpcClient.config({ operation: {} }, config))
            .resolves.toMatchObject({ ok: true });

        const calls = state.calls.filter(call => call.name === "config");
        expect(calls).toHaveLength(2);
        expect(calls[0]?.metadata.get("authorization")).toEqual(["Bearer secret"]);
        expect(calls[0]?.metadata.get("x-request-id")).toEqual(["request-1"]);
        expect(state.instances.filter(instance => instance.address === config.address?.slice(5)))
            .toHaveLength(1);
        sparkGrpcClient.close(config);
    });

    it("builds Basic and legacy authentication metadata", async () => {
        const basic = connection({
            allowInsecureAuth: true,
            auth: { type: "basic", username: "alice", password: "secret" },
        });
        const legacy = connection({
            allowInsecureAuth: true,
            sessionConfig: {
                "spark.auth.type": "token",
                "spark.auth.token": " legacy-token ",
            },
        });
        const legacyBasic = connection({
            allowInsecureAuth: true,
            sessionConfig: {
                "spark.auth.type": "basic",
                "spark.auth.username": "legacy-user",
                "spark.auth.password": "legacy-pass",
            },
        });
        setUnary("interrupt", {}, {}, {});
        await sparkGrpcClient.interrupt({}, basic);
        await sparkGrpcClient.interrupt({}, legacy);
        await sparkGrpcClient.interrupt({}, legacyBasic);

        const calls = state.calls.filter(call => call.name === "interrupt");
        expect(calls[0]?.metadata.get("authorization")[0]).toMatch(/^Basic /);
        expect(calls[1]?.metadata.get("authorization")).toEqual(["Bearer legacy-token"]);
        expect(calls[2]?.metadata.get("authorization")[0]).toMatch(/^Basic /);
        sparkGrpcClient.close(basic);
        sparkGrpcClient.close(legacy);
        sparkGrpcClient.close(legacyBasic);
    });

    it("covers legacy/incomplete authorization headers and bare channel options", () => {
        expect(() => assertSecureAuthTransport({
            address: "sc://spark:15002",
            sessionConfig: {
                unrelated: "value",
                "spark.connect.header.x-id": "id",
                "spark.connect.header.Authorization": "Basic encoded",
            },
        })).toThrow(/Basic credentials/);
        expect(() => assertSecureAuthTransport({
            address: "sc://spark:15002",
            sessionConfig: {
                "spark.auth.type": "basic",
                "spark.auth.username": "missing-password",
                "spark.connect.header.Authorization": "Digest value",
            },
        })).not.toThrow();
        expect(() => assertSecureAuthTransport({
            address: "sc://spark:15002",
            sessionConfig: {
                "spark.auth.type": "token",
                "spark.auth.token": " ",
            },
        })).not.toThrow();
        expect(getClientCacheKey({ address: "spark:15002" })).toContain('"scheme":"bare"');
        expect(getClientCacheKey({
            address: "sc://spark:15002",
            sessionConfig: { unrelated: "value" },
        })).toContain('"secure":false');
        expect(buildChannelCredentials({
            sessionConfig: { "spark.ssl.enabled": true },
        })._isSecure()).toBe(true);
        expect(buildChannelOptions({
            tls: { serverNameOverride: "spark.internal" },
        })).toMatchObject({
            "grpc.ssl_target_name_override": "spark.internal",
            "grpc.default_authority": "spark.internal",
        });
    });

    it("propagates non-ENOENT TLS fingerprint read errors", () => {
        const originalRead = fs.readFileSync.bind(fs);
        vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
            if (String(file).includes("denied-ca.pem")) {
                throw Object.assign(new Error("permission denied"), { code: "EACCES" });
            }
            return originalRead(file, options as never);
        });
        expect(() => getClientCacheKey({
            address: "scs://spark:15002",
            tls: { trustStorePath: "/denied-ca.pem" },
        })).toThrow(/permission denied/);
    });

    it("executes and reattaches an incremental plan before releasing it", async () => {
        const config = connection({ retry: { maxRetries: 1, initialBackoffMs: 0 } });
        const request = executeRequest();
        state.executeStreams.push([
            {
                kind: "data",
                value: {
                    operation_id: request.operation_id as string,
                    response_id: "response-1",
                    server_side_session_id: "server-1",
                },
            },
            {
                kind: "error",
                error: Object.assign(new Error("connection lost"), { code: 14 }),
            },
        ]);
        state.reattachStreams.push(complete(request, {
            response_id: "response-2",
            serverSideSessionId: "server-1",
        }));
        setUnary("releaseExecute", {});
        const logger = vi.fn();

        const values: ReattachableResponse[] = [];
        for await (const value of sparkGrpcClient.executePlanStream(request, {
            ...config,
            logger,
        })) values.push(value);

        expect(values.map(value => value.response_id)).toEqual(["response-1", "response-2"]);
        expect(state.calls.some(call => call.name === "reattachExecute")).toBe(true);
        expect(logger).toHaveBeenCalledWith(expect.objectContaining({
            name: "spark.execute.reattach",
        }));
        expect(state.calls.find(call => call.name === "releaseExecute")?.request)
            .toMatchObject({
                client_observed_server_side_session_id: "server-1",
            });
        sparkGrpcClient.close(config);
    });

    it("collects streams and enriches execution failures with remote details", async () => {
        const config = connection();
        const request = executeRequest();
        state.executeStreams.push(complete(request));
        setUnary("releaseExecute", {});
        await expect(sparkGrpcClient.executePlan(request, config)).resolves.toHaveLength(1);

        const failedRequest = executeRequest();
        const failure = Object.assign(new Error("bad operation"), {
            errorId: "error-1",
            code: 13,
        });
        state.executeStreams.push([{ kind: "error", error: failure }]);
        setUnary("releaseExecute", {}, {});
        setUnary("fetchErrorDetails", {
            detail: "remote detail",
            session_id: "session-1",
        });
        await expect(sparkGrpcClient.executePlan(failedRequest, config))
            .rejects.toMatchObject({
                errorId: "error-1",
                remoteDetails: { detail: "remote detail" },
            });
        sparkGrpcClient.close(config);
    });

    it("reports cleanup failure after successful execution without failing results", async () => {
        const logger = vi.fn();
        const config = connection({ logger });
        const request = executeRequest();
        state.executeStreams.push(complete(request));
        setUnary("releaseExecute", Object.assign(new Error("release failed"), {
            code: 13,
        }));

        await expect(sparkGrpcClient.executePlan(request, config)).resolves.toHaveLength(1);
        expect(logger).toHaveBeenCalledWith(expect.objectContaining({
            name: "spark.execute.cleanup_error",
        }));
        sparkGrpcClient.close(config);
    });

    it("executes with default configuration and enriches after an observed server identity", async () => {
        const request = executeRequest();
        state.executeStreams.push(complete(request));
        setUnary("releaseExecute", {});
        await expect(sparkGrpcClient.executePlan(request)).resolves.toHaveLength(1);
        sparkGrpcClient.close();

        const config = connection();
        const failed = executeRequest();
        state.executeStreams.push([
            {
                kind: "data",
                value: {
                    operation_id: failed.operation_id as string,
                    response_id: "first",
                    server_side_session_id: "server-observed",
                },
            },
            {
                kind: "error",
                error: Object.assign(new Error("failed later"), { code: 13 }),
            },
        ]);
        setUnary("releaseExecute", {});
        await expect(sparkGrpcClient.executePlan(failed, config)).rejects.toThrow(/failed later/);
        sparkGrpcClient.close(config);
    });

    it("extracts every explain response shape and rejects a missing explanation", async () => {
        const config = connection();
        setUnary(
            "analyzePlan",
            { explain: { explain_string: "snake nested" } },
            { explain: { explainString: "camel nested" } },
            { explain_string: "snake root" },
            { explainString: "camel root" },
            {},
            { explainString: "shortcut" },
        );
        await expect(sparkGrpcClient.explainWithResponse({}, config))
            .resolves.toMatchObject({ explainString: "snake nested" });
        await expect(sparkGrpcClient.explainWithResponse({}, config))
            .resolves.toMatchObject({ explainString: "camel nested" });
        await expect(sparkGrpcClient.explainWithResponse({}, config))
            .resolves.toMatchObject({ explainString: "snake root" });
        await expect(sparkGrpcClient.explainWithResponse({}, config))
            .resolves.toMatchObject({ explainString: "camel root" });
        await expect(sparkGrpcClient.explainWithResponse({}, config))
            .rejects.toThrow(/expected explain string/);
        await expect(sparkGrpcClient.explain({}, config)).resolves.toBe("shortcut");
        sparkGrpcClient.close(config);
    });

    it("delegates every unary operation including retrying error-detail fetches", async () => {
        const config = connection({
            retry: { maxRetries: 1, initialBackoffMs: 0, maxBackoffMs: 0 },
        });
        setUnary("analyzePlan", { analyzed: true });
        setUnary("interrupt", { interrupted: true });
        setUnary("releaseSession", { released: true });
        setUnary(
            "fetchErrorDetails",
            Object.assign(new Error("temporary"), { code: 14 }),
            { details: true },
        );

        await expect(sparkGrpcClient.analyze({}, config))
            .resolves.toMatchObject({ analyzed: true });
        await expect(sparkGrpcClient.interrupt({}, config))
            .resolves.toMatchObject({ interrupted: true });
        await expect(sparkGrpcClient.releaseSession({}, config))
            .resolves.toMatchObject({ released: true });
        await expect(sparkGrpcClient.fetchErrorDetails({}, config))
            .resolves.toMatchObject({ details: true });
        sparkGrpcClient.close(config);
    });

    it("tracks retained references and closes only the final owner", async () => {
        const config = connection();
        const firstIdentity = sparkGrpcClient.retain(config);
        const secondIdentity = sparkGrpcClient.retain(config);
        expect(firstIdentity).toBe(secondIdentity);
        expect(getClientReferenceCount(config)).toBe(2);
        setUnary("config", {});
        await sparkGrpcClient.config({}, config);
        const instance = state.instances.at(-1);

        sparkGrpcClient.close(config);
        expect(getClientReferenceCount(config)).toBe(1);
        expect(instance?.close).not.toHaveBeenCalled();
        sparkGrpcClient.close(config);
        expect(getClientReferenceCount(config)).toBe(0);
        expect(instance?.close).toHaveBeenCalledOnce();
        sparkGrpcClient.close(config);
    });
});

describe("streaming handle completion", () => {
    afterEach(() => vi.restoreAllMocks());

    const startRequest: RpcMessage = {
        session_id: "session-1",
        user_context: { user_id: "user-1" },
        client_type: "test-client",
        operation_id: "stream-operation",
    };

    it("rejects absent starts and malformed query IDs", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([])
            .mockResolvedValueOnce([{
                writeStreamOperationStartResult: {
                    queryId: { id: "query-only" },
                },
            }]);
        await expect(sparkGrpcClient.executePlanStreaming(startRequest))
            .rejects.toThrow(/before reporting a start/i);
        await expect(sparkGrpcClient.executePlanStreaming(startRequest))
            .rejects.toThrow(/missing query_id/i);
    });

    it("uses all query-name fallbacks and updates camelCase server identity", async () => {
        const starts = [
            { name: "name" },
            { query_name: "snake" },
            { queryName: "camel" },
            {},
        ];
        for (const [index, naming] of starts.entries()) {
            const execute = vi.spyOn(sparkGrpcClient, "executePlan")
                .mockResolvedValueOnce([{
                    serverSideSessionId: "server-start",
                    writeStreamOperationStartResult: {
                        ...naming,
                        queryId: { id: `query-${index}`, runId: `run-${index}` },
                    },
                }])
                .mockResolvedValueOnce([{
                    serverSideSessionId: "server-command",
                    streamingQueryCommandResult: {},
                }]);
            const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
            expect(handle.name).toBe(
                naming.name ?? naming.query_name ?? naming.queryName ?? "",
            );
            await expect(handle.stop()).resolves.toBeUndefined();
            expect(handle.serverSideSessionId).toBe("server-command");
            execute.mockRestore();
        }
    });

    it("rejects false without timeout and returns after a terminated query with no exception message", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{
                write_stream_operation_start_result: {
                    query_id: { id: "query", run_id: "run" },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    await_termination: { terminated: false },
                },
            }]);
        const falseHandle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(falseHandle.awaitTermination()).rejects
            .toThrow(/returned false without a timeout/);

        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{
                write_stream_operation_start_result: {
                    query_id: { id: "query", run_id: "run" },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    await_termination: { terminated: true },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    exception: {},
                },
            }]);
        const cleanHandle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(cleanHandle.awaitTermination()).resolves.toBeUndefined();
    });

    it("maps camelCase exception metadata to StreamingQueryError", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{
                write_stream_operation_start_result: {
                    query_id: { id: "query", run_id: "run" },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    await_termination: { terminated: true },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    exception: {
                        exceptionMessage: "camel failure",
                        errorClass: "CAMEL_ERROR",
                        stackTrace: "camel stack",
                    },
                },
            }]);
        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.awaitTermination()).rejects.toMatchObject({
            name: "StreamingQueryError",
            message: "camel failure",
            errorClass: "CAMEL_ERROR",
            remoteStack: "camel stack",
        });
    });

    it("starts from a caller-observed server identity when responses omit it", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{
                write_stream_operation_start_result: {
                    query_id: { id: "query", run_id: "run" },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {},
            }]);
        const handle = await sparkGrpcClient.executePlanStreaming({
            ...startRequest,
            client_observed_server_side_session_id: "server-from-request",
        });
        await handle.stop();
        expect(handle.serverSideSessionId).toBe("server-from-request");
        expect(execute.mock.calls[1]?.[0]).toMatchObject({
            client_observed_server_side_session_id: "server-from-request",
        });
    });
});
