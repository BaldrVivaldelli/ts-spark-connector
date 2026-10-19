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
import { CLEANUP_RPC_TIMEOUT_MS } from "../src/client/rpcLifecycle";

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
        // Like a real gRPC stream, events arrive from I/O on a later turn of
        // the event loop, never inside the microtasks that opened the call.
        setImmediate(() => {
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
    | "fetchErrorDetails"
    | "cloneSession"
    | "artifactStatus"
    | "getStatus";

type UnaryOutcome = RpcMessage | Error;

const state = {
    instances: [] as FakeService[],
    /** Outcome of the next AddArtifacts stream and the requests it received. */
    artifactOutcome: undefined as RpcMessage | Error | undefined,
    artifactRequests: [] as RpcMessage[],
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
    cloneSession = this.unary("cloneSession");
    artifactStatus = this.unary("artifactStatus");
    getStatus = this.unary("getStatus");

    addArtifacts(
        metadata: Grpc.Metadata,
        options: Grpc.CallOptions,
        callback: (error: Error | null, response: RpcMessage) => void,
    ): Grpc.ClientWritableStream<RpcMessage> {
        state.calls.push({ name: "addArtifacts", request: {}, metadata, options });
        const listeners: Array<() => void> = [];
        let writes = 0;
        const stream = {
            write: (request: RpcMessage) => {
                state.artifactRequests.push(request);
                writes += 1;
                // Every other write asks the caller to wait for drain.
                if (writes % 2 === 0) {
                    setImmediate(() => listeners.splice(0).forEach(listener => listener()));
                    return false;
                }
                return true;
            },
            once: (event: string, listener: () => void) => {
                if (event === "drain") listeners.push(listener);
                return stream;
            },
            on: (event: string, listener: () => void) => stream.once(event, listener),
            removeListener: () => stream,
            off: () => stream,
            end: () => {
                const outcome = state.artifactOutcome ?? {};
                setImmediate(() => outcome instanceof Error ? callback(outcome, {}) : callback(null, outcome));
            },
            cancel: vi.fn(),
        };
        return stream as unknown as Grpc.ClientWritableStream<RpcMessage>;
    }

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
        state.artifactRequests.length = 0;
        state.artifactOutcome = undefined;
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
            // @types/node 26 narrows this overload's return type; naming the new
            // type here would not compile against 22, so defer to the real call.
            return originalRead(file, options as never) as never;
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
    it("shares one absolute execution deadline across ExecutePlan and every ReattachExecute", async () => {
        const config = connection({
            rpcTimeoutMs: 50,
            executeTimeoutMs: 60_000,
            retry: { maxRetries: 1, initialBackoffMs: 0 },
        });
        const request = executeRequest();
        state.executeStreams.push([
            { kind: "data", value: { operation_id: request.operation_id as string, response_id: "r1" } },
            { kind: "error", error: Object.assign(new Error("connection lost"), { code: 14 }) },
        ]);
        state.reattachStreams.push(complete(request, { response_id: "r2" }));
        setUnary("releaseExecute", {});
        const before = Date.now();

        await expect(sparkGrpcClient.executePlan(request, config)).resolves.toHaveLength(2);

        const execute = state.calls.find(call => call.name === "executePlan")!;
        const reattach = state.calls.find(call => call.name === "reattachExecute")!;
        const deadline = execute.options.deadline as number;
        expect(deadline).toBeGreaterThanOrEqual(before + 60_000);
        expect(deadline).toBeLessThan(before + 61_000);
        // The continuation does not restart the clock (a per-RPC rpcTimeoutMs would).
        expect(reattach.options.deadline).toBe(deadline);
        // Unary cleanup keeps the (bounded) per-RPC deadline.
        const release = state.calls.find(call => call.name === "releaseExecute")!;
        expect(release.options.deadline as number).toBeLessThanOrEqual(Date.now() + 50);
        sparkGrpcClient.close(config);
    });

    it("lets executions run without a stream deadline while unary RPCs keep rpcTimeoutMs", async () => {
        const config = connection({ rpcTimeoutMs: 50, executeTimeoutMs: Number.POSITIVE_INFINITY });
        const request = executeRequest();
        state.executeStreams.push(complete(request));
        setUnary("releaseExecute", {});

        await expect(sparkGrpcClient.executePlan(request, config)).resolves.toHaveLength(1);
        expect(state.calls.find(call => call.name === "executePlan")?.options).toEqual({});
        expect(state.calls.find(call => call.name === "releaseExecute")?.options.deadline)
            .toEqual(expect.any(Number));
        sparkGrpcClient.close(config);
    });

    it("keeps the legacy per-stream rpcTimeoutMs deadline when no execution timeout is set", async () => {
        const config = connection({ rpcTimeoutMs: 5_000 });
        const request = executeRequest();
        state.executeStreams.push(complete(request));
        setUnary("releaseExecute", {});
        const before = Date.now();

        await sparkGrpcClient.executePlan(request, config);
        const deadline = state.calls.find(call => call.name === "executePlan")?.options.deadline as number;
        expect(deadline).toBeGreaterThanOrEqual(before + 5_000);
        expect(deadline).toBeLessThan(before + 6_000);
        sparkGrpcClient.close(config);
    });

    it("bounds ReleaseExecute with the cleanup deadline when no rpcTimeoutMs is set", async () => {
        const config = connection();
        const request = executeRequest();
        state.executeStreams.push(complete(request));
        setUnary("releaseExecute", {});
        const before = Date.now();

        await sparkGrpcClient.executePlan(request, config);
        expect(state.calls.find(call => call.name === "executePlan")?.options).toEqual({});
        const deadline = state.calls.find(call => call.name === "releaseExecute")?.options.deadline as number;
        expect(deadline).toBeGreaterThanOrEqual(before + CLEANUP_RPC_TIMEOUT_MS);
        expect(deadline).toBeLessThan(before + CLEANUP_RPC_TIMEOUT_MS + 1_000);
        sparkGrpcClient.close(config);
    });

    it("resolves provider credentials again for every continuation and release", async () => {
        let issued = 0;
        const provider = vi.fn(async () => {
            issued += 1;
            // Already inside the refresh window, so every resolution refreshes.
            return { headers: { authorization: `Bearer token-${issued}` }, expiresAt: Date.now() };
        });
        const config = connection({
            auth: { type: "provider", provider },
            allowInsecureAuth: true,
            retry: { maxRetries: 1, initialBackoffMs: 0 },
        });
        const request = executeRequest();
        state.executeStreams.push([
            { kind: "data", value: { operation_id: request.operation_id as string, response_id: "r1" } },
            { kind: "error", error: Object.assign(new Error("connection lost"), { code: 14 }) },
        ]);
        state.reattachStreams.push(complete(request, { response_id: "r2" }));
        setUnary("releaseExecute", {});

        await expect(sparkGrpcClient.executePlan(request, config)).resolves.toHaveLength(2);
        const tokens = ["executePlan", "reattachExecute", "releaseExecute"].map(name =>
            state.calls.find(call => call.name === name)?.metadata.get("authorization")[0]
        );
        expect(tokens).toEqual(["Bearer token-1", "Bearer token-2", "Bearer token-3"]);
        sparkGrpcClient.close(config);
    });

    it("streams artifact requests, honouring backpressure, and returns the summary", async () => {
        const config = connection();
        state.artifactOutcome = { session_id: "session-1", artifacts: [{ name: "jars/a.jar", is_crc_successful: true }] };
        const requests = [1, 2, 3].map(index => ({ session_id: "session-1", batch: { artifacts: [{ name: `jars/${index}.jar` }] } }));

        await expect(sparkGrpcClient.addArtifacts(requests, config)).resolves.toMatchObject({
            artifacts: [{ name: "jars/a.jar" }],
        });
        expect(state.artifactRequests).toEqual(requests);
        expect(state.calls.find(call => call.name === "addArtifacts")).toBeDefined();
        sparkGrpcClient.close(config);
    });

    it("enriches AddArtifacts failures and never retries them", async () => {
        const config = connection({ retry: { maxRetries: 3, initialBackoffMs: 0 } });
        state.artifactOutcome = Object.assign(new Error("unavailable"), { code: 14, errorId: "error-3" });
        setUnary("fetchErrorDetails", { detail: "upload detail" });

        await expect(sparkGrpcClient.addArtifacts([{ session_id: "session-1", batch: { artifacts: [] } }], config))
            .rejects.toMatchObject({ operation: "AddArtifacts", remoteDetails: { detail: "upload detail" } });
        expect(state.calls.filter(call => call.name === "addArtifacts")).toHaveLength(1);
        sparkGrpcClient.close(config);
    });

    it("rejects when the request iterator itself fails", async () => {
        const config = connection();
        function* broken(): Generator<RpcMessage> {
            yield { session_id: "session-1", batch: { artifacts: [] } };
            throw new Error("disk vanished");
        }
        await expect(sparkGrpcClient.addArtifacts(broken(), config)).rejects.toThrow(/disk vanished/);
        sparkGrpcClient.close(config);
    });

    it("sends ArtifactStatus and GetStatus as retried unary calls", async () => {
        const config = connection({ retry: { maxRetries: 1, initialBackoffMs: 0 } });
        setUnary("artifactStatus", Object.assign(new Error("unavailable"), { code: 14 }), { statuses: { "jars/a.jar": { exists: true } } });
        await expect(sparkGrpcClient.artifactStatus({ session_id: "session-1", names: ["jars/a.jar"] }, config))
            .resolves.toEqual({ statuses: { "jars/a.jar": { exists: true } } });
        expect(state.calls.filter(call => call.name === "artifactStatus")).toHaveLength(2);

        setUnary("getStatus", { operation_statuses: [{ operation_id: "op", state: "OPERATION_STATE_RUNNING" }] });
        await expect(sparkGrpcClient.getStatus({ session_id: "session-1" }, config))
            .resolves.toMatchObject({ operation_statuses: [{ operation_id: "op" }] });
        sparkGrpcClient.close(config);
    })

    it("clones a session without retrying and enriches CloneSession failures", async () => {
        const config = connection({ retry: { maxRetries: 3, initialBackoffMs: 0 } });
        const request = { ...executeRequest(), new_session_id: "new-1" };
        setUnary("cloneSession", { session_id: "session-1", new_session_id: "new-1" });
        await expect(sparkGrpcClient.cloneSession(request, config))
            .resolves.toMatchObject({ new_session_id: "new-1" });

        const failure = Object.assign(new Error("unavailable"), { code: 14, errorId: "error-9" });
        setUnary("cloneSession", failure);
        setUnary("fetchErrorDetails", { detail: "clone detail" });
        await expect(sparkGrpcClient.cloneSession(request, config)).rejects.toMatchObject({
            operation: "CloneSession",
            remoteDetails: { detail: "clone detail" },
        });
        expect(state.calls.filter(call => call.name === "cloneSession")).toHaveLength(2);
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
