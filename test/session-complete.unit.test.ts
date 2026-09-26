import { afterEach, describe, expect, it, vi } from "vitest";
import {
    SparkSession,
    SparkSessionBuilder,
} from "../src/client/session";
import {
    SPARK_CLIENT_CACHE_IDENTITY,
    sparkGrpcClient,
} from "../src/client/sparkClient";
import type { ProtoExpr, ProtoGroup, ProtoRel } from "../src/engine/compilerRead";
import type { StreamingReadCap } from "../src/algebra/read/streaming-capabilities";
import type { StreamingMark } from "../src/algebra/read";

describe("SparkSession complete behavior", () => {
    afterEach(() => vi.restoreAllMocks());

    it("creates stream readers and delegates session.writeStream", () => {
        const session = SparkSession.builder().getOrCreate();
        const dataframe = session.readStream<ProtoRel, ProtoExpr, ProtoGroup>(
            "rate",
            { rowsPerSecond: "1" },
        );
        const writer = session.writeStream(
            dataframe as typeof dataframe & {
                readonly __streamingCapability?: StreamingReadCap<ProtoRel>
                    & StreamingMark<ProtoRel>;
            },
        );
        expect(dataframe.toProtoJSON()).toContain('"is_streaming": true');
        expect(writer.toClientASTJSON()).toContain("streamingWrite");
    });

    it("updates only nonblank user identity fields before remote use", () => {
        const session = new SparkSession(undefined, {
            userContext: { user_id: "initial", user_name: "Initial" },
        });
        session.setUserContext({ user_id: " next ", user_name: " Next " });
        expect(session.getUserContext()).toMatchObject({
            user_id: " next ",
            user_name: " Next ",
        });
        session.setUserContext({ user_id: "", user_name: " " });
        expect(session.getUserContext()).toMatchObject({
            user_id: " next ",
            user_name: " Next ",
        });
    });

    it("replaces connection state and returns independent nested copies", () => {
        const session = SparkSession.builder().getOrCreate();
        const retry = { maxRetries: 1 };
        session.setConnectionConfig({
            address: "sc://replacement:15002",
            auth: { type: "token", token: "secret" },
            tls: { serverNameOverride: "spark" },
            retry,
            sessionConfig: { "spark.sql.answer": 42 },
        });
        const first = session.getConnectionConfig();
        const second = session.getConnectionConfig();
        expect(first).toMatchObject({
            address: "sc://replacement:15002",
            retry: { maxRetries: 1 },
        });
        expect(first.auth).not.toBe(second.auth);
        expect(first.tls).not.toBe(second.tls);
        expect(first.retry).not.toBe(second.retry);
        expect(first.sessionConfig).not.toBe(second.sessionConfig);
        expect(first.retry).not.toBe(retry);
    });

    it("retries failed initial remote config application", async () => {
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        const failure = new Error("config failed");
        const config = vi.spyOn(sparkGrpcClient, "config")
            .mockRejectedValueOnce(failure)
            .mockResolvedValueOnce({});
        const session = SparkSession.builder()
            .config("spark.sql.answer", 42)
            .getOrCreate();

        await expect(session.ensureRemoteConfigApplied()).rejects.toBe(failure);
        await expect(session.ensureRemoteConfigApplied()).resolves.toBeUndefined();
        await expect(session.ensureRemoteConfigApplied()).resolves.toBeUndefined();
        expect(config).toHaveBeenCalledTimes(2);
        await session.close();
    });

    it("handles an internally absent session config as an empty initial config", async () => {
        const session = SparkSession.builder().getOrCreate();
        (session as unknown as {
            connectionConfig: { sessionConfig?: Record<string, string> };
        }).connectionConfig.sessionConfig = undefined;
        await expect(session.ensureRemoteConfigApplied()).resolves.toBeUndefined();
    });

    it("accepts camelCase identity and ignores blank or absent server IDs", () => {
        const session = SparkSession.builder().getOrCreate();
        expect(() => session.observeServerSideSessionId(null)).not.toThrow();
        expect(() => session.observeServerSideSessionId({})).not.toThrow();
        expect(() => session.observeServerSideSessionId({
            sessionId: session.getSessionId(),
            serverSideSessionId: "",
        })).not.toThrow();
        session.observeServerSideSessionId({
            sessionId: session.getSessionId(),
            serverSideSessionId: "server-camel",
        });
        expect(session.getServerSideSessionId()).toBe("server-camel");
        session.observeServerSideSessionId({
            server_side_session_id: "server-camel",
        });
    });

    it("recognizes stale-session errors from metadata and reason", async () => {
        for (const error of [
            { errorInfo: { metadata: { errorClass: "INVALID_HANDLE.SESSION_CHANGED" } } },
            { errorInfo: { reason: "INVALID_HANDLE.SESSION_CHANGED" } },
        ]) {
            const session = SparkSession.builder().getOrCreate();
            session.observeRemoteError(error);
            expect(() => session.read).toThrow(/server-side session changed/i);
            await session.close();
        }
        const healthy = SparkSession.builder().getOrCreate();
        expect(() => healthy.observeRemoteError(null)).not.toThrow();
        expect(() => healthy.observeRemoteError(new Error("ordinary"))).not.toThrow();
    });

    it("shares an in-progress close, exposes closing state and supports stop alias", async () => {
        let release!: (value: Record<string, unknown>) => void;
        vi.spyOn(sparkGrpcClient, "releaseSession").mockImplementation(() =>
            new Promise(resolve => {
                release = resolve;
            }),
        );
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        const session = new SparkSession(undefined, {
            connectionConfig: { address: "sc://closing:15002" },
        });
        session.markRemoteTouched();

        const first = session.close();
        const second = session.close();
        expect(() => session.read).toThrow(/closing/i);
        release({});
        await Promise.all([first, second]);
        await expect(session.stop()).resolves.toBeUndefined();
    });

    it("continues deterministic cleanup when release itself invalidates the session", async () => {
        const changed = {
            errorInfo: { reason: "INVALID_HANDLE.SESSION_CHANGED" },
        };
        vi.spyOn(sparkGrpcClient, "releaseSession").mockRejectedValue(changed);
        const close = vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        const session = new SparkSession(undefined, {
            connectionConfig: { address: "sc://stale-release:15002" },
        });
        session.markRemoteTouched();

        await expect(session.close()).resolves.toBeUndefined();
        expect(close).toHaveBeenCalledOnce();
    });

    it("does not hide identity invalidation when local channel close throws", () => {
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => {
            throw new Error("close failed");
        });
        const session = new SparkSession(undefined, {
            connectionConfig: { address: "sc://throwing-close:15002" },
        });
        session.markRemoteTouched();
        expect(() => session.observeServerSideSessionId({
            session_id: "wrong-session",
        })).toThrow(/returned session_id/i);
        expect(() => session.observeServerSideSessionId({
            session_id: "another-wrong-session",
        })).toThrow(/returned session_id/i);
        expect(() => session.read).toThrow(/returned session_id/i);
    });

    it("attaches cache identity and sends the known server identity to Config", async () => {
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({
            session_id: "00112233-4455-4677-8899-aabbccddeeff",
            server_side_session_id: "server-1",
        });
        const config = vi.spyOn(sparkGrpcClient, "config").mockResolvedValue({
            session_id: "00112233-4455-4677-8899-aabbccddeeff",
            server_side_session_id: "server-1",
            pairs: [],
        });
        const session = new SparkSession(
            "00112233-4455-4677-8899-aabbccddeeff",
            { connectionConfig: { address: "sc://config-identity:15002" } },
        );
        session.markRemoteTouched();
        const retainedConfig = session.getConnectionConfig() as typeof session extends never
            ? never
            : ReturnType<SparkSession["getConnectionConfig"]> & {
                [SPARK_CLIENT_CACHE_IDENTITY]?: string;
            };
        expect(retainedConfig[SPARK_CLIENT_CACHE_IDENTITY]).toEqual(expect.any(String));
        session.observeServerSideSessionId({
            server_side_session_id: "server-1",
        });
        await session.runConfigOperation({ get_all: {} });
        expect(config).toHaveBeenCalledWith(expect.objectContaining({
            client_observed_server_side_session_id: "server-1",
        }), expect.any(Object));
        await session.close();
    });

    it("retains a retry-configured connection snapshot", async () => {
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        const session = new SparkSession(undefined, {
            connectionConfig: {
                address: "sc://retry-retain:15002",
                retry: { maxRetries: 1 },
            },
        });
        session.markRemoteTouched();
        expect(session.getConnectionConfig().retry).toEqual({ maxRetries: 1 });
        await session.close();
    });
});

describe("SparkRuntimeConfig completion", () => {
    afterEach(() => vi.restoreAllMocks());

    it("uses get_with_default and rejects a missing required value", async () => {
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        const config = vi.spyOn(sparkGrpcClient, "config")
            .mockResolvedValueOnce({ pairs: [{ key: "spark.answer", value: "42" }] })
            .mockResolvedValueOnce({ pairs: [] });
        const session = SparkSession.builder().getOrCreate();

        await expect(session.conf.get("spark.answer", 7)).resolves.toBe("42");
        expect(config.mock.calls[0]?.[0]).toMatchObject({
            operation: {
                get_with_default: {
                    pairs: [{ key: "spark.answer", value: "7" }],
                },
            },
        });
        await expect(session.conf.get("spark.missing")).rejects.toThrow(/not returned/);
        await session.close();
    });

    it("handles absent/malformed pairs and getAll without a prefix", async () => {
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "config").mockImplementation(async request => {
            const operation = request.operation as Record<string, unknown>;
            if (operation.get_option) return { pairs: "malformed" };
            if (operation.is_modifiable) return {};
            return {
                pairs: [
                    {},
                    { key: "", value: "ignored" },
                    { key: "missing-value" },
                    { key: "spark.valid", value: "yes" },
                ],
            };
        });
        const session = SparkSession.builder().getOrCreate();
        await expect(session.conf.getOption("spark.unknown")).resolves.toBeUndefined();
        vi.mocked(sparkGrpcClient.config).mockResolvedValueOnce({
            pairs: [{ key: "spark.other", value: "value" }],
        });
        await expect(session.conf.getOption("spark.unknown")).resolves.toBeUndefined();
        await expect(session.conf.isModifiable("spark.unknown")).resolves.toBe(false);
        await expect(session.conf.getAll()).resolves.toEqual({ "spark.valid": "yes" });
        await session.close();
    });

    it("rejects blank and all connection-only runtime keys", async () => {
        const session = SparkSession.builder().getOrCreate();
        await expect(session.conf.get(" ")).rejects.toThrow(/non-empty/);
        await expect(session.conf.get("spark.connect.header.trace")).rejects
            .toThrow(/connection setting/);
        await session.close();
    });
});

describe("SparkSessionBuilder completion", () => {
    it("applies bulk configs, identities, legacy auth and every TLS draft key", () => {
        const builder = new SparkSessionBuilder()
            .configs({
                "spark.connect.userId": "user-id",
                user_name: "User Name",
                "spark.auth.type": "basic",
                "spark.auth.username": "alice",
                "spark.auth.password": "secret",
                "spark.ssl.enabled": true,
                "spark.ssl.keyStore": "key.p12",
                "spark.ssl.keyStorePassword": "key-pass",
                "spark.ssl.trustStore": "trust.p12",
                "spark.ssl.trustStorePassword": "trust-pass",
                "spark.ssl.certChain": "cert.pem",
                "spark.ssl.privateKey": "key.pem",
                "spark.ssl.serverNameOverride": "spark.internal",
                "spark.sql.answer": 42,
            })
            .user({ user_name: "Explicit Name" });
        const session = builder.getOrCreate();

        expect(session.getUserContext()).toMatchObject({
            user_id: "user-id",
            user_name: "Explicit Name",
        });
        expect(session.getConnectionConfig()).toMatchObject({
            auth: { type: "basic", username: "alice", password: "secret" },
            tls: {
                keyStorePath: "key.p12",
                keyStorePassword: "key-pass",
                trustStorePath: "trust.p12",
                trustStorePassword: "trust-pass",
                certChainPath: "cert.pem",
                privateKeyPath: "key.pem",
                serverNameOverride: "spark.internal",
            },
            sessionConfig: {
                "spark.ssl.enabled": true,
                "spark.sql.answer": 42,
            },
        });
    });

    it("applies token drafts, alternate TLS enablement and unknown reserved values", () => {
        const token = SparkSession.builder()
            .config("spark.auth.type", "token")
            .config("spark.auth.token", "token-secret")
            .config("spark.connect.grpc.ssl.enabled", true)
            .getOrCreate();
        expect(token.getConnectionConfig()).toMatchObject({
            auth: { type: "token", token: "token-secret" },
            tls: {},
        });

        const invalid = SparkSession.builder()
            .config("spark.auth.type", "unknown")
            .config("unrelated", "value")
            .getOrCreate();
        expect(invalid.getConnectionConfig().auth).toBeUndefined();
    });

    it("covers fluent connection, observability and authentication methods", () => {
        const controller = new AbortController();
        const logger = vi.fn();
        const metrics = vi.fn();
        const retry = { maxRetries: 1 };
        const session = SparkSession.builder()
            .withAuthAndTLS(
                { type: "token", token: "secret" },
                { serverNameOverride: "spark" },
            )
            .allowInsecureAuth(false)
            .withRpcTimeout(100)
            .withAbortSignal(controller.signal)
            .withRetry(retry)
            .withLogger(logger)
            .withMetrics(metrics)
            .getOrCreate();

        expect(session.getConnectionConfig()).toMatchObject({
            auth: { type: "token", token: "secret" },
            allowInsecureAuth: false,
            rpcTimeoutMs: 100,
            signal: controller.signal,
            retry,
            logger,
            metrics,
        });
        expect(() => SparkSession.builder().withRpcTimeout(0)).toThrow(RangeError);
        expect(() => SparkSession.builder().withRpcTimeout(1.5)).toThrow(RangeError);
    });

    it("resets basic and token drafts and honors all configured address keys", () => {
        const basic = SparkSession.builder()
            .withAuth({ type: "basic", username: "user", password: "pass" })
            .getOrCreate();
        expect(basic.getConnectionConfig().auth).toMatchObject({ type: "basic" });
        const token = SparkSession.builder()
            .withAuth({ type: "token", token: "token" })
            .getOrCreate();
        expect(token.getConnectionConfig().auth).toMatchObject({ type: "token" });

        expect(SparkSession.builder()
            .config("spark.connect.address", "sc://address:1")
            .getOrCreate()
            .getConnectionConfig().address).toBe("sc://address:1");
        expect(SparkSession.builder()
            .config("SPARK_CONNECT_URL", "sc://upper:2")
            .getOrCreate()
            .getConnectionConfig().address).toBe("sc://upper:2");
    });
});
