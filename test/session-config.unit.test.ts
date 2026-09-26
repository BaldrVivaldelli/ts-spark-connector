import { afterEach, describe, expect, it, vi } from "vitest";
import { createSparkSession, SparkSession } from "../src/client/session";
import { getClientReferenceCount, sparkGrpcClient } from "../src/client/sparkClient";
import { SparkConnectExecutor } from "../src/client/sparkConnectExecutor";
import { SparkConnectError } from "../src/client/errors";
import type { LogicalPlan } from "../src/engine/logicalPlan";

describe("SparkSession remote configuration", () => {
    afterEach(() => vi.restoreAllMocks());

    it("applies builder Spark configs once and filters connection-only settings", async () => {
        const config = vi.spyOn(sparkGrpcClient, "config").mockResolvedValue({
            server_side_session_id: "server-session-1",
        });
        const session = SparkSession.builder()
            .config("spark.connect.url", "sc://spark:15002")
            .config("spark.connect.header.x-request-id", "request-1")
            .config("spark.sql.shuffle.partitions", 8)
            .getOrCreate();

        await Promise.all([
            session.ensureRemoteConfigApplied(),
            session.ensureRemoteConfigApplied(),
        ]);

        expect(config).toHaveBeenCalledTimes(1);
        expect(config.mock.calls[0][0]).toMatchObject({
            operation: {
                set: {
                    pairs: [{ key: "spark.sql.shuffle.partitions", value: "8" }],
                },
            },
        });
        expect(session.getServerSideSessionId()).toBe("server-session-1");
    });

    it("provides set/get/getOption/getAll/unset/isModifiable over Config RPC", async () => {
        vi.spyOn(sparkGrpcClient, "config").mockImplementation(async request => {
            const operation = request.operation as Record<string, unknown>;
            if (operation.get) return { pairs: [{ key: "spark.answer", value: "42" }] };
            if (operation.get_option) return { pairs: [] };
            if (operation.get_all) return {
                pairs: [
                    { key: "spark.a", value: "1" },
                    { key: "spark.b", value: "2" },
                ],
            };
            if (operation.is_modifiable) {
                return { pairs: [{ key: "spark.answer", value: "true" }] };
            }
            return {};
        });
        const session = SparkSession.builder().getOrCreate();

        await session.conf.set("spark.answer", 42);
        expect(session.getSessionConfig()["spark.answer"]).toBe(42);
        expect(await session.conf.get("spark.answer")).toBe("42");
        expect(await session.conf.getOption("spark.missing")).toBeUndefined();
        expect(await session.conf.getAll("spark.")).toEqual({ "spark.a": "1", "spark.b": "2" });
        expect(await session.conf.isModifiable("spark.answer")).toBe(true);
        await session.conf.unset("spark.answer");
        expect(session.getSessionConfig()["spark.answer"]).toBeUndefined();
    });

    it("releases touched remote state, closes the channel, and rejects later use", async () => {
        const release = vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        const closeChannel = vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        const session = SparkSession.builder().getOrCreate();
        session.markRemoteTouched();

        await session.close();
        await session.close();

        expect(release).toHaveBeenCalledTimes(1);
        expect(closeChannel).toHaveBeenCalledTimes(1);
        expect(() => session.read).toThrow(/closed/i);
    });

    it("rejects connection settings through the remote conf facade", async () => {
        const session = SparkSession.builder().getOrCreate();
        await expect(session.conf.set("spark.connect.url", "sc://other:15002"))
            .rejects.toThrow(/connection setting/i);
    });

    it("freezes connection and user identity after remote state is touched", () => {
        const session = SparkSession.builder().getOrCreate();
        session.markRemoteTouched();

        expect(() => session.setConnectionConfig({ address: "sc://other:15002" }))
            .toThrow(/cannot change.*connection/i);
        expect(() => session.setUserContext({ user_id: "other-user" }))
            .toThrow(/cannot change.*user/i);
        sparkGrpcClient.close(session.getConnectionConfig());
    });

    it("ignores an operation abort during cleanup and allows retrying a failed release", async () => {
        const controller = new AbortController();
        controller.abort("query cancelled");
        const release = vi.spyOn(sparkGrpcClient, "releaseSession")
            .mockRejectedValueOnce(new Error("temporary cleanup failure"))
            .mockResolvedValueOnce({});
        const closeChannel = vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        const session = SparkSession.builder().withAbortSignal(controller.signal).getOrCreate();
        session.markRemoteTouched();

        await expect(session.close()).rejects.toThrow(/cleanup failure/);
        expect(release.mock.calls[0][1]?.signal).toBeUndefined();
        expect(closeChannel).not.toHaveBeenCalled();
        expect(() => session.read).not.toThrow();

        await session.close();
        expect(release).toHaveBeenCalledTimes(2);
        expect(closeChannel).toHaveBeenCalledTimes(1);
        expect(() => session.read).toThrow(/closed/i);
    });

    it("requires canonical UUID session ids at the public constructor boundary", () => {
        expect(() => createSparkSession("example-session")).toThrow(/sessionId.*UUID/i);
        expect(createSparkSession("00112233-4455-4677-8899-aabbccddeeff").getSessionId())
            .toBe("00112233-4455-4677-8899-aabbccddeeff");
    });

    it("invalidates a session when response identity changes", async () => {
        const session = new SparkSession("00112233-4455-4677-8899-aabbccddeeff", {
            connectionConfig: { address: "sc://identity-release.invalid:15002" },
        });
        session.markRemoteTouched();
        expect(getClientReferenceCount(session.getConnectionConfig())).toBe(1);
        session.observeServerSideSessionId({
            session_id: session.getSessionId(),
            server_side_session_id: "server-session-1",
        });

        expect(() => session.observeServerSideSessionId({
            session_id: session.getSessionId(),
            server_side_session_id: "server-session-2",
        })).toThrow(/server session changed/i);
        expect(() => session.read).toThrow(/server session changed/i);
        expect(getClientReferenceCount(session.getConnectionConfig())).toBe(0);
        await session.close();

        const wrongClientSession = createSparkSession("11112233-4455-4677-8899-aabbccddeeff");
        expect(() => wrongClientSession.observeServerSideSessionId({
            session_id: "99992233-4455-4677-8899-aabbccddeeff",
        })).toThrow(/returned session_id/i);
        await wrongClientSession.close();
    });

    it("invalidates remote config use on INVALID_HANDLE.SESSION_CHANGED and still closes locally", async () => {
        const remoteError = new SparkConnectError("session changed", {
            operation: "Config",
            errorClass: "INVALID_HANDLE.SESSION_CHANGED",
        });
        vi.spyOn(sparkGrpcClient, "config").mockRejectedValue(remoteError);
        const release = vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        const closeChannel = vi.spyOn(sparkGrpcClient, "close");
        const session = SparkSession.builder()
            .config("spark.connect.url", "sc://identity.invalid:15002")
            .getOrCreate();

        await expect(session.conf.getOption("spark.answer")).rejects.toBe(remoteError);
        expect(() => session.read).toThrow(/server-side session changed/i);
        await session.close();

        expect(release).not.toHaveBeenCalled();
        expect(closeChannel).toHaveBeenCalledTimes(1);
    });

    it("preserves and observes identity from the first explain response", async () => {
        const session = SparkSession.builder()
            .config("spark.connect.url", "sc://explain.invalid:15002")
            .getOrCreate();
        const explain = vi.spyOn(sparkGrpcClient, "explainWithResponse")
            .mockImplementation(async request => ({
                explainString: "== Physical Plan ==",
                response: {
                    session_id: request.session_id,
                    server_side_session_id: "server-session-explain",
                    explain: { explain_string: "== Physical Plan ==" },
                },
            }));
        vi.spyOn(sparkGrpcClient, "releaseSession").mockImplementation(async request => ({
            session_id: request.session_id,
            server_side_session_id: "server-session-explain",
        }));

        await expect(SparkConnectExecutor.for(session).explain({} as LogicalPlan, "simple"))
            .resolves.toBe("== Physical Plan ==");
        expect(session.getServerSideSessionId()).toBe("server-session-explain");
        expect(explain).toHaveBeenCalledTimes(1);
        await session.close();
    });
});
