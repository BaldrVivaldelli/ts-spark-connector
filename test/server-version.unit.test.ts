import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession, UnsupportedServerVersionError } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { compareSparkVersions, parseSparkVersion } from "../src/client/serverVersion";

function session(): SparkSession {
    vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
    return SparkSession.builder().getOrCreate();
}

describe("Spark version parsing", () => {
    it("reads the numeric prefix of release and snapshot versions", () => {
        expect(parseSparkVersion("4.1.3")).toEqual([4, 1, 3]);
        expect(parseSparkVersion(" 4.2.0-SNAPSHOT ")).toEqual([4, 2, 0]);
        expect(parseSparkVersion("5")).toEqual([5, 0, 0]);
        expect(parseSparkVersion("4.1")).toEqual([4, 1, 0]);
        expect(() => parseSparkVersion("main")).toThrow(/Unrecognised Spark version/);
    });

    it("orders versions by major, minor and patch", () => {
        expect(compareSparkVersions("4.0.4", "4.1.0")).toBeLessThan(0);
        expect(compareSparkVersions("4.1.0", "4.1")).toBe(0);
        expect(compareSparkVersions("4.10.0", "4.9.9")).toBeGreaterThan(0);
        expect(compareSparkVersions("5.0.0", "4.99.99")).toBeGreaterThan(0);
        expect(compareSparkVersions("4.1.3", "4.1.4")).toBeLessThan(0);
    });
});

describe("SparkSession.version()", () => {
    afterEach(() => vi.restoreAllMocks());

    it("asks AnalyzePlan once and caches the answer", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({
            session_id: "",
            server_side_session_id: "server-1",
            spark_version: { version: "4.1.3" },
        });
        const spark = session();

        await expect(spark.version()).resolves.toBe("4.1.3");
        await expect(spark.version()).resolves.toBe("4.1.3");
        expect(analyze).toHaveBeenCalledOnce();
        expect(analyze.mock.calls[0]?.[0]).toMatchObject({
            session_id: spark.getSessionId(),
            client_type: "ts-spark-connector",
            spark_version: {},
        });
        expect(spark.getServerSideSessionId()).toBe("server-1");
    });

    it("sends the observed server session and retries after a failure", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze")
            .mockRejectedValueOnce(new Error("unavailable"))
            .mockResolvedValueOnce({ spark_version: {} })
            .mockResolvedValueOnce({ spark_version: { version: "4.2.0" } });
        const spark = session();
        spark.observeServerSideSessionId({ server_side_session_id: "server-7" });

        await expect(spark.version()).rejects.toThrow("unavailable");
        await expect(spark.version()).rejects.toThrow(/missing spark_version/);
        await expect(spark.version()).resolves.toBe("4.2.0");
        expect(analyze.mock.calls[0]?.[0]).toMatchObject({ client_observed_server_side_session_id: "server-7" });
    });

    it("invalidates the session when the server reports a changed identity", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockRejectedValue(
            Object.assign(new Error("changed"), { errorClass: "INVALID_HANDLE.SESSION_CHANGED" }),
        );
        const spark = session();
        await expect(spark.version()).rejects.toThrow("changed");
        await expect(spark.version()).rejects.toThrow(/server-side session changed/);
    });

    it("rejects on a closed session", async () => {
        const spark = session();
        await spark.close();
        await expect(spark.version()).rejects.toThrow(/closed/);
    });
});

describe("SparkSession.cloneSession()", () => {
    afterEach(() => vi.restoreAllMocks());

    it("refuses servers older than 4.1 before sending CloneSession", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.0.4" } });
        const clone = vi.spyOn(sparkGrpcClient, "cloneSession");
        const spark = session();

        const error = await spark.cloneSession().catch(e => e);
        expect(error).toBeInstanceOf(UnsupportedServerVersionError);
        expect(error).toMatchObject({
            name: "UnsupportedServerVersionError",
            feature: "cloneSession()",
            requiredVersion: "4.1.0",
            serverVersion: "4.0.4",
            message: "cloneSession() requires Spark Connect 4.1.0 or newer; the server runs 4.0.4.",
        });
        expect(clone).not.toHaveBeenCalled();
    });

    it("returns a session bound to the clone's identities without re-sending builder configs", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({
            server_side_session_id: "server-1",
            spark_version: { version: "4.1.3" },
        });
        const clone = vi.spyOn(sparkGrpcClient, "cloneSession").mockImplementation(async request => ({
            session_id: request.session_id,
            server_side_session_id: "server-1",
            new_session_id: request.new_session_id,
            new_server_side_session_id: "server-2",
        }));
        const config = vi.spyOn(sparkGrpcClient, "config");
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        const spark = SparkSession.builder()
            .config("spark.sql.shuffle.partitions", 3)
            .user({ user_id: "ana" })
            .withRpcTimeout(5_000)
            .getOrCreate();
        spark.addTag("not-copied");

        const copy = await spark.cloneSession();
        const request = clone.mock.calls[0]![0];
        expect(request).toMatchObject({
            session_id: spark.getSessionId(),
            client_observed_server_side_session_id: "server-1",
            new_session_id: copy.getSessionId(),
        });
        expect(copy.getSessionId()).not.toBe(spark.getSessionId());
        expect(copy.getServerSideSessionId()).toBe("server-2");
        expect(copy.getUserContext()).toEqual(spark.getUserContext());
        expect(copy.getConnectionConfig().rpcTimeoutMs).toBe(5_000);
        expect(copy.getTags()).toEqual([]);

        await copy.ensureRemoteConfigApplied();
        expect(config).not.toHaveBeenCalled();
    });

    it("rejects a clone answered with an unexpected session id", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.2.0" } });
        vi.spyOn(sparkGrpcClient, "cloneSession").mockImplementation(async request => ({
            session_id: request.session_id,
            new_session_id: "someone-else",
        }));
        await expect(session().cloneSession()).rejects.toThrow(/returned session id someone-else/);
    });

    it("reports CloneSession failures through the session", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.2.0" } });
        vi.spyOn(sparkGrpcClient, "cloneSession").mockRejectedValue(
            Object.assign(new Error("changed"), { errorClass: "INVALID_HANDLE.SESSION_CHANGED" }),
        );
        const spark = session();
        await expect(spark.cloneSession()).rejects.toThrow("changed");
        expect(() => spark.read).toThrow(/server-side session changed/);
    });
});
