import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SparkSession } from "../src/client/session";
import { col } from "../src/engine/column";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { combineSignals, resolveExecution } from "../src/client/executionOptions";

const OPERATION_ID = "00112233-4455-4677-8899-aabbccddeeff";

function touchedSession(): SparkSession {
    vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
    const session = SparkSession.builder().getOrCreate();
    session.markRemoteTouched();
    return session;
}

describe("per-execution options", () => {
    it("merges session and execution tags without duplicates and keeps a caller operation id", () => {
        const resolved = resolveExecution(new Set(["etl", "nightly"]), {
            tags: ["nightly", "job-7"],
            operationId: OPERATION_ID,
        });
        expect(resolved).toEqual({ tags: ["etl", "nightly", "job-7"], operationId: OPERATION_ID });

        const generated = resolveExecution([]);
        expect(generated.tags).toEqual([]);
        expect(generated.operationId).toMatch(/^[0-9a-f-]{36}$/);
        expect(resolveExecution([]).operationId).not.toBe(generated.operationId);
    });

    it("rejects malformed options before any RPC", () => {
        expect(() => resolveExecution([], null as never)).toThrow(/must be an object/);
        expect(() => resolveExecution([], { signal: {} as AbortSignal })).toThrow(/AbortSignal/);
        expect(() => resolveExecution([], { tags: "a" as never })).toThrow(/array of strings/);
        expect(() => resolveExecution([], { tags: [""] })).toThrow(/empty/);
        expect(() => resolveExecution([], { tags: ["a,b"] })).toThrow(/cannot contain ','/);
        expect(() => resolveExecution([], { tags: [1 as never] })).toThrow(/must be strings/);
        expect(() => resolveExecution([], { operationId: "op-1" })).toThrow(/canonical UUID/);
    });

    it("combines the session signal with the execution signal", () => {
        const session = new AbortController();
        const execution = new AbortController();
        expect(combineSignals(undefined, undefined)).toBeUndefined();
        expect(combineSignals(session.signal, undefined)).toBe(session.signal);
        expect(combineSignals(undefined, execution.signal)).toBe(execution.signal);

        const combined = combineSignals(session.signal, execution.signal)!;
        expect(combined.aborted).toBe(false);
        execution.abort("only this query");
        expect(combined.aborted).toBe(true);
        expect(combined.reason).toBe("only this query");
        expect(session.signal.aborted).toBe(false);
    });
});

describe("DataFrame actions carry execution options to ExecutePlan", () => {
    // mergeInto() is checked against the server version (Spark 4.0+).
    beforeEach(() => {
        vi.spyOn(SparkSession.prototype, "version").mockResolvedValue("4.0.4");
    });
    afterEach(() => vi.restoreAllMocks());

    it("sends session and per-call tags, the caller's operation id and a combined signal", async () => {
        const sessionAbort = new AbortController();
        const session = SparkSession.builder().withAbortSignal(sessionAbort.signal).getOrCreate();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        const queryAbort = new AbortController();

        session.addTag("etl");
        const pending = session.sql("SELECT 1").collect({
            tags: ["job-7"],
            operationId: OPERATION_ID,
            signal: queryAbort.signal,
        });
        // Tags are snapshotted when the action starts.
        session.clearTags();
        await pending;

        const [request, config] = execute.mock.calls[0]!;
        expect(request).toMatchObject({ operation_id: OPERATION_ID, tags: ["etl", "job-7"] });
        queryAbort.abort("cancel one query");
        expect(config?.signal?.aborted).toBe(true);
        expect(sessionAbort.signal.aborted).toBe(false);
        await session.close().catch(() => undefined);
    });

    it("omits tags and keeps the session config when no options are given", async () => {
        const session = SparkSession.builder().getOrCreate();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);

        await session.sql("SELECT 1").collectRaw();
        const [request, config] = execute.mock.calls[0]!;
        expect(request).not.toHaveProperty("tags");
        expect(config?.signal).toBeUndefined();
        await session.close();
    });

    it("tags writeTo and mergeInto commands with the session tags", async () => {
        const session = SparkSession.builder().getOrCreate();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        session.addTag("nightly");

        const source = session.read.parquet("/data/updates.parquet");
        await source.writeTo("catalog.target").append();
        await source.mergeInto("catalog.target", col("catalog.target.id").eq(col("id")))
            .whenMatched().updateAll()
            .merge();

        expect(execute).toHaveBeenCalledTimes(2);
        for (const [request] of execute.mock.calls) {
            expect(request).toMatchObject({ tags: ["nightly"], operation_id: expect.any(String) });
            expect(request.plan).toHaveProperty("command");
        }
        await session.close();
    });

    it("passes options through the streaming actions", async () => {
        const session = SparkSession.builder().getOrCreate();
        const stream = vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(async function* () {
            yield* [];
        });
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);

        const rows = [];
        for await (const row of session.sql("SELECT 1").toRows({ tags: ["stream"] })) rows.push(row);
        expect(rows).toEqual([]);
        expect(stream.mock.calls[0]?.[0]).toMatchObject({ tags: ["stream"] });
        await session.close();
    });
});

describe("session tags and interrupts", () => {
    afterEach(() => vi.restoreAllMocks());

    it("manages session tags with Spark's validation rules", () => {
        const session = SparkSession.builder().getOrCreate();
        session.addTag("a");
        session.addTag("b");
        session.addTag("a");
        expect(session.getTags()).toEqual(["a", "b"]);
        session.removeTag("a");
        expect(session.getTags()).toEqual(["b"]);
        session.clearTags();
        expect(session.getTags()).toEqual([]);
        expect(() => session.addTag("")).toThrow(/empty/);
        expect(() => session.removeTag("x,y")).toThrow(/cannot contain ','/);
    });

    it("sends the three interrupt kinds and returns the interrupted ids", async () => {
        const session = touchedSession();
        session.observeServerSideSessionId({ server_side_session_id: "server-1" });
        const interrupt = vi.spyOn(sparkGrpcClient, "interrupt")
            .mockResolvedValueOnce({ interrupted_ids: ["op-1", "op-2"], server_side_session_id: "server-1" })
            .mockResolvedValueOnce({ interruptedIds: ["op-3", 4] })
            .mockResolvedValueOnce({});

        await expect(session.interruptAll()).resolves.toEqual(["op-1", "op-2"]);
        await expect(session.interruptTag("etl")).resolves.toEqual(["op-3"]);
        await expect(session.interruptOperation(OPERATION_ID)).resolves.toEqual([]);

        expect(interrupt.mock.calls.map(call => call[0])).toEqual([
            expect.objectContaining({
                session_id: session.getSessionId(),
                client_type: "ts-spark-connector",
                client_observed_server_side_session_id: "server-1",
                interrupt_type: "INTERRUPT_TYPE_ALL",
            }),
            expect.objectContaining({ interrupt_type: "INTERRUPT_TYPE_TAG", operation_tag: "etl" }),
            expect.objectContaining({
                interrupt_type: "INTERRUPT_TYPE_OPERATION_ID",
                operation_id: OPERATION_ID,
            }),
        ]);
    });

    it("needs no RPC when the session never reached the server", async () => {
        const interrupt = vi.spyOn(sparkGrpcClient, "interrupt");
        const session = SparkSession.builder().getOrCreate();
        await expect(session.interruptAll()).resolves.toEqual([]);
        expect(interrupt).not.toHaveBeenCalled();
    });

    it("rejects invalid targets and closed sessions without an RPC", async () => {
        const interrupt = vi.spyOn(sparkGrpcClient, "interrupt");
        const session = touchedSession();
        await expect(session.interruptTag("a,b")).rejects.toThrow(/cannot contain ','/);
        await expect(session.interruptOperation("not-a-uuid")).rejects.toThrow(/canonical UUID/);
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        await session.close();
        await expect(session.interruptAll()).rejects.toThrow(/closed/);
        expect(interrupt).not.toHaveBeenCalled();
    });

    it("reports interrupt failures and invalidates on a changed server session", async () => {
        const session = touchedSession();
        const failure = Object.assign(new Error("changed"), { errorClass: "INVALID_HANDLE.SESSION_CHANGED" });
        vi.spyOn(sparkGrpcClient, "interrupt").mockRejectedValueOnce(failure);

        await expect(session.interruptAll()).rejects.toBe(failure);
        expect(() => session.read).toThrow(/server-side session changed/);
    });
});
