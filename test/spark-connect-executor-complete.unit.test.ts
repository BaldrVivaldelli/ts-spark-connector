import { afterEach, describe, expect, it, vi } from "vitest";
import type { SparkSession } from "../src/client/session";
import {
    sparkGrpcClient,
    type StreamingQueryHandle,
} from "../src/client/sparkClient";
import { SparkConnectExecutor } from "../src/client/sparkConnectExecutor";
import type { ProtoRel } from "../src/engine/compilerRead";
import type { ProtoWriteRoot } from "../src/write/compilerWrite";
import { resolveExecution, type ExecutionOptions } from "../src/client/executionOptions";

function handle(serverSideSessionId?: string, startedEventJson?: string): StreamingQueryHandle {
    return {
        id: "query-1",
        runId: "run-1",
        name: "query",
        serverSideSessionId,
        ...(startedEventJson === undefined ? {} : { startedEventJson }),
        stop: vi.fn(async () => undefined),
        awaitTermination: vi.fn(async (timeoutMs?: number) =>
            timeoutMs === undefined ? undefined : true
        ) as StreamingQueryHandle["awaitTermination"],
        isActive: vi.fn(async () => true),
        status: vi.fn(async () => ({ message: "ok", isDataAvailable: true, isTriggerActive: false, isActive: true })),
        lastProgress: vi.fn(async () => undefined),
        recentProgress: vi.fn(async () => []),
        explain: vi.fn(async () => "plan"),
        exception: vi.fn(async () => undefined),
        processAllAvailable: vi.fn(async () => undefined),
    };
}

function fakeSession(serverId?: string) {
    const methods = {
        ensureRemoteConfigApplied: vi.fn(async () => undefined),
        markRemoteTouched: vi.fn(),
        getSessionId: vi.fn(() => "session-1"),
        getUserContext: vi.fn(() => ({
            user_id: "user-1",
            user_name: "User",
        })),
        getConnectionConfig: vi.fn(() => ({ address: "sc://spark:15002" })),
        observeServerSideSessionId: vi.fn(),
        observeRemoteError: vi.fn(),
        getServerSideSessionId: vi.fn(() => serverId),
        resolveExecution: vi.fn((options?: ExecutionOptions) => resolveExecution([], options)),
        notifyStreamingQueryStarted: vi.fn(),
    };
    return {
        methods,
        session: methods as unknown as SparkSession,
    };
}

const plan: ProtoRel = { sql: { query: "SELECT 1" } };
const batchRoot: ProtoWriteRoot = {
    child: plan,
    writerKind: "batch",
    spec: { options: {}, partitionBy: [], sortBy: [] },
};
const streamRoot: ProtoWriteRoot = {
    child: plan,
    writerKind: "stream",
    spec: { options: {}, partitionBy: [], sortBy: [] },
};

describe("SparkConnectExecutor complete behavior", () => {
    afterEach(() => vi.restoreAllMocks());

    it("executes, observes every response and includes an observed server ID", async () => {
        const { session, methods } = fakeSession("server-1");
        const responses = [
            { session_id: "session-1", value: 1 },
            { session_id: "session-1", value: 2 },
        ];
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValue(responses);

        await expect(SparkConnectExecutor.for(session).execute(plan)).resolves.toBe(responses);
        expect(methods.ensureRemoteConfigApplied).toHaveBeenCalledOnce();
        expect(methods.markRemoteTouched).toHaveBeenCalledOnce();
        expect(methods.observeServerSideSessionId).toHaveBeenCalledTimes(2);
        expect(execute).toHaveBeenCalledWith(expect.objectContaining({
            session_id: "session-1",
            client_observed_server_side_session_id: "server-1",
            operation_id: expect.any(String),
            plan: { root: plan },
        }), { address: "sc://spark:15002" });
    });

    it("tags writes and uses the caller's operation id", async () => {
        const { session } = fakeSession();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const execution = resolveExecution(["etl"], {
            operationId: "00112233-4455-4677-8899-aabbccddeeff",
        });

        await SparkConnectExecutor.for(session).runWrite(batchRoot, execution);
        expect(execute.mock.calls[0]?.[0]).toMatchObject({
            operation_id: "00112233-4455-4677-8899-aabbccddeeff",
            tags: ["etl"],
        });
    });

    it("streams responses and reports stream iteration failures", async () => {
        const { session, methods } = fakeSession();
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(
            async function* () {
                yield { session_id: "session-1", value: 1 };
                yield { session_id: "session-1", value: 2 };
            },
        );
        const executor = SparkConnectExecutor.for(session);
        const values: unknown[] = [];
        for await (const value of executor.stream(plan)) values.push(value);
        expect(values).toHaveLength(2);
        expect(methods.observeServerSideSessionId).toHaveBeenCalledTimes(2);

        const failure = new Error("stream failed");
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(
            () => ({
                next: async () => {
                    throw failure;
                },
                return: async () => ({ done: true, value: undefined }),
                throw: async (error?: unknown) => {
                    throw error;
                },
                [Symbol.asyncIterator]() {
                    return this;
                },
                // The stub only implements what the executor consumes, so go through
                // unknown as the compiler itself suggests for a partial async iterator.
            }) as unknown as AsyncGenerator<Record<string, unknown>, void, void>,
        );
        await expect(async () => {
            for await (const value of executor.stream(plan)) void value;
        }).rejects.toBe(failure);
        expect(methods.observeRemoteError).toHaveBeenCalledWith(failure);
    });

    it("explains plans in every supported mode and observes identity", async () => {
        const { session, methods } = fakeSession();
        const explain = vi.spyOn(sparkGrpcClient, "explainWithResponse")
            .mockResolvedValue({
                explainString: "plan text",
                response: { session_id: "session-1" },
            });
        const executor = SparkConnectExecutor.for(session);

        for (const mode of ["simple", "extended", "codegen", "cost", "formatted"] as const) {
            await expect(executor.explain(plan, mode)).resolves.toBe("plan text");
        }
        expect(explain).toHaveBeenCalledTimes(5);
        expect(methods.observeServerSideSessionId).toHaveBeenCalledTimes(5);
    });

    it("encodes every storage level and both unpersist forms", async () => {
        const { session, methods } = fakeSession("server-1");
        const analyze = vi.spyOn(sparkGrpcClient, "analyze")
            .mockResolvedValue({ session_id: "session-1" });
        const executor = SparkConnectExecutor.for(session);
        const levels = [
            "NONE",
            "DISK_ONLY",
            "MEMORY_ONLY",
            "MEMORY_AND_DISK",
            "MEMORY_ONLY_SER",
            "MEMORY_AND_DISK_SER",
            "OFF_HEAP",
        ];
        for (const level of levels) {
            await executor.runAnalyzeAction({ kind: "persist", relation: plan, level });
        }
        await executor.runAnalyzeAction({ kind: "unpersist", relation: plan });
        await executor.runAnalyzeAction({
            kind: "unpersist",
            relation: plan,
            blocking: false,
        });

        expect(analyze).toHaveBeenCalledTimes(9);
        expect(analyze.mock.calls[0]?.[0]).toMatchObject({
            client_observed_server_side_session_id: "server-1",
            persist: {
                relation: plan,
                storage_level: {
                    use_disk: false,
                    use_memory: false,
                    use_off_heap: false,
                    deserialized: false,
                    replication: 1,
                },
            },
        });
        expect(analyze.mock.calls[6]?.[0]).toMatchObject({
            persist: {
                storage_level: {
                    use_disk: true,
                    use_memory: true,
                    use_off_heap: true,
                    deserialized: false,
                },
            },
        });
        expect(analyze.mock.calls[7]?.[0]).not.toHaveProperty("unpersist.blocking");
        expect(analyze.mock.calls[8]?.[0]).toMatchObject({
            unpersist: { blocking: false },
        });
        expect(methods.observeServerSideSessionId).toHaveBeenCalledTimes(9);
        await expect(executor.runAnalyzeAction({
            kind: "persist",
            relation: plan,
            level: "invalid",
        })).rejects.toThrow(RangeError);
    });

    it("executes batch writes and observes all returned identities", async () => {
        const { session, methods } = fakeSession();
        const responses = [{ session_id: "session-1" }];
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValue(responses);

        await expect(SparkConnectExecutor.for(session).runWrite(batchRoot))
            .resolves.toBe(responses);
        expect(execute).toHaveBeenCalledWith(expect.objectContaining({
            session_id: "session-1",
            client_type: "ts-spark-connector",
            operation_id: expect.any(String),
            plan: expect.objectContaining({
                command: { write_operation: expect.any(Object) },
            }),
        }), expect.any(Object));
        expect(methods.observeServerSideSessionId).toHaveBeenCalledWith(responses[0]);
    });

    it("runs a direct stream and wraps all handle operations", async () => {
        const { session, methods } = fakeSession();
        const raw = handle();
        const start = vi.spyOn(sparkGrpcClient, "executePlanStreaming")
            .mockResolvedValue(raw);
        const wrapped = await SparkConnectExecutor.for(session).runStream(streamRoot);

        expect(start).toHaveBeenCalledWith(expect.objectContaining({
            plan: expect.objectContaining({
                command: { write_stream_operation_start: expect.any(Object) },
            }),
        }), expect.any(Object));
        await expect(wrapped.awaitTermination()).resolves.toBeUndefined();
        await expect(wrapped.awaitTermination(100)).resolves.toBe(true);
        await expect(wrapped.stop()).resolves.toBeUndefined();
        expect(raw.awaitTermination).toHaveBeenCalledWith();
        expect(raw.awaitTermination).toHaveBeenCalledWith(100);
        expect(raw.stop).toHaveBeenCalledOnce();
        expect(methods.observeServerSideSessionId).not.toHaveBeenCalled();
    });

    it("orders a view before stream start, synchronizes identity and awaits when requested", async () => {
        const { session, methods } = fakeSession();
        const raw = handle("server-stream");
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValue([{ session_id: "session-1" }]);
        vi.spyOn(sparkGrpcClient, "executePlanStreaming").mockResolvedValue(raw);
        const root: ProtoWriteRoot = {
            ...streamRoot,
            createStreamingView: "source_view",
            awaitTermination: true,
        };

        const wrapped = await SparkConnectExecutor.for(session).runStream(root);
        expect(execute).toHaveBeenCalledWith(expect.objectContaining({
            plan: expect.objectContaining({
                command: { create_dataframe_view: expect.any(Object) },
            }),
        }), expect.any(Object));
        expect(raw.awaitTermination).toHaveBeenCalledOnce();
        expect(methods.observeServerSideSessionId).toHaveBeenCalledWith({
            session_id: "session-1",
            server_side_session_id: "server-stream",
        });
        expect(wrapped.serverSideSessionId).toBe("server-stream");
    });

    it("reports remote failures from ordinary and wrapped streaming operations", async () => {
        const { session, methods } = fakeSession();
        const executor = SparkConnectExecutor.for(session);
        const executeFailure = new Error("execute failed");
        vi.spyOn(sparkGrpcClient, "executePlan").mockRejectedValue(executeFailure);
        await expect(executor.execute(plan)).rejects.toBe(executeFailure);
        expect(methods.observeRemoteError).toHaveBeenCalledWith(executeFailure);

        const raw = handle("server-stream");
        const stopFailure = new Error("stop failed");
        vi.mocked(raw.stop).mockRejectedValue(stopFailure);
        vi.spyOn(sparkGrpcClient, "executePlanStreaming").mockResolvedValue(raw);
        const wrapped = await executor.runStream(streamRoot);
        await expect(wrapped.stop()).rejects.toBe(stopFailure);
        expect(methods.observeRemoteError).toHaveBeenCalledWith(stopFailure);
    });
});

describe("SparkConnectExecutor streaming commands and handles", () => {
    afterEach(() => vi.restoreAllMocks());

    it("streams a command, lifting the stream deadline only when asked", async () => {
        const { session, methods } = fakeSession("server-1");
        const stream = vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(
            async function* () {
                yield { session_id: "session-1", streaming_query_listener_events_result: {} };
            },
        );
        const executor = SparkConnectExecutor.for(session);
        const command = { streaming_query_listener_bus_command: { add_listener_bus_listener: true } };

        const bounded = [];
        for await (const response of executor.streamCommand(command)) bounded.push(response);
        expect(bounded).toHaveLength(1);
        expect(stream.mock.calls[0]![0]).toMatchObject({
            plan: { command },
            client_observed_server_side_session_id: "server-1",
            operation_id: expect.any(String),
        });
        expect(stream.mock.calls[0]![1]).not.toHaveProperty("executeTimeoutMs");
        expect(methods.observeServerSideSessionId).toHaveBeenCalledOnce();

        const execution = resolveExecution(["bus"]);
        for await (const response of executor.streamCommand(command, execution, { unboundedStreams: true })) void response;
        expect(stream.mock.calls[1]![0]).toMatchObject({ operation_id: execution.operationId, tags: ["bus"] });
        expect(stream.mock.calls[1]![1]).toMatchObject({ executeTimeoutMs: Number.POSITIVE_INFINITY });

        const failure = new Error("bus failed");
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(async function* () {
            yield { session_id: "session-1" };
            throw failure;
        });
        await expect((async () => {
            for await (const response of executor.streamCommand(command)) void response;
        })()).rejects.toBe(failure);
        expect(methods.observeRemoteError).toHaveBeenCalledWith(failure);
    });

    it("wraps handles of listed queries and forwards the start event of a new query", async () => {
        const { session, methods } = fakeSession("server-1");
        const raw = handle("server-2");
        const factory = vi.spyOn(sparkGrpcClient, "streamingQueryHandle").mockReturnValue(raw);
        const executor = SparkConnectExecutor.for(session);

        const wrapped = executor.streamingQueryHandle({ id: "q", run_id: "r" }, "listed");
        expect(factory).toHaveBeenCalledWith(expect.objectContaining({
            request: expect.objectContaining({ session_id: "session-1", client_observed_server_side_session_id: "server-1" }),
            queryId: { id: "q", run_id: "r" },
            name: "listed",
            serverSideSessionId: "server-1",
        }));
        expect(wrapped).toMatchObject({ id: "query-1", runId: "run-1", name: "query" });
        await expect(wrapped.status()).resolves.toMatchObject({ isActive: true });
        await expect(wrapped.isActive()).resolves.toBe(true);
        await expect(wrapped.lastProgress()).resolves.toBeUndefined();
        await expect(wrapped.recentProgress()).resolves.toEqual([]);
        await expect(wrapped.explain(true)).resolves.toBe("plan");
        expect(raw.explain).toHaveBeenCalledWith(true);
        await expect(wrapped.exception()).resolves.toBeUndefined();
        await expect(wrapped.processAllAvailable()).resolves.toBeUndefined();
        expect(methods.observeServerSideSessionId).toHaveBeenCalledWith({
            session_id: "session-1",
            server_side_session_id: "server-2",
        });

        const started = handle(undefined, '{"id":"q"}');
        vi.spyOn(sparkGrpcClient, "executePlanStreaming").mockResolvedValue(started);
        const startedWrapped = await executor.runStream(streamRoot);
        expect(methods.notifyStreamingQueryStarted).toHaveBeenCalledWith('{"id":"q"}');
        expect(startedWrapped.startedEventJson).toBe('{"id":"q"}');
    });
});
