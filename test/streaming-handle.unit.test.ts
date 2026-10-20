import { afterEach, describe, expect, it, vi } from "vitest";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { StreamingQueryError } from "../src/client/streamingProtocol";

const startRequest = {
    session_id: "session-1",
    user_context: { user_id: "user-1" },
    client_type: "ts-spark-connector",
    operation_id: "00000000-0000-4000-8000-000000000001",
    plan: { command: { write_stream_operation_start: {} } },
};

const startResponse = {
    server_side_session_id: "server-session-1",
    write_stream_operation_start_result: {
        name: "events",
        query_id: { id: "query-1", run_id: "run-1" },
    },
};

function commandOf(spy: { mock: { calls: unknown[][] } }, index: number): Record<string, unknown> {
    const request = spy.mock.calls[index]![0] as { plan: { command: { streaming_query_command: Record<string, unknown> } } };
    return request.plan.command.streaming_query_command;
}

const progress = (batchId: number) =>
    JSON.stringify({ id: "query-1", runId: "run-1", name: "events", timestamp: "2026-10-19T00:00:00.000Z", batchId });

describe("streaming query handle introspection", () => {
    afterEach(() => vi.restoreAllMocks());

    it("exposes the query identity and the start event when Spark attaches one", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValueOnce([{
            write_stream_operation_start_result: {
                query_id: { id: "query-1", runId: "run-1" },
                queryName: "named",
                query_started_event_json: '{"id":"query-1"}',
            },
        }]);
        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        expect(handle).toMatchObject({ id: "query-1", runId: "run-1", name: "named", startedEventJson: '{"id":"query-1"}' });

        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValueOnce([{
            writeStreamOperationStartResult: {
                queryId: { id: "query-2", run_id: "run-2" },
                queryStartedEventJson: '{"id":"query-2"}',
            },
        }]);
        const camel = await sparkGrpcClient.executePlanStreaming(startRequest);
        expect(camel).toMatchObject({ id: "query-2", runId: "run-2", name: "", startedEventJson: '{"id":"query-2"}' });

        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValueOnce([startResponse]);
        const plain = await sparkGrpcClient.executePlanStreaming(startRequest);
        expect(plain).not.toHaveProperty("startedEventJson");
    });

    it("reports status, isActive and the start of a trigger", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    status: { status_message: "Waiting for data", is_data_available: false, is_trigger_active: false, is_active: true },
                },
            }])
            .mockResolvedValueOnce([{
                streamingQueryCommandResult: {
                    status: { statusMessage: "Processing", isDataAvailable: true, isTriggerActive: true, isActive: false },
                },
            }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.status()).resolves.toEqual({
            message: "Waiting for data", isDataAvailable: false, isTriggerActive: false, isActive: true,
        });
        expect(commandOf(execute, 1)).toMatchObject({ query_id: { id: "query-1", run_id: "run-1" }, status: true });
        await expect(handle.isActive()).resolves.toBe(false);
    });

    it("parses the last and the recent progress reports", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: { recent_progress: { recent_progress_json: [progress(7)] } },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: { recentProgress: { recentProgressJson: [] } },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: { recent_progress: { recent_progress_json: [progress(1), progress(2)] } },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: { recent_progress: {} },
            }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.lastProgress()).resolves.toMatchObject({ batchId: 7, name: "events" });
        expect(commandOf(execute, 1)).toMatchObject({ last_progress: true });
        await expect(handle.lastProgress()).resolves.toBeUndefined();
        await expect(handle.recentProgress()).resolves.toEqual([
            expect.objectContaining({ batchId: 1 }),
            expect.objectContaining({ batchId: 2 }),
        ]);
        expect(commandOf(execute, 3)).toMatchObject({ recent_progress: true });
        await expect(handle.recentProgress()).resolves.toEqual([]);
    });

    it("rejects malformed progress reports and missing results", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: { recent_progress: { recent_progress_json: ["{not json"] } },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: { recent_progress: { recent_progress_json: ["[1]"] } },
            }])
            .mockResolvedValueOnce([{ streaming_query_command_result: {} }])
            .mockResolvedValueOnce([{ result_complete: {} }])
            .mockResolvedValueOnce([{ streaming_query_command_result: {} }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.recentProgress()).rejects.toThrow(/Invalid streaming progress report/);
        await expect(handle.recentProgress()).rejects.toThrow(/expected a JSON object/);
        await expect(handle.lastProgress()).rejects.toThrow(/missing progress result/);
        await expect(handle.status()).rejects.toThrow(/missing query command result/);
        await expect(handle.status()).rejects.toThrow(/missing status result/);
    });

    it("explains the query with and without the extended plans", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{ streaming_query_command_result: { explain: { result: "== Physical Plan ==" } } }])
            .mockResolvedValueOnce([{ streaming_query_command_result: { explain: { result: "== Parsed Logical Plan ==" } } }])
            .mockResolvedValueOnce([{ streaming_query_command_result: { explain: {} } }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.explain()).resolves.toBe("== Physical Plan ==");
        expect(commandOf(execute, 1)).toMatchObject({ explain: { extended: false } });
        await expect(handle.explain(true)).resolves.toBe("== Parsed Logical Plan ==");
        expect(commandOf(execute, 2)).toMatchObject({ explain: { extended: true } });
        await expect(handle.explain()).rejects.toThrow(/missing explain result/);
        await expect(handle.explain("yes" as never)).rejects.toThrow(TypeError);
    });

    it("returns the query failure as an error object without throwing", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    exception: { exception_message: "sink failed", error_class: "SINK", stack_trace: "at Sink" },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    exception: { exceptionMessage: "camel failed", errorClass: "CAMEL", stackTrace: "at Camel" },
                },
            }])
            .mockResolvedValueOnce([{ streaming_query_command_result: { exception: {} } }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        const failure = await handle.exception();
        expect(failure).toBeInstanceOf(StreamingQueryError);
        expect(failure).toMatchObject({ name: "StreamingQueryError", message: "sink failed", errorClass: "SINK", remoteStack: "at Sink" });
        await expect(handle.exception()).resolves.toMatchObject({ message: "camel failed", errorClass: "CAMEL", remoteStack: "at Camel" });
        await expect(handle.exception()).resolves.toBeUndefined();
    });

    it("waits for all available data to be processed", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{ server_side_session_id: "server-session-2" }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.processAllAvailable()).resolves.toBeUndefined();
        expect(commandOf(execute, 1)).toMatchObject({ process_all_available: true });
        expect(handle.serverSideSessionId).toBe("server-session-2");
    });

    it("builds a handle for a query the manager listed", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValueOnce([{
            streaming_query_command_result: {
                status: { status_message: "Active", is_data_available: true, is_trigger_active: true, is_active: true },
            },
        }]);
        const handle = sparkGrpcClient.streamingQueryHandle({
            request: { session_id: "session-1", user_context: { user_id: "user-1" }, client_type: "ts-spark-connector" },
            queryId: { id: "query-9", run_id: "run-9" },
            name: "listed",
        });
        expect(handle).toMatchObject({ id: "query-9", runId: "run-9", name: "listed" });
        expect(handle.serverSideSessionId).toBeUndefined();
        await expect(handle.status()).resolves.toMatchObject({ isActive: true });
        expect(execute.mock.calls[0]![0]).toMatchObject({
            session_id: "session-1",
            client_type: "ts-spark-connector",
            plan: { command: { streaming_query_command: { query_id: { id: "query-9", run_id: "run-9" } } } },
        });
        expect(execute.mock.calls[0]![0]).not.toHaveProperty("client_observed_server_side_session_id");
    });
});
