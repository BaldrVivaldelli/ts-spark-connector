import { afterEach, describe, expect, it, vi } from "vitest";
import { sparkGrpcClient } from "../src/client/sparkClient";

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

describe("streaming query handles", () => {
    afterEach(() => vi.restoreAllMocks());

    it("supports protocol timeouts and returns false instead of reporting success", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    await_termination: { terminated: false },
                },
            }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.awaitTermination(2_500)).resolves.toBe(false);
        expect(execute.mock.calls[1][0]).toMatchObject({
            operation_id: expect.any(String),
            client_observed_server_side_session_id: "server-session-1",
            plan: {
                command: {
                    streaming_query_command: {
                        query_id: { id: "query-1", run_id: "run-1" },
                        await_termination: { timeout_ms: "2500" },
                    },
                },
            },
        });
        expect(handle.serverSideSessionId).toBe("server-session-1");
    });

    it("rejects malformed awaitTermination responses rather than treating them as success", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{ result_complete: {} }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.awaitTermination()).rejects.toThrow(/missing termination result/i);
    });

    it("propagates a query exception after termination", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    await_termination: { terminated: true },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    exception: {
                        exception_message: "stream failed",
                        error_class: "STREAM_FAILED",
                        stack_trace: "remote stack",
                    },
                },
            }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.awaitTermination()).rejects.toMatchObject({
            name: "StreamingQueryError",
            message: "stream failed",
            errorClass: "STREAM_FAILED",
            remoteStack: "remote stack",
        });
    });

    it("accepts an omitted exception status after a clean query termination", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    await_termination: { terminated: true },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {},
            }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.awaitTermination()).resolves.toBeUndefined();
    });

    it("rejects a missing query command result after a query terminates", async () => {
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse])
            .mockResolvedValueOnce([{
                streaming_query_command_result: {
                    await_termination: { terminated: true },
                },
            }])
            .mockResolvedValueOnce([{ result_complete: {} }]);

        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);
        await expect(handle.awaitTermination()).rejects.toThrow(/missing query command result/i);
    });

    it("validates timeout values before issuing the command", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([startResponse]);
        const handle = await sparkGrpcClient.executePlanStreaming(startRequest);

        await expect(handle.awaitTermination(-1)).rejects.toThrow(RangeError);
        await expect(handle.awaitTermination(1.5)).rejects.toThrow(RangeError);
        expect(execute).toHaveBeenCalledTimes(1);
    });
});
