import * as grpc from "@grpc/grpc-js";
import { loadSync, MessageTypeDefinition } from "@grpc/proto-loader";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
    executeReattachable,
    ReattachableResponse,
    RpcReadable,
} from "../src/client/reattachableExecution";
import { resolveRetryConfig } from "../src/client/retry";

type Event<T> =
    | { type: "data"; value: T }
    | { type: "error"; value: Error }
    | { type: "end" };

class ScriptedCall<T> implements RpcReadable<T> {
    private readonly listeners = {
        data: [] as Array<(response: T) => void>,
        end: [] as Array<() => void>,
        error: [] as Array<(error: Error) => void>,
    };
    cancelled = false;

    constructor(events: Event<T>[]) {
        queueMicrotask(() => {
            for (const event of events) {
                if (this.cancelled) break;
                if (event.type === "data") this.listeners.data.forEach(listener => listener(event.value));
                if (event.type === "error") this.listeners.error.forEach(listener => listener(event.value));
                if (event.type === "end") this.listeners.end.forEach(listener => listener());
            }
        });
    }

    cancel(): void { this.cancelled = true; }
    pause(): void { /* no-op fake */ }
    resume(): void { /* no-op fake */ }

    on(event: "data", listener: (response: T) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (error: Error) => void): this;
    on(event: "data" | "end" | "error", listener: ((value: T | Error) => void) | (() => void)): this {
        (this.listeners[event] as Array<typeof listener>).push(listener);
        return this;
    }
}

function transient(message: string): Error & { code: number } {
    return Object.assign(new Error(message), { code: grpc.status.UNAVAILABLE });
}

const request = {
    session_id: "session-1",
    user_context: { user_id: "user-1" },
    client_type: "ts-spark-connector",
    operation_id: "00000000-0000-4000-8000-000000000001",
    plan: { command: { write_operation: {} } },
};

const protoRoot = path.resolve(__dirname, "../proto");
const packageDefinition = loadSync(
    [path.join(protoRoot, "spark/connect/base.proto")],
    {
        includeDirs: [protoRoot],
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    }
);

function wireRoundTrip(typeName: string, value: Record<string, unknown>): Record<string, unknown> {
    const definition = packageDefinition[typeName] as MessageTypeDefinition<
        Record<string, unknown>,
        Record<string, unknown>
    >;
    return definition.deserialize(definition.serialize(value));
}

describe("reattachable ExecutePlan", () => {
    it("never replays the original plan and resumes after the last delivered response", async () => {
        const execute = vi.fn((_request: Record<string, unknown>) => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    operation_id: request.operation_id,
                    response_id: "response-1",
                    server_side_session_id: "server-session-1",
                },
            },
            { type: "error", value: transient("connection lost") },
        ]));
        const reattach = vi.fn((_request: Record<string, unknown>) => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    operation_id: request.operation_id,
                    response_id: "response-1",
                },
            },
            {
                type: "data",
                value: {
                    operation_id: request.operation_id,
                    response_id: "response-2",
                    result_complete: {},
                },
            },
            { type: "end" },
        ]));
        const release = vi.fn(async (_request: Record<string, unknown>) => ({}));
        const responses: ReattachableResponse[] = [];

        for await (const response of executeReattachable(request, {
            execute,
            reattach,
            release,
        }, {
            retry: resolveRetryConfig({ retry: { maxRetries: 1 } }),
            sleep: async () => {},
            jitter: value => value,
        })) responses.push(response);

        expect(responses.map(response => response.response_id)).toEqual(["response-1", "response-2"]);
        expect(execute).toHaveBeenCalledTimes(1);
        expect(execute.mock.calls[0][0]).toMatchObject({
            request_options: [{ reattach_options: { reattachable: true } }],
        });
        expect(reattach).toHaveBeenCalledWith(expect.objectContaining({
            operation_id: request.operation_id,
            last_response_id: "response-1",
            client_observed_server_side_session_id: "server-session-1",
        }));
        expect(release).toHaveBeenCalledWith(expect.objectContaining({
            operation_id: request.operation_id,
            release_all: {},
            client_observed_server_side_session_id: "server-session-1",
        }));

        const decodedExecute = wireRoundTrip(
            "spark.connect.ExecutePlanRequest",
            execute.mock.calls[0][0]
        );
        const decodedReattach = wireRoundTrip(
            "spark.connect.ReattachExecuteRequest",
            reattach.mock.calls[0][0]
        );
        const decodedRelease = wireRoundTrip(
            "spark.connect.ReleaseExecuteRequest",
            release.mock.calls[0][0]
        );
        expect(decodedExecute).toMatchObject({
            operation_id: request.operation_id,
            request_options: [{ reattach_options: { reattachable: true } }],
        });
        expect(decodedReattach).toMatchObject({
            operation_id: request.operation_id,
            last_response_id: "response-1",
        });
        expect(decodedRelease).toMatchObject({
            operation_id: request.operation_id,
            release_all: {},
        });
    });

    it("uses ReattachExecute after an ambiguous failure before the first response", async () => {
        const execute = vi.fn((_request: Record<string, unknown>) => {
            throw transient("ambiguous write outcome");
        });
        const reattach = vi.fn((_request: Record<string, unknown>) => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    result_complete: {},
                    operation_id: request.operation_id,
                    response_id: "r1",
                },
            },
            { type: "end" },
        ]));

        for await (const _response of executeReattachable(request, {
            execute,
            reattach,
            release: async () => ({}),
        }, {
            retry: resolveRetryConfig({ retry: { maxRetries: 1 } }),
            sleep: async () => {},
        })) {
            // Consume the completion marker.
        }

        expect(execute).toHaveBeenCalledTimes(1);
        expect(reattach).toHaveBeenCalledTimes(1);
        expect(reattach.mock.calls[0][0]).not.toHaveProperty("last_response_id");
    });

    it("continues a cleanly-ended incomplete stream even when transient retries are disabled", async () => {
        const execute = vi.fn((_request: Record<string, unknown>) => new ScriptedCall<ReattachableResponse>([
            { type: "data", value: { response_id: "r1", operation_id: request.operation_id } },
            { type: "end" },
        ]));
        const reattach = vi.fn((_request: Record<string, unknown>) => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    response_id: "r2",
                    operation_id: request.operation_id,
                    result_complete: {},
                },
            },
            { type: "end" },
        ]));
        const ids: unknown[] = [];

        for await (const response of executeReattachable(request, {
            execute,
            reattach,
            release: async () => ({}),
        }, { retry: resolveRetryConfig() })) ids.push(response.response_id);

        expect(ids).toEqual(["r1", "r2"]);
        expect(reattach).toHaveBeenCalledWith(expect.objectContaining({ last_response_id: "r1" }));
    });

    it("releases the operation when an incremental consumer stops early", async () => {
        const call = new ScriptedCall<ReattachableResponse>([
            { type: "data", value: { response_id: "r1", operation_id: request.operation_id } },
        ]);
        const release = vi.fn(async (_request: Record<string, unknown>) => ({}));

        for await (const _response of executeReattachable(request, {
            execute: () => call,
            reattach: () => { throw new Error("should not reattach"); },
            release,
        }, { retry: resolveRetryConfig() })) break;

        expect(call.cancelled).toBe(true);
        expect(release).toHaveBeenCalledTimes(1);
    });

    it("does not let duplicate responses reset the transient failure budget", async () => {
        const execute = vi.fn(() => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    operation_id: request.operation_id,
                    response_id: "response-1",
                },
            },
            { type: "error", value: transient("initial disconnect") },
        ]));
        const reattach = vi.fn(() => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    operation_id: request.operation_id,
                    response_id: "response-1",
                },
            },
            { type: "error", value: transient("duplicate then disconnect") },
        ]));

        const consume = async () => {
            for await (const _response of executeReattachable(request, {
                execute,
                reattach,
                release: async () => ({}),
            }, {
                retry: resolveRetryConfig({ retry: { maxRetries: 1 } }),
                sleep: async () => {},
            })) {
                // consume until recovery is exhausted
            }
        };

        await expect(consume()).rejects.toThrow("duplicate then disconnect");
        expect(execute).toHaveBeenCalledTimes(1);
        expect(reattach).toHaveBeenCalledTimes(1);
    });

    it("rejects response identity changes before reattach or release can adopt the wrong server id", async () => {
        const release = vi.fn(async (_request: Record<string, unknown>) => ({}));
        const execute = vi.fn(() => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    session_id: request.session_id,
                    operation_id: request.operation_id,
                    response_id: "response-1",
                    server_side_session_id: "server-session-1",
                },
            },
            {
                type: "data",
                value: {
                    session_id: request.session_id,
                    operation_id: request.operation_id,
                    response_id: "response-2",
                    server_side_session_id: "server-session-2",
                    result_complete: {},
                },
            },
            { type: "end" },
        ]));

        const consume = async () => {
            for await (const _response of executeReattachable(request, {
                execute,
                reattach: () => { throw new Error("should not reattach"); },
                release,
            }, { retry: resolveRetryConfig() })) {
                // consume until the identity violation is detected
            }
        };

        await expect(consume()).rejects.toMatchObject({
            errorClass: "INVALID_HANDLE.SESSION_CHANGED",
        });
        expect(release).toHaveBeenCalledWith(expect.objectContaining({
            client_observed_server_side_session_id: "server-session-1",
        }));
    });

    it("rejects a response from another client session", async () => {
        const execute = vi.fn(() => new ScriptedCall<ReattachableResponse>([
            {
                type: "data",
                value: {
                    session_id: "other-session",
                    operation_id: request.operation_id,
                    response_id: "response-1",
                    result_complete: {},
                },
            },
            { type: "end" },
        ]));

        const consume = async () => {
            for await (const _response of executeReattachable(request, {
                execute,
                reattach: () => { throw new Error("should not reattach"); },
                release: async () => ({}),
            }, { retry: resolveRetryConfig() })) {
                // consume until the identity violation is detected
            }
        };

        await expect(consume()).rejects.toThrow(/session_id other-session/i);
    });

    it("reports but does not surface ReleaseExecute failure after result_complete", async () => {
        const cleanupError = Object.assign(new Error("release unavailable"), { code: 14 });
        const onCleanupError = vi.fn();
        const responses: ReattachableResponse[] = [];

        for await (const response of executeReattachable(request, {
            execute: () => new ScriptedCall<ReattachableResponse>([
                {
                    type: "data",
                    value: {
                        operation_id: request.operation_id,
                        response_id: "response-complete",
                        result_complete: {},
                    },
                },
                { type: "end" },
            ]),
            reattach: () => { throw new Error("should not reattach"); },
            release: async () => { throw cleanupError; },
        }, {
            retry: resolveRetryConfig(),
            onCleanupError,
        })) responses.push(response);

        expect(responses).toHaveLength(1);
        expect(onCleanupError).toHaveBeenCalledWith(cleanupError);
    });
});
