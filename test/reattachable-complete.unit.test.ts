import * as grpc from "@grpc/grpc-js";
import { describe, expect, it, vi } from "vitest";
import {
    EMPTY_STREAM_GRACE_MS,
    executeReattachable,
    type ReattachableResponse,
    type RpcMessage,
    type RpcReadable,
} from "../src/client/reattachableExecution";
import { resolveRetryConfig } from "../src/client/retry";

type ListenerMap<T> = {
    data: Array<(value: T) => void>;
    end: Array<() => void>;
    error: Array<(error: Error) => void>;
};

class ManualCall<T> implements RpcReadable<T> {
    readonly listeners: ListenerMap<T> = { data: [], end: [], error: [] };
    readonly cancel = vi.fn();
    readonly pause = vi.fn();
    readonly resume = vi.fn();

    on(event: "data", listener: (response: T) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (error: Error) => void): this;
    on(
        event: keyof ListenerMap<T>,
        listener: ((response: T) => void) | (() => void) | ((error: Error) => void),
    ): this {
        this.listeners[event].push(listener as never);
        return this;
    }

    data(value: T): void {
        this.listeners.data.forEach(listener => listener(value));
    }

    end(): void {
        this.listeners.end.forEach(listener => listener());
    }

    error(error: Error): void {
        this.listeners.error.forEach(listener => listener(error));
    }
}

function scheduledCall<T>(
    values: T[],
    ending: "end" | Error = "end",
): ManualCall<T> {
    const call = new ManualCall<T>();
    queueMicrotask(() => {
        values.forEach(value => call.data(value));
        if (ending === "end") call.end();
        else call.error(ending);
    });
    return call;
}

const baseRequest: RpcMessage = {
    session_id: "session-1",
    user_context: { user_id: "user-1" },
    client_type: "client",
    operation_id: "operation-1",
};

function response(
    responseId: string,
    extra: ReattachableResponse = {},
): ReattachableResponse {
    return {
        operation_id: "operation-1",
        response_id: responseId,
        ...extra,
    };
}

const noRetry = resolveRetryConfig();

async function consume(
    request: RpcMessage,
    execute: (request: RpcMessage) => RpcReadable<ReattachableResponse>,
    overrides: Partial<Parameters<typeof executeReattachable>[2]> = {},
): Promise<ReattachableResponse[]> {
    const rows: ReattachableResponse[] = [];
    for await (const value of executeReattachable(request, {
        execute,
        reattach: () => scheduledCall([
            response("complete", { result_complete: {} }),
        ]),
        release: async () => ({}),
    }, {
        retry: noRetry,
        ...overrides,
    })) rows.push(value);
    return rows;
}

describe("reattachable execution completion", () => {
    it("validates required request identity before opening or releasing calls", async () => {
        for (const invalid of [
            { ...baseRequest, operation_id: "" },
            { ...baseRequest, operation_id: 1 },
            { ...baseRequest, session_id: "" },
            { ...baseRequest, user_context: null },
            { ...baseRequest, user_context: "user" },
        ]) {
            const execute = vi.fn(() => scheduledCall([]));
            await expect(consume(invalid, execute)).rejects.toThrow(/requires/i);
            expect(execute).not.toHaveBeenCalled();
        }
    });

    it("updates existing snake_case and camelCase reattach options", async () => {
        for (const option of [
            { reattach_options: { reattachable: false }, other: true },
            { reattachOptions: { reattachable: false }, other: true },
        ]) {
            const execute = vi.fn((request: RpcMessage) => scheduledCall([
                response("done", { result_complete: {}, request }),
            ]));
            await consume({
                ...baseRequest,
                request_options: [{ untouched: true }, option],
            }, execute);
            expect(execute.mock.calls[0]?.[0]).toMatchObject({
                request_options: [
                    { untouched: true },
                    { other: true, reattach_options: { reattachable: true } },
                ],
            });
        }
    });

    it("accepts camelCase response IDs and server identity", async () => {
        const release = vi.fn(async (_request: RpcMessage) => ({}));
        const values: ReattachableResponse[] = [];
        for await (const value of executeReattachable({
            ...baseRequest,
            client_type: "",
        }, {
            execute: () => scheduledCall([{
                operationId: "operation-1",
                responseId: "response-1",
                serverSideSessionId: "server-1",
                resultComplete: {},
            }]),
            reattach: () => scheduledCall([]),
            release,
        }, { retry: noRetry })) values.push(value);

        expect(values).toHaveLength(1);
        expect(release).toHaveBeenCalledWith(expect.objectContaining({
            client_observed_server_side_session_id: "server-1",
        }));
        expect(release.mock.calls[0]?.[0]).not.toHaveProperty("client_type");
    });

    it("rejects invalid maximum empty continuation counts", async () => {
        for (const maxEmptyReattachments of [0, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
            await expect(consume(
                baseRequest,
                () => scheduledCall([]),
                { maxEmptyReattachments },
            )).rejects.toThrow(RangeError);
        }
    });

    it("rejects wrong operation and missing response IDs", async () => {
        await expect(consume(baseRequest, () => scheduledCall([{
            operation_id: "wrong",
            response_id: "response-1",
        }]))).rejects.toThrow(/operation_id wrong/i);
        await expect(consume(baseRequest, () => scheduledCall([{
            operation_id: "operation-1",
        }]))).rejects.toThrow(/without response_id/i);
    });

    it("limits repeatedly empty continuation streams", async () => {
        const execute = vi.fn(() => scheduledCall<ReattachableResponse>([]));
        const reattach = vi.fn(() => scheduledCall<ReattachableResponse>([]));
        const run = async () => {
            for await (const value of executeReattachable(baseRequest, {
                execute,
                reattach,
                release: async () => ({}),
            }, {
                retry: noRetry,
                maxEmptyReattachments: 1,
            })) void value;
        };
        await expect(run()).rejects.toThrow(/ended 2 times without a response/i);
        expect(execute).toHaveBeenCalledOnce();
        expect(reattach).toHaveBeenCalledOnce();
    });

    it("reattaches without optional client or observed-response fields", async () => {
        const reattach = vi.fn(() => scheduledCall([
            response("done", { result_complete: {} }),
        ]));
        for await (const value of executeReattachable({
            ...baseRequest,
            client_type: "",
        }, {
            execute: () => scheduledCall([]),
            reattach,
            release: async () => ({}),
        }, {
            retry: noRetry,
        })) void value;
        expect(reattach).toHaveBeenCalledWith({
            session_id: "session-1",
            user_context: { user_id: "user-1" },
            operation_id: "operation-1",
        });
    });

    it("pauses and resumes a burst above the bounded queue watermarks", async () => {
        const call = new ManualCall<ReattachableResponse>();
        const generated = executeReattachable(baseRequest, {
            execute: () => call,
            reattach: () => scheduledCall([]),
            release: async () => ({}),
        }, { retry: noRetry });
        const collecting = (async () => {
            const values: ReattachableResponse[] = [];
            for await (const value of generated) values.push(value);
            return values;
        })();
        await Promise.resolve();
        for (let index = 0; index < 17; index += 1) {
            call.data(response(`response-${index}`, index === 16
                ? { result_complete: {} }
                : {}));
        }
        call.end();

        const values = await collecting;
        expect(values).toHaveLength(17);
        expect(call.pause).toHaveBeenCalledOnce();
        expect(call.resume).toHaveBeenCalledOnce();
    });

    it("cancels a pending stream on abort and preserves AbortError over later errors", async () => {
        const controller = new AbortController();
        const call = new ManualCall<ReattachableResponse>();
        const run = async () => {
            for await (const value of executeReattachable(baseRequest, {
                execute: () => call,
                reattach: () => scheduledCall([]),
                release: async () => ({}),
            }, {
                retry: noRetry,
                signal: controller.signal,
            })) void value;
        };
        const pending = run();
        await Promise.resolve();
        controller.abort("stop now");
        call.error(Object.assign(new Error("late transport error"), {
            code: grpc.status.UNAVAILABLE,
        }));
        await expect(pending).rejects.toMatchObject({
            name: "AbortError",
            message: "stop now",
        });
        expect(call.cancel).toHaveBeenCalled();
    });

    it("does not release when cancellation happens before the initial call", async () => {
        const controller = new AbortController();
        controller.abort("already stopped");
        const execute = vi.fn(() => scheduledCall([]));
        const release = vi.fn(async () => ({}));
        const run = async () => {
            for await (const value of executeReattachable(baseRequest, {
                execute,
                reattach: () => scheduledCall([]),
                release,
            }, {
                retry: noRetry,
                signal: controller.signal,
            })) void value;
        };
        await expect(run()).rejects.toMatchObject({ name: "AbortError" });
        expect(execute).not.toHaveBeenCalled();
        expect(release).not.toHaveBeenCalled();
    });

    it("isolates a throwing cleanup observer after confirmed completion", async () => {
        const onCleanupError = vi.fn(() => {
            throw new Error("observer failed");
        });
        await expect(consume(
            baseRequest,
            () => scheduledCall([response("done", { result_complete: {} })]),
            { onCleanupError },
        )).resolves.toHaveLength(1);

        const cleanup = new Error("cleanup failed");
        const values: ReattachableResponse[] = [];
        for await (const value of executeReattachable(baseRequest, {
            execute: () => scheduledCall([response("done", { result_complete: {} })]),
            reattach: () => scheduledCall([]),
            release: async () => {
                throw cleanup;
            },
        }, {
            retry: noRetry,
            onCleanupError,
        })) values.push(value);
        expect(values).toHaveLength(1);
        expect(onCleanupError).toHaveBeenCalledWith(cleanup);
    });

    it("does not let release failure override an intentional early consumer return", async () => {
        const iterator = executeReattachable(baseRequest, {
            execute: () => scheduledCall([response("first")]),
            reattach: () => scheduledCall([]),
            release: async () => {
                throw new Error("release failed");
            },
        }, { retry: noRetry });

        await expect(iterator.next()).resolves.toMatchObject({ done: false });
        await expect(iterator.return()).resolves.toMatchObject({ done: true });
    });

    it("never lets release failure mask an existing execution failure", async () => {
        const run = async () => {
            for await (const value of executeReattachable(baseRequest, {
                execute: () => scheduledCall([{
                    operation_id: "wrong",
                    response_id: "response",
                }]),
                reattach: () => scheduledCall([]),
                release: async () => {
                    throw new Error("release also failed");
                },
            }, { retry: noRetry })) void value;
        };
        await expect(run()).rejects.toThrow(/operation_id wrong/);
    });
});

/** Emits on a later event-loop turn, like a real gRPC stream fed by I/O. */
function ioCall<T>(values: T[], ending: "end" | Error = "end"): ManualCall<T> {
    const call = new ManualCall<T>();
    setImmediate(() => {
        values.forEach(value => call.data(value));
        if (ending === "end") call.end();
        else call.error(ending);
    });
    return call;
}

describe("reattachable execution liveness", () => {
    it("follows a long job through idle continuation streams of any count", async () => {
        // Spark closes a reattachable stream after senderMaxStreamDuration even
        // while the job is still computing; each idle stream "lasts" the grace.
        let clock = 0;
        let idleStreams = 0;
        const reattach = vi.fn(() => {
            clock += EMPTY_STREAM_GRACE_MS;
            idleStreams += 1;
            return scheduledCall(idleStreams <= 20 ? [] : [response("done", { result_complete: {} })]);
        });

        const rows: ReattachableResponse[] = [];
        for await (const value of executeReattachable(baseRequest, {
            execute: () => {
                clock += EMPTY_STREAM_GRACE_MS;
                return scheduledCall([]);
            },
            reattach,
            release: async () => ({}),
        }, { retry: noRetry, maxEmptyReattachments: 1, now: () => clock })) rows.push(value);

        expect(rows.map(row => row.response_id)).toEqual(["done"]);
        expect(reattach).toHaveBeenCalledTimes(21);
    });

    it("counts only streams that end empty immediately, and an idle one resets them", async () => {
        let clock = 0;
        const outcomes = ["fast", "idle", "fast", "fast", "done"];
        const reattach = vi.fn(() => {
            const outcome = outcomes.shift();
            if (outcome === "idle") clock += EMPTY_STREAM_GRACE_MS;
            return scheduledCall(outcome === "done" ? [response("done", { result_complete: {} })] : []);
        });
        const run = async () => {
            const values: ReattachableResponse[] = [];
            for await (const value of executeReattachable(baseRequest, {
                execute: () => scheduledCall([]),
                reattach,
                release: async () => ({}),
            }, { retry: noRetry, maxEmptyReattachments: 2, now: () => clock })) values.push(value);
            return values;
        };
        // execute (fast #1), fast #2, idle (reset), fast #1, fast #2, done.
        await expect(run()).resolves.toEqual([response("done", { result_complete: {} })]);

        const broken = vi.fn(() => scheduledCall<ReattachableResponse>([]));
        await expect((async () => {
            for await (const value of executeReattachable(baseRequest, {
                execute: () => scheduledCall([]),
                reattach: broken,
                release: async () => ({}),
            }, { retry: noRetry, maxEmptyReattachments: 2, now: () => clock })) void value;
        })()).rejects.toThrow(/ended 3 times without a response/);
        expect(broken).toHaveBeenCalledTimes(2);
    });

    it("never retries a failure once the execution deadline has passed", async () => {
        let clock = 0;
        const reattach = vi.fn(() => scheduledCall([response("done", { result_complete: {} })]));
        const release = vi.fn(async () => ({}));
        const unavailable = Object.assign(new Error("connection lost"), { code: grpc.status.UNAVAILABLE });

        const run = async () => {
            for await (const value of executeReattachable(baseRequest, {
                execute: () => {
                    clock = 1_500;
                    return scheduledCall([response("first")], unavailable);
                },
                reattach,
                release,
            }, {
                retry: resolveRetryConfig({ retry: { maxRetries: 5, initialBackoffMs: 0 } }),
                deadline: 1_000,
                now: () => clock,
            })) void value;
        };

        await expect(run()).rejects.toBe(unavailable);
        expect(reattach).not.toHaveBeenCalled();
        expect(release).toHaveBeenCalledOnce();
    });

    it("refuses to open a continuation after the deadline with DEADLINE_EXCEEDED", async () => {
        let clock = 0;
        const reattach = vi.fn(() => scheduledCall<ReattachableResponse>([]));
        const release = vi.fn(async () => ({}));
        const run = async () => {
            for await (const value of executeReattachable(baseRequest, {
                execute: () => {
                    clock = 2_000;
                    return scheduledCall([response("first")]);
                },
                reattach,
                release,
            }, {
                retry: resolveRetryConfig({ retry: { maxRetries: 5, initialBackoffMs: 0 } }),
                deadline: 1_000,
                now: () => clock,
            })) void value;
        };

        await expect(run()).rejects.toMatchObject({
            code: grpc.status.DEADLINE_EXCEEDED,
            message: expect.stringMatching(/execution timeout elapsed/),
        });
        expect(reattach).not.toHaveBeenCalled();
        expect(release).toHaveBeenCalledOnce();
    });

    it("keeps reattaching within the deadline", async () => {
        let clock = 0;
        const unavailable = Object.assign(new Error("connection lost"), { code: grpc.status.UNAVAILABLE });
        const rows = await consume(baseRequest, () => {
            clock = 500;
            return scheduledCall([response("first")], unavailable);
        }, {
            retry: resolveRetryConfig({ retry: { maxRetries: 1, initialBackoffMs: 0 } }),
            deadline: 1_000,
            now: () => clock,
        });
        expect(rows.map(row => row.response_id)).toEqual(["first", "complete"]);
    });

    it("waits for asynchronously opened streams, e.g. after refreshing credentials", async () => {
        const execute = vi.fn(async () => ioCall([response("first")], Object.assign(
            new Error("connection lost"),
            { code: grpc.status.UNAVAILABLE },
        )));
        const reattach = vi.fn(async () => ioCall([response("done", { result_complete: {} })]));
        const rows: ReattachableResponse[] = [];
        for await (const value of executeReattachable(baseRequest, {
            execute,
            reattach,
            release: async () => ({}),
        }, {
            retry: resolveRetryConfig({ retry: { maxRetries: 1, initialBackoffMs: 0 } }),
        })) rows.push(value);

        expect(rows.map(row => row.response_id)).toEqual(["first", "done"]);
        expect(reattach).toHaveBeenCalledWith(expect.objectContaining({ last_response_id: "first" }));
    });
});
