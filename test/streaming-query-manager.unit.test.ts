import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession, StreamingQueryManager } from "../src";
import type { SparkTelemetryEvent, StreamingQueryListener } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";

type Response = Record<string, unknown>;

/** A long-running ExecutePlan stream the test feeds, which ends on abort like a real cancelled call. */
function busStream() {
    const queue: Response[] = [];
    let wake: (() => void) | undefined;
    let ended = false;
    let failure: unknown;
    let signal: AbortSignal | undefined;
    const notify = () => { const w = wake; wake = undefined; w?.(); };
    const stream = {
        requests: [] as Array<{ request: Response; config: Record<string, unknown> | undefined }>,
        push(response: Response) { queue.push(response); notify(); },
        end() { ended = true; notify(); },
        fail(error: unknown) { failure = error; notify(); },
        get aborted() { return signal?.aborted ?? false; },
        generator: async function* (request: Response, config?: Record<string, unknown>) {
            stream.requests.push({ request, config });
            signal = config?.signal as AbortSignal | undefined;
            signal?.addEventListener("abort", () => { ended = true; notify(); }, { once: true });
            for (;;) {
                if (failure !== undefined) throw failure;
                if (queue.length > 0) { yield queue.shift()!; continue; }
                if (ended) return;
                await new Promise<void>(resolve => { wake = resolve; });
            }
        },
    };
    return stream;
}

function newSession(events: SparkTelemetryEvent[] = []) {
    vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
    vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
    return SparkSession.builder().withLogger(event => events.push(event)).getOrCreate();
}

function managerCommand(spy: { mock: { calls: unknown[][] } }, index = 0): Response {
    return (spy.mock.calls[index]![0] as { plan: { command: Response } }).plan.command;
}

const ack = { streaming_query_listener_events_result: { listener_bus_listener_added: true } };
const progressEvent = (batchId: number) => ({
    streaming_query_listener_events_result: {
        events: [{
            event_type: "QUERY_PROGRESS_EVENT",
            event_json: JSON.stringify({ progress: { id: "q", runId: "r", name: null, timestamp: "t", batchId } }),
        }],
    },
});

async function settled(): Promise<void> {
    for (let i = 0; i < 10; i += 1) await new Promise(resolve => setImmediate(resolve));
}

describe("StreamingQueryManager commands", () => {
    afterEach(() => vi.restoreAllMocks());

    it("is one object per session and lists the active queries as handles", async () => {
        const session = newSession();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{
                streaming_query_manager_command_result: {
                    active: { active_queries: [
                        { id: { id: "q1", run_id: "r1" }, name: "first" },
                        { id: { id: "q2", runId: "r2" } },
                    ] },
                },
            }])
            .mockResolvedValueOnce([{
                streaming_query_command_result: { status: { is_active: true } },
            }]);

        expect(session.streams).toBe(session.streams);
        expect(session.streams).toBeInstanceOf(StreamingQueryManager);
        const active = await session.streams.active();
        expect(managerCommand(execute)).toEqual({ streaming_query_manager_command: { active: true } });
        expect(active.map(handle => [handle.id, handle.runId, handle.name])).toEqual([["q1", "r1", "first"], ["q2", "r2", ""]]);
        await expect(active[0]!.isActive()).resolves.toBe(true);
        expect(execute.mock.calls[1]![0]).toMatchObject({
            plan: { command: { streaming_query_command: { query_id: { id: "q1", run_id: "r1" }, status: true } } },
        });
        await session.close();
        expect(() => session.streams).toThrow(/closed/);
    });

    it("accepts the camelCase manager result and rejects malformed ones", async () => {
        const session = newSession();
        vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{ streamingQueryManagerCommandResult: { active: { activeQueries: [] } } }])
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: {} }])
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: { active: { active_queries: [{ name: "no id" }] } } }])
            .mockResolvedValueOnce([{ result_complete: {} }]);

        await expect(session.streams.active()).resolves.toEqual([]);
        await expect(session.streams.active()).rejects.toThrow(/missing active result/);
        await expect(session.streams.active()).rejects.toThrow(/query without id/);
        await expect(session.streams.active()).rejects.toThrow(/missing command result/);
        await session.close();
    });

    it("finds a query by id or returns undefined", async () => {
        const session = newSession();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{
                streaming_query_manager_command_result: { query: { id: { id: "q1", run_id: "r1" }, name: "found" } },
            }])
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: {} }]);

        const found = await session.streams.get("q1");
        expect(managerCommand(execute)).toEqual({ streaming_query_manager_command: { get_query: "q1" } });
        expect(found).toMatchObject({ id: "q1", runId: "r1", name: "found" });
        await expect(session.streams.get("missing")).resolves.toBeUndefined();
        await expect(session.streams.get(" ")).rejects.toThrow(TypeError);
        await session.close();
    });

    it("waits for any termination with and without a timeout", async () => {
        const session = newSession();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: { await_any_termination: { terminated: false } } }])
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: { awaitAnyTermination: { terminated: true } } }])
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: { await_any_termination: {} } }])
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: { await_any_termination: { terminated: false } } }])
            .mockResolvedValueOnce([{ streaming_query_manager_command_result: { reset_terminated: true } }]);

        await expect(session.streams.awaitAnyTermination(1_500)).resolves.toBe(false);
        expect(managerCommand(execute)).toEqual({
            streaming_query_manager_command: { await_any_termination: { timeout_ms: "1500" } },
        });
        await expect(session.streams.awaitAnyTermination()).resolves.toBeUndefined();
        expect(managerCommand(execute, 1)).toEqual({ streaming_query_manager_command: { await_any_termination: {} } });
        await expect(session.streams.awaitAnyTermination()).rejects.toThrow(/missing termination result/);
        await expect(session.streams.awaitAnyTermination()).rejects.toThrow(/returned false without a timeout/);
        await expect(session.streams.awaitAnyTermination(-1)).rejects.toThrow(RangeError);
        await expect(session.streams.resetTerminated()).resolves.toBeUndefined();
        expect(managerCommand(execute, 4)).toEqual({ streaming_query_manager_command: { reset_terminated: true } });
        await session.close();
    });
});

describe("streaming query listeners", () => {
    afterEach(() => vi.restoreAllMocks());

    it("opens one unbounded listener bus for the first listener and dispatches every event kind", async () => {
        const events: SparkTelemetryEvent[] = [];
        const session = newSession(events);
        const bus = busStream();
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(bus.generator as never);
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const received: Array<[string, unknown]> = [];
        const listener: StreamingQueryListener = {
            onQueryStarted: event => { received.push(["started", event]); },
            onQueryProgress: async event => { received.push(["progress", event]); },
            onQueryIdle: event => { received.push(["idle", event]); },
            onQueryTerminated: event => { received.push(["terminated", event]); },
        };
        const progressOnly: StreamingQueryListener = {
            onQueryProgress: () => { throw new Error("listener exploded"); },
        };

        const adding = session.streams.addListener(listener);
        await settled();
        expect(bus.requests).toHaveLength(1);
        expect(bus.requests[0]!.request).toMatchObject({
            plan: { command: { streaming_query_listener_bus_command: { add_listener_bus_listener: true } } },
        });
        expect(bus.requests[0]!.config).toMatchObject({ executeTimeoutMs: Number.POSITIVE_INFINITY });
        bus.push(ack);
        await adding;
        await session.streams.addListener(progressOnly);
        await session.streams.addListener(listener);
        expect(session.streams.getListeners()).toEqual([listener, progressOnly]);
        expect(bus.requests).toHaveLength(1);

        bus.push(progressEvent(4));
        bus.push({ streaming_query_listener_events_result: { events: [
            { eventType: 3, eventJson: JSON.stringify({ id: "q", runId: "r", timestamp: "t" }) },
            { event_type: "QUERY_TERMINATED_EVENT", event_json: JSON.stringify({ id: "q", runId: "r", exception: null }) },
            { event_type: "QUERY_PROGRESS_UNSPECIFIED", event_json: "{}" },
            { event_type: "QUERY_PROGRESS_EVENT", event_json: "{not json" },
            { event_type: "QUERY_PROGRESS_EVENT", event_json: "[]" },
            { event_type: "QUERY_PROGRESS_EVENT", event_json: JSON.stringify({ progress: null }) },
        ] } });
        bus.push({ arrow_batch: {} });
        // Numeric enum values (a descriptor loaded without `enums: String`) and an event without payload.
        bus.push({ streamingQueryListenerEventsResult: { events: [
            { event_type: 1, event_json: JSON.stringify({ progress: { id: "q", runId: "r", name: null, timestamp: "t", batchId: 5 } }) },
            { event_type: 2, event_json: JSON.stringify({ id: "q", runId: "r", exception: "boom", errorClassName: "X" }) },
            { event_type: "QUERY_IDLE_EVENT", event_json: JSON.stringify({ id: "q", runId: "r", timestamp: "t2" }) },
            { eventType: "SOMETHING_ELSE" },
        ] } });
        await settled();
        session.notifyStreamingQueryStarted(JSON.stringify({ id: "q", runId: "r", name: "n", timestamp: "t" }));
        await settled();

        expect(received).toEqual([
            ["progress", { progress: expect.objectContaining({ batchId: 4 }) }],
            ["idle", { id: "q", runId: "r", timestamp: "t" }],
            ["terminated", { id: "q", runId: "r", exception: null }],
            ["progress", { progress: expect.objectContaining({ batchId: 5 }) }],
            ["terminated", { id: "q", runId: "r", exception: "boom", errorClassName: "X" }],
            ["idle", { id: "q", runId: "r", timestamp: "t2" }],
            ["started", { id: "q", runId: "r", name: "n", timestamp: "t" }],
        ]);
        expect(events.map(event => event.name)).toEqual([
            "spark.streaming.listener_error",
            "spark.streaming.listener_event_ignored",
            "spark.streaming.listener_event_invalid",
            "spark.streaming.listener_event_invalid",
            "spark.streaming.listener_event_invalid",
            "spark.streaming.listener_error",
            "spark.streaming.listener_event_ignored",
        ]);
        expect(events[0]!.attributes).toEqual({ reason: "listener exploded" });
        expect(events[6]!.attributes).toEqual({ reason: "Unknown streaming query event type SOMETHING_ELSE." });

        // Removing the last listener tells the server and closes the stream.
        await session.streams.removeListener(progressOnly);
        expect(execute).not.toHaveBeenCalled();
        await session.streams.removeListener(listener);
        await session.streams.removeListener(listener);
        expect(managerCommand(execute)).toEqual({
            streaming_query_listener_bus_command: { remove_listener_bus_listener: true },
        });
        expect(bus.aborted).toBe(true);
        expect(session.streams.getListeners()).toEqual([]);
        await session.close();
    });

    it("rejects addListener when the bus ends or fails before the server acknowledges it", async () => {
        const events: SparkTelemetryEvent[] = [];
        const session = newSession(events);
        const ended = busStream();
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementationOnce(ended.generator as never);
        const listener: StreamingQueryListener = { onQueryIdle: () => undefined };

        const adding = session.streams.addListener(listener);
        await settled();
        ended.end();
        await expect(adding).rejects.toThrow(/ended before the server acknowledged/);
        expect(session.streams.getListeners()).toEqual([]);

        const failing = busStream();
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementationOnce(failing.generator as never);
        const retry = session.streams.addListener(listener);
        await settled();
        failing.fail("bus broke");
        await expect(retry).rejects.toBe("bus broke");
        expect(events.map(event => [event.name, event.attributes.reason])).toEqual([
            ["spark.streaming.listener_bus_error", "bus broke"],
        ]);
        await expect(session.streams.addListener(null as never)).rejects.toThrow(TypeError);
        await session.close();
    });

    it("drops the listeners and the bus when the session closes, without a remove command", async () => {
        const events: SparkTelemetryEvent[] = [];
        const session = newSession(events);
        const bus = busStream();
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(bus.generator as never);
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const terminated = vi.fn();
        const listener: StreamingQueryListener = { onQueryTerminated: terminated };

        const adding = session.streams.addListener(listener);
        await settled();
        bus.push({ streaming_query_listener_events_result: { listenerBusListenerAdded: true } });
        await adding;
        // Nobody listens for progress, so the event is not even decoded.
        bus.push(progressEvent(1));
        await settled();
        expect(terminated).not.toHaveBeenCalled();
        expect(events).toEqual([]);

        await session.close();
        expect(bus.aborted).toBe(true);
        expect(execute).not.toHaveBeenCalled();
    });

    it("does not report the abort of a bus it closed on purpose", async () => {
        const events: SparkTelemetryEvent[] = [];
        const session = newSession(events);
        let signal: AbortSignal | undefined;
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(
            async function* (_request, config) {
                signal = config?.signal;
                yield ack;
                await new Promise<void>((_resolve, reject) => {
                    signal?.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })));
                });
            },
        );
        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const listener: StreamingQueryListener = { onQueryIdle: () => undefined };

        await session.streams.addListener(listener);
        await session.streams.removeListener(listener);
        expect(signal?.aborted).toBe(true);
        expect(events).toEqual([]);
        await session.close();
    });
});
