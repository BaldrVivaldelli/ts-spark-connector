import type { SparkSession } from "./session";
import { SparkConnectExecutor } from "./sparkConnectExecutor";
import type { StreamingQueryHandle } from "./sparkClient";
import { emitTelemetry } from "./telemetry";
import {
    extractListenerEventsResult,
    extractStreamingQueryInstance,
    extractStreamingQueryManagerResult,
    parseStreamingProgress,
    validateTerminationTimeout,
    type StreamingQueryListenerEventMessage,
    type StreamingQueryManagerCommandResult,
    type StreamingQueryProgress,
} from "./streamingProtocol";

/**
 * A streaming query started in this session.
 * @public
 */
export type QueryStartedEvent = {
    id: string;
    runId: string;
    name: string | null;
    timestamp: string;
} & Record<string, unknown>;

/**
 * A streaming query completed a batch.
 * @public
 */
export type QueryProgressEvent = {
    progress: StreamingQueryProgress;
};

/**
 * A streaming query had no data to process in a trigger.
 * @public
 */
export type QueryIdleEvent = {
    id: string;
    runId: string;
    timestamp: string;
} & Record<string, unknown>;

/**
 * A streaming query stopped, with the failure message when it failed.
 * @public
 */
export type QueryTerminatedEvent = {
    id: string;
    runId: string;
    exception?: string | null;
    errorClassName?: string | null;
} & Record<string, unknown>;

/**
 * Callbacks for the lifecycle of the session's streaming queries, like
 * PySpark's `StreamingQueryListener`. Every method is optional; a rejected
 * promise or a thrown error is reported through the session logger and never
 * stops the event stream.
 * @public
 */
export interface StreamingQueryListener {
    onQueryStarted?(event: QueryStartedEvent): void | Promise<void>;
    onQueryProgress?(event: QueryProgressEvent): void | Promise<void>;
    onQueryIdle?(event: QueryIdleEvent): void | Promise<void>;
    onQueryTerminated?(event: QueryTerminatedEvent): void | Promise<void>;
}

type ListenerMethod = keyof StreamingQueryListener;

/** Reads the first manager result of a command's responses, which every manager command reports. */
function managerResult(
    responses: Array<Record<string, unknown>>,
    label: string,
): StreamingQueryManagerCommandResult {
    const result = responses
        .map(extractStreamingQueryManagerResult)
        .find((value): value is StreamingQueryManagerCommandResult => value !== undefined);
    if (!result) {
        throw new Error(`Invalid streaming query manager ${label} response: missing command result.`);
    }
    return result;
}

function parseEvent(json: string, label: string): Record<string, unknown> {
    let parsed: unknown;
    try {
        parsed = JSON.parse(json);
    } catch (error) {
        throw new Error(`Invalid ${label} event: ${(error as Error).message}`, { cause: error });
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error(`Invalid ${label} event: expected a JSON object.`);
    }
    return parsed as Record<string, unknown>;
}

/**
 * Spark's `StreamingQueryManager` for one session: lists and finds running
 * queries, waits for terminations and delivers query lifecycle events to
 * listeners. Obtained from `session.streams`.
 * @public
 */
export class StreamingQueryManager {
    private readonly listeners: StreamingQueryListener[] = [];
    private bus?: {
        abort: AbortController;
        /** Resolves once the server acknowledged the listener bus. */
        ready: Promise<void>;
        /** Settles when the event stream ends. */
        done: Promise<void>;
    };

    /** @internal Obtained from `session.streams`. */
    constructor(private readonly session: SparkSession) {}

    /** Handles of the queries currently running in this session. */
    async active(): Promise<StreamingQueryHandle[]> {
        const executor = SparkConnectExecutor.for(this.session);
        const result = managerResult(
            await executor.runCommand({ streaming_query_manager_command: { active: true } }),
            "active",
        );
        const instances = result.active?.active_queries ?? result.active?.activeQueries;
        if (!instances) throw new Error("Invalid streaming query manager active response: missing active result.");
        return instances.map(instance => {
            const query = extractStreamingQueryInstance(instance);
            if (!query) throw new Error("Invalid streaming query manager active response: query without id.");
            return executor.streamingQueryHandle(query.queryId, query.name);
        });
    }

    /** The query with this id, or `undefined` when no such query is running. */
    async get(id: string): Promise<StreamingQueryHandle | undefined> {
        if (typeof id !== "string" || !id.trim()) throw new TypeError("get() id must be a non-empty string.");
        const executor = SparkConnectExecutor.for(this.session);
        const result = managerResult(
            await executor.runCommand({ streaming_query_manager_command: { get_query: id } }),
            "get",
        );
        const query = extractStreamingQueryInstance(result.query);
        return query ? executor.streamingQueryHandle(query.queryId, query.name) : undefined;
    }

    /**
     * Waits until any query of the session terminates (or already did since the
     * last `resetTerminated()`). With a timeout, resolves to whether one did.
     */
    awaitAnyTermination(): Promise<void>;
    awaitAnyTermination(timeoutMs: number): Promise<boolean>;
    async awaitAnyTermination(timeoutMs?: number): Promise<void | boolean> {
        if (timeoutMs !== undefined) validateTerminationTimeout(timeoutMs);
        const command = timeoutMs === undefined
            ? { await_any_termination: {} }
            : { await_any_termination: { timeout_ms: String(timeoutMs) } };
        const result = managerResult(
            await SparkConnectExecutor.for(this.session).runCommand({ streaming_query_manager_command: command }),
            "awaitAnyTermination",
        );
        const terminated = (result.await_any_termination ?? result.awaitAnyTermination)?.terminated;
        if (typeof terminated !== "boolean") {
            throw new Error("Invalid awaitAnyTermination response: missing termination result.");
        }
        if (timeoutMs !== undefined) return terminated;
        if (!terminated) throw new Error("awaitAnyTermination returned false without a timeout.");
    }

    /** Forgets terminated queries so `awaitAnyTermination()` waits for a new termination. */
    async resetTerminated(): Promise<void> {
        managerResult(
            await SparkConnectExecutor.for(this.session).runCommand({
                streaming_query_manager_command: { reset_terminated: true },
            }),
            "resetTerminated",
        );
    }

    /** The listeners registered through `addListener`. */
    getListeners(): readonly StreamingQueryListener[] {
        return [...this.listeners];
    }

    /**
     * Registers a listener. The first listener opens a long-running execution
     * on which Spark delivers the session's query events; the promise resolves
     * once the server acknowledged it, so events of queries started afterwards
     * are not missed.
     */
    async addListener(listener: StreamingQueryListener): Promise<void> {
        if (listener === null || typeof listener !== "object") {
            throw new TypeError("addListener() requires a listener object.");
        }
        if (this.listeners.includes(listener)) return;
        this.listeners.push(listener);
        if (!this.bus) this.bus = this.openBus();
        try {
            await this.bus.ready;
        } catch (error) {
            this.listeners.splice(this.listeners.indexOf(listener), 1);
            throw error;
        }
    }

    /** Unregisters a listener; the last removal closes the event stream. */
    async removeListener(listener: StreamingQueryListener): Promise<void> {
        const index = this.listeners.indexOf(listener);
        if (index < 0) return;
        this.listeners.splice(index, 1);
        if (this.listeners.length === 0) await this.closeBus(true);
    }

    /** @internal Delivers the start event Spark attached to a query started by this session. */
    dispatchQueryStarted(eventJson: string): void {
        void this.dispatch("onQueryStarted", () => parseEvent(eventJson, "query started") as QueryStartedEvent);
    }

    /** @internal Drops the listeners and the event stream when the session closes. */
    async shutdown(): Promise<void> {
        this.listeners.length = 0;
        await this.closeBus(false);
    }

    private openBus(): NonNullable<StreamingQueryManager["bus"]> {
        const abort = new AbortController();
        let markReady!: () => void;
        let failReady!: (error: unknown) => void;
        const ready = new Promise<void>((resolve, reject) => {
            markReady = resolve;
            failReady = reject;
        });
        // The rejection reaches callers through `addListener`; it must not
        // surface as an unhandled rejection when nobody awaits `ready`.
        ready.catch(() => undefined);
        const bus = { abort, ready, done: Promise.resolve() };
        bus.done = (async () => {
            const executor = SparkConnectExecutor.for(this.session);
            const execution = this.session.resolveExecution({ signal: abort.signal });
            try {
                for await (const response of executor.streamCommand(
                    { streaming_query_listener_bus_command: { add_listener_bus_listener: true } },
                    execution,
                    { unboundedStreams: true },
                )) {
                    const result = extractListenerEventsResult(response);
                    if (!result) continue;
                    if (result.listener_bus_listener_added ?? result.listenerBusListenerAdded) markReady();
                    for (const event of result.events ?? []) await this.dispatchBusEvent(event);
                }
                failReady(new Error("The streaming listener bus ended before the server acknowledged it."));
            } catch (error) {
                failReady(error);
                if (!abort.signal.aborted) this.report("spark.streaming.listener_bus_error", error);
            } finally {
                // A stream that ended on its own is gone; the next listener opens a new one.
                if (this.bus === bus) this.bus = undefined;
            }
        })();
        return bus;
    }

    private async closeBus(notifyServer: boolean): Promise<void> {
        const bus = this.bus;
        if (!bus) return;
        this.bus = undefined;
        if (notifyServer) {
            // Spark ends the long-running execution once the bus listener is
            // removed; the abort below only covers a server that does not.
            await SparkConnectExecutor.for(this.session).runCommand({
                streaming_query_listener_bus_command: { remove_listener_bus_listener: true },
            });
        }
        bus.abort.abort("streaming listener bus closed");
        await bus.done;
    }

    private async dispatchBusEvent(event: StreamingQueryListenerEventMessage): Promise<void> {
        const json = event.event_json ?? event.eventJson ?? "";
        switch (event.event_type ?? event.eventType) {
            case "QUERY_PROGRESS_EVENT":
            case 1:
                await this.dispatch("onQueryProgress", () => {
                    const parsed = parseEvent(json, "query progress");
                    const progress = parsed.progress;
                    if (progress === null || typeof progress !== "object") {
                        throw new Error("Invalid query progress event: missing progress.");
                    }
                    return { progress: parseStreamingProgress(JSON.stringify(progress)) };
                });
                return;
            case "QUERY_TERMINATED_EVENT":
            case 2:
                await this.dispatch("onQueryTerminated", () => parseEvent(json, "query terminated") as QueryTerminatedEvent);
                return;
            case "QUERY_IDLE_EVENT":
            case 3:
                await this.dispatch("onQueryIdle", () => parseEvent(json, "query idle") as QueryIdleEvent);
                return;
            default:
                this.report("spark.streaming.listener_event_ignored", new Error(
                    `Unknown streaming query event type ${String(event.event_type ?? event.eventType)}.`,
                ));
        }
    }

    private async dispatch<M extends ListenerMethod>(
        method: M,
        decode: () => Parameters<NonNullable<StreamingQueryListener[M]>>[0],
    ): Promise<void> {
        const targets = this.listeners.filter(listener => typeof listener[method] === "function");
        if (targets.length === 0) return;
        let event: Parameters<NonNullable<StreamingQueryListener[M]>>[0];
        try {
            event = decode();
        } catch (error) {
            this.report("spark.streaming.listener_event_invalid", error);
            return;
        }
        for (const listener of targets) {
            try {
                await (listener[method] as (payload: typeof event) => void | Promise<void>).call(listener, event);
            } catch (error) {
                this.report("spark.streaming.listener_error", error);
            }
        }
    }

    private report(name: string, error: unknown): void {
        emitTelemetry(this.session.getConnectionConfig(), name, "warn", {
            reason: error instanceof Error ? error.message : String(error),
        });
    }
}
