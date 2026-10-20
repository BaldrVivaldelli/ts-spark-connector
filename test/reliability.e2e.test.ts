import { afterAll, describe, expect, it } from "vitest";
import { status } from "@grpc/grpc-js";
import { SparkConnectError, SparkSession, schema } from "../src";
import type { SparkTelemetryEvent } from "../src";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";

function connectedBuilder() {
    const builder = SparkSession.builder().config("spark.connect.url", connectUrl);
    if (connectUrl.startsWith("scs://")) {
        builder.enableTLS({
            trustStorePath: caPath,
            serverNameOverride: serverName,
        });
    }
    return builder;
}

const session = connectedBuilder().getOrCreate();
afterAll(async () => session.close());

/** Resolves with the rejection reason so several assertions can target one error object. */
async function captureRejection(operation: Promise<unknown>): Promise<unknown> {
    try {
        await operation;
    } catch (error) {
        return error;
    }
    throw new Error("Expected the operation to reject.");
}

/** Parses Spark byte strings such as "6m", "6291456b" or "1073741824". */
function sparkBytes(value: string): number {
    const match = /^(\d+)\s*([kmgt]?)b?$/i.exec(value.trim());
    if (!match) throw new Error(`Unrecognised Spark byte value: ${value}`);
    const unit = match[2]!.toLowerCase();
    const factor = unit === "k" ? 1024 : unit === "m" ? 1024 ** 2 : unit === "g" ? 1024 ** 3 : unit === "t" ? 1024 ** 4 : 1;
    return Number(match[1]) * factor;
}

function eventNames(events: readonly SparkTelemetryEvent[]): string[] {
    return events.map(event => event.name);
}

function releasedExecute(events: readonly SparkTelemetryEvent[]): boolean {
    return events.some(event => event.name === "spark.rpc.end" && event.attributes.rpc === "ReleaseExecute");
}

/**
 * spark-server/conf/spark-defaults.conf lowers this static conf to 6 MiB so a
 * result larger than that is delivered through several ReattachExecute
 * streams. Spark rejects runtime changes with CANNOT_MODIFY_CONFIG.
 */
const SENDER_MAX_STREAM_SIZE = "spark.connect.execute.reattachable.senderMaxStreamSize";
const SENDER_MAX_STREAM_SIZE_CEILING = 8 * 1024 * 1024;

/**
 * 192 single-row partitions that each sleep `sleepMs`. Spark Connect streams
 * the result partition by partition, so the first Arrow batch arrives after
 * about `sleepMs` on any machine while the whole job takes about
 * `sleepMs * ceil(192 / cores)`: long enough to notice when a cancelled job is
 * left running, bounded if server-side cleanup ever regressed. java_method is a
 * Spark builtin, so no extra jars are involved.
 */
function slowJobSql(sleepMs: number): string {
    return `SELECT java_method('java.lang.Thread', 'sleep', ${sleepMs}L) AS napped FROM range(0, 192, 1, 192)`;
}
const SLOW_JOB_MIN_MS = 1_500;
const SLOW_JOB_SQL = slowJobSql(SLOW_JOB_MIN_MS);

/**
 * Two partitions: the first sleeps 0.5 s so the first batch arrives at once,
 * the second sleeps 30 s so the job stays running. Several of these fit the
 * executor at the same time, which the 192-partition job does not: with FIFO
 * scheduling a second job only starts once every task of the first has been
 * launched.
 */
const SMALL_SLOW_JOB_SQL =
    "SELECT java_method('java.lang.Thread', 'sleep', CASE WHEN id = 0 THEN 500L ELSE 30000L END) AS napped " +
    "FROM range(0, 2, 1, 2)";

type RunningJob = {
    /** Resolves once the first Arrow batch arrived, i.e. the job is running on the server. */
    started: Promise<void>;
    /** Settles when the stream ends; resolves with the number of Arrow batches. */
    finished: Promise<number>;
};

/**
 * Consumes a raw response stream. Tests that cancel or interrupt wait for
 * `started` first: an interrupt that reaches the server before the job is
 * submitted finds nothing to cancel, and the job then runs to completion.
 */
function runSlowJob(responses: AsyncIterable<Record<string, unknown>>): RunningJob {
    let markStarted!: () => void;
    const started = new Promise<void>(resolve => { markStarted = resolve; });
    const finished = (async () => {
        let batches = 0;
        for await (const response of responses) {
            if ((response as { arrow_batch?: { data?: unknown } }).arrow_batch?.data) {
                batches += 1;
                if (batches === 1) markStarted();
            }
        }
        return batches;
    })();
    // A job that fails before its first batch must not hang the test; the
    // failure surfaces when the test awaits `finished`.
    finished.catch(() => markStarted());
    return { started, finished };
}

describe("production reliability (E2E)", () => {
    it("round-trips decimal, timestamp, array, struct and map values through Arrow", async () => {
        const rows = await session.sql(`
            SELECT
              CAST(123.45 AS DECIMAL(10, 2)) AS amount,
              TIMESTAMP '2024-01-02 03:04:05' AS instant,
              array(1, 2, 3) AS values,
              named_struct('count', 2, 'label', 'ok') AS nested,
              map('x', 7) AS labels
        `).collect() as Array<{
            amount: string;
            instant: string;
            values: number[];
            nested: { count: number; label: string };
            labels: Map<string, number>;
        }>;

        expect(rows).toHaveLength(1);
        expect(rows[0]).toMatchObject({
            amount: "123.45",
            instant: "2024-01-02T03:04:05.000Z",
            values: [1, 2, 3],
            nested: { count: 2, label: "ok" },
        });
        expect(rows[0]?.labels).toEqual(new Map([["x", 7]]));
    });

    it("preserves structured remote SQL failures", async () => {
        const error = await captureRejection(session.sql(
            "SELECT definitely_missing_column FROM VALUES (1)",
        ).collect()) as SparkConnectError;

        expect(error).toBeInstanceOf(SparkConnectError);
        expect(error).toMatchObject({
            operation: "ExecutePlan",
            code: status.INTERNAL,
            errorClass: "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        });
        expect(error.message).toMatch(/^ExecutePlan failed: \[UNRESOLVED_COLUMN\.WITH_SUGGESTION\]/);
        expect(error.message).toContain("definitely_missing_column");
        // google.rpc.ErrorInfo decoded from grpc-status-details-bin.
        expect(error.errorId).toMatch(/^[0-9a-f-]{36}$/);
        // FetchErrorDetails enrichment: the response carries no session_id, and an
        // empty id used to be rejected as a foreign session, leaving this undefined.
        expect(error.remoteDetails).toBeDefined();
        expect(error.errorInfo).toMatchObject({
            reason: "org.apache.spark.sql.catalyst.ExtendedAnalysisException",
            domain: "org.apache.spark",
            metadata: {
                errorClass: "UNRESOLVED_COLUMN.WITH_SUGGESTION",
                sqlState: "42703",
                errorId: error.errorId,
            },
        });
    });

    it("recovers a large result through ReattachExecute when the server ends the stream early", async () => {
        const events: SparkTelemetryEvent[] = [];
        const observed = connectedBuilder().withLogger(event => events.push(event)).getOrCreate();
        try {
            // Precondition: the static conf in spark-defaults.conf is in effect.
            const limit = sparkBytes(await observed.conf.get(SENDER_MAX_STREAM_SIZE));
            expect(limit).toBeLessThanOrEqual(SENDER_MAX_STREAM_SIZE_CEILING);

            // 2,000,000 bigint ids are ~16 MiB of Arrow data: at least two
            // server-side stream completions, each resumed with ReattachExecute.
            const rows = await observed.sql("SELECT id FROM range(2000000)").collect() as Array<{ id: bigint }>;
            expect(rows).toHaveLength(2_000_000);
            expect(rows[0]).toEqual({ id: 0n });
            expect(rows[rows.length - 1]).toEqual({ id: 1_999_999n });
            let checksum = 0n;
            for (const row of rows) checksum += row.id;
            expect(checksum).toBe(1_999_999_000_000n); // sum of 0..1,999,999

            const starts = events.filter(event => event.name === "spark.execute.start");
            const reattaches = events.filter(event => event.name === "spark.execute.reattach");
            expect(starts).toHaveLength(1);
            expect(reattaches.length).toBeGreaterThanOrEqual(1);
            for (const reattach of reattaches) {
                expect(reattach.level).toBe("info");
                // Resumed from the last acknowledged response of the same operation,
                // never replayed from scratch.
                expect(reattach.attributes.operationId).toBe(starts[0]!.attributes.operationId);
                expect(typeof reattach.attributes.lastResponseId).toBe("string");
                expect(reattach.attributes.lastResponseId).not.toBe("");
            }
            expect(events.filter(event => event.name === "spark.execute.end")).toHaveLength(1);
            expect(eventNames(events)).not.toContain("spark.execute.error");
            expect(releasedExecute(events)).toBe(true);
        } finally {
            await observed.close();
        }
    });

    it("concatenates a multi-batch Arrow result in order without any reattach", async () => {
        const events: SparkTelemetryEvent[] = [];
        const observed = connectedBuilder().withLogger(event => events.push(event)).getOrCreate();
        try {
            // Six explicit partitions and Spark's 10,000-row Arrow batches keep this
            // result (~3.7 MiB) under the 6 MiB reattach threshold while still
            // spanning dozens of batches.
            const df = observed.sql(
                "SELECT id, CAST(id AS INT) AS id_int, id % 2 = 0 AS is_even FROM range(0, 300000, 1, 6)",
            );
            const rows = await df.collect() as Array<{ id: bigint; id_int: number; is_even: boolean }>;
            expect(rows).toHaveLength(300_000);
            expect(rows[0]).toEqual({ id: 0n, id_int: 0, is_even: true });
            expect(rows[299_999]).toEqual({ id: 299_999n, id_int: 299_999, is_even: false });
            // Batches are concatenated in partition order with nothing lost or repeated.
            for (let index = 0; index < rows.length; index += 1) {
                const row = rows[index]!;
                if (row.id !== BigInt(index) || row.id_int !== index || row.is_even !== (index % 2 === 0)) {
                    throw new Error(`Row ${index} decoded as ${JSON.stringify({ ...row, id: String(row.id) })}`);
                }
            }

            let batches = 0;
            for await (const _buffer of df.toArrowBatches()) batches += 1;
            expect(batches).toBeGreaterThan(1);

            expect(eventNames(events)).not.toContain("spark.execute.reattach");
            expect(eventNames(events)).not.toContain("spark.execute.error");
        } finally {
            await observed.close();
        }
    });

    it("honors a signal that is already aborted before an RPC", async () => {
        const controller = new AbortController();
        controller.abort("cancelled by E2E");
        const abortedSession = connectedBuilder()
            .withAbortSignal(controller.signal)
            .getOrCreate();

        await expect(abortedSession.sql("SELECT 1").collect()).rejects.toMatchObject({
            name: "AbortError",
        });
        await abortedSession.close().catch(() => undefined);
    });

    it("aborts an in-flight ExecutePlan and leaves the session permanently aborted", async () => {
        const events: SparkTelemetryEvent[] = [];
        const controller = new AbortController();
        const abortable = connectedBuilder()
            .withAbortSignal(controller.signal)
            .withLogger(event => events.push(event))
            .getOrCreate();
        const reason = "cancelled mid-stream by E2E";

        const job = runSlowJob(abortable.sql(SLOW_JOB_SQL).streamRaw());
        await job.started;
        const abortedAt = Date.now();
        controller.abort(reason);
        const error = await captureRejection(job.finished) as Error;
        const elapsedMs = Date.now() - abortedAt;

        // The rejection is the abort itself, not a transport failure, and it
        // does not wait for the remaining partitions.
        expect(error).toBeInstanceOf(Error);
        expect(error).not.toBeInstanceOf(SparkConnectError);
        expect(error.name).toBe("AbortError");
        expect(error.message).toBe(reason);
        expect(elapsedMs).toBeLessThan(5_000);

        // Cancellation still releases the operation server-side.
        expect(eventNames(events)).toContain("spark.execute.start");
        expect(releasedExecute(events)).toBe(true);
        expect(events.find(event => event.name === "spark.execute.error")?.attributes).toMatchObject({
            code: undefined,
        });
        expect(eventNames(events)).not.toContain("spark.execute.end");

        // Contract: the signal is session-wide and sticky. Every later RPC on this
        // session rejects immediately with the same AbortError ...
        await expect(abortable.sql("SELECT 1 AS alive").collect()).rejects.toMatchObject({
            name: "AbortError",
            message: reason,
        });
        // ... while close() strips the signal for cleanup and still succeeds.
        await expect(abortable.close()).resolves.toBeUndefined();

        // Liveness: a fresh session is unaffected, and it is fast because the
        // released job no longer occupies the executor.
        const probeStartedAt = Date.now();
        expect(await session.sql("SELECT 1 AS alive").collect()).toEqual([{ alive: 1 }]);
        expect(Date.now() - probeStartedAt).toBeLessThan(5_000);
    });

    it("maps a per-RPC deadline to DEADLINE_EXCEEDED and releases the server-side job", async () => {
        const events: SparkTelemetryEvent[] = [];
        const deadlineSession = connectedBuilder()
            .withRpcTimeout(1_000)
            .withLogger(event => events.push(event))
            .getOrCreate();
        try {
            // Partitions of 0.5 s: the first batch arrives before the 1 s
            // deadline, so the deadline cuts a running job, and the deadline
            // fires before the server ends the stream at 2 s (which would
            // start a fresh per-stream deadline).
            const startedAt = Date.now();
            const error = await captureRejection(deadlineSession.sql(slowJobSql(500)).collect()) as SparkConnectError;
            const elapsedMs = Date.now() - startedAt;

            expect(error).toBeInstanceOf(SparkConnectError);
            expect(error).toMatchObject({
                name: "SparkConnectError",
                operation: "ExecutePlan",
                code: status.DEADLINE_EXCEEDED,
            });
            expect(error.message).toMatch(/^ExecutePlan failed: Deadline exceeded/);
            expect(error.details).toMatch(/^Deadline exceeded/);
            expect(error.errorId).toBeUndefined();
            expect(elapsedMs).toBeGreaterThanOrEqual(900);
            expect(elapsedMs).toBeLessThan(10_000);

            // The client reports the failure and releases the operation, which
            // interrupts the job instead of leaving it running until the
            // server's abandoned-execution timeout.
            expect(events.find(event => event.name === "spark.execute.error")?.attributes).toMatchObject({
                code: status.DEADLINE_EXCEEDED,
            });
            expect(releasedExecute(events)).toBe(true);
        } finally {
            await deadlineSession.close();
        }

        // With FIFO scheduling an un-released job would queue this single task
        // behind ~190 pending 1.5 s tasks; after ReleaseExecute it runs at once.
        const probeStartedAt = Date.now();
        expect(await session.sql("SELECT 1 AS alive").collect()).toEqual([{ alive: 1 }]);
        expect(Date.now() - probeStartedAt).toBeLessThan(5_000);
    });

    it("decodes readWith(schema) rows from a file source with the declared shape", async () => {
        // Spark marks every file-source column nullable (HadoopFsRelation applies
        // dataSchema.asNullable), so the Arrow schema reports id/name as nullable
        // even though the declared DDL says NOT NULL. The decoder therefore checks
        // the data rather than the metadata: a declared non-null read of a
        // well-formed file must decode, and only an actual null fails.
        const People = schema({ id: "int", name: "string" });
        const rows = await session.read
            .readWith(People, "csv", "/data/people.tsv", { header: "true", delimiter: "\t" })
            .collect();

        expect(rows).toEqual([
            { id: 1, name: "Alice" },
            { id: 2, name: "Bob" },
            { id: 3, name: "Carol" },
            { id: 3, name: "Carol" },
        ]);
        for (const row of rows) {
            expect(typeof row.id).toBe("number");
            expect(typeof row.name).toBe("string");
        }
    });

    it.runIf(connectUrl.startsWith("scs://"))(
        "rejects a TLS server-name mismatch",
        async () => {
            const invalidTlsSession = SparkSession.builder()
                .config("spark.connect.url", connectUrl)
                .enableTLS({
                    trustStorePath: caPath,
                    serverNameOverride: "invalid.spark-connect.test",
                })
                .withRpcTimeout(5_000)
                .getOrCreate();

            const error = await captureRejection(invalidTlsSession.sql("SELECT 1").collect()) as SparkConnectError;

            // The handshake failure surfaces as an UNAVAILABLE ExecutePlan with the
            // TLS verification reason preserved in the message.
            expect(error).toBeInstanceOf(SparkConnectError);
            expect(error).toMatchObject({
                name: "SparkConnectError",
                operation: "ExecutePlan",
                code: status.UNAVAILABLE,
            });
            expect(error.message).toMatch(/^ExecutePlan failed: /);
            expect(error.message).toMatch(/Hostname\/IP does not match certificate's altnames/);
            expect(error.message).toContain("invalid.spark-connect.test");
            expect(error.details).toContain("ERR_TLS_CERT_ALTNAME_INVALID");

            // Nothing reached the server, so no remote error context exists.
            expect(error.errorId).toBeUndefined();
            expect(error.errorClass).toBeUndefined();

            await invalidTlsSession.close().catch(() => undefined);
        },
        30_000,
    );
});

/**
 * One task that sleeps `ms`, so the job lasts that long on any machine and
 * produces its single row only at the end. java_method is a Spark builtin.
 */
function sleepJobSql(ms: number): string {
    return "SELECT COUNT(napped) AS n FROM (" +
        `SELECT java_method('java.lang.Thread', 'sleep', ${ms}L) AS napped FROM range(0, 1, 1, 1))`;
}

/** spark-server/conf/spark-defaults.conf ends every response stream after 2 s. */
const SENDER_MAX_STREAM_DURATION_MS = 2_000;

function reattachCount(events: readonly SparkTelemetryEvent[]): number {
    return events.filter(event => event.name === "spark.execute.reattach").length;
}

describe("execution liveness and per-query control (E2E)", () => {
    it("follows a job that stays silent through more than eight empty streams", async ctx => {
        const events: SparkTelemetryEvent[] = [];
        const observed = connectedBuilder().withLogger(event => events.push(event)).getOrCreate();
        try {
            const rows = await observed.sql(sleepJobSql(20_000)).collect();
            expect(rows).toHaveLength(1);
        } finally {
            await observed.close();
        }

        // The former limit failed the query after eight empty streams, so the
        // regression is only observable when the server produces more. Servers
        // from 4.0.4 on end an idle stream every senderMaxStreamDuration (2 s
        // here), about ten in this job; Spark 4.0.0 ends them irregularly (one
        // to three in 20 s), which is not enough to tell the fix from the bug.
        const idleStreams = reattachCount(events);
        const serverVersion = process.env.SPARK_VERSION;
        if (idleStreams <= 8 && (serverVersion === undefined || serverVersion === "4.0.0")) {
            return ctx.skip(`only ${idleStreams} idle streams on this server; the regression is not observable`);
        }
        expect(idleStreams).toBeGreaterThan(8);
    });

    it("cancels one execution with its own signal and keeps the session usable", async () => {
        const controller = new AbortController();
        const job = runSlowJob(session.sql(SLOW_JOB_SQL).streamRaw({ signal: controller.signal }));
        await job.started;
        const abortedAt = Date.now();
        controller.abort("stop this query");

        expect(await captureRejection(job.finished)).toMatchObject({ name: "AbortError", message: "stop this query" });
        expect(Date.now() - abortedAt).toBeLessThan(5_000);

        // Same session, immediately usable, and fast because the job was released.
        const probeStartedAt = Date.now();
        expect(await session.sql("SELECT 1 AS alive").collect()).toEqual([{ alive: 1 }]);
        expect(Date.now() - probeStartedAt).toBeLessThan(5_000);
    });

    it("interrupts tagged operations from a different call", async () => {
        const operationId = "6f1c5a52-6a0e-4a46-9f39-0c6f6a3b0c11";
        session.addTag("e2e-session-tag");
        const job = runSlowJob(session.sql(SLOW_JOB_SQL).streamRaw({ tags: ["e2e-slow"], operationId }));
        session.removeTag("e2e-session-tag");
        await job.started;

        await expect(session.interruptTag("e2e-session-tag")).resolves.toEqual([operationId]);
        const error = await captureRejection(job.finished) as SparkConnectError;
        expect(error).toBeInstanceOf(SparkConnectError);
        expect(error.message).toMatch(/cancel/i);
        await expect(session.interruptTag("e2e-slow")).resolves.toEqual([]);
        expect(await session.sql("SELECT 1 AS alive").collect()).toEqual([{ alive: 1 }]);
    });

    it("interrupts one operation by id and the rest with interruptAll", async () => {
        const first = "0b9f7c1e-2d4a-4c55-8e7f-1a2b3c4d5e6f";
        const second = "7d3e9a10-5b6c-4d7e-9f80-a1b2c3d4e5f6";
        const firstJob = runSlowJob(session.sql(SMALL_SLOW_JOB_SQL).streamRaw({ operationId: first }));
        const secondJob = runSlowJob(session.sql(SMALL_SLOW_JOB_SQL).streamRaw({ operationId: second }));
        await Promise.all([firstJob.started, secondJob.started]);

        await expect(session.interruptOperation(first)).resolves.toEqual([first]);
        expect(await captureRejection(firstJob.finished)).toBeInstanceOf(SparkConnectError);
        await expect(session.interruptAll()).resolves.toEqual([second]);
        expect(await captureRejection(secondJob.finished)).toBeInstanceOf(SparkConnectError);
        expect(await session.sql("SELECT 1 AS alive").collect()).toEqual([{ alive: 1 }]);
    });

    it("keeps unary deadlines short without capping how long a query may run", async () => {
        const unbounded = connectedBuilder()
            .withRpcTimeout(SENDER_MAX_STREAM_DURATION_MS / 4)
            .withExecuteTimeout(Number.POSITIVE_INFINITY)
            .getOrCreate();
        try {
            await expect(unbounded.sql(sleepJobSql(5_000)).collect()).resolves.toHaveLength(1);
            await expect(unbounded.conf.get("spark.sql.shuffle.partitions")).resolves.toEqual(expect.any(String));
        } finally {
            await unbounded.close();
        }
    });

    it("bounds the whole execution across reattaches and retries with executeTimeoutMs", async () => {
        const events: SparkTelemetryEvent[] = [];
        const bounded = connectedBuilder()
            .withExecuteTimeout(5_000)
            .withRetry({ maxRetries: 5, initialBackoffMs: 50, maxBackoffMs: 100 })
            .withLogger(event => events.push(event))
            .getOrCreate();
        try {
            const startedAt = Date.now();
            const error = await captureRejection(bounded.sql(SLOW_JOB_SQL).collect()) as SparkConnectError;
            const elapsedMs = Date.now() - startedAt;

            expect(error).toMatchObject({ operation: "ExecutePlan", code: status.DEADLINE_EXCEEDED });
            expect(elapsedMs).toBeGreaterThanOrEqual(4_900);
            expect(elapsedMs).toBeLessThan(15_000);
            // On servers that end idle streams every 2 s the budget spans
            // several continuations; Spark 4.0.0 keeps the first stream open,
            // and the deadline must hold either way.
            expect(releasedExecute(events)).toBe(true);
        } finally {
            await bounded.close();
        }
        const probeStartedAt = Date.now();
        expect(await session.sql("SELECT 1 AS alive").collect()).toEqual([{ alive: 1 }]);
        expect(Date.now() - probeStartedAt).toBeLessThan(5_000);
    });
});
