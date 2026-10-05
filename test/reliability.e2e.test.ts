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
 * 192 single-row tasks that each sleep 1.5 s. The job needs at least 1.5 s of
 * wall time on any machine and about 1.5 s * ceil(192 / cores) otherwise, so it
 * is comfortably longer than the deadline/abort below while staying bounded if
 * server-side cleanup ever regressed. java_method is a Spark builtin, so no
 * extra jars are involved.
 */
const SLOW_JOB_SQL =
    "SELECT COUNT(napped) AS n FROM (" +
    "SELECT java_method('java.lang.Thread', 'sleep', 1500L) AS napped FROM range(0, 192, 1, 192))";
const SLOW_JOB_MIN_MS = 1_500;

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

        const startedAt = Date.now();
        const timer = setTimeout(() => controller.abort(reason), 300);
        const error = await captureRejection(abortable.sql(SLOW_JOB_SQL).collect()) as Error;
        clearTimeout(timer);
        const elapsedMs = Date.now() - startedAt;

        // The rejection is the abort itself, not a transport failure.
        expect(error).toBeInstanceOf(Error);
        expect(error).not.toBeInstanceOf(SparkConnectError);
        expect(error.name).toBe("AbortError");
        expect(error.message).toBe(reason);
        expect(elapsedMs).toBeLessThan(SLOW_JOB_MIN_MS);

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
            const startedAt = Date.now();
            const error = await captureRejection(deadlineSession.sql(SLOW_JOB_SQL).collect()) as SparkConnectError;
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
            expect(elapsedMs).toBeLessThan(SLOW_JOB_MIN_MS * 2);

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
