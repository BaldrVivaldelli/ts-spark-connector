import { afterAll, describe, expect, it } from "vitest";
import { SparkSession, UnsupportedServerVersionError, col, schema } from "../src";
import type {
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryHandle,
} from "../src";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";

function connectedBuilder() {
    const builder = SparkSession.builder().config("spark.connect.url", connectUrl);
    if (connectUrl.startsWith("scs://")) {
        builder.enableTLS({ trustStorePath: caPath, serverNameOverride: serverName });
    }
    return builder;
}

const session = connectedBuilder().getOrCreate();
afterAll(async () => session.close());

const People = schema({ id: "int", name: "string" });
const people = (rows: Array<{ id: number; name: string }>) => session.createDataFrame(rows, People);
const byId = <T extends { id: number }>(rows: T[]) => [...rows].sort((a, b) => a.id - b.id);

async function waitFor<T>(probe: () => Promise<T | undefined>, timeoutMs: number, label: string): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        const value = await probe();
        if (value !== undefined) return value;
        if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
        await new Promise(resolve => setTimeout(resolve, 250));
    }
}

async function atLeast(version: string, major: number, minor: number): Promise<boolean> {
    const [maj, min] = version.split(".").map(Number);
    return maj! > major || (maj === major && min! >= minor);
}

function rateQuery(name: string) {
    return session
        .readStream("rate", { rowsPerSecond: "5", numPartitions: "1" })
        .select("value")
        .writeStream()
        .format("memory")
        .outputMode("append")
        .trigger({ processingTime: "500 milliseconds" })
        .queryName(name);
}

async function stopAndAwait(handle: StreamingQueryHandle): Promise<void> {
    await handle.stop();
    expect(await handle.awaitTermination(30_000)).toBe(true);
}

describe("batch writers: insertInto and JDBC (E2E)", () => {
    it("inserts rows into an existing table by position and overwrites on request", async () => {
        const table = `insert_into_${Date.now()}`;
        await people([{ id: 1, name: "ada" }]).write().saveAsTable(table);
        await people([{ id: 2, name: "grace" }, { id: 3, name: "linus" }]).write().insertInto(table);
        expect(byId(await session.table(table).collect() as Array<{ id: number; name: string }>)).toEqual([
            { id: 1, name: "ada" }, { id: 2, name: "grace" }, { id: 3, name: "linus" },
        ]);

        await people([{ id: 9, name: "zed" }]).write().insertInto(table, true);
        expect(await session.table(table).collect()).toEqual([{ id: 9, name: "zed" }]);

        await people([{ id: 10, name: "ten" }]).write().mode("append").insertInto(table);
        expect(byId(await session.table(table).collect() as Array<{ id: number; name: string }>)).toEqual([
            { id: 9, name: "zed" }, { id: 10, name: "ten" },
        ]);
    });

    it("round-trips rows through an embedded Derby database over JDBC", async () => {
        // Derby ships with the Spark distribution (it backs the Hive metastore).
        const url = `jdbc:derby:memory:e2e_${Date.now()};create=true`;
        const driver = "org.apache.derby.jdbc.EmbeddedDriver";
        const rows = [{ id: 1, name: "ada" }, { id: 2, name: "grace" }, { id: 3, name: "linus" }];

        await people(rows).write().jdbc(url, "people", "overwrite", { driver });
        await people([{ id: 4, name: "ken" }]).write().jdbc(url, "people", "append", { driver });
        const expected = [...rows, { id: 4, name: "ken" }];

        const plain = await session.read.jdbc(url, "people", { properties: { driver } }).collect();
        expect(byId(plain as Array<{ id: number; name: string }>)).toEqual(expected);

        const predicated = await session.read
            .jdbc(url, "people", { predicates: ["\"id\" < 3", "\"id\" >= 3"], properties: { driver } })
            .collect();
        expect(byId(predicated as Array<{ id: number; name: string }>)).toEqual(expected);

        const partitioned = session.read.jdbc(url, "people", {
            column: "id", lowerBound: 1, upperBound: 4, numPartitions: 2, properties: { driver },
        });
        expect(byId(await partitioned.collect() as Array<{ id: number; name: string }>)).toEqual(expected);
        expect(await partitioned.filter(col("id").gt(2)).count()).toBe(2n);

        const query = await session.read
            .jdbc(url, "(SELECT \"id\", \"name\" FROM people WHERE \"id\" = 2) AS two", { properties: { driver } })
            .collect();
        expect(query).toEqual([{ id: 2, name: "grace" }]);
    });

    it("gates writeTo().withSchemaEvolution() on Spark 4.2 and evolves a Delta table from there", async () => {
        const table = `schema_evolution_${Date.now()}`;
        const version = await session.version();
        await people([{ id: 1, name: "ada" }]).writeTo(table).using("delta").create();
        const evolving = session
            .createDataFrame([{ id: 2, name: "grace", extra: "x" }], schema({ id: "int", name: "string", extra: "string" }))
            .writeTo(table)
            .withSchemaEvolution();

        // 4.1 accepts the field but writes without evolving the schema
        // (DELTA_METADATA_MISMATCH), so 4.0 and 4.1 must be refused up front.
        if (!(await atLeast(version, 4, 2))) {
            const error = await evolving.append().catch(e => e);
            expect(error).toBeInstanceOf(UnsupportedServerVersionError);
            expect(error).toMatchObject({ requiredVersion: "4.2.0", serverVersion: version });
            return;
        }

        await evolving.append();
        expect(await session.table(table).columns()).toEqual(["id", "name", "extra"]);
        expect(byId(await session.table(table).collect() as Array<{ id: number; name: string; extra: string | null }>)).toEqual([
            { id: 1, name: "ada", extra: null },
            { id: 2, name: "grace", extra: "x" },
        ]);
    });
});

describe("streaming query control (E2E)", () => {
    it("introspects a running query and finds it through the manager", async () => {
        const name = `rate_control_${Date.now()}`;
        const handle = await rateQuery(name).start();
        try {
            expect(handle.name).toBe(name);
            expect(handle.id).toMatch(/^[0-9a-f-]{36}$/);
            expect(handle.runId).toMatch(/^[0-9a-f-]{36}$/);
            expect(await handle.isActive()).toBe(true);
            expect(await handle.status()).toMatchObject({ isActive: true, message: expect.any(String) });
            await handle.processAllAvailable();

            const progress = await waitFor(() => handle.lastProgress(), 30_000, "the first progress report");
            expect(progress).toMatchObject({ id: handle.id, runId: handle.runId, name, batchId: expect.any(Number) });
            expect(typeof progress.timestamp).toBe("string");
            const recent = await handle.recentProgress();
            expect(recent.length).toBeGreaterThan(0);
            expect(recent[recent.length - 1]!.batchId).toBeGreaterThanOrEqual(recent[0]!.batchId);
            expect(await handle.explain()).toContain("Physical Plan");
            expect(await handle.explain(true)).toContain("Logical Plan");
            expect(await handle.exception()).toBeUndefined();

            const active = await session.streams.active();
            expect(active.map(query => query.id)).toContain(handle.id);
            const found = await session.streams.get(handle.id);
            expect(found).toMatchObject({ id: handle.id, runId: handle.runId, name });
            expect(await found!.isActive()).toBe(true);
            expect(await session.streams.get("00000000-0000-0000-0000-000000000000")).toBeUndefined();
            expect(await session.streams.awaitAnyTermination(500)).toBe(false);
        } finally {
            await stopAndAwait(handle);
        }

        expect(await handle.isActive()).toBe(false);
        expect(await session.streams.awaitAnyTermination(5_000)).toBe(true);
        await session.streams.resetTerminated();
        expect(await session.streams.awaitAnyTermination(500)).toBe(false);
        expect((await session.streams.active()).map(query => query.id)).not.toContain(handle.id);
    });

    it("delivers started, progress and terminated events to listeners", async () => {
        const started: QueryStartedEvent[] = [];
        const progressed: QueryProgressEvent[] = [];
        const idled: QueryIdleEvent[] = [];
        const terminated: QueryTerminatedEvent[] = [];
        const listener = {
            onQueryStarted: (event: QueryStartedEvent) => { started.push(event); },
            onQueryProgress: (event: QueryProgressEvent) => { progressed.push(event); },
            onQueryIdle: (event: QueryIdleEvent) => { idled.push(event); },
            onQueryTerminated: (event: QueryTerminatedEvent) => { terminated.push(event); },
        };
        await session.streams.addListener(listener);
        expect(session.streams.getListeners()).toEqual([listener]);

        const name = `rate_listener_${Date.now()}`;
        const handle = await rateQuery(name).start();
        try {
            expect(started.map(event => event.id)).toEqual([handle.id]);
            expect(started[0]).toMatchObject({ runId: handle.runId, name, timestamp: expect.any(String) });
            await waitFor(() => Promise.resolve(progressed.length > 0 ? true : undefined), 30_000, "a progress event");
            expect(progressed[0]!.progress).toMatchObject({ id: handle.id, runId: handle.runId, name });
        } finally {
            await stopAndAwait(handle);
        }
        await waitFor(() => Promise.resolve(terminated.length > 0 ? true : undefined), 30_000, "the terminated event");
        expect(terminated[0]).toMatchObject({ id: handle.id, runId: handle.runId });
        expect(terminated[0]!.exception ?? null).toBeNull();

        await session.streams.removeListener(listener);
        expect(session.streams.getListeners()).toEqual([]);

        // Without a listener bus Spark attaches no start event and nothing is delivered.
        const quiet = await rateQuery(`rate_quiet_${Date.now()}`).start();
        try {
            expect(started).toHaveLength(1);
        } finally {
            await stopAndAwait(quiet);
        }
        expect(terminated).toHaveLength(1);
        expect(idled.every(event => event.id === handle.id)).toBe(true);
    });
});
