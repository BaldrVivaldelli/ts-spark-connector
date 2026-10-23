import { afterAll, describe, expect, it } from "vitest";
import { Observation, SparkSession, col, lit, schema, functions as F } from "../src";
import { supportedOrRefused } from "./fixtures/server-version";

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

const Sales = schema({ k: "string", name: "string", v: "int" });
const sales = () => session.createDataFrame([
    { k: "a", name: "x", v: 1 },
    { k: "a", name: "y", v: 2 },
    { k: "b", name: "x", v: 3 },
], Sales);

const Trades = schema({ t: "int", side: "string" });
const trades = () => session.createDataFrame([
    { t: 1, side: "L1" },
    { t: 5, side: "L5" },
    { t: 10, side: "L10" },
], Trades);

const QuoteSchema = schema({ rt: "int", q: "string" });
const quotes = () => session.createDataFrame([
    { rt: 2, q: "q2" },
    { rt: 5, q: "q5" },
    { rt: 6, q: "q6" },
], QuoteSchema);

const sorted = <T extends Record<string, unknown>>(rows: T[], ...keys: string[]): T[] =>
    [...rows].sort((a, b) => {
        for (const key of keys) {
            const left = String(a[key] ?? "");
            const right = String(b[key] ?? "");
            if (left !== right) return left < right ? -1 : 1;
        }
        return 0;
    });

describe("grouping variants (E2E)", () => {
    it("rolls up, cubes and groups by grouping sets", async () => {
        const rolled = await sales().rollup("k").agg({ total: "sum(v)" }).collect();
        expect(sorted(rolled, "k")).toEqual([
            { k: null, total: 6n },
            { k: "a", total: 3n },
            { k: "b", total: 3n },
        ]);

        const cubed = await sales().cube("k", "name").agg({ total: "sum(v)" }).collect();
        expect(cubed).toHaveLength(8);
        expect(cubed.find(row => row.k === null && row.name === null)).toEqual({ k: null, name: null, total: 6n });
        expect(cubed.find(row => row.k === null && row.name === "x")).toEqual({ k: null, name: "x", total: 4n });

        const sets = () => sales().groupingSets([["k"], []], "k").agg({ total: "sum(v)" }).collect();
        if (!await supportedOrRefused(session, "4.0.0", "groupingSets()", sets)) return;
        expect(sorted(await sets(), "k")).toEqual([
            { k: null, total: 6n },
            { k: "a", total: 3n },
            { k: "b", total: 3n },
        ]);
    });

    it("pivots on collected or explicit values", async () => {
        const collected = await sales().groupBy("k").pivot("name").agg({ total: "sum(v)" }).collect();
        expect(sorted(collected, "k")).toEqual([
            { k: "a", x: 1n, y: 2n },
            { k: "b", x: 3n, y: null },
        ]);
        const explicit = await sales().groupBy("k").pivot("name", ["x"]).agg({ total: "sum(v)" }).collect();
        expect(sorted(explicit, "k")).toEqual([{ k: "a", x: 1n }, { k: "b", x: 3n }]);
    });
});

describe("partition-local sorting and deduplication (E2E)", () => {
    it("sorts each partition on its own, not the whole result", async () => {
        // 1..30 in a scrambled order, spread over three partitions.
        const Values = schema({ v: "int" });
        const scrambled = session.createDataFrame(
            [...Array(30).keys()].map(index => ({ v: ((index * 7) % 30) + 1 })),
            Values,
        ).repartition(3);

        const rows = await scrambled
            .sortWithinPartitions("v")
            .select(col("v"), F.spark_partition_id().alias("p"))
            .collect() as Array<{ v: number; p: number }>;
        const partitions = new Map<number, number[]>();
        for (const row of rows) partitions.set(row.p, [...(partitions.get(row.p) ?? []), row.v]);
        expect(partitions.size).toBeGreaterThan(1);
        for (const values of partitions.values()) {
            expect(values).toEqual([...values].sort((left, right) => left - right));
        }
        const flat = rows.map(row => row.v);
        expect(flat).not.toEqual([...flat].sort((left, right) => left - right));
        // The physical plan carries the partition-local flag Spark planned with.
        expect(await scrambled.sortWithinPartitions("v").explain()).toMatch(/Sort \[v#\d+ ASC NULLS FIRST\], false, 0/);
    });

    it("drops duplicates within the watermark of a stream and refuses batch inputs", async () => {
        // Spark only plans DeduplicateWithinWatermark for streams.
        await expect(sales().union(sales()).dropDuplicatesWithinWatermark().count())
            .rejects.toThrow(/not supported with batch/);

        // Output is immediate; the watermark only bounds how long each key's
        // state is kept. Some Spark lines need a few batches before the first
        // rows reach the memory sink, so the sink is polled with a generous budget.
        const queryName = `parity2_dedup_${Date.now()}`;
        const handle = await session.readStream("rate", { rowsPerSecond: "5", numPartitions: "1" })
            .withWatermark(col("timestamp"), "1 second")
            .dropDuplicatesWithinWatermark("value")
            .writeStream()
            .format("memory")
            .outputMode("append")
            .trigger({ processingTime: "500 milliseconds" })
            .queryName(queryName)
            .start();
        let rows: Array<{ value: bigint }> = [];
        const deadline = Date.now() + 30_000;
        while (rows.length === 0 && Date.now() < deadline) {
            expect(await handle.awaitTermination(1_000)).toBe(false);
            rows = await session.table(queryName).collect() as Array<{ value: bigint }>;
        }
        await handle.stop();
        expect(await handle.awaitTermination(30_000)).toBe(true);

        expect(rows.length).toBeGreaterThan(0);
        // Every rate value is distinct, so deduplication keeps each one exactly once.
        const values = rows.map(row => row.value).sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
        expect(values).toEqual(values.map((_value, index) => BigInt(index)));
    }, 120_000);
});

describe("column references (E2E)", () => {
    it("selects columns by regex and reads file metadata columns", async () => {
        const df = sales();
        const regex = await df.select(df.colRegex("`(k|v)`")).collect();
        expect(Object.keys(regex[0]!)).toEqual(["k", "v"]);

        const path = `/tmp/parity2_metadata_${Date.now()}`;
        await sales().write().parquet().mode("overwrite").save(path);
        const files = session.read.parquet(path);
        const [row] = await files.select(col("k"), files.metadataColumn("_metadata")).limit(1).collect() as Array<{
            _metadata: { file_path: string; file_name: string };
        }>;
        expect(row!._metadata.file_name).toMatch(/\.parquet$/);
        expect(row!._metadata.file_path).toContain(path);
    });
});

describe("lateral and as-of joins (E2E)", () => {
    const Left = schema({ k: "int" });
    const Right = schema({ v: "int" });
    const left = () => session.createDataFrame([{ k: 1 }, { k: 2 }], Left);
    const right = () => session.createDataFrame([{ v: 1 }, { v: 2 }, { v: 3 }], Right);

    it("lateral-joins with and without a condition", async () => {
        if (!await supportedOrRefused(session, "4.0.0", "lateralJoin()", () => left().lateralJoin(right()).collect())) return;
        const inner = await left().lateralJoin(right(), col("v").gt(col("k"))).collect();
        expect(sorted(inner, "k", "v")).toEqual([{ k: 1, v: 2 }, { k: 1, v: 3 }, { k: 2, v: 3 }]);
        expect(await left().lateralJoin(right()).count()).toBe(6n);
        const outer = await left().lateralJoin(right(), col("v").gt(lit(5)), "left").collect();
        expect(sorted(outer, "k")).toEqual([{ k: 1, v: null }, { k: 2, v: null }]);
    });

    it("matches each trade with the closest quote in the requested direction", async () => {
        if (!await supportedOrRefused(session, "4.0.0", "asOfJoin()", () => trades().asOfJoin(quotes(), "t", "rt").collect())) return;
        const backward = await trades().asOfJoin(quotes(), "t", "rt").collect();
        expect(sorted(backward, "t")).toEqual([
            { t: 10, side: "L10", rt: 6, q: "q6" },
            { t: 5, side: "L5", rt: 5, q: "q5" },
        ]);

        const strictLeft = await trades().asOfJoin(quotes(), "t", "rt", undefined, "left", { allowExactMatches: false }).collect();
        expect(sorted(strictLeft, "t").map(row => [row.t, row.q])).toEqual([[1, null], [10, "q6"], [5, "q2"]]);

        const forward = await trades().asOfJoin(quotes(), "t", "rt", undefined, "inner", { direction: "forward" }).collect();
        expect(sorted(forward, "t").map(row => [row.t, row.q])).toEqual([[1, "q2"], [5, "q5"]]);

        const nearest = await trades().asOfJoin(quotes(), "t", "rt", undefined, "left", {
            direction: "nearest",
            tolerance: 1,
        }).collect();
        expect(sorted(nearest, "t").map(row => [row.t, row.q])).toEqual([[1, "q2"], [10, null], [5, "q5"]]);
    });
});

describe("observe (E2E)", () => {
    it("collects named metrics during the action", async () => {
        const stats = new Observation("sales_stats");
        const rows = await sales()
            .observe(stats, F.count(F.lit(1)).alias("rows"), F.sum("v").alias("total"), F.max("name").alias("last"))
            .collect();
        expect(rows).toHaveLength(3);
        await expect(stats.get()).resolves.toEqual({ rows: 3n, total: 6n, last: "y" });
    });
});

describe("checkpoints and storage levels (E2E)", () => {
    it("checkpoints locally and reliably, keeping the rows and the schema", async () => {
        if (!await supportedOrRefused(session, "4.0.0", "checkpoint() and localCheckpoint()", () => sales().localCheckpoint())) return;
        const local = await sales().localCheckpoint();
        expect(sorted(await local.collect(), "k", "name")).toEqual(sorted(await sales().collect(), "k", "name"));
        expect(await local.filter(col("v").gt(1)).count()).toBe(2n);

        const reliable = await sales().checkpoint();
        expect(await reliable.count()).toBe(3n);
        const lazy = await sales().localCheckpoint(false, "MEMORY_ONLY");
        expect(await lazy.count()).toBe(3n);

        // Explicit release is idempotent and the data is gone afterwards.
        await local.releaseCheckpoint();
        await local.releaseCheckpoint();
        await expect(local.count()).rejects.toThrow();
        expect(await reliable.count()).toBe(3n);
    });

    it("reports the storage level after persist", async () => {
        expect((await sales().storageLevel()).name).toBe("NONE");
        const persisted = sales().persist("DISK_ONLY");
        try {
            expect(await persisted.count()).toBe(3n);
            expect(await persisted.storageLevel()).toMatchObject({ useDisk: true, useMemory: false, name: "DISK_ONLY" });
        } finally {
            await persisted.unpersist().count();
        }
    });
});

describe("schema reconciliation, metadata, JSON and global views (E2E)", () => {
    it("reconciles rows to a declared schema or a DDL string", async () => {
        const typed = await sales().to(schema({ name: "string?", k: "string", v: "long" })).collect();
        // File sources report every column nullable; the declaration still
        // types and verifies the rows.
        const path = `/tmp/parity2_to_${Date.now()}`;
        await sales().write().parquet().mode("overwrite").save(path);
        const fromParquet = await session.read.parquet(path).to(schema({ v: "long", k: "string", name: "string" })).collect();
        expect(sorted(fromParquet, "v")).toEqual([
            { v: 1n, k: "a", name: "x" },
            { v: 2n, k: "a", name: "y" },
            { v: 3n, k: "b", name: "x" },
        ]);
        expect(sorted(typed, "k", "name")).toEqual([
            { name: "x", k: "a", v: 1n },
            { name: "y", k: "a", v: 2n },
            { name: "x", k: "b", v: 3n },
        ]);
        const ddl = await sales().to("v STRING, k STRING").collect();
        expect(sorted(ddl as Array<{ v: string; k: string }>, "v")).toEqual([
            { v: "1", k: "a" },
            { v: "2", k: "a" },
            { v: "3", k: "b" },
        ]);
    });

    it("keeps values with column metadata and renders rows as JSON", async () => {
        const withMeta = sales().withMetadata("k", { comment: "key" });
        expect(await withMeta.count()).toBe(3n);
        const { fields } = await withMeta.schema();
        expect(fields.map(field => field.name)).toEqual(["k", "name", "v"]);
        expect(JSON.parse(fields[0]!.metadata!)).toEqual({ comment: "key" });
        expect(fields[1]!.metadata).toBeUndefined();

        const json = await sales().orderBy("v").toJSON().collect();
        expect(json.map(row => JSON.parse(row.value))).toEqual([
            { k: "a", name: "x", v: 1 },
            { k: "a", name: "y", v: 2 },
            { k: "b", name: "x", v: 3 },
        ]);
    });

    it("registers global temp views visible from other sessions", async () => {
        const view = `parity2_global_${Date.now()}`;
        const other = connectedBuilder().getOrCreate();
        try {
            await sales().createOrReplaceGlobalTempView(view);
            expect(await session.sql(`SELECT count(*) AS n FROM global_temp.${view}`).collect()).toEqual([{ n: 3n }]);
            expect(await other.sql(`SELECT count(*) AS n FROM global_temp.${view}`).collect()).toEqual([{ n: 3n }]);
            await expect(sales().createGlobalTempView(view)).rejects.toThrow(/already exists/i);
            await sales().limit(1).createOrReplaceGlobalTempView(view);
            expect(await other.sql(`SELECT count(*) AS n FROM global_temp.${view}`).collect()).toEqual([{ n: 1n }]);
        } finally {
            await session.catalog.dropGlobalTempView(view);
            await other.close();
        }
    });
});
