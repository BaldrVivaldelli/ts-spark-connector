import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ARTIFACT_CHUNK_SIZE } from "../src/client/artifacts";
import {
    SparkSession,
    UnsupportedServerVersionError,
    arrayType,
    call,
    decimalType,
    lit,
    mapType,
    schema,
    structType,
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
let scratch: string;

beforeAll(() => {
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), "ts-spark-session-e2e-"));
});

afterAll(async () => {
    fs.rmSync(scratch, { recursive: true, force: true });
    await session.close();
});

function atLeast(version: string, major: number, minor: number): boolean {
    const [maj, min] = version.split(".").map(Number);
    return maj! > major || (maj === major && min! >= minor);
}

describe("session.range and newSession (E2E)", () => {
    it("produces the id column Spark's range does", async () => {
        expect(await session.range(3).collect()).toEqual([{ id: 0n }, { id: 1n }, { id: 2n }]);
        expect(await session.range(1, 10, 4).collect()).toEqual([{ id: 1n }, { id: 5n }, { id: 9n }]);
        expect(await session.range(5, 0, -2).collect()).toEqual([{ id: 5n }, { id: 3n }, { id: 1n }]);
        expect(await session.range(0, 8, 1, 4).count()).toBe(8n);
        expect(await session.range(2n ** 40n, 2n ** 40n + 2n).collect()).toEqual([{ id: 2n ** 40n }, { id: 2n ** 40n + 1n }]);
    });

    it("keeps temporary views local to each session", async () => {
        const child = session.newSession();
        try {
            await child.sql("CREATE OR REPLACE TEMP VIEW new_session_view AS SELECT 7 AS n").collectRaw();
            expect(await child.catalog.tableExists("new_session_view")).toBe(true);
            expect(await session.catalog.tableExists("new_session_view")).toBe(false);
            expect(await child.sql("SELECT n FROM new_session_view").collect()).toEqual([{ n: 7 }]);
        } finally {
            await child.close();
        }
        expect(await session.sql("SELECT 1 AS alive").collect()).toEqual([{ alive: 1 }]);
    });
});

describe("createDataFrame with complex columns (E2E)", () => {
    it("round-trips decimal, array, map and struct values through Spark", async () => {
        const Orders = schema({
            id: "int",
            amount: decimalType(10, 2),
            tags: arrayType("string"),
            counts: mapType("string", "long"),
            addr: structType({ city: "string", zip: "int" }),
            lines: arrayType(structType({ sku: "string", qty: "int" }), true),
        });
        const rows = [
            {
                id: 1,
                amount: "1452.00",
                tags: ["a", null, "c"],
                counts: new Map<string, bigint | null>([["x", 7n], ["y", null]]),
                addr: { city: "Rosario", zip: 2000 },
                lines: [{ sku: "k1", qty: 2 }, null],
            },
            {
                id: 2,
                amount: "-0.5",
                tags: [],
                counts: new Map(),
                addr: { city: null, zip: null },
                lines: null,
            },
        ];
        const df = session.createDataFrame(rows, Orders);
        expect(await df.collect()).toEqual([
            { ...rows[0]!, counts: new Map([["x", 7n], ["y", null]]) },
            { ...rows[1]!, amount: "-0.50" },
        ]);
        // The values are real Spark values: expressions can reach inside them.
        expect(await df.select(call("try_element_at", ["tags", lit(1)]).alias("first"), call("size", ["counts"]).alias("n")).collect())
            .toEqual([{ first: "a", n: 2 }, { first: null, n: 0 }]);
        expect(await session.createDataFrame(await df.collect(), Orders).count()).toBe(2n);
    });
});

describe("catalog metadata, tables and caching (E2E)", () => {
    const view = "session_e2e_view";
    const external = "session_e2e_people";
    const partitioned = "session_e2e_partitioned";

    afterAll(async () => {
        await session.sql(`DROP TABLE IF EXISTS ${external}`).collectRaw();
        await session.sql(`DROP TABLE IF EXISTS ${partitioned}`).collectRaw();
        await session.catalog.dropTempView(view);
    });

    it("looks up functions, databases and tables", async () => {
        const functions = await session.catalog.listFunctions(undefined, "upper").collect();
        expect(functions.map(f => f.name)).toContain("upper");
        const upper = await session.catalog.getFunction("upper");
        // Spark reports built-in functions as temporary (they live in no database).
        expect(upper).toMatchObject({ name: "upper", isTemporary: true, catalog: null });
        expect(upper.className).toMatch(/Upper$/);

        expect(await session.catalog.getDatabase("default")).toMatchObject({ name: "default" });
        await expect(session.catalog.getDatabase("session_e2e_missing")).rejects.toThrow(/session_e2e_missing/);

        await session.sql(`CREATE OR REPLACE TEMP VIEW ${view} AS SELECT 1 AS id`).collectRaw();
        expect(await session.catalog.getTable(view)).toMatchObject({ name: view, isTemporary: true });
    });

    it("creates an external table from a file and manages its cache", async () => {
        const People = schema({ id: "int", name: "string" });
        const rows = await session.catalog
            .createTable(external, {
                path: "/data/people.tsv",
                source: "csv",
                schema: People,
                options: { header: "true", delimiter: "\t" },
            })
            .collect();
        expect(rows.length).toBeGreaterThan(0);
        expect(await session.catalog.getTable(external)).toMatchObject({ name: external, tableType: "EXTERNAL" });
        expect(await session.table(external).count()).toBe(BigInt(rows.length));

        expect(await session.catalog.isCached(external)).toBe(false);
        await session.catalog.cacheTable(external, "MEMORY_ONLY");
        expect(await session.catalog.isCached(external)).toBe(true);
        await session.catalog.uncacheTable(external);
        expect(await session.catalog.isCached(external)).toBe(false);
        await session.catalog.cacheTable(external);
        await session.catalog.clearCache();
        expect(await session.catalog.isCached(external)).toBe(false);

        await session.catalog.refreshTable(external);
        await session.catalog.refreshByPath("/data");
        expect(await session.table(external).count()).toBe(BigInt(rows.length));
    });

    it("recovers partitions of a partitioned table", async () => {
        await session.sql(`CREATE TABLE ${partitioned} (id INT, day STRING) USING parquet PARTITIONED BY (day)`).collectRaw();
        await session.sql(`INSERT INTO ${partitioned} VALUES (1, 'd1'), (2, 'd2')`).collectRaw();
        await expect(session.catalog.recoverPartitions(partitioned)).resolves.toBeUndefined();
        expect(await session.table(partitioned).count()).toBe(2n);
    });
});

describe("artifacts and Java UDFs (E2E)", () => {
    const jar = path.resolve(__dirname, "fixtures/udf/ts-spark-e2e-udf.jar");

    it("uploads a jar and a file, reports their status and runs the registered UDF", async () => {
        const notes = path.join(scratch, "notes.txt");
        fs.writeFileSync(notes, "hello from the e2e suite\n");
        // A large enough file to exercise the chunked upload path.
        const big = path.join(scratch, "big.bin");
        fs.writeFileSync(big, Buffer.alloc(100_000, 5));

        expect(await session.addArtifact(jar)).toBe("jars/ts-spark-e2e-udf.jar");
        expect(await session.addArtifacts({ path: notes, kind: "file" }, { path: big, kind: "file" }))
            .toEqual(["files/notes.txt", "files/big.bin"]);

        // Blobs stored by hash are the only artifacts the server reports on.
        const hash = await session.cacheArtifact(Buffer.alloc(ARTIFACT_CHUNK_SIZE * 2 + 7, 9));
        expect(hash).toMatch(/^[0-9a-f]{64}$/);
        expect(await session.isArtifactCached(hash)).toBe(true);
        expect(await session.isArtifactCached("0".repeat(64))).toBe(false);
        expect(await session.artifactStatus(`cache/${hash}`, "jars/ts-spark-e2e-udf.jar"))
            .toEqual({ [`cache/${hash}`]: true, "jars/ts-spark-e2e-udf.jar": false });

        await session.udf.registerJavaFunction("e2e_shout", "io.tsspark.e2e.Shout", "string");
        expect(await session.sql("SELECT e2e_shout('hi') AS v, e2e_shout(NULL) AS n").collect()).toEqual([{ v: "HI!", n: null }]);
        expect(await session.range(2).select(call("e2e_shout", [call("concat", [lit("id"), "id"])]).alias("v")).collect())
            .toEqual([{ v: "ID0!" }, { v: "ID1!" }]);
        expect(await session.catalog.functionExists("e2e_shout")).toBe(true);
    });
});

describe("operation status (E2E)", () => {
    it("reports operation states on 4.2+ and refuses older servers before any RPC", async () => {
        const version = await session.version();
        if (!atLeast(version, 4, 2)) {
            const error = await session.getStatus().catch(e => e);
            expect(error).toBeInstanceOf(UnsupportedServerVersionError);
            expect(error).toMatchObject({ requiredVersion: "4.2.0", serverVersion: version });
            return;
        }

        const operationId = "5a2e0c1f-7b3d-4c8e-9f10-2a3b4c5d6e7f";
        let firstBatch!: () => void;
        const started = new Promise<void>(resolve => { firstBatch = resolve; });
        const job = (async () => {
            let batches = 0;
            // Partition 0 answers almost at once so the first batch arrives while
            // the other partitions keep the operation running for seconds more,
            // whatever the server's core count.
            for await (const response of session.sql(
                "SELECT java_method('java.lang.Thread', 'sleep', CASE WHEN id = 0 THEN 300L ELSE 4000L END) AS napped " +
                "FROM range(0, 8, 1, 8)",
            ).streamRaw({ operationId })) {
                if ((response as { arrow_batch?: unknown }).arrow_batch) {
                    batches += 1;
                    if (batches === 1) firstBatch();
                }
            }
            return batches;
        })();
        await started;

        expect(await session.getStatus([operationId])).toEqual([{ operationId, state: "RUNNING" }]);
        expect(await job).toBeGreaterThan(0);
        const after = await session.getStatus([operationId]);
        // Once the operation is released the server may stop tracking it.
        expect(after.every(status => status.operationId === operationId && status.state !== "RUNNING")).toBe(true);
        expect(Array.isArray(await session.getStatus())).toBe(true);
    });
});
