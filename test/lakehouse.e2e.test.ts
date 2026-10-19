import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SparkSession, bucket, col, schema, years } from "../src";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";
const destRoot = process.env.SPARK_TEST_DEST ?? "/data/dest/e2e";

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

const Events = schema({
    id: "long",
    name: "string",
    amount: "double",
});

const rows = [
    { id: 1n, name: "ada", amount: 10.5 },
    { id: 2n, name: "grace", amount: 20.0 },
];

const appended = [{ id: 3n, name: "ida", amount: 30.25 }];

function byId<T extends { id: bigint }>(collected: T[]): T[] {
    return [...collected].sort((a, b) => (a.id < b.id ? -1 : 1));
}

// Iceberg publishes no Spark 4.2 runtime yet, so its cases run only where the
// bundled image carries the jar. The Delta cases run on every matrix version.
let icebergExpected = false;

beforeAll(async () => {
    const version = await session.sql("SELECT version() AS v").collect();
    const sparkVersion = String(version[0]!.v);
    icebergExpected = !sparkVersion.startsWith("4.2.");
});

describe("delta", () => {
    const path = `${destRoot}/lakehouse/delta_events`;

    it("round-trips a Delta table written from local rows", async () => {
        await session
            .createDataFrame(rows, Events)
            .write()
            .format("delta")
            .mode("overwrite")
            .save(path);

        const back = await session.read.format("delta").load(path).as<typeof rows[number]>().collect();
        expect(byId(back)).toEqual(rows);
    });

    it("appends and time-travels across Delta versions", async () => {
        await session
            .createDataFrame(appended, Events)
            .write()
            .format("delta")
            .mode("append")
            .save(path);

        const current = await session.read.format("delta").load(path).collect();
        expect(current).toHaveLength(3);

        const firstVersion = await session.read
            .format("delta")
            .option("versionAsOf", 0)
            .load(path)
            .collect();
        expect(firstVersion).toHaveLength(2);
    });

    it("filters pushed to the Delta source return the right rows", async () => {
        const filtered = await session.read
            .format("delta")
            .load(path)
            .filter(col("amount").gt(15))
            .select("name")
            .collect();
        expect(filtered.map(row => row.name).sort((a, b) => String(a).localeCompare(String(b)))).toEqual([
            "grace",
            "ida",
        ]);
    });
});

describe("iceberg", () => {
    const table = "iceberg.e2e.lakehouse_events";

    it("round-trips an Iceberg table through its Hadoop catalog", async ctx => {
        if (!icebergExpected) return ctx.skip();

        await session.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.e2e").collectRaw();
        await session.sql(`DROP TABLE IF EXISTS ${table}`).collectRaw();
        await session.createDataFrame(rows, Events).write().format("iceberg").saveAsTable(table);

        const back = await session.table(table).as<typeof rows[number]>().collect();
        expect(byId(back)).toEqual(rows);
    });

    it("exposes Iceberg metadata tables", async ctx => {
        if (!icebergExpected) return ctx.skip();

        const snapshots = await session.sql(`SELECT COUNT(*) AS n FROM ${table}.snapshots`).collect();
        expect(Number(snapshots[0]!.n)).toBeGreaterThan(0);
    });

    it("creates a table partitioned by transforms through writeTo", async ctx => {
        if (!icebergExpected) return ctx.skip();

        const partitioned = "iceberg.e2e.lakehouse_partitioned";
        const Visits = schema({ id: "long", at: "timestamp" });
        const visits = [
            { id: 1n, at: "2024-01-02T03:04:05.000Z" },
            { id: 2n, at: "2025-06-07T08:09:10.000Z" },
        ];
        await session.sql(`DROP TABLE IF EXISTS ${partitioned}`).collectRaw();
        await session
            .createDataFrame(visits, Visits)
            .writeTo(partitioned)
            .using("iceberg")
            .partitionedBy(years(col("at")), bucket(4, col("id")))
            .create();

        const back = await session.table(partitioned).as<typeof visits[number]>().collect();
        expect(byId(back)).toEqual(visits);
        const [created] = await session.sql(`SHOW CREATE TABLE ${partitioned}`).collect();
        expect(JSON.stringify(created)).toMatch(/PARTITIONED BY \(years\(at\), bucket\(4, id\)\)/);
    });

    it("appends through the catalog identifier", async ctx => {
        if (!icebergExpected) return ctx.skip();

        await session
            .createDataFrame(appended, Events)
            .write()
            .format("iceberg")
            .mode("append")
            .saveAsTable(table);
        const count = await session.sql(`SELECT COUNT(*) AS n FROM ${table}`).collect();
        expect(count[0]!.n).toBe(3n);
    });
});
