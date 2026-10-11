import { afterAll, describe, expect, it } from "vitest";
import { SparkSession, col, schema } from "../src";

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

const Events = schema({
    id: "long",
    score: "double",
    ratio: "float?",
    count: "int",
    name: "string",
    active: "boolean?",
    day: "date",
    at: "timestamp?",
});

const rows = [
    {
        id: 1n,
        score: 0.5,
        ratio: 0.25,
        count: 7,
        name: "ada",
        active: true,
        day: "2026-01-31",
        at: "2026-01-31T12:30:45.123Z",
    },
    {
        id: 9007199254740993n, // beyond Number.MAX_SAFE_INTEGER: must survive as bigint
        score: -1.5,
        ratio: null,
        count: -3,
        name: "grace",
        active: null,
        day: "1969-12-31",
        at: null,
    },
];

describe("createDataFrame", () => {
    it("round-trips every primitive token through Spark", async () => {
        const collected = await session.createDataFrame(rows, Events).collect();
        expect(collected).toEqual([
            { ...rows[0], ratio: 0.25 },
            rows[1],
        ]);
    });

    it("composes with transformations like any other relation", async () => {
        const names = await session
            .createDataFrame(rows, Events)
            .filter(col("count").gt(0))
            .select("name")
            .collect();
        expect(names).toEqual([{ name: "ada" }]);
    });

    it("joins local data against SQL relations", async () => {
        const People = schema({ person_id: "long", country: "string" });
        const people = session.createDataFrame(
            [
                { person_id: 1n, country: "ar" },
                { person_id: 2n, country: "uy" },
            ],
            People,
        );
        const joined = await session
            .sql("SELECT CAST(1 AS BIGINT) AS id, 'ada' AS name")
            .join(people, col("id").eq(col("person_id")), "inner")
            .select("name", "country")
            .collect();
        expect(joined).toEqual([{ name: "ada", country: "ar" }]);
    });

    it("creates an empty, correctly-typed relation from zero rows", async () => {
        const empty = session.createDataFrame([], Events);
        expect(await empty.collect()).toEqual([]);
        expect(await empty.select("name").filter(col("count").gt(0)).collect()).toEqual([]);
    });

    it("registers local data as a temp view visible to the catalog", async () => {
        await session.createDataFrame(rows, Events).write().createOrReplaceTempView("local_events");
        expect(await session.catalog.tableExists("local_events")).toBe(true);
        const total = await session.sql("SELECT COUNT(*) AS n FROM local_events").collect();
        expect(total[0]!.n).toBe(2n);
        await session.catalog.dropTempView("local_events");
    });
});
