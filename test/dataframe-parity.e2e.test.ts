import { afterAll, describe, expect, it } from "vitest";
import { SparkSession, col, lit, schema } from "../src";

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

const People = schema({ id: "long", name: "string?", age: "int?", score: "double?" });
const peopleRows = [
    { id: 1n, name: "ada", age: 30, score: 1.5 },
    { id: 2n, name: null, age: null, score: null },
    { id: 3n, name: "bob", age: null, score: 2.0 },
];
const people = () => session.createDataFrame(peopleRows, People);

const Points = schema({ k: "string", x: "double", y: "double" });
const points = () => session.createDataFrame([
    { k: "a", x: 1, y: 2 },
    { k: "a", x: 2, y: 4 },
    { k: "b", x: 3, y: 6 },
    { k: "b", x: 4, y: 8 },
], Points);

const byId = <T extends { id: unknown }>(rows: T[]): T[] =>
    [...rows].sort((a, b) => Number(a.id) - Number(b.id));

describe("na functions (E2E)", () => {
    it("fills nulls by type, by subset and by column mapping", async () => {
        expect(byId(await people().fillna(0).collect())).toEqual([
            { id: 1n, name: "ada", age: 30, score: 1.5 },
            { id: 2n, name: null, age: 0, score: 0 },
            { id: 3n, name: "bob", age: 0, score: 2 },
        ]);
        expect(byId(await people().na.fill(-1, "age").collect()).map(row => row.age)).toEqual([30, -1, -1]);
        expect(byId(await people().fillna({ name: "?", score: 9.5 }).collect())[1]).toEqual({
            id: 2n,
            name: "?",
            age: null,
            score: 9.5,
        });
    });

    it("drops rows by how, threshold and subset", async () => {
        expect((await people().dropna().collect()).map(row => row.id)).toEqual([1n]);
        expect(byId(await people().na.drop("all", undefined, ["name", "age"]).collect()).map(row => row.id))
            .toEqual([1n, 3n]);
        expect(byId(await people().dropna("any", 3).collect()).map(row => row.id)).toEqual([1n, 3n]);
    });

    it("replaces values in a subset", async () => {
        const replaced = byId(await people().replace(["ada", "bob"], ["Ada", "Bob"], "name").collect());
        expect(replaced.map(row => row.name)).toEqual(["Ada", null, "Bob"]);
        const numeric = byId(await people().na.replace(30, 31, "age").collect());
        expect(numeric.map(row => row.age)).toEqual([31, null, null]);
    });
});

describe("stat functions (E2E)", () => {
    it("computes correlation and covariance", async () => {
        expect(await points().corr("x", "y")).toBeCloseTo(1, 10);
        expect(await points().stat.cov("x", "y")).toBeCloseTo(10 / 3, 10);
    });

    it("computes exact quantiles with relative error 0", async () => {
        expect(await points().approxQuantile("x", [0, 0.5, 1], 0)).toEqual([1, 2, 4]);
        expect(await points().stat.approxQuantile(["x", "y"], [1], 0)).toEqual([[4], [8]]);
    });

    it("builds crosstab and frequent-item tables", async () => {
        const table = await points().crosstab("k", "x").collect() as Array<Record<string, unknown>>;
        const a = table.find(row => row.k_x === "a");
        expect(a).toMatchObject({ "1.0": 1n, "2.0": 1n, "3.0": 0n, "4.0": 0n });
        const items = await points().stat.freqItems(["k"], 0.4).collect() as Array<{ k_freqItems: string[] }>;
        expect(items[0]!.k_freqItems).toEqual(expect.arrayContaining(["a", "b"]));
    });

    it("samples whole strata deterministically", async () => {
        const sampled = await points().sampleBy("k", { a: 1, b: 0 }, 7).collect();
        expect(sampled.map(row => row.k)).toEqual(["a", "a"]);
    });
});

describe("reshaping relations (E2E)", () => {
    it("unpivots and melts value columns", async () => {
        const unpivoted = await points()
            .filter(col("k").eq("a"))
            .unpivot("k", ["x", "y"], "axis", "value")
            .collect() as Array<{ k: string; axis: string; value: number }>;
        expect(unpivoted).toEqual([
            { k: "a", axis: "x", value: 1 },
            { k: "a", axis: "y", value: 2 },
            { k: "a", axis: "x", value: 2 },
            { k: "a", axis: "y", value: 4 },
        ]);
        expect(await points().melt("k", undefined, "axis", "value").count()).toBe(8n);
    });

    it("self-joins through aliases", async () => {
        const Staff = schema({ id: "long", manager_id: "long?" });
        const staff = session.createDataFrame([
            { id: 1n, manager_id: null },
            { id: 2n, manager_id: 1n },
            { id: 3n, manager_id: 1n },
        ], Staff);
        const pairs = await staff.alias("e")
            .join(staff.alias("m"), col("e.manager_id").eq(col("m.id")))
            .select(col("e.id").alias("employee"), col("m.id").alias("manager"))
            .orderBy("employee")
            .collect();
        expect(pairs).toEqual([{ employee: 2n, manager: 1n }, { employee: 3n, manager: 1n }]);
    });

    it("renames positionally, adds columns, offsets and transposes", async () => {
        expect(await people().toDF("a", "b", "c", "d").columns()).toEqual(["a", "b", "c", "d"]);
        const added = await people()
            .withColumns({ two: lit(2), flag: lit(true) })
            .select("id", "two", "flag")
            .orderBy("id")
            .limit(1)
            .collect();
        expect(added).toEqual([{ id: 1n, two: 2, flag: true }]);
        const offset = await people().orderBy("id").offset(1).collect();
        expect(offset.map(row => row.id)).toEqual([2n, 3n]);

        const Wide = schema({ key: "string", a: "int", b: "int" });
        const transposed = await session
            .createDataFrame([{ key: "x", a: 1, b: 2 }, { key: "y", a: 3, b: 4 }], Wide)
            .transpose("key")
            .collect() as Array<Record<string, unknown>>;
        expect(transposed).toEqual([{ key: "a", x: 1, y: 3 }, { key: "b", x: 2, y: 4 }]);
    });

    it("cross-joins and repartitions without changing the rows", async () => {
        expect(await people().crossJoin(points()).count()).toBe(12n);
        const hashed = await people().repartition(3, "id").collect();
        expect(byId(hashed)).toEqual(byId(await people().collect()));
        const ranged = await people().repartitionByRange(2, col("id").desc()).collect();
        expect(byId(ranged)).toEqual(byId(await people().collect()));
    });
});

describe("actions and introspection (E2E)", () => {
    it("counts, takes, heads, tails and checks emptiness", async () => {
        expect(await people().count()).toBe(3n);
        expect(await people().orderBy("id").first()).toEqual(peopleRows[0]);
        expect(await people().orderBy("id").head(2)).toEqual(peopleRows.slice(0, 2));
        expect(await people().orderBy("id").take(1)).toEqual([peopleRows[0]]);
        expect(await people().orderBy("id").tail(2)).toEqual(peopleRows.slice(1));
        expect(await people().isEmpty()).toBe(false);
        expect(await people().filter(col("id").gt(10)).isEmpty()).toBe(true);
        expect(await people().filter(col("id").gt(10)).first()).toBeUndefined();
    });

    it("reports the resolved schema", async () => {
        expect(await people().schema()).toEqual({
            fields: [
                { name: "id", dataType: "bigint", nullable: false },
                { name: "name", dataType: "string", nullable: true },
                { name: "age", dataType: "int", nullable: true },
                { name: "score", dataType: "double", nullable: true },
            ],
        });
        expect(await people().select("id", "name").columns()).toEqual(["id", "name"]);
        expect(await people().dtypes()).toEqual([["id", "bigint"], ["name", "string"], ["age", "int"], ["score", "double"]]);
        const tree = await people().treeString();
        expect(tree).toContain("root");
        expect(tree).toContain("|-- id: long (nullable = false)");
    });

    it("answers the plan introspection requests", async () => {
        expect(await people().isLocal()).toBe(true);
        expect(await people().isStreaming()).toBe(false);
        expect(await session.readStream("rate").isStreaming()).toBe(true);
        const files = await session.read.option("header", "true").csv("/data/people.tsv").inputFiles();
        expect(files.some(file => file.endsWith("people.tsv"))).toBe(true);
        const filtered = people().filter(col("id").gt(1));
        expect(await filtered.sameSemantics(people().filter(col("id").gt(1)))).toBe(true);
        expect(await filtered.sameSemantics(people())).toBe(false);
        expect(await filtered.semanticHash()).toBe(await people().filter(col("id").gt(1)).semanticHash());
    });
});
