import { describe, expect, it } from "vitest";
import { SparkSession } from "../src";

describe("DataFrameReader format shortcuts", () => {
    const reader = () => SparkSession.builder().getOrCreate().read;

    it("json() reads with the json data source", () => {
        const proto = JSON.parse(reader().json("/input.json").toProtoJSON());
        expect(proto.read.data_source.format).toBe("json");
        expect(proto.read.data_source.paths).toEqual(["/input.json"]);
    });

    it("load() defaults to parquet when no format was selected", () => {
        const proto = JSON.parse(reader().load("/input.parquet").toProtoJSON());
        expect(proto.read.data_source.format).toBe("parquet");
        expect(proto.read.data_source.paths).toEqual(["/input.parquet"]);
    });

    it("load() honours an explicit format()", () => {
        const proto = JSON.parse(reader().format("orc").load("/input.orc").toProtoJSON());
        expect(proto.read.data_source.format).toBe("orc");
    });
});

describe("DataFrameReader.jdbc()", () => {
    const reader = () => SparkSession.builder().getOrCreate().read;
    const dataSource = (df: { toProtoJSON(): string }) => JSON.parse(df.toProtoJSON()).read.data_source;

    it("reads a table through the jdbc source with connection properties", () => {
        const source = dataSource(reader().option("fetchsize", 100).jdbc("jdbc:derby:memory:db", "people", {
            properties: { driver: "org.apache.derby.jdbc.EmbeddedDriver", user: "app" },
        }));
        expect(source).toEqual({
            format: "jdbc",
            paths: [],
            options: {
                fetchsize: "100",
                driver: "org.apache.derby.jdbc.EmbeddedDriver",
                user: "app",
                url: "jdbc:derby:memory:db",
                dbtable: "people",
            },
        });
        expect(dataSource(reader().jdbc("jdbc:x", "t"))).toEqual({ format: "jdbc", paths: [], options: { url: "jdbc:x", dbtable: "t" } });
    });

    it("splits reads by a partition column or by predicates", () => {
        const partitioned = dataSource(reader().jdbc("jdbc:x", "t", {
            column: "id", lowerBound: 0, upperBound: 1000n, numPartitions: 4,
        }));
        expect(partitioned.options).toMatchObject({ partitionColumn: "id", lowerBound: "0", upperBound: "1000", numPartitions: "4" });
        expect(partitioned).not.toHaveProperty("predicates");

        const predicated = dataSource(reader().jdbc("jdbc:x", "t", { predicates: ["id < 5", "id >= 5"] }));
        expect(predicated.predicates).toEqual(["id < 5", "id >= 5"]);
        expect(JSON.parse(reader().jdbc("jdbc:x", "t", { predicates: ["id < 5"] }).toSparkLogicalPlanJSON()))
            .toMatchObject({ type: "Relation", format: "jdbc", predicates: ["id < 5"] });
        expect(JSON.parse(reader().jdbc("jdbc:x", "t").toSparkLogicalPlanJSON())).not.toHaveProperty("predicates");
        expect(reader().jdbc("jdbc:x", "t", { predicates: ["id < 5", "id >= 5"] }).toClientASTJSON())
            .toContain("relation(jdbc,predicates[2])");
    });

    it("validates its arguments before building the plan", () => {
        const read = reader();
        expect(() => read.jdbc("", "t")).toThrow(TypeError);
        expect(() => read.jdbc("jdbc:x", " ")).toThrow(TypeError);
        expect(() => read.jdbc("jdbc:x", "t", null as never)).toThrow(/options must be an object/);
        expect(() => read.jdbc("jdbc:x", "t", { column: "id" })).toThrow(/requires lowerBound, upperBound and numPartitions/);
        expect(() => read.jdbc("jdbc:x", "t", { column: "id", lowerBound: 0, upperBound: 9, numPartitions: 0 })).toThrow(RangeError);
        expect(() => read.jdbc("jdbc:x", "t", { column: " ", lowerBound: 0, upperBound: 9, numPartitions: 2 })).toThrow(TypeError);
        expect(() => read.jdbc("jdbc:x", "t", { column: "id", lowerBound: 0, upperBound: 9, numPartitions: 2, predicates: ["a"] }))
            .toThrow(/cannot be combined/);
        expect(() => read.jdbc("jdbc:x", "t", { predicates: [] })).toThrow(/non-empty array/);
        expect(() => read.jdbc("jdbc:x", "t", { predicates: [" "] })).toThrow(/non-empty array/);
        expect(() => read.jdbc("jdbc:x", "t", { properties: { "": "x" } })).toThrow(TypeError);
        expect(() => read.jdbc("jdbc:x", "t", { properties: { fetchsize: Number.NaN } })).toThrow(RangeError);
    });
});
