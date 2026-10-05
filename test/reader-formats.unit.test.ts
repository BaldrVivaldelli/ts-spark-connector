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
