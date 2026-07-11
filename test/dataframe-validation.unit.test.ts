import { describe, expect, it } from "vitest";
import { SparkSession } from "../src";

const frame = () => SparkSession.builder().getOrCreate().read.csv("/tmp/input.csv");

describe("DataFrame argument validation", () => {
    it("rejects duplicate named projections and grouping keys", () => {
        expect(() => frame().select("value", "value")).toThrow(/duplicate column/i);
        expect(() => frame().groupBy("value", "value")).toThrow(/duplicate column/i);
    });

    it("rejects invalid row and partition limits before contacting Spark", () => {
        expect(() => frame().limit(-1)).toThrow(RangeError);
        expect(() => frame().limit(1.5)).toThrow(RangeError);
        expect(() => frame().repartition(0)).toThrow(RangeError);
        expect(() => frame().coalescePartitions(Number.NaN)).toThrow(RangeError);
        expect(() => frame().limit(2_147_483_648)).toThrow(/2147483647/);
        expect(() => frame().repartition(2_147_483_648)).toThrow(/2147483647/);
        expect(() => frame().coalesce(2_147_483_648)).toThrow(/2147483647/);
        expect(() => frame().coalescePartitions(2_147_483_648)).toThrow(/2147483647/);
    });

    it("accepts valid sampling bounds and rejects invalid fractions and seeds", () => {
        expect(() => frame().sample(-0.1)).toThrow(RangeError);
        expect(() => frame().sample(Number.POSITIVE_INFINITY, true)).toThrow(RangeError);
        expect(() => frame().sample(1.1)).toThrow(/\[0,1\]/);
        expect(() => frame().sample(2, true, 42).toProtoJSON()).not.toThrow();
        expect(() => frame().sample(0.5, false, 1.2)).toThrow(RangeError);
    });

    it("requires randomSplit weights to be finite and non-negative", () => {
        expect(() => frame().randomSplit([])).toThrow(/must not be empty/);
        expect(() => frame().randomSplit([0, 0])).toThrow(/sum of weights/);
        expect(() => frame().randomSplit([1, -1])).toThrow(RangeError);
        expect(() => frame().randomSplit([1, Number.NaN])).toThrow(RangeError);
        expect(() => frame().randomSplit([Number.MAX_VALUE, Number.MAX_VALUE]))
            .toThrow(/sum.*finite/i);
        expect(frame().randomSplit([8, 2], 7)).toHaveLength(2);
    });
});
