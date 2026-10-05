import { describe, expect, it } from "vitest";
import { SparkSession } from "../src/client/session";
import { SparkDFAlg, SparkExprAlg } from "../src/read/readDataFrameInterpreter";
import {
    assertStatisticsColumns,
    buildDescribePlan,
    buildSummaryPlan,
} from "../src/read/statistics";

// describe()/summary() are emulated client-side with aggregations and unions
// so the result keeps PySpark's row layout (one row per metric).

describe("statistics planner", () => {
    const relation = SparkDFAlg.relation("csv", "/input");

    it("rejects a column named summary because it collides with the metric column", () => {
        assertStatisticsColumns("describe", ["id"]);
        expect(() => assertStatisticsColumns("describe", ["summary"])).toThrow(/describe/);
        expect(() => assertStatisticsColumns("summary", ["summary"])).toThrow(/summarize/);
    });

    it("builds describe and every built-in summary metric as a union of metric rows", () => {
        expect(buildDescribePlan(SparkDFAlg, SparkExprAlg, relation, ["id"]))
            .toMatchObject({ type: "Union" });
        expect(buildSummaryPlan(
            SparkDFAlg,
            SparkExprAlg,
            relation,
            ["count", "mean", "std", "min", "median", "75%", "max"],
            ["id"],
        )).toMatchObject({ type: "Union" });
        expect(buildSummaryPlan(SparkDFAlg, SparkExprAlg, relation, [], ["id"]))
            .toMatchObject({ type: "Union" });
    });

    it("rejects percentiles outside 0-100 and unknown metrics", () => {
        expect(() => buildSummaryPlan(SparkDFAlg, SparkExprAlg, relation, ["101%"], ["id"]))
            .toThrow(/invalid percentile/);
        expect(() => buildSummaryPlan(SparkDFAlg, SparkExprAlg, relation, ["unknown"], ["id"]))
            .toThrow(/unsupported metric/);
    });

    it("compiles the high-level describe and summary lazily into set operations", () => {
        const dataframe = SparkSession.builder().getOrCreate().read.csv("/input");
        expect(dataframe.describe(["id"]).toProtoJSON()).toContain('"set_op"');
        expect(dataframe.summary(undefined, ["id"]).toProtoJSON()).toContain('"set_op"');
    });
});
