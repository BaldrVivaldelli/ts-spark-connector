import { describe, expect, it } from "vitest";
import { ProtoDFAlg, ProtoExprAlg } from "../src/engine/compilerRead";
import { SparkDFAlg, SparkExprAlg } from "../src/read/readDataFrameInterpreter";
import { TraceDFAlg, TraceExprAlg, TraceNode } from "../src/trace/trace";

/**
 * Interpreter methods (and optional arguments) that the DataFrame surface
 * never reaches with every shape: `corr` always passes its method, the
 * statistic scalars never go through the logical-plan or trace printers, and
 * range partitioning always hands over sort keys.
 */

const labels = (node: TraceNode): string[] => [node.label, ...node.children.flatMap(labels)];

describe("proto interpreter", () => {
    const input = ProtoDFAlg.relation("parquet", "/data/x.parquet");
    const id = ProtoExprAlg.col("id");

    it("omits the optional correlation method", () => {
        expect(ProtoDFAlg.corr(input, "a", "b")).toEqual({ corr: { input, col1: "a", col2: "b" } });
    });

    it("sorts plain range-partition expressions ascending", () => {
        expect(ProtoDFAlg.repartitionByExpression(input, [id], undefined, true)).toEqual({
            repartition_by_expression: {
                input,
                partition_exprs: [{
                    sort_order: {
                        child: id,
                        direction: "SORT_DIRECTION_ASCENDING",
                        null_ordering: "SORT_NULLS_FIRST",
                    },
                }],
            },
        });
    });

    it("rejects sort keys for hash partitioning", () => {
        expect(() => ProtoDFAlg.repartitionByExpression(input, [ProtoExprAlg.sortKey(id, "desc")], 2, false))
            .toThrow(/use repartitionByRange/);
    });
});

describe("logical-plan interpreter", () => {
    const input = SparkDFAlg.relation("parquet", "/data/x.parquet");
    const id = SparkExprAlg.col("id");

    it("serializes the statistic relations with and without optional fields", () => {
        expect(SparkDFAlg.cov(input, "a", "b")).toEqual({ type: "Cov", input, col1: "a", col2: "b" });
        expect(SparkDFAlg.corr(input, "a", "b")).toEqual({ type: "Corr", input, col1: "a", col2: "b" });
        expect(SparkDFAlg.corr(input, "a", "b", "pearson")).toMatchObject({ method: "pearson" });
        expect(SparkDFAlg.approxQuantile(input, ["a"], [0.5], 0.1)).toEqual({
            type: "ApproxQuantile",
            input,
            columns: ["a"],
            probabilities: [0.5],
            relativeError: 0.1,
        });
        expect(SparkDFAlg.freqItems(input, ["a"])).toEqual({ type: "FreqItems", input, columns: ["a"] });
        expect(SparkDFAlg.freqItems(input, ["a"], 0.2)).toMatchObject({ support: 0.2 });
        expect(SparkDFAlg.crosstab(input, "a", "b")).toEqual({ type: "Crosstab", input, col1: "a", col2: "b" });
    });

    it("keeps optional relation fields absent", () => {
        expect(SparkDFAlg.dropNa(input, [])).toEqual({ type: "DropNa", input, columns: [] });
        expect(SparkDFAlg.unpivot(input, [id], undefined, "k", "v")).toEqual({
            type: "Unpivot",
            input,
            ids: [id],
            variableColumnName: "k",
            valueColumnName: "v",
        });
        expect(SparkDFAlg.repartitionByExpression(input, [id], undefined, false)).toEqual({
            type: "RepartitionByExpression",
            input,
            partitionExprs: [id],
            range: false,
        });
        expect(SparkDFAlg.tail(input, 3)).toEqual({ type: "Tail", input, limit: 3 });
    });
});

describe("trace interpreter", () => {
    const input = TraceDFAlg.relation("parquet", "/data/x.parquet");
    const id = TraceExprAlg.col("id");

    it("labels every relation, with and without its optional parts", () => {
        const nodes = [
            TraceDFAlg.fillNa(input, [], [0]),
            TraceDFAlg.dropNa(input, []),
            TraceDFAlg.replaceNa(input, [], [{ from: 1, to: 2 }]),
            TraceDFAlg.cov(input, "a", "b"),
            TraceDFAlg.corr(input, "a", "b"),
            TraceDFAlg.corr(input, "a", "b", "pearson"),
            TraceDFAlg.approxQuantile(input, ["a", "b"], [0.5], 0.01),
            TraceDFAlg.freqItems(input, ["a"]),
            TraceDFAlg.unpivot(input, [id], undefined, "k", "v"),
            TraceDFAlg.tail(input, 2),
            TraceDFAlg.repartitionByExpression(input, [id], undefined, false),
        ];
        expect(nodes.map(node => node.label)).toEqual([
            "fillNa[1]",
            "dropNa",
            "replaceNa[1]",
            "cov(a,b)",
            "corr(a,b)",
            "corr(a,b,pearson)",
            "approxQuantile(a,b;p[1];e=0.01)",
            "freqItems(a)",
            "unpivot(k,v;ids[1])",
            "tail(2)",
            "repartition[1]",
        ]);
        expect(labels(TraceDFAlg.crossJoin(input, input))).toEqual([
            "join(CROSS)",
            "relation(parquet)",
            "relation(parquet)",
        ]);
    });
});
