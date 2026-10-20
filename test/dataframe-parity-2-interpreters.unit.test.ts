import { describe, expect, it } from "vitest";
import { ProtoDFAlg, ProtoExprAlg } from "../src/engine/compilerRead";
import { GroupType, JoinType } from "../src/engine/sparkConnectEnums";
import { SparkDFAlg, SparkExprAlg } from "../src/read/readDataFrameInterpreter";
import { TraceDFAlg, TraceExprAlg, TraceNode } from "../src/trace/trace";

/**
 * Interpreter shapes the DataFrame surface never produces on its own: explicit
 * group types overriding a grouping spec, lateral joins without conditions,
 * sort keys inside partition-local sorts, and every optional label part of the
 * trace printer.
 */

const labels = (node: TraceNode): string[] => [node.label, ...node.children.flatMap(labels)];

describe("proto interpreter", () => {
    const input = ProtoDFAlg.relation("parquet", "/data/x.parquet");
    const id = ProtoExprAlg.col("id");
    const count = ProtoExprAlg.call("count", [ProtoExprAlg.lit(1)]);

    it("keeps a plain groupBy untouched and lets agg override the grouping spec", () => {
        expect(ProtoDFAlg.groupBy(input, [id])).toEqual({ __group__: { input, keys: [id] } });
        const rolled = ProtoDFAlg.groupBy(input, [id], { groupType: "rollup" });
        expect(ProtoDFAlg.agg(rolled, { n: count }, "cube").aggregate).toMatchObject({
            group_type: GroupType.GROUP_TYPE_CUBE,
        });
        expect(ProtoDFAlg.agg(rolled, { n: count }).aggregate).toMatchObject({
            group_type: GroupType.GROUP_TYPE_ROLLUP,
        });
    });

    it("omits pivot values when Spark has to collect them", () => {
        const pivoted = ProtoDFAlg.groupBy(input, [id], { groupType: "pivot", pivot: { column: ProtoExprAlg.col("k") } });
        const aggregate = ProtoDFAlg.agg(pivoted, { n: count }).aggregate as Record<string, unknown>;
        expect(aggregate.pivot).toEqual({ col: ProtoExprAlg.col("k") });
        expect(aggregate).not.toHaveProperty("grouping_sets");
    });

    it("omits the lateral join condition and the optional as-of fields", () => {
        expect(ProtoDFAlg.lateralJoin(input, input, undefined, "cross")).toEqual({
            lateral_join: { left: input, right: input, join_type: JoinType.JOIN_TYPE_CROSS },
        });
        expect(ProtoDFAlg.asOfJoin(input, input, {
            leftAsOf: id,
            rightAsOf: id,
            joinType: "inner",
            allowExactMatches: false,
            direction: "nearest",
        })).toEqual({
            as_of_join: {
                left: input,
                right: input,
                left_as_of: id,
                right_as_of: id,
                join_type: "inner",
                allow_exact_matches: false,
                direction: "nearest",
            },
        });
    });

    it("scopes regexes and metadata columns to a plan id when given", () => {
        expect(ProtoExprAlg.colRegex("`a.*`", 7)).toEqual({ unresolved_regex: { col_name: "`a.*`", plan_id: 7 } });
        expect(ProtoExprAlg.metadataCol("_metadata", 7)).toEqual({
            unresolved_attribute: { unparsed_identifier: "_metadata", plan_id: 7, is_metadata_column: true },
        });
    });
});

describe("logical-plan interpreter", () => {
    const input = SparkDFAlg.relation("parquet", "/data/x.parquet");
    const id = SparkExprAlg.col("id");
    const count = SparkExprAlg.call("count", [SparkExprAlg.lit(1)]);

    it("records grouping specs on the GroupBy node and lets agg override the type", () => {
        expect(SparkDFAlg.groupBy(input, [id])).toEqual({ type: "GroupBy", input, expressions: [id] });
        const sets = SparkDFAlg.groupBy(input, [id], { groupType: "grouping_sets", groupingSets: [[id], []] });
        expect(sets).toEqual({ type: "GroupBy", input, expressions: [id], groupType: "grouping_sets", groupingSets: [[id], []] });
        const pivoted = SparkDFAlg.groupBy(input, [id], { groupType: "pivot", pivot: { column: id, values: ["a"] } });
        expect(pivoted).toMatchObject({ groupType: "pivot", pivot: { column: id, values: ["a"] } });
        expect(SparkDFAlg.agg(sets, { n: count })).toMatchObject({ groupType: "grouping_sets" });
        expect(SparkDFAlg.agg(sets, { n: count }, "cube")).toMatchObject({ groupType: "cube" });
        expect(SparkDFAlg.agg(SparkDFAlg.groupBy(input, [id]), { n: count })).toMatchObject({ groupType: undefined });
    });

    it("sorts within partitions with sort keys and plain expressions", () => {
        const plan = SparkDFAlg.sortWithinPartitions(input, [
            { expr: SparkExprAlg.sortKey(id, "desc", "nullsLast"), direction: "asc" },
            { expr: id, direction: "asc" },
        ]);
        expect(plan).toEqual({
            type: "Sort",
            input,
            orders: [
                { expr: id, direction: "desc", nulls: "nullsLast" },
                { expr: id, direction: "asc" },
            ],
            isGlobal: false,
        });
    });

    it("deduplicates within the watermark on every column, on names, and rejects expressions", () => {
        expect(SparkDFAlg.dropDuplicatesWithinWatermark(input)).toEqual({ type: "Deduplicate", input, withinWatermark: true });
        expect(SparkDFAlg.dropDuplicatesWithinWatermark(input, [])).toEqual({ type: "Deduplicate", input, withinWatermark: true });
        expect(SparkDFAlg.dropDuplicatesWithinWatermark(input, [id])).toEqual({
            type: "Deduplicate",
            input,
            columnNames: ["id"],
            withinWatermark: true,
        });
        expect(() => SparkDFAlg.dropDuplicatesWithinWatermark(input, [count])).toThrow(/plain column references/);
    });

    it("serializes the remaining parity relations", () => {
        expect(SparkDFAlg.lateralJoin(input, input, undefined, "left")).toEqual({
            type: "LateralJoin",
            left: input,
            right: input,
            joinType: "LEFT",
        });
        expect(SparkDFAlg.lateralJoin(input, input, id, "inner")).toMatchObject({ on: id, joinType: "INNER" });
        const spec = { leftAsOf: id, rightAsOf: id, joinType: "left", allowExactMatches: true, direction: "backward" as const };
        expect(SparkDFAlg.asOfJoin(input, input, spec)).toEqual({ type: "AsOfJoin", left: input, right: input, spec });
        expect(SparkDFAlg.observe(input, "m", [count])).toEqual({ type: "CollectMetrics", input, name: "m", metrics: [count] });
        expect(SparkDFAlg.cachedRemoteRelation("cp")).toEqual({ type: "CachedRemoteRelation", relationId: "cp" });
        expect(SparkDFAlg.toSchema(input, "id INT")).toEqual({ type: "ToSchema", input, schema: "id INT" });
        expect(SparkDFAlg.withMetadata(input, "id", id, "{}")).toEqual({
            type: "WithMetadata",
            input,
            name: "id",
            column: id,
            metadata: "{}",
        });
        expect(SparkExprAlg.colRegex("`x`")).toEqual({ type: "ColumnRegex", regex: "`x`" });
        expect(SparkExprAlg.colRegex("`x`", 3)).toEqual({ type: "ColumnRegex", regex: "`x`", planId: 3 });
        expect(SparkExprAlg.metadataCol("_metadata")).toEqual({ type: "MetadataColumn", name: "_metadata" });
        expect(SparkExprAlg.metadataCol("_metadata", 3)).toEqual({ type: "MetadataColumn", name: "_metadata", planId: 3 });
    });
});

describe("trace interpreter", () => {
    const input = TraceDFAlg.relation("parquet", "/data/x.parquet");
    const id = TraceExprAlg.col("id");
    const count = TraceExprAlg.call("count", [TraceExprAlg.lit(1)]);

    it("labels grouping variants with their values and sets", () => {
        expect(TraceDFAlg.groupBy(input, [id]).label).toBe("groupBy[1]");
        const pivotWithValues = TraceDFAlg.groupBy(input, [id], {
            groupType: "pivot",
            pivot: { column: TraceExprAlg.col("k"), values: ["a", "b"] },
        });
        expect(pivotWithValues.label).toBe("pivot[1](values[2])");
        expect(pivotWithValues.children).toHaveLength(3);
        const pivotWithoutValues = TraceDFAlg.groupBy(input, [], { groupType: "pivot", pivot: { column: id } });
        expect(pivotWithoutValues.label).toBe("pivot[0]");
        const sets = TraceDFAlg.groupBy(input, [id], { groupType: "grouping_sets", groupingSets: [[id], []] });
        expect(sets.label).toBe("grouping_sets[1](sets[2])");
        expect(sets.children).toHaveLength(3);
        expect(TraceDFAlg.groupBy(input, [id], { groupType: "cube" }).label).toBe("cube[1]");
    });

    it("labels the remaining parity relations with every optional part", () => {
        expect(labels(TraceDFAlg.sortWithinPartitions(input, [{ expr: id, direction: "asc" }]))[0]).toBe("sortWithinPartitions[1]");
        expect(TraceDFAlg.dropDuplicatesWithinWatermark(input).label).toBe("dropDuplicatesWithinWatermark[0]");
        expect(TraceDFAlg.dropDuplicatesWithinWatermark(input, [id]).label).toBe("dropDuplicatesWithinWatermark[1]");
        expect(TraceDFAlg.lateralJoin(input, input, undefined, "cross")).toMatchObject({ label: "lateralJoin(CROSS)" });
        expect(TraceDFAlg.lateralJoin(input, input, id, "inner").children).toHaveLength(3);

        const minimal = TraceDFAlg.asOfJoin(input, input, {
            leftAsOf: id,
            rightAsOf: id,
            joinType: "inner",
            allowExactMatches: true,
            direction: "backward",
        });
        expect(minimal.label).toBe("asOfJoin(inner,backward)");
        expect(minimal.children).toHaveLength(4);
        const full = TraceDFAlg.asOfJoin(input, input, {
            leftAsOf: id,
            rightAsOf: id,
            on: id,
            usingColumns: ["k"],
            joinType: "left",
            tolerance: TraceExprAlg.lit(1),
            allowExactMatches: false,
            direction: "forward",
        });
        expect(full.label).toBe("asOfJoin(left,forward,strict;using[k])");
        expect(full.children).toHaveLength(6);

        expect(TraceDFAlg.observe(input, "m", [count]).label).toBe("observe(m)[1]");
        expect(TraceDFAlg.cachedRemoteRelation("cp").label).toBe("checkpoint(cp)");
        expect(TraceDFAlg.toSchema(input, "id INT").label).toBe("to(id INT)");
        expect(TraceDFAlg.withMetadata(input, "id", id, "{}").label).toBe("withMetadata(id)");
        expect(TraceExprAlg.colRegex("`x`").label).toBe("colRegex(`x`)");
        expect(TraceExprAlg.colRegex("`x`", 2).label).toBe("colRegex(`x`@2)");
        expect(TraceExprAlg.metadataCol("_metadata").label).toBe("metadataCol(_metadata)");
        expect(TraceExprAlg.metadataCol("_metadata", 2).label).toBe("metadataCol(_metadata@2)");
    });
});
