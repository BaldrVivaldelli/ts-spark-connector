import * as arrow from "apache-arrow";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession, schema } from "../src";
import { SparkConnectExecutor } from "../src/client/sparkConnectExecutor";
import {
    ProtoDFAlg,
    ProtoExec,
    ProtoExprAlg,
} from "../src/engine/compilerRead";
import {
    ReadChainedDataFrame,
    asc,
    col,
    desc,
    eq,
    lit,
} from "../src/read/readChainedDataFrame";
import { resolveOrderInput } from "../src/read/orderResolver";
import { TypedColumn } from "../src/typed/typed-column";

function arrowResponse(columns: Record<string, unknown[]>): Record<string, unknown> {
    return {
        arrow_batch: {
            data: Buffer.from(arrow.tableToIPC(arrow.tableFromArrays(columns), "stream")),
        },
    };
}

describe("ReadChainedDataFrame completion", () => {
    afterEach(() => vi.restoreAllMocks());

    it("executes standalone expression and ordering builders", () => {
        expect(col("value").build(ProtoExprAlg)).toEqual(ProtoExprAlg.col("value"));
        expect(lit(1).build(ProtoExprAlg)).toEqual(ProtoExprAlg.lit(1));
        expect(eq(col("value"), lit(1)).build(ProtoExprAlg)).toHaveProperty(
            "unresolved_function.function_name",
            "=",
        );
        expect(eq(col("value"), 2).build(ProtoExprAlg)).toHaveProperty(
            "unresolved_function.arguments.1.literal.integer",
            2,
        );
        expect(asc(col("value"), "nullsFirst")(ProtoExprAlg)).toMatchObject({
            direction: "asc",
            nulls: "nullsFirst",
        });
        expect(desc(col("value"), "nullsLast")(ProtoExprAlg)).toMatchObject({
            direction: "desc",
            nulls: "nullsLast",
        });
        expect(resolveOrderInput(
            () => new TypedColumn(EX => EX.col("typed")),
            ProtoExprAlg,
        )).toMatchObject({ direction: "asc" });
        const neutralThenLegacy = (value: unknown) =>
            value === ProtoExprAlg
                ? { expr: ProtoExprAlg.col("legacy"), direction: "asc" as const }
                : {};
        expect(resolveOrderInput(neutralThenLegacy, ProtoExprAlg)).toMatchObject({
            direction: "asc",
        });
    });

    it("covers the CSV factory, sort and multi-column rename programs", () => {
        const session = SparkSession.builder().getOrCreate();
        const frame = ReadChainedDataFrame
            .fromCSV("/input.csv", session, { header: "true" })
            .sort(desc(col("value")))
            .withColumnsRenamed({ value: "renamed" });
        expect(frame.toProtoJSON()).toContain('"format": "csv"');
        expect(frame.toProtoJSON()).toContain('"with_columns_renamed"');
        expect(frame.toSparkLogicalPlanJSON()).toContain('"type": "WithColumnsRenamed"');
    });

    it("executes every hint shortcut and both persist level forms", () => {
        const frame = SparkSession.builder().getOrCreate().read.parquet("/input");
        for (const hinted of [
            frame.broadcast(),
            frame.mergeHint(),
            frame.shuffleHashHint(),
            frame.shuffleReplicateNLHint(),
        ]) {
            expect(hinted.toProtoJSON()).toContain('"hint"');
        }
        expect(frame.persist().toProtoJSON()).toContain('"format": "parquet"');
        expect(frame.persist("DISK_ONLY").toProtoJSON()).toContain('"format": "parquet"');
    });

    it("covers schema-preserving and schema-dropping set-operation branches", () => {
        const session = SparkSession.builder().getOrCreate();
        const A = schema({ id: "int?" });
        const B = schema({ id: "int?", value: "string?" });
        const left = session.read.readWith(A, "parquet", "/left");
        const same = session.read.readWith(A, "parquet", "/right");
        const different = session.read.readWith(B, "parquet", "/different");
        const runtimeDifferent = different as unknown as typeof left;

        for (const result of [
            left.union(same),
            left.union(runtimeDifferent),
            left.unionByName(same),
            left.unionByName(different, true),
            left.intersect(same),
            left.intersect(runtimeDifferent),
            left.intersectAll(same),
            left.intersectAll(runtimeDifferent),
            left.except(same),
            left.except(runtimeDifferent),
            left.exceptAll(same),
            left.exceptAll(runtimeDifferent),
        ]) {
            expect(result.toProtoJSON()).toContain('"set_op"');
        }

        const stream = ReadChainedDataFrame.readStream("rate", session);
        expect(stream.union(left as unknown as typeof stream).isStreamingDataFrame()).toBe(true);
    });

    it("covers grouping builders, aggregation parsing and invalid aggregations", () => {
        const frame = SparkSession.builder().getOrCreate().read.parquet("/input");
        expect(frame.groupBy(col("group")).agg({
            count: "count(value)",
            maximum: col("value"),
        }).toProtoJSON()).toContain('"aggregate"');
        expect(frame.groupBy("group").agg({
            count: "count(value)",
        }).toProtoJSON()).toContain('"grouping_expressions"');
        expect(() => frame.groupBy(col("group")).agg({
            invalid: "not an aggregation",
        }).toProtoJSON()).toThrow(/invalid aggregation/i);
    });

    it("covers empty/nonempty duplicate keys and every coalesce expression kind", () => {
        const frame = SparkSession.builder().getOrCreate().read.parquet("/input");
        expect(frame.dropDuplicates().toProtoJSON()).toContain("all_columns_as_keys");
        expect(frame.dropDuplicates("value").toProtoJSON()).toContain("column_names");
        expect(frame.coalesce("result", "source", col("fallback"), 1, true).toProtoJSON())
            .toContain('"coalesce"');
    });

    it("streams raw responses, Arrow batches and decoded rows", async () => {
        const session = SparkSession.builder().getOrCreate();
        const response = arrowResponse({ id: [1, 2] });
        const stream = vi.fn(async function* () {
            yield response;
            yield { result_complete: {} };
        });
        vi.spyOn(SparkConnectExecutor, "for").mockReturnValue({
            runAnalyzeAction: vi.fn(async () => undefined),
            stream,
        } as unknown as SparkConnectExecutor);
        const frame = session.read.parquet("/input");

        const raw: unknown[] = [];
        for await (const value of frame.streamRaw()) raw.push(value);
        expect(raw).toHaveLength(2);
        const batches: Buffer[] = [];
        for await (const value of frame.toArrowBatches()) batches.push(value);
        expect(batches).toHaveLength(1);
        const rows: unknown[] = [];
        for await (const value of frame.toRows()) rows.push(value);
        expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
    });

    it("validates show inputs and prints bounded raw results", async () => {
        const collectRaw = vi.spyOn(ReadChainedDataFrame.prototype, "collectRaw")
            .mockResolvedValue([arrowResponse({ value: ["abc"] })]);
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
        const frame = SparkSession.builder().getOrCreate().read.parquet("/input");

        await expect(frame.show()).resolves.toBeUndefined();
        await expect(frame.show(1, 2)).resolves.toBeUndefined();
        await expect(frame.show(-1)).rejects.toThrow(/non-negative integer/);
        await expect(frame.show(1.5)).rejects.toThrow(/non-negative integer/);
        await expect(frame.show(1, -1)).rejects.toThrow(/truncate/);
        await expect(frame.show(1, 1.5)).rejects.toThrow(/truncate/);
        expect(collectRaw).toHaveBeenCalledTimes(2);
        expect(log).toHaveBeenCalled();
    });

    it("delegates default/custom explain modes and serializes Mermaid", async () => {
        const explain = vi.spyOn(ProtoExec, "explain")
            .mockResolvedValue("explain text");
        const frame = SparkSession.builder().getOrCreate().read.parquet("/input");

        await expect(frame.explain()).resolves.toBe("explain text");
        await expect(frame.explain("extended")).resolves.toBe("explain text");
        expect(explain).toHaveBeenNthCalledWith(1, expect.any(Object), expect.any(Object), "simple");
        expect(explain).toHaveBeenNthCalledWith(2, expect.any(Object), expect.any(Object), "extended");
        expect(frame.toClientASTMermaid()).toContain("flowchart TD");
    });

    it("runs the same program directly through protobuf interpreters", () => {
        const frame = SparkSession.builder().getOrCreate().read.parquet("/input");
        expect(frame.runWith(ProtoDFAlg, ProtoExprAlg)).toMatchObject({
            read: { data_source: { format: "parquet" } },
        });
    });
});
