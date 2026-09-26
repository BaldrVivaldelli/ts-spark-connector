import { describe, expect, it } from "vitest";
import type { LogicalPlan } from "../src/engine/logicalPlan";
import { SparkDFAlg, SparkExprAlg } from "../src/read/readDataFrameInterpreter";
import { TraceDFAlg, TraceExprAlg, type TraceNode } from "../src/trace/trace";
import { toJSON, toMermaid } from "../src/trace/traceSerializers";
import {
    TraceWriterAlg,
    writerToClientAST,
    writerToClientASTJSON,
    writerToClientASTMermaid,
} from "../src/trace/traceWriterAlg";
import {
    ProtoWritingAlg,
    protoWriteRootToPlan,
    type ProtoWriteRoot,
} from "../src/write/compilerWrite";

describe("trace interpreters", () => {
    it("executes every expression and dataframe trace operation", () => {
        const col = TraceExprAlg.col("id");
        const plannedCol = TraceExprAlg.col("id", 7);
        const literal = TraceExprAlg.lit(2n);
        const ordinaryLiteral = TraceExprAlg.lit("x");
        const binary = TraceExprAlg.bin("=", col, literal);
        const call = TraceExprAlg.call("f", [ordinaryLiteral]);
        const alias = TraceExprAlg.alias(call, "value");
        const caseWhen = TraceExprAlg.caseWhen(
            [{ when: binary, then: alias }],
            ordinaryLiteral,
        );
        const coalesced = TraceExprAlg.coalesce([caseWhen, plannedCol]);

        let dataframe = TraceDFAlg.relation("csv", "/input");
        expect(TraceDFAlg.relation("json", ["/a", "/b"], {}, "id INT").label)
            .toContain(",schema");
        dataframe = TraceDFAlg.withPlanId(dataframe, 10);
        dataframe = TraceDFAlg.select(dataframe, [col, coalesced]);
        dataframe = TraceDFAlg.filter(dataframe, binary);
        dataframe = TraceDFAlg.withColumn(dataframe, "computed", alias);
        dataframe = TraceDFAlg.join(
            dataframe,
            TraceDFAlg.sql("SELECT 1"),
            binary,
            "inner",
        );
        const grouped = TraceDFAlg.groupBy(dataframe, [col]);
        dataframe = TraceDFAlg.agg(grouped, { total: call });
        dataframe = TraceDFAlg.orderBy(dataframe, [{ expr: col, direction: "asc" }]);
        dataframe = TraceDFAlg.sort(dataframe, [{ expr: col, direction: "desc" }]);
        dataframe = TraceDFAlg.limit(dataframe, 3);
        dataframe = TraceDFAlg.distinct(dataframe);
        dataframe = TraceDFAlg.dropDuplicates(dataframe);
        dataframe = TraceDFAlg.dropDuplicates(dataframe, [col]);
        dataframe = TraceDFAlg.union(dataframe, grouped);
        dataframe = TraceDFAlg.intersect(dataframe, grouped);
        expect(TraceDFAlg.intersect(dataframe, grouped, { all: true }).label)
            .toBe("intersectAll");
        dataframe = TraceDFAlg.except(dataframe, grouped);
        expect(TraceDFAlg.except(dataframe, grouped, { all: true }).label)
            .toBe("exceptAll");
        dataframe = TraceDFAlg.withColumnRenamed(dataframe, "a", "b");
        dataframe = TraceDFAlg.withColumnsRenamed(dataframe, { b: "c" });
        dataframe = TraceDFAlg.repartition(dataframe, 4, true);
        dataframe = TraceDFAlg.coalesce(dataframe, 2);
        dataframe = TraceDFAlg.cache(dataframe);
        dataframe = TraceDFAlg.persist(dataframe, "MEMORY_ONLY");
        dataframe = TraceDFAlg.unpersist(dataframe, true);
        expect(dataframe.label).toBe("unpersist");
        const streamWithoutOptions = TraceDFAlg.readStream("rate");
        const stream = TraceDFAlg.readStream("rate", { rowsPerSecond: "1" });
        expect(streamWithoutOptions.label).not.toContain(",opts");
        dataframe = TraceDFAlg.withWatermark(stream, col, "1 minute");
        expect(TraceDFAlg.withTrigger(dataframe, { once: true }).label).toContain("once");
        expect(TraceDFAlg.withTrigger(dataframe, { processingTimeMs: 500 }).label)
            .toContain("500ms");
        expect(TraceDFAlg.withTrigger(dataframe, {}).label).toContain("withTrigger()");
        dataframe = TraceDFAlg.withOutputMode(dataframe, "append");

        expect(toJSON(dataframe)).toContain("outputMode");
    });

    it("serializes shared and quoted trace nodes to Mermaid exactly once", () => {
        const child: TraceNode = { id: "child", label: 'quoted "child"', children: [] };
        const root: TraceNode = {
            id: "root",
            label: "root",
            children: [child, child],
        };
        const mermaid = toMermaid(root);

        expect(mermaid).toContain('\\"child\\"');
        expect(mermaid.match(/child\["/g)).toHaveLength(1);
        expect(mermaid.match(/root --> child/g)).toHaveLength(2);
    });

    it("executes every writer trace operation and Mermaid branch", () => {
        const child = { node: "dataframe" };
        let batch = TraceWriterAlg.fromChild(child);
        batch = TraceWriterAlg.format(batch, "parquet");
        batch = TraceWriterAlg.option(batch, "compression", "snappy");
        batch = TraceWriterAlg.options(batch, { header: "true" });
        batch = TraceWriterAlg.partitionBy(batch, "country");
        batch = TraceWriterAlg.mode(batch, "overwrite");
        batch = TraceWriterAlg.bucketBy(batch, 8, "id", "day");
        batch = TraceWriterAlg.sortBy(batch, "day", "id");
        batch = TraceWriterAlg.targetPath(batch, "/output");

        expect(writerToClientAST(batch)).toMatchObject({
            node: "batchWrite",
            format: "parquet",
            target: { kind: "path", path: "/output" },
        });
        expect(writerToClientASTJSON(batch)).toContain('"bucketBy"');
        expect(writerToClientASTMermaid(batch)).toContain("path:/output");
        expect(writerToClientASTMermaid(
            TraceWriterAlg.targetTable(batch, "catalog.table"),
        )).toContain("table:catalog.table");
        expect(writerToClientASTMermaid(
            TraceWriterAlg.createTempView(batch, "view"),
        )).toContain("create:view");
        expect(writerToClientASTMermaid(
            TraceWriterAlg.createOrReplaceTempView(batch, "view"),
        )).toContain("createOrReplace:view");

        let stream = TraceWriterAlg.writeStream(child);
        stream = TraceWriterAlg.outputMode(stream, "append");
        stream = TraceWriterAlg.queryName(stream, "query");
        expect(TraceWriterAlg.start(stream)).toBe(stream);
        expect(TraceWriterAlg.awaitTermination(stream)).toBe(stream);
        expect(TraceWriterAlg.fromTempView(stream, "source")).toMatchObject({
            target: { kind: "tempView", name: "source", replace: false },
        });
        expect(writerToClientASTMermaid(stream)).toContain("no-target");
        expect(writerToClientASTMermaid(
            TraceWriterAlg.trigger(stream, { processingTime: "1 second" }),
        )).toContain("trigger=1 second");
        expect(writerToClientASTMermaid(
            TraceWriterAlg.trigger(stream, { once: true }),
        )).toContain("trigger=once");
        expect(writerToClientASTMermaid(
            TraceWriterAlg.trigger(stream, { availableNow: true }),
        )).toContain("trigger=availableNow");
    });
});

describe("Spark logical-plan interpreters", () => {
    it("executes every expression branch", () => {
        const col = SparkExprAlg.col("id");
        const planned = SparkExprAlg.col("id", 5);
        const literal = SparkExprAlg.lit(1);
        const binary = SparkExprAlg.bin("=", col, literal);
        const logical = SparkExprAlg.logical("AND", binary, binary);
        const alias = SparkExprAlg.alias(logical, "ok");
        const call = SparkExprAlg.call("f", [alias]);
        const sort = SparkExprAlg.sortKey(col, "desc", "nullsLast");
        const star = SparkExprAlg.star();
        const caseWhen = SparkExprAlg.caseWhen([{ when: binary, then: literal }], literal);
        const windowWithoutFrame = SparkExprAlg.win(call, {
            partitionBy: [col],
            orderBy: [{ input: col, direction: "asc" }],
        });
        const windowWithFrame = SparkExprAlg.win(call, {
            partitionBy: [],
            orderBy: [],
            frame: {
                type: "rows",
                start: { type: "CurrentRow" },
                end: { type: "UnboundedFollowing" },
            },
        });

        const results = [
            planned,
            sort,
            star,
            caseWhen,
            windowWithoutFrame,
            windowWithFrame,
            SparkExprAlg.isNull(col),
            SparkExprAlg.isNotNull(col),
            SparkExprAlg.coalesce([col, literal]),
            SparkExprAlg.explode(col),
            SparkExprAlg.posexplode(col),
            SparkExprAlg.getField(col, "nested"),
            SparkExprAlg.map_keys(col),
            SparkExprAlg.map_values(col),
            SparkExprAlg.elementAt(col, literal),
            SparkExprAlg.getItem(col, literal),
            SparkExprAlg.getItem(col, 0),
            SparkExprAlg.getItem(col, "key"),
            SparkExprAlg.split(col, literal),
            SparkExprAlg.split(col, ","),
            SparkExprAlg.from_json(col, "id INT"),
            SparkExprAlg.to_json(col),
        ];
        expect(results).toHaveLength(22);
    });

    it("executes every dataframe operation and validation branch", () => {
        const col = SparkExprAlg.col("id");
        const sortKey = SparkExprAlg.sortKey(col, "desc", "nullsLast");
        let plan = SparkDFAlg.relation("csv", "/input");
        if (!SparkDFAlg.withPlanId) throw new Error("withPlanId must be implemented");
        plan = SparkDFAlg.withPlanId(plan, 3);
        plan = SparkDFAlg.select(plan, [col]);
        plan = SparkDFAlg.filter(plan, SparkExprAlg.isNotNull(col));
        plan = SparkDFAlg.withColumn(plan, "copy", col);
        plan = SparkDFAlg.join(plan, SparkDFAlg.sql("SELECT 1"), SparkExprAlg.lit(true));
        expect(SparkDFAlg.join(plan, plan, SparkExprAlg.lit(true), "left"))
            .toMatchObject({ joinType: "LEFT" });
        const group = SparkDFAlg.groupBy(plan, [col]);
        plan = SparkDFAlg.agg(group, { total: SparkExprAlg.call("count", [col]) }, "rollup");
        plan = SparkDFAlg.orderBy(plan, [
            { expr: sortKey, direction: "asc" },
            { expr: col, direction: "desc" },
        ]);
        plan = SparkDFAlg.sort(plan, [
            { expr: sortKey, direction: "asc" },
            { expr: col, direction: "desc" },
        ]);
        plan = SparkDFAlg.limit(plan, 2);
        plan = SparkDFAlg.distinct(plan);
        expect(SparkDFAlg.dropDuplicates(plan)).toMatchObject({ type: "Distinct" });
        expect(SparkDFAlg.dropDuplicates(plan, [])).toMatchObject({ type: "Distinct" });
        expect(SparkDFAlg.dropDuplicates(plan, [col])).toMatchObject({
            type: "Deduplicate",
            columnNames: ["id"],
        });
        expect(() => SparkDFAlg.dropDuplicates(plan, [SparkExprAlg.lit(1)]))
            .toThrow(/plain column/);
        expect(SparkDFAlg.union(plan, plan)).toMatchObject({
            byName: false,
            allowMissingColumns: false,
        });
        plan = SparkDFAlg.union(plan, plan, { byName: true, allowMissingColumns: true });
        expect(SparkDFAlg.intersect(plan, plan)).toMatchObject({ isAll: false });
        plan = SparkDFAlg.intersect(plan, plan, { all: true });
        expect(SparkDFAlg.except(plan, plan)).toMatchObject({ isAll: false });
        plan = SparkDFAlg.except(plan, plan, { all: true });
        plan = SparkDFAlg.withColumnRenamed(plan, "id", "new_id");
        plan = SparkDFAlg.withColumnsRenamed(plan, { new_id: "id" });
        plan = SparkDFAlg.describe(plan, [col]);
        plan = SparkDFAlg.summary(plan, [SparkExprAlg.lit("count")], [col]);
        plan = SparkDFAlg.cache(plan);
        plan = SparkDFAlg.persist(plan, "MEMORY_ONLY");
        plan = SparkDFAlg.unpersist(plan, true);
        plan = SparkDFAlg.repartition(plan, 4, true);
        plan = SparkDFAlg.coalesce(plan, 2);
        expect(SparkDFAlg.hint(plan, "broadcast")).toMatchObject({ params: [] });
        plan = SparkDFAlg.hint(plan, "merge", [SparkExprAlg.lit("right")]);
        expect(SparkDFAlg.sample(plan, 0, 1)).not.toHaveProperty("seed");
        plan = SparkDFAlg.sample(plan, 0.1, 0.9, true, 4, true);
        plan = SparkDFAlg.drop(plan, ["id"]);
        const stream = SparkDFAlg.readStream("rate");
        expect(stream).toMatchObject({ data_source: { options: {} } });
        expect(SparkDFAlg.readStream("rate", { rowsPerSecond: "1" }))
            .toMatchObject({ data_source: { options: { rowsPerSecond: "1" } } });
        expect(() => SparkDFAlg.withWatermark(plan, SparkExprAlg.lit("bad"), "1 minute"))
            .toThrow(/plain event-time/);
        plan = SparkDFAlg.withWatermark(stream, col, "1 minute");

        expect((plan as LogicalPlan).type).toBe("EventTimeWatermark");
    });
});

describe("protobuf writer interpreter", () => {
    it("executes every writer algebra operation", () => {
        const child = { read: { named_table: { unparsed_identifier: "source" } } };
        let batch = ProtoWritingAlg.fromChild(child);
        batch = ProtoWritingAlg.format(batch, "delta");
        batch = ProtoWritingAlg.option(batch, "a", "1");
        batch = ProtoWritingAlg.options(batch, { b: "2" });
        batch = ProtoWritingAlg.partitionBy(batch, "day");
        batch = ProtoWritingAlg.mode(batch, "append");
        batch = ProtoWritingAlg.bucketBy(batch, 8, "id", "day");
        batch = ProtoWritingAlg.sortBy(batch, "day", "id");
        batch = ProtoWritingAlg.targetPath(batch, "/output");
        batch = ProtoWritingAlg.targetTable(batch, "catalog.table");
        batch = ProtoWritingAlg.createTempView(batch, "view");
        batch = ProtoWritingAlg.createOrReplaceTempView(batch, "replace_view");

        let stream = ProtoWritingAlg.writeStream(child);
        stream = ProtoWritingAlg.outputMode(stream, "complete");
        stream = ProtoWritingAlg.trigger(stream, { kind: "Once" });
        stream = ProtoWritingAlg.queryName(stream, "query");
        stream = ProtoWritingAlg.targetPath(stream, "/stream");

        const extended = ProtoWritingAlg as typeof ProtoWritingAlg & {
            start(writer: typeof stream): typeof stream;
            awaitTermination(writer: typeof stream): typeof stream;
            fromTempView(writer: typeof stream, name: string): typeof stream;
        };
        stream = extended.start(stream);
        stream = extended.awaitTermination(stream);
        stream = extended.fromTempView(stream, "source_view");

        expect(batch).toMatchObject({ writerKind: "batch" });
        expect(stream).toMatchObject({
            writerKind: "stream",
            start: true,
            awaitTermination: true,
            createStreamingView: "source_view",
        });
    });

    it("covers every streaming trigger encoding and invalid trigger path", () => {
        const base: Omit<ProtoWriteRoot, "spec"> = {
            child: {},
            writerKind: "stream",
        };
        const triggers: ProtoWriteRoot["spec"]["trigger"][] = [
            { processingTime: "1 second" },
            { once: true },
            { availableNow: true },
            { continuous: "2 seconds" },
            { kind: "ProcessingTime", intervalMs: 100 },
            { kind: "Once" },
            { kind: "Continuous", checkpointIntervalMs: 200 },
        ];
        for (const trigger of triggers) {
            expect(protoWriteRootToPlan({
                ...base,
                spec: {
                    options: {},
                    partitionBy: [],
                    sortBy: [],
                    trigger,
                },
            })).toHaveProperty("command.write_stream_operation_start");
        }

        const invalid = {
            kind: "ProcessingTime",
            intervalMs: 0,
        } as const;
        expect(() => protoWriteRootToPlan({
            ...base,
            spec: {
                options: {},
                partitionBy: [],
                sortBy: [],
                trigger: invalid,
            },
        })).toThrow(/invalid streaming trigger/i);
    });
});
