import { afterEach, describe, expect, it, vi } from "vitest";
import type { DFAlg, DFProgram, ExprAlg } from "../src/algebra/read";
import type { BatchWriterAlg, StreamWriterAlg } from "../src/algebra/write/dataframe";
import type { WBatch, WStream } from "../src/algebra/write";
import { SparkSession } from "../src/client/session";
import type { StreamingQueryHandle } from "../src/client/sparkClient";
import { SparkConnectExecutor } from "../src/client/sparkConnectExecutor";
import {
    ProtoDFAlg,
    ProtoExprAlg,
    type ProtoExpr,
    type ProtoGroup,
    type ProtoRel,
} from "../src/engine/compilerRead";
import { ReadChainedDataFrame } from "../src/read/readChainedDataFrame";
import {
    DataFrameWriterTF,
    type Impl,
} from "../src/write/dataFrameWriterTF";
import {
    ProtoWritingAlg,
    type ProtoWriteRoot,
} from "../src/write/compilerWrite";
import type { DFWritingExec } from "../src/write/writeDataFrame";
import { ProtoWritingExec } from "../src/write/protoWriterExec";

const session = SparkSession.builder().getOrCreate();
const handle: StreamingQueryHandle = {
    name: "query",
    stop: vi.fn(async () => undefined),
    awaitTermination: vi.fn(async (timeoutMs?: number) =>
        timeoutMs === undefined ? undefined : true
    ) as StreamingQueryHandle["awaitTermination"],
};

function executor() {
    return {
        run: vi.fn(async () => undefined),
        runStream: vi.fn(async () => handle),
    } satisfies DFWritingExec<ProtoWriteRoot>;
}

const dataframeProgram: DFProgram<ProtoRel, ProtoExpr, ProtoGroup, {}, {}> =
    DF => DF.relation("parquet", "/input");

function batchImpl(
    EXE = executor(),
    WR: BatchWriterAlg<ProtoRel> = ProtoWritingAlg,
): Impl<
    ProtoRel,
    ProtoExpr,
    ProtoGroup,
    WBatch,
    {},
    {},
    BatchWriterAlg<ProtoRel>
> {
    return {
        DF: ProtoDFAlg as DFAlg<ProtoRel, ProtoExpr, ProtoGroup, {}>,
        EX: ProtoExprAlg as ExprAlg<ProtoExpr>,
        WR,
        EXE,
    };
}

function streamImpl(
    EXE = executor(),
    WR: StreamWriterAlg<ProtoRel> = ProtoWritingAlg,
): Impl<
    ProtoRel,
    ProtoExpr,
    ProtoGroup,
    WStream,
    {},
    {},
    StreamWriterAlg<ProtoRel>
> {
    return {
        DF: ProtoDFAlg as DFAlg<ProtoRel, ProtoExpr, ProtoGroup, {}>,
        EX: ProtoExprAlg as ExprAlg<ProtoExpr>,
        WR,
        EXE,
    };
}

function concreteBatch() {
    return DataFrameWriterTF.fromParts({
        session,
        dfProgram: dataframeProgram,
        wProgram: (WR: BatchWriterAlg<ProtoRel>, DF, EX) =>
            WR.fromChild(dataframeProgram(DF, EX)),
    });
}

function concreteStream() {
    return DataFrameWriterTF.fromParts({
        session,
        dfProgram: dataframeProgram,
        wProgram: (WR: StreamWriterAlg<ProtoRel>, DF, EX) =>
            WR.writeStream(dataframeProgram(DF, EX)),
    });
}

describe("DataFrameWriterTF complete behavior", () => {
    afterEach(() => vi.restoreAllMocks());

    it("covers public and legacy-private dataframe factories", () => {
        const publicDataframe = {
            getSession: () => session,
            getProgram: () => dataframeProgram,
        };
        const privateDataframe = {
            _getSession: () => session,
            _getProgram: () => dataframeProgram,
        };

        const batch = DataFrameWriterTF.fromBatchProgram(
            publicDataframe,
            (WR, DF, EX) => WR.fromChild(dataframeProgram(DF, EX)),
        );
        const stream = DataFrameWriterTF.fromStreamProgram(
            privateDataframe,
            (WR, DF, EX) => WR.writeStream(dataframeProgram(DF, EX)),
        );

        expect(batch.toClientASTJSON()).toContain("batchWrite");
        expect(stream.toClientASTJSON()).toContain("streamingWrite");
    });

    it("executes all batch transformations, shortcuts and action overloads", async () => {
        const EXE = executor();
        const impl = batchImpl(EXE);
        const writer = concreteBatch()
            .withBackend(impl)
            .format("delta")
            .option("single", 1)
            .options({ flag: true, exact: 2n })
            .partitionBy("country", "day")
            .mode("overwrite")
            .bucketBy(8, "id", "day")
            .sortBy("day", "id");

        expect(writer.toClientASTJSON()).toContain('"format": "delta"');
        expect(writer.toClientASTMermaid()).toContain("Write");
        expect(concreteBatch().parquet().toClientASTJSON()).toContain("parquet");
        expect(concreteBatch().csv().toClientASTJSON()).toContain("csv");
        expect(concreteBatch().json().toClientASTJSON()).toContain("json");
        expect(concreteBatch().orc().toClientASTJSON()).toContain("orc");
        expect(concreteBatch().avro().toClientASTJSON()).toContain("avro");
        expect(concreteBatch().text().toClientASTJSON()).toContain("text");

        await writer.save();
        await writer.save("/output");
        await writer.save(impl);
        await writer.save("/other", impl);
        await writer.saveAsTable("catalog.table", impl);
        await writer.createTempView("view", impl);
        await writer.createOrReplaceTempView("replace_view", impl);

        expect(EXE.run).toHaveBeenCalledTimes(7);
    });

    it("executes all stream transformations, shortcuts and actions", async () => {
        const EXE = executor();
        const writer = concreteStream()
            .withBackend(streamImpl(EXE))
            .format("custom")
            .option("a", "1")
            .partitionBy("day")
            .outputMode("append")
            .trigger({ processingTime: "1 second" })
            .checkpoint("/checkpoint")
            .queryName("query");

        expect(concreteStream().console().toClientASTJSON()).toContain("console");
        expect(concreteStream().kafka().toClientASTJSON()).toContain("kafka");
        expect(concreteStream().memory().toClientASTJSON()).toContain("memory");
        expect(writer.toClientASTMermaid()).toContain("WriteStream");

        await expect(writer.start()).resolves.toBe(handle);
        await expect(writer.start("/output")).resolves.toBe(handle);
        await expect(writer.toTable("catalog.table")).resolves.toBe(handle);
        await expect(writer.startAndAwaitTermination()).resolves.toBe(handle);
        await expect(writer.startAndAwaitTermination("/output")).resolves.toBe(handle);

        const fromView = writer.fromTempView("source").awaitTermination();
        expect(fromView.toClientASTJSON()).toContain("source");
        expect(fromView.toClientASTMermaid()).toContain("create:source");
        expect(EXE.runStream).toHaveBeenCalledTimes(5);
    });

    it("uses the default protobuf backend for batch and stream actions", async () => {
        const run = vi.spyOn(ProtoWritingExec, "run").mockResolvedValue();
        const runStream = vi.spyOn(ProtoWritingExec, "runStream").mockResolvedValue(handle);
        const batch = session.read.parquet("/input").write();
        const stream = ReadChainedDataFrame
            .readStream<ProtoRel, ProtoExpr, ProtoGroup>("rate", session)
            .writeStream();

        await batch.save();
        await batch.saveAsTable("catalog.default_table");
        await stream.start();

        expect(run).toHaveBeenCalledTimes(2);
        expect(runStream).toHaveBeenCalledOnce();
    });

    it("delegates valid roots through the concrete protobuf executor", async () => {
        const runWrite = vi.fn(async () => []);
        const runStream = vi.fn(async () => handle);
        vi.spyOn(SparkConnectExecutor, "for").mockReturnValue({
            runWrite,
            runStream,
        } as unknown as SparkConnectExecutor);
        const batch = ProtoWritingAlg.fromChild({}) as unknown as ProtoWriteRoot;
        const stream = ProtoWritingAlg.writeStream({}) as unknown as ProtoWriteRoot;

        await expect(ProtoWritingExec.run(batch, session)).resolves.toBeUndefined();
        await expect(ProtoWritingExec.runStream(stream, session)).resolves.toBe(handle);
        expect(runWrite).toHaveBeenCalledWith(batch);
        expect(runStream).toHaveBeenCalledWith(stream);
    });

    it("falls back to an option when a stream backend has no queryName method", async () => {
        const EXE = executor();
        const noQueryName = {
            ...ProtoWritingAlg,
            queryName: undefined,
        };
        const writer = concreteStream().withBackend(streamImpl(
            EXE,
            noQueryName as unknown as StreamWriterAlg<ProtoRel>,
        ));

        await writer.queryName("fallback").start();
        expect(EXE.runStream).toHaveBeenCalledWith(
            expect.objectContaining({
                spec: expect.objectContaining({
                    options: expect.objectContaining({ queryName: "fallback" }),
                }),
            }),
            session,
        );
    });

    it("reports missing optional backend methods", async () => {
        const withoutViews = {
            ...ProtoWritingAlg,
            createTempView: undefined,
            createOrReplaceTempView: undefined,
        };
        const batch = concreteBatch().withBackend(batchImpl(
            executor(),
            withoutViews as unknown as BatchWriterAlg<ProtoRel>,
        ));
        await expect(batch.createTempView("view")).rejects.toThrow(/createTempView/);
        await expect(batch.createOrReplaceTempView("view")).rejects
            .toThrow(/createOrReplaceTempView/);

        const withoutStreamActions = {
            ...ProtoWritingAlg,
            start: undefined,
            awaitTermination: undefined,
            fromTempView: undefined,
        };
        const stream = concreteStream().withBackend(streamImpl(
            executor(),
            withoutStreamActions as unknown as StreamWriterAlg<ProtoRel>,
        ));
        await expect(stream.start()).rejects.toThrow(/start/);
        await expect(stream.toTable("table")).rejects.toThrow(/start/);
        await expect(stream.startAndAwaitTermination()).rejects.toThrow(/awaitTermination/);
        await expect(stream.fromTempView("view").save()).rejects.toThrow(/fromTempView/);
        await expect(stream.awaitTermination().save()).rejects.toThrow(/awaitTermination/);
    });

    it("validates every malformed batch input without unsafe casts", () => {
        const runtime = concreteBatch() as unknown as {
            format(value: unknown): unknown;
            option(key: unknown, value: unknown): unknown;
            options(values: Record<string, unknown>): unknown;
            partitionBy(...columns: string[]): unknown;
            mode(mode: unknown): unknown;
            bucketBy(count: number, column: string): unknown;
            sortBy(column: string): unknown;
        };

        expect(() => runtime.format(null)).toThrow(/non-empty/);
        expect(() => runtime.option("", "x")).toThrow(/non-empty/);
        expect(() => runtime.option("x", null)).toThrow(/null or undefined/);
        expect(() => runtime.option("x", Number.NaN)).toThrow(/finite/);
        expect(() => runtime.options({ "": "x" })).toThrow(/non-empty/);
        expect(() => runtime.options({ x: undefined })).toThrow(/null or undefined/);
        expect(() => runtime.partitionBy()).toThrow(/at least one/);
        expect(() => runtime.partitionBy("")).toThrow(/non-empty/);
        expect(() => runtime.mode("invalid")).toThrow(/Unsupported save mode/);
        expect(() => runtime.bucketBy(Number.NaN, "id")).toThrow();
        expect(() => runtime.bucketBy(0, "id")).toThrow();
        expect(() => runtime.bucketBy(2_147_483_648, "id")).toThrow();
        expect(() => runtime.bucketBy(1, "")).toThrow(/non-empty/);
        expect(() => runtime.sortBy("")).toThrow(/non-empty/);
    });

    it("validates every malformed and valid stream trigger branch", () => {
        const runtime = concreteStream() as unknown as {
            outputMode(mode: unknown): unknown;
            trigger(trigger: unknown): DataFrameWriterTF;
        };

        expect(() => runtime.outputMode("invalid")).toThrow(/Unsupported output mode/);
        for (const invalid of [
            null,
            {},
            { once: true, availableNow: true },
            { processingTime: "" },
            { continuous: "" },
            { once: false },
            { availableNow: false },
            { kind: "Invalid" },
            { kind: "ProcessingTime", intervalMs: 0 },
            { kind: "ProcessingTime", intervalMs: 1.5 },
            { kind: "Continuous", checkpointIntervalMs: 0 },
            { kind: "Continuous", checkpointIntervalMs: 1.5 },
        ]) {
            expect(() => runtime.trigger(invalid)).toThrow();
        }

        for (const valid of [
            { processingTime: "1 second" },
            { continuous: "1 second" },
            { once: true },
            { availableNow: true },
            { kind: "Once" },
            { kind: "ProcessingTime", intervalMs: 1 },
            { kind: "Continuous", checkpointIntervalMs: 1 },
        ]) {
            expect(runtime.trigger(valid)).toBeInstanceOf(DataFrameWriterTF);
        }
    });
});
