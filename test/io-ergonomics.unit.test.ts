import { describe, expect, it } from "vitest";
import { SparkSession, lit } from "../src";
import { ReadChainedDataFrame } from "../src/read/readChainedDataFrame";
import { ProtoWritingExec } from "../src/write/protoWriterExec";

const session = () => SparkSession.builder().getOrCreate();

function protoOf(df: { toProtoJSON(): string }): any {
  return JSON.parse(df.toProtoJSON());
}

describe("read ergonomics and validation", () => {
  it("normalizes primitive reader options to wire strings", () => {
    const plan = protoOf(
      session().read.options({ header: true, maxRows: 20, exact: 20n }).csv("/input.csv"),
    );

    expect(plan.read.data_source.options).toEqual({
      header: "true",
      maxRows: "20",
      exact: "20",
    });
  });

  it("supports no-path provider load and common format shortcuts", () => {
    const jdbc = protoOf(
      session().read
        .format("jdbc")
        .options({ url: "jdbc:postgresql://db/app", dbtable: "events" })
        .load(),
    );
    expect(jdbc.read.data_source).toMatchObject({ format: "jdbc", paths: [] });

    expect(protoOf(session().read.orc("/input.orc")).read.data_source.format).toBe("orc");
    expect(protoOf(session().read.text("/input.txt")).read.data_source.format).toBe("text");
    expect(protoOf(session().read.avro("/input.avro")).read.data_source.format).toBe("avro");
  });

  it("rejects malformed formats, paths and options before execution", () => {
    expect(() => session().read.format(" ")).toThrow(/non-empty/);
    expect(() => session().read.csv()).toThrow(/at least one non-empty path/);
    expect(() => session().read.option(" ", "x")).toThrow(/key.*non-empty/);
    expect(() => session().read.option("x", Number.NaN)).toThrow(RangeError);
    const runtimeReader = session().read as unknown as {
      option(key: string, value: unknown): unknown;
    };
    expect(() => runtimeReader.option("x", undefined)).toThrow(/null or undefined/);
  });

  it("supports Spark-style coalesce(partitions) while retaining column coalesce", () => {
    const partitions = protoOf(session().read.csv("/input.csv").coalesce(3));
    expect(partitions.repartition).toMatchObject({ num_partitions: 3, shuffle: false });

    const column = protoOf(
      session().read.csv("/input.csv").coalesce("first_value", "a", lit("fallback")),
    );
    expect(column.with_columns.aliases[0]).toMatchObject({ name: ["first_value"] });
    expect(column.with_columns.aliases[0].expr.unresolved_function).toMatchObject({
      function_name: "coalesce",
    });

    expect(() => session().read.csv("/input.csv").coalesce(0)).toThrow(RangeError);
    expect(() => session().read.csv("/input.csv").coalesce("empty")).toThrow(/one expression/);
  });

  it("builds randomSplit from bounded Sample relations without a temporary column", () => {
    const plans = session().read.csv("/input.csv").randomSplit([8, 2], 7).map(protoOf);

    expect(plans[0].sample).toMatchObject({
      lower_bound: 0,
      upper_bound: 0.8,
      seed: 7,
      deterministic_order: true,
    });
    expect(plans[1].sample).toMatchObject({
      lower_bound: 0.8,
      upper_bound: 1,
      seed: 7,
      deterministic_order: true,
    });
    expect(JSON.stringify(plans)).not.toContain("__rand_split__");
  });

  it("generates required sample seeds once per lazy DataFrame", () => {
    const sampled = session().read.csv("/input.csv").sample(0.25);
    const first = protoOf(sampled).sample.seed;
    const second = protoOf(sampled).sample.seed;
    expect(first).toBeTypeOf("number");
    expect(second).toBe(first);

    const splits = session().read.csv("/input.csv").randomSplit([1, 1]).map(protoOf);
    expect(splits[0].sample.seed).toBeTypeOf("number");
    expect(splits[1].sample.seed).toBe(splits[0].sample.seed);
  });

  it("validates the deprecated DataFrame.sql shim", () => {
    expect(() => session().read.csv("/input.csv").sql(" ")).toThrow(/non-empty/);
    expect(protoOf(session().read.csv("/input.csv").sql("SELECT 1")).sql.query).toBe("SELECT 1");
  });

  it("validates streaming read formats and watermark delays", () => {
    expect(() => ReadChainedDataFrame.readStream<any, any, any>(" ", session())).toThrow(/non-empty/);
    const streaming = ReadChainedDataFrame.readStream<any, any, any>("rate", session());
    expect(() => streaming.withWatermark({ build: EX => EX.col("timestamp") }, " ")).toThrow(/non-empty/);
  });
});

describe("write ergonomics and validation", () => {
  const batch = () => session().read.csv("/input.csv").write();
  const stream = () => ReadChainedDataFrame
    .readStream<any, any, any>("rate", session())
    .writeStream();

  it("accepts arbitrary providers and exposes common batch/stream shortcuts", () => {
    expect(batch().format("delta").toClientASTJSON()).toContain('"format": "delta"');
    expect(batch().text().toClientASTJSON()).toContain('"format": "text"');
    expect(stream().console().toClientASTJSON()).toContain('"format": "console"');
    expect(stream().kafka().toClientASTJSON()).toContain('"format": "kafka"');
    expect(stream().memory().toClientASTJSON()).toContain('"format": "memory"');
  });

  it("normalizes primitive writer options and accepts continuous triggers", () => {
    const writer = batch().options({ header: true, maxRecordsPerFile: 1000, exact: 1000n });
    expect(writer.toClientASTJSON()).toContain('"header": "true"');
    expect(writer.toClientASTJSON()).toContain('"maxRecordsPerFile": "1000"');
    expect(writer.toClientASTJSON()).toContain('"exact": "1000"');

    expect(stream().trigger({ continuous: "5 seconds" }).toClientASTJSON())
      .toContain('"continuous": "5 seconds"');
  });

  it("rejects invalid destinations, columns, options and triggers", async () => {
    expect(() => batch().format(" ")).toThrow(/non-empty/);
    expect(() => batch().partitionBy()).toThrow(/at least one column/);
    expect(() => batch().bucketBy(0, "id")).toThrow(RangeError);
    expect(() => batch().sortBy(" ")).toThrow(/non-empty/);
    expect(() => batch().option("x", Number.POSITIVE_INFINITY)).toThrow(RangeError);
    expect(() => stream().checkpoint(" ")).toThrow(/non-empty/);
    expect(() => stream().queryName(" ")).toThrow(/non-empty/);
    const runtimeStream = stream() as unknown as {
      trigger(input: unknown): unknown;
    };
    expect(() => runtimeStream.trigger({ processingTime: "", once: true })).toThrow(/exactly one/);
    expect(() => stream().trigger({ kind: "Continuous", checkpointIntervalMs: 0 })).toThrow(RangeError);
    await expect(batch().save(" ")).rejects.toThrow(/non-empty/);
    await expect(batch().saveAsTable(" ")).rejects.toThrow(/non-empty/);
    await expect(stream().start(" ")).rejects.toThrow(/non-empty/);
    await expect(stream().toTable(" ")).rejects.toThrow(/non-empty/);
    await expect(stream().startAndAwaitTermination(" ")).rejects.toThrow(/non-empty/);
  });

  it("does not execute a streaming root through the batch path (or vice versa)", async () => {
    const fakeSession = session();
    await expect(ProtoWritingExec.run({
      child: {},
      writerKind: "stream",
      spec: { options: {}, partitionBy: [], sortBy: [] },
    }, fakeSession)).rejects.toThrow(/Streaming writes.*start/);
    await expect(ProtoWritingExec.runStream({
      child: {},
      writerKind: "batch",
      spec: { options: {}, partitionBy: [], sortBy: [] },
    }, fakeSession)).rejects.toThrow(/Batch writes.*save/);

    const streamingDf = ReadChainedDataFrame.readStream<any, any, any>("rate", fakeSession);
    expect(() => (streamingDf as unknown as { write(): unknown }).write()).toThrow(/batch.*streaming/i);
    const batchDf = fakeSession.read.parquet("/input.parquet");
    expect(() => (batchDf as unknown as { writeStream(): unknown }).writeStream()).toThrow(/writeStream.*batch/i);
  });
});
