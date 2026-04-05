import { describe, expect, it } from "vitest";
import { SparkSession, col } from "../src";
import { ReadChainedDataFrame } from "../src/read/readChainedDataFrame";
import { ProtoWriteRoot, protoWriteRootToPlan } from "../src/write/compilerWrite";

describe("hardening fixes", () => {
  it("keeps auth/tls connection config separate from Spark user_context", () => {
    const session = SparkSession.builder()
      .withAuth({ type: "token", token: "secret-token" })
      .enableTLS({ trustStorePath: "./spark-server/certs/cert.crt" })
      .user({ user_id: "alice", user_name: "Alice" })
      .getOrCreate();

    expect(session.getUserContext()).toEqual({
      user_id: "alice",
      user_name: "Alice",
    });

    expect(session.getConnectionConfig().auth).toEqual({
      type: "token",
      token: "secret-token",
    });
    expect(session.getConnectionConfig().tls?.trustStorePath).toBe("./spark-server/certs/cert.crt");
    expect(session.getSessionConfig()["spark.auth.token"]).toBeUndefined();
    expect(session.getSessionConfig()["spark.ssl.trustStore"]).toBeUndefined();
    expect(session.getSessionConfig()["spark.ssl.enabled"]).toBe(true);
  });

  it("serializes column renames with with_columns_renamed", () => {
    const session = SparkSession.builder().getOrCreate();
    const df = session.read.csv("/tmp/in.csv").withColumnRenamed("user_id", "customer_id");
    const proto = JSON.parse(df.toProtoJSON());

    expect(proto.with_columns_renamed).toBeTruthy();
    expect(proto.with_columns_renamed.rename_columns_map).toEqual({
      user_id: "customer_id",
    });
  });

  it("serializes dropDuplicates on named columns as deduplicate.column_names", () => {
    const session = SparkSession.builder().getOrCreate();
    const df = session.read.csv("/tmp/in.csv").dropDuplicates("user_id", "product");
    const proto = JSON.parse(df.toProtoJSON());

    expect(proto.deduplicate).toBeTruthy();
    expect(proto.deduplicate.column_names).toEqual(["user_id", "product"]);
  });

  it("rejects dropDuplicates on non-column expressions", () => {
    const session = SparkSession.builder().getOrCreate();
    const df = session.read.csv("/tmp/in.csv");

    expect(() => df.dropDuplicates(col("user_id").gt(10)).toProtoJSON()).toThrow(/plain column references/i);
  });

  it("serializes withWatermark using the Spark Connect with_watermark relation", () => {
    const session = SparkSession.builder().getOrCreate();
    const df = ReadChainedDataFrame
      .readStream<any, any, any>("rate", session, { rowsPerSecond: "1" })
      .withWatermark(col("timestamp"), "10 minutes")
      .select("value");

    const proto = JSON.parse(df.toProtoJSON());
    const withWatermark = proto.project?.input?.with_watermark;

    expect(withWatermark).toBeTruthy();
    expect(withWatermark.event_time).toBe("timestamp");
    expect(withWatermark.delay_threshold).toBe("10 minutes");
  });

  it("uses writerKind instead of sink heuristics to classify batch vs streaming writes", () => {
    const plan = protoWriteRootToPlan({
      child: { read: { data_source: { format: "csv", paths: ["/tmp/in.csv"], options: {} } } },
      writerKind: "batch",
      spec: {
        format: "csv",
        options: {},
        partitionBy: [],
        sortBy: [],
        target: { path: "/tmp/out" },
      },
    } satisfies ProtoWriteRoot);

    expect(Array.isArray(plan)).toBe(false);
    if (Array.isArray(plan)) {
      throw new Error("Expected a single batch write plan.");
    }

    expect("write_operation" in plan.command).toBe(true);
    expect("write_stream_operation_start" in plan.command).toBe(false);
  });

  it("serializes temporary dataframe views with create_dataframe_view.is_global=false", () => {
    const plan = protoWriteRootToPlan({
      child: { read: { data_source: { format: "csv", paths: ["/tmp/in.csv"], options: {} } } },
      writerKind: "batch",
      tempViewName: "tmp_orders",
      spec: {
        options: {},
        partitionBy: [],
        sortBy: [],
      },
    } satisfies ProtoWriteRoot);

    expect(Array.isArray(plan)).toBe(false);
    if (Array.isArray(plan)) {
      throw new Error("Expected a single create_dataframe_view plan.");
    }

    const command = plan.command as { create_dataframe_view?: { is_global?: boolean } };
    expect(command.create_dataframe_view).toBeTruthy();
    expect(command.create_dataframe_view?.is_global).toBe(false);
  });

  it("serializes streaming writes with write_stream_operation_start and proto-correct fields", () => {
    const plan = protoWriteRootToPlan({
      child: { read: { data_source: { format: "rate", options: { rowsPerSecond: "1" } } } },
      writerKind: "stream",
      spec: {
        format: "console",
        options: { truncate: "false" },
        partitionBy: ["date"],
        sortBy: [],
        outputMode: "append",
        queryName: "rate_q",
        trigger: { processingTime: "1 second" },
        target: { path: "/tmp/out" },
      },
    } satisfies ProtoWriteRoot);

    expect(Array.isArray(plan)).toBe(false);
    if (Array.isArray(plan)) {
      throw new Error("Expected a single streaming write plan.");
    }

    const command = plan.command as { write_stream_operation_start?: Record<string, unknown> };
    expect(command.write_stream_operation_start).toBeTruthy();
    const operation = command.write_stream_operation_start as Record<string, unknown>;

    expect(operation).toMatchObject({
      format: "console",
      options: { truncate: "false" },
      partitioning_column_names: ["date"],
      output_mode: "append",
      query_name: "rate_q",
      processing_time_interval: "1 second",
      path: "/tmp/out",
    });
    expect("source" in operation).toBe(false);
    expect("trigger" in operation).toBe(false);
    expect("start" in operation).toBe(false);
    expect("await_termination" in operation).toBe(false);
  });
});
