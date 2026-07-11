import path from "node:path";
import { loadSync, type MessageTypeDefinition } from "@grpc/proto-loader";
import { describe, expect, it } from "vitest";
import { SparkSession } from "../src";
import {
  type ProtoPlan,
  type ProtoWriteRoot,
  protoWriteRootToPlan,
} from "../src/write/compilerWrite";

type ProtoObject = Record<string, unknown>;

const protoRoot = path.resolve(__dirname, "../proto");
const packageDefinition = loadSync(
  [path.join(protoRoot, "spark/connect/base.proto")],
  {
    includeDirs: [protoRoot],
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  },
);

const executePlanRequest = packageDefinition[
  "spark.connect.ExecutePlanRequest"
] as MessageTypeDefinition<ProtoObject, ProtoObject>;

const child = {
  read: {
    data_source: {
      format: "csv",
      paths: ["/data/input.csv"],
      options: { header: "true" },
    },
  },
};

function plansOf(root: ProtoWriteRoot): ProtoPlan[] {
  const plan = protoWriteRootToPlan(root);
  return Array.isArray(plan) ? plan : [plan];
}

function wireRoundTrip(plan: ProtoPlan): any {
  const encoded = executePlanRequest.serialize({
    session_id: "00112233-4455-6677-8899-aabbccddeeff",
    user_context: { user_id: "write-wire-conformance" },
    client_type: "ts-spark-connector-test",
    plan,
  });
  return executePlanRequest.deserialize(encoded).plan;
}

describe("Spark Connect write protobuf wire conformance", () => {
  it("rejects num_buckets overflow before protobuf can wrap int32", () => {
    const writer = SparkSession.builder().getOrCreate().read.parquet("/data/input").write();
    expect(() => writer.bucketBy(2_147_483_648, "id")).toThrow(/2147483647/);
  });

  it("encodes a path write, including independent sort columns", () => {
    const [plan] = plansOf({
      child,
      writerKind: "batch",
      spec: {
        format: "delta" as any,
        mode: "overwrite",
        options: { compression: "snappy" },
        partitionBy: ["date"],
        sortBy: ["event_time"],
        target: { path: "/data/output" },
      },
    });
    const operation = wireRoundTrip(plan).command.write_operation;

    expect(operation).toMatchObject({
      source: "delta",
      mode: "SAVE_MODE_OVERWRITE",
      path: "/data/output",
      partitioning_columns: ["date"],
      sort_column_names: ["event_time"],
      options: { compression: "snappy" },
    });
    expect(operation.save_type).toBe("path");
  });

  it("encodes saveAsTable with the correct save method and default mode", () => {
    const [plan] = plansOf({
      child,
      writerKind: "batch",
      spec: {
        options: {},
        partitionBy: [],
        sortBy: [],
        target: { table: "catalog.analytics.events" },
      },
    });
    const operation = wireRoundTrip(plan).command.write_operation;

    expect(operation.mode).toBe("SAVE_MODE_ERROR_IF_EXISTS");
    expect(operation.table).toMatchObject({
      table_name: "catalog.analytics.events",
      save_method: "TABLE_SAVE_METHOD_SAVE_AS_TABLE",
    });
    expect(operation.save_type).toBe("table");
    expect(operation._source).toBeUndefined();
  });

  it("allows provider-driven writes with no path/table destination", () => {
    const [plan] = plansOf({
      child,
      writerKind: "batch",
      spec: {
        format: "jdbc" as any,
        options: { url: "jdbc:postgresql://db/app", dbtable: "events" },
        partitionBy: [],
        sortBy: [],
      },
    });
    const operation = wireRoundTrip(plan).command.write_operation;

    expect(operation.source).toBe("jdbc");
    expect(operation.save_type).toBeUndefined();
    expect(operation.path).toBeUndefined();
    expect(operation.table).toBeUndefined();
  });

  it.each([
    [{ processingTime: "2 seconds" }, "processing_time_interval", "2 seconds"],
    [{ once: true }, "once", true],
    [{ availableNow: true }, "available_now", true],
    [{ continuous: "5 seconds" }, "continuous_checkpoint_interval", "5 seconds"],
  ] as const)("encodes streaming trigger %o", (trigger, field, expected) => {
    const [plan] = plansOf({
      child: { read: { ...child.read, is_streaming: true } },
      writerKind: "stream",
      spec: {
        format: "console",
        outputMode: "append",
        options: {},
        partitionBy: [],
        sortBy: [],
        trigger: trigger as any,
      },
    });
    const operation = wireRoundTrip(plan).command.write_stream_operation_start;

    expect(operation[field]).toBe(expected);
    expect(operation.trigger).toBe(field);
  });

  it.each([
    [{ path: "/stream/output" }, "path", "/stream/output"],
    [{ table: "catalog.analytics.live_events" }, "table_name", "catalog.analytics.live_events"],
  ] as const)("encodes streaming sink destination %o", (target, field, expected) => {
    const [plan] = plansOf({
      child: { read: { ...child.read, is_streaming: true } },
      writerKind: "stream",
      spec: {
        format: "parquet",
        options: {},
        partitionBy: [],
        sortBy: [],
        target,
      },
    });
    const operation = wireRoundTrip(plan).command.write_stream_operation_start;

    expect(operation[field]).toBe(expected);
    expect(operation.sink_destination).toBe(field);
  });

  it("encodes create-view then streaming named-table read without losing the relation", () => {
    const plans = plansOf({
      child: { read: { ...child.read, is_streaming: true } },
      writerKind: "stream",
      createStreamingView: "stream_source",
      spec: {
        format: "memory",
        queryName: "stream_sink",
        options: {},
        partitionBy: [],
        sortBy: [],
      },
    });

    expect(plans).toHaveLength(2);
    const create = wireRoundTrip(plans[0]).command.create_dataframe_view;
    const start = wireRoundTrip(plans[1]).command.write_stream_operation_start;

    expect(create).toMatchObject({
      name: "stream_source",
      replace: true,
      is_global: false,
    });
    expect(start.input.read).toMatchObject({
      is_streaming: true,
      named_table: { unparsed_identifier: "stream_source" },
      read_type: "named_table",
    });
    expect(start.input.rel_type).toBe("read");
  });
});
