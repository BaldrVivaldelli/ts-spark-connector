import { describe, expect, it } from "vitest";
import { ProtoWriteRoot, protoWriteRootToPlan } from "../src/write/compilerWrite";

// Pins the serialization of WriterSpec -> Spark Connect command plans for both
// batch and streaming writes. Complements compilerRead.unit.test.ts so the
// write side also has regression coverage without needing a live server.

const childRelation = {
  read: { data_source: { format: "csv", paths: ["/tmp/in.csv"], options: {} } },
};

function singlePlan(root: ProtoWriteRoot) {
  const plan = protoWriteRootToPlan(root);
  if (Array.isArray(plan)) {
    throw new Error("Expected a single plan, got an array.");
  }
  return plan;
}

describe("protoWriteRootToPlan - batch", () => {
  it("serializes a batch write to a path with format and save mode", () => {
    const plan = singlePlan({
      child: childRelation,
      writerKind: "batch",
      spec: {
        format: "parquet",
        mode: "overwrite",
        options: { compression: "snappy" },
        partitionBy: ["country"],
        sortBy: [],
        target: { path: "/tmp/out" },
      },
    });

    const op = (plan.command as { write_operation: Record<string, unknown> }).write_operation;
    expect(op).toMatchObject({
      source: "parquet",
      mode: 2, // SAVE_MODE_OVERWRITE
      options: { compression: "snappy" },
      partitioning_columns: ["country"],
      path: "/tmp/out",
    });
  });

  it("maps each save mode to its proto enum value", () => {
    const modes: Array<[ProtoWriteRoot["spec"]["mode"], number]> = [
      ["append", 1],
      ["overwrite", 2],
      ["error", 3],
      ["errorifexists", 3],
      ["ignore", 4],
      [undefined, 3],
    ];

    for (const [mode, expected] of modes) {
      const plan = singlePlan({
        child: childRelation,
        writerKind: "batch",
        spec: { options: {}, partitionBy: [], sortBy: [], mode, target: { path: "/tmp/out" } },
      });
      const op = (plan.command as { write_operation: Record<string, unknown> }).write_operation;
      expect(op.mode).toBe(expected);
    }
  });

  it("serializes saveAsTable target with save_method", () => {
    const plan = singlePlan({
      child: childRelation,
      writerKind: "batch",
      spec: { format: "parquet", options: {}, partitionBy: [], sortBy: [], target: { table: "db.events" } },
    });

    const op = (plan.command as { write_operation: Record<string, unknown> }).write_operation;
    expect(op.table).toEqual({ table_name: "db.events", save_method: 1 });
  });

  it("serializes bucketBy with sort columns", () => {
    const plan = singlePlan({
      child: childRelation,
      writerKind: "batch",
      spec: {
        format: "parquet",
        options: {},
        partitionBy: [],
        sortBy: ["ts"],
        bucketBy: { numBuckets: 8, columns: ["user_id"] },
        target: { path: "/tmp/out" },
      },
    });

    const op = (plan.command as { write_operation: Record<string, unknown> }).write_operation;
    expect(op.bucket_by).toEqual({ bucket_column_names: ["user_id"], num_buckets: 8 });
    expect(op.sort_column_names).toEqual(["ts"]);
  });

  it("allows provider-defined destinations with no path or table target", () => {
    const plan = singlePlan({
      child: childRelation,
      writerKind: "batch",
      spec: {
        format: "jdbc",
        options: { url: "jdbc:postgresql://db/app", dbtable: "events" },
        partitionBy: [],
        sortBy: [],
      },
    });
    const operation = (plan.command as { write_operation: Record<string, unknown> }).write_operation;
    expect(operation.path).toBeUndefined();
    expect(operation.table).toBeUndefined();
  });
});

describe("protoWriteRootToPlan - streaming", () => {
  it("serializes a streaming write with output mode, trigger and query name", () => {
    const plan = singlePlan({
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
    });

    const op = (plan.command as { write_stream_operation_start: Record<string, unknown> })
      .write_stream_operation_start;
    expect(op).toMatchObject({
      format: "console",
      options: { truncate: "false" },
      partitioning_column_names: ["date"],
      output_mode: "append",
      query_name: "rate_q",
      processing_time_interval: "1 second",
      path: "/tmp/out",
    });
  });

  it("serializes the once trigger flag", () => {
    const plan = singlePlan({
      child: childRelation,
      writerKind: "stream",
      spec: { format: "console", options: {}, partitionBy: [], sortBy: [], trigger: { once: true } },
    });
    const op = (plan.command as { write_stream_operation_start: Record<string, unknown> })
      .write_stream_operation_start;
    expect(op.once).toBe(true);
  });

  it("serializes the availableNow trigger flag", () => {
    const plan = singlePlan({
      child: childRelation,
      writerKind: "stream",
      spec: { format: "console", options: {}, partitionBy: [], sortBy: [], trigger: { availableNow: true } },
    });
    const op = (plan.command as { write_stream_operation_start: Record<string, unknown> })
      .write_stream_operation_start;
    expect(op.available_now).toBe(true);
  });

  it("emits a create_dataframe_view + write pair when a streaming view is requested", () => {
    const plan = protoWriteRootToPlan({
      child: childRelation,
      writerKind: "stream",
      createStreamingView: "stream_src",
      spec: { format: "console", options: {}, partitionBy: [], sortBy: [] },
    });

    expect(Array.isArray(plan)).toBe(true);
    if (!Array.isArray(plan)) throw new Error("Expected an array of plans.");

    expect(plan[0].command).toHaveProperty("create_dataframe_view");
    expect(plan[1].command).toHaveProperty("write_stream_operation_start");
  });
});

describe("protoWriteRootToPlan - views", () => {
  it("serializes registerView as a non-global create_dataframe_view", () => {
    const plan = singlePlan({
      child: childRelation,
      writerKind: "batch",
      spec: { options: {}, partitionBy: [], sortBy: [], registerView: { name: "v", replace: false } },
    });

    const cmd = plan.command as { create_dataframe_view?: { name: string; is_global: boolean; replace?: boolean } };
    expect(cmd.create_dataframe_view).toMatchObject({ name: "v", is_global: false, replace: false });
  });
});
