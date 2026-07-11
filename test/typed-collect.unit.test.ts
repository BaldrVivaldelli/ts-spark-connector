import { afterEach, describe, expect, it, vi } from "vitest";
import * as arrow from "apache-arrow";
import { arrowBuffersFromResponses, rowsFromArrowBuffers } from "../src/typed/arrow-rows";
import { SparkSession, schema } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";

// Builds a Spark-Connect-shaped response carrying an Arrow IPC buffer, so we can
// exercise the typed `collect()` decoding path without a live server.
function arrowResponse(columns: Record<string, unknown[]>) {
  const table = arrow.tableFromArrays(columns as any);
  return tableResponse(table);
}

function tableResponse(table: arrow.Table) {
  const ipc = arrow.tableToIPC(table, "stream");
  return { arrow_batch: { data: Buffer.from(ipc) } };
}

type Person = { id: number; name: string; age: number };

describe("arrow-rows decoding (typed collect)", () => {
  afterEach(() => vi.restoreAllMocks());
  it("decodes Arrow batches into row objects keyed by column", () => {
    const responses = [
      arrowResponse({
        id: [1, 2, 3],
        name: ["alice", "bob", "carol"],
        age: [30, 25, 41],
      }),
    ];

    const rows = rowsFromArrowBuffers<Person>(arrowBuffersFromResponses(responses));
    expect(rows).toEqual([
      { id: 1, name: "alice", age: 30 },
      { id: 2, name: "bob", age: 25 },
      { id: 3, name: "carol", age: 41 },
    ]);
  });

  it("ignores responses without an arrow_batch", () => {
    const responses = [
      { some_other_field: true },
      arrowResponse({ id: [7], name: ["zoe"], age: [50] }),
      { arrow_batch: {} },
    ];
    const rows = rowsFromArrowBuffers<Person>(arrowBuffersFromResponses(responses));
    expect(rows).toEqual([{ id: 7, name: "zoe", age: 50 }]);
  });

  it("concatenates rows across multiple batches", () => {
    const responses = [
      arrowResponse({ id: [1], name: ["a"], age: [10] }),
      arrowResponse({ id: [2], name: ["b"], age: [20] }),
    ];
    const rows = rowsFromArrowBuffers<Person>(arrowBuffersFromResponses(responses));
    expect(rows.map(r => r.id)).toEqual([1, 2]);
  });

  it("preserves every Int64 value as bigint", () => {
    const responses = [
      arrowResponse({ big: [BigInt(42)], small: [1] }),
    ];
    const rows = rowsFromArrowBuffers<{ big: bigint; small: number }>(
      arrowBuffersFromResponses(responses)
    );
    expect(rows[0].big).toBe(42n);
    expect(typeof rows[0].big).toBe("bigint");
  });

  it("preserves unsafe BigInt values without losing precision", () => {
    const unsafe = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    const responses = [arrowResponse({ big: [unsafe] })];
    const rows = rowsFromArrowBuffers<{ big: bigint }>(
      arrowBuffersFromResponses(responses)
    );

    expect(rows[0].big).toBe(unsafe);
    expect(typeof rows[0].big).toBe("bigint");
  });

  it("returns an empty array for no batches", () => {
    expect(rowsFromArrowBuffers<Person>([])).toEqual([]);
  });

  it("rejects duplicate Arrow column names before creating row objects", () => {
    const duplicateSchema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int32(), false),
      new arrow.Field("id", new arrow.Int32(), false),
    ]);
    const duplicateTable = new arrow.Table(duplicateSchema);

    expect(() => rowsFromArrowBuffers(
      arrowBuffersFromResponses([tableResponse(duplicateTable)])
    )).toThrow(/duplicate column name.*id/i);
  });

  it("validates the declared schema against Spark's Arrow schema", () => {
    const response = arrowResponse({ id: [1] });
    expect(() => rowsFromArrowBuffers(
      arrowBuffersFromResponses([response]),
      { id: "long?" },
    )).toThrow(/expected long.*Float64/i);

    expect(() => rowsFromArrowBuffers(
      arrowBuffersFromResponses([response]),
      { id: "double" },
    )).toThrow(/expects a non-null column/i);

    expect(rowsFromArrowBuffers(
      arrowBuffersFromResponses([response]),
      { id: "double?" },
    )).toEqual([{ id: 1 }]);
  });

  it("propagates readWith descriptors through non-colliding joins", async () => {
    const Left = schema({ id: "double?" });
    const Right = schema({ right_id: "double?", label: "string?" });
    const session = SparkSession.builder().getOrCreate();
    const resultWithWrongLabelType = arrowResponse({
      id: [1],
      right_id: [1],
      label: [42],
    });
    vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([
      resultWithWrongLabelType as any,
    ]);

    const joined = session.read.readWith(Left, "csv", "/tmp/left.csv").join(
      session.read.readWith(Right, "csv", "/tmp/right.csv"),
      (left, right) => left.id.eq(right.right_id),
    );
    await expect(joined.collect()).rejects.toThrow(/label.*expected string.*Float64/i);
  });

  it("normalizes temporal, decimal and nested Arrow values", () => {
    const entryField = new arrow.Field(
      "entries",
      new arrow.Struct([
        new arrow.Field("key", new arrow.Utf8(), false),
        new arrow.Field("value", new arrow.Int32(), true),
      ]),
      false,
    );
    const decimal = arrow.vectorFromArray(
      [arrow.util.BN.decimal(new Uint32Array([12345, 0, 0, 0]))],
      new arrow.Decimal(2, 10, 128),
    );
    const table = new arrow.Table({
      day: arrow.vectorFromArray([new Date("2024-01-02T00:00:00Z")], new arrow.DateDay()),
      instant: arrow.vectorFromArray([new Date("2024-01-02T03:04:05Z")], new arrow.TimestampMillisecond()),
      amount: decimal,
      values: arrow.vectorFromArray([[1, 2]], new arrow.List(new arrow.Field("item", new arrow.Int32()))),
      nested: arrow.vectorFromArray(
        [{ count: 2, label: "ok" }],
        new arrow.Struct([
          new arrow.Field("count", new arrow.Int32()),
          new arrow.Field("label", new arrow.Utf8()),
        ]),
      ),
      labels: arrow.vectorFromArray([new Map([["x", 7]])], new arrow.Map_(entryField, false)),
    });

    const [row] = rowsFromArrowBuffers<{
      day: string;
      instant: string;
      amount: string;
      values: number[];
      nested: { count: number; label: string };
      labels: Map<string, number>;
    }>(arrowBuffersFromResponses([tableResponse(table)]));

    expect(row).toEqual({
      day: "2024-01-02",
      instant: "2024-01-02T03:04:05.000Z",
      amount: "123.45",
      values: [1, 2],
      nested: { count: 2, label: "ok" },
      labels: new Map([["x", 7]]),
    });
  });
});
