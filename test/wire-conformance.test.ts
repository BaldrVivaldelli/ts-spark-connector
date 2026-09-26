import path from "node:path";
import { loadSync, type MessageTypeDefinition } from "@grpc/proto-loader";
import { describe, expect, it } from "vitest";
import { SparkSession, col, lit } from "../src";
import { ProtoDFAlg, ProtoExprAlg } from "../src/engine/compilerRead";
import { Window, call } from "../src/engine/column";
import { schema } from "../src/schema/schema";

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

function planOf(df: { toProtoJSON(): string }): ProtoObject {
  return JSON.parse(df.toProtoJSON()) as ProtoObject;
}

function wireRoundTrip(root: ProtoObject): any {
  const encoded = executePlanRequest.serialize({
    session_id: "00112233-4455-6677-8899-aabbccddeeff",
    user_context: { user_id: "wire-conformance" },
    client_type: "ts-spark-connector-test",
    plan: { root },
  });
  return executePlanRequest.deserialize(encoded);
}

const session = () => SparkSession.builder().getOrCreate();

describe("Spark Connect protobuf wire conformance", () => {
  it.each([
    ["INNER", "JOIN_TYPE_INNER"],
    ["FULL", "JOIN_TYPE_FULL_OUTER"],
    ["LEFT", "JOIN_TYPE_LEFT_OUTER"],
    ["RIGHT", "JOIN_TYPE_RIGHT_OUTER"],
    ["LEFT_ANTI", "JOIN_TYPE_LEFT_ANTI"],
    ["LEFT_SEMI", "JOIN_TYPE_LEFT_SEMI"],
    ["CROSS", "JOIN_TYPE_CROSS"],
  ] as const)("encodes join type %s as %s", (input, expected) => {
    const relation = ProtoDFAlg.relation("parquet", "/data/input.parquet");
    const root = ProtoDFAlg.join(
      relation,
      relation,
      ProtoExprAlg.bin("=", ProtoExprAlg.col("id"), ProtoExprAlg.col("right_id")),
      input,
    );

    expect(wireRoundTrip(root).plan.root.join.join_type).toBe(expected);
  });

  it("wire-binds same-name typed join columns to distinct relation plan ids", () => {
    const Side = schema({ id: "long" });
    const left = session().read.readWith(Side, "parquet", "/data/left");
    const right = session().read.readWith(Side, "parquet", "/data/right");
    const decoded = wireRoundTrip(planOf(left.join(right, (l, r) => l.id.eq(r.id))));
    const join = decoded.plan.root.join;
    const leftPlanId = join.left.common.plan_id;
    const rightPlanId = join.right.common.plan_id;
    const args = join.join_condition.unresolved_function.arguments;

    expect(leftPlanId).not.toBe(rightPlanId);
    expect(args[0].unresolved_attribute.plan_id).toBe(leftPlanId);
    expect(args[1].unresolved_attribute.plan_id).toBe(rightPlanId);
  });

  it("preserves multipath, WithColumns, join condition/type, sort and a complete window", () => {
    const windowSpec = Window
      .partitionBy("group_id")
      .orderBy(col("score").descNullsLast())
      .rowsBetween(
        { type: "UnboundedPreceding" },
        { type: "CurrentRow" },
      );

    const left = session()
      .read.csv("/data/left-a.csv", "/data/left-b.csv")
      .withColumn("row_num", call("row_number", []).over(windowSpec));
    const right = session().read.parquet("/data/right.parquet");
    const root = planOf(
      left
        .join(right, col("id").eq(col("right_id")), "LEFT")
        .orderBy(col("id").descNullsLast()),
    );

    const decoded = wireRoundTrip(root);
    const sort = decoded.plan.root.sort;
    expect(sort.order[0]).toMatchObject({
      direction: "SORT_DIRECTION_DESCENDING",
      null_ordering: "SORT_NULLS_LAST",
    });
    expect(sort.order[0].child.unresolved_attribute.unparsed_identifier).toBe("id");

    const join = sort.input.join;
    expect(join.join_type).toBe("JOIN_TYPE_LEFT_OUTER");
    expect(join.join_condition.unresolved_function.function_name).toBe("=");
    expect(join.join_condition.unresolved_function.arguments).toHaveLength(2);

    const withColumns = join.left.with_columns;
    expect(withColumns.input.read.data_source.paths).toEqual([
      "/data/left-a.csv",
      "/data/left-b.csv",
    ]);
    expect(withColumns.aliases).toHaveLength(1);
    expect(withColumns.aliases[0].name).toEqual(["row_num"]);

    const window = withColumns.aliases[0].expr.window;
    expect(window.window_function.unresolved_function.function_name).toBe("row_number");
    expect(window.partition_spec[0].unresolved_attribute.unparsed_identifier).toBe("group_id");
    expect(window.order_spec[0]).toMatchObject({
      direction: "SORT_DIRECTION_DESCENDING",
      null_ordering: "SORT_NULLS_LAST",
    });
    expect(window.frame_spec).toMatchObject({
      frame_type: "FRAME_TYPE_ROW",
      lower: { unbounded: true },
      upper: { current_row: true },
    });
  });

  it("encodes range-window value boundaries with the correct sign", () => {
    const input = ProtoDFAlg.relation("parquet", "/data/input.parquet");
    const expression = ProtoExprAlg.win(
      ProtoExprAlg.call("sum", [ProtoExprAlg.col("amount")]),
      {
        partitionBy: [ProtoExprAlg.col("account_id")],
        orderBy: [{
          input: ProtoExprAlg.col("event_time"),
          direction: "asc",
          nulls: "nullsFirst",
        }],
        frame: {
          type: "range",
          start: { type: "ValuePreceding", value: 5 },
          end: { type: "ValueFollowing", value: 2 },
        },
      },
    );
    const decoded = wireRoundTrip(ProtoDFAlg.select(input, [expression]));
    const window = decoded.plan.root.project.expressions[0].window;

    expect(window.frame_spec.frame_type).toBe("FRAME_TYPE_RANGE");
    expect(window.frame_spec.lower.value.literal.long).toBe("-5");
    expect(window.frame_spec.upper.value.literal.long).toBe("2");
  });

  it("uses int32 offsets for row windows and rejects overflowing boundaries", () => {
    const func = ProtoExprAlg.call("sum", [ProtoExprAlg.col("amount")]);
    const rowWindow = ProtoExprAlg.win(func, {
      partitionBy: [],
      orderBy: [],
      frame: {
        type: "rows",
        start: { type: "ValuePreceding", value: 3 },
        end: { type: "ValueFollowing", value: 4 },
      },
    });
    const input = ProtoDFAlg.relation("parquet", "/data/input.parquet");
    const decoded = wireRoundTrip(ProtoDFAlg.select(input, [rowWindow]));
    const frame = decoded.plan.root.project.expressions[0].window.frame_spec;

    expect(frame.lower.value.literal.integer).toBe(-3);
    expect(frame.upper.value.literal.integer).toBe(4);
    expect(() => ProtoExprAlg.win(func, {
      partitionBy: [],
      orderBy: [],
      frame: {
        type: "rows",
        start: { type: "ValuePreceding", value: 2_147_483_649 },
        end: { type: "CurrentRow" },
      },
    })).toThrow(/signed int32/i);
  });

  it("keeps null, int32 and exact int64 literals distinct across the wire", () => {
    const root = planOf(
      session().read.csv("/data/input.csv").select(
        lit(null),
        lit(2_147_483_647),
        lit(2_147_483_648),
        lit(Number.MAX_SAFE_INTEGER),
        lit(9_223_372_036_854_775_807n),
      ),
    );
    const expressions = wireRoundTrip(root).plan.root.project.expressions;

    expect(expressions[0].literal.null.null).toBeTruthy();
    expect(expressions[1].literal.integer).toBe(2_147_483_647);
    expect(expressions[2].literal.long).toBe("2147483648");
    expect(expressions[3].literal.long).toBe("9007199254740991");
    expect(expressions[4].literal.long).toBe("9223372036854775807");
  });

  it("rejects integer literals that cannot be represented exactly or as int64", () => {
    expect(() =>
      session().read.csv("/data/input.csv").select(
        lit(Number.MAX_SAFE_INTEGER + 1),
      ).toProtoJSON()
    ).toThrow(/not exactly representable/i);

    expect(() =>
      session().read.csv("/data/input.csv").select(
        lit(9_223_372_036_854_775_808n),
      ).toProtoJSON()
    ).toThrow(/outside Spark's signed int64 range/i);
  });

  it("preserves declared schemas, named tables, SQL and literal hint parameters", () => {
    const declared = session().read.readWith(
      schema({ id: "long" }),
      "csv",
      "/data/typed.csv",
    );
    const typed = wireRoundTrip(planOf(declared));
    expect(typed.plan.root.read.data_source.schema).toBe("id BIGINT NOT NULL");

    const table = wireRoundTrip(planOf(session().read.table("catalog.db.events")));
    expect(table.plan.root.read.named_table.unparsed_identifier).toBe("catalog.db.events");

    const sql = wireRoundTrip(planOf(session().read.sql("SELECT 1")));
    expect(sql.plan.root.sql.query).toBe("SELECT 1");

    const hinted = wireRoundTrip(planOf(
      session().read.parquet("/data/input.parquet").hint("repartition", 8, "id"),
    ));
    expect(hinted.plan.root.hint.parameters[0].literal.integer).toBe(8);
    expect(hinted.plan.root.hint.parameters[1].literal.string).toBe("id");
  });

  it("rejects empty batch paths before producing an invalid request", () => {
    expect(() => session().read.csv()).toThrow(/at least one non-empty path/i);
    expect(() => session().read.sql(" ")).toThrow(/exactly one non-empty string/i);
  });
});
