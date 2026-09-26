import { describe, expect, it } from "vitest";
import { SparkSession, col, lit, when } from "../src";

// Compiles a DataFrame program down to the Spark Connect proto JSON the client
// sends over the wire. These tests pin the serialization of the core read
// transformations so proto regressions surface without needing a live server.

function planOf(df: { toProtoJSON(): string }): any {
  return JSON.parse(df.toProtoJSON());
}

const session = () => SparkSession.builder().getOrCreate();

describe("ProtoDFAlg / ProtoExprAlg serialization", () => {
  it("serializes a CSV relation with options", () => {
    const df = session().read.option("header", "true").csv("/tmp/in.csv");
    const proto = planOf(df);

    expect(proto.read.data_source).toMatchObject({
      format: "csv",
      paths: ["/tmp/in.csv"],
      options: { header: "true" },
    });
  });

  it("serializes select() as a project with unresolved attributes", () => {
    const df = session().read.csv("/tmp/in.csv").select("a", "b");
    const proto = planOf(df);

    expect(proto.project.expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "a" } },
      { unresolved_attribute: { unparsed_identifier: "b" } },
    ]);
  });

  it("serializes filter() with a comparison as an unresolved_function", () => {
    const df = session().read.csv("/tmp/in.csv").filter(col("amount").gt(100));
    const proto = planOf(df);

    expect(proto.filter.condition.unresolved_function).toMatchObject({
      function_name: ">",
    });
    const args = proto.filter.condition.unresolved_function.arguments;
    expect(args[0]).toEqual({ unresolved_attribute: { unparsed_identifier: "amount" } });
    expect(args[1]).toEqual({ literal: { integer: 100 } });
  });

  it("distinguishes integer from double literals", () => {
    const intDf = planOf(session().read.csv("/x").filter(col("a").gt(3)));
    const dblDf = planOf(session().read.csv("/x").filter(col("a").gt(3.5)));

    expect(intDf.filter.condition.unresolved_function.arguments[1]).toEqual({ literal: { integer: 3 } });
    expect(dblDf.filter.condition.unresolved_function.arguments[1]).toEqual({ literal: { double: 3.5 } });
  });

  it("serializes withColumn() as the protocol WithColumns relation", () => {
    const df = session().read.csv("/tmp/in.csv").withColumn("doubled", col("amount").gt(0));
    const proto = planOf(df);

    expect(proto.with_columns.aliases).toEqual([
      {
        expr: {
          unresolved_function: {
            function_name: ">",
            arguments: expect.any(Array),
          },
        },
        name: ["doubled"],
      },
    ]);
  });

  it("serializes join() with the mapped proto join type", () => {
    const left = session().read.csv("/tmp/people.csv");
    const right = session().read.csv("/tmp/purchases.csv");
    const df = left.join(right, col("id").eq(col("user_id")), "LEFT");
    const proto = planOf(df);

    expect(proto.join.join_type).toBe(3); // JOIN_TYPE_LEFT_OUTER
    expect(proto.join.left).toBeTruthy();
    expect(proto.join.right).toBeTruthy();
    expect(proto.join.join_condition.unresolved_function.function_name).toBe("=");
  });

  it("accepts PySpark-style lowercase join types", () => {
    const left = session().read.csv("/tmp/people.csv");
    const right = session().read.csv("/tmp/purchases.csv");
    const df = left.join(right, col("id").eq(col("user_id")), "left");
    const proto = planOf(df);

    expect(proto.join.join_type).toBe(3); // JOIN_TYPE_LEFT_OUTER, same as "LEFT"
  });

  it("serializes groupBy().agg() as an aggregate with grouping + aggregate expressions", () => {
    const df = session()
      .read.csv("/tmp/in.csv")
      .groupBy("country")
      .agg({ total: "sum(amount)" });
    const proto = planOf(df);

    expect(proto.aggregate.grouping_expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "country" } },
    ]);
    expect(proto.aggregate.group_type).toBe(1); // GROUP_TYPE_GROUPBY
    const aggExpr = proto.aggregate.aggregate_expressions[0];
    expect(aggExpr.alias.name).toEqual(["total"]);
    expect(aggExpr.alias.expr.unresolved_function.function_name).toBe("sum");
  });

  it("serializes orderBy() into sort directions and nulls ordering", () => {
    const df = session()
      .read.csv("/tmp/in.csv")
      .orderBy(col("a").descNullsLast());
    const proto = planOf(df);

    const order = proto.sort.order[0];
    expect(order.direction).toBe("SORT_DIRECTION_DESCENDING");
    expect(order.null_ordering).toBe("SORT_NULLS_LAST");
    expect(order.child).toEqual({ unresolved_attribute: { unparsed_identifier: "a" } });
  });

  it("serializes limit()", () => {
    const proto = planOf(session().read.csv("/x").limit(10));
    expect(proto.limit.limit).toBe(10);
  });

  it("serializes distinct() as deduplicate with all_columns_as_keys", () => {
    const proto = planOf(session().read.csv("/x").distinct());
    expect(proto.deduplicate.all_columns_as_keys).toBe(true);
  });

  it("serializes union() as a set_op of type union", () => {
    const left = session().read.csv("/a");
    const right = session().read.csv("/b");
    const proto = planOf(left.union(right));

    expect(proto.set_op.set_op_type).toBe(2); // SET_OP_TYPE_UNION
    expect(proto.set_op.is_all).toBe(true);
    expect(proto.set_op.left_input).toBeTruthy();
    expect(proto.set_op.right_input).toBeTruthy();
  });

  it("serializes unionByName() with by_name / allow_missing_columns flags", () => {
    const left = session().read.csv("/a");
    const right = session().read.csv("/b");
    const proto = planOf(left.unionByName(right, true));

    expect(proto.set_op.by_name).toBe(true);
    expect(proto.set_op.allow_missing_columns).toBe(true);
  });

  it("serializes intersect() as a set_op of type intersect (distinct by default)", () => {
    const left = session().read.csv("/a");
    const right = session().read.csv("/b");
    const proto = planOf(left.intersect(right));

    expect(proto.set_op.set_op_type).toBe(1); // SET_OP_TYPE_INTERSECT
    expect(proto.set_op.is_all).toBe(false);
    expect(proto.set_op.left_input).toBeTruthy();
    expect(proto.set_op.right_input).toBeTruthy();
  });

  it("serializes intersectAll() with is_all=true", () => {
    const left = session().read.csv("/a");
    const right = session().read.csv("/b");
    const proto = planOf(left.intersectAll(right));

    expect(proto.set_op.set_op_type).toBe(1); // SET_OP_TYPE_INTERSECT
    expect(proto.set_op.is_all).toBe(true);
  });

  it("serializes except() as a set_op of type except (distinct by default)", () => {
    const left = session().read.csv("/a");
    const right = session().read.csv("/b");
    const proto = planOf(left.except(right));

    expect(proto.set_op.set_op_type).toBe(3); // SET_OP_TYPE_EXCEPT
    expect(proto.set_op.is_all).toBe(false);
  });

  it("serializes exceptAll() with is_all=true", () => {
    const left = session().read.csv("/a");
    const right = session().read.csv("/b");
    const proto = planOf(left.exceptAll(right));

    expect(proto.set_op.set_op_type).toBe(3); // SET_OP_TYPE_EXCEPT
    expect(proto.set_op.is_all).toBe(true);
  });

  it("serializes repartition() with shuffle=true and coalesce with shuffle=false", () => {
    const repart = planOf(session().read.csv("/x").repartition(8));
    const coalesced = planOf(session().read.csv("/x").coalescePartitions(2));

    expect(repart.repartition).toMatchObject({ num_partitions: 8, shuffle: true });
    expect(coalesced.repartition).toMatchObject({ num_partitions: 2, shuffle: false });
  });

  it("serializes drop() with column_names", () => {
    const proto = planOf(session().read.csv("/x").drop("a", "b"));
    expect(proto.drop.column_names).toEqual(["a", "b"]);
  });

  it("serializes hint() with name and parameters", () => {
    const proto = planOf(session().read.csv("/x").hint("broadcast"));
    expect(proto.hint).toMatchObject({ name: "broadcast" });
  });

  it("serializes sample() with bounds", () => {
    const proto = planOf(session().read.csv("/x").sample(0.5, false, 42));
    expect(proto.sample).toMatchObject({
      lower_bound: 0,
      upper_bound: 0.5,
      with_replacement: false,
      seed: 42,
    });
  });

  it("serializes when().otherwise() as nested if() functions", () => {
    const df = session()
      .read.csv("/x")
      .withColumn("bucket", when(col("a").gt(10), "hi").otherwise("lo"));
    const proto = planOf(df);

    const expr = proto.with_columns.aliases[0].expr;
    expect(expr.unresolved_function.function_name).toBe("if");
    // condition, then, else
    expect(expr.unresolved_function.arguments).toHaveLength(3);
  });

  it("treats a bare string in eq() as a literal, not a column", () => {
    const proto = planOf(session().read.csv("/x").filter(col("name").eq("alice")));
    const args = proto.filter.condition.unresolved_function.arguments;
    expect(args[1]).toEqual({ literal: { string: "alice" } });
  });

  it("serializes sql() as a sql relation", () => {
    const proto = planOf(session().sql("SELECT 1"));
    expect(proto.sql.query).toBe("SELECT 1");
  });
});
