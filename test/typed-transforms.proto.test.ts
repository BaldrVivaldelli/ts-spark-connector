import { describe, expect, it } from "vitest";
import { SparkSession, schema } from "../src";

const Purchases = schema({
  user_id: "int",
  product: "string",
  amount: "double",
  country: "string",
});

const sess = SparkSession.builder().getOrCreate();
const purchases = () => sess.read.readWith(Purchases, "csv", "/tmp/purchases.csv");

describe("Phase 2 transforms (runtime plan)", () => {
  it("orderBy serializes sort direction and nulls ordering", () => {
    const proto = JSON.parse(purchases().orderBy(c => c.amount.desc("nullsLast")).toProtoJSON());
    const order = proto.sort.order[0];
    expect(order.direction).toBe("SORT_DIRECTION_DESCENDING");
    expect(order.null_ordering).toBe("SORT_NULLS_LAST");
    expect(order.child).toEqual({ unresolved_attribute: { unparsed_identifier: "amount" } });
  });

  it("orderBy defaults to ascending for a bare column", () => {
    const proto = JSON.parse(purchases().orderBy(c => c.product).toProtoJSON());
    expect(proto.sort.order[0].direction).toBe("SORT_DIRECTION_ASCENDING");
    expect(proto.sort.order[0].null_ordering).toBe("SORT_NULLS_FIRST");
  });

  it("limit serializes", () => {
    const proto = JSON.parse(purchases().limit(5).toProtoJSON());
    expect(proto.limit.limit).toBe(5);
  });

  it("distinct serializes as deduplicate", () => {
    const proto = JSON.parse(purchases().distinct().toProtoJSON());
    expect(proto.deduplicate.all_columns_as_keys).toBe(true);
  });

  it("drop serializes column_names and removes them from the schema", () => {
    const dropped = purchases().drop("country");
    const proto = JSON.parse(dropped.toProtoJSON());
    expect(proto.drop.column_names).toEqual(["country"]);
    // `country` is gone; selecting the rest still compiles.
    JSON.parse(dropped.select("user_id", "product", "amount").toProtoJSON());
  });

  it("union/intersect/except serialize as set_op with the right type", () => {
    const a = purchases();
    const b = purchases();
    expect(JSON.parse(a.union(b).toProtoJSON()).set_op.set_op_type).toBe(2);
    expect(JSON.parse(a.intersect(b).toProtoJSON()).set_op.set_op_type).toBe(1);
    expect(JSON.parse(a.except(b).toProtoJSON()).set_op.set_op_type).toBe(3);
  });
});

describe("Phase 2 groupBy().agg() (runtime plan)", () => {
  it("serializes grouping keys and aggregate expressions", () => {
    const df = purchases()
      .groupBy("country")
      .agg(a => [a.count().as("n"), a.sum(c => c.amount).as("total")]);
    const proto = JSON.parse(df.toProtoJSON());

    expect(proto.aggregate.grouping_expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "country" } },
    ]);
    const aliases = proto.aggregate.aggregate_expressions.map((e: any) => e.alias.name[0]);
    expect(aliases).toEqual(["n", "total"]);
    const fns = proto.aggregate.aggregate_expressions.map(
      (e: any) => e.alias.expr.unresolved_function.function_name
    );
    expect(fns).toEqual(["count", "sum"]);
  });

  it("lets you select grouping keys and aggregated outputs downstream", () => {
    const df = purchases()
      .groupBy("country")
      .agg(a => [a.avg(c => c.amount).as("avg_amount")]);
    const proto = JSON.parse(df.select("country", "avg_amount").toProtoJSON());
    expect(proto.project.expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "country" } },
      { unresolved_attribute: { unparsed_identifier: "avg_amount" } },
    ]);
  });

  it("rejects empty, duplicate and grouping-colliding aggregate aliases", () => {
    expect(() => purchases()
      .groupBy("country")
      .agg(a => [a.count().as(" ")])
      .toProtoJSON()).toThrow(/non-empty/i);

    expect(() => purchases()
      .groupBy("country")
      .agg(a => [a.count().as("same"), a.avg(c => c.amount).as("same")] as any)
      .toProtoJSON()).toThrow(/duplicate column/i);

    expect(() => purchases()
      .groupBy("country")
      .agg(a => [a.count().as("country")])
      .toProtoJSON()).toThrow(/collides with a grouping column/i);
  });
});

// ---------------------------------------------------------------------------
// Type-level tests: invalid usage must NOT compile.
// ---------------------------------------------------------------------------
describe("Phase 2 (compile-time safety)", () => {
  it("rejects invalid columns and post-aggregation schema mistakes", () => {
    const df = purchases();

    // @ts-expect-error - "missing" is not a column
    df.drop("missing");

    // @ts-expect-error - "missing" is not a column to order by
    df.orderBy(c => c.missing.asc());

    // @ts-expect-error - "missing" is not a grouping column
    df.groupBy("missing");

    const grouped = df.groupBy("country").agg(a => [a.count().as("n")]);
    // result schema is { country, n }; `product` was not carried through
    // @ts-expect-error - "product" is not in the aggregated schema
    grouped.select("product");

    const invalidAliases = () => {
      // @ts-expect-error - aggregation aliases must not be empty
      df.groupBy("country").agg(a => [a.count().as("")]);
      // @ts-expect-error - aggregation aliases must be unique
      df.groupBy("country").agg(a => [a.count().as("x"), a.sum("amount").as("x")]);
    };

    expect(typeof invalidAliases).toBe("function");
  });

  it("accepts valid grouped/aggregated usage", () => {
    const grouped = purchases()
      .groupBy("country")
      .agg(a => [a.count().as("n"), a.max(c => c.amount).as("max_amount")]);
    grouped.select("country", "n", "max_amount");
    grouped.orderBy(c => c.n.desc());
    expect(true).toBe(true);
  });
});
