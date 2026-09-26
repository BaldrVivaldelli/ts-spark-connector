import { describe, expect, it } from "vitest";
import { SparkSession, schema } from "../src";

const People = schema({ id: "int", name: "string" });
const Purchases = schema({ user_id: "int", product: "string", amount: "double" });

const sess = SparkSession.builder().getOrCreate();
const people = () => sess.read.readWith(People, "csv", "/tmp/people.csv");
const purchases = () => sess.read.readWith(Purchases, "csv", "/tmp/purchases.csv");

describe("typed join (runtime plan)", () => {
  it("serializes a join with the mapped proto join type and condition", () => {
    const df = people().join(purchases(), (l, r) => l.id.eq(r.user_id), "LEFT");
    const proto = JSON.parse(df.toProtoJSON());

    expect(proto.join.join_type).toBe(3); // JOIN_TYPE_LEFT_OUTER
    expect(proto.join.left).toBeTruthy();
    expect(proto.join.right).toBeTruthy();
    const leftPlanId = proto.join.left.common.plan_id;
    const rightPlanId = proto.join.right.common.plan_id;
    expect(leftPlanId).not.toBe(rightPlanId);
    const fn = proto.join.join_condition.unresolved_function;
    expect(fn.function_name).toBe("=");
    expect(fn.arguments[0]).toEqual({
      unresolved_attribute: { unparsed_identifier: "id", plan_id: leftPlanId },
    });
    expect(fn.arguments[1]).toEqual({
      unresolved_attribute: { unparsed_identifier: "user_id", plan_id: rightPlanId },
    });
  });

  it("defaults to INNER when no join type is given", () => {
    const df = people().join(purchases(), (l, r) => l.id.eq(r.user_id));
    const proto = JSON.parse(df.toProtoJSON());
    expect(proto.join.join_type).toBe(1); // JOIN_TYPE_INNER
  });

  it("merges both schemas so columns from each side are selectable afterwards", () => {
    const joined = people().join(purchases(), (l, r) => l.id.eq(r.user_id));
    // `name` comes from the left, `product`/`amount` from the right.
    const proto = JSON.parse(joined.select("name", "product", "amount").toProtoJSON());
    expect(proto.project.expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "name" } },
      { unresolved_attribute: { unparsed_identifier: "product" } },
      { unresolved_attribute: { unparsed_identifier: "amount" } },
    ]);
  });

  it("supports composed conditions across both sides", () => {
    const df = people().join(
      purchases(),
      (l, r) => l.id.eq(r.user_id).and(r.amount.gt(100))
    );
    const proto = JSON.parse(df.toProtoJSON());
    expect(proto.join.join_condition.unresolved_function.function_name).toBe("AND");
  });
});

// ---------------------------------------------------------------------------
// Type-level tests. Each expect-error directive marks usage that must NOT
// compile; if any stopped erroring the test typecheck would fail.
// ---------------------------------------------------------------------------
describe("typed join (compile-time safety)", () => {
  it("rejects invalid column references and merged-schema mistakes", () => {
    const l = people();
    const r = purchases();

    // right-side column referenced through the left accessor
    // @ts-expect-error - `user_id` is not a column of People (left side)
    l.join(r, (left, right) => left.user_id.eq(right.user_id));

    // comparing columns of different types
    // @ts-expect-error - id is int, product is string
    l.join(r, (left, right) => left.id.eq(right.product));

    // selecting a column that exists on neither side after the join
    const joined = l.join(r, (left, right) => left.id.eq(right.user_id));
    // @ts-expect-error - `missing` is in neither People nor Purchases
    joined.select("missing");

    expect(true).toBe(true);
  });

  it("accepts valid cross-side usage", () => {
    const joined = people().join(purchases(), (l, r) => l.id.eq(r.user_id));
    joined.select("id", "name", "user_id", "product", "amount");
    joined.filter(c => c.amount.gt(50));
    expect(true).toBe(true);
  });
});
