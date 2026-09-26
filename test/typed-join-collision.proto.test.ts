import { describe, expect, it } from "vitest";
import { SparkSession, schema } from "../src";

// Both sides declare an `id` column, but with DIFFERENT types (int vs string).
// This exercises the documented name-collision behavior of Joined<L,R>.
const A = schema({ id: "int", a: "string" });
const B = schema({ id: "string", b: "int" });
const C = schema({ id: "int", c: "string" });

const sess = SparkSession.builder().getOrCreate();
const dfA = () => sess.read.readWith(A, "csv", "/tmp/a.csv");
const dfB = () => sess.read.readWith(B, "csv", "/tmp/b.csv");
const dfC = () => sess.read.readWith(C, "csv", "/tmp/c.csv");

describe("typed join - safe name collisions", () => {
  it("marks a colliding column ambiguous while keeping disjoint columns usable", () => {
    const joined = dfA().join(dfB(), (l, r) => l.a.eq(r.id)); // both string: valid condition

    // `id` belongs to both sides, so it cannot be used without resolving the
    // collision explicitly (for example by renaming before the join).
    // @ts-expect-error - merged `id` is AmbiguousColumn<"id">
    joined.filter(c => c.id.gt(1));
    // @ts-expect-error - ambiguous columns cannot be projected either
    joined.select("id");

    const proto = JSON.parse(joined.filter(c => c.b.gt(1)).select("a", "b").toProtoJSON());
    expect(proto.project.expressions).toHaveLength(2);
  });

  it("qualifies equal-name predicate columns with their relation plan ids", () => {
    const proto = JSON.parse(dfA().join(dfC(), (l, r) => l.id.eq(r.id)).toProtoJSON());
    const leftPlanId = proto.join.left.common.plan_id;
    const rightPlanId = proto.join.right.common.plan_id;
    const [left, right] = proto.join.join_condition.unresolved_function.arguments;

    expect(leftPlanId).not.toBe(rightPlanId);
    expect(left.unresolved_attribute).toEqual({ unparsed_identifier: "id", plan_id: leftPlanId });
    expect(right.unresolved_attribute).toEqual({ unparsed_identifier: "id", plan_id: rightPlanId });
  });
});
