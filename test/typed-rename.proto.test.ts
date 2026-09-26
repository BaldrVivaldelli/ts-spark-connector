import { describe, expect, it } from "vitest";
import { SparkSession, schema } from "../src";

const People = schema({ id: "int", name: "string", age: "int" });
const sess = SparkSession.builder().getOrCreate();
const people = () => sess.read.readWith(People, "csv", "/tmp/people.csv");

describe("typed withColumnRenamed (runtime plan)", () => {
  it("serializes a rename via with_columns_renamed", () => {
    const proto = JSON.parse(people().withColumnRenamed("name", "fullName").toProtoJSON());
    expect(proto.with_columns_renamed.rename_columns_map).toEqual({ name: "fullName" });
  });

  it("lets you select the new name and preserves the column type", () => {
    const renamed = people().withColumnRenamed("age", "years");
    const proto = JSON.parse(renamed.filter(c => c.years.gt(18)).toProtoJSON());
    // `years` is still numeric (preserved from `age`), so gt(18) is a valid int literal.
    expect(proto.filter.condition.unresolved_function.arguments[1]).toEqual({
      literal: { integer: 18 },
    });
  });

  it("rejects rename targets that would create duplicate row keys", () => {
    const runtime = people() as unknown as {
      withColumnRenamed(from: string, to: string): unknown;
      withColumnsRenamed(mapping: Record<string, string>): unknown;
    };
    expect(() => runtime.withColumnRenamed("name", "id"))
      .toThrow(/duplicate column/i);
    expect(() => runtime.withColumnsRenamed({ id: "same", name: "same" }))
      .toThrow(/multiple columns/i);
  });
});

describe("typed withColumnRenamed enables collision-free joins", () => {
  it("renaming a colliding key before join keeps both columns distinct", () => {
    const A = schema({ id: "int", a: "string" });
    const B = schema({ id: "int", b: "string" });

    const left = sess.read.readWith(A, "csv", "/tmp/a.csv").withColumnRenamed("id", "a_id");
    const right = sess.read.readWith(B, "csv", "/tmp/b.csv");

    const joined = left.join(right, (l, r) => l.a_id.eq(r.id));
    // Both `a_id` and `id` exist and are selectable; no ambiguity.
    const proto = JSON.parse(joined.select("a_id", "id", "a", "b").toProtoJSON());
    expect(proto.project.expressions).toHaveLength(4);
  });
});

// ---------------------------------------------------------------------------
// Type-level tests. Each expect-error directive marks usage that must NOT
// compile; if any stopped erroring the test typecheck would fail.
// ---------------------------------------------------------------------------
describe("typed withColumnRenamed (compile-time safety)", () => {
  it("rejects renaming an unknown column and using the old name afterwards", () => {
    const df = people();
    const invalidUsage = () => {
      // @ts-expect-error - "height" is not a column of People
      df.withColumnRenamed("height", "h");

      const renamed = df.withColumnRenamed("name", "fullName");
      // @ts-expect-error - "name" no longer exists after the rename
      renamed.select("name");
    };

    expect(typeof invalidUsage).toBe("function");
  });

  it("accepts selecting the renamed column", () => {
    const renamed = people().withColumnRenamed("name", "fullName");
    renamed.select("id", "fullName", "age");
    expect(true).toBe(true);
  });
});
