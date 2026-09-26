import { describe, expect, it } from "vitest";
import { SparkSession } from "../src";

// Schema used across the prototype tests.
type People = { id: number; name: string; age: number; active: boolean };

const sess = SparkSession.builder().getOrCreate();
const people = () =>
  sess.read.options({ header: "true" }).csv("/tmp/people.csv").as<People>();

describe("TypedDataFrame (runtime plan)", () => {
  it("produces the same proto relation as the untyped reader", () => {
    const proto = JSON.parse(people().toProtoJSON());
    expect(proto.read.data_source).toMatchObject({
      format: "csv",
      paths: ["/tmp/people.csv"],
      options: { header: "true" },
    });
  });

  it("serializes select() as a project over the chosen columns", () => {
    const proto = JSON.parse(people().select("name", "age").toProtoJSON());
    expect(proto.project.expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "name" } },
      { unresolved_attribute: { unparsed_identifier: "age" } },
    ]);
  });

  it("serializes a typed filter as the expected comparison", () => {
    const proto = JSON.parse(people().filter(c => c.age.gt(18)).toProtoJSON());
    const fn = proto.filter.condition.unresolved_function;
    expect(fn.function_name).toBe(">");
    expect(fn.arguments[0]).toEqual({ unresolved_attribute: { unparsed_identifier: "age" } });
    expect(fn.arguments[1]).toEqual({ literal: { integer: 18 } });
  });

  it("composes conditions with and()", () => {
    const proto = JSON.parse(
      people().filter(c => c.age.gte(18).and(c.active.eq(true))).toProtoJSON()
    );
    expect(proto.filter.condition.unresolved_function.function_name).toBe("AND");
  });

  it("extends the schema with withColumn and allows selecting the new column", () => {
    const df = people().withColumn("isAdult", c => c.age); // number column
    const proto = JSON.parse(df.select("isAdult", "name").toProtoJSON());
    // select compiles only because `isAdult` is now part of the schema.
    expect(proto.project.expressions[0]).toEqual({
      unresolved_attribute: { unparsed_identifier: "isAdult" },
    });
  });
});

// ---------------------------------------------------------------------------
// Type-level tests. These assert that INVALID usage does NOT compile. If any of
// the @ts-expect-error lines stopped being an error, tsc would fail the build,
// which is exactly the safety net we are prototyping.
// ---------------------------------------------------------------------------
describe("TypedDataFrame (compile-time safety)", () => {
  it("rejects unknown columns and type-mismatched comparisons at compile time", () => {
    const df = people();

    // selecting a column that doesn't exist
    // @ts-expect-error - "height" is not a key of People
    df.select("height");

    // comparing a numeric column against a string literal
    // @ts-expect-error - age is number, "old" is string
    df.filter(c => c.age.gt("old"));

    // accessing a column that doesn't exist in the predicate
    // @ts-expect-error - "salary" is not a column of People
    df.filter(c => c.salary.gt(10));

    // after select, dropped columns are no longer in scope
    const projected = df.select("name");
    // @ts-expect-error - "age" was dropped by select("name")
    projected.select("age");

    expect(true).toBe(true);
  });

  it("accepts valid usage (sanity for the negative tests above)", () => {
    const df = people();
    df.select("name", "age");
    df.filter(c => c.age.gt(18));
    df.filter(c => c.name.eq("alice"));
    df.filter(c => c.active.eq(true));
    expect(true).toBe(true);
  });
});
