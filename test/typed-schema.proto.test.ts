import { describe, expect, it } from "vitest";
import { SparkSession, schema } from "../src";

// One declaration that drives BOTH the compile-time type and the runtime DDL.
const People = schema({
  id: "int",
  name: "string",
  age: "int",
  active: "boolean",
  signup: "timestamp",
});

const sess = SparkSession.builder().getOrCreate();
const people = () => sess.read.readWith(People, "csv", "/tmp/people.csv", { header: "true" });

describe("schema() -> DDL (runtime)", () => {
  it("serializes tokens to a Spark DDL string with NOT NULL for non-nullable fields", () => {
    expect(People.toDDL()).toBe(
      "id INT NOT NULL, name STRING NOT NULL, age INT NOT NULL, active BOOLEAN NOT NULL, signup TIMESTAMP NOT NULL"
    );
  });

  it("leaves nullable (`?`) fields without NOT NULL", () => {
    const s = schema({ id: "int", note: "string?" });
    expect(s.toDDL()).toBe("id INT NOT NULL, note STRING");
  });

  it("backtick-quotes identifiers that need it", () => {
    const weird = schema({ "user id": "string", normal: "int" });
    expect(weird.toDDL()).toBe("`user id` STRING NOT NULL, normal INT NOT NULL");
  });
});

describe("readWith() injects the schema into the proto (no server inference)", () => {
  it("sets data_source.schema to the DDL string", () => {
    const proto = JSON.parse(people().toProtoJSON());
    expect(proto.read.data_source).toMatchObject({
      format: "csv",
      paths: ["/tmp/people.csv"],
      options: { header: "true" },
      schema: "id INT NOT NULL, name STRING NOT NULL, age INT NOT NULL, active BOOLEAN NOT NULL, signup TIMESTAMP NOT NULL",
    });
  });

  it("keeps the inferred schema usable downstream (select compiles + serializes)", () => {
    const proto = JSON.parse(people().select("name", "age").toProtoJSON());
    expect(proto.project.expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "name" } },
      { unresolved_attribute: { unparsed_identifier: "age" } },
    ]);
  });

  it("infers numeric vs string column types for comparisons", () => {
    const proto = JSON.parse(people().filter(c => c.age.gt(18)).toProtoJSON());
    expect(proto.filter.condition.unresolved_function.arguments[1]).toEqual({
      literal: { integer: 18 },
    });
  });
});

// ---------------------------------------------------------------------------
// Type-level tests: the inferred type must reject invalid usage. If any of the
// expect-error directives below stopped erroring, the test typecheck would fail.
// ---------------------------------------------------------------------------
describe("readWith() inferred schema (compile-time safety)", () => {
  it("rejects unknown columns and wrong comparison types", () => {
    const df = people();

    // @ts-expect-error - "height" was not declared in the schema
    df.select("height");

    // @ts-expect-error - age is int (number), compared against a string
    df.filter(c => c.age.gt("old"));

    // @ts-expect-error - "active" is boolean, gt() expects boolean | TypedColumn<boolean>
    df.filter(c => c.active.gt(1));

    expect(true).toBe(true);
  });

  it("accepts valid usage against the inferred schema", () => {
    const df = people();
    df.select("id", "name", "age", "active", "signup");
    df.filter(c => c.age.gte(21).and(c.active.eq(true)));
    df.filter(c => c.name.eq("alice"));
    expect(true).toBe(true);
  });

  it("types the result of collect() as the schema rows", () => {
    // Type-only assertion: collect() must resolve to the inferred row type.
    // We never call it here (no server); we only check the static type.
    const typeCheck = async () => {
      const rows = await people().select("name", "age").collect();
      // rows: { name: string; age: number }[]
      const name: string = rows[0].name;
      const age: number = rows[0].age;
      // @ts-expect-error - `id` was dropped by select("name", "age")
      const id = rows[0].id;
      return { name, age, id };
    };
    expect(typeof typeCheck).toBe("function");
  });
});
