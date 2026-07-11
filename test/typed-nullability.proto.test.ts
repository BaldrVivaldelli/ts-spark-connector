import { describe, expect, it } from "vitest";
import { SparkSession, schema } from "../src";

// `age` is nullable (declared with the `?` suffix); `id`/`name` are not.
const People = schema({
  id: "int",
  name: "string",
  age: "int?",
  score: "double?",
});

const sess = SparkSession.builder().getOrCreate();
const people = () => sess.read.readWith(People, "csv", "/tmp/people.csv");

describe("nullability — DDL (runtime)", () => {
  it("emits NOT NULL for non-nullable tokens and leaves nullable ones open", () => {
    expect(People.toDDL()).toBe(
      "id INT NOT NULL, name STRING NOT NULL, age INT, score DOUBLE"
    );
  });
});

describe("nullability — expressions (runtime plan)", () => {
  it("isNull / isNotNull serialize to the right functions", () => {
    const a = JSON.parse(people().filter(c => c.age.isNull()).toProtoJSON());
    const b = JSON.parse(people().filter(c => c.age.isNotNull()).toProtoJSON());
    expect(a.filter.condition.unresolved_function.function_name).toBe("isnull");
    expect(b.filter.condition.unresolved_function.function_name).toBe("isnotnull");
  });

  it("coalesce serializes and yields a non-null column", () => {
    const df = people().withColumn("age2", c => c.age.coalesce(0));
    const proto = JSON.parse(df.toProtoJSON());
    expect(proto.with_columns.aliases[0].expr.unresolved_function.function_name).toBe("coalesce");
  });

  it("numeric arithmetic serializes as a binary op", () => {
    const df = people().withColumn("bonus", c => c.score.times(2));
    const proto = JSON.parse(df.toProtoJSON());
    expect(proto.with_columns.aliases[0].expr.unresolved_function.function_name).toBe("*");
  });
});

// ---------------------------------------------------------------------------
// Type-level tests for nullability. Invalid usage must NOT compile.
// ---------------------------------------------------------------------------
describe("nullability — compile-time safety", () => {
  it("propagates and removes null through the type system", () => {
    // collect() rows reflect nullability: age is number | null.
    const typeCheck = async () => {
      const rows = await people().collect();
      const name: string = rows[0].name;       // non-null
      const age: number | null = rows[0].age;   // nullable

      // @ts-expect-error - age may be null, not assignable to plain number
      const ageNum: number = rows[0].age;

      // coalesce removes the null: age2 becomes a non-null number column
      const df2 = people().withColumn("age2", c => c.age.coalesce(0));
      const rows2 = await df2.collect();
      const age2: number = rows2[0].age2;       // OK: non-null after coalesce

      return { name, age, ageNum, age2 };
    };
    expect(typeof typeCheck).toBe("function");
  });

  it("sum/avg/min/max are nullable, count is not", () => {
    const typeCheck = async () => {
      const g = people()
        .groupBy("name")
        .agg(a => [a.count().as("n"), a.avg(c => c.score).as("avg_score")]);
      const rows = await g.collect();

      const n: bigint = rows[0].n;                  // COUNT is BIGINT and never null
      const avg: number | null = rows[0].avg_score; // avg can be null

      // @ts-expect-error - avg_score may be null
      const avgNum: number = rows[0].avg_score;

      return { n, avg, avgNum };
    };
    expect(typeof typeCheck).toBe("function");
  });

  it("comparisons still reject the wrong scalar type on nullable columns", () => {
    const df = people();
    // @ts-expect-error - age is numeric, compared against a string
    df.filter(c => c.age.gt("old"));
    expect(true).toBe(true);
  });
});
