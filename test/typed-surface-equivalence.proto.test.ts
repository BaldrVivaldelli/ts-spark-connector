import { describe, expect, it } from "vitest";
import { SparkSession, col } from "../src";

type Person = { id: number; name: string; age: number; amount: number };
type Purchase = { user_id: number; product: string };

const session = SparkSession.builder().getOrCreate();
const laxPeople = () => session.read.csv("/tmp/people.csv");
const people = () => laxPeople().as<Person>();
const laxPurchases = () => session.read.csv("/tmp/purchases.csv");
const purchases = () => laxPurchases().as<Purchase>();

const plan = (df: { toProtoJSON(): string }) => JSON.parse(df.toProtoJSON());
const withoutGeneratedIds = (value: unknown) =>
  JSON.parse(JSON.stringify(value, (key, nested) => key === "id" ? undefined : nested));
const withoutRelationQualification = (value: unknown) =>
  JSON.parse(JSON.stringify(value, (key, nested) => {
    if (key === "plan_id") return undefined;
    if (key === "common" && nested && typeof nested === "object"
      && Object.keys(nested).every(name => name === "plan_id")) return undefined;
    return nested;
  }));

describe("unified typed surface keeps the lax runtime plan", () => {
  it(".as<T>() is a type-only assertion", () => {
    expect(plan(people())).toEqual(plan(laxPeople()));
    expect(plan(people().as<Person>())).toEqual(plan(people()));
  });

  it("select/filter/withColumn/rename/drop are plan-equivalent", () => {
    expect(plan(people().select("name", "age"))).toEqual(
      plan(laxPeople().select("name", "age"))
    );
    expect(plan(people().filter(c => c.age.gt(18)))).toEqual(
      plan(laxPeople().filter(col("age").gt(18)))
    );
    expect(plan(people().withColumn("adultAge", c => c.age.plus(1)))).toEqual(
      plan(laxPeople().withColumn("adultAge", {
        build: EX => EX.bin("+", EX.col("age"), EX.lit(1)),
      }))
    );
    expect(plan(people().withColumnRenamed("name", "fullName"))).toEqual(
      plan(laxPeople().withColumnRenamed("name", "fullName"))
    );
    expect(plan(people().drop("amount"))).toEqual(plan(laxPeople().drop("amount")));
  });

  it("keeps interpreter parametricity beyond the proto compiler", () => {
    const typed = people().filter(c => c.age.gte(21)).select("name", "age");
    const lax = laxPeople().filter(col("age").gte(21)).select("name", "age");

    expect(withoutGeneratedIds(JSON.parse(typed.toClientASTJSON()))).toEqual(
      withoutGeneratedIds(JSON.parse(lax.toClientASTJSON()))
    );
    expect(withoutGeneratedIds(JSON.parse(typed.toSparkLogicalPlanJSON()))).toEqual(
      withoutGeneratedIds(JSON.parse(lax.toSparkLogicalPlanJSON()))
    );
  });

  it("order/group/join and set operations are plan-equivalent", () => {
    expect(plan(people().orderBy(c => c.age.desc("nullsLast")))).toEqual(
      plan(laxPeople().orderBy(col("age").descNullsLast()))
    );

    expect(plan(people().groupBy("name").agg(a => [a.sum("amount").as("total")]))).toEqual(
      plan(laxPeople().groupBy("name").agg({ total: "sum(amount)" }))
    );

    expect(withoutRelationQualification(
      plan(people().join(purchases(), (l, r) => l.id.eq(r.user_id), "LEFT"))
    )).toEqual(
      withoutRelationQualification(
        plan(laxPeople().join(laxPurchases(), col("id").eq(col("user_id")), "LEFT"))
      )
    );

    expect(plan(people().union(people()))).toEqual(plan(laxPeople().union(laxPeople())));
    expect(plan(people().intersect(people()))).toEqual(plan(laxPeople().intersect(laxPeople())));
    expect(plan(people().except(people()))).toEqual(plan(laxPeople().except(laxPeople())));
  });
});
