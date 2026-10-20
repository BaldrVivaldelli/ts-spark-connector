import { describe, expect, it } from "vitest";
import {
  ReadChainedDataFrame,
  SparkSession,
  UnknownSchema,
  col,
  schema,
} from "../../src";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends
  (<T>() => T extends B ? 1 : 2) ? true : false;
type Expect<T extends true> = T;
type SchemaOf<T> = T extends ReadChainedDataFrame<
  infer S,
  infer _R,
  infer _E,
  infer _G,
  infer _CDF,
  infer _CEX
>
  ? S
  : never;

const session = SparkSession.builder().getOrCreate();
const Sales = schema({ region: "string", amount: "double", note: "string?" });
const sales = session.read.readWith(Sales, "csv", "/tmp/sales.csv");

const summarized = sales.groupBy("region").agg(a => [
  a.first("amount").as("firstAmount"),
  a.last("note", true).as("lastNote"),
  a.collectList("note").as("notes"),
  a.collectSet("amount").as("amounts"),
  a.stddev("amount").as("sd"),
  a.variance("amount").as("var"),
  a.approxCountDistinct("region").as("regions"),
  a.countIf(c => c.amount.gt(100)).as("big"),
]);

// first/last are nullable even over a non-null column (empty group); the
// collected arrays never hold nulls and are never null themselves.
type _Summarized = Expect<Equal<SchemaOf<typeof summarized>, {
  region: string;
  firstAmount: number | null;
  lastNote: string | null;
  notes: string[];
  amounts: number[];
  sd: number | null;
  var: number | null;
  regions: bigint;
  big: bigint;
}>>;

// SQL-string filters keep the schema; selectExpr cannot know it.
const filtered = sales.filter("amount > 100").where(col("region").eq("AR")).where(c => c.amount.lt(5));
type _Filtered = Expect<Equal<SchemaOf<typeof filtered>, SchemaOf<typeof sales>>>;
const projected = sales.selectExpr("region", "amount * 2 AS doubled");
type _Projected = Expect<Equal<SchemaOf<typeof projected>, UnknownSchema>>;

// @ts-expect-error stddev only accepts numeric columns.
sales.groupBy("region").agg(a => [a.stddev("note").as("bad")]);

describe("typed aggregates and SQL-string DataFrame methods", () => {
  it("are checked at compile time", () => {
    expect(summarized).toBeInstanceOf(ReadChainedDataFrame);
    expect(filtered).toBeInstanceOf(ReadChainedDataFrame);
    expect(projected).toBeInstanceOf(ReadChainedDataFrame);
  });
});
