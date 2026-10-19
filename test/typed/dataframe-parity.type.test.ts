import { describe, expect, it } from "vitest";
import {
  ReadChainedDataFrame,
  RowOf,
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
const People = schema({ id: "long", name: "string?", age: "int?" });
const Orders = schema({ order_id: "long", amount: "double" });
const people = session.read.readWith(People, "parquet", "/tmp/people.parquet");
const orders = session.read.readWith(Orders, "parquet", "/tmp/orders.parquet");

type PeopleRow = { id: bigint; name: string | null; age: number | null };

// Null handling, aliasing, offsets, sampling and partitioning keep the schema.
const kept = people
  .fillna(0, ["age"])
  .dropna("any", undefined, "name")
  .replace("n/a", null, "name")
  .alias("p")
  .offset(1)
  .sampleBy("age", { a: 0.5 })
  .repartition(4, "id")
  .repartitionByRange(col("age").desc());
type _Kept = Expect<Equal<SchemaOf<typeof kept>, PeopleRow>>;
type _NaKept = Expect<Equal<SchemaOf<ReturnType<typeof people.na.fill>>, PeopleRow>>;

// Shape-changing relations fall back to the lax surface.
type _Crosstab = Expect<Equal<SchemaOf<ReturnType<typeof people.crosstab>>, UnknownSchema>>;
type _Unpivot = Expect<Equal<SchemaOf<ReturnType<typeof people.unpivot>>, UnknownSchema>>;
type _ToDF = Expect<Equal<SchemaOf<ReturnType<typeof people.toDF>>, UnknownSchema>>;
type _Transpose = Expect<Equal<SchemaOf<ReturnType<typeof people.transpose>>, UnknownSchema>>;

// Typed withColumns adds or replaces columns with the builder's types.
const widened = people.withColumns(c => ({ next_age: c.age.plus(1), id: c.name }));
type _Widened = Expect<Equal<SchemaOf<typeof widened>, {
  name: string | null;
  age: number | null;
  next_age: number | null;
  id: string | null;
}>>;

// A cross join combines both sides; an untyped side makes the result untyped.
const crossed = people.crossJoin(orders);
type _Crossed = Expect<Equal<SchemaOf<typeof crossed>, PeopleRow & { order_id: bigint; amount: number } extends infer T
  ? { [K in keyof T]: T[K] }
  : never>>;
const laxCrossed = people.crossJoin(session.read.parquet("/tmp/lax.parquet"));
type _LaxCrossed = Expect<Equal<SchemaOf<typeof laxCrossed>, UnknownSchema>>;

// Row actions return the typed row shape.
type _First = Expect<Equal<Awaited<ReturnType<typeof people.first>>, RowOf<PeopleRow> | undefined>>;
type _Tail = Expect<Equal<Awaited<ReturnType<typeof people.tail>>, Array<RowOf<PeopleRow>>>>;
type _Count = Expect<Equal<Awaited<ReturnType<typeof people.count>>, bigint>>;

function typeLevelGuards() {
  // @ts-expect-error fillna subsets are typed column names
  people.fillna(0, "missing");
  // @ts-expect-error dropna subsets are typed column names
  people.dropna("any", undefined, ["missing"]);
  // @ts-expect-error crosstab columns are typed column names
  people.crosstab("id", "missing");
  // @ts-expect-error approxQuantile columns are typed column names
  void people.approxQuantile("missing", [0.5], 0);
  // @ts-expect-error alias keeps the schema, so unknown columns stay unknown
  people.alias("p").select("missing");
}

describe("DataFrame parity typing", () => {
  it("is checked by the TypeScript compiler", () => {
    expect(typeof typeLevelGuards).toBe("function");
  });
});
