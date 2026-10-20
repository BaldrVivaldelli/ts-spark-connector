import { describe, expect, it } from "vitest";
import {
  GroupedDataFrameTF,
  Observation,
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
type GroupSchemaOf<T> = T extends GroupedDataFrameTF<
  infer S,
  infer _K,
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
const Quotes = schema({ rid: "long", q: "string" });
const people = session.read.readWith(People, "parquet", "/tmp/people.parquet");
const quotes = session.read.readWith(Quotes, "parquet", "/tmp/quotes.parquet");

type PeopleRow = { id: bigint; name: string | null; age: number | null };

// Partition-local sorting, watermark deduplication, metadata and observation keep the schema.
const kept = people
  .sortWithinPartitions("id")
  .dropDuplicatesWithinWatermark("id")
  .withMetadata("id", { comment: "pk" })
  .observe(new Observation("stats"), col("id").alias("ids"));
type _Kept = Expect<Equal<SchemaOf<typeof kept>, PeopleRow>>;

// Grouping variants and pivots are untyped: their key columns become nullable
// or depend on the data.
type _Rollup = Expect<Equal<GroupSchemaOf<ReturnType<typeof people.rollup>>, UnknownSchema>>;
type _Cube = Expect<Equal<GroupSchemaOf<ReturnType<typeof people.cube>>, UnknownSchema>>;
const pivoted = people.groupBy("name").pivot("age", [30]);
type _Pivot = Expect<Equal<GroupSchemaOf<typeof pivoted>, UnknownSchema>>;
type _Sets = Expect<Equal<GroupSchemaOf<ReturnType<typeof people.groupingSets>>, UnknownSchema>>;

// Joins that may reference the other side are untyped.
const lateral = people.lateralJoin(quotes);
type _Lateral = Expect<Equal<SchemaOf<typeof lateral>, UnknownSchema>>;
const asOf = people.asOfJoin(quotes, "id", "rid");
type _AsOf = Expect<Equal<SchemaOf<typeof asOf>, UnknownSchema>>;

// to(schema) types the result by the declaration; a DDL string does not.
const reconciled = people.to(schema({ name: "string?", id: "int" }));
type _Reconciled = Expect<Equal<SchemaOf<typeof reconciled>, { name: string | null; id: number }>>;
const ddl = people.to("name STRING");
type _DdlOnly = Expect<Equal<SchemaOf<typeof ddl>, UnknownSchema>>;

// toJSON is one string column; checkpoints keep the schema.
type _Json = Expect<Equal<SchemaOf<ReturnType<typeof people.toJSON>>, { value: string }>>;
type _Checkpoint = Expect<Equal<Awaited<ReturnType<typeof people.checkpoint>>, typeof people>>;

describe("parity 2 typing", () => {
  it("compiles the type-level assertions above", () => {
    expect(kept).toBeDefined();
    expect(pivoted).toBeDefined();
    expect(lateral).toBeDefined();
    expect(asOf).toBeDefined();
    expect(reconciled).toBeDefined();
    expect(ddl).toBeDefined();
  });
});
