import { describe, expect, it } from "vitest";
import {
  AmbiguousColumn,
  ReadChainedDataFrame,
  SparkSession,
  UnknownSchema,
  col,
  schema,
} from "../../src";
import type { JoinTypeInput } from "../../src/engine/sparkConnectEnums";

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
const Left = schema({ id: "int", name: "string" });
const Right = schema({ user_id: "int", amount: "double" });
const Colliding = schema({ id: "string", flag: "boolean" });

const left = session.read.readWith(Left, "csv", "/tmp/left.csv");
const right = session.read.readWith(Right, "csv", "/tmp/right.csv");
const colliding = session.read.readWith(Colliding, "csv", "/tmp/colliding.csv");

const projected = left.select("name");
type _Projected = Expect<Equal<SchemaOf<typeof projected>, { name: string }>>;

const leftJoin = left.join(right, (l, r) => l.id.eq(r.user_id), "LEFT");
type _LeftJoin = Expect<Equal<SchemaOf<typeof leftJoin>, {
  id: number;
  name: string;
  user_id: number | null;
  amount: number | null;
}>>;

const rightJoin = left.join(right, (l, r) => l.id.eq(r.user_id), "RIGHT");
type _RightJoin = Expect<Equal<SchemaOf<typeof rightJoin>, {
  id: number | null;
  name: string | null;
  user_id: number;
  amount: number;
}>>;

const fullJoin = left.join(right, (l, r) => l.id.eq(r.user_id), "FULL");
type _FullJoin = Expect<Equal<SchemaOf<typeof fullJoin>, {
  id: number | null;
  name: string | null;
  user_id: number | null;
  amount: number | null;
}>>;

const dynamicJoinType = (Math.random() > 0.5 ? "LEFT" : "INNER") as JoinTypeInput;
const dynamicJoin = left.join(right, (l, r) => l.id.eq(r.user_id), dynamicJoinType);
type _DynamicJoinLeft = Expect<
  Equal<SchemaOf<typeof dynamicJoin>["id"], number | null>
>;
// @ts-expect-error - a dynamic join may be LEFT_SEMI/LEFT_ANTI and omit right columns
type _DynamicJoinRight = SchemaOf<typeof dynamicJoin>["amount"];

const semiJoin = left.join(right, (l, r) => l.id.eq(r.user_id), "LEFT_SEMI");
type _SemiJoin = Expect<Equal<SchemaOf<typeof semiJoin>, { id: number; name: string }>>;

const collision = left.join(colliding, (l, r) => l.name.eq(r.id));
type _Collision = Expect<Equal<SchemaOf<typeof collision>["id"], AmbiguousColumn<"id">>>;

const renamedMany = left.withColumnsRenamed({ id: "personId", name: "fullName" });
type _RenamedMany = Expect<Equal<SchemaOf<typeof renamedMany>, {
  personId: number;
  fullName: string;
}>>;

const described = left.describe(["id", "name"] as const);
type _DescribeSummary = Expect<Equal<SchemaOf<typeof described>["summary"], string>>;
type _DescribeId = Expect<Equal<SchemaOf<typeof described>["id"], string | null>>;

const laxColumn = left.withColumn("idCopy", col("id"));
const laxCoalesce = left.coalesce("first", "name", "id");
const dynamicSql = left.sql("SELECT 1");
const missingUnion = left.unionByName(right, true);
type _LaxColumnUnknown = Expect<Equal<SchemaOf<typeof laxColumn>, UnknownSchema>>;
type _LaxCoalesceUnknown = Expect<Equal<SchemaOf<typeof laxCoalesce>, UnknownSchema>>;
type _SqlUnknown = Expect<Equal<SchemaOf<typeof dynamicSql>, UnknownSchema>>;
type _MissingUnionUnknown = Expect<Equal<SchemaOf<typeof missingUnion>, UnknownSchema>>;

function _invalidTypedUsage(): void {
  // @ts-expect-error - projected schema no longer contains id
  projected.select("id");
  // @ts-expect-error - a collided name is not a usable column
  collision.select("id");
  // @ts-expect-error - set operations require exactly the same schema
  left.union(right);
  // @ts-expect-error - right-side columns cannot be accessed through the left accessor
  left.join(right, (l, r) => l.user_id.eq(r.user_id));
  // @ts-expect-error - bulk rename keys must exist in the known schema
  left.withColumnsRenamed({ missing: "renamed" });
  // @ts-expect-error - a rename cannot overwrite an untouched existing column
  left.withColumnRenamed("name", "id");
  // @ts-expect-error - bulk rename targets must be unique
  left.withColumnsRenamed({ id: "same", name: "same" });
  // @ts-expect-error - a bulk rename cannot overwrite an untouched column
  left.withColumnsRenamed({ name: "id" });
}

describe("unified typed surface (type-level)", () => {
  it("tracks transforms, join nullability and collisions", () => {
    expect(typeof _invalidTypedUsage).toBe("function");
    const assertions: true[] = [
      true satisfies _Projected,
      true satisfies _LeftJoin,
      true satisfies _RightJoin,
      true satisfies _FullJoin,
      true satisfies _DynamicJoinLeft,
      true satisfies _SemiJoin,
      true satisfies _Collision,
      true satisfies _RenamedMany,
      true satisfies _DescribeSummary,
      true satisfies _DescribeId,
      true satisfies _LaxColumnUnknown,
      true satisfies _LaxCoalesceUnknown,
      true satisfies _SqlUnknown,
      true satisfies _MissingUnionUnknown,
    ];
    expect(assertions.every(Boolean)).toBe(true);
  });
});
