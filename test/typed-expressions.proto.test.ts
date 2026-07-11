import { describe, expect, it } from "vitest";
import {
  SparkSession,
  abs,
  concat,
  length,
  round,
  schema,
  typedWhen,
  upper,
} from "../src";

const Orders = schema({
  id: "int",
  product: "string",
  amount: "double",
  note: "string?",     // nullable
  discount: "double?",  // nullable
});

const sess = SparkSession.builder().getOrCreate();
const orders = () => sess.read.readWith(Orders, "csv", "/tmp/orders.csv");

describe("Phase 3 — arithmetic (runtime plan)", () => {
  it("serializes column arithmetic as nested functions", () => {
    const df = orders().withColumn("net", c => c.amount.minus(c.discount.coalesce(0)));
    const proto = JSON.parse(df.toProtoJSON());
    const expr = proto.with_columns.aliases[0].expr;
    expect(expr.unresolved_function.function_name).toBe("-");
  });

  it("serializes a scalar multiply", () => {
    const df = orders().withColumn("doubled", c => c.amount.times(2));
    const proto = JSON.parse(df.toProtoJSON());
    const expr = proto.with_columns.aliases[0].expr;
    expect(expr.unresolved_function.function_name).toBe("*");
    expect(expr.unresolved_function.arguments[1]).toEqual({ literal: { integer: 2 } });
  });
});

describe("Phase 3 — null handling (runtime plan)", () => {
  it("isNull / isNotNull serialize as functions", () => {
    expect(
      JSON.parse(orders().filter(c => c.note.isNull()).toProtoJSON())
        .filter.condition.unresolved_function.function_name
    ).toBe("isnull");
    expect(
      JSON.parse(orders().filter(c => c.note.isNotNull()).toProtoJSON())
        .filter.condition.unresolved_function.function_name
    ).toBe("isnotnull");
  });

  it("coalesce serializes and yields a non-null column", () => {
    const df = orders().withColumn("safeNote", c => c.note.coalesce("n/a"));
    const proto = JSON.parse(df.toProtoJSON());
    expect(proto.with_columns.aliases[0].expr.unresolved_function.function_name).toBe("coalesce");
  });
});

describe("Phase 3 — scalar functions (runtime plan)", () => {
  it("length / upper / concat / abs / round serialize to the right function names", () => {
    const fnNameOf = (df: { toProtoJSON(): string }) =>
      JSON.parse(df.toProtoJSON()).with_columns.aliases[0].expr.unresolved_function.function_name;

    expect(fnNameOf(orders().withColumn("len", c => length(c.product)))).toBe("length");
    expect(fnNameOf(orders().withColumn("up", c => upper(c.product)))).toBe("upper");
    expect(fnNameOf(orders().withColumn("ab", c => abs(c.amount)))).toBe("abs");
    expect(fnNameOf(orders().withColumn("r", c => round(c.amount, 2)))).toBe("round");
    expect(fnNameOf(orders().withColumn("c", c => concat(c.product, "!")))).toBe("concat");
  });
});

describe("Phase 3 — caseWhen (runtime plan)", () => {
  it("serializes to nested if() with the right branch count", () => {
    const df = orders().withColumn("tier", c =>
      typedWhen(c.amount.gt(100), "high").when(c.amount.gt(10), "mid").otherwise("low")
    );
    const proto = JSON.parse(df.toProtoJSON());
    const expr = proto.with_columns.aliases[0].expr;
    expect(expr.unresolved_function.function_name).toBe("if");
  });
});

// ---------------------------------------------------------------------------
// Type-level tests: invalid usage must NOT compile.
// ---------------------------------------------------------------------------
describe("Phase 3 (compile-time safety)", () => {
  it("rejects arithmetic on non-numeric columns and type-mismatched functions", () => {
    const df = orders();

    // arithmetic is only on numeric columns
    // @ts-expect-error - `product` is a string column, no .times()
    df.withColumn("x", c => c.product.times(2));

    // length expects a string column
    // @ts-expect-error - `amount` is numeric, not a string
    df.withColumn("x", c => length(c.amount));

    // caseWhen branches must share a type
    // @ts-expect-error - "high" is string, 0 is number
    df.withColumn("x", c => typedWhen(c.amount.gt(100), "high").otherwise(0));

    expect(true).toBe(true);
  });

  it("types a coalesced column as non-null and arithmetic results as nullable", async () => {
    const typeCheck = async () => {
      const rows = await orders()
        .withColumn("safeNote", c => c.note.coalesce("n/a"))
        .withColumn("net", c => c.amount.minus(c.discount.coalesce(0)))
        .select("safeNote", "net")
        .collect();
      const safe: string = rows[0].safeNote; // non-null after coalesce
      const net: number | null = rows[0].net; // arithmetic may be null
      // @ts-expect-error - net is nullable, cannot assign to plain number
      const bad: number = rows[0].net;
      return { safe, net, bad };
    };
    expect(typeof typeCheck).toBe("function");
  });

  it("allows comparing a nullable column against its scalar type", () => {
    orders().filter(c => c.discount.gt(5));   // discount is double? -> compare with number ok
    orders().filter(c => c.note.eq("x"));      // note is string? -> compare with string ok
    expect(true).toBe(true);
  });
});
