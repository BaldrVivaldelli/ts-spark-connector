import { describe, expect, it } from "vitest";
import { ProtoExprAlg } from "../src/engine/compilerRead";
import { SparkExprAlg } from "../src/read/readDataFrameInterpreter";
import * as Column from "../src/engine/column";

// Pins how the untyped column helpers coerce their arguments on the wire:
// comparison right-hand sides treat bare strings as literals (PySpark
// semantics), while standalone helpers treat bare strings as column names.

type ProtoExpression = ReturnType<typeof ProtoExprAlg.col>;

const attribute = (name: string) => ({ unresolved_attribute: { unparsed_identifier: name } });
const fn = (expression: ProtoExpression) =>
    (expression as { unresolved_function: { function_name: string; arguments: ProtoExpression[] } }).unresolved_function;

describe("column builder coercions", () => {
    const id = Column.col("id");
    const rhs = Column.col("rhs");

    it("compares against columns, string literals and numbers", () => {
        expect(fn(id.eq(rhs).build(ProtoExprAlg))).toEqual({
            function_name: "=",
            arguments: [attribute("id"), attribute("rhs")],
        });
        expect(fn(id.eq("literal").build(ProtoExprAlg)).arguments[1]).toEqual({ literal: { string: "literal" } });
        expect(fn(id.gt(1).build(ProtoExprAlg)).arguments[1]).toEqual({ literal: { integer: 1 } });
        expect(fn(id.gte(1).build(ProtoExprAlg)).function_name).toBe(">=");
        expect(fn(id.lt(2).build(ProtoExprAlg)).function_name).toBe("<");
        expect(fn(id.lte(2).build(ProtoExprAlg)).function_name).toBe("<=");
        expect(fn(id.and(rhs.eq(true)).build(ProtoExprAlg)).function_name.toLowerCase()).toBe("and");
        expect(fn(id.or(rhs.eq(false)).build(ProtoExprAlg)).function_name.toLowerCase()).toBe("or");
    });

    it("treats bare strings as column names in standalone helpers", () => {
        const mixed = fn(Column.call("mixed", [id, "column", 1, true]).build(ProtoExprAlg));
        expect(mixed.function_name).toBe("mixed");
        expect(mixed.arguments).toEqual([
            attribute("id"),
            attribute("column"),
            { literal: { integer: 1 } },
            { literal: { boolean: true } },
        ]);
        expect(fn(Column.coalesce(id, "column", 1, true).build(ProtoExprAlg)).arguments).toEqual(mixed.arguments);
        expect(fn(Column.explode("items").build(ProtoExprAlg)).arguments).toEqual([attribute("items")]);
        expect(fn(Column.posexplode("items").build(ProtoExprAlg)).arguments).toEqual([attribute("items")]);
        expect(fn(Column.isNull("id").build(ProtoExprAlg)).arguments).toEqual([attribute("id")]);
        expect(fn(Column.isNotNull("id").build(ProtoExprAlg)).arguments).toEqual([attribute("id")]);
        expect(fn(Column.explode(id).build(ProtoExprAlg)).arguments).toEqual([attribute("id")]);
    });

    it("builds collection helpers through the generic function call", () => {
        expect(fn(Column.map_keys(id).build(ProtoExprAlg)).function_name).toBe("map_keys");
        expect(fn(Column.map_values(id).build(ProtoExprAlg)).function_name).toBe("map_values");
        expect(fn(Column.elementAt(id, "key").build(ProtoExprAlg))).toMatchObject({
            function_name: "element_at",
            arguments: [attribute("id"), attribute("key")],
        });
        expect(fn(Column.split(id, ",").build(ProtoExprAlg)).arguments).toHaveLength(2);
        expect(fn(Column.split(id, rhs).build(ProtoExprAlg)).arguments[1]).toEqual(attribute("rhs"));
        expect(fn(Column.struct(id, rhs).build(ProtoExprAlg))).toMatchObject({ function_name: "struct" });
        expect(JSON.stringify(Column.getItem(id, "key").build(ProtoExprAlg))).toContain('"string":"key"');
        expect(JSON.stringify(Column.getItem(id, rhs).build(ProtoExprAlg))).toContain('"unparsed_identifier":"rhs"');
        // The helper and the column method are the same 0-based extraction.
        for (const key of [0, "key", rhs] as const) {
            expect(Column.getItem(id, key).build(ProtoExprAlg)).toEqual(id.getItem(key).build(ProtoExprAlg));
            expect(Column.getItem(id, key).build(SparkExprAlg)).toEqual(id.getItem(key).build(SparkExprAlg));
        }
        expect(JSON.stringify(Column.getField(id, "nested").build(ProtoExprAlg))).toContain("nested");
        expect(JSON.stringify(Column.to_json(id).build(ProtoExprAlg))).toContain("to_json");
        expect(JSON.stringify(id.to_json().build(ProtoExprAlg))).toContain("to_json");
        expect(JSON.stringify(Column.from_json(id, "x INT").build(ProtoExprAlg))).toContain("x INT");
        expect(JSON.stringify(id.from_json("x INT").build(ProtoExprAlg))).toContain("x INT");
        expect(JSON.stringify(id.alias("alias").build(ProtoExprAlg))).toContain('"alias"');
    });

    it("builds CASE WHEN chains with scalar and builder branches", () => {
        const caseWhen = fn(
            Column.when(id.eq(1), "one")
                .when(id.eq(2), Column.lit("two"))
                .otherwise("other")
                .build(ProtoExprAlg),
        );
        // Spark Connect has no CASE WHEN expression; the chain compiles to nested
        // if(cond, then, else) calls, innermost branch last.
        expect(caseWhen.function_name).toBe("if");
        expect(caseWhen.arguments).toHaveLength(3);
        expect(caseWhen.arguments[1]).toEqual({ literal: { string: "one" } });
        const inner = fn(caseWhen.arguments[2]!);
        expect(inner.function_name).toBe("if");
        expect(inner.arguments[1]).toEqual({ literal: { string: "two" } });
        expect(inner.arguments[2]).toEqual({ literal: { string: "other" } });
    });

    it("pins the null ordering of every sort key builder", () => {
        const orders: Array<[ReturnType<typeof id.asc>, string, string | undefined]> = [
            [id.asc(), "asc", undefined],
            [id.asc("nullsFirst"), "asc", "nullsFirst"],
            [id.desc(), "desc", undefined],
            [id.desc("nullsLast"), "desc", "nullsLast"],
            [id.ascNullsFirst(), "asc", "nullsFirst"],
            [id.ascNullsLast(), "asc", "nullsLast"],
            [id.descNullsFirst(), "desc", "nullsFirst"],
            [id.descNullsLast(), "desc", "nullsLast"],
        ];
        for (const [order, direction, nulls] of orders) {
            const resolved = order(ProtoExprAlg);
            expect(resolved.expr).toEqual(attribute("id"));
            expect(resolved.direction).toBe(direction);
            expect(resolved.nulls).toBe(nulls);
        }
    });

    it("builds window specifications from names, builders and sort keys", () => {
        const specification = Column.Window
            .partitionBy("group", id)
            .orderBy("id", id.descNullsLast(), rhs)
            .rowsBetween({ type: "UnboundedPreceding" }, { type: "CurrentRow" });
        const window = (id.over(specification).build(ProtoExprAlg) as {
            window: {
                partition_spec: unknown[];
                order_spec: Array<{ direction: string; null_ordering: string }>;
                frame_spec: unknown;
            };
        }).window;
        expect(window.partition_spec).toEqual([attribute("group"), attribute("id")]);
        expect(window.order_spec).toHaveLength(3);
        expect(window.order_spec[1]!.direction).toContain("DESCENDING");
        expect(window.order_spec[1]!.null_ordering).toContain("LAST");
        expect(window.order_spec[2]!.direction).toContain("ASCENDING");
        expect(window.frame_spec).toBeTruthy();
    });
});

describe("column methods", () => {
    const id = Column.col("id");
    const rhs = Column.col("rhs");
    const built = (expression: { build: typeof id.build }) => expression.build(ProtoExprAlg) as Record<string, any>;
    const literal = (value: unknown) => ProtoExprAlg.lit(value as never);

    it("builds binary operators with literal or column right-hand sides", () => {
        const operators: Array<[(x: Column.ColumnOperand) => Column.EBuilder, string]> = [
            [id.eqNullSafe, "<=>"],
            [id.plus, "+"],
            [id.minus, "-"],
            [id.multiply, "*"],
            [id.divide, "/"],
            [id.mod, "%"],
            [id.bitwiseAND, "&"],
            [id.bitwiseOR, "|"],
            [id.bitwiseXOR, "^"],
        ];
        for (const [operator, name] of operators) {
            expect(fn(built(operator(2)))).toEqual({ function_name: name, arguments: [attribute("id"), literal(2)] });
            expect(fn(built(operator(rhs))).arguments[1]).toEqual(attribute("rhs"));
        }
        // A bare string is a literal on the right-hand side, null is a null literal.
        expect(fn(built(id.eqNullSafe(null))).arguments[1]).toEqual({ literal: { null: { null: {} } } });
        // `!=` is SQL syntax only; Spark Connect cannot resolve it as a function.
        expect(fn(built(id.neq("x")))).toEqual({
            function_name: "not",
            arguments: [{ unresolved_function: { function_name: "=", arguments: [attribute("id"), literal("x")] } }],
        });
    });

    it("negates, names and checks NaN", () => {
        expect(fn(built(id.gt(1).not()))).toMatchObject({ function_name: "not" });
        expect(built(id.name("renamed"))).toEqual({ alias: { expr: attribute("id"), name: ["renamed"] } });
        expect(fn(built(id.isNaN()))).toEqual({ function_name: "isnan", arguments: [attribute("id")] });
    });

    it("builds IN lists from values and arrays", () => {
        expect(fn(built(id.isin(1, "two", rhs)))).toEqual({
            function_name: "in",
            arguments: [attribute("id"), literal(1), literal("two"), attribute("rhs")],
        });
        expect(fn(built(id.isin([1, 2], 3))).arguments).toEqual([attribute("id"), literal(1), literal(2), literal(3)]);
    });

    it("expands between into an inclusive range", () => {
        const range = built(id.between(1, rhs));
        expect(fn(range).function_name.toLowerCase()).toBe("and");
        expect(fn(fn(range).arguments[0]!)).toEqual({ function_name: ">=", arguments: [attribute("id"), literal(1)] });
        expect(fn(fn(range).arguments[1]!)).toEqual({ function_name: "<=", arguments: [attribute("id"), attribute("rhs")] });
    });

    it("builds string predicates and substr with literal patterns", () => {
        const predicates: Array<[(pattern: string) => Column.EBuilder, string]> = [
            [id.like, "like"],
            [id.ilike, "ilike"],
            [id.rlike, "rlike"],
            [id.startsWith, "startswith"],
            [id.endsWith, "endswith"],
            [id.contains, "contains"],
        ];
        for (const [predicate, name] of predicates) {
            expect(fn(built(predicate("a%")))).toEqual({ function_name: name, arguments: [attribute("id"), literal("a%")] });
        }
        expect(fn(built(id.substr(2, rhs)))).toEqual({
            function_name: "substr",
            arguments: [attribute("id"), literal(2), attribute("rhs")],
        });
    });

    it("casts with an optional try evaluation mode", () => {
        expect(built(id.cast("decimal(10,2)"))).toEqual({ cast: { expr: attribute("id"), type_str: "decimal(10,2)" } });
        expect(built(id.tryCast("int"))).toEqual({
            cast: { expr: attribute("id"), type_str: "int", eval_mode: "EVAL_MODE_TRY" },
        });
    });

    it("extracts and updates nested values like PySpark", () => {
        expect(built(id.getItem(0))).toEqual({ unresolved_extract_value: { child: attribute("id"), extraction: literal(0) } });
        expect(built(id.getItem("key"))).toEqual({
            unresolved_extract_value: { child: attribute("id"), extraction: literal("key") },
        });
        expect(built(id.getItem(rhs))).toEqual({ unresolved_extract_value: { child: attribute("id"), extraction: attribute("rhs") } });
        expect(built(id.getField("a").getField("b"))).toEqual({
            unresolved_extract_value: {
                child: { unresolved_extract_value: { child: attribute("id"), extraction: literal("a") } },
                extraction: literal("b"),
            },
        });
        expect(built(id.withField("x", Column.lit(1)))).toEqual({
            update_fields: { struct_expression: attribute("id"), field_name: "x", value_expression: literal(1) },
        });
        // Dropping several fields nests one update per field, in order.
        expect(built(id.dropFields("a", "b"))).toEqual({
            update_fields: {
                struct_expression: { update_fields: { struct_expression: attribute("id"), field_name: "a" } },
                field_name: "b",
            },
        });
    });

    it("parses SQL expressions on the server and rejects blank ones", () => {
        expect(built(Column.expr("amount * 2"))).toEqual({ expression_string: { expression: "amount * 2" } });
        expect(() => Column.expr("  ")).toThrow("expr() requires a non-empty SQL expression.");
        expect(() => Column.expr(undefined as unknown as string)).toThrow(TypeError);
    });
});

describe("window specifications", () => {
    const id = Column.col("id");
    type ProtoWindow = {
        window: {
            window_function: unknown;
            partition_spec: unknown[];
            order_spec: Array<{ child: unknown; direction: string }>;
            frame_spec?: { frame_type: string; lower: unknown; upper: unknown };
        };
    };
    const windowOf = (spec?: Column.WindowSpecBuilder) =>
        (Column.call("row_number", []).over(spec).build(ProtoExprAlg) as ProtoWindow).window;

    it("evaluates over the whole result without a specification", () => {
        expect(windowOf()).toEqual({
            window_function: { unresolved_function: { function_name: "row_number", arguments: [] } },
            partition_spec: [],
            order_spec: [],
        });
    });

    it("accepts partition-only and order-only specifications", () => {
        const partitioned = windowOf(Column.Window.partitionBy("group"));
        expect(partitioned.partition_spec).toEqual([attribute("group")]);
        expect(partitioned.order_spec).toEqual([]);
        expect(partitioned.frame_spec).toBeUndefined();

        const ordered = windowOf(Column.Window.orderBy(id.desc()));
        expect(ordered.partition_spec).toEqual([]);
        expect(ordered.order_spec[0]!.direction).toContain("DESCENDING");

        // Refining keeps the earlier parts.
        const both = windowOf(Column.Window.orderBy("id").partitionBy(id));
        expect(both.partition_spec).toEqual([attribute("id")]);
        expect(both.order_spec).toHaveLength(1);
    });

    it("maps PySpark-style numeric boundaries to frame boundaries", () => {
        const rows = windowOf(Column.Window.partitionBy("g").orderBy("t").rowsBetween(-Infinity, 0)).frame_spec;
        expect(rows).toEqual({ frame_type: "FRAME_TYPE_ROW", lower: { unbounded: true }, upper: { current_row: true } });

        const range = windowOf(Column.Window.orderBy("t").rangeBetween(-3, Infinity)).frame_spec;
        expect(range).toEqual({
            frame_type: "FRAME_TYPE_RANGE",
            lower: { value: { literal: { long: "-3" } } },
            upper: { unbounded: true },
        });

        const following = windowOf(Column.Window.rowsBetween(Column.Window.currentRow, 2)).frame_spec;
        expect(following?.upper).toEqual({ value: { literal: { integer: 2 } } });

        const frameOnly = windowOf(
            Column.Window.rangeBetween(Column.Window.unboundedPreceding, Column.Window.unboundedFollowing),
        );
        expect(frameOnly.frame_spec).toEqual({
            frame_type: "FRAME_TYPE_RANGE",
            lower: { unbounded: true },
            upper: { unbounded: true },
        });
    });

    it("rejects NaN frame boundaries", () => {
        expect(() => Column.Window.rowsBetween(Number.NaN, 0)).toThrow(
            "Window frame boundaries must be numbers, not NaN.",
        );
    });
});
