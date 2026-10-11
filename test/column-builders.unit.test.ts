import { describe, expect, it } from "vitest";
import { ProtoExprAlg } from "../src/engine/compilerRead";
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
