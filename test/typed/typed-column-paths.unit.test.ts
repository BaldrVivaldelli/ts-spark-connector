import { describe, expect, it } from "vitest";
import { ProtoExprAlg } from "../../src/engine/compilerRead";
import { SparkExprAlg } from "../../src/read/readDataFrameInterpreter";
import { makeAggFactory } from "../../src/typed/aggregations";
import { when as typedWhen } from "../../src/typed/functions";
import { Condition, NumericColumn, SortKey } from "../../src/typed/typed-column";

type SparkExpression = ReturnType<typeof SparkExprAlg.col>;

describe("typed column arithmetic, logic and ordering", () => {
    const left = new NumericColumn<number, SparkExpression>(EX => EX.col("left"));
    const right = new NumericColumn<number, SparkExpression>(EX => EX.col("right"));
    const truthy = new Condition<SparkExpression>(EX => EX.lit(true));
    const falsy = new Condition<SparkExpression>(EX => EX.lit(false));

    it("combines conditions and builds sort keys", () => {
        expect(truthy.or(falsy).build(SparkExprAlg)).toMatchObject({ type: "Logical" });
        expect(new SortKey(EX => EX.col("id"), "asc").build(SparkExprAlg))
            .toMatchObject({ type: "Column" });
        expect(left.asc().build(SparkExprAlg)).toMatchObject({ type: "Column" });
    });

    it("accepts numbers and columns as arithmetic and coalesce operands", () => {
        expect(left.div(2).build(SparkExprAlg)).toMatchObject({ op: "/" });
        expect(left.div(right).build(SparkExprAlg)).toMatchObject({ op: "/" });
        expect(left.coalesce(0).build(SparkExprAlg)).toMatchObject({ name: "coalesce" });
        expect(left.coalesce(right).build(SparkExprAlg)).toMatchObject({ name: "coalesce" });
    });

    it("builds CASE WHEN with scalar, null and column branches", () => {
        expect(typedWhen(truthy, "yes").otherwise(null).build(SparkExprAlg))
            .toMatchObject({ type: "CaseWhen" });
        expect(typedWhen(truthy, left).otherwise(right).build(SparkExprAlg))
            .toMatchObject({ type: "CaseWhen" });
    });
});

describe("typed aggregation references", () => {
    type Shape = { amount: number };

    it("resolves names, columns and callbacks to the same aliased aggregate", () => {
        const factory = makeAggFactory<Shape, ReturnType<typeof ProtoExprAlg.col>>();
        const column = new NumericColumn<number, ReturnType<typeof ProtoExprAlg.col>>(
            EX => EX.col("amount"),
        );
        const cases = [
            ["sum", factory.sum("amount").as("named")],
            ["avg", factory.avg(column).as("column")],
            ["max", factory.max(columns => columns.amount).as("callback")],
        ] as const;

        // The alias is applied by agg() when it names the output column; the
        // builder itself only produces the aggregate call.
        expect(cases.map(([, aggregation]) => aggregation.alias)).toEqual(["named", "column", "callback"]);
        for (const [fn, aggregation] of cases) {
            const json = JSON.stringify(aggregation.build(ProtoExprAlg));
            expect(json).toContain(`"function_name":"${fn}"`);
            expect(json).toContain('"unparsed_identifier":"amount"');
        }
    });
});
