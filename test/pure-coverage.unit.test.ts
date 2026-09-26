import { describe, expect, it, vi } from "vitest";
import * as Column from "../src/engine/column";
import {
    toProtoExplainMode,
    toProtoGroupType,
    toProtoJoinType,
    toProtoNullsOrder,
    toProtoSaveMode,
    toProtoSetOpType,
    toProtoSortDirection,
} from "../src/engine/sparkConnectEnums";
import { SparkExprAlg } from "../src/read/readDataFrameInterpreter";
import { resolveOrderInput } from "../src/read/orderResolver";
import {
    assertRenameMapping,
    assertUniqueColumnNames,
    dropRuntimeSchema,
    joinRuntimeSchema,
    renameRuntimeSchema,
    schemaDefsEqual,
    selectRuntimeSchema,
    statisticsRuntimeSchema,
} from "../src/read/runtimeSchema";
import {
    assertStatisticsColumns,
    buildDescribePlan,
    buildSummaryPlan,
} from "../src/read/statistics";
import {
    arrayType,
    decimalType,
    mapType,
    schema,
    structType,
    type FieldSpec,
} from "../src/schema/schema";
import {
    Condition,
    NumericColumn,
    SortKey,
    TypedColumn,
} from "../src/typed/typed-column";
import { makeAggFactory } from "../src/typed/aggregations";
import { when as typedWhen } from "../src/typed/functions";
import { SparkDFAlg } from "../src/read/readDataFrameInterpreter";
import { SparkSession } from "../src/client/session";
import { printArrowResults } from "../src/utils/arrowPrinter";
import * as arrow from "apache-arrow";

describe("column builders", () => {
    it("executes every builder, coercion and window branch", () => {
        const id = Column.col("id");
        const rhs = Column.col("rhs");
        const expressions = [
            id.alias("alias"),
            id.eq(rhs),
            id.eq("literal"),
            id.gt(1),
            id.gte(1),
            id.lt(2),
            id.lte(2),
            id.and(rhs.eq(true)),
            id.or(rhs.eq(false)),
            id.isNull(),
            id.isNotNull(),
            id.from_json("x INT"),
            id.to_json(),
            Column.call("mixed", [id, "column", 1, true]),
            Column.map_keys(id),
            Column.map_values(id),
            Column.elementAt(id, "key"),
            Column.split(id, ","),
            Column.split(id, rhs),
            Column.getField(id, "nested"),
            Column.to_json(id),
            Column.from_json(id, "x INT"),
            Column.struct(id, rhs),
            Column.getItem(id, rhs),
            Column.getItem(id, "key"),
            Column.explode(id),
            Column.explode("items"),
            Column.posexplode(id),
            Column.posexplode("items"),
            Column.isNull(id),
            Column.isNull("id"),
            Column.isNotNull(id),
            Column.isNotNull("id"),
            Column.coalesce(id, "column", 1, true),
            Column.when(id.eq(1), "one")
                .when(id.eq(2), Column.lit("two"))
                .otherwise("other"),
        ];

        for (const expression of expressions) {
            expect(expression.build(SparkExprAlg)).toBeTypeOf("object");
        }

        const orderBuilders = [
            id.asc(),
            id.asc("nullsFirst"),
            id.desc(),
            id.desc("nullsLast"),
            id.ascNullsFirst(),
            id.ascNullsLast(),
            id.descNullsFirst(),
            id.descNullsLast(),
        ];
        for (const order of orderBuilders) {
            expect(order(SparkExprAlg).expr).toMatchObject({ type: "Column" });
        }

        const specification = Column.Window
            .partitionBy("group", id)
            .orderBy("id", id.descNullsLast(), rhs)
            .rowsBetween(
                { type: "UnboundedPreceding" },
                { type: "CurrentRow" },
            );
        expect(id.over(specification).build(SparkExprAlg)).toMatchObject({
            type: "Window",
        });
    });
});

describe("order resolution and typed builders", () => {
    it("covers string, untyped, typed-column, typed-sort and legacy callbacks", () => {
        const untyped = { build: () => SparkExprAlg.col("object") };
        const typed = new TypedColumn<number, ReturnType<typeof SparkExprAlg.col>>(
            EX => EX.col("typed"),
        );
        const sort = new SortKey(EX => EX.col("sorted"), "desc", "nullsLast");

        expect(resolveOrderInput("name", SparkExprAlg)).toMatchObject({ direction: "asc" });
        expect(resolveOrderInput(untyped, SparkExprAlg)).toMatchObject({ direction: "asc" });
        expect(resolveOrderInput(() => typed, SparkExprAlg)).toMatchObject({ direction: "asc" });
        expect(resolveOrderInput(() => sort, SparkExprAlg)).toMatchObject({
            direction: "desc",
            nulls: "nullsLast",
        });
        expect(resolveOrderInput(
            (algebra: typeof SparkExprAlg) => ({
                expr: algebra.col("legacy"),
                direction: "desc",
            }),
            SparkExprAlg,
        )).toMatchObject({ direction: "desc" });
    });

    it("executes remaining typed-column and typed-function paths", () => {
        const left = new NumericColumn<number, ReturnType<typeof SparkExprAlg.col>>(
            EX => EX.col("left"),
        );
        const right = new NumericColumn<number, ReturnType<typeof SparkExprAlg.col>>(
            EX => EX.col("right"),
        );
        type SparkExpression = ReturnType<typeof SparkExprAlg.col>;
        const truthy = new Condition<SparkExpression>(EX => EX.lit(true));
        const falsy = new Condition<SparkExpression>(EX => EX.lit(false));

        expect(truthy.or(falsy).build(SparkExprAlg)).toMatchObject({ type: "Logical" });
        expect(new SortKey(EX => EX.col("id"), "asc").build(SparkExprAlg))
            .toMatchObject({ type: "Column" });
        expect(left.asc().build(SparkExprAlg)).toMatchObject({ type: "Column" });
        expect(left.div(2).build(SparkExprAlg)).toMatchObject({ op: "/" });
        expect(left.div(right).build(SparkExprAlg)).toMatchObject({ op: "/" });
        expect(left.coalesce(0).build(SparkExprAlg)).toMatchObject({ name: "coalesce" });
        expect(left.coalesce(right).build(SparkExprAlg)).toMatchObject({ name: "coalesce" });
        expect(typedWhen(truthy, "yes").otherwise(null).build(SparkExprAlg))
            .toMatchObject({ type: "CaseWhen" });
        expect(typedWhen(truthy, left).otherwise(right).build(SparkExprAlg))
            .toMatchObject({ type: "CaseWhen" });
    });

    it("resolves aggregation references supplied as names, columns and callbacks", () => {
        type Shape = { amount: number };
        const factory = makeAggFactory<Shape, ReturnType<typeof SparkExprAlg.col>>();
        const column = new NumericColumn<number, ReturnType<typeof SparkExprAlg.col>>(
            EX => EX.col("amount"),
        );
        const aggregations = [
            factory.sum("amount").as("named"),
            factory.avg(column).as("column"),
            factory.max(columns => columns.amount).as("callback"),
        ];

        for (const aggregation of aggregations) {
            expect(aggregation.build(SparkExprAlg)).toBeTypeOf("object");
        }
    });
});

describe("runtime schema helpers", () => {
    const left = { id: "int", nested: structType({ value: "string" }) } as const;
    const right = { label: "string" } as const;

    it("covers equality, selection, dropping and statistics", () => {
        expect(schemaDefsEqual()).toBe(true);
        expect(schemaDefsEqual(left)).toBe(false);
        expect(schemaDefsEqual(left, { ...left })).toBe(true);
        expect(schemaDefsEqual(left, right)).toBe(false);

        expect(selectRuntimeSchema(undefined, ["id"])).toBeUndefined();
        expect(selectRuntimeSchema(left, ["missing"])).toBeUndefined();
        expect(selectRuntimeSchema(left, ["id", "id"])).toBeUndefined();
        expect(selectRuntimeSchema(left, ["id"])).toEqual({ id: "int" });
        expect(dropRuntimeSchema(undefined, ["id"])).toBeUndefined();
        expect(dropRuntimeSchema(left, ["id"])).toEqual({ nested: left.nested });
        expect(statisticsRuntimeSchema(left, ["missing"])).toBeUndefined();
        expect(statisticsRuntimeSchema(left, ["id"])).toEqual({
            summary: "string",
            id: "string?",
        });
    });

    it("covers every join nullability and collision branch", () => {
        expect(joinRuntimeSchema(undefined, right, "INNER")).toBeUndefined();
        expect(joinRuntimeSchema(left, { id: "int" }, "INNER")).toBeUndefined();
        expect(joinRuntimeSchema(left, right, "LEFT_SEMI")).toBe(left);
        expect(joinRuntimeSchema(left, right, "LEFT_ANTI")).toBe(left);
        expect(joinRuntimeSchema(left, right, "INNER")).toEqual({ ...left, ...right });
        expect(joinRuntimeSchema(left, right, "LEFT")).toMatchObject({ label: "string?" });
        expect(joinRuntimeSchema(left, right, "RIGHT")).toMatchObject({
            id: "int?",
            nested: { ...left.nested, nullable: true },
        });
        expect(joinRuntimeSchema(left, right, "FULL")).toMatchObject({
            id: "int?",
            label: "string?",
        });
        expect(joinRuntimeSchema({ id: "int?" }, right, "RIGHT")).toMatchObject({
            id: "int?",
        });
    });

    it("validates renames and duplicate names", () => {
        expect(() => assertRenameMapping({ "": "id" })).toThrow();
        expect(() => assertRenameMapping({ id: "" })).toThrow();
        expect(() => assertRenameMapping({ id: "same", label: "same" })).toThrow();
        expect(() => assertUniqueColumnNames("select", ["id", "id"])).toThrow();
        expect(renameRuntimeSchema(undefined, { id: "new_id" })).toBeUndefined();
        expect(() => renameRuntimeSchema(left, { missing: "x" })).toThrow();
        expect(() => renameRuntimeSchema(left, { nested: "id" })).toThrow();
        expect(renameRuntimeSchema(left, { id: "new_id" })).toEqual({
            new_id: "int",
            nested: left.nested,
        });
    });
});

describe("statistics planner", () => {
    const relation = SparkDFAlg.relation("csv", "/input");

    it("builds describe and every summary metric", () => {
        assertStatisticsColumns("describe", ["id"]);
        expect(() => assertStatisticsColumns("describe", ["summary"])).toThrow(/describe/);
        expect(() => assertStatisticsColumns("summary", ["summary"])).toThrow(/summarize/);

        expect(buildDescribePlan(SparkDFAlg, SparkExprAlg, relation, ["id"]))
            .toMatchObject({ type: "Union" });
        expect(buildSummaryPlan(
            SparkDFAlg,
            SparkExprAlg,
            relation,
            ["count", "mean", "std", "min", "median", "75%", "max"],
            ["id"],
        )).toMatchObject({ type: "Union" });
        expect(buildSummaryPlan(SparkDFAlg, SparkExprAlg, relation, [], ["id"]))
            .toMatchObject({ type: "Union" });
        expect(() => buildSummaryPlan(
            SparkDFAlg,
            SparkExprAlg,
            relation,
            ["101%"],
            ["id"],
        )).toThrow(/invalid percentile/);
        expect(() => buildSummaryPlan(
            SparkDFAlg,
            SparkExprAlg,
            relation,
            ["unknown"],
            ["id"],
        )).toThrow(/unsupported metric/);
    });

    it("executes high-level describe and summary lazily", () => {
        const dataframe = SparkSession.builder().getOrCreate().read.csv("/input");
        expect(dataframe.describe(["id"]).toProtoJSON()).toContain('"set_op"');
        expect(dataframe.summary(undefined, ["id"]).toProtoJSON()).toContain('"set_op"');
    });
});

describe("schema descriptors and enums", () => {
    it("covers nullable descriptor branches and invalid DDL descriptors", () => {
        expect(decimalType(10, 2, true)).toMatchObject({ nullable: true });
        expect(decimalType(10, 2)).not.toHaveProperty("nullable");
        expect(arrayType("int", true)).toMatchObject({ nullable: true });
        expect(arrayType("int")).not.toHaveProperty("nullable");
        expect(mapType("string", "int", true)).toMatchObject({ nullable: true });
        expect(mapType("string", "int")).not.toHaveProperty("nullable");
        expect(structType({ id: "int" }, true)).toMatchObject({ nullable: true });
        expect(structType({ id: "int" })).not.toHaveProperty("nullable");

        const invalid = schema({
            broken: { kind: "unsupported" } as unknown as FieldSpec,
        });
        expect(() => invalid.toDDL()).toThrow(/Unsupported Spark schema descriptor/);
    });

    it("covers valid defaults and every enum error", () => {
        expect(toProtoJoinType("inner")).toBe(1);
        expect(() => toProtoJoinType("invalid" as unknown as "inner")).toThrow();
        expect(toProtoGroupType()).toBe(1);
        expect(() => toProtoGroupType("invalid" as unknown as "groupby")).toThrow();
        expect(toProtoSetOpType()).toBe(2);
        expect(() => toProtoSetOpType("invalid" as unknown as "union")).toThrow();
        expect(toProtoSortDirection("asc")).toContain("ASCENDING");
        expect(toProtoSortDirection("desc")).toContain("DESCENDING");
        expect(toProtoNullsOrder("nullsFirst", "desc")).toContain("FIRST");
        expect(toProtoNullsOrder("nullsLast", "asc")).toContain("LAST");
        expect(toProtoNullsOrder(undefined, "asc")).toContain("FIRST");
        expect(toProtoNullsOrder(undefined, "desc")).toContain("LAST");
        for (const mode of [undefined, "append", "overwrite", "error", "errorifexists", "ignore"] as const) {
            expect(toProtoSaveMode(mode)).toBeTypeOf("number");
        }
        expect(() => toProtoSaveMode("invalid" as unknown as "append")).toThrow();
        for (const mode of ["simple", "extended", "codegen", "cost", "formatted"] as const) {
            expect(toProtoExplainMode(mode)).toBeGreaterThan(0);
        }
        expect(() => toProtoExplainMode("invalid" as unknown as "simple")).toThrow();
    });
});

describe("remaining reader and Arrow printer paths", () => {
    it("covers the JSON reader shortcut and default load format", () => {
        const reader = SparkSession.builder().getOrCreate().read;
        expect(reader.json("/input.json").toProtoJSON()).toContain('"json"');
        expect(reader.load("/input.parquet").toProtoJSON()).toContain('"parquet"');
    });

    it("prints nulls, truncation widths, missing vectors and row limits", () => {
        const table = arrow.tableFromArrays({
            id: [1, 2],
            value: [null, "long-value"],
        });
        const buffer = Buffer.from(arrow.tableToIPC(table, "stream"));
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);

        printArrowResults([buffer], { maxRows: 1, truncate: 1 });
        printArrowResults([buffer, buffer], { maxRows: 2, truncate: 0 });

        expect(log).toHaveBeenCalled();
        log.mockRestore();
    });
});
