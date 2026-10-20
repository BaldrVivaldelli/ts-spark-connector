import { describe, expect, it } from "vitest";
import { SparkSession, col } from "../src";

// SQL text accepted by the DataFrame API is parsed by Spark, so the client only
// forwards it as Expression.expression_string and rejects blank input early.

const session = SparkSession.builder().getOrCreate();
const people = () => session.read.parquet("/data/people.parquet");
const planOf = (df: { toProtoJSON(): string }) => JSON.parse(df.toProtoJSON());
const sqlExpression = (expression: string) => ({ expression_string: { expression } });

describe("SQL expressions in the DataFrame API", () => {
    it("projects each selectExpr string", () => {
        const project = planOf(people().selectExpr("id", "upper(name) AS shout")).project;
        expect(project.expressions).toEqual([sqlExpression("id"), sqlExpression("upper(name) AS shout")]);
    });

    it("filters with a SQL predicate and offers where as an alias", () => {
        expect(planOf(people().filter("age > 21")).filter.condition).toEqual(sqlExpression("age > 21"));
        expect(planOf(people().where("age > 21"))).toEqual(planOf(people().filter("age > 21")));
        expect(planOf(people().where(col("age").gt(21))).filter.condition).toHaveProperty(
            "unresolved_function.function_name",
            ">",
        );
    });

    it("rejects missing or blank SQL", () => {
        expect(() => people().selectExpr()).toThrow("selectExpr() requires at least one expression.");
        expect(() => people().selectExpr("id", " ")).toThrow(/selectExpr\(\) expression/);
        expect(() => people().filter("")).toThrow(/filter\(\) condition/);
        expect(() => people().where("  ")).toThrow(/filter\(\) condition/);
    });
});
