import {col, Column} from "../engine/column";
import {DataFrame, DataFrameDSL, GroupedDSL} from "./dataframe";
import {Expression, LogicalPlan} from "../engine/logicalPlan";


export function dataframeInterpreter(base: DataFrame): DataFrameDSL<DataFrame> {
    return {
        select: (columns: (string | Column)[]): DataFrame => {
            const exprs = columns.map(c =>
                typeof c === "string" ? col(c).expr : c.expr
            );

            return new DataFrame({
                type: "Project",
                input: base.plan,
                columns: exprs, // <--- esto ahora es Expression[]
            });
        }
        ,

        filter: (condition: Column) =>
            new DataFrame({
                type: "Filter",
                input: base.plan,
                condition: condition.expr
            }),

        /*        join: (other: DataFrame, on: string) =>
                    new DataFrame({
                        type: "Join",
                        left: base.plan,
                        right: other.plan,
                        on,
                    }),

                groupBy: (columns: string[]): GroupedDSL<DataFrame> => ({
                    agg: (aggregations: Record<string, string>): DataFrame => ({
                        plan: {
                            type: "Aggregate",
                            input: {
                                type: "GroupBy",
                                input: base.plan,
                                columns,
                            },
                            aggregations,
                        },
                    } as DataFrame),
                }),*/

        withColumn(name: string, expr: Column): DataFrame {
            const prevCols = extractColumns(base.plan);

            const cleaned = prevCols.filter((c: Expression) => {
                if (c.type === "Alias") return c.alias !== name;
                if (c.type === "Column") return c.name !== name;
                return true;
            });

            return new DataFrame({
                type: "Project",
                input: base.plan,
                columns: [
                    ...cleaned,
                    expr.alias(name).expr,
                ],
            });
        },
    };
}

function extractColumns(plan: LogicalPlan): Expression[] {
    if (plan.type === "Project") {
        return plan.columns;
    }

    if (plan.type === "Aggregate") {
        const groupByExprs =
            plan.input.type === "GroupBy"
                ? (plan.input as unknown as Extract<LogicalPlan, { type: "GroupBy" }>).groupBy
                : [];

        const aggExprs = Object.entries(plan.aggregations).map(
            ([alias, fnCall]): Expression => ({
                type: "Alias",
                input: {
                    type: "UnresolvedFunction",
                    name: fnCall.split("(")[0], // solo función
                    args: [], // opcional: parse args
                },
                alias,
            })
        );

        return [...groupByExprs, ...aggExprs];
    }

    if (plan.type === "GroupBy") {

        return plan.groupBy;
    }

    return [];
}
