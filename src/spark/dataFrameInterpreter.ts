import {col, Column} from "../engine/column";
import {DataFrameDSLFactory} from "./dataframe";
import {Expression, GroupBy, LogicalPlan} from "../engine/logicalPlan";
import {printArrowResults} from "../utils/arrowPrinter";
import {compileToProtobuf} from "../engine/compiler";
import {sparkGrpcClient} from "../client/sparkClient";
import {SparkSession} from "./session";


export function dataframeInterpreter(plan: LogicalPlan, session: SparkSession): DataFrameDSLFactory<LogicalPlan> {
    return {
        select: (plan, columns) => ({
            type: "Project",
            input: plan,
            columns: columns.map(c => typeof c === "string" ? col(c).expr : c.expr),
        }),

        filter: (plan, condition) => ({
            type: "Filter",
            input: plan,
            condition: condition.expr,
        }),

        withColumn: (plan, name, expr) => {
            const cols = extractColumns(plan);
            const updatedCols = replaceOrAppendColumn(cols, name, expr.alias(name).expr);
            return {
                type: "Project",
                input: plan,
                columns: updatedCols,
            };
        },
        join: (left, right, on) => ({
            type: "Join",
            left,
            right,
            on: on.expr,
            joinType: "INNER",
        }),

        groupBy(plan, columns) {
            const groupByExprs =
                plan.type === "GroupBy" ? plan.groupBy : [];

            const groupedNode: GroupBy = {
                type: "GroupBy",
                input: plan,
                expressions: groupByExprs,
            };

            return {
                agg: (aggregations: Record<string, string>): LogicalPlan => ({
                    type: "Aggregate",
                    input: groupedNode,
                    aggregations,
                }),
            };
        },
        orderBy(plan, columns) {
            return {
                type: "Sort",
                input: plan,
                orders: columns.map(c => {
                    const e: Expression = typeof c === "string" ? col(c).expr : c.expr;
                    if (e.type === "SortKey") {
                        const {input, direction, nulls} = e;
                        return {expr: input, direction, nulls};
                    }
                    return {expr: e, direction: "asc" as const};
                }),
            };
        },
        sort(plan, columns) {
            return {
                type: "Sort",
                input: plan,
                orders: columns.map(c => {
                    const e: Expression = typeof c === "string" ? col(c).expr : c.expr;
                    if (e.type === "SortKey") {
                        const {input, direction, nulls} = e;
                        return {expr: input, direction, nulls};
                    }
                    return {expr: e, direction: "asc" as const};
                }),
            };
        },

        limit(plan, n) {
            if (!Number.isInteger(n) || n < 0) throw new Error("limit(n) requiere entero >= 0");
            return {type: "Limit", input: plan, limit: n};
        },
        distinct(plan) {
            return {type: "Distinct", input: plan};
        },

        union(left, right, opts) {
            return {
                type: "Union",
                inputs: [left, right],
                byName: !!opts?.byName,
                allowMissingColumns: !!opts?.allowMissingColumns,
            };
        },

        dropDuplicates(plan, columns) {
            if (!columns || columns.length === 0) return {type: "Distinct", input: plan};
            const exprs = columns.map(c => (typeof c === "string" ? col(c).expr : c.expr));
            const gb: GroupBy = {type: "GroupBy", input: plan, expressions: exprs};
            return {type: "Aggregate", input: gb, aggregations: {}};
        },
        withColumnRenamed(plan, oldName, newName) {
            return {
                type: "Project",
                input: plan,
                columns: [{type: "UnresolvedStar"}, {type: "Alias", input: col(oldName).expr, alias: newName}],
            };
        },

        withColumnsRenamed(plan, mapping) {
            const columns: Expression[] = Object.entries(mapping).map(([from, to]) => ({
                type: "Alias",
                input: col(from).expr,
                alias: to,
            }));
            return {type: "Project", input: plan, columns};
        },


        async show(plan) {
            const result = await this.collect(plan);
            const arrowBuffers = result
                .filter(r => r.arrow_batch?.data)
                .map(r => r.arrow_batch.data as Buffer);
            printArrowResults(arrowBuffers);
        },
        async collect(plan): Promise<any[]> {
            const logicalPlan = compileToProtobuf(plan);
            const request = {
                session_id: session.getSessionId(),
                user_context: session.getUserContext(),
                plan: logicalPlan.plan
            };
            return await sparkGrpcClient.executePlan(request);
        },
    };
}


function replaceOrAppendColumn(
    columns: Expression[],
    name: string,
    newExpr: Expression
): Expression[] {
    return [
        ...columns.filter(c =>
            c.type === "Alias" ? c.alias !== name :
                c.type === "Column" ? c.name !== name :
                    true
        ),
        newExpr
    ];
}

function extractColumns(plan: LogicalPlan): Expression[] {
    if (plan.type === "Project") {
        return plan.columns;
    }

    if (plan.type === "Filter") {
        return extractColumns(plan.input);
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
                    name: fnCall.split("(")[0],
                    args: [],
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
