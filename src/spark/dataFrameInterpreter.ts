import {col} from "../engine/column";
import {DataFrameDSLFactory} from "./dataframe";
import {Expression, LogicalPlan} from "../engine/logicalPlan";
import {printArrowResults} from "../utils/arrowPrinter";
import {compileToProtobuf} from "../engine/compiler";
import {sparkGrpcClient} from "../client/sparkClient";
import {SparkSession} from "./session";


export function dataframeInterpreter(plan: LogicalPlan, session:SparkSession): DataFrameDSLFactory<LogicalPlan> {
    return {
        select: (plan, columns)  => ({
            type: "Project",
            input: plan,
            columns: columns.map(c => typeof c === "string" ? col(c).expr : c.expr),
        }),

        filter: (plan, condition)  => ({
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
        return extractColumns(plan.input); // <- 🧠 seguir bajando
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
                    args: [], // (podés mejorar esto si parseás bien)
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
