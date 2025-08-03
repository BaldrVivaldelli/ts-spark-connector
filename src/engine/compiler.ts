import {LogicalPlan, Expression} from "./logicalPlan";
import {toProtoJoinType, JoinTypeInput, DEFAULT_JOIN_TYPE} from "./sparkConnectEnums";

function parseFunction(expr: string): Expression {
    const match = expr.match(/^(\w+)\(([^)]+)\)$/);
    if (!match) throw new Error(`Invalid aggregation: ${expr}`);
    const [, fn, arg] = match;
    return {
        type: "UnresolvedFunction",
        name: fn,
        args: [{type: "Column", name: arg}],
    };
}

function compileRead(p: LogicalPlan & { type: "Relation" }) {
    return {
        read: {
            data_source: {
                format: p.format,
                paths: [p.path],
                options: p.options ?? {},
            },
        },
    };
}

function compileFilter(p: LogicalPlan & { type: "Filter" }) {
    return {
        filter: {
            input: compileRelation(p.input).relation,
            condition: compileExpression(p.condition),
        },
    };
}

function compileProject(p: LogicalPlan & { type: "Project" }) {
    return {
        project: {
            input: compileRelation(p.input).relation,
            expressions: p.columns.map(compileExpression),
        },
    };
}

function compileAggregate(p: LogicalPlan & { type: "Aggregate" }) {
    const groupBy = p.input;
    return {
        aggregate: {
            input: compileRelation(groupBy.input).relation,
            grouping_expressions: groupBy.columns.map(compileExpression),
            aggregate_expressions: Object.entries(p.aggregations).map(([alias, rawFn]) => ({
                alias: {
                    expr: compileExpression(parseFunction(rawFn)),
                    name: alias,
                },
            })),
        },
    };
}


function compileJoin(p: LogicalPlan & { type: "Join" }) {
    return {
        join: {
            join_type: toProtoJoinType(p.joinType ?? DEFAULT_JOIN_TYPE),
            left: compileRelation(p.left).relation,
            right: compileRelation(p.right).relation,
            condition: compileExpression(p.on),
        },
    };
}

const handlers: Record<
    LogicalPlan["type"],
    (plan: any) => Record<"relation", any>
> = {
    Relation: (p) => ({relation: compileRead(p)}),
    Filter: (p) => ({relation: compileFilter(p)}),
    Project: (p) => ({relation: compileProject(p)}),
    Aggregate: (p) => ({relation: compileAggregate(p)}),
    Join: (p) => ({relation: compileJoin(p)}),
    GroupBy: () => {
        throw new Error("GroupBy should not be compiled directly");
    },
};

export function compileRelation(plan: LogicalPlan): { relation: any } {
    const handler = handlers[plan.type];
    if (!handler) throw new Error(`Unsupported plan type: ${plan.type}`);
    return handler(plan);
}

export function compileExpression(expr: Expression): any {
    const visit = (e: Expression): any => {
        if (!expr || typeof expr !== "object" || !("type" in expr)) {
            throw new Error(`Invalid expression: ${JSON.stringify(expr)}`);
        }
        switch (e.type) {
            case "Column":
                return {
                    unresolved_attribute: {
                        unparsed_identifier: [e.name],
                    },
                };

            case "Literal":
                return {
                    literal: {
                        [typeof e.value === "number" ? "integer" : "string"]: e.value,
                    },
                };

            case "Binary":
                return {
                    unresolved_function: {
                        function_name: e.op,
                        arguments: [visit(e.left), visit(e.right)],
                    },
                };

            case "Logical":
                return {
                    unresolved_function: {
                        function_name: e.op,
                        arguments: [visit(e.left), visit(e.right)],
                    },
                };

            case "Alias":
                return {
                    alias: {
                        expr: visit(e.input),
                        name: [e.alias],
                    },
                };

            default:
                throw new Error(`Unsupported expression type: ${(e as any).type}`);
        }
    };

    return visit(expr);
}

/*

function compileRelation(plan: LogicalPlan): any {
    if (plan.type === "Relation") {
        return {
            relation: {
                read: {
                    data_source: {
                        format: plan.format,                // ej: "csv", "json", "parquet"
                        paths: [plan.path],                // ej: "/data/people.tsv"
                        options: plan.options ?? {}        // ej: { header: "true", delimiter: "\t" }
                    }
                }
            }
        };
    }

    if (plan.type === "Filter") {
        return {
            relation: {
                filter: {
                    input: compileRelation(plan.input).relation,
                    condition: compileExpression(plan.condition)
                }
            }
        };
    }

    if (plan.type === "Project") {
        return {
            relation: {
                project: {
                    input: compileRelation(plan.input).relation,
                    expressions: plan.columns.map(compileExpression)
                }
            }
        };
    }

    throw new Error(`Unsupported LogicalPlan type: ${JSON.stringify(plan)}`);
}
*/


export function compileToProtobuf(plan: LogicalPlan): any {
    const compiled = compileRelation(plan);
    console.log("[DEBUG] compiled protobuf:", JSON.stringify(compiled));
    return {
        plan: {
            root: compiled.relation
        }
    };
}

