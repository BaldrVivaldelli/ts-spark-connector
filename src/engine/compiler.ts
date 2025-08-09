import {LogicalPlan, Expression} from "./logicalPlan";
import {
    toProtoJoinType,
    JoinTypeInput,
    DEFAULT_JOIN_TYPE,
    toProtoGroupType,
    toProtoSortDirection, toProtoNullsOrder, toProtoSetOpType
} from "./sparkConnectEnums";

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
            grouping_expressions: groupBy.expressions.map(compileExpression),
            group_type: toProtoGroupType(p.groupType ?? "groupby"),
            aggregate_expressions: Object.entries(p.aggregations).map(([alias, rawFn]) => ({
                alias: {
                    expr: compileExpression(parseFunction(rawFn)),
                    name: [alias],
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


function resolveSortExprAgainstAggregate(e: Expression, child: LogicalPlan): Expression {
    if (child.type !== "Aggregate" || e.type !== "Column") return e;

    const raw = child.aggregations[e.name]; // p.ej. "sum(amount)"
    if (!raw) return e;

    const m = raw.match(/^\s*(\w+)\s*\(\s*([^)]+)\s*\)\s*$/);
    if (!m) return e;

    return {
        type: "UnresolvedFunction",
        name: m[1],                                // "sum"
        args: [{type: "Column", name: m[2]}],    // "amount"
    };
}

function compileSort(p: LogicalPlan & { type: "Sort" }) {
    return {
        sort: {
            input: compileRelation(p.input).relation,
            order: p.orders.map(o => ({
                child: compileExpression(
                    resolveSortExprAgainstAggregate(o.expr, p.input) // si tenés el rewrite de alias→func
                ),
                direction: toProtoSortDirection(o.direction),      // "ASCENDING"/"DESCENDING"
                nulls_first: o.nulls === "nullsFirst"
                    ? true
                    : o.nulls === "nullsLast"
                        ? false
                        : undefined, // omitir si no lo seteaste
            })),
        },
    };
}


function compileLimit(p: LogicalPlan & { type: "Limit" }) {
    return {
        limit: {
            input: compileRelation(p.input).relation,
            limit: p.limit,
        },
    };
}

function compileDistinct(p: LogicalPlan & { type: "Distinct" }) {
    const child = p.input;

    return {
        deduplicate: {
            input: compileRelation(p.input).relation,
            all_columns_as_keys: true
        },
    };
}

function compileUnion(p: LogicalPlan & {
    type: "Union";
    inputs?: LogicalPlan[];
    byName?: boolean;
    allowMissingColumns?: boolean
}) {
    const rels = (p.inputs && p.inputs.length ? p.inputs : [(p as any).left, (p as any).right])
        .filter(Boolean)
        .map(pl => compileRelation(pl).relation);

    if (rels.length < 2) {
        throw new Error("Union requiere al menos dos relaciones.");
    }

    const foldSetOp = (leftRel: any, rightRel: any) => ({
        set_op: {
            left_input: leftRel,
            right_input: rightRel,
            set_op_type: toProtoSetOpType("union"), // UNION
            is_all: true,
            ...(p.byName !== undefined ? {by_name: p.byName} : {}),
            ...(p.allowMissingColumns !== undefined ? {allow_missing_columns: p.allowMissingColumns} : {}),
        }
    });

    let acc = foldSetOp(rels[0], rels[1]);
    for (let i = 2; i < rels.length; i++) {
        acc = foldSetOp(acc, rels[i]);
    }
    return acc;
}

const handlers: Record<LogicalPlan["type"], (plan: any) => Record<"relation", any>
> = {
    Relation: (p) => ({relation: compileRead(p)}),
    Filter: (p) => ({relation: compileFilter(p)}),
    Project: (p) => ({relation: compileProject(p)}),
    Aggregate: (p) => ({relation: compileAggregate(p)}),
    Join: (p) => ({relation: compileJoin(p)}),
    GroupBy: () => {
        throw new Error("GroupBy should not be compiled directly");
    },
    Sort: (p) => ({relation: compileSort(p)}),
    Limit: (p) => ({relation: compileLimit(p)}),
    Distinct: (p) => ({relation: compileDistinct(p)}),
    Union: (p) => ({relation: compileUnion(p)}),
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
            case "UnresolvedFunction":
                return {
                    unresolved_function: {
                        function_name: e.name,
                        arguments: e.args.map(visit),
                    },
                };
            case "UnresolvedStar":
                return {unresolved_star: {}};
            case "CaseWhen": {
                const args: any[] = [];
                for (const b of e.branches) {
                    args.push(compileExpression(b.when));
                    args.push(compileExpression(b.then));
                }
                if (e.else) args.push(compileExpression(e.else));
                return {
                    unresolved_function: {
                        function_name: "case_when",
                        arguments: args,
                    },
                };
            }
            default:
                throw new Error(`Unsupported expression type: ${(e as any).type}`);
        }
    };

    return visit(expr);
}

export function compileToProtobuf(plan: LogicalPlan): any {
    const compiled = compileRelation(plan);
    console.log("[DEBUG] compiled protobuf:", JSON.stringify(compiled));
    return {
        plan: {
            root: compiled.relation
        }
    };
}

