import { LogicalPlan, Expression } from "./logicalPlan";

function compileExpression(expr: Expression): any {
    if (expr.type === "Column") {
        return { unresolved_attribute: { unparsed_identifier: [expr.name] } };
    }
    if (expr.type === "Literal") {
        return { literal: { integer: expr.value } };
    }
    if (expr.type === "Binary") {
        return {
            unresolved_function: {
                function_name: expr.op,
                arguments: [compileExpression(expr.left), compileExpression(expr.right)]
            }
        };
    }
    throw new Error("Unsupported expression type");
}

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


export function compileToProtobuf(plan: LogicalPlan): any {
    const compiled = compileRelation(plan);
    console.log("[DEBUG] compiled protobuf:", JSON.stringify(compiled));
    return {
        plan: {
            root: compiled.relation
        }
    };
}

