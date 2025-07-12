export type LogicalPlan =
    | { type: "Relation"; format: "csv"; options: Record<string, string>; path: string }
    | { type: "Project"; input: LogicalPlan; columns: Expression[] }
    | { type: "Filter"; input: LogicalPlan; condition: Expression };

export type Expression =
    | { type: "Column"; name: string }
    | { type: "Literal"; value: string | number }
    | { type: "Binary"; op: string; left: Expression; right: Expression };
