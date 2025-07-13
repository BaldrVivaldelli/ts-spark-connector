export type GroupBy = {
    type: "GroupBy";
    input: LogicalPlan;
    columns: Expression[];
};
export type LogicalPlan =
    | { type: "Relation"; format: string; path: string; options?: Record<string, string> }
    | { type: "Filter"; input: LogicalPlan; condition: Expression }
    | { type: "Project"; input: LogicalPlan; columns: Expression[] }
    | { type: "Aggregate"; input: GroupBy; aggregations: Record<string, string> }
    | { type: "GroupBy"; input: LogicalPlan; groupBy: Expression[] }
    | { type: "Join"; left: LogicalPlan; right: LogicalPlan; on: Expression };

export type Expression =
    | { type: "Column"; name: string }
    | { type: "Literal"; value: string | number | boolean }
    | { type: "Binary"; op: string; left: Expression; right: Expression }
    | { type: "Logical"; op: "AND" | "OR"; left: Expression; right: Expression }
    | { type: "Alias"; input: Expression; alias: string }
    | { type: "UnresolvedFunction"; name: string; args: Expression[] };
