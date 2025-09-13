import {GroupTypeInput, JoinHintName, JoinTypeInput} from "./sparkConnectEnums";
import {FrameBoundary} from "./column";
import {FrameType, SortOrder} from "../types";

export interface WindowSpecExpr {
    partitionBy: Expression[];
    orderBy: Array<{
        type: "SortKey";
        input: Expression;
        direction: "asc" | "desc";
        nulls?: "nullsFirst" | "nullsLast";
    }>;
    frame?: { type: FrameType; start: FrameBoundary; end: FrameBoundary };
}

// ⬇ Unificado: siempre 'expressions'
export type GroupBy = {
    type: "GroupBy";
    input: LogicalPlan;
    expressions: Expression[];
};

export type LogicalPlan =
    | { type: "Relation"; format: string; path: string | string[]; options?: Record<string, string> }
    | { type: "Filter"; input: LogicalPlan; condition: Expression }
    | { type: "Project"; input: LogicalPlan; columns: Expression[] }
    | { type: "Aggregate";input: GroupBy;aggregations: Record<string, Expression>;groupType?: GroupTypeInput;}
    | { type: "GroupBy"; input: LogicalPlan; expressions: Expression[] }
    | { type: "Join"; left: LogicalPlan; right: LogicalPlan; on: Expression; joinType: JoinTypeInput }
    | { type: "Sort"; input: LogicalPlan; orders: SortOrder<any>[] }
    | { type: "Limit"; input: LogicalPlan; limit: number }
    | { type: "Distinct"; input: LogicalPlan }
    | { type: "Union"; inputs: LogicalPlan[]; byName?: boolean; allowMissingColumns?: boolean }
    | { type: "Describe"; input: LogicalPlan; columns: Expression[] }
    | { type: "Summary"; input: LogicalPlan; metrics: Expression[]; columns: Expression[] }
    | { type: "Cache"; input: LogicalPlan }
    | { type: "Persist"; input: LogicalPlan; level?: string }
    | { type: "Unpersist"; input: LogicalPlan; blocking?: boolean }
    | { type: "Repartition"; input: LogicalPlan; numPartitions: number; shuffle: boolean }
    | { type: "Coalesce"; input: LogicalPlan; numPartitions: number }
    | { type: "Sql"; query: string }
    | { type: "Hint"; name: JoinHintName | string; params?: any[]; child: LogicalPlan }
    | { type: "Sample"; input: LogicalPlan; lowerBound: number; upperBound: number; withReplacement?: boolean; seed?: number; deterministicOrder?: boolean }
    | { type: "Drop"; input: LogicalPlan; columnNames: string[] };


export type Expression =
    | { type: "Column"; name: string }
    | { type: "Literal"; value: string | number | boolean }
    | { type: "Binary"; op: string; left: Expression; right: Expression }
    | { type: "Logical"; op: "AND" | "OR"; left: Expression; right: Expression }
    | { type: "Alias"; input: Expression; alias: string }
    | { type: "UnresolvedFunction"; name: string; args: Expression[] }
    | { type: "SortKey"; input: Expression; direction: "asc" | "desc"; nulls?: "nullsFirst" | "nullsLast" }
    | { type: "UnresolvedStar" }
    | { type: "CaseWhen"; branches: Array<{ when: Expression; then: Expression }>; else?: Expression }
    | { type: "Window"; func: Expression; spec: WindowSpecExpr };
