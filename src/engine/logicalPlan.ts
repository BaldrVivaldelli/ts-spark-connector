import { GroupTypeInput, JoinTypeInput } from "./sparkConnectEnums";
import {FrameBoundary} from "./column";
import {FrameType, SortOrder} from "../read/readDataframe";

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
    | {
    type: "Aggregate";
    input: GroupBy;
    // ⬇⬇⬇ AQUÍ el cambio clave
    aggregations: Record<string, Expression>;
    groupType?: GroupTypeInput;
}
    | { type: "GroupBy"; input: LogicalPlan; expressions: Expression[] }
    | { type: "Join"; left: LogicalPlan; right: LogicalPlan; on: Expression; joinType: JoinTypeInput }
    | { type: "Sort"; input: LogicalPlan; orders: SortOrder<any>[] }
    | { type: "Limit"; input: LogicalPlan; limit: number }
    | { type: "Distinct"; input: LogicalPlan }
    | { type: "Union"; inputs: LogicalPlan[]; byName?: boolean; allowMissingColumns?: boolean };

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
