// src/engine/sparkConnectEnums.ts



import {SaveMode} from "../algebra/write";

export type ProtoSortDirection = "ASCENDING" | "DESCENDING";
export type ProtoNullsOrder = "UNSPECIFIED" | "NULLS_FIRST" | "NULLS_LAST";

export const DEFAULT_JOIN_TYPE: JoinTypeInput = "INNER";

export enum JoinType {
    JOIN_TYPE_INNER = 1,
    JOIN_TYPE_LEFT_OUTER = 2,
    JOIN_TYPE_RIGHT_OUTER = 3,
    JOIN_TYPE_FULL_OUTER = 4,
    JOIN_TYPE_LEFT_SEMI = 5,
    JOIN_TYPE_LEFT_ANTI = 6,
}

export type JoinTypeInput =
    | "INNER"
    | "LEFT"
    | "RIGHT"
    | "OUTER"
    | "FULL"
    | "LEFT_OUTER"
    | "RIGHT_OUTER"
    | "FULL_OUTER"
    | "LEFT_SEMI"
    | "LEFT_ANTI";

const joinTypeMap: Record<JoinTypeInput, JoinType> = {
    INNER: JoinType.JOIN_TYPE_INNER,
    LEFT: JoinType.JOIN_TYPE_LEFT_OUTER,
    LEFT_OUTER: JoinType.JOIN_TYPE_LEFT_OUTER,
    RIGHT: JoinType.JOIN_TYPE_RIGHT_OUTER,
    RIGHT_OUTER: JoinType.JOIN_TYPE_RIGHT_OUTER,
    OUTER: JoinType.JOIN_TYPE_FULL_OUTER,
    FULL: JoinType.JOIN_TYPE_FULL_OUTER,
    FULL_OUTER: JoinType.JOIN_TYPE_FULL_OUTER,
    LEFT_SEMI: JoinType.JOIN_TYPE_LEFT_SEMI,
    LEFT_ANTI: JoinType.JOIN_TYPE_LEFT_ANTI,
};

export function toProtoJoinType(joinType: JoinTypeInput): number {
    const normalized = joinType as JoinTypeInput;
    const result = joinTypeMap[normalized];
    if (!result) {
        throw new Error(`Unsupported join type: "${joinType}".`);
    }
    return result;
}

export enum GroupType {
    GROUP_TYPE_GROUPBY = 1,
    GROUP_TYPE_ROLLUP = 2,
    GROUP_TYPE_CUBE = 3,
    GROUP_TYPE_PIVOT = 4,
}

export type GroupTypeInput = "groupby" | "rollup" | "cube" | "pivot";

const groupTypeMap: Record<GroupTypeInput, GroupType> = {
    groupby: GroupType.GROUP_TYPE_GROUPBY,
    rollup: GroupType.GROUP_TYPE_ROLLUP,
    cube: GroupType.GROUP_TYPE_CUBE,
    pivot: GroupType.GROUP_TYPE_PIVOT,
};

export function toProtoGroupType(type: GroupTypeInput = "groupby"): GroupType {
    const value = groupTypeMap[type.toLowerCase() as GroupTypeInput];
    if (!value) throw new Error(`Unsupported group type: ${type}`);
    return value;
}

// 🆕 SetOperation Types
export enum SetOpType {
    SET_OP_TYPE_UNSPECIFIED = 0,
    SET_OP_TYPE_INTERSECT = 1,
    SET_OP_TYPE_UNION = 2,
    SET_OP_TYPE_EXCEPT = 3,
}

export type SetOpTypeInput = "union" | "intersect" | "except";

const setOpTypeMap: Record<SetOpTypeInput, SetOpType> = {
    union: SetOpType.SET_OP_TYPE_UNION,
    intersect: SetOpType.SET_OP_TYPE_INTERSECT,
    except: SetOpType.SET_OP_TYPE_EXCEPT,
};

export function toProtoSetOpType(kind: SetOpTypeInput = "union"): SetOpType {
    const result = setOpTypeMap[kind];
    if (result === undefined) {
        throw new Error(`Unsupported SetOpType: "${kind}".`);
    }
    return result;
}

export function toProtoSortDirection(dir: "asc" | "desc"): ProtoSortDirection {
    return dir === "asc" ? "ASCENDING" : "DESCENDING";
}

export function toProtoNullsOrder(n?: "nullsFirst" | "nullsLast"): ProtoNullsOrder {
    if (!n) return "UNSPECIFIED";
    return n === "nullsFirst" ? "NULLS_FIRST" : "NULLS_LAST";
}


export function toProtoSaveMode(mode: SaveMode | undefined): number {
    if (!mode) return 0; // SAVE_MODE_UNSPECIFIED

    switch (mode) {
        case "append":
            return 1; // SAVE_MODE_APPEND
        case "overwrite":
            return 2; // SAVE_MODE_OVERWRITE
        case "error":
        case "errorifexists":
            return 3; // SAVE_MODE_ERROR_IF_EXISTS
        case "ignore":
            return 4; // SAVE_MODE_IGNORE
        default:
            throw new Error(`Unsupported save mode: ${mode}`);
    }
}

export enum StorageLevel {
    NONE = "NONE",
    DISK_ONLY = "DISK_ONLY",
    MEMORY_ONLY = "MEMORY_ONLY",
    MEMORY_AND_DISK = "MEMORY_AND_DISK",
    MEMORY_ONLY_SER = "MEMORY_ONLY_SER",
    MEMORY_AND_DISK_SER = "MEMORY_AND_DISK_SER",
    OFF_HEAP = "OFF_HEAP",
}

export enum ExplainMode {
    EXPLAIN_MODE_SIMPLE = 1,
    EXPLAIN_MODE_EXTENDED = 2,
    EXPLAIN_MODE_CODEGEN = 3,
    EXPLAIN_MODE_COST = 4,
    EXPLAIN_MODE_FORMATTED = 5,
}

export type ExplainModeInput = "simple" | "extended" | "codegen" | "cost" | "formatted";

const explainModeMap: Record<ExplainModeInput, ExplainMode> = {
    simple: ExplainMode.EXPLAIN_MODE_SIMPLE,
    extended: ExplainMode.EXPLAIN_MODE_EXTENDED,
    codegen: ExplainMode.EXPLAIN_MODE_CODEGEN,
    cost: ExplainMode.EXPLAIN_MODE_COST,
    formatted: ExplainMode.EXPLAIN_MODE_FORMATTED,
};

export function toProtoExplainMode(mode: ExplainModeInput): ExplainMode {
    const m = explainModeMap[mode];
    if (!m) throw new Error(`Unsupported explain mode: ${mode}`);
    return m;
}

export type JoinHintName =
    | "broadcast"
    | "merge"
    | "shuffle_hash"
    | "shuffle_replicate_nl";