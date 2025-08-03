// src/engine/sparkConnectEnums.ts


export const DEFAULT_JOIN_TYPE: JoinTypeInput = "INNER";

// Enum exacto como lo espera Spark Connect
export enum JoinType {
    JOIN_TYPE_INNER = 1,
    JOIN_TYPE_LEFT_OUTER = 2,
    JOIN_TYPE_RIGHT_OUTER = 3,
    JOIN_TYPE_FULL_OUTER = 4,
    JOIN_TYPE_LEFT_SEMI = 5,
    JOIN_TYPE_LEFT_ANTI = 6,
}

// Inputs válidos para el usuario (como en PySpark)
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

// Mapeo de strings legibles a enums gRPC
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

// Función para convertir el input del usuario al enum numérico
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
    GROUP_TYPE_PIVOT = 4, // si algún día lo soportás
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