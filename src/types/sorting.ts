export type SortDirection = "asc" | "desc";
export type NullsOrder = "nullsFirst" | "nullsLast";


export type SortOrder<E> = {
    expr: E;
    direction: SortDirection;
    nulls?: NullsOrder;
};