/** @public */
export type SortDirection = "asc" | "desc";
/** @public */
export type NullsOrder = "nullsFirst" | "nullsLast";


/** @public */
export type SortOrder<E> = {
    expr: E;
    direction: SortDirection;
    nulls?: NullsOrder;
};