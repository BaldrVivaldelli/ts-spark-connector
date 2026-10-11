import { SortDirection, NullsOrder } from "./sorting";


/** @public */
export type FrameType = "rows" | "range";


/** @public */
export type FrameBoundary =
    | { type: "UnboundedPreceding" }
    | { type: "UnboundedFollowing" }
    | { type: "CurrentRow" }
    | { type: "ValuePreceding"; value: number }
    | { type: "ValueFollowing"; value: number };

/**
 * Alias of `FrameBoundary`, kept for compatibility.
 *
 * @deprecated Use `FrameBoundary`; both names refer to the same type. This alias
 * will be removed from the root entry point in the next major release.
 * @public
 */
export type WindowFrameBoundary = FrameBoundary;


/** @public */
export type WindowSpec<E> = {
    partitionBy: E[];
    orderBy: Array<{ input: E; direction: SortDirection; nulls?: NullsOrder }>;
    frame?: { type: FrameType; start: FrameBoundary; end: FrameBoundary };
};