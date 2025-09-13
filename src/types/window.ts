import { SortDirection, NullsOrder } from "./sorting";


export type FrameType = "rows" | "range";


export type FrameBoundary =
    | { type: "UnboundedPreceding" }
    | { type: "UnboundedFollowing" }
    | { type: "CurrentRow" }
    | { type: "ValuePreceding"; value: number }
    | { type: "ValueFollowing"; value: number };


export type WindowSpec<E> = {
    partitionBy: E[];
    orderBy: Array<{ input: E; direction: SortDirection; nulls?: NullsOrder }>;
    frame?: { type: FrameType; start: FrameBoundary; end: FrameBoundary };
};