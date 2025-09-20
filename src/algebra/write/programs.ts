import type { WriteCore } from "./write-core";
import type { ModeCap, BucketSortCap, ViewsCap } from "./write-batch-capabilities";
import type {
    OutputModeCap,
    TriggerCap,
    CheckpointCap,
    QueryNameCap,
} from "./write-stream-capabilities";

export type DFWritingAlg<R, W> =
    WriteCore<R, W> &
    ModeCap<W> &
    BucketSortCap<W> &
    ViewsCap<W>;



export type StreamWritingAlg<R, W> =
    WriteCore<R, W> &
    OutputModeCap<W> &
    TriggerCap<W> &
    CheckpointCap<W> &
    QueryNameCap<W>;