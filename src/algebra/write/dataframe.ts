import type { WriteCore, WBatch, WStream } from "./write-core";
import type { BucketSortCap, ViewsCap, BatchFormatCap } from "./write-batch-capabilities";
import type {
    OutputModeCap, TriggerCap, CheckpointCap, QueryNameCap, StreamLiftCap, StreamFormatCap
} from "./write-stream-capabilities";

export type BatchWriterAlg<R> =
    WriteCore<R, WBatch> &
    BatchFormatCap<WBatch> &
    BucketSortCap<WBatch> &
    ViewsCap<WBatch>;

export type StreamWriterAlg<R> =
    WriteCore<R, WStream> &
    StreamLiftCap<R> &         // ← writeStream(df)
    StreamFormatCap &          // ← AQUÍ
    OutputModeCap<WStream> &
    TriggerCap<WStream> &
    CheckpointCap<WStream> &
    QueryNameCap<WStream>;
