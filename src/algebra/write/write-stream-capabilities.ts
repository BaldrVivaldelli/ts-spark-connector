// src/write/stream-capabilities.ts
import {StreamWriterFormat, WStream} from "./write-core";

/** @public */
export type OutputMode = "append" | "update" | "complete";

/** @public */
export type Trigger =
    | { kind: "ProcessingTime"; intervalMs: number }
    | { kind: "Once" }
    | { kind: "Continuous"; checkpointIntervalMs: number };

/** @internal */
export interface OutputModeCap<W> {
    outputMode(w: W, m: OutputMode): W;
}

/** @internal */
export interface TriggerCap<W> {
    trigger(w: W, t: Trigger): W;
}

/** @internal */
export interface CheckpointCap<W> {
    checkpoint(w: W, location: string): W;
}

/** @internal */
export interface QueryNameCap<W> {
    queryName(w: W, name: string): W;
}
/** @internal */
export interface StreamLiftCap<R> {
    writeStream(df: R): WStream;
}
/** @internal */
export interface StreamFormatCap {
    format(w: WStream, fmt: StreamWriterFormat): WStream;
}
