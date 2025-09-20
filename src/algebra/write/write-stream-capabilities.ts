// src/write/stream-capabilities.ts
import {StreamWriterFormat, WStream} from "./write-core";

export type OutputMode = "append" | "update" | "complete";

export type Trigger =
    | { kind: "ProcessingTime"; intervalMs: number }
    | { kind: "Once" }
    | { kind: "Continuous"; checkpointIntervalMs: number };

export interface OutputModeCap<W> {
    outputMode(w: W, m: OutputMode): W;
}

export interface TriggerCap<W> {
    trigger(w: W, t: Trigger): W;
}

export interface CheckpointCap<W> {
    checkpoint(w: W, location: string): W;
}

export interface QueryNameCap<W> {
    queryName(w: W, name: string): W;
}
export interface StreamLiftCap<R> {
    writeStream(df: R): WStream;
}
export interface StreamFormatCap {
    format(w: WStream, fmt: StreamWriterFormat): WStream;
}
