// src/write/stream-capabilities.ts
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
