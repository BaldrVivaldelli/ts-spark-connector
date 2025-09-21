// src/write/write-core.ts
export type SaveMode = "append" | "overwrite" | "ignore" | "error" | "errorifexists";
export type BatchWriterFormat = "parquet" | "csv" | "json" | "orc" | "avro";

export type StreamWriterFormat = "console" | "kafka" | "memory";

export interface WriterTarget {
    path?: string;
    table?: string;
}

export interface WriterCommonSpec {
    format?: BatchWriterFormat;
    options: Record<string, string>;
    partitionBy: string[];
    target?: WriterTarget;
}

/** Núcleo común del algebra de write (batch/stream). */
export interface WriteCore<R, W> {
    /** crea el writer a partir del DF hijo (mismo R que tu DFCore) */
    fromChild(child: R): W;

    // comunes a ambos mundos
    format(w: W, fmt: BatchWriterFormat): W;
    option(w: W, k: string, v: string): W;
    options(w: W, opts: Record<string, string>): W;
    partitionBy(w: W, ...cols: string[]): W;

    // destino
    targetPath(w: W, path: string): W;
    targetTable(w: W, table: string): W;
}

export interface WriterSpec extends WriterCommonSpec {
    mode?: SaveMode;
    bucketBy?: { numBuckets: number; columns: string[] };
    sortBy: string[];
    viewName?: string;
    asTempView?: boolean;
    registerView?: { name: string; replace: boolean };
    outputMode?: "append" | "complete" | "update";
    trigger?: { processingTime?: string } | { once?: boolean } | { availableNow?: boolean };
    queryName?: string;
}
export interface WBatchBrand { __wflavor?: "batch" }
export interface WStreamBrand { __wflavor?: "stream" }
export type WBatch = {} & WBatchBrand;
export type WStream = {} & WStreamBrand;