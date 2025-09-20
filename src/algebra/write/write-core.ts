// src/write/write-core.ts
export type SaveMode = "append" | "overwrite" | "ignore" | "error" | "errorifexists";
export type WriterFormat = "parquet" | "csv" | "json" | "orc" | "avro";

export interface WriterTarget {
    path?: string;
    table?: string;
}

export interface WriterCommonSpec {
    format?: WriterFormat;
    options: Record<string, string>;
    partitionBy: string[];
    target?: WriterTarget;
}

/** Núcleo común del algebra de write (batch/stream). */
export interface WriteCore<R, W> {
    /** crea el writer a partir del DF hijo (mismo R que tu DFCore) */
    fromChild(child: R): W;

    // comunes a ambos mundos
    format(w: W, fmt: WriterFormat): W;
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
}
