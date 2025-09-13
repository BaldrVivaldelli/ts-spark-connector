// src/write/algebra.ts
import type { SparkSession } from "../client/session";
import {DFAlg, ExprAlg} from "../algebra";

export type SaveMode = "append" | "overwrite" | "ignore" | "error" | "errorifexists";
export type WriterFormat =
    | "parquet"
    | "csv"
    | "json"
    | "orc"
    | "avro";


export interface WriterSpec {
    format?: WriterFormat;
    mode?: SaveMode;
    options: Record<string, string>;
    partitionBy: string[];
    bucketBy?: { numBuckets: number; columns: string[] };
    sortBy: string[];
    target?: { path?: string; table?: string };
    viewName?: string;
    asTempView?: boolean;
    registerView?: { name: string; replace: boolean };
}

/** Álgebra TF de escritura: el child es el MISMO tipo R del DF. */
export interface DFWritingAlg<R, W> {
    fromChild(child: R): W;
    format(w: W, fmt: WriterFormat): W;
    mode(w: W, m: SaveMode): W;
    option(w: W, k: string, v: string): W;
    options(w: W, opts: Record<string,string>): W;
    partitionBy(w: W, ...cols: string[]): W;
    bucketBy(w: W, numBuckets: number, col: string, ...cols: string[]): W;
    sortBy(w: W, col: string, ...cols: string[]): W;
    targetPath(w: W, path: string): W;
    targetTable(w: W, table: string): W;
    createOrReplaceTempView(w: W, viewName: string): W;
    createTempView(w: W, name: string): W;
}

export interface DFWritingExec<W> {
    run(root: W, session: SparkSession): Promise<void>;
}

/** TF full-lazy: el programa de write recibe DF/EX e instancia desde el child R. */
// writeDataFrame.ts
export type DFWritingProgramFull<R, E, G, W, CDF = unknown, CEX = unknown> = (
    WR:  DFWritingAlg<R, W>,
    DF:  DFAlg<R, E, G, CDF>,
    EX:  ExprAlg<E> & CEX
) => W;
