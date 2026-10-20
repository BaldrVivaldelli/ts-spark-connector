// src/write/write-batch-capabilities.ts
import type { SaveMode } from "./write-core";

/** @internal */
export interface BatchFormatCap<W> {
    mode(w: W, m: SaveMode): W;
}

/** @internal */
export interface BucketSortCap<W> {
    bucketBy(w: W, numBuckets: number, col: string, ...cols: string[]): W;
    sortBy(w: W, col: string, ...cols: string[]): W;
}

/** @internal */
export interface InsertIntoCap<W> {
    /** Positional insert into an existing table (`WriteOperation.table.save_method = INSERT_INTO`). */
    insertInto(w: W, table: string): W;
}

/** @internal */
export interface ViewsCap<W> {
    createOrReplaceTempView(w: W, viewName: string): W;
    createTempView(w: W, name: string): W;
    /** Global temp views live in the `global_temp` database and outlive the session. */
    createGlobalTempView(w: W, name: string): W;
    createOrReplaceGlobalTempView(w: W, name: string): W;
    registerView?(w: W, name: string, replace: boolean): W;
}
