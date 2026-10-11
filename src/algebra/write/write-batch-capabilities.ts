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
export interface ViewsCap<W> {
    createOrReplaceTempView(w: W, viewName: string): W;
    createTempView(w: W, name: string): W;
    registerView?(w: W, name: string, replace: boolean): W;
}
