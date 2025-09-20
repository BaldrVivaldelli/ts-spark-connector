// src/write/write-batch-capabilities.ts
import type { SaveMode } from "./write-core";

export interface ModeCap<W> {
    mode(w: W, m: SaveMode): W;
}

export interface BucketSortCap<W> {
    bucketBy(w: W, numBuckets: number, col: string, ...cols: string[]): W;
    sortBy(w: W, col: string, ...cols: string[]): W;
}

export interface ViewsCap<W> {
    createOrReplaceTempView(w: W, viewName: string): W;
    createTempView(w: W, name: string): W;
    registerView?(w: W, name: string, replace: boolean): W;
}
