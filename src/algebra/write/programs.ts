// src/write/programs.ts
import type { DFAlg, ExprAlg } from "../read";
import type { WBatch, WStream } from "./write-core";
import type { BatchWriterAlg, StreamWriterAlg } from "./dataframe";

/**
 * Programa genérico de escritura (batch o streaming).
 * WRALG = álgebra concreta del writer (BatchWriterAlg<R> o StreamWriterAlg<R>).
 */
export type WProgram<
    R, E, G,            // tipos del mundo DF
    W,                  // nodo del writer (WBatch | WStream)
    CDF = unknown,
    CEX = unknown,
    WRALG = unknown
> = (
    WR: WRALG,
    DF: DFAlg<R, E, G, CDF>,
    EX: ExprAlg<E> & CEX
) => W;

/** Programa especializado para BATCH */
export type BatchWProgram<
    R, E, G, CDF = unknown, CEX = unknown
> = WProgram<R, E, G, WBatch, CDF, CEX, BatchWriterAlg<R>>;

/** Programa especializado para STREAMING */
export type StreamWProgram<
    R, E, G, CDF = unknown, CEX = unknown
> = WProgram<R, E, G, WStream, CDF, CEX, StreamWriterAlg<R>>;
