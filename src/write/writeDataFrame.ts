// src/write/algebra.ts
import type { SparkSession } from "../client/session";
import {DFAlg, ExprAlg} from "../algebra/read";
import {DFWritingAlg} from "../algebra/write";

export interface DFWritingExec<W> {
    run(root: W, session: SparkSession): Promise<void>;
}

/** TF full-lazy: el programa de write recibe DF/EX e instancia desde el child R. */
export type DFWritingProgramFull<R, E, G, W, CDF = unknown, CEX = unknown> = (
    WR:  DFWritingAlg<R, W>,
    DF:  DFAlg<R, E, G, CDF>,
    EX:  ExprAlg<E> & CEX
) => W;
