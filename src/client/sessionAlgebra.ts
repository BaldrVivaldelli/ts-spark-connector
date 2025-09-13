import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import {SqlCap} from "../algebra/batch-capabilities";

export interface SessionAlgebra {
    sql<R = unknown, E = unknown, G = unknown>(query: string): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>;
    table<R = unknown, E = unknown, G = unknown>(name: string): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>;
}
