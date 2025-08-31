import {ReadChainedDataFrame} from "../read/readChainedDataFrame";

export interface SessionAlgebra {
    sql<R = unknown, E = unknown, G = unknown>(query: string): ReadChainedDataFrame<R, E, G>;
    table<R = unknown, E = unknown, G = unknown>(name: string): ReadChainedDataFrame<R, E, G>;
}
