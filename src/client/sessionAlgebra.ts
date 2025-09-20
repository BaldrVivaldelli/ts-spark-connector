import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import {SqlCap} from "../algebra/read/batch-capabilities";
import {StreamingReadCap} from "../algebra/read/streaming-capabilities";

export interface SessionAlgebra {
    sql<R = unknown, E = unknown, G = unknown>(query: string): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>;
    table<R = unknown, E = unknown, G = unknown>(name: string): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>;
    readStream<R = unknown, E = unknown, G = unknown>(format: string,options?: Record<string, string>): ReadChainedDataFrame<R, E, G, StreamingReadCap<R>, unknown>;
}
