import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import {SqlCap} from "../algebra/read/batch-capabilities";
import {StreamingMark, StreamingReadCap} from "../algebra/read";
import { UnknownSchema } from "../schema/schema-model";

export interface SessionAlgebra {
    sql<R = unknown, E = unknown, G = unknown>(query: string): ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown>;
    table<R = unknown, E = unknown, G = unknown>(name: string): ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown>;
    readStream<R = unknown, E = unknown, G = unknown>(format: string,options?: Record<string, string>): ReadChainedDataFrame<UnknownSchema, R, E, G, StreamingReadCap<R> & StreamingMark<R>, unknown>;
}
