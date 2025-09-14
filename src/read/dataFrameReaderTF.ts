// src/read/DataFrameReaderTF.ts
import { ReadChainedDataFrame } from "./readChainedDataFrame";
import { SparkSession } from "../client/session";
import {DFProgram} from "../algebra";

type Opts = Record<string, string>;

export class DataFrameReaderTF<R = unknown, E = unknown, G = unknown>  {
    private readonly fmt?: string;
    private readonly opts: Opts;

    constructor(private readonly session: SparkSession, fmt?: string, opts: Opts = {}) {
        this.fmt = fmt;
        this.opts = opts;
    }

    format(fmt: string): DataFrameReaderTF<R,E,G> {
        return new DataFrameReaderTF<R,E,G>(this.session, fmt, this.opts);
    }

    option(key: string, value: string): DataFrameReaderTF<R,E,G> {
        return new DataFrameReaderTF<R,E,G>(this.session, this.fmt, { ...this.opts, [key]: value });
    }

    options(kv: Opts): DataFrameReaderTF<R,E,G> {
        return new DataFrameReaderTF<R,E,G>(this.session, this.fmt, { ...this.opts, ...kv });
    }

    // Atajos con multipath
    csv(...paths: string[]): ReadChainedDataFrame<R,E,G> {
        return this.make("csv", paths.length <= 1 ? paths[0] : paths);
    }
    json(...paths: string[]): ReadChainedDataFrame<R,E,G> {
        return this.make("json", paths.length <= 1 ? paths[0] : paths);
    }
    parquet(...paths: string[]): ReadChainedDataFrame<R,E,G> {
        return this.make("parquet", paths.length <= 1 ? paths[0] : paths);
    }

    load(...paths: string[]): ReadChainedDataFrame<R,E,G> {
        const fmt = this.fmt ?? "parquet";
        return this.make(fmt, paths.length <= 1 ? paths[0] : paths);
    }

    table(name: string): ReadChainedDataFrame<R,E,G> {
        return this.make("table", name);
    }

    sql(query: string): ReadChainedDataFrame<R, E, G> {
        return this.make("sql", query);
    }


    // ---------- interno ----------
    private make(format: string, path: string | string[]): ReadChainedDataFrame<R,E,G> {
        const prog: DFProgram<R,E,G> = (DF) => DF.relation(format, path, this.opts);
        return new ReadChainedDataFrame<R,E,G>(prog, this.session);
    }
}
