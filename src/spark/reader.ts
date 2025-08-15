// DataFrameReaderTF.ts
import { ChainedDataFrame } from "./ChainedDataFrame";
import { SparkSession } from "./session";
import { DFProgram } from "./dataframe";

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
    csv(...paths: string[]): ChainedDataFrame<R,E,G> {
        return this.make("csv", paths.length <= 1 ? paths[0] : paths);
    }
    json(...paths: string[]): ChainedDataFrame<R,E,G> {
        return this.make("json", paths.length <= 1 ? paths[0] : paths);
    }
    parquet(...paths: string[]): ChainedDataFrame<R,E,G> {
        return this.make("parquet", paths.length <= 1 ? paths[0] : paths);
    }

    load(...paths: string[]): ChainedDataFrame<R,E,G> {
        const fmt = this.fmt ?? "parquet";
        return this.make(fmt, paths.length <= 1 ? paths[0] : paths);
    }

    table(name: string): ChainedDataFrame<R,E,G> {
        return this.make("table", name);
    }

    // ---------- interno ----------
    private make(format: string, path: string | string[]): ChainedDataFrame<R,E,G> {
        const prog: DFProgram<R,E,G> = (DF) => DF.relation(format, path, this.opts);
        return new ChainedDataFrame<R,E,G>(prog, this.session);
    }
}
