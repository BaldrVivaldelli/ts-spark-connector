import { ChainedDataFrame } from "./ChainedDataFrame";
import { SparkSession } from "./session";
import {DFProgram} from "./dataframe";

type Opts = Record<string, string>;

export class DataFrameReaderTF {
    private readonly fmt?: string;
    private readonly opts: Opts;

    constructor(private readonly session: SparkSession, fmt?: string, opts: Opts = {}) {
        this.fmt = fmt;
        this.opts = opts;
    }

    /** Igual que PySpark: setea el formato y devuelve un reader nuevo (inmutable). */
    format(fmt: string): DataFrameReaderTF {
        return new DataFrameReaderTF(this.session, fmt, this.opts);
    }

    /** Opción individual (inmutable). */
    option(key: string, value: string): DataFrameReaderTF {
        return new DataFrameReaderTF(this.session, this.fmt, { ...this.opts, [key]: value });
    }

    /** Varias opciones (inmutable). */
    options(kv: Opts): DataFrameReaderTF {
        return new DataFrameReaderTF(this.session, this.fmt, { ...this.opts, ...kv });
    }

    /** Atajo: csv(path) */
    csv(path: string): ChainedDataFrame {
        return this.make("csv", path);
    }
    /** Atajo: json(path) */
    json(path: string): ChainedDataFrame {
        return this.make("json", path);
    }
    /** Atajo: parquet(path) */
    parquet(path: string): ChainedDataFrame {
        return this.make("parquet", path);
    }

    /** Carga genérica respetando `format()` y `options()` previos. */
    load(path: string): ChainedDataFrame {
        const fmt = this.fmt ?? "parquet"; // decisión por defecto (ajusta si querés otro default)
        return this.make(fmt, path);
    }

    /** Leer tabla por nombre (si tu backend lo soporta como "format: table"). */
    table(name: string): ChainedDataFrame {
        return this.make("table", name);
    }

    // ---------- interno ----------
    private make(format: string, path: string): ChainedDataFrame {
        // 👇 ESTA es la clave: devolvemos un PROGRAMA TF, no un LogicalPlan.
        const prog: DFProgram<any, any, any> = (DF /*, EX */) => DF.relation(format, path, this.opts);
        return new ChainedDataFrame(prog, this.session);
    }
}
