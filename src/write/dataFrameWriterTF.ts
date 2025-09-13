import { ProtoWriteRoot, ProtoWritingAlg } from "./compilerWrite";
import { SparkSession } from "../client/session";
import {
    DFWritingAlg,
    DFWritingExec,
    DFWritingProgramFull,
    SaveMode,
    WriterFormat
} from "./writeDataFrame";
import { ProtoDFAlg, ProtoExprAlg } from "../engine/compilerRead";
import { ProtoWritingExec } from "./protoWriterExec";
import { DFAlg, DFProgram, ExprAlg } from "../algebra";

// ==================== tipos base ====================
type DefaultR = any;
export type DefaultW = ProtoWriteRoot;

// El dataframe "fuente" expone sesión y programa (incluye capacidades)
export type ReadDFPublic<R, E, G, CDF = unknown, CEX = unknown> = {
    getSession(): SparkSession;
    getProgram(): DFProgram<R, E, G, CDF, CEX>;
};
export type ReadDFPrivate<R, E, G, CDF = unknown, CEX = unknown> = {
    _getSession(): SparkSession;
    _getProgram(): DFProgram<R, E, G, CDF, CEX>;
};
export type ReadDFAny<R, E, G, CDF = unknown, CEX = unknown> =
    | ReadDFPublic<R, E, G, CDF, CEX>
    | ReadDFPrivate<R, E, G, CDF, CEX>;

function sessionOf<R, E, G, CDF, CEX>(df: ReadDFAny<R, E, G, CDF, CEX>): SparkSession {
    return (df as any).getSession?.() ?? (df as any)._getSession();
}
function programOf<R, E, G, CDF, CEX>(
    df: ReadDFAny<R, E, G, CDF, CEX>
): DFProgram<R, E, G, CDF, CEX> {
    return (df as any).getProgram?.() ?? (df as any)._getProgram();
}

// Backend requerido para ejecutar el write (parametrizado por caps)
type Impl<R, E, G, W, CDF, CEX> = {
    DF: DFAlg<R, E, G, CDF>;
    EX: ExprAlg<E> & CEX;
    WR: DFWritingAlg<R, W>;
    EXE: DFWritingExec<W>;
};

// alias para no repetir
type WProg<R, E, G, W, CDF, CEX> = DFWritingProgramFull<R, E, G, W, CDF, CEX>;

// ==================== Writer ====================
export class DataFrameWriterTF<
    R = DefaultR,
    E = unknown,
    G = unknown,
    W = DefaultW,
    CDF = unknown,
    CEX = unknown
> {
    private constructor(
        private readonly session: SparkSession,
        private readonly dfProgram: DFProgram<R, E, G, CDF, CEX>,
        private readonly wProgram: WProg<R, E, G, W, CDF, CEX>,
        private readonly WR?: DFWritingAlg<R, W>,
        private readonly EXE?: DFWritingExec<W>,
        private readonly DF?: DFAlg<R, E, G, CDF>,
        private readonly EX?: ExprAlg<E> & CEX
    ) {}

    static fromDF<R, E, G, W = DefaultW, CDF = unknown, CEX = unknown>(
        df: ReadDFAny<R, E, G, CDF, CEX>
    ): DataFrameWriterTF<R, E, G, W, CDF, CEX> {
        const session = sessionOf(df);
        const dfProgram = programOf<R, E, G, CDF, CEX>(df);
        const wProgram: WProg<R, E, G, W, CDF, CEX> = (WR, DF, EX) =>
            WR.fromChild(dfProgram(DF, EX));
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX>(
            session,
            dfProgram,
            wProgram
        );
    }

    static fromParts<R, E, G, W = DefaultW, CDF = unknown, CEX = unknown>(args: {
        session: SparkSession;
        dfProgram: DFProgram<R, E, G, CDF, CEX>;
        wProgram?: WProg<R, E, G, W, CDF, CEX>;
        WR?: DFWritingAlg<R, W>;
        EXE?: DFWritingExec<W>;
        DF?: DFAlg<R, E, G, CDF>;
        EX?: ExprAlg<E> & CEX;
    }) {
        const wProgram: WProg<R, E, G, W, CDF, CEX> =
            args.wProgram ?? ((WR, DF, EX) => WR.fromChild(args.dfProgram(DF, EX)));
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX>(
            args.session,
            args.dfProgram,
            wProgram,
            args.WR,
            args.EXE,
            args.DF,
            args.EX
        );
    }

    withBackend(impl: Impl<R, E, G, W, CDF, CEX>) {
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX>(
            this.session,
            this.dfProgram,
            this.wProgram,
            impl.WR,
            impl.EXE,
            impl.DF,
            impl.EX
        );
    }

    private chain(
        k: (p: WProg<R, E, G, W, CDF, CEX>) => WProg<R, E, G, W, CDF, CEX>
    ) {
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX>(
            this.session,
            this.dfProgram,
            k(this.wProgram),
            this.WR,
            this.EXE,
            this.DF,
            this.EX
        );
    }

    format(fmt: WriterFormat) {
        return this.chain(p => (WR, DF, EX) => WR.format(p(WR, DF, EX), fmt));
    }
    mode(m: SaveMode) {
        return this.chain(p => (WR, DF, EX) => WR.mode(p(WR, DF, EX), m));
    }
    option(k: string, v: any) {
        return this.chain(p => (WR, DF, EX) => WR.option(p(WR, DF, EX), k, String(v)));
    }
    options(o: Record<string, any>) {
        const norm = Object.fromEntries(Object.entries(o).map(([k, v]) => [k, String(v)]));
        return this.chain(p => (WR, DF, EX) => WR.options(p(WR, DF, EX), norm));
    }
    partitionBy(...cols: string[]) {
        return this.chain(p => (WR, DF, EX) => WR.partitionBy(p(WR, DF, EX), ...cols));
    }
    bucketBy(n: number, col: string, ...cols: string[]) {
        return this.chain(p => (WR, DF, EX) => WR.bucketBy(p(WR, DF, EX), n, col, ...cols));
    }
    sortBy(col: string, ...cols: string[]) {
        return this.chain(p => (WR, DF, EX) => WR.sortBy(p(WR, DF, EX), col, ...cols));
    }

    async save(): Promise<void>;
    async save(path: string): Promise<void>;
    async save(impl: Impl<R, E, G, W, CDF, CEX>): Promise<void>;
    async save(path: string, impl: Impl<R, E, G, W, CDF, CEX>): Promise<void>;
    async save(
        a?: string | Impl<R, E, G, W, CDF, CEX>,
        b?: Impl<R, E, G, W, CDF, CEX>
    ): Promise<void> {
        const path = typeof a === "string" ? a : undefined;
        const impl = (typeof a === "object" ? a : b) ?? this.defaults();
        const { DF, EX, WR, EXE } = impl;
        const w0 = this.wProgram(WR, DF, EX);
        const root = path ? WR.targetPath(w0, path) : w0;
        return EXE.run(root, this.session);
    }

    async createOrReplaceTempView(viewName: string): Promise<void>;
    async createOrReplaceTempView(
        viewName: string,
        impl: Impl<R, E, G, W, CDF, CEX>
    ): Promise<void>;
    async createOrReplaceTempView(
        viewName: string,
        impl?: Impl<R, E, G, W, CDF, CEX>
    ): Promise<void> {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = WR.createOrReplaceTempView(this.wProgram(WR, DF, EX), viewName);
        return EXE.run(root, this.session);
    }

    async createTempView(viewName: string): Promise<void>;
    async createTempView(
        viewName: string,
        impl: Impl<R, E, G, W, CDF, CEX>
    ): Promise<void>;
    async createTempView(
        viewName: string,
        impl?: Impl<R, E, G, W, CDF, CEX>
    ): Promise<void> {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = WR.createTempView(this.wProgram(WR, DF, EX), viewName);
        return EXE.run(root, this.session);
    }

    async saveAsTable(table: string): Promise<void>;
    async saveAsTable(
        table: string,
        impl: Impl<R, E, G, W, CDF, CEX>
    ): Promise<void>;
    async saveAsTable(
        table: string,
        impl?: Impl<R, E, G, W, CDF, CEX>
    ): Promise<void> {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = WR.targetTable(this.wProgram(WR, DF, EX), table);
        return EXE.run(root, this.session);
    }

    parquet() { return this.format("parquet"); }
    csv()     { return this.format("csv"); }
    json()    { return this.format("json"); }
    orc()     { return this.format("orc"); }
    avro()    { return this.format("avro"); }

    private defaults(
        this: DataFrameWriterTF<R, E, G, W, unknown, unknown>
    ): Impl<R, E, G, W, unknown, unknown>;
    private defaults(): Impl<R, E, G, W, CDF, CEX>;
    private defaults(): Impl<R, E, G, W, CDF, CEX> {
        return {
            DF: (this.DF ?? (ProtoDFAlg as DFAlg<R, E, G, CDF>)),
            EX: (this.EX ?? (ProtoExprAlg as ExprAlg<E> & CEX)),
            WR: (this.WR ?? (ProtoWritingAlg as DFWritingAlg<R, W>)),
            EXE: (this.EXE ?? (ProtoWritingExec as DFWritingExec<W>)),
        };
    }
}
