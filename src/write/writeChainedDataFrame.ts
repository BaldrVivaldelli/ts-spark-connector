// src/write/dataFrameWriterTF.ts
import { ProtoWriteRoot, ProtoWritingAlg } from "./compilerWrite";
import { SparkSession } from "../client/session";
import {
    DFWritingAlg, DFWritingExec, DFWritingProgramFull,
    SaveMode, WriterFormat
} from "./writeDataFrame";
import { DFAlg, DFProgram, ExprAlg } from "../read/readDataframe";
import { ProtoDFAlg, ProtoExprAlg } from "../engine/compilerRead";
import { ProtoWritingExec } from "./protoWriterExec";

type DefaultR = any;               // ProtoRel
type DefaultW = ProtoWriteRoot;

type ReadDFPublic<R,E,G>  = { getSession(): SparkSession; getProgram(): DFProgram<R,E,G> };
type ReadDFPrivate<R,E,G> = { _getSession(): SparkSession; _getProgram(): DFProgram<R,E,G> };
type ReadDFAny<R,E,G> = ReadDFPublic<R,E,G> | ReadDFPrivate<R,E,G>;

// type guards
const isPrivate = <R,E,G>(x: any): x is ReadDFPrivate<R,E,G> =>
    typeof x?._getSession === "function" && typeof x?._getProgram === "function";
const isPublic =  <R,E,G>(x: any): x is ReadDFPublic<R,E,G>  =>
    typeof x?.getSession === "function" && typeof x?.getProgram === "function";

export class DataFrameWriterTF<R = DefaultR, E = unknown, G = unknown, W = DefaultW> {
    private readonly session:   SparkSession;
    private readonly dfProgram: DFProgram<R,E,G>;
    private readonly wProgram:  DFWritingProgramFull<R,E,G,W>;
    private readonly WR?:  DFWritingAlg<R,W>;
    private readonly EXE?: DFWritingExec<W>;
    private readonly DF?:  DFAlg<R,E,G>;
    private readonly EX?:  ExprAlg<E>;

    // -------- SOBRECARGAS (IMPORTANTE: incluir la de 1 parámetro) --------
    constructor(df: ReadDFAny<R,E,G>);
    constructor(
        session: SparkSession,
        dfProgram: DFProgram<R,E,G>,
        wProgram?: DFWritingProgramFull<R,E,G,W>,
        WR?: DFWritingAlg<R,W>,
        EXE?: DFWritingExec<W>,
        DF?: DFAlg<R,E,G>,
        EX?: ExprAlg<E>
    );

    // -------- IMPLEMENTACIÓN ÚNICA --------
    constructor(
        a: ReadDFAny<R,E,G> | SparkSession,
        b?: DFProgram<R,E,G>,
        c?: DFWritingProgramFull<R,E,G,W>,
        d?: DFWritingAlg<R,W>,
        e?: DFWritingExec<W>,
        f?: DFAlg<R,E,G>,
        g?: ExprAlg<E>
    ) {
        if (isPrivate<R,E,G>(a)) {
            this.session   = a._getSession();
            this.dfProgram = a._getProgram();
            this.wProgram  = (WR, DF, EX) => WR.fromChild(this.dfProgram(DF, EX)); // full-lazy
        } else if (isPublic<R,E,G>(a)) {
            this.session   = a.getSession();
            this.dfProgram = a.getProgram();
            this.wProgram  = (WR, DF, EX) => WR.fromChild(this.dfProgram(DF, EX)); // full-lazy
        } else {
            // forma larga
            this.session   = a as SparkSession;
            this.dfProgram = b!;
            this.wProgram  = c ?? ((WR, DF, EX) => WR.fromChild(this.dfProgram(DF, EX)));
            this.WR = d; this.EXE = e; this.DF = f; this.EX = g;
        }
    }

    private chain(k: (p: DFWritingProgramFull<R,E,G,W>) => DFWritingProgramFull<R,E,G,W>) {
        return new DataFrameWriterTF<R,E,G,W>(
            this.session, this.dfProgram, k(this.wProgram), this.WR, this.EXE, this.DF, this.EX
        );
    }

    // -------- encadenables (TF, no ejecutan) --------
    format(fmt: WriterFormat)      { return this.chain(p => (WR,DF,EX) => WR.format(p(WR,DF,EX), fmt)); }
    mode(m: SaveMode)              { return this.chain(p => (WR,DF,EX) => WR.mode(p(WR,DF,EX), m)); }
    option(k: string, v: any)      { return this.chain(p => (WR,DF,EX) => WR.option(p(WR,DF,EX), k, String(v))); }
    options(o: Record<string, any>) {
        const norm = Object.fromEntries(Object.entries(o).map(([k,v]) => [k, String(v)]));
        return this.chain(p => (WR,DF,EX) => WR.options(p(WR,DF,EX), norm));
    }
    partitionBy(...cols: string[]) { return this.chain(p => (WR,DF,EX) => WR.partitionBy(p(WR,DF,EX), ...cols)); }
    bucketBy(n: number, col: string, ...cols: string[]) {
        return this.chain(p => (WR,DF,EX) => WR.bucketBy(p(WR,DF,EX), n, col, ...cols));
    }
    sortBy(col: string, ...cols: string[]) {
        return this.chain(p => (WR,DF,EX) => WR.sortBy(p(WR,DF,EX), col, ...cols));
    }

    // -------- acciones (recién acá compila/ejecuta) --------
    async save(path?: string, impl?: { DF: DFAlg<R,E,G>, EX: ExprAlg<E>, WR: DFWritingAlg<R,W>, EXE: DFWritingExec<W> }) {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const w0   = this.wProgram(WR, DF, EX);
        const root = path ? WR.targetPath(w0, path) : w0;
        return EXE.run(root, this.session);
    }

    async saveAsTable(table: string, impl?: { DF: DFAlg<R,E,G>, EX: ExprAlg<E>, WR: DFWritingAlg<R,W>, EXE: DFWritingExec<W> }) {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = WR.targetTable(this.wProgram(WR, DF, EX), table);
        return EXE.run(root, this.session);
    }

    withBackend(impl: { DF: DFAlg<R,E,G>, EX: ExprAlg<E>, WR: DFWritingAlg<R,W>, EXE: DFWritingExec<W> }) {
        return new DataFrameWriterTF<R,E,G,W>(
            this.session, this.dfProgram, this.wProgram, impl.WR, impl.EXE, impl.DF, impl.EX
        );
    }

    private defaults() {
        return {
            DF:  (this.DF  ?? (ProtoDFAlg    as unknown as DFAlg<R,E,G>)),
            EX:  (this.EX  ?? (ProtoExprAlg  as unknown as ExprAlg<E>)),
            WR:  (this.WR  ?? (ProtoWritingAlg as unknown as DFWritingAlg<R,W>)),
            EXE: (this.EXE ?? (ProtoWritingExec as unknown as DFWritingExec<W>)),
        };
    }
}
