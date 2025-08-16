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

// DF “lector”: soportamos públicos o internos (elegí uno en tu ReadChainedDataFrame)
export type ReadDFPublic<R,E,G>  = { getSession(): SparkSession; getProgram(): DFProgram<R,E,G> };
export type ReadDFPrivate<R,E,G> = { _getSession(): SparkSession; _getProgram(): DFProgram<R,E,G> };
export type ReadDFAny<R,E,G> = ReadDFPublic<R,E,G> | ReadDFPrivate<R,E,G>;

function sessionOf<R,E,G>(df: ReadDFAny<R,E,G>): SparkSession {
    return (df as any).getSession?.() ?? (df as any)._getSession();
}
function programOf<R,E,G>(df: ReadDFAny<R,E,G>): DFProgram<R,E,G> {
    return (df as any).getProgram?.() ?? (df as any)._getProgram();
}

export class DataFrameWriterTF<R = DefaultR, E = unknown, G = unknown, W = DefaultW> {
    private constructor(
        private readonly session:   SparkSession,
        private readonly dfProgram: DFProgram<R,E,G>,
        private readonly wProgram:  DFWritingProgramFull<R,E,G,W>,
        private readonly WR?:  DFWritingAlg<R,W>,
        private readonly EXE?: DFWritingExec<W>,
        private readonly DF?:  DFAlg<R,E,G>,
        private readonly EX?:  ExprAlg<E>,
    ) {}

    /** Forma corta: construir desde un DataFrame (para `df.write`) */
    static fromDF<R,E,G,W = DefaultW>(df: ReadDFAny<R,E,G>) {
        const session   = sessionOf(df);
        const dfProgram = programOf<R,E,G>(df);
        const wProgram: DFWritingProgramFull<R,E,G,W> =
            (WR, DF, EX) => WR.fromChild(dfProgram(DF, EX));   // full-lazy
        return new DataFrameWriterTF<R,E,G,W>(session, dfProgram, wProgram);
    }

    /** Forma larga: si querés inyectar todo a mano */
    static fromParts<R,E,G,W = DefaultW>(args: {
        session: SparkSession,
        dfProgram: DFProgram<R,E,G>,
        wProgram?: DFWritingProgramFull<R,E,G,W>,
        WR?: DFWritingAlg<R,W>,
        EXE?: DFWritingExec<W>,
        DF?: DFAlg<R,E,G>,
        EX?: ExprAlg<E>,
    }) {
        const wProgram = args.wProgram ?? ((WR, DF, EX) => WR.fromChild(args.dfProgram(DF, EX)));
        return new DataFrameWriterTF<R,E,G,W>(
            args.session, args.dfProgram, wProgram, args.WR, args.EXE, args.DF, args.EX
        );
    }

    /** Inyectar backend por si no querés pasarlo en cada save() */
    withBackend(impl: { DF: DFAlg<R,E,G>, EX: ExprAlg<E>, WR: DFWritingAlg<R,W>, EXE: DFWritingExec<W> }) {
        return new DataFrameWriterTF<R,E,G,W>(
            this.session, this.dfProgram, this.wProgram, impl.WR, impl.EXE, impl.DF, impl.EX
        );
    }

    // ---------- encadenables (TF, no ejecutan) ----------
    private chain(k: (p: DFWritingProgramFull<R,E,G,W>) => DFWritingProgramFull<R,E,G,W>) {
        return new DataFrameWriterTF<R,E,G,W>(
            this.session, this.dfProgram, k(this.wProgram), this.WR, this.EXE, this.DF, this.EX
        );
    }

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

    // ---------- acciones (recién acá compila/ejecuta) ----------
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

    private defaults() {
        return {
            DF:  (this.DF  ?? (ProtoDFAlg      as unknown as DFAlg<R,E,G>)),
            EX:  (this.EX  ?? (ProtoExprAlg    as unknown as ExprAlg<E>)),
            WR:  (this.WR  ?? (ProtoWritingAlg as unknown as DFWritingAlg<R,W>)),
            EXE: (this.EXE ?? (ProtoWritingExec as unknown as DFWritingExec<W>)),
        };
    }
}
