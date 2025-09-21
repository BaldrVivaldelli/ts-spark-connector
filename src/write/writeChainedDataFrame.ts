// src/write/dataFrameWriterTF.ts
import { SparkSession } from "../client/session";

import { ProtoDFAlg, ProtoExprAlg, ProtoGroup } from "../engine/compilerRead";
import { ProtoWritingExec } from "./protoWriterExec";

import { DFAlg, DFProgram, ExprAlg } from "../algebra/read";
import {
    SaveMode,
    WBatch,
    WStream,
    BatchWriterFormat,
    StreamWriterFormat, WProgram, BatchWProgram, StreamWProgram,
} from "../algebra/write";
import {ProtoWritingAlg} from "./compilerWrite";
import {TraceDFAlg, TraceExprAlg} from "../trace/trace";
import {TraceWriterAlg} from "../trace/traceWriterAlg";
import {BatchWriterAlg, StreamWriterAlg} from "../algebra/write/dataframe";
import {DFWritingExec} from "./writeDataFrame";


// ==================== tipos base ====================
type DefaultR = any;

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
function programOf<R, E, G, CDF, CEX>(df: ReadDFAny<R, E, G, CDF, CEX>): DFProgram<R, E, G, CDF, CEX> {
    return (df as any).getProgram?.() ?? (df as any)._getProgram();
}

type Impl<R, E, G, W, CDF, CEX, WRALG> = {
    DF: DFAlg<R, E, G, CDF>;
    EX: ExprAlg<E> & CEX;
    WR: WRALG;               // álgebra concreto (batch o stream)
    EXE: DFWritingExec<W>;
};

// ==================== Writer ====================
export class DataFrameWriterTF<
    R = DefaultR,
    E = unknown,
    G = unknown,
    W = unknown,         // WBatch | WStream
    CDF = unknown,
    CEX = unknown,
    WRALG = unknown      // Álgebra concreto del writer (batch o stream)
> {
    private constructor(
        private readonly session: SparkSession,
        private readonly dfProgram: DFProgram<R, E, G, CDF, CEX>,
        private readonly wProgram: WProgram<R, E, G, W, CDF, CEX, WRALG>,
        private readonly WR?: WRALG,
        private readonly EXE?: DFWritingExec<W>,
        private readonly DF?: DFAlg<R, E, G, CDF>,
        private readonly EX?: ExprAlg<E> & CEX
    ) {}

    // ==================== FACTORIES por modo ====================
    static fromBatchProgram<R, E, G, CDF, CEX>(
        df: ReadDFAny<R, E, G, CDF, CEX>,
        prog: BatchWProgram<R, E, G, CDF, CEX>
    ): DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, BatchWriterAlg<R>> {
        const session = sessionOf(df);
        const dfProgram = programOf<R, E, G, CDF, CEX>(df);
        const wProgram: WProgram<R, E, G, WBatch, CDF, CEX, BatchWriterAlg<R>> =
            (WR, DF, EX) => prog(WR, DF, EX);
        return new DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, BatchWriterAlg<R>>(session, dfProgram, wProgram);
    }

    static fromStreamProgram<R, E, G, CDF, CEX>(
        df: ReadDFAny<R, E, G, CDF, CEX>,
        prog: StreamWProgram<R, E, G, CDF, CEX>
    ): DataFrameWriterTF<R, E, G, WStream, CDF, CEX, StreamWriterAlg<R>> {
        const session = sessionOf(df);
        const dfProgram = programOf<R, E, G, CDF, CEX>(df);
        const wProgram: WProgram<R, E, G, WStream, CDF, CEX, StreamWriterAlg<R>> =
            (WR, DF, EX) => prog(WR, DF, EX);
        return new DataFrameWriterTF<R, E, G, WStream, CDF, CEX, StreamWriterAlg<R>>(session, dfProgram, wProgram);
    }

    // (Opcional) Factory genérico
    static fromParts<R, E, G, W, CDF, CEX, WRALG>(args: {
        session: SparkSession;
        dfProgram: DFProgram<R, E, G, CDF, CEX>;
        wProgram: WProgram<R, E, G, W, CDF, CEX, WRALG>;
        WR?: WRALG;
        EXE?: DFWritingExec<W>;
        DF?: DFAlg<R, E, G, CDF>;
        EX?: ExprAlg<E> & CEX;
    }) {
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX, WRALG>(
            args.session, args.dfProgram, args.wProgram, args.WR, args.EXE, args.DF, args.EX
        );
    }

    withBackend(impl: Impl<R, E, G, W, CDF, CEX, WRALG>) {
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX, WRALG>(
            this.session, this.dfProgram, this.wProgram, impl.WR, impl.EXE, impl.DF, impl.EX
        );
    }

    private chain(
        k: (p: WProgram<R, E, G, W, CDF, CEX, WRALG>) => WProgram<R, E, G, W, CDF, CEX, WRALG>
    ) {
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX, WRALG>(
            this.session, this.dfProgram, k(this.wProgram), this.WR, this.EXE, this.DF, this.EX
        );
    }

    // ==================== CORE compartido (batch + streaming) ====================
    // Overloads por modo
    format(this: DataFrameWriterTF<R,E,G,WBatch,CDF,CEX,WRALG>, fmt: BatchWriterFormat):
        DataFrameWriterTF<R,E,G,WBatch,CDF,CEX,WRALG>;
    format(this: DataFrameWriterTF<R,E,G,WStream,CDF,CEX,WRALG>, fmt: StreamWriterFormat):
        DataFrameWriterTF<R,E,G,WStream,CDF,CEX,WRALG>;
    format(fmt: any): any {
        return this.chain(p => (WR: WRALG, DF, EX) => (WR as any).format(p(WR, DF, EX), fmt));
    }

    option(k: string, v: any) {
        return this.chain(p => (WR: WRALG, DF, EX) => (WR as any).option(p(WR, DF, EX), k, String(v)));
    }
    options(o: Record<string, any>) {
        const norm = Object.fromEntries(Object.entries(o).map(([k, v]) => [k, String(v)]));
        return this.chain(p => (WR: WRALG, DF, EX) => (WR as any).options(p(WR, DF, EX), norm));
    }
    partitionBy(...cols: string[]) {
        return this.chain(p => (WR: WRALG, DF, EX) => (WR as any).partitionBy(p(WR, DF, EX), ...cols));
    }

    // ==================== CAPACIDADES BATCH (solo WBatch) ====================
    mode(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>, m: SaveMode) {
        const next: WProgram<R, E, G, WBatch, CDF, CEX, WRALG> =
            (WR: WRALG, DF, EX) => (WR as any).mode((this as any).wProgram(WR, DF, EX), m);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }
    bucketBy(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>, n: number, col: string, ...cols: string[]) {
        const next: WProgram<R, E, G, WBatch, CDF, CEX, WRALG> =
            (WR: WRALG, DF, EX) => (WR as any).bucketBy((this as any).wProgram(WR, DF, EX), n, col, ...cols);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }
    sortBy(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>, col: string, ...cols: string[]) {
        const next: WProgram<R, E, G, WBatch, CDF, CEX, WRALG> =
            (WR: WRALG, DF, EX) => (WR as any).sortBy((this as any).wProgram(WR, DF, EX), col, ...cols);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    // ==================== CAPACIDADES STREAMING (solo WStream) ====================
    outputMode(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, m: "append" | "complete" | "update") {
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR: WRALG, DF, EX) => (WR as any).outputMode((this as any).wProgram(WR, DF, EX), m);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }
    trigger(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, t: { processingTime?: string; once?: true; availableNow?: true }) {
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR: WRALG, DF, EX) => (WR as any).trigger((this as any).wProgram(WR, DF, EX), t);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }
    checkpoint(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, path: string) {
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR: WRALG, DF, EX) => (WR as any).option((this as any).wProgram(WR, DF, EX), "checkpointLocation", path);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }
    queryName(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, name: string) {
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR: WRALG, DF, EX) => (WR as any).queryName
                ? (WR as any).queryName((this as any).wProgram(WR, DF, EX), name)
                : (WR as any).option((this as any).wProgram(WR, DF, EX), "queryName", name);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    // ==================== EJECUCIÓN / TARGETS ====================
    async save(): Promise<void>;
    async save(path: string): Promise<void>;
    async save(impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async save(path: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async save(a?: string | Impl<R, E, G, W, CDF, CEX, WRALG>, b?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        const path = typeof a === "string" ? a : undefined;
        const impl = (typeof a === "object" ? a : b) ?? this.defaults();
        const { DF, EX, WR, EXE } = impl;
        const w0 = this.wProgram(WR as WRALG, DF, EX);
        const root = path ? (WR as any).targetPath(w0, path) : w0;
        return EXE.run(root, this.session);
    }

    async createOrReplaceTempView(viewName: string): Promise<void>;
    async createOrReplaceTempView(viewName: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async createOrReplaceTempView(viewName: string, impl?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = (WR as any).createOrReplaceTempView(this.wProgram(WR as WRALG, DF, EX), viewName);
        return EXE.run(root, this.session);
    }

    async createTempView(viewName: string): Promise<void>;
    async createTempView(viewName: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async createTempView(viewName: string, impl?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = (WR as any).createTempView(this.wProgram(WR as WRALG, DF, EX), viewName);
        return EXE.run(root, this.session);
    }

    async saveAsTable(table: string): Promise<void>;
    async saveAsTable(table: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async saveAsTable(table: string, impl?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = (WR as any).targetTable(this.wProgram(WR as WRALG, DF, EX), table);
        return EXE.run(root, this.session);
    }

    // ==================== helpers de formato ====================
    // (solo batch; evitamos que aparezcan en streaming)
    parquet(this: DataFrameWriterTF<R,E,G,WBatch,CDF,CEX,WRALG>) {
        return this.format("parquet");
    }
    csv(this: DataFrameWriterTF<R,E,G,WBatch,CDF,CEX,WRALG>) {
        return this.format("csv");
    }
    json(this: DataFrameWriterTF<R,E,G,WBatch,CDF,CEX,WRALG>) {
        return this.format("json");
    }
    orc(this: DataFrameWriterTF<R,E,G,WBatch,CDF,CEX,WRALG>) {
        return this.format("orc");
    }
    avro(this: DataFrameWriterTF<R,E,G,WBatch,CDF,CEX,WRALG>) {
        return this.format("avro");
    }

    // ==================== backends por defecto ====================
    private defaults(
        this: DataFrameWriterTF<R, E, ProtoGroup, W, unknown, unknown, WRALG>
    ): Impl<R, E, ProtoGroup, W, unknown, unknown, WRALG>;
    private defaults(): Impl<R, E, G, W, CDF, CEX, WRALG>;
    private defaults(): any {
        if (this.DF && this.EX && this.WR && this.EXE) {
            return { DF: this.DF, EX: this.EX, WR: this.WR, EXE: this.EXE };
        }
        // Por defecto usamos los intérpretes Proto* (siempre que el overload typed calce)
        return {
            DF:  ProtoDFAlg,
            EX:  ProtoExprAlg,
            WR:  ProtoWritingAlg as unknown as WRALG, // implementa ambas APIs (batch/stream)
            EXE: ProtoWritingExec,
        };
    }

    // ==================== SERIALIZACIÓN DEL WRITER (para tests) ====================
    // Construimos el nodo de writer con trazas y lo serializamos directo.
    toClientASTJSON(): string {
        const DF = TraceDFAlg as any;
        const EX = TraceExprAlg as any;
        const WR = TraceWriterAlg as any; // no requiere toJSON; devolvemos el nodo tal cual

        const wnode = this.wProgram(WR, DF, EX) as any;

        // Normalizamos un shape estable si preferís (descomentar):
        // const stable = {
        //   node: /stream/i.test(String(wnode?.kind)) ? "writeStream" : "write",
        //   format: wnode?.format,
        //   options: wnode?.options ?? {},
        //   partitionBy: wnode?.partitionBy ?? [],
        //   mode: wnode?.mode,
        //   outputMode: wnode?.outputMode,
        //   trigger: wnode?.trigger,
        //   queryName: wnode?.queryName,
        //   target: wnode?.target ?? {},
        //   child: wnode?.child,
        // };
        // return JSON.stringify(stable, null, 2);

        return JSON.stringify(wnode, null, 2);
    }

    toClientASTMermaid(): string {
        const DF = TraceDFAlg as any;
        const EX = TraceExprAlg as any;
        const WR = TraceWriterAlg as any;

        const wnode = this.wProgram(WR, DF, EX) as any;

        const kind = wnode?.kind ?? (wnode?.node ?? "writer");
        const isStream = /stream/i.test(String(kind));
        const id = isStream ? "WriteStream" : "Write";

        const fmt = wnode?.format ?? "-";
        const mode = wnode?.mode ?? "-";
        const outMode = wnode?.outputMode ?? "-";
        const trig =
            wnode?.trigger?.processingTime ??
            (wnode?.trigger?.once ? "once" :
                wnode?.trigger?.availableNow ? "availableNow" : "-");
        const chk = wnode?.options?.checkpointLocation ?? "-";
        const qn  = wnode?.queryName ?? "-";
        const part = Array.isArray(wnode?.partitionBy) && wnode.partitionBy.length
            ? wnode.partitionBy.join(",")
            : "-";

        let target = "no-target";
        if (wnode?.target?.path)  target = `path:${wnode.target.path}`;
        if (wnode?.target?.table) target = `table:${wnode.target.table}`;
        if (wnode?.target?.name)  target = `${wnode?.target?.replace ? "createOrReplace" : "create"}:${wnode.target.name}`;

        const caption =
            `${id}\\nformat=${fmt}\\nmode=${mode}\\n` +
            `outputMode=${outMode}\\ntrigger=${trig}\\n` +
            `checkpoint=${chk}\\nqueryName=${qn}\\n` +
            `partitionBy=${part}\\ntarget=${target}`;

        return `flowchart TD
  W(["${caption}"])
  C["DF pipeline"]
  W --> C
`;
    }
}
