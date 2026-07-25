// src/write/dataFrameWriterTF.ts
import { SparkSession } from "../client/session";

import { ProtoDFAlg, ProtoExprAlg } from "../engine/compilerRead";
import { ProtoWritingExec } from "./protoWriterExec";

import { DFAlg, DFProgram, ExprAlg } from "../algebra/read";
import {
    SaveMode,
    WBatch,
    WStream,
    WProgram,
    BatchWProgram,
    StreamWProgram,
} from "../algebra/write";

import { ProtoWritingAlg } from "./compilerWrite";
import { TraceDFAlg, TraceExprAlg } from "../trace/trace";
import {
    TraceWriterAlg,
    TWNode,
    writerToClientASTMermaid,
} from "../trace/traceWriterAlg";
import { BatchWriterAlg, StreamWriterAlg } from "../algebra/write/dataframe";
import { DFWritingExec } from "./writeDataFrame";
import { StreamingQueryHandle } from "../client/sparkClient";

// ==================== tipos base ====================
export type DefaultR = unknown;
const PROTO_INT32_MAX = 2_147_483_647;
export type WriterOptionValue = string | number | boolean | bigint;
export type StreamTriggerInput =
    | { processingTime: string }
    | { once: true }
    | { availableNow: true }
    | { continuous: string }
    | { kind: "ProcessingTime"; intervalMs: number }
    | { kind: "Once" }
    | { kind: "Continuous"; checkpointIntervalMs: number };

function requireNonEmpty(value: string, label: string): string {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${label} must be a non-empty string.`);
    }
    return value;
}

function requireColumns(columns: string[], label: string): string[] {
    if (columns.length === 0) {
        throw new TypeError(`${label} requires at least one column.`);
    }
    for (const column of columns) requireNonEmpty(column, `${label} column`);
    return columns;
}

function normalizeOptionValue(value: WriterOptionValue, label: string): string {
    if (value === null || value === undefined) {
        throw new TypeError(`${label} cannot be null or undefined.`);
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
        throw new RangeError(`${label} must be a finite number.`);
    }
    return String(value);
}

function validateTrigger(trigger: StreamTriggerInput): StreamTriggerInput {
    if (!trigger || typeof trigger !== "object") {
        throw new TypeError("trigger() requires a trigger object.");
    }
    const selected = ["processingTime", "once", "availableNow", "continuous", "kind"]
        .filter(key => key in trigger);
    if (selected.length !== 1) {
        throw new TypeError("trigger() requires exactly one trigger kind.");
    }
    if ("processingTime" in trigger) {
        requireNonEmpty(trigger.processingTime, "trigger() processingTime");
    } else if ("continuous" in trigger) {
        requireNonEmpty(trigger.continuous, "trigger() continuous interval");
    } else if ("once" in trigger && trigger.once !== true) {
        throw new TypeError("trigger() once must be true.");
    } else if ("availableNow" in trigger && trigger.availableNow !== true) {
        throw new TypeError("trigger() availableNow must be true.");
    } else if ("kind" in trigger) {
        if (!["ProcessingTime", "Once", "Continuous"].includes(trigger.kind)) {
            throw new RangeError(`Unsupported trigger kind: ${String(trigger.kind)}.`);
        }
        if (trigger.kind === "ProcessingTime"
            && (!Number.isSafeInteger(trigger.intervalMs) || trigger.intervalMs <= 0)) {
            throw new RangeError("trigger() intervalMs must be a positive safe integer.");
        }
        if (trigger.kind === "Continuous"
            && (!Number.isSafeInteger(trigger.checkpointIntervalMs)
                || trigger.checkpointIntervalMs <= 0)) {
            throw new RangeError("trigger() checkpointIntervalMs must be a positive safe integer.");
        }
    }
    return trigger;
}

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

function isPublicReadDF<R, E, G, CDF, CEX>(
    df: ReadDFAny<R, E, G, CDF, CEX>
): df is ReadDFPublic<R, E, G, CDF, CEX> {
    return "getSession" in df && "getProgram" in df;
}

function sessionOf<R, E, G, CDF, CEX>(df: ReadDFAny<R, E, G, CDF, CEX>): SparkSession {
    return isPublicReadDF(df) ? df.getSession() : df._getSession();
}

function programOf<R, E, G, CDF, CEX>(df: ReadDFAny<R, E, G, CDF, CEX>): DFProgram<R, E, G, CDF, CEX> {
    return isPublicReadDF(df) ? df.getProgram() : df._getProgram();
}

export type Impl<R, E, G, W, CDF, CEX, WRALG> = {
    DF: DFAlg<R, E, G, CDF>;
    EX: ExprAlg<E> & CEX;
    WR: WRALG;
    EXE: DFWritingExec<W>;
};

type SharedWriterRuntimeAlg<W> = {
    format(w: W, fmt: string): W;
    option(w: W, k: string, v: string): W;
    options(w: W, opts: Record<string, string>): W;
    partitionBy(w: W, ...cols: string[]): W;
    targetPath(w: W, path: string): W;
    targetTable(w: W, table: string): W;
    createOrReplaceTempView?(w: W, viewName: string): W;
    createTempView?(w: W, viewName: string): W;
};

type BatchWriterRuntimeAlg<W> = SharedWriterRuntimeAlg<W> & {
    mode(w: W, m: SaveMode): W;
    bucketBy(w: W, n: number, col: string, ...cols: string[]): W;
    sortBy(w: W, col: string, ...cols: string[]): W;
};

type StreamWriterRuntimeAlg<W> = SharedWriterRuntimeAlg<W> & {
    outputMode(w: W, m: "append" | "complete" | "update"): W;
    trigger(w: W, t: StreamTriggerInput): W;
    queryName?(w: W, name: string): W;
    start?(w: W): W;
    fromTempView?(w: W, name: string): W;
    awaitTermination?(w: W): W;
};

// ==================== Writer ====================
export class DataFrameWriterTF<
    R = DefaultR,
    E = unknown,
    G = unknown,
    W = unknown,
    CDF = unknown,
    CEX = unknown,
    WRALG = unknown
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
            args.session,
            args.dfProgram,
            args.wProgram,
            args.WR,
            args.EXE,
            args.DF,
            args.EX
        );
    }

    withBackend(impl: Impl<R, E, G, W, CDF, CEX, WRALG>) {
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX, WRALG>(
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
        nextProgram: (p: WProgram<R, E, G, W, CDF, CEX, WRALG>) => WProgram<R, E, G, W, CDF, CEX, WRALG>
    ) {
        return new DataFrameWriterTF<R, E, G, W, CDF, CEX, WRALG>(
            this.session,
            this.dfProgram,
            nextProgram(this.wProgram),
            this.WR,
            this.EXE,
            this.DF,
            this.EX
        );
    }

    private sharedWriterAlg(WR: WRALG): SharedWriterRuntimeAlg<W> {
        return WR as unknown as SharedWriterRuntimeAlg<W>;
    }

    private batchWriterAlg(WR: WRALG): BatchWriterRuntimeAlg<WBatch> {
        return WR as unknown as BatchWriterRuntimeAlg<WBatch>;
    }

    private streamWriterAlg(WR: WRALG): StreamWriterRuntimeAlg<WStream> {
        return WR as unknown as StreamWriterRuntimeAlg<WStream>;
    }

    private requireWriteMethod<T>(method: T | undefined, name: string): T {
        if (!method) {
            throw new Error(`The configured writer backend does not implement ${name}().`);
        }
        return method;
    }

    format(this: DataFrameWriterTF<R, E, G, W, CDF, CEX, WRALG>, fmt: string): DataFrameWriterTF<R, E, G, W, CDF, CEX, WRALG> {
        requireNonEmpty(fmt, "format()");
        return this.chain(program => (WR, DF, EX) =>
            this.sharedWriterAlg(WR).format(program(WR, DF, EX), fmt)
        );
    }

    option(k: string, v: WriterOptionValue) {
        requireNonEmpty(k, "option() key");
        const normalized = normalizeOptionValue(v, `option(${JSON.stringify(k)}) value`);
        return this.chain(program => (WR, DF, EX) =>
            this.sharedWriterAlg(WR).option(program(WR, DF, EX), k, normalized)
        );
    }

    options(opts: Record<string, WriterOptionValue>) {
        const normalized = Object.fromEntries(
            Object.entries(opts).map(([key, value]) => [
                requireNonEmpty(key, "options() key"),
                normalizeOptionValue(value, `options()[${JSON.stringify(key)}]`),
            ])
        );
        return this.chain(program => (WR, DF, EX) =>
            this.sharedWriterAlg(WR).options(program(WR, DF, EX), normalized)
        );
    }

    partitionBy(...cols: string[]) {
        requireColumns(cols, "partitionBy()");
        return this.chain(program => (WR, DF, EX) =>
            this.sharedWriterAlg(WR).partitionBy(program(WR, DF, EX), ...cols)
        );
    }

    mode(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>, m: SaveMode) {
        if (!["append", "overwrite", "ignore", "error", "errorifexists"].includes(m)) {
            throw new RangeError(`Unsupported save mode: ${String(m)}.`);
        }
        const next: WProgram<R, E, G, WBatch, CDF, CEX, WRALG> =
            (WR, DF, EX) => this.batchWriterAlg(WR).mode(this.wProgram(WR, DF, EX) as WBatch, m);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    bucketBy(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>, n: number, col: string, ...cols: string[]) {
        if (!Number.isSafeInteger(n) || n <= 0 || n > PROTO_INT32_MAX) {
            throw new RangeError(
                `bucketBy() numBuckets must be an integer between 1 and ${PROTO_INT32_MAX}.`
            );
        }
        requireColumns([col, ...cols], "bucketBy()");
        const next: WProgram<R, E, G, WBatch, CDF, CEX, WRALG> =
            (WR, DF, EX) => this.batchWriterAlg(WR).bucketBy(this.wProgram(WR, DF, EX) as WBatch, n, col, ...cols);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    sortBy(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>, col: string, ...cols: string[]) {
        requireColumns([col, ...cols], "sortBy()");
        const next: WProgram<R, E, G, WBatch, CDF, CEX, WRALG> =
            (WR, DF, EX) => this.batchWriterAlg(WR).sortBy(this.wProgram(WR, DF, EX) as WBatch, col, ...cols);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    outputMode(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, m: "append" | "complete" | "update") {
        if (!["append", "complete", "update"].includes(m)) {
            throw new RangeError(`Unsupported output mode: ${String(m)}.`);
        }
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR, DF, EX) => this.streamWriterAlg(WR).outputMode(this.wProgram(WR, DF, EX) as WStream, m);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    trigger(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, t: StreamTriggerInput) {
        validateTrigger(t);
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR, DF, EX) => this.streamWriterAlg(WR).trigger(this.wProgram(WR, DF, EX) as WStream, t);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    checkpoint(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, path: string) {
        requireNonEmpty(path, "checkpoint() path");
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR, DF, EX) => this.streamWriterAlg(WR).option(this.wProgram(WR, DF, EX) as WStream, "checkpointLocation", path);
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    queryName(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, name: string) {
        requireNonEmpty(name, "queryName()");
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR, DF, EX) => {
                const writerAlg = this.streamWriterAlg(WR);
                const queryName = writerAlg.queryName;
                return queryName
                    ? queryName(this.wProgram(WR, DF, EX) as WStream, name)
                    : writerAlg.option(this.wProgram(WR, DF, EX) as WStream, "queryName", name);
            };
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    async save(): Promise<void>;
    async save(path: string): Promise<void>;
    async save(impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async save(path: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async save(a?: string | Impl<R, E, G, W, CDF, CEX, WRALG>, b?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        const path = typeof a === "string" ? a : undefined;
        if (path !== undefined) requireNonEmpty(path, "save() path");
        const impl = (typeof a === "object" ? a : b) ?? this.defaults();
        const { DF, EX, WR, EXE } = impl;
        const writer = this.wProgram(WR, DF, EX);
        const root = path ? this.sharedWriterAlg(WR).targetPath(writer, path) : writer;
        return EXE.run(root, this.session);
    }

    async createOrReplaceTempView(viewName: string): Promise<void>;
    async createOrReplaceTempView(viewName: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async createOrReplaceTempView(viewName: string, impl?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        requireNonEmpty(viewName, "createOrReplaceTempView() name");
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const method = this.requireWriteMethod(this.sharedWriterAlg(WR).createOrReplaceTempView, "createOrReplaceTempView");
        const root = method(this.wProgram(WR, DF, EX), viewName);
        return EXE.run(root, this.session);
    }

    async createTempView(viewName: string): Promise<void>;
    async createTempView(viewName: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async createTempView(viewName: string, impl?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        requireNonEmpty(viewName, "createTempView() name");
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const method = this.requireWriteMethod(this.sharedWriterAlg(WR).createTempView, "createTempView");
        const root = method(this.wProgram(WR, DF, EX), viewName);
        return EXE.run(root, this.session);
    }

    async saveAsTable(table: string): Promise<void>;
    async saveAsTable(table: string, impl: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void>;
    async saveAsTable(table: string, impl?: Impl<R, E, G, W, CDF, CEX, WRALG>): Promise<void> {
        requireNonEmpty(table, "saveAsTable() table");
        const { DF, EX, WR, EXE } = impl ?? this.defaults();
        const root = this.sharedWriterAlg(WR).targetTable(this.wProgram(WR, DF, EX), table);
        return EXE.run(root, this.session);
    }

    parquet(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>) {
        return this.format("parquet");
    }

    csv(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>) {
        return this.format("csv");
    }

    json(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>) {
        return this.format("json");
    }

    orc(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>) {
        return this.format("orc");
    }

    avro(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>) {
        return this.format("avro");
    }

    text(this: DataFrameWriterTF<R, E, G, WBatch, CDF, CEX, WRALG>) {
        return this.format("text");
    }

    console(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>) {
        return this.format("console");
    }

    kafka(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>) {
        return this.format("kafka");
    }

    memory(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>) {
        return this.format("memory");
    }

    async start(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, path?: string): Promise<StreamingQueryHandle> {
        if (path !== undefined) requireNonEmpty(path, "start() path");
        const { DF, EX, WR, EXE } = this.defaults();
        const writerNode = this.wProgram(WR, DF, EX) as WStream;
        const start = this.requireWriteMethod(this.streamWriterAlg(WR).start, "start");
        const targeted = path === undefined
            ? writerNode
            : this.sharedWriterAlg(WR).targetPath(writerNode, path) as WStream;
        return EXE.runStream(start(targeted), this.session);
    }

    async toTable(
        this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>,
        table: string,
    ): Promise<StreamingQueryHandle> {
        requireNonEmpty(table, "toTable() table");
        const { DF, EX, WR, EXE } = this.defaults();
        const writerNode = this.wProgram(WR, DF, EX) as WStream;
        const start = this.requireWriteMethod(this.streamWriterAlg(WR).start, "start");
        const targeted = this.sharedWriterAlg(WR).targetTable(writerNode, table) as WStream;
        return EXE.runStream(start(targeted), this.session);
    }

    async startAndAwaitTermination(
        this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>,
        path?: string,
    ): Promise<StreamingQueryHandle> {
        if (path !== undefined) requireNonEmpty(path, "startAndAwaitTermination() path");
        const { DF, EX, WR, EXE } = this.defaults();
        const writerNode = this.wProgram(WR, DF, EX) as WStream;
        const awaitTermination = this.requireWriteMethod(
            this.streamWriterAlg(WR).awaitTermination,
            "awaitTermination",
        );
        const start = this.requireWriteMethod(this.streamWriterAlg(WR).start, "start");
        const targeted = path === undefined
            ? writerNode
            : this.sharedWriterAlg(WR).targetPath(writerNode, path) as WStream;
        return EXE.runStream(start(awaitTermination(targeted)), this.session);
    }

    fromTempView(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>, name: string) {
        requireNonEmpty(name, "fromTempView() name");
        return this.chain(program => (WR, DF, EX) => {
            const method = this.requireWriteMethod(this.streamWriterAlg(WR).fromTempView, "fromTempView");
            return method(program(WR, DF, EX) as WStream, name);
        });
    }

    awaitTermination(this: DataFrameWriterTF<R, E, G, WStream, CDF, CEX, WRALG>) {
        const next: WProgram<R, E, G, WStream, CDF, CEX, WRALG> =
            (WR, DF, EX) => {
                const method = this.requireWriteMethod(this.streamWriterAlg(WR).awaitTermination, "awaitTermination");
                return method(this.wProgram(WR, DF, EX) as WStream);
            };
        return new DataFrameWriterTF(this.session, this.dfProgram, next, this.WR, this.EXE, this.DF, this.EX);
    }

    private defaults(): Impl<R, E, G, W, CDF, CEX, WRALG> {
        if (this.DF && this.EX && this.WR && this.EXE) {
            return { DF: this.DF, EX: this.EX, WR: this.WR, EXE: this.EXE };
        }

        return {
            DF: ProtoDFAlg as unknown as DFAlg<R, E, G, CDF>,
            EX: ProtoExprAlg as unknown as ExprAlg<E> & CEX,
            WR: ProtoWritingAlg as unknown as WRALG,
            EXE: ProtoWritingExec as unknown as DFWritingExec<W>,
        };
    }

    private buildTraceWriterNode(): TWNode {
        const DF = TraceDFAlg as unknown as DFAlg<unknown, unknown, unknown, unknown>;
        const EX = TraceExprAlg as unknown as ExprAlg<unknown>;
        const WR = TraceWriterAlg as unknown as SharedWriterRuntimeAlg<TWNode>;
        return this.wProgram(WR as unknown as WRALG, DF as unknown as DFAlg<R, E, G, CDF>, EX as ExprAlg<E> & CEX) as unknown as TWNode;
    }

    toClientASTJSON(): string {
        return JSON.stringify(this.buildTraceWriterNode(), null, 2);
    }

    toClientASTMermaid(): string {
        return writerToClientASTMermaid(this.buildTraceWriterNode());
    }
}
