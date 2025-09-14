import { SparkDFAlg, SparkExprAlg } from "./readDataFrameInterpreter";
import { LogicalPlan } from "../engine/logicalPlan";
import { ProtoDFAlg, ProtoExec, ProtoExprAlg } from "../engine/compilerRead";
import { SparkSession } from "../client/session";
import {
    DEFAULT_JOIN_TYPE,
    ExplainModeInput,
    JoinHintName,
    JoinTypeInput,
} from "../engine/sparkConnectEnums";
import { printArrowResults } from "../utils/arrowPrinter";
import {DataFrameWriterTF, DefaultW} from "../write/dataFrameWriterTF";
import { toJSON, toMermaid } from "../trace/traceSerializers";
import { TraceDFAlg, TraceExprAlg } from "../trace/trace";
import { NullsOrder, SortOrder } from "../types";
import {DFAlg, DFProgram, EventTimeWatermarkCap, ExprAlg, StreamingReadCap} from "../algebra";
import {CacheCap, HintCap, RepartitionCap, SamplingCap, SqlCap} from "../algebra/batch-capabilities";

export type EBuilder = { build<E>(EX: ExprAlg<E>): E };
export type SortKeyInput =
    | string
    | EBuilder
    | ((EX: ExprAlg<any>) => SortOrder<any>);

export const col = (name: string): EBuilder => ({ build: EX => EX.col(name) });
export const lit = (v: string | number | boolean): EBuilder => ({ build: EX => EX.lit(v) });
export const eq = (l: EBuilder, r: EBuilder | string | number | boolean): EBuilder => ({
    build: EX => EX.bin("=", l.build(EX), typeof r === "object" ? (r as EBuilder).build(EX) : EX.lit(r as any))
});
export const asc = (e: EBuilder, nulls?: NullsOrder) =>
    (EX: ExprAlg<any>): SortOrder<any> => ({ expr: e.build(EX), direction: "asc", nulls });
export const desc = (e: EBuilder, nulls?: NullsOrder) =>
    (EX: ExprAlg<any>): SortOrder<any> => ({ expr: e.build(EX), direction: "desc", nulls });

export class ReadChainedDataFrame<R, E, G, CDF = {}, CEX = {}> {
    private readonly prog: DFProgram<R, E, G, CDF, CEX>;

    constructor(p: DFProgram<R, E, G, CDF, CEX>, private readonly session: SparkSession) {
        this.prog = p;
    }

    static fromCSV<R, E, G, CDF = {}, CEX = {}>(
        path: string | string[],
        session: SparkSession,
        options?: Record<string, string>
    ) {
        const p: DFProgram<R, E, G, CDF, CEX> = (DF) => DF.relation("csv", path, options);
        return new ReadChainedDataFrame<R, E, G, CDF, CEX>(p, session);
    }


    private chain<CDF2 = unknown, CEX2 = unknown>(
        step: (df: R, EX: ExprAlg<E> & CEX & CEX2, DF: DFAlg<R, E, G, CDF & CDF2>) => R
    ): ReadChainedDataFrame<R, E, G, CDF & CDF2, CEX & CEX2> {
        const next: DFProgram<R, E, G, CDF & CDF2, CEX & CEX2> = (DF, EX) =>
            step(this.prog(DF, EX), EX, DF);
        return new ReadChainedDataFrame(next, this.session);
    }

    select(...cols: string[]): ReadChainedDataFrame<R, E, G, CDF, CEX>;
    select(...cols: EBuilder[]): ReadChainedDataFrame<R, E, G, CDF, CEX>;
    select(...cols: (string | EBuilder)[]): ReadChainedDataFrame<R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) =>
            DF.select(df, cols.map(c => typeof c === "string" ? EX.col(c) : c.build(EX)))
        );
    }

    filter(cond: EBuilder) {
        return this.chain((df, EX, DF) => DF.filter(df, cond.build(EX)));
    }

    withColumn(name: string, e: EBuilder) {
        return this.chain((df, EX, DF) => DF.withColumn(df, name, e.build(EX)));
    }

    join<RCDF = unknown, RCEX = unknown>(right: ReadChainedDataFrame<R, E, G, RCDF, RCEX>,
        on: EBuilder,
        jt: JoinTypeInput = DEFAULT_JOIN_TYPE
    ): ReadChainedDataFrame<R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chain<RCDF, RCEX>((_, EX, DF) => {
            const leftPlan  = this.prog(DF, EX);
            // DF tiene al menos CDF & RCDF; EX tiene al menos CEX & RCEX.
            const rightPlan = right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX);
            return DF.join(leftPlan, rightPlan, on.build(EX), jt);
        });
    }


    groupBy(...by: (string | EBuilder)[]) {
        const self = this;

        const parseAgg = (s: string) => {
            const m = s.match(/^\s*([A-Za-z_]\w*)\s*\(\s*([^)]+)\s*\)\s*$/);
            if (!m) throw new Error(`Invalid aggregation: ${s}`);
            return { fn: m[1], arg: m[2] };
        };

        return {
            agg(aggs: Record<string, EBuilder | string>) {
                const next: DFProgram<R, E, G, CDF, CEX> = (DF, EX) => {
                    const keys = by.map(b => typeof b === "string" ? EX.col(b) : b.build(EX));
                    const g = DF.groupBy(self.prog(DF, EX), keys);
                    const exprs = Object.fromEntries(
                        Object.entries(aggs).map(([alias, v]) => {
                            if (typeof v === "string") {
                                const { fn, arg } = parseAgg(v);
                                return [alias, EX.call(fn, [EX.col(arg)])];
                            }
                            return [alias, v.build(EX)];
                        })
                    );
                    return DF.agg(g, exprs);
                };
                return new ReadChainedDataFrame(next, self.session);
            }
        };
    }

    orderBy(...colsOrKeys: SortKeyInput[]) {
        return this.chain((df, EX, DF) => {
            const orders = colsOrKeys.map(k => {
                if (typeof k === "string") return { expr: EX.col(k), direction: "asc" as const };
                if (typeof k === "function") return k(EX);
                return { expr: k.build(EX), direction: "asc" as const };
            });
            return DF.orderBy(df, orders);
        });
    }

    sort(...colsOrKeys: SortKeyInput[]) {
        return this.chain((df, EX, DF) => {
            const orders = colsOrKeys.map(k => {
                if (typeof k === "string") return { expr: EX.col(k), direction: "asc" as const };
                if (typeof k === "function") return k(EX);
                return { expr: k.build(EX), direction: "asc" as const };
            });
            return DF.sort(df, orders);
        });
    }

    limit(n: number) {
        return this.chain((df, _EX, DF) => DF.limit(df, n));
    }

    distinct() {
        return this.chain((df, _EX, DF) => DF.distinct(df));
    }

    dropDuplicates(...cols: string[]): ReadChainedDataFrame<R, E, G, CDF, CEX>;
    dropDuplicates(...cols: EBuilder[]): ReadChainedDataFrame<R, E, G, CDF, CEX>;
    dropDuplicates(...cols: (string | EBuilder)[]): ReadChainedDataFrame<R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) => {
            const exprs =
                cols.length === 0
                    ? undefined
                    : cols.map(c => (typeof c === "string" ? EX.col(c) : c.build(EX)));
            return DF.dropDuplicates(df, exprs);
        });
    }

    union(right: ReadChainedDataFrame<R, E, G, CDF, CEX>): ReadChainedDataFrame<R, E, G, CDF, CEX> {
        const next: DFProgram<R, E, G, CDF, CEX> = (DF, EX) => {
            const leftPlan = this.prog(DF, EX);
            const rightPlan = right.prog(DF, EX);
            return DF.union(leftPlan, rightPlan);
        };
        return new ReadChainedDataFrame(next, this.session);
    }

    unionByName<RCDF = unknown, RCEX = unknown>(
        right: ReadChainedDataFrame<R, E, G, RCDF, RCEX>,
        allowMissingColumns = false
    ): ReadChainedDataFrame<R, E, G, CDF & RCDF, CEX & RCEX> {
        return this.chain<RCDF, RCEX>((_, EX, DF) =>
            DF.union(
                this.prog(DF, EX),
                right.getProgram()(DF as DFAlg<R, E, G, RCDF>, EX as ExprAlg<E> & RCEX),
                { byName: true, allowMissingColumns }
            )
        );
    }

    withColumnRenamed(oldName: string, newName: string) {
        return this.chain((df, _EX, DF) => DF.withColumnRenamed(df, oldName, newName));
    }

    withColumnsRenamed(mapping: Record<string, string>) {
        return this.chain((df, _EX, DF) => DF.withColumnsRenamed(df, mapping));
    }

    coalesce(name: string, ...exprs: Array<string | EBuilder | number | boolean>): ReadChainedDataFrame<R, E, G, CDF, CEX> {
        return this.chain((df, EX, DF) => {
            const toE = (x: string | EBuilder | number | boolean) =>
                typeof x === "string"
                    ? EX.col(x)
                    : typeof x === "object" && x !== null && "build" in x
                        ? (x as EBuilder).build(EX)
                        : EX.lit(x as any);

            const coalesced = EX.coalesce(exprs.map(toE));
            return DF.withColumn(df, name, coalesced);
        });
    }

    describe(colNames: string[]) {
        return this.chain((df, EX, DF) => {
            const toString = (e: any) => EX.call("concat", [EX.lit(""), e]);
            const stats = ["count", "mean", "stddev", "min", "max"] as const;
            const NULL_D = EX.call("nullif", [EX.lit(1.0), EX.lit(1.0)]);
            const NUMERIC_RX = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$";
            const toDoubleIfNumeric = (name: string) =>
                EX.caseWhen(
                    [{
                        when: EX.call("rlike", [
                            EX.call("concat", [EX.lit(""), EX.col(name)]),
                            EX.lit(NUMERIC_RX),
                        ]),
                        then: EX.bin("*", EX.lit(1.0), EX.col(name)),
                    }],
                    NULL_D
                );

            const pruned = DF.select(df, colNames.map(n => EX.col(n)));

            const numExpr: Record<string, any> = Object.fromEntries(
                colNames.map(n => [n, toDoubleIfNumeric(n)])
            );

            const measures = Object.fromEntries(
                colNames.flatMap(n => ([
                    [`__${n}_count`, EX.call("count", [EX.col(n)])],
                    [`__${n}_mean`,   EX.call("avg", [numExpr[n]])],
                    [`__${n}_stddev`, EX.call("stddev_samp", [numExpr[n]])],
                    [`__${n}_min`, EX.call("min", [EX.col(n)])],
                    [`__${n}_max`, EX.call("max", [EX.col(n)])],
                ]))
            );

            const aggregated = DF.agg(DF.groupBy(pruned, [] as any[]), measures);

            const projectFor = (stat: typeof stats[number]) =>
                DF.select(aggregated, [
                    EX.alias(EX.lit(stat), "summary"),
                    ...colNames.map(n =>
                        EX.alias(toString(EX.col(`__${n}_${stat}`)), n)
                    ),
                ]);

            return stats.slice(1).reduce(
                (acc, s) => DF.union(acc, projectFor(s), { byName: true }),
                projectFor(stats[0])
            );
        });
    }

    summary(metrics: string[] | undefined, colNames: string[]) {
        return this.chain((df, EX, DF) => {
            const DEFAULTS = ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"] as const;
            const req = (metrics?.length ? metrics : DEFAULTS).map(m => m.toLowerCase());

            type Parsed =
                | { kind: "builtin"; name: "count" | "mean" | "stddev" | "min" | "max"; label: string; suffix: string }
                | { kind: "pct"; p: number; label: string; suffix: string };

            const norm = (m: string): Parsed => {
                if (m === "median") m = "50%";
                if (/%$/.test(m)) {
                    const p = parseFloat(m) / 100;
                    if (!(p >= 0 && p <= 1)) throw new Error(`summary(): percentil inválido '${m}'`);
                    const pct = Math.round(p * 100);
                    return { kind: "pct", p, label: `${pct}%`, suffix: `p${pct}` };
                }
                if (m === "std") m = "stddev";
                const ok = ["count", "mean", "stddev", "min", "max"] as const;
                if ((ok as readonly string[]).includes(m)) {
                    return { kind: "builtin", name: m as any, label: m, suffix: (m === "stddev" ? "stddev" : m) };
                }
                throw new Error(`summary(): métrica no soportada '${m}'`);
            };
            const parsed: Parsed[] = req.map(norm);

            const toString = (e: any) => EX.call("concat", [EX.lit(""), e]);
            const NULL_D = EX.call("nullif", [EX.lit(1.0), EX.lit(1.0)]);
            const NUMERIC_RX = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$";
            const toDoubleIfNumeric = (name: string) =>
                EX.caseWhen(
                    [{
                        when: EX.call("rlike", [
                            EX.call("concat", [EX.lit(""), EX.col(name)]),
                            EX.lit(NUMERIC_RX),
                        ]),
                        then: EX.bin("*", EX.lit(1.0), EX.col(name)),
                    }],
                    NULL_D
                );

            const pruned = DF.select(df, colNames.map(n => EX.col(n)));

            const pairs: [string, any][] = [];
            for (const n of colNames) {
                const numArg = toDoubleIfNumeric(n);
                for (const m of parsed) {
                    if (m.kind === "builtin") {
                        if (m.name === "count") pairs.push([`__${n}_count`, EX.call("count", [EX.col(n)])]);
                        if (m.name === "mean") pairs.push([`__${n}_mean`, EX.call("avg", [numArg])]);
                        if (m.name === "stddev") pairs.push([`__${n}_stddev`, EX.call("stddev_samp", [numArg])]);
                        if (m.name === "min") pairs.push([`__${n}_min`, EX.call("min", [EX.col(n)])]);
                        if (m.name === "max") pairs.push([`__${n}_max`, EX.call("max", [EX.col(n)])]);
                    } else {
                        pairs.push([`__${n}_${m.suffix}`, EX.call("percentile_approx", [numArg, EX.lit(m.p)])]);
                    }
                }
            }
            const measures = Object.fromEntries(pairs);
            const aggregated = DF.agg(DF.groupBy(pruned, [] as any[]), measures);

            const projectFor = (m: Parsed) =>
                DF.select(aggregated, [
                    EX.alias(EX.lit(m.label), "summary"),
                    ...colNames.map(n =>
                        EX.alias(toString(EX.col(`__${n}_${m.suffix}`)), n)
                    ),
                ]);

            const rows = parsed.map(projectFor);
            return rows.slice(1).reduce(
                (acc, r) => DF.union(acc, r, { byName: true }),
                rows[0]
            );
        });
    }

    repartition(numPartitions: number, shuffle = true): ReadChainedDataFrame<R, E, G, CDF & RepartitionCap<R>, CEX> {
        return this.chain<RepartitionCap<R>>((df, _EX, DF) => DF.repartition(df, numPartitions, shuffle));
    }

    hint(name: JoinHintName | string, ...params: any[]) {
        return this.chain<HintCap<R>>((df, _EX, DF) => DF.hint(df, name, params));
    }

    broadcast() {
        return this.hint("broadcast");
    }

    mergeHint() {
        return this.hint("merge");
    }

    shuffleHashHint() {
        return this.hint("shuffle_hash");
    }

    shuffleReplicateNLHint() {
        return this.hint("shuffle_replicate_nl");
    }

    coalescePartitions(numPartitions: number): ReadChainedDataFrame<R, E, G, CDF & RepartitionCap<R>, CEX> {
        return this.chain<RepartitionCap<R>>((df, _EX, DF) => DF.coalesce(df, numPartitions));
    }

    sql(query: string): ReadChainedDataFrame<R, E, G, CDF & SqlCap<R>, CEX> {
        return this.chain<SqlCap<R>>((_df, _EX, DF) => DF.sql(query));
    }

    cache(): ReadChainedDataFrame<R, E, G, CDF & CacheCap<R>, CEX> {
        return this.chain<CacheCap<R>>((df, _EX, DF) => DF.cache(df));
    }

    persist(level?: string): ReadChainedDataFrame<R, E, G, CDF & CacheCap<R>, CEX> {
        return this.chain<CacheCap<R>>((df, _EX, DF) => DF.persist(df, level ?? "MEMORY_AND_DISK"));
    }

    unpersist(blocking?: boolean): ReadChainedDataFrame<R, E, G, CDF & CacheCap<R>, CEX> {
        return this.chain<CacheCap<R>>((df, _EX, DF) => DF.unpersist(df, blocking));
    }

    runWith(DF: DFAlg<R, E, G, CDF>, EX: ExprAlg<E> & CEX): R {
        return this.prog(DF, EX);
    }

    getProgram(): DFProgram<R, E, G, CDF, CEX> {
        return this.prog;
    }

    getSession(): SparkSession {
        return this.session;
    }

    private compileToSparkPlan(): LogicalPlan {
        return this.prog(SparkDFAlg as any, SparkExprAlg as any) as LogicalPlan;
    }

    async collectRaw(): Promise<any[]> {
        const root = this.prog(ProtoDFAlg as any, ProtoExprAlg as any);
        return ProtoExec.collect(root, this.session);
    }

    async show(): Promise<void> {
        const result = await this.collectRaw();
        const arrowBuffers = result
            .filter(r => r.arrow_batch?.data)
            .map(r => r.arrow_batch.data as Buffer);
        printArrowResults(arrowBuffers);
    }

    explain(mode: ExplainModeInput = "simple"): Promise<any[]> {
        const root = this.prog(ProtoDFAlg as any, ProtoExprAlg as any);
        return ProtoExec.explain(root, this.session, mode);
    }

    get write(): DataFrameWriterTF<R, E, G, DefaultW, CDF, CEX> {
        return DataFrameWriterTF.fromDF<R, E, G, DefaultW, CDF, CEX>(this);
    }

    private compileTrace(): any {
        return this.prog(TraceDFAlg as any, TraceExprAlg as any);
    }

    toClientASTJSON(): string {
        const root = this.compileTrace();
        return toJSON(root);
    }

    toClientASTMermaid(): string {
        const root = this.compileTrace();
        return toMermaid(root);
    }

    toSparkLogicalPlanJSON(): string {
        const lp = this.compileToSparkPlan();
        return JSON.stringify(lp, null, 2);
    }

    toProtoJSON(): string {
        const protoRoot = this.prog(ProtoDFAlg as any, ProtoExprAlg as any);
        return JSON.stringify(protoRoot, null, 2);
    }

    sample(fraction: number, withReplacement = false, seed?: number, deterministicOrder = false) {
        if (!withReplacement && (fraction < 0 || fraction > 1)) {
            throw new Error("sample(): fraction debe estar en [0,1] cuando withReplacement=false");
        }
        const lb = 0.0, ub = fraction;
        return this.chain<SamplingCap<R>>((df, _EX, DF) => DF.sample(df, lb, ub, withReplacement, seed, deterministicOrder));
    }

    drop(...columnNames: string[]) {
        return this.chain((df, _EX, DF) => DF.drop(df, columnNames));
    }

    randomSplit(weights: number[], seed?: number): ReadChainedDataFrame<R, E, G, CDF, CEX>[] {
        if (!weights?.length) throw new Error("randomSplit(): weights vacío");
        const sum = weights.reduce((a, b) => a + b, 0);
        if (sum <= 0) throw new Error("randomSplit(): suma de weights debe ser > 0");

        const bounds: Array<[number, number]> = [];
        let acc = 0;
        for (const w of weights) {
            const start = acc / sum;
            acc += w;
            const end = acc / sum;
            bounds.push([start, end]);
        }

        const RAND_COL = "__rand_split__";

        const dfWithRand = this.withColumn(
            RAND_COL,
            { build: EX => EX.call("rand", seed != null ? [EX.lit(seed)] : []) }
        );

        const splits = bounds.map(([lo, hi], idx) => {
            const split = dfWithRand.filter({
                build: EX => EX.logical(
                    "AND",
                    EX.bin(">=", EX.col(RAND_COL), EX.lit(lo)),
                    EX.bin(idx === bounds.length - 1 ? "<=" : "<", EX.col(RAND_COL), EX.lit(hi)),
                )
            }).drop(RAND_COL);
            return split;
        });

        return splits;
    }

    static readStream<R, E, G, CDF = unknown, CEX = unknown>(
        format: string,
        session: SparkSession,
        options?: Record<string, string>
    ) {
        type Need = StreamingReadCap<R>;
        const p: DFProgram<R, E, G, CDF & Need, CEX> =
            (DF: DFAlg<R, E, G, CDF & Need>) => DF.readStream(format, options);
        return new ReadChainedDataFrame<R, E, G, CDF & Need, CEX>(p, session);
    }

    withWatermark(eventTimeCol: EBuilder, delay: string) {
        type Need = EventTimeWatermarkCap<R, E>;
        return this.chain<Need>((df, EX, DF) =>
            (DF as DFAlg<R, E, G, CDF & Need>).withWatermark(df, eventTimeCol.build(EX), delay)
        );
    }
}
