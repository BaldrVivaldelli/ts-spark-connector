import {SparkDFAlg, SparkExprAlg} from "./readDataFrameInterpreter";
import {LogicalPlan} from "../engine/logicalPlan";
import {ProtoDFAlg, ProtoExec, ProtoExprAlg} from "../engine/compilerRead";
import {SparkSession} from "../client/session";
import {DFAlg, DFProgram, ExprAlg, NullsOrder, SortOrder} from "./readDataframe";
import {DEFAULT_JOIN_TYPE, JoinTypeInput} from "../engine/sparkConnectEnums";
import {printArrowResults} from "../utils/arrowPrinter";
import {DataFrameWriterTF} from "../write/dataFrameWriterTF";

type EBuilder = { build<E>(EX: ExprAlg<E>): E };
type SortKeyInput =
    | string
    | EBuilder
    | ((EX: ExprAlg<any>) => SortOrder<any>);
export const col = (name: string): EBuilder => ({build: EX => EX.col(name)});
export const lit = (v: string | number | boolean): EBuilder => ({build: EX => EX.lit(v)});
export const eq = (l: EBuilder, r: EBuilder | string | number | boolean): EBuilder => ({
    build: EX => EX.bin("=", l.build(EX), typeof r === "object" ? (r as EBuilder).build(EX) : EX.lit(r as any))
});
export const asc = (e: EBuilder, nulls?: NullsOrder) => (EX: ExprAlg<any>): SortOrder<any> =>
    ({expr: e.build(EX), direction: "asc", nulls});
export const desc = (e: EBuilder, nulls?: NullsOrder) => (EX: ExprAlg<any>): SortOrder<any> =>
    ({expr: e.build(EX), direction: "desc", nulls});

export class ReadChainedDataFrame<R,E,G> {
    private readonly prog: DFProgram<any, any, any>;

    constructor(p: DFProgram<R,E,G>, private readonly session: SparkSession) {
        this.prog = p;
    }

    static fromCSV<R,E,G>(
        path: string | string[],
        session: SparkSession,
        options?: Record<string, string>
    ) {
        const p: DFProgram<R,E,G> = (DF) => DF.relation("csv", path, options);
        return new ReadChainedDataFrame<R,E,G>(p, session);
    }

    private chain(step: (df: any, EX: ExprAlg<any>, DF: DFAlg<any, any, any>) => any) {
        const next: DFProgram<any, any, any> = (DF, EX) => step(this.prog(DF, EX), EX, DF);
        return new ReadChainedDataFrame(next, this.session);
    }

    select(...cols: string[]): ReadChainedDataFrame<R,E,G>;
    select(...cols: EBuilder[]): ReadChainedDataFrame<R,E,G>;
    select(...cols: (string | EBuilder)[]): ReadChainedDataFrame<R,E,G> {
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

    join(right: ReadChainedDataFrame<R,E,G>, on: EBuilder, jt: JoinTypeInput = DEFAULT_JOIN_TYPE) {
        const next: DFProgram<any, any, any> = (DF, EX) =>
            DF.join(this.prog(DF, EX), right.prog(DF, EX), on.build(EX), jt);
        return new ReadChainedDataFrame(next, this.session);
    }

    groupBy(...by: (string | EBuilder)[]) {
        const self = this;

        const parseAgg = (s: string) => {
            const m = s.match(/^\s*([A-Za-z_]\w*)\s*\(\s*([^)]+)\s*\)\s*$/);
            if (!m) throw new Error(`Invalid aggregation: ${s}`);
            return {fn: m[1], arg: m[2]};
        };

        return {
            agg(aggs: Record<string, EBuilder | string>) {
                const next: DFProgram<any, any, any> = (DF, EX) => {
                    // by: string|EBuilder -> E
                    const keys = by.map(b => typeof b === "string" ? EX.col(b) : b.build(EX));

                    const g = DF.groupBy(self.prog(DF, EX), keys);

                    // aggs: alias -> E (si es string, parsear "fn(col)")
                    const exprs = Object.fromEntries(
                        Object.entries(aggs).map(([alias, v]) => {
                            if (typeof v === "string") {
                                const {fn, arg} = parseAgg(v);
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
                if (typeof k === "string") return {expr: EX.col(k), direction: "asc" as const};
                if (typeof k === "function") return k(EX); // ya devuelve SortOrder<E>
                /* k es EBuilder */
                return {expr: k.build(EX), direction: "asc" as const};
            });
            return DF.orderBy(df, orders);
        });
    }

    sort(...colsOrKeys: SortKeyInput[]) {
        return this.chain((df, EX, DF) => {
            const orders = colsOrKeys.map(k => {
                if (typeof k === "string") return {expr: EX.col(k), direction: "asc" as const};
                if (typeof k === "function") return k(EX);
                return {expr: k.build(EX), direction: "asc" as const};
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


    dropDuplicates(...cols: string[]): ReadChainedDataFrame<R,E,G>;
    dropDuplicates(...cols: EBuilder[]): ReadChainedDataFrame<R,E,G>;
    dropDuplicates(...cols: (string | EBuilder)[]): ReadChainedDataFrame<R,E,G> {
        return this.chain((df, EX, DF) => {
            const exprs =
                cols.length === 0
                    ? undefined
                    : cols.map(c => (typeof c === "string" ? EX.col(c) : c.build(EX)));
            return DF.dropDuplicates(df, exprs);
        });
    }

    union(right: ReadChainedDataFrame<R,E,G>): ReadChainedDataFrame<R,E,G> {
        const next: DFProgram<any, any, any> = (DF, EX) => {
            const leftPlan = this.prog(DF, EX);
            const rightPlan = right.prog(DF, EX);
            return DF.union(leftPlan, rightPlan);
        };
        return new ReadChainedDataFrame(next, this.session);
    }

    unionByName(right: ReadChainedDataFrame<R,E,G>, allowMissingColumns = false) {
        const next: DFProgram<any, any, any> = (DF, EX) =>
            DF.union(this.prog(DF, EX), right.prog(DF, EX), {byName: true, allowMissingColumns});
        return new ReadChainedDataFrame(next, this.session);
    }

    withColumnRenamed(oldName: string, newName: string) {
        return this.chain((df, _EX, DF) => DF.withColumnRenamed(df, oldName, newName));
    }

    withColumnsRenamed(mapping: Record<string, string>) {
        return this.chain((df, _EX, DF) => DF.withColumnsRenamed(df, mapping));
    }

    coalesce(name: string, ...exprs: Array<string | EBuilder | number | boolean>): ReadChainedDataFrame<R,E,G> {
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
    describe(colNames: string[])  {
        return this.chain((df, EX, DF) => {
            const toString = (e:any) => EX.call("concat", [EX.lit(""), e]);
            const stats = ["count", "mean", "stddev", "min", "max"] as const;
            const fn = (s: typeof stats[number]) =>
                s === "mean" ? "avg" : s === "stddev" ? "stddev_samp" : s;

            // NULL(double) sin cast: nullif(1.0,1.0)
            const NULL_D = EX.call("nullif", [EX.lit(1.0), EX.lit(1.0)]);

            // rlike sobre stringificado (soporta cols string o numéricas)
            const NUMERIC_RX = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$";
            const toDoubleIfNumeric = (name: string) =>
                EX.caseWhen(
                    [{
                        when: EX.call("rlike", [
                            EX.call("concat", [EX.lit(""), EX.col(name)]),
                            EX.lit(NUMERIC_RX),
                        ]),
                        then: EX.bin("*", EX.lit(1.0), EX.col(name)), // 1.0 * col -> DOUBLE
                    }],
                    NULL_D
                );

            // (1) Pushdown de proyección (si la fuente lo soporta, ahorra IO)
            const pruned = DF.select(df, colNames.map(n => EX.col(n)));

            // (2) UN SOLO AGG con TODAS las medidas
            const numExpr: Record<string, any> = Object.fromEntries(
                colNames.map(n => [n, toDoubleIfNumeric(n)])
            );

            const measures = Object.fromEntries(
                colNames.flatMap(n => ([
                    [`__${n}_count`,  EX.call("count",        [EX.col(n)])],
                    [`__${n}_mean`,   EX.call("avg",          [numExpr[n]])],
                    [`__${n}_stddev`, EX.call("stddev_samp",  [numExpr[n]])],
                    [`__${n}_min`,    EX.call("min",          [EX.col(n)])],
                    [`__${n}_max`,    EX.call("max",          [EX.col(n)])],
                ]))
            );

            const aggregated = DF.agg(DF.groupBy(pruned, [] as any[]), measures);

            // (3) 5 Projects sobre el mismo 'aggregated' y UNION ALL por nombre
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
            const DEFAULTS = ["count","mean","stddev","min","25%","50%","75%","max"] as const;
            const req = (metrics?.length ? metrics : DEFAULTS).map(m => m.toLowerCase());

            type Parsed =
                | { kind: "builtin"; name: "count"|"mean"|"stddev"|"min"|"max"; label: string; suffix: string }
                | { kind: "pct"; p: number; label: string; suffix: string };

            const norm = (m: string): Parsed => {
                if (m === "median") m = "50%";
                if (/%$/.test(m)) {
                    const p = parseFloat(m)/100;
                    if (!(p >= 0 && p <= 1)) throw new Error(`summary(): percentil inválido '${m}'`);
                    const pct = Math.round(p*100);
                    return { kind: "pct", p, label: `${pct}%`, suffix: `p${pct}` };
                }
                if (m === "std") m = "stddev";
                const ok = ["count","mean","stddev","min","max"] as const;
                if ((ok as readonly string[]).includes(m)) {
                    return { kind: "builtin", name: m as any, label: m, suffix: (m === "stddev" ? "stddev" : m) };
                }
                throw new Error(`summary(): métrica no soportada '${m}'`);
            };
            const parsed: Parsed[] = req.map(norm);

            // 1) Helpers (mismos que en describe optimizado)
            const toString = (e:any) => EX.call("concat", [EX.lit(""), e]);
            const NULL_D  = EX.call("nullif", [EX.lit(1.0), EX.lit(1.0)]); // NULL DOUBLE
            const NUMERIC_RX = "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?$";
            const toDoubleIfNumeric = (name: string) =>
                EX.caseWhen(
                    [{
                        when: EX.call("rlike", [
                            EX.call("concat", [EX.lit(""), EX.col(name)]), // stringify para rlike
                            EX.lit(NUMERIC_RX),
                        ]),
                        then: EX.bin("*", EX.lit(1.0), EX.col(name)),   // fuerza DOUBLE sin cast
                    }],
                    NULL_D
                );

            // 2) Pushdown de columnas
            const pruned = DF.select(df, colNames.map(n => EX.col(n)));

            // 3) Un solo AGG con todas las medidas solicitadas
            const pairs: [string, any][] = [];
            for (const n of colNames) {
                const numArg = toDoubleIfNumeric(n);
                for (const m of parsed) {
                    if (m.kind === "builtin") {
                        if (m.name === "count")   pairs.push([`__${n}_count`,  EX.call("count",       [EX.col(n)])]);
                        if (m.name === "mean")    pairs.push([`__${n}_mean`,   EX.call("avg",         [numArg])]);
                        if (m.name === "stddev")  pairs.push([`__${n}_stddev`, EX.call("stddev_samp", [numArg])]);
                        if (m.name === "min")     pairs.push([`__${n}_min`,    EX.call("min",         [EX.col(n)])]);
                        if (m.name === "max")     pairs.push([`__${n}_max`,    EX.call("max",         [EX.col(n)])]);
                    } else {
                        // percentiles (uno por alias; simple y robusto)
                        pairs.push([`__${n}_${m.suffix}`, EX.call("percentile_approx", [numArg, EX.lit(m.p)])]);
                        // (si querés menos funciones, podés usar array de percentiles y luego element_at)
                    }
                }
            }
            const measures = Object.fromEntries(pairs);
            const aggregated = DF.agg(DF.groupBy(pruned, [] as any[]), measures);

            // 4) Un Project por métrica y UNION ALL por nombre
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



    private compileToSparkPlan(): LogicalPlan {
        return this.prog(SparkDFAlg, SparkExprAlg) as LogicalPlan;
    }

    async collectRaw(): Promise<any[]> {
        const root = this.prog(ProtoDFAlg, ProtoExprAlg);
        return ProtoExec.collect(root, this.session);
    }

    async show(): Promise<void> {
        const result = await this.collectRaw();
        const arrowBuffers = result
            .filter(r => r.arrow_batch?.data)
            .map(r => r.arrow_batch.data as Buffer);
        printArrowResults(arrowBuffers);
    }

    getSession(): SparkSession { return this.session; }
    getProgram(): DFProgram<R,E,G> { return this.prog; }

    get write() {
        return DataFrameWriterTF.fromDF<R,E,G, any>(this);
    }
}
