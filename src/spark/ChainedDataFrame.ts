import {SparkDFAlg, SparkExprAlg} from "./dataFrameInterpreter";
import {LogicalPlan} from "../engine/logicalPlan";
import {ProtoDFAlg, ProtoExec, ProtoExprAlg} from "../engine/compiler";
import {SparkSession} from "./session";
import {DFAlg, DFProgram, ExprAlg, NullsOrder, SortOrder} from "./dataframe";
import {DEFAULT_JOIN_TYPE, JoinTypeInput} from "../engine/sparkConnectEnums";
import {printArrowResults} from "../utils/arrowPrinter";

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

export class ChainedDataFrame<R,E,G> {
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
        return new ChainedDataFrame<R,E,G>(p, session);
    }

    private chain(step: (df: any, EX: ExprAlg<any>, DF: DFAlg<any, any, any>) => any) {
        const next: DFProgram<any, any, any> = (DF, EX) => step(this.prog(DF, EX), EX, DF);
        return new ChainedDataFrame(next, this.session);
    }

    select(...cols: string[]): ChainedDataFrame<R,E,G>;
    select(...cols: EBuilder[]): ChainedDataFrame<R,E,G>;
    select(...cols: (string | EBuilder)[]): ChainedDataFrame<R,E,G> {
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

    join(right: ChainedDataFrame<R,E,G>, on: EBuilder, jt: JoinTypeInput = DEFAULT_JOIN_TYPE) {
        const next: DFProgram<any, any, any> = (DF, EX) =>
            DF.join(this.prog(DF, EX), right.prog(DF, EX), on.build(EX), jt);
        return new ChainedDataFrame(next, this.session);
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
                return new ChainedDataFrame(next, self.session);
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


    dropDuplicates(...cols: string[]): ChainedDataFrame<R,E,G>;
    dropDuplicates(...cols: EBuilder[]): ChainedDataFrame<R,E,G>;
    dropDuplicates(...cols: (string | EBuilder)[]): ChainedDataFrame<R,E,G> {
        return this.chain((df, EX, DF) => {
            const exprs =
                cols.length === 0
                    ? undefined
                    : cols.map(c => (typeof c === "string" ? EX.col(c) : c.build(EX)));
            return DF.dropDuplicates(df, exprs);
        });
    }

    union(right: ChainedDataFrame<R,E,G>): ChainedDataFrame<R,E,G> {
        const next: DFProgram<any, any, any> = (DF, EX) => {
            const leftPlan = this.prog(DF, EX);
            const rightPlan = right.prog(DF, EX);
            return DF.union(leftPlan, rightPlan);
        };
        return new ChainedDataFrame(next, this.session);
    }

    unionByName(right: ChainedDataFrame<R,E,G>, allowMissingColumns = false) {
        const next: DFProgram<any, any, any> = (DF, EX) =>
            DF.union(this.prog(DF, EX), right.prog(DF, EX), {byName: true, allowMissingColumns});
        return new ChainedDataFrame(next, this.session);
    }

    withColumnRenamed(oldName: string, newName: string) {
        return this.chain((df, _EX, DF) => DF.withColumnRenamed(df, oldName, newName));
    }

    withColumnsRenamed(mapping: Record<string, string>) {
        return this.chain((df, _EX, DF) => DF.withColumnsRenamed(df, mapping));
    }

    coalesce(name: string, ...exprs: Array<string | EBuilder | number | boolean>): ChainedDataFrame<R,E,G> {
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

    getSession() {
        return this.session;
    }
}
