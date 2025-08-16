// src/engine/column.ts  — Tagless Final builders (sin AST público)

import {ExprAlg, NullsOrder, SortDirection, WindowSpec} from "../read/readDataframe";


export type FrameBoundary =
    | { type: "UnboundedPreceding" }
    | { type: "UnboundedFollowing" }
    | { type: "CurrentRow" }
    | { type: "ValuePreceding"; value: number }
    | { type: "ValueFollowing"; value: number };

export type EBuilder = {
    build<E>(EX: ExprAlg<E>): E;

    // helpers encadenables tipo PySpark
    alias(name: string): EBuilder;
    eq(x: EBuilder | string | number | boolean): EBuilder;
    gt(x: EBuilder | number): EBuilder;
    gte(x: EBuilder | number): EBuilder;
    lt(x: EBuilder | number): EBuilder;
    lte(x: EBuilder | number): EBuilder;
    and(x: EBuilder): EBuilder;
    or(x: EBuilder): EBuilder;

    // order keys
    asc(nulls?: NullsOrder): SortKeyBuilder;
    desc(nulls?: NullsOrder): SortKeyBuilder;
    ascNullsFirst(): SortKeyBuilder;
    ascNullsLast():  SortKeyBuilder;
    descNullsFirst():SortKeyBuilder;
    descNullsLast(): SortKeyBuilder;

    isNull(): EBuilder;
    isNotNull(): EBuilder;
    over(spec: (EX: ExprAlg<any>) => WindowSpec<any>): EBuilder

};


const toE =
    (x: EBuilder | string | number | boolean) =>
        <E>(EX: ExprAlg<E>) =>
            typeof x === "object" && x !== null && "build" in x
                ? x.build(EX)
                : typeof x === "string"
                    ? EX.col(x)
                    : EX.lit(x as any);

export type SortKeyBuilder = <E>(EX: ExprAlg<E>) => { expr: E; direction: SortDirection; nulls?: NullsOrder };

const EB = (f: <E>(EX: ExprAlg<E>) => E): EBuilder => {
    const toE = (x: EBuilder | string | number | boolean) =>
        <E>(EX: ExprAlg<E>) => (typeof x === "object" && "build" in x) ? x.build(EX) : EX.lit(x as any);

    return {
        build: f,

        alias: (name) => EB(EX => EX.alias(f(EX), name)),

        eq: (x) => EB(EX => EX.bin("=", f(EX), toE(x)(EX))),
        gt: (x) => EB(EX => EX.bin(">", f(EX), toE(x)(EX))),
        gte: (x) => EB(EX => EX.bin(">=", f(EX), toE(x)(EX))),
        lt: (x) => EB(EX => EX.bin("<", f(EX), toE(x)(EX))),
        lte: (x) => EB(EX => EX.bin("<=", f(EX), toE(x)(EX))),

        and: (x) => EB(EX => EX.logical("AND", f(EX), x.build(EX))),
        or: (x) => EB(EX => EX.logical("OR", f(EX), x.build(EX))),

        asc: (nulls) => (EX) => ({expr: f(EX), direction: "asc" as const, nulls}),
        desc: (nulls) => (EX) => ({expr: f(EX), direction: "desc" as const, nulls}),
        ascNullsFirst: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "asc", nulls: "nullsFirst"}),
        ascNullsLast: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "asc", nulls: "nullsLast"}),
        descNullsFirst: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "desc", nulls: "nullsFirst"}),
        descNullsLast: (): SortKeyBuilder => (EX) => ({expr: f(EX), direction: "desc", nulls: "nullsLast"}),
        isNull:    () => EB(EX => EX.isNull(f(EX))),
        isNotNull: () => EB(EX => EX.isNotNull(f(EX))),
        over: (specB) => EB(EX => EX.win(f(EX), specB(EX))),
    };
};


export function isNull(x: EBuilder | string): EBuilder {
    return EB(EX => EX.isNull(toE(x)(EX)));
}

export function isNotNull(x: EBuilder | string): EBuilder {
    return EB(EX => EX.isNotNull(toE(x)(EX)));
}
export function coalesce(...xs: Array<EBuilder | string | number | boolean>): EBuilder {
    return EB(EX => EX.coalesce(xs.map(x => toE(x)(EX))));
}

export const col = (name: string): EBuilder => EB(EX => EX.col(name));
export const lit = (v: string | number | boolean): EBuilder => EB(EX => EX.lit(v));

type CaseChain = {
    when(cond: EBuilder, val: EBuilder | string | number | boolean): CaseChain;
    otherwise(val: EBuilder | string | number | boolean): EBuilder;
};

export function when(cond: EBuilder, val: EBuilder | string | number | boolean): CaseChain {
    // acumulamos ramas en forma inmutable
    const branches: Array<{ when: EBuilder; then: EBuilder }> = [
        { when: cond, then: asE(val) }
    ];
    const make = (br: typeof branches): CaseChain => ({
        when(nextCond, nextVal) {
            return make([...br, { when: nextCond, then: asE(nextVal) }]);
        },
        otherwise(elseVal) {
            const elseB = asE(elseVal);
            return EB(EX => EX.caseWhen(
                br.map(b => ({ when: b.when.build(EX), then: b.then.build(EX) })),
                elseB.build(EX)
            ));
        }
    });
    return make(branches);
}

export const Window = {
    partitionBy: (...keys: (string | EBuilder)[]) => ({
        orderBy: (...ords: Array<string | EBuilder | SortKeyBuilder>) => ({
            rowsBetween: (start: FrameBoundary, end: FrameBoundary) =>
                <E>(EX: ExprAlg<E>): WindowSpec<E> => ({
                    partitionBy: keys.map(k => typeof k === "string" ? EX.col(k) : (k as EBuilder).build(EX)),
                    orderBy: ords.map(o => {
                        if (typeof o === "string") return { input: EX.col(o), direction: "asc" as const };
                        if (typeof o === "function") {
                            const so = (o as SortKeyBuilder)(EX);
                            return { input: so.expr, direction: so.direction, nulls: so.nulls };
                        }
                        const e = (o as EBuilder).build(EX);
                        return { input: e, direction: "asc" as const };
                    }),
                    frame: { type: "rows", start, end },
                }),
        }),
    }),
};


// util
const asE = (x: EBuilder | string | number | boolean): EBuilder =>
    (typeof x === "object" && "build" in x) ? x as EBuilder : lit(x as any);
