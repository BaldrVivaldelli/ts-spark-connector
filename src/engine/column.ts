// src/engine/column.ts  — Tagless Final builders (sin AST público)

import {ExprAlg, SortOrder} from "../spark/dataframe";
import {NullsOrder, SortDirection} from "./logicalPlan";


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
};

/** SortKeyBuilder: produce un SortOrder cuando recibe un ExprAlg */
export type SortKeyBuilder = <E>(EX: ExprAlg<E>) => { expr: E; direction: SortDirection; nulls?: NullsOrder };

// fábrica para EBuilder
const EB = (f: <E>(EX: ExprAlg<E>) => E): EBuilder => {
    const toE = (x: EBuilder | string | number | boolean) =>
        <E>(EX: ExprAlg<E>) => (typeof x === "object" && "build" in x) ? x.build(EX) : EX.lit(x as any);

    const self: EBuilder = {
        build: f,

        alias: (name) => EB(EX => EX.alias(f(EX), name)),

        eq:  (x) => EB(EX => EX.bin("=",  f(EX), toE(x)(EX))),
        gt:  (x) => EB(EX => EX.bin(">",  f(EX), toE(x)(EX))),
        gte: (x) => EB(EX => EX.bin(">=", f(EX), toE(x)(EX))),
        lt:  (x) => EB(EX => EX.bin("<",  f(EX), toE(x)(EX))),
        lte: (x) => EB(EX => EX.bin("<=", f(EX), toE(x)(EX))),

        and: (x) => EB(EX => EX.logical("AND", f(EX), x.build(EX))),
        or:  (x) => EB(EX => EX.logical("OR",  f(EX), x.build(EX))),

        asc:  (nulls) => (EX) => ({ expr: f(EX), direction: "asc"  as const, nulls }),
        desc: (nulls) => (EX) => ({ expr: f(EX), direction: "desc" as const, nulls }),
        ascNullsFirst: (): SortKeyBuilder => (EX) => ({ expr: f(EX), direction: "asc",  nulls: "nullsFirst" }),
        ascNullsLast:  (): SortKeyBuilder => (EX) => ({ expr: f(EX), direction: "asc",  nulls: "nullsLast"  }),
        descNullsFirst:(): SortKeyBuilder => (EX) => ({ expr: f(EX), direction: "desc", nulls: "nullsFirst" }),
        descNullsLast: (): SortKeyBuilder => (EX) => ({ expr: f(EX), direction: "desc", nulls: "nullsLast"  }),
    };
    return self;
};

/** Column-like helpers (no AST expuesto) */
export const col = (name: string): EBuilder => EB(EX => EX.col(name));
export const lit = (v: string | number | boolean): EBuilder => EB(EX => EX.lit(v));

/** CASE WHEN builder encadenable */
type CaseChain = {
    when(cond: EBuilder, val: EBuilder | string | number | boolean): CaseChain;
    otherwise(val: EBuilder | string | number | boolean): EBuilder;
};

/** when(cond, val).when(...).otherwise(...) */
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



// util
const asE = (x: EBuilder | string | number | boolean): EBuilder =>
    (typeof x === "object" && "build" in x) ? x as EBuilder : lit(x as any);
