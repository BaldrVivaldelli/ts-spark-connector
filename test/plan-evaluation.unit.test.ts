import { describe, expect, it } from "vitest";
import { ReadChainedDataFrame, SparkSession, col } from "../src";

/**
 * Regression guard for the exponential plan re-evaluation bug: binary
 * operations used to ignore the already-interpreted left plan handed to them
 * by `chainAs` and ran `this.prog` again, so a left-deep chain of N binary
 * nodes interpreted the leftmost relation 2^N times. These tests interpret the
 * program with algebras that count every call and assert linear growth.
 */

type CountingAlgebra = {
    algebra: Record<string, (...args: unknown[]) => unknown>;
    counts: Record<string, number>;
};

function countingAlgebra(): CountingAlgebra {
    const counts: Record<string, number> = {};
    const algebra = new Proxy({} as Record<string, (...args: unknown[]) => unknown>, {
        get: (_target, property) => {
            const name = String(property);
            return (...args: unknown[]) => {
                counts[name] = (counts[name] ?? 0) + 1;
                return { op: name, args };
            };
        },
    });
    return { algebra, counts };
}

function interpretationCounts(df: ReadChainedDataFrame<any, any, any, any, any, any>): Record<string, number> {
    const DF = countingAlgebra();
    const EX = countingAlgebra();
    df.getProgram()(DF.algebra as never, EX.algebra as never);
    return { ...DF.counts, ...Object.fromEntries(Object.entries(EX.counts).map(([k, v]) => [`expr.${k}`, v])) };
}

const session = SparkSession.builder().getOrCreate();
const relation = () => session.read.option("header", "true").csv("/tmp/relation.csv");

type BinaryOp = (acc: ReturnType<typeof relation>, next: ReturnType<typeof relation>) => ReadChainedDataFrame<any, any, any, any, any, any>;

const binaryOperations: Array<[string, BinaryOp, string]> = [
    ["union", (acc, next) => acc.union(next), "union"],
    ["unionByName", (acc, next) => acc.unionByName(next), "union"],
    ["unionByName(allowMissingColumns)", (acc, next) => acc.unionByName(next, true), "union"],
    ["join", (acc, next) => acc.join(next, col("id").eq(col("other_id")), "left"), "join"],
    ["intersect", (acc, next) => acc.intersect(next), "intersect"],
    ["intersectAll", (acc, next) => acc.intersectAll(next), "intersect"],
    ["except", (acc, next) => acc.except(next), "except"],
    ["exceptAll", (acc, next) => acc.exceptAll(next), "except"],
];

describe("plan interpretation cost", () => {
    for (const [name, apply, algebraCall] of binaryOperations) {
        it(`${name}: a left-deep chain of N nodes interprets each relation exactly once`, () => {
            for (const depth of [1, 4, 12, 20]) {
                let acc: ReadChainedDataFrame<any, any, any, any, any, any> = relation();
                for (let i = 0; i < depth; i += 1) acc = apply(acc as ReturnType<typeof relation>, relation());
                const counts = interpretationCounts(acc);
                expect(counts.relation, `${name} depth ${depth}`).toBe(depth + 1);
                expect(counts[algebraCall], `${name} depth ${depth}`).toBe(depth);
            }
        });
    }

    it("a 20-deep unionByName chain still interprets every operation exactly once", () => {
        let acc: ReadChainedDataFrame<any, any, any, any, any, any> = relation();
        for (let i = 0; i < 20; i += 1) acc = acc.unionByName(relation());
        const counts = interpretationCounts(acc);
        expect(counts.relation).toBe(21);
        expect(counts.union).toBe(20);
    });

    it("unary steps on top of a binary chain do not re-interpret the subtree", () => {
        const df = relation()
            .join(relation(), col("id").eq(col("other_id")))
            .filter(col("amount").gt(10))
            .select("id", "amount")
            .union(relation())
            .limit(5);
        const counts = interpretationCounts(df);
        expect(counts.relation).toBe(3);
        expect(counts.join).toBe(1);
        expect(counts.filter).toBe(1);
        expect(counts.select).toBe(1);
        expect(counts.union).toBe(1);
        expect(counts.limit).toBe(1);
    });

    it("a typed join qualifies each side once and interprets each relation once", () => {
        const left = relation().as<{ id: number; name: string }>();
        const right = relation().as<{ user_id: number; amount: number }>();
        const joined = left.join(right, (l, r) => l.id.eq(r.user_id), "INNER");
        const counts = interpretationCounts(joined);
        expect(counts.relation).toBe(2);
        expect(counts.withPlanId).toBe(2);
        expect(counts.join).toBe(1);
    });

    it("compiles a 24-deep unionByName chain to proto quickly", () => {
        let acc: ReadChainedDataFrame<any, any, any, any, any, any> = relation();
        for (let i = 0; i < 24; i += 1) acc = acc.unionByName(relation());
        const started = performance.now();
        const proto = JSON.parse(acc.toProtoJSON());
        const elapsedMs = performance.now() - started;
        expect(proto.set_op).toBeTruthy();
        expect(elapsedMs).toBeLessThan(1_000);
    });
});
