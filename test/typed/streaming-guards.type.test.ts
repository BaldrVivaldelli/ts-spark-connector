import { describe, expect, it } from "vitest";
import { SparkSession, col } from "../../src";

/**
 * The `this:` parameters on `write()` / `writeStream()` only reject crossed
 * flavours because the streaming brand inside `CDF` is kept invariant by the
 * class. These checks are compiled by `npm run typecheck:test`; the body never
 * runs so the runtime guards do not throw during the Vitest pass.
 */
function typeLevelGuards() {
    const session = SparkSession.builder().getOrCreate();
    const batch = session.read.option("header", "true").csv("/tmp/batch.csv");
    const stream = session.readStream("rate", { rowsPerSecond: "1" });

    batch.write();
    stream.writeStream();

    // @ts-expect-error a streaming DataFrame cannot use the batch writer
    stream.write();
    // @ts-expect-error a batch DataFrame cannot use the streaming writer
    batch.writeStream();

    const joinedRight = batch.join(stream, col("id").eq(col("value")));
    joinedRight.writeStream();
    // @ts-expect-error a join with a streaming side is a streaming DataFrame
    joinedRight.write();

    const joinedLeft = stream.join(batch, col("value").eq(col("id")));
    joinedLeft.writeStream();
    // @ts-expect-error a join with a streaming side is a streaming DataFrame
    joinedLeft.write();

    const unioned = batch.union(stream);
    unioned.writeStream();
    // @ts-expect-error a union with a streaming side is a streaming DataFrame
    unioned.write();

    const unionedByName = batch.unionByName(stream);
    unionedByName.writeStream();
    // @ts-expect-error a unionByName with a streaming side is a streaming DataFrame
    unionedByName.write();

    // Capabilities other than the streaming brand stay free: a sampled frame
    // and a plain frame still union, and the unary steps keep the flavour.
    batch.sample(0.5).union(batch).write();
    stream.withWatermark(col("timestamp"), "10 seconds").filter(col("value").gt(1)).writeStream();
    // @ts-expect-error unary steps keep the streaming flavour
    stream.filter(col("value").gt(1)).write();
}

describe("streaming flavour propagation", () => {
    const session = SparkSession.builder().getOrCreate();
    const batch = () => session.read.option("header", "true").csv("/tmp/batch.csv");
    const stream = () => session.readStream("rate", { rowsPerSecond: "1" });

    it("keeps the type-level guards referenced", () => {
        expect(typeof typeLevelGuards).toBe("function");
    });

    it("marks every binary operation as streaming when either side is streaming", () => {
        const cases: Array<[string, () => { isStreamingDataFrame(): boolean }]> = [
            ["batch.join(stream)", () => batch().join(stream(), col("id").eq(col("value")))],
            ["stream.join(batch)", () => stream().join(batch(), col("value").eq(col("id")))],
            ["batch.union(stream)", () => batch().union(stream())],
            ["stream.union(batch)", () => stream().union(batch())],
            ["batch.unionByName(stream)", () => batch().unionByName(stream())],
            ["batch.unionByName(stream, true)", () => batch().unionByName(stream(), true)],
            ["batch.intersect(stream)", () => batch().intersect(stream())],
            ["batch.intersectAll(stream)", () => batch().intersectAll(stream())],
            ["batch.except(stream)", () => batch().except(stream())],
            ["batch.exceptAll(stream)", () => batch().exceptAll(stream())],
        ];
        for (const [name, build] of cases) {
            expect(build().isStreamingDataFrame(), name).toBe(true);
        }
    });

    it("keeps batch-only combinations batch", () => {
        expect(batch().join(batch(), col("id").eq(col("id2"))).isStreamingDataFrame()).toBe(false);
        expect(batch().union(batch()).isStreamingDataFrame()).toBe(false);
        expect(batch().unionByName(batch()).isStreamingDataFrame()).toBe(false);
        expect(batch().except(batch()).isStreamingDataFrame()).toBe(false);
    });

    it("routes a stream-static join to the streaming writer at runtime", () => {
        const joined = batch().join(stream(), col("id").eq(col("value")));
        expect(() => (joined as unknown as { write(): unknown }).write()).toThrow(/streaming DataFrame/);
        expect(() => joined.writeStream()).not.toThrow();
    });
});
