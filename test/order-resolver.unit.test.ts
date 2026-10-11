import { describe, expect, it } from "vitest";
import { resolveOrderInput } from "../src/read/orderResolver";
import { SparkExprAlg } from "../src/read/readDataFrameInterpreter";
import { SortKey, TypedColumn } from "../src/typed/typed-column";

// orderBy()/sort() accept column names, untyped builders, typed column
// callbacks, typed sort keys and the legacy SortKeyBuilder callback. Every
// input shape must resolve to the same SortOrder structure.

describe("resolveOrderInput", () => {
    it("resolves every accepted input shape to a SortOrder", () => {
        const untyped = { build: () => SparkExprAlg.col("object") };
        const typed = new TypedColumn<number, ReturnType<typeof SparkExprAlg.col>>(
            EX => EX.col("typed"),
        );
        const sort = new SortKey(EX => EX.col("sorted"), "desc", "nullsLast");

        expect(resolveOrderInput("name", SparkExprAlg)).toMatchObject({ direction: "asc" });
        expect(resolveOrderInput(untyped, SparkExprAlg)).toMatchObject({ direction: "asc" });
        expect(resolveOrderInput(() => typed, SparkExprAlg)).toMatchObject({ direction: "asc" });
        expect(resolveOrderInput(() => sort, SparkExprAlg)).toMatchObject({
            direction: "desc",
            nulls: "nullsLast",
        });
        expect(resolveOrderInput(
            (algebra: typeof SparkExprAlg) => ({
                expr: algebra.col("legacy"),
                direction: "desc",
            }),
            SparkExprAlg,
        )).toMatchObject({ direction: "desc" });
    });
});
