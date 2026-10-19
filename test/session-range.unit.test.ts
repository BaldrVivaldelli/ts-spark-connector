import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession } from "../src";
import type { RangeRow } from "../src";
import { ProtoDFAlg } from "../src/engine/compilerRead";
import { SparkDFAlg } from "../src/read/readDataFrameInterpreter";
import { TraceDFAlg } from "../src/trace/trace";
import { sparkGrpcClient } from "../src/client/sparkClient";

const session = SparkSession.builder().config("spark.connect.url", "sc://localhost:15002").getOrCreate();

describe("range relation", () => {
    it("compiles bounds as int64 strings and omits absent optional fields", () => {
        expect(ProtoDFAlg.range(undefined, 10n, 1n, undefined)).toEqual({ range: { end: "10", step: "1" } });
        expect(ProtoDFAlg.range(2n, 9007199254740993n, -3n, 4)).toEqual({
            range: { start: "2", end: "9007199254740993", step: "-3", num_partitions: 4 },
        });
    });

    it("exposes the bounds in the logical plan and labels the trace node", () => {
        expect(SparkDFAlg.range(undefined, 5n, 1n, undefined)).toEqual({ type: "Range", end: 5n, step: 1n });
        expect(SparkDFAlg.range(1n, 5n, 2n, 3)).toEqual({ type: "Range", start: 1n, end: 5n, step: 2n, numPartitions: 3 });
        expect(JSON.stringify(TraceDFAlg.range(undefined, 5n, 1n, undefined))).toContain("range[0..5 step 1]");
        expect(JSON.stringify(TraceDFAlg.range(1n, 5n, 2n, 3))).toContain("range[1..5 step 2 partitions 3]");
    });

    it("builds a typed DataFrame from one or four arguments", () => {
        const single = session.range(5);
        expect(JSON.parse(single.toProtoJSON())).toEqual({ range: { end: "5", step: "1" } });
        const full = session.range(1n, 10, 3, 2);
        expect(JSON.parse(full.toProtoJSON())).toEqual({ range: { start: "1", end: "10", step: "3", num_partitions: 2 } });
        // The logical plan keeps bigints, serialised as strings.
        expect(JSON.parse(session.range(2, 4).toSparkLogicalPlanJSON())).toEqual({
            type: "Range", start: "2", end: "4", step: "1",
        });
        const rows: RangeRow[] = [{ id: 1n }];
        expect(rows[0]?.id).toBe(1n);
    });

    it("rejects invalid bounds, steps and partition counts eagerly", () => {
        expect(() => session.range(1.5)).toThrow(/safe integer or a bigint/);
        expect(() => session.range(0, 10, 0)).toThrow(/step must not be 0/);
        expect(() => session.range(0, 10, 1, 0)).toThrow(/positive integer/);
        expect(() => session.range(0, 10, 1, 1.5)).toThrow(/positive integer/);
        expect(() => session.range(2n ** 63n)).toThrow(/int64/);
        expect(() => session.range(0, 2n ** 64n)).toThrow(/int64/);
        expect(() => session.range("3" as never)).toThrow(TypeError);
    });
});

describe("newSession", () => {
    afterEach(() => vi.restoreAllMocks());

    it("creates an independent session with the same connection and user, without an RPC", async () => {
        const config = vi.spyOn(sparkGrpcClient, "config");
        const execute = vi.spyOn(sparkGrpcClient, "executePlan");
        const parent = SparkSession.builder()
            .config("spark.connect.url", "sc://host:15002")
            .config("spark.sql.shuffle.partitions", 3)
            .user({ user_id: "ana" })
            .withRpcTimeout(4_000)
            .getOrCreate();
        parent.addTag("only-parent");

        const child = parent.newSession();
        expect(child).toBeInstanceOf(SparkSession);
        expect(child.getSessionId()).not.toBe(parent.getSessionId());
        expect(child.getSessionId()).toMatch(/^[0-9a-f-]{36}$/);
        expect(child.getUserContext()).toEqual(parent.getUserContext());
        expect(child.getConnectionConfig().rpcTimeoutMs).toBe(4_000);
        expect(child.getSessionConfig()).toEqual(parent.getSessionConfig());
        expect(child.getTags()).toEqual([]);
        expect(config).not.toHaveBeenCalled();
        expect(execute).not.toHaveBeenCalled();

        // Later changes on either side stay local.
        child.addTag("only-child");
        expect(parent.getTags()).toEqual(["only-parent"]);
    });

    it("refuses to spawn from a closed session", async () => {
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        const parent = SparkSession.builder().getOrCreate();
        await parent.close();
        expect(() => parent.newSession()).toThrow(/closed/);
    });
});
