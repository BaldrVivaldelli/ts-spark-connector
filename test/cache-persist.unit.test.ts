import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { getPendingAnalyzeActions, ProtoDFAlg, ProtoExprAlg } from "../src/engine/compilerRead";

describe("cache/persist AnalyzePlan operations", () => {
    afterEach(() => vi.restoreAllMocks());

    it("keeps the relation wire-valid and defers persist until execution", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const df = SparkSession.builder().getOrCreate().read.parquet("/data/input.parquet").cache();
        const rawPlan = df.runWith(ProtoDFAlg, ProtoExprAlg);
        const plan = JSON.parse(df.toProtoJSON());

        expect(plan).toEqual({
            read: {
                data_source: {
                    format: "parquet",
                    paths: ["/data/input.parquet"],
                    options: {},
                },
            },
        });
        expect(getPendingAnalyzeActions(rawPlan)).toHaveLength(1);

        await df.collectRaw();
        expect(analyze).toHaveBeenCalledTimes(1);
        expect(analyze.mock.calls[0][0]).toMatchObject({
            persist: {
                relation: plan,
                storage_level: {
                    use_disk: true,
                    use_memory: true,
                    use_off_heap: false,
                    deserialized: true,
                    replication: 1,
                },
            },
        });
    });

    it("serializes unpersist as AnalyzePlan and validates storage levels", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const session = SparkSession.builder().getOrCreate();

        await session.read.parquet("/data/input.parquet").unpersist(true).collectRaw();
        expect(analyze.mock.calls[0][0]).toMatchObject({ unpersist: { blocking: true } });

        await expect(
            session.read.parquet("/data/input.parquet").persist("UNKNOWN").collectRaw()
        ).rejects.toThrow(/Unsupported storage level/);
    });

    it("applies deferred actions before incremental response/Arrow/row streams", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({});
        const stream = vi.spyOn(sparkGrpcClient, "executePlanStream")
            .mockImplementation(async function* () {
                yield { result_complete: {} };
            });
        const df = SparkSession.builder().getOrCreate()
            .read.parquet("/data/input.parquet")
            .persist("MEMORY_ONLY");

        const responses: unknown[] = [];
        for await (const response of df.streamRaw()) responses.push(response);

        expect(responses).toHaveLength(1);
        expect(analyze).toHaveBeenCalledTimes(1);
        expect(stream).toHaveBeenCalledTimes(1);
        expect(analyze.mock.invocationCallOrder[0]).toBeLessThan(stream.mock.invocationCallOrder[0]);
    });
});
