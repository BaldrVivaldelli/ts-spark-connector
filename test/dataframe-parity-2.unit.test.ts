import * as arrow from "apache-arrow";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Observation, SparkSession, col, lit, schema } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { GroupType, JoinType } from "../src/engine/sparkConnectEnums";
import { ProtoDFAlg, ProtoExprAlg } from "../src/engine/compilerRead";
import { ProtoWritingAlg } from "../src/write/compilerWrite";
import { storageLevelFromProto } from "../src/client/storageLevel";

type ProtoObject = Record<string, any>;

function proto(df: { toProtoJSON(): string }): ProtoObject {
    return JSON.parse(df.toProtoJSON()) as ProtoObject;
}

function arrowResponse(columns: Record<string, ArrayLike<unknown>>): Record<string, unknown> {
    const table = arrow.tableFromArrays(columns as Record<string, any>) as arrow.Table;
    return { arrow_batch: { data: Buffer.from(arrow.tableToIPC(table, "stream")) } };
}

function peopleResponse(): Record<string, unknown> {
    const table = new arrow.Table({
        id: arrow.vectorFromArray([1n], new arrow.Int64()),
        name: arrow.vectorFromArray(["ada"], new arrow.Utf8()),
        age: arrow.vectorFromArray([30], new arrow.Int32()),
    });
    return { arrow_batch: { data: Buffer.from(arrow.tableToIPC(table, "stream")) } };
}

/** Captures the ExecutePlan requests and answers each with the given responses. */
function mockExecute(...responses: Array<Record<string, unknown>>) {
    return vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue(responses as never);
}

function mockAnalyze(response: Record<string, unknown>) {
    return vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue(response as never);
}

function executedPlan(spy: ReturnType<typeof mockExecute>, call = 0): ProtoObject {
    return (spy.mock.calls[call]![0] as ProtoObject).plan;
}

const session = SparkSession.builder().getOrCreate();
const People = schema({ id: "long", name: "string?", age: "int?" });
const people = () => session.read.readWith(People, "parquet", "/data/people.parquet");
const lax = () => session.read.parquet("/data/lax.parquet");
const attribute = (name: string) => ({ unresolved_attribute: { unparsed_identifier: name } });

afterEach(() => vi.restoreAllMocks());

describe("grouping variants", () => {
    it("compiles rollup, cube and grouping sets to the Aggregate group types", () => {
        expect(proto(people().rollup("name").agg({ n: "count(id)" })).aggregate).toMatchObject({
            group_type: GroupType.GROUP_TYPE_ROLLUP,
            grouping_expressions: [attribute("name")],
        });
        expect(proto(lax().cube(col("a"), "b").agg({ n: col("x").alias("ignored") })).aggregate).toMatchObject({
            group_type: GroupType.GROUP_TYPE_CUBE,
            grouping_expressions: [attribute("a"), attribute("b")],
        });
        expect(proto(people().groupingSets([["name"], []], "name").agg({ n: "count(id)" })).aggregate).toMatchObject({
            group_type: GroupType.GROUP_TYPE_GROUPING_SETS,
            grouping_expressions: [attribute("name")],
            grouping_sets: [{ grouping_set: [attribute("name")] }, { grouping_set: [] }],
        });
        expect(proto(lax().groupingSets([[col("k")], ["k"]], "k").agg({ n: "count(id)" })).aggregate.grouping_sets)
            .toEqual([{ grouping_set: [attribute("k")] }, { grouping_set: [attribute("k")] }]);
        expect(proto(people().rollup().agg({ n: "count(id)" })).aggregate).toMatchObject({
            group_type: GroupType.GROUP_TYPE_ROLLUP,
            grouping_expressions: [],
        });
    });

    it("compiles pivots with explicit literal values or none", () => {
        const explicit = proto(people().groupBy("name").pivot("age", [30, "x", true, null, 2n]).agg({ n: "count(id)" })).aggregate;
        expect(explicit).toMatchObject({
            group_type: GroupType.GROUP_TYPE_PIVOT,
            pivot: {
                col: attribute("age"),
                values: [{ integer: 30 }, { string: "x" }, { boolean: true }, { null: { null: {} } }, { long: "2" }],
            },
        });
        const collected = proto(people().groupBy("name").pivot(col("age")).agg({ n: "count(id)" })).aggregate;
        expect(collected.pivot).toEqual({ col: attribute("age") });
        expect(collected).not.toHaveProperty("grouping_sets");
    });

    it("validates grouping inputs eagerly", () => {
        expect(() => people().rollup("id", "id")).toThrow(/rollup\(\)/);
        expect(() => people().cube("id", "id")).toThrow(/cube\(\)/);
        expect(() => people().groupingSets([])).toThrow(/at least one grouping set/);
        expect(() => people().groupingSets("id" as never)).toThrow(/at least one grouping set/);
        expect(() => people().groupingSets(["id"] as never)).toThrow(/arrays of columns/);
        expect(() => lax().groupingSets([[""]])).toThrow(TypeError);
        expect(() => people().rollup("id").pivot("name")).toThrow(/plain groupBy/);
        expect(() => people().groupBy("id").pivot("")).toThrow(TypeError);
        expect(() => people().groupBy("id").pivot("name", [])).toThrow(/non-empty array/);
        expect(() => people().groupBy("id").pivot("name", "x" as never)).toThrow(/non-empty array/);
    });
});

describe("sorting, deduplication and column references", () => {
    it("sorts within partitions and deduplicates within the watermark", () => {
        const sorted = proto(people().sortWithinPartitions(col("age").desc(), "id")).sort;
        expect(sorted.is_global).toBe(false);
        expect(sorted.order).toHaveLength(2);
        expect(() => people().sortWithinPartitions()).toThrow(/at least one column/);

        expect(proto(people().dropDuplicatesWithinWatermark()).deduplicate).toEqual({
            input: expect.anything(),
            all_columns_as_keys: true,
            within_watermark: true,
        });
        expect(proto(people().dropDuplicatesWithinWatermark("id")).deduplicate).toMatchObject({
            column_names: ["id"],
            within_watermark: true,
        });
        expect(proto(lax().dropDuplicatesWithinWatermark(col("k"))).deduplicate).toMatchObject({ column_names: ["k"] });
    });

    it("builds regex and metadata column references", () => {
        const df = lax();
        const expressions = proto(df.select(df.colRegex("`(id|name)`"), df.metadataColumn("_metadata"))).project.expressions;
        expect(expressions).toEqual([
            { unresolved_regex: { col_name: "`(id|name)`" } },
            { unresolved_attribute: { unparsed_identifier: "_metadata", is_metadata_column: true } },
        ]);
        expect(() => df.colRegex("")).toThrow(TypeError);
        expect(() => df.metadataColumn(" ")).toThrow(TypeError);
    });
});

describe("lateral and as-of joins", () => {
    it("compiles lateral joins with an optional condition", () => {
        const right = lax();
        expect(proto(people().lateralJoin(right, col("id").eq(col("k")), "left")).lateral_join).toMatchObject({
            join_condition: { unresolved_function: { function_name: "=" } },
            join_type: JoinType.JOIN_TYPE_LEFT_OUTER,
        });
        const bare = proto(people().lateralJoin(right)).lateral_join;
        expect(bare.join_type).toBe(JoinType.JOIN_TYPE_INNER);
        expect(bare).not.toHaveProperty("join_condition");
        expect(proto(people().lateralJoin(right, undefined, "cross")).lateral_join.join_type).toBe(JoinType.JOIN_TYPE_CROSS);
        expect(() => people().lateralJoin(right, undefined, "RIGHT")).toThrow(/inner, cross and left outer/);
        expect(people().lateralJoin(session.readStream("rate")).isStreamingDataFrame()).toBe(true);
    });

    it("compiles as-of joins with using columns, expressions and options", () => {
        const right = lax();
        expect(proto(people().asOfJoin(right, "id", "rid")).as_of_join).toEqual({
            left: expect.anything(),
            right: expect.anything(),
            left_as_of: attribute("id"),
            right_as_of: attribute("rid"),
            join_type: "inner",
            allow_exact_matches: true,
            direction: "backward",
        });
        expect(proto(people().asOfJoin(right, "id", "rid", "name")).as_of_join).toMatchObject({ using_columns: ["name"] });
        expect(proto(people().asOfJoin(right, "id", "rid", ["name", "k"], "left", {
            tolerance: 2,
            allowExactMatches: false,
            direction: "forward",
        })).as_of_join).toMatchObject({
            using_columns: ["name", "k"],
            join_type: "left",
            tolerance: { literal: { integer: 2 } },
            allow_exact_matches: false,
            direction: "forward",
        });
        expect(proto(people().asOfJoin(right, col("id"), col("rid"), col("name").eq(col("n")), "inner", {
            tolerance: lit(5n),
        })).as_of_join).toMatchObject({
            join_expr: { unresolved_function: { function_name: "=" } },
            tolerance: { literal: { long: "5" } },
        });
        expect(proto(people().asOfJoin(right, "id", "rid", undefined, "inner", { tolerance: 3n })).as_of_join)
            .toMatchObject({ tolerance: { literal: { long: "3" } } });
    });

    it("validates as-of join inputs eagerly", () => {
        const right = lax();
        expect(() => people().asOfJoin(right, "", "rid")).toThrow(TypeError);
        expect(() => people().asOfJoin(right, "id", "")).toThrow(TypeError);
        expect(() => people().asOfJoin(right, "id", "rid", undefined, "")).toThrow(TypeError);
        expect(() => people().asOfJoin(right, "id", "rid", undefined, "inner", { direction: "sideways" as never }))
            .toThrow(/backward, forward or nearest/);
        expect(() => people().asOfJoin(right, "id", "rid", undefined, "inner", { allowExactMatches: "yes" as never }))
            .toThrow(/must be a boolean/);
        expect(() => people().asOfJoin(right, "id", "rid", [])).toThrow(/at least one column/);
        expect(() => people().asOfJoin(right, "id", "rid", [""])).toThrow(TypeError);
        expect(() => people().asOfJoin(right, "id", "rid", undefined, "inner", { tolerance: Number.POSITIVE_INFINITY }))
            .toThrow(/finite/);
    });
});

describe("observe", () => {
    it("compiles CollectMetrics and routes the observed metrics to the Observation", async () => {
        const stats = new Observation("stats");
        const df = people().observe(stats, col("id").alias("ids"), lit(1).alias("one"));
        expect(proto(df).collect_metrics).toMatchObject({
            name: "stats",
            metrics: [{ alias: { expr: attribute("id"), name: ["ids"] } }, { alias: { name: ["one"] } }],
        });

        const execute = mockExecute(
            { observed_metrics: [{ name: "stats", keys: ["ids"], values: [{ literal_type: "long", long: "3" }] }] },
            { observed_metrics: [{ name: "other", keys: ["x"], values: [{ literal_type: "long", long: "9" }] }] },
            peopleResponse(),
        );
        expect(await df.collect()).toEqual([{ id: 1n, name: "ada", age: 30 }]);
        await expect(stats.get()).resolves.toEqual({ ids: 3n });
        expect(execute).toHaveBeenCalledOnce();
    });

    it("delivers metrics through the streaming actions and accepts plain names", async () => {
        const stats = new Observation();
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(async function* () {
            yield { observed_metrics: [{ name: stats.name, keys: ["ids"], values: [{ literal_type: "long", long: "1" }] }] };
            yield arrowResponse({ k: ["a"] });
        });
        const rows = [];
        for await (const row of lax().observe(stats, col("k").alias("ids")).toRows()) rows.push(row);
        expect(rows).toEqual([{ k: "a" }]);
        expect(stats.values).toEqual({ ids: 1n });

        expect(proto(lax().observe("named", col("k"))).collect_metrics.name).toBe("named");
        expect(() => lax().observe("named")).toThrow(/at least one metric/);
        expect(() => lax().observe("", col("k"))).toThrow(TypeError);
    });
});

describe("schema reconciliation, metadata and JSON rows", () => {
    it("compiles to(schema) with a DDL string or a declaration and types the result", async () => {
        expect(proto(lax().to("name STRING, id INT")).to_schema).toEqual({
            input: expect.anything(),
            schema: { unparsed: { data_type_string: "name STRING, id INT" } },
        });
        const declared = schema({ name: "string?", id: "int" });
        const typed = lax().to(declared);
        expect(proto(typed).to_schema.schema.unparsed.data_type_string).toBe(declared.toDDL());

        const table = new arrow.Table({
            name: arrow.vectorFromArray(["ada"], new arrow.Utf8()),
            id: arrow.vectorFromArray([1], new arrow.Int32()),
        });
        mockExecute({ arrow_batch: { data: Buffer.from(arrow.tableToIPC(table, "stream")) } });
        expect(await typed.collect()).toEqual([{ name: "ada", id: 1 }]);

        expect(() => lax().to("")).toThrow(TypeError);
        expect(() => lax().to({} as never)).toThrow(/DDL string or a schema/);
    });

    it("attaches JSON metadata to a column and rejects anything but objects", () => {
        expect(proto(people().withMetadata("id", { comment: "pk" })).with_columns).toEqual({
            input: expect.anything(),
            aliases: [{ expr: attribute("id"), name: ["id"], metadata: '{"comment":"pk"}' }],
        });
        expect(proto(people().withMetadata("name", '{"a":1}')).with_columns.aliases[0].metadata).toBe('{"a":1}');
        expect(() => lax().withMetadata("", {})).toThrow(TypeError);
        expect(() => people().withMetadata("id", "not json")).toThrow(/JSON object/);
        expect(() => people().withMetadata("id", "[1]")).toThrow(/JSON object/);
        expect(() => people().withMetadata("id", "null")).toThrow(/JSON object/);
    });

    it("projects every row to a JSON document named value", async () => {
        const json = proto(people().toJSON()).project.expressions[0].alias;
        expect(json.name).toEqual(["value"]);
        expect(json.expr).toEqual({
            unresolved_function: {
                function_name: "to_json",
                arguments: [{ unresolved_function: { function_name: "struct", arguments: [{ unresolved_star: {} }] } }],
            },
        });
        mockExecute(arrowResponse({ value: ['{"id":1}'] }));
        expect(await people().toJSON().collect()).toEqual([{ value: '{"id":1}' }]);
    });
});

describe("storage levels", () => {
    it("decodes the reported level and names the known combinations", async () => {
        const analyze = mockAnalyze({
            get_storage_level: {
                storage_level: { use_disk: true, use_memory: true, use_off_heap: false, deserialized: true, replication: 1 },
            },
        });
        await expect(people().storageLevel()).resolves.toEqual({
            useDisk: true,
            useMemory: true,
            useOffHeap: false,
            deserialized: true,
            replication: 1,
            name: "MEMORY_AND_DISK",
        });
        expect(analyze.mock.calls[0]![0]).toMatchObject({ get_storage_level: { relation: proto(people()) } });

        mockAnalyze({ get_storage_level: { storage_level: {} } });
        await expect(people().storageLevel()).resolves.toMatchObject({ name: "NONE", replication: 1 });

        mockAnalyze({ get_storage_level: { storage_level: { use_disk: true, use_memory: true, deserialized: true, replication: 2 } } });
        const replicated = await people().storageLevel();
        expect(replicated.replication).toBe(2);
        expect(replicated).not.toHaveProperty("name");
        expect(storageLevelFromProto(undefined)).toMatchObject({ name: "NONE", useDisk: false });
    });
});

describe("checkpoints", () => {
    const checkpointResult = { checkpoint_command_result: { relation: { relation_id: "cp-1" } } };

    it("runs the checkpoint command and reads the cached relation back with the same schema", async () => {
        const execute = mockExecute(checkpointResult);
        const checkpointed = await people().checkpoint();
        expect(executedPlan(execute).command.checkpoint_command).toEqual({
            relation: proto(people()),
            local: false,
            eager: true,
        });
        expect(proto(checkpointed)).toEqual({ cached_remote_relation: { relation_id: "cp-1" } });

        mockExecute(peopleResponse());
        expect(await checkpointed.collect()).toEqual([{ id: 1n, name: "ada", age: 30 }]);
    });

    it("passes local checkpoint options and validates them", async () => {
        const execute = mockExecute(checkpointResult);
        await lax().localCheckpoint(false, "disk_only");
        expect(executedPlan(execute).command.checkpoint_command).toMatchObject({
            local: true,
            eager: false,
            storage_level: { use_disk: true, use_memory: false, use_off_heap: false, deserialized: false, replication: 1 },
        });
        await expect(lax().localCheckpoint(true, "nope")).rejects.toThrow(/Unsupported storage level/);
        await expect(lax().checkpoint("yes" as never)).rejects.toThrow(/eager must be a boolean/);
        await expect(session.readStream("rate").checkpoint()).rejects.toThrow(/not available on a streaming/);
        mockExecute({});
        await expect(lax().checkpoint()).rejects.toThrow(/missing the cached relation id/);
    });

    it("releases checkpointed relations when the session closes, even if the server refuses", async () => {
        const owner = SparkSession.builder().getOrCreate();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([checkpointResult] as never)
            .mockResolvedValueOnce([{ ...checkpointResult, checkpoint_command_result: { relation: { relation_id: "cp-2" } } }] as never)
            .mockRejectedValueOnce(new Error("gone"))
            .mockResolvedValueOnce([] as never);
        const release = vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);

        await owner.read.parquet("/data/a.parquet").localCheckpoint();
        await owner.read.parquet("/data/b.parquet").checkpoint();
        await owner.close();

        expect(execute).toHaveBeenCalledTimes(4);
        expect(executedPlan(execute, 2).command).toEqual({
            remove_cached_remote_relation_command: { relation: { relation_id: "cp-1" } },
        });
        expect(executedPlan(execute, 3).command).toEqual({
            remove_cached_remote_relation_command: { relation: { relation_id: "cp-2" } },
        });
        expect(release).toHaveBeenCalledOnce();
        expect(execute.mock.invocationCallOrder[3]).toBeLessThan(release.mock.invocationCallOrder[0]!);
    });

    it("names the observed server session when releasing checkpoints", async () => {
        const owner = SparkSession.builder().getOrCreate();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([{ ...checkpointResult, server_side_session_id: "server-1" }] as never)
            .mockResolvedValueOnce([] as never);
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);

        await owner.read.parquet("/data/a.parquet").localCheckpoint();
        expect(owner.getServerSideSessionId()).toBe("server-1");
        await owner.close();
        expect(execute.mock.calls[1]![0]).toMatchObject({
            client_observed_server_side_session_id: "server-1",
            plan: { command: { remove_cached_remote_relation_command: { relation: { relation_id: "cp-1" } } } },
        });
    });

    it("sends no release when nothing was checkpointed", async () => {
        const owner = SparkSession.builder().getOrCreate();
        const execute = mockExecute(arrowResponse({ k: ["a"] }));
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
        await owner.read.parquet("/data/a.parquet").collect();
        await owner.close();
        expect(execute).toHaveBeenCalledOnce();
    });
});

describe("temp views from the DataFrame", () => {
    it("registers session and global views with the right flags", async () => {
        const execute = mockExecute();
        await people().createTempView("tv");
        await people().createOrReplaceTempView("tv");
        await people().createGlobalTempView("gv");
        await people().createOrReplaceGlobalTempView("gv");
        expect([0, 1, 2, 3].map(call => executedPlan(execute, call).command.create_dataframe_view)).toEqual([
            expect.objectContaining({ name: "tv", is_global: false, replace: false }),
            expect.objectContaining({ name: "tv", is_global: false, replace: true }),
            expect.objectContaining({ name: "gv", is_global: true, replace: false }),
            expect.objectContaining({ name: "gv", is_global: true, replace: true }),
        ]);
    });

    it("accepts a writer implementation and reports backends without global views", async () => {
        const run = vi.fn(async () => undefined);
        const impl = { DF: ProtoDFAlg, EX: ProtoExprAlg, WR: ProtoWritingAlg, EXE: { run, runStream: vi.fn() } } as never;
        await people().write().createGlobalTempView("gv", impl);
        await people().write().createOrReplaceGlobalTempView("gv", impl);
        expect(run).toHaveBeenCalledTimes(2);

        const bare = { ...(impl as object), WR: {} } as never;
        await expect(people().write().createGlobalTempView("gv", bare)).rejects.toThrow(/createGlobalTempView/);
        await expect(people().write().createOrReplaceGlobalTempView("gv", bare)).rejects.toThrow(/createOrReplaceGlobalTempView/);
        await expect(people().write().createGlobalTempView("")).rejects.toThrow(TypeError);
        await expect(people().write().createOrReplaceGlobalTempView(" ")).rejects.toThrow(TypeError);
    });
});
