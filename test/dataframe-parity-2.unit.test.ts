import * as arrow from "apache-arrow";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Observation, SparkSession, col, lit, schema } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { GroupType, JoinType } from "../src/engine/sparkConnectEnums";
import { ProtoDFAlg, ProtoExprAlg } from "../src/engine/compilerRead";
import { ProtoWritingAlg } from "../src/write/compilerWrite";
import { storageLevelFromProto } from "../src/client/storageLevel";

function metricsResponse(name: string, values: Record<string, bigint>): Record<string, unknown> {
    return {
        observed_metrics: [{
            name,
            keys: Object.keys(values),
            values: Object.values(values).map(value => ({ literal_type: "long", long: String(value) })),
        }],
    };
}

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

// Checkpoints and MERGE are Spark 4.0 commands, checked against the server version.
beforeEach(() => {
    vi.spyOn(SparkSession.prototype, "version").mockResolvedValue("4.0.4");
});
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
        expect(() => people().lateralJoin(right, undefined, "LEFTOUTER" as never)).toThrow(/inner, cross and left outer/);
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
        expect(() => session.readStream("rate").observe(new Observation("s"), col("value")))
            .toThrow(/streaming DataFrame/);
    });

    it("delivers metrics through save", async () => {
        const saved = new Observation("saved");
        const execute = mockExecute(metricsResponse("saved", { ks: 2n }));
        await lax().observe(saved, col("k").alias("ks")).write().parquet().mode("overwrite").save("/tmp/out");
        expect(execute).toHaveBeenCalledOnce();
        expect(saved.values).toEqual({ ks: 2n });
        await expect(saved.get()).resolves.toEqual({ ks: 2n });
    });

    it("delivers metrics through writeTo", async () => {
        const appended = new Observation("appended");
        mockExecute(metricsResponse("appended", { ks: 3n }));
        await lax().observe(appended, col("k").alias("ks")).writeTo("catalog.t").append();
        expect(appended.values).toEqual({ ks: 3n });
        await expect(appended.get()).resolves.toEqual({ ks: 3n });
    });

    it("delivers metrics through mergeInto", async () => {
        const merged = new Observation("merged");
        mockExecute(metricsResponse("merged", { ks: 4n }));
        await lax().observe(merged, col("k").alias("ks"))
            .mergeInto("catalog.t", col("catalog.t.k").eq(col("k")))
            .whenMatched().updateAll()
            .merge();
        expect(merged.values).toEqual({ ks: 4n });
        await expect(merged.get()).resolves.toEqual({ ks: 4n });
    });

    it("hands a writer backend the observations with the execution", async () => {
        const run = vi.fn(async () => undefined);
        const impl = { DF: ProtoDFAlg, EX: ProtoExprAlg, WR: ProtoWritingAlg, EXE: { run, runStream: vi.fn() } } as never;
        const stats = new Observation("stats");
        await lax().observe(stats, col("k")).write().save("/x", impl);
        expect(run.mock.calls[0]).toHaveLength(3);
        expect((run.mock.calls[0] as unknown[])[2]).toMatchObject({ observations: [stats], tags: [] });
        await lax().write().save("/x", impl);
        expect(run.mock.calls[1]).toHaveLength(2);
    });

    it("settles the observation when the action fails or its stream is abandoned", async () => {
        const failed = new Observation("failed");
        vi.spyOn(sparkGrpcClient, "executePlan").mockRejectedValueOnce(new Error("boom"));
        const df = lax().observe(failed, col("k").alias("ks"));
        await expect(df.collect()).rejects.toThrow("boom");
        await expect(failed.get()).rejects.toThrow("boom");

        // Anything thrown that is not an Error still settles the observation.
        vi.spyOn(sparkGrpcClient, "executePlan").mockRejectedValueOnce("plain failure");
        await expect(df.collect()).rejects.toBe("plain failure");
        await expect(failed.get()).rejects.toThrow("plain failure");

        // A later successful action replaces the failure.
        mockExecute(metricsResponse("failed", { ks: 1n }), arrowResponse({ k: ["a"] }));
        await df.collect();
        await expect(failed.get()).resolves.toEqual({ ks: 1n });

        const abandoned = new Observation("abandoned");
        vi.spyOn(sparkGrpcClient, "executePlanStream").mockImplementation(async function* () {
            yield arrowResponse({ k: ["a"] });
            yield metricsResponse("abandoned", { ks: 1n });
        });
        for await (const row of lax().observe(abandoned, col("k").alias("ks")).toRows()) {
            expect(row).toEqual({ k: "a" });
            break;
        }
        await expect(abandoned.get()).rejects.toThrow(/abandoned before Spark reported/);

        const silent = new Observation("silent");
        mockExecute(arrowResponse({ k: ["a"] }));
        await lax().observe(silent, col("k").alias("ks")).collect();
        await expect(silent.get()).rejects.toThrow(/without Spark reporting the metrics/);
    });

    it("delivers only to the observations of the plan that ran, even with equal names", async () => {
        const first = new Observation("stats");
        const second = new Observation("stats");
        const one = lax().observe(first, col("k").alias("ks"));
        const two = lax().observe(second, col("k").alias("ks"));
        const pendingSecond = second.get();

        mockExecute(metricsResponse("stats", { ks: 1n }));
        await one.collect();
        expect(first.values).toEqual({ ks: 1n });
        expect(second.values).toBeUndefined();

        mockExecute(metricsResponse("stats", { ks: 2n }));
        await two.collect();
        await expect(pendingSecond).resolves.toEqual({ ks: 2n });
    });

    it("carries observations through binary operations, grouping and schema casts", async () => {
        const left = new Observation("left");
        const right = new Observation("right");
        const grouped = new Observation("grouped");
        mockExecute(metricsResponse("left", { ks: 1n }), metricsResponse("right", { ks: 2n }));
        await lax().observe(left, col("k").alias("ks")).union(lax().observe(right, col("k").alias("ks"))).collect();
        expect(left.values).toEqual({ ks: 1n });
        expect(right.values).toEqual({ ks: 2n });

        mockExecute(metricsResponse("grouped", { ks: 3n }));
        await lax().observe(grouped, col("k").alias("ks")).groupBy("k").agg({ n: "count(k)" }).as().collect();
        expect(grouped.values).toEqual({ ks: 3n });
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
        expect(proto(typed).to_schema.schema.unparsed.data_type_string).toBe("name STRING, id INT");

        const table = new arrow.Table({
            name: arrow.vectorFromArray(["ada"], new arrow.Utf8()),
            id: arrow.vectorFromArray([1], new arrow.Int32()),
        });
        mockExecute({ arrow_batch: { data: Buffer.from(arrow.tableToIPC(table, "stream")) } });
        expect(await typed.collect()).toEqual([{ name: "ada", id: 1 }]);

        expect(() => lax().to("")).toThrow(TypeError);
        expect(() => lax().to({} as never)).toThrow(/DDL string or a schema/);
    });

    it("sends a nullable DDL for to(schema) while enforcing the declaration on decode", async () => {
        const declared = schema({
            id: "long",
            tags: { kind: "array", element: "int" },
            labels: { kind: "map", key: "string", value: "int" },
            nested: { kind: "struct", fields: { a: "int", b: "string?" } },
            amount: { kind: "decimal", precision: 10, scale: 2 },
        });
        const ddl = proto(lax().to(declared)).to_schema.schema.unparsed.data_type_string as string;
        expect(ddl).not.toMatch(/NOT NULL/);
        expect(ddl).toContain("id BIGINT");
        expect(ddl).toContain("ARRAY<INT>");
        expect(ddl).toMatch(/MAP<STRING, ?INT>/);
        expect(ddl).toContain("STRUCT<");
        expect(ddl).toMatch(/DECIMAL\(10, ?2\)/);

        const table = new arrow.Table({
            id: arrow.vectorFromArray([1n, null], new arrow.Int64()),
        });
        mockExecute({ arrow_batch: { data: Buffer.from(arrow.tableToIPC(table, "stream")) } });
        await expect(lax().to(schema({ id: "long" })).collect()).rejects.toThrow(/id/);
    });

    it("exposes column metadata reported by Spark's schema", async () => {
        mockAnalyze({
            schema: {
                schema: {
                    struct: {
                        fields: [
                            { name: "k", data_type: { string: {} }, nullable: true, metadata: '{"comment":"key"}' },
                            { name: "v", data_type: { integer: {} }, nullable: false, metadata: "" },
                        ],
                    },
                },
            },
        });
        const { fields } = await lax().withMetadata("k", { comment: "key" }).schema();
        expect(fields[0]).toEqual({ name: "k", dataType: "string", nullable: true, metadata: '{"comment":"key"}' });
        expect(fields[1]).toEqual({ name: "v", dataType: "int", nullable: false });
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

    it("releases a checkpoint explicitly, once, and only from the DataFrame checkpoint() returned", async () => {
        const execute = mockExecute(checkpointResult);
        const checkpointed = await lax().localCheckpoint();
        await checkpointed.releaseCheckpoint();
        expect(executedPlan(execute, 1).command).toEqual({
            remove_cached_remote_relation_command: { relation: { relation_id: "cp-1" } },
        });
        await checkpointed.releaseCheckpoint();
        expect(execute).toHaveBeenCalledTimes(2);
        await expect(checkpointed.filter(col("k").eq("a")).releaseCheckpoint()).rejects.toThrow(/returned by checkpoint/);
        await expect(lax().releaseCheckpoint()).rejects.toThrow(TypeError);
        await expect(checkpointed.as().releaseCheckpoint()).resolves.toBeUndefined();
    });

    it("keeps tracking a checkpoint whose release failed so close() retries it", async () => {
        const owner = SparkSession.builder().getOrCreate();
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([checkpointResult] as never)
            .mockRejectedValueOnce(new Error("down"))
            .mockResolvedValueOnce([] as never);
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);

        const checkpointed = await owner.read.parquet("/data/a.parquet").localCheckpoint();
        await expect(checkpointed.releaseCheckpoint()).rejects.toThrow("down");
        await owner.close();
        expect(execute).toHaveBeenCalledTimes(3);
        expect(executedPlan(execute, 2).command).toEqual({
            remove_cached_remote_relation_command: { relation: { relation_id: "cp-1" } },
        });
    });

    it("releases the remaining checkpoints in parallel and stops at the first UNAVAILABLE", async () => {
        const owner = SparkSession.builder().getOrCreate();
        const settled: string[] = [];
        const execute = vi.spyOn(sparkGrpcClient, "executePlan")
            .mockResolvedValueOnce([checkpointResult] as never)
            .mockResolvedValueOnce([{ checkpoint_command_result: { relation: { relation_id: "cp-2" } } }] as never)
            .mockImplementationOnce(async () => {
                settled.push("first");
                throw Object.assign(new Error("unavailable"), { code: 14 });
            })
            .mockImplementationOnce(async (_request, config) => {
                await new Promise(resolve => setTimeout(resolve, 5));
                settled.push(`second:${config?.signal?.aborted}`);
                return [];
            });
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);

        await owner.read.parquet("/data/a.parquet").localCheckpoint();
        await owner.read.parquet("/data/b.parquet").checkpoint();
        await owner.close();

        // Both releases were in flight at once and share one abort signal,
        // which the UNAVAILABLE failure tripped before the second one finished.
        expect(settled).toEqual(["first", "second:true"]);
        const releases = execute.mock.calls.slice(2);
        expect(releases).toHaveLength(2);
        expect(releases[0]![1]?.signal).toBeInstanceOf(AbortSignal);
        expect(releases[0]![1]?.signal).toBe(releases[1]![1]?.signal);
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

describe("transform", () => {
    it("applies a pipeline step and returns its result", () => {
        const df = session.read.parquet("/data/people.parquet");
        const onlyAdults = (input: typeof df) => input.filter(col("age").gte(18));
        expect(proto(df.transform(onlyAdults))).toEqual(proto(onlyAdults(df)));
        expect(df.transform(input => input.toProtoJSON())).toBe(df.toProtoJSON());
        expect(() => df.transform("nope" as never)).toThrow(/takes a function/);
    });
});
