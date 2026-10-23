import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
    DataFrameWriterV2,
    MergeIntoWriter,
    SparkSession,
    call,
    col,
    lit,
} from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";

const session = SparkSession.builder().getOrCreate();
const source = () => session.read.parquet("/data/updates.parquet");

type ProtoObject = Record<string, any>;

function executedCommand(spy: { mock: { calls: unknown[][] } }, index = 0): ProtoObject {
    return (spy.mock.calls[index]![0] as ProtoObject).plan.command;
}

afterEach(() => vi.restoreAllMocks());

describe("DataFrameWriterV2", () => {
    it("builds the write_operation_v2 command with table configuration", () => {
        const writer = source()
            .writeTo("db.events")
            .using("delta")
            .option("mergeSchema", true)
            .options({ compression: "zstd", retries: 3 })
            .tableProperty("delta.appendOnly", "false")
            .partitionedBy("day", call("bucket", [lit(4), col("id")]))
            .clusterBy("region");
        expect(writer).toBeInstanceOf(DataFrameWriterV2);
        const command = writer.buildCommand("createOrReplace").write_operation_v2 as ProtoObject;
        expect(command).toMatchObject({
            input: { read: { data_source: { format: "parquet" } } },
            table_name: "db.events",
            provider: "delta",
            options: { mergeSchema: "true", compression: "zstd", retries: "3" },
            table_properties: { "delta.appendOnly": "false" },
            mode: "MODE_CREATE_OR_REPLACE",
            clustering_columns: ["region"],
        });
        expect(command.partitioning_columns).toEqual([
            { unresolved_attribute: { unparsed_identifier: "day" } },
            {
                unresolved_function: {
                    function_name: "bucket",
                    arguments: [{ literal: { integer: 4 } }, { unresolved_attribute: { unparsed_identifier: "id" } }],
                },
            },
        ]);
        expect(command.overwrite_condition).toBeUndefined();
    });

    it("omits the provider by default and is immutable", () => {
        const base = source().writeTo("t");
        const configured = base.using("delta");
        expect(base.buildCommand("append").write_operation_v2).not.toHaveProperty("provider");
        expect(configured.buildCommand("append").write_operation_v2).toHaveProperty("provider", "delta");
    });

    it.each([
        ["create", (w: DataFrameWriterV2) => w.create(), "MODE_CREATE"],
        ["replace", (w: DataFrameWriterV2) => w.replace(), "MODE_REPLACE"],
        ["createOrReplace", (w: DataFrameWriterV2) => w.createOrReplace(), "MODE_CREATE_OR_REPLACE"],
        ["append", (w: DataFrameWriterV2) => w.append(), "MODE_APPEND"],
        ["overwritePartitions", (w: DataFrameWriterV2) => w.overwritePartitions(), "MODE_OVERWRITE_PARTITIONS"],
    ] as const)("%s executes the command with %s", async (_name, run, mode) => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([
            { session_id: session.getSessionId(), server_side_session_id: "server-v2" },
        ]);
        await expect(run(source().writeTo("t"))).resolves.toBeUndefined();
        const request = execute.mock.calls[0]![0] as ProtoObject;
        expect(request.operation_id).toEqual(expect.any(String));
        expect(executedCommand(execute).write_operation_v2).toMatchObject({ table_name: "t", mode });
    });

    it("overwrites the rows matching a condition", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        await source().writeTo("t").overwrite(col("day").eq("2026-10-19"));
        expect(executedCommand(execute).write_operation_v2).toMatchObject({
            mode: "MODE_OVERWRITE",
            overwrite_condition: { unresolved_function: { function_name: "=" } },
        });
        await expect(source().writeTo("t").overwrite(undefined as never)).rejects.toThrow(/condition expression/);
    });

    it("applies pending cache actions before the command", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({});
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        await source().cache().writeTo("t").append();
        expect(analyze).toHaveBeenCalledOnce();
        expect(execute).toHaveBeenCalledOnce();
    });

    it("validates its arguments", () => {
        expect(() => source().writeTo(" ")).toThrow(TypeError);
        const writer = source().writeTo("t");
        expect(() => writer.using("")).toThrow(TypeError);
        expect(() => writer.option("", "x")).toThrow(TypeError);
        expect(() => writer.option("k", null as never)).toThrow(/null or undefined/);
        expect(() => writer.option("k", Number.NaN)).toThrow(RangeError);
        expect(() => writer.options({ "": "x" })).toThrow(TypeError);
        expect(() => writer.tableProperty("", "x")).toThrow(TypeError);
        expect(() => writer.tableProperty("k", 1 as never)).toThrow(/must be a string/);
        expect(() => writer.partitionedBy()).toThrow(/at least one column/);
        expect(() => writer.partitionedBy("")).toThrow(TypeError);
        expect(() => writer.partitionedBy(42 as never)).toThrow(/names or column expressions/);
        expect(() => writer.clusterBy()).toThrow(/at least one column/);
        expect(() => writer.clusterBy(" ")).toThrow(TypeError);
    });

    it("refuses streaming DataFrames", () => {
        const stream = session.readStream("rate");
        expect(() => (stream as any).writeTo("t")).toThrow(/streaming DataFrame/);
        expect(() => (stream as any).mergeInto("t", lit(true))).toThrow(/streaming DataFrame/);
    });
});

describe("MergeIntoWriter", () => {
    // MERGE is a Spark 4.0 command, checked against the server version.
    beforeEach(() => {
        vi.spyOn(SparkSession.prototype, "version").mockResolvedValue("4.0.4");
    });

    const condition = col("t.id").eq(col("s.id"));

    it("builds every clause kind in order", () => {
        const writer = source()
            .alias("s")
            .mergeInto("t", condition)
            .whenMatched(col("s.deleted").eq(true)).delete()
            .whenMatched().update({ name: col("s.name"), version: lit(2) })
            .whenMatched().updateAll()
            .whenNotMatched(col("s.ok").eq(true)).insert({ id: col("s.id") })
            .whenNotMatched().insertAll()
            .whenNotMatchedBySource(col("t.stale").eq(true)).delete()
            .whenNotMatchedBySource().update({ active: lit(false) });
        expect(writer).toBeInstanceOf(MergeIntoWriter);
        const command = writer.buildCommand().merge_into_table_command as ProtoObject;
        expect(command.target_table_name).toBe("t");
        expect(command.source_table_plan.subquery_alias.alias).toBe("s");
        expect(command.merge_condition.unresolved_function.function_name).toBe("=");
        expect(command.with_schema_evolution).toBe(false);
        expect(command.match_actions.map((a: ProtoObject) => a.merge_action.action_type)).toEqual([
            "ACTION_TYPE_DELETE",
            "ACTION_TYPE_UPDATE",
            "ACTION_TYPE_UPDATE_STAR",
        ]);
        expect(command.match_actions[0].merge_action.condition).toBeDefined();
        expect(command.match_actions[1].merge_action).not.toHaveProperty("condition");
        expect(command.match_actions[1].merge_action.assignments).toEqual([
            {
                key: { unresolved_attribute: { unparsed_identifier: "name" } },
                value: { unresolved_attribute: { unparsed_identifier: "s.name" } },
            },
            {
                key: { unresolved_attribute: { unparsed_identifier: "version" } },
                value: { literal: { integer: 2 } },
            },
        ]);
        expect(command.not_matched_actions.map((a: ProtoObject) => a.merge_action.action_type)).toEqual([
            "ACTION_TYPE_INSERT",
            "ACTION_TYPE_INSERT_STAR",
        ]);
        expect(command.not_matched_by_source_actions.map((a: ProtoObject) => a.merge_action.action_type)).toEqual([
            "ACTION_TYPE_DELETE",
            "ACTION_TYPE_UPDATE",
        ]);
    });

    it("enables schema evolution without mutating the original writer", () => {
        const base = source().mergeInto("t", condition).whenNotMatched().insertAll();
        const evolving = base.withSchemaEvolution();
        expect((base.buildCommand().merge_into_table_command as ProtoObject).with_schema_evolution).toBe(false);
        expect((evolving.buildCommand().merge_into_table_command as ProtoObject).with_schema_evolution).toBe(true);
    });

    it("executes the command after pending cache actions", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({});
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        await source().cache().mergeInto("t", condition).whenMatched().updateAll().merge();
        expect(analyze).toHaveBeenCalledOnce();
        expect(executedCommand(execute).merge_into_table_command.match_actions).toHaveLength(1);
    });

    it("requires at least one clause and valid expressions", async () => {
        await expect(source().mergeInto("t", condition).merge()).rejects.toThrow(/at least one/);
        expect(() => source().mergeInto("", condition)).toThrow(TypeError);
        expect(() => source().mergeInto("t", "t.id = s.id" as never)).toThrow(/merge condition/);
        const writer = source().mergeInto("t", condition);
        expect(() => writer.whenMatched("x" as never)).toThrow(/whenMatched\(\) condition/);
        expect(() => writer.whenNotMatched(1 as never)).toThrow(/whenNotMatched\(\) condition/);
        expect(() => writer.whenNotMatchedBySource({} as never)).toThrow(/whenNotMatchedBySource\(\) condition/);
        expect(() => writer.whenMatched().update({})).toThrow(/at least one column assignment/);
        expect(() => writer.whenNotMatched().insert({ " ": lit(1) })).toThrow(/non-empty/);
        expect(() => writer.whenNotMatchedBySource().update({ a: "x" as never })).toThrow(/wrap constants in lit/);
    });
});

describe("DataFrameWriterV2 partitioning", () => {
    it("replaces earlier partitionedBy and clusterBy lists like PySpark", () => {
        const command = source()
            .writeTo("catalog.target")
            .partitionedBy("a")
            .clusterBy("c")
            .partitionedBy("b")
            .clusterBy("d")
            .buildCommand("create") as { write_operation_v2: ProtoObject };
        const operation = command.write_operation_v2;
        expect(operation.partitioning_columns).toEqual([
            { unresolved_attribute: { unparsed_identifier: "b" } },
        ]);
        expect(operation.clustering_columns).toEqual(["d"]);
    });
});

describe("DataFrameWriterV2 schema evolution", () => {
    it("is gated on Spark 4.2 and sent only when requested", async () => {
        const gated = SparkSession.builder().getOrCreate();
        const reader = () => gated.read.parquet("/data/updates.parquet");
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.0.4" } });
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);

        await expect(reader().writeTo("t").withSchemaEvolution().append()).rejects.toMatchObject({
            name: "UnsupportedServerVersionError",
            feature: "withSchemaEvolution()",
            requiredVersion: "4.2.0",
            serverVersion: "4.0.4",
        });
        // 4.1 defines the field but does not act on it, so it is rejected too.
        const midway = SparkSession.builder().getOrCreate();
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.1.3" } });
        await expect(midway.read.parquet("/data/updates.parquet").writeTo("t").withSchemaEvolution().append())
            .rejects.toMatchObject({ name: "UnsupportedServerVersionError", requiredVersion: "4.2.0", serverVersion: "4.1.3" });
        expect(execute).not.toHaveBeenCalled();

        await reader().writeTo("t").append();
        expect(executedCommand(execute).write_operation_v2).not.toHaveProperty("with_schema_evolution");

        const modern = SparkSession.builder().getOrCreate();
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.2.0" } });
        const writer = modern.read.parquet("/data/updates.parquet").writeTo("t");
        const evolving = writer.withSchemaEvolution();
        await evolving.append();
        expect(executedCommand(execute, 1).write_operation_v2).toMatchObject({ with_schema_evolution: true });
        expect(writer.buildCommand("append").write_operation_v2).not.toHaveProperty("with_schema_evolution");
    });
});
