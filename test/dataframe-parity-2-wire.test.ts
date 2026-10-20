import path from "node:path";
import { loadSync, type MessageTypeDefinition } from "@grpc/proto-loader";
import { describe, expect, it } from "vitest";
import { SparkSession, col, lit, schema } from "../src";
import { ProtoDFAlg } from "../src/engine/compilerRead";
import { protoWriteRootToPlan, type ProtoWriteRoot } from "../src/write/compilerWrite";

type ProtoObject = Record<string, unknown>;

const protoRoot = path.resolve(__dirname, "../proto");
const packageDefinition = loadSync(
    [path.join(protoRoot, "spark/connect/base.proto")],
    {
        includeDirs: [protoRoot],
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    },
);

const executePlanRequest = packageDefinition[
    "spark.connect.ExecutePlanRequest"
] as MessageTypeDefinition<ProtoObject, ProtoObject>;
const analyzePlanRequest = packageDefinition[
    "spark.connect.AnalyzePlanRequest"
] as MessageTypeDefinition<ProtoObject, ProtoObject>;

/** Serializes with the vendored descriptor and decodes again: unknown fields vanish. */
function wire(plan: ProtoObject): any {
    const encoded = executePlanRequest.serialize({
        session_id: "00112233-4455-6677-8899-aabbccddeeff",
        user_context: { user_id: "parity-2-wire-conformance" },
        client_type: "ts-spark-connector-test",
        plan,
    });
    return executePlanRequest.deserialize(encoded).plan;
}

function rootOf(df: { toProtoJSON(): string }): any {
    return wire({ root: JSON.parse(df.toProtoJSON()) }).root;
}

const session = SparkSession.builder().getOrCreate();
const People = schema({ id: "long", name: "string?", age: "int?" });
const people = () => session.read.readWith(People, "parquet", "/data/people.parquet");
const lax = () => session.read.parquet("/data/lax.parquet");

describe("parity relations survive a protobuf round trip", () => {
    it("keeps grouping variants, pivot literals and grouping sets", () => {
        const rolled = rootOf(people().rollup("name").agg({ n: "count(id)" })).aggregate;
        expect(rolled.group_type).toBe("GROUP_TYPE_ROLLUP");
        expect(rootOf(people().cube("name").agg({ n: "count(id)" })).aggregate.group_type).toBe("GROUP_TYPE_CUBE");

        const pivoted = rootOf(people().groupBy("name").pivot("age", [30, "x", true, null]).agg({ n: "count(id)" })).aggregate;
        expect(pivoted.group_type).toBe("GROUP_TYPE_PIVOT");
        expect(pivoted.pivot.col.unresolved_attribute.unparsed_identifier).toBe("age");
        expect(pivoted.pivot.values.map((value: any) => value.literal_type)).toEqual(["integer", "string", "boolean", "null"]);
        expect(pivoted.pivot.values[0].integer).toBe(30);

        const sets = rootOf(people().groupingSets([["name"], []], "name").agg({ n: "count(id)" })).aggregate;
        expect(sets.group_type).toBe("GROUP_TYPE_GROUPING_SETS");
        expect(sets.grouping_sets).toHaveLength(2);
        expect(sets.grouping_sets[0].grouping_set[0].unresolved_attribute.unparsed_identifier).toBe("name");
        expect(sets.grouping_sets[1].grouping_set).toEqual([]);
    });

    it("keeps partition-local sorts and watermark deduplication", () => {
        const sorted = rootOf(people().sortWithinPartitions(col("age").desc(), "id")).sort;
        expect(sorted.is_global).toBe(false);
        expect(sorted.order.map((order: any) => order.direction)).toEqual(["SORT_DIRECTION_DESCENDING", "SORT_DIRECTION_ASCENDING"]);

        expect(rootOf(people().dropDuplicatesWithinWatermark()).deduplicate).toMatchObject({
            all_columns_as_keys: true,
            within_watermark: true,
        });
        expect(rootOf(people().dropDuplicatesWithinWatermark("id", "name")).deduplicate).toMatchObject({
            column_names: ["id", "name"],
            within_watermark: true,
        });
    });

    it("keeps regex columns and metadata columns", () => {
        const df = lax();
        const expressions = rootOf(df.select(df.colRegex("`(id|name)`"), df.metadataColumn("_metadata"))).project.expressions;
        expect(expressions[0].unresolved_regex.col_name).toBe("`(id|name)`");
        expect(expressions[1].unresolved_attribute).toMatchObject({ unparsed_identifier: "_metadata", is_metadata_column: true });
    });

    it("keeps lateral and as-of joins with every field", () => {
        const right = lax();
        const lateral = rootOf(people().lateralJoin(right, col("id").eq(col("k")), "left")).lateral_join;
        expect(lateral.join_type).toBe("JOIN_TYPE_LEFT_OUTER");
        expect(lateral.join_condition.unresolved_function.function_name).toBe("=");
        expect(rootOf(people().lateralJoin(right)).lateral_join.join_type).toBe("JOIN_TYPE_INNER");

        const asOf = rootOf(people().asOfJoin(right, "id", "rid", ["name"], "left", {
            tolerance: 2,
            allowExactMatches: false,
            direction: "forward",
        })).as_of_join;
        expect(asOf).toMatchObject({
            join_type: "left",
            using_columns: ["name"],
            allow_exact_matches: false,
            direction: "forward",
        });
        expect(asOf.left_as_of.unresolved_attribute.unparsed_identifier).toBe("id");
        expect(asOf.right_as_of.unresolved_attribute.unparsed_identifier).toBe("rid");
        expect(asOf.tolerance.literal.integer).toBe(2);

        const expr = rootOf(people().asOfJoin(right, col("id"), col("rid"), col("name").eq(col("n")))).as_of_join;
        expect(expr.join_expr.unresolved_function.function_name).toBe("=");
        expect(expr.using_columns).toEqual([]);
        expect(expr).toMatchObject({ join_type: "inner", allow_exact_matches: true, direction: "backward" });
    });

    it("keeps observed metrics, checkpoints, schema reconciliation and column metadata", () => {
        const metrics = rootOf(people().observe("stats", col("id").alias("ids"))).collect_metrics;
        expect(metrics.name).toBe("stats");
        expect(metrics.metrics[0].alias.name).toEqual(["ids"]);

        expect(wire({ root: ProtoDFAlg.cachedRemoteRelation("cp-1") }).root.cached_remote_relation.relation_id).toBe("cp-1");

        expect(rootOf(people().to("name STRING, id INT")).to_schema.schema.unparsed.data_type_string).toBe("name STRING, id INT");
        const declared = schema({ name: "string?", id: "int" });
        // Nullable on the wire: Spark refuses NOT NULL targets for nullable sources.
        expect(rootOf(people().to(declared)).to_schema.schema.unparsed.data_type_string).toBe("name STRING, id INT");

        const alias = rootOf(people().withMetadata("id", { comment: "pk" })).with_columns.aliases[0];
        expect(alias.name).toEqual(["id"]);
        expect(alias.metadata).toBe('{"comment":"pk"}');

        const json = rootOf(people().toJSON()).project.expressions[0].alias;
        expect(json.name).toEqual(["value"]);
        expect(json.expr.unresolved_function.function_name).toBe("to_json");
        expect(json.expr.unresolved_function.arguments[0].unresolved_function.arguments[0]).toHaveProperty("unresolved_star");
        expect(lit(1)).toBeDefined();
    });

    it("keeps checkpoint commands and the storage-level analysis", () => {
        const root = JSON.parse(people().toProtoJSON()) as ProtoObject;
        const checkpoint = wire({
            command: {
                checkpoint_command: {
                    relation: root,
                    local: true,
                    eager: false,
                    storage_level: { use_disk: true, use_memory: false, use_off_heap: false, deserialized: false, replication: 1 },
                },
            },
        }).command.checkpoint_command;
        expect(checkpoint).toMatchObject({ local: true, eager: false });
        expect(checkpoint.storage_level).toMatchObject({ use_disk: true, use_memory: false, replication: 1 });
        expect(checkpoint.relation.read.data_source.format).toBe("parquet");

        const remove = wire({
            command: { remove_cached_remote_relation_command: { relation: { relation_id: "cp-1" } } },
        }).command.remove_cached_remote_relation_command;
        expect(remove.relation.relation_id).toBe("cp-1");

        const encoded = analyzePlanRequest.serialize({
            session_id: "00112233-4455-6677-8899-aabbccddeeff",
            user_context: { user_id: "parity-2-wire-conformance" },
            get_storage_level: { relation: ProtoDFAlg.cachedRemoteRelation("cp-1") },
        });
        const analyze = analyzePlanRequest.deserialize(encoded) as any;
        expect(analyze.analyze).toBe("get_storage_level");
        expect(analyze.get_storage_level.relation.cached_remote_relation.relation_id).toBe("cp-1");
    });

    it("keeps global temp view commands", () => {
        const child = JSON.parse(people().toProtoJSON()) as ProtoObject;
        const plan = (registerView: { name: string; replace: boolean; global?: boolean }) => protoWriteRootToPlan({
            child,
            writerKind: "batch",
            spec: { options: {}, partitionBy: [], sortBy: [], registerView },
        } as ProtoWriteRoot) as unknown as ProtoObject;
        expect(wire(plan({ name: "gv", replace: false, global: true })).command.create_dataframe_view)
            .toMatchObject({ name: "gv", is_global: true, replace: false });
        expect(wire(plan({ name: "gv", replace: true, global: true })).command.create_dataframe_view)
            .toMatchObject({ name: "gv", is_global: true, replace: true });
        expect(wire(plan({ name: "tv", replace: false })).command.create_dataframe_view)
            .toMatchObject({ name: "tv", is_global: false, replace: false });
    });
});
