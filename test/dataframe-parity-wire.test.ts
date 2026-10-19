import path from "node:path";
import { loadSync, type MessageTypeDefinition } from "@grpc/proto-loader";
import { describe, expect, it } from "vitest";
import { SparkSession, call, col, lit, schema } from "../src";
import { ProtoDFAlg } from "../src/engine/compilerRead";

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

function wire(plan: ProtoObject): any {
    const encoded = executePlanRequest.serialize({
        session_id: "00112233-4455-6677-8899-aabbccddeeff",
        user_context: { user_id: "parity-wire-conformance" },
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

describe("Spark Connect wire conformance for the DataFrame parity relations", () => {
    it("encodes na.fill values with the literal kinds Spark accepts", () => {
        const root = rootOf(people().fillna({ age: 0, name: "?", score: 1.5, ok: false }));
        expect(root.rel_type).toBe("fill_na");
        expect(root.fill_na.cols).toEqual(["age", "name", "score", "ok"]);
        expect(root.fill_na.values.map((value: any) => value.literal_type)).toEqual([
            "long",
            "string",
            "double",
            "boolean",
        ]);
        expect(root.fill_na.values[0].long).toBe("0");
    });

    it("encodes na.drop with an explicit threshold and na.replace as doubles", () => {
        const dropped = rootOf(people().dropna("all", undefined, ["name"]));
        expect(dropped.drop_na).toMatchObject({ cols: ["name"], min_non_nulls: 1 });
        const replaced = rootOf(people().replace([1, 2n], [null, 3], "age"));
        expect(replaced.replace.replacements).toEqual([
            {
                old_value: expect.objectContaining({ literal_type: "double", double: 1 }),
                new_value: expect.objectContaining({ literal_type: "null" }),
            },
            {
                old_value: expect.objectContaining({ literal_type: "double", double: 2 }),
                new_value: expect.objectContaining({ literal_type: "double", double: 3 }),
            },
        ]);
    });

    it("encodes the statistic relations", () => {
        expect(rootOf(people().crosstab("name", "age")).crosstab).toMatchObject({ col1: "name", col2: "age" });
        expect(rootOf(people().freqItems(["name"], 0.3)).freq_items).toMatchObject({ cols: ["name"], support: 0.3 });
        const sampled = rootOf(people().sampleBy("age", new Map([[30, 0.5]]), 11)).sample_by;
        expect(sampled.seed).toBe("11");
        expect(sampled.fractions[0]).toMatchObject({ fraction: 0.5, stratum: { literal_type: "integer", integer: 30 } });
    });

    it("encodes unpivot, toDF, alias, offset, tail and transpose", () => {
        const unpivoted = rootOf(people().unpivot("id", ["age"], "attr", "val")).unpivot;
        expect(unpivoted.ids[0].unresolved_attribute.unparsed_identifier).toBe("id");
        expect(unpivoted.values.values).toHaveLength(1);
        expect(unpivoted).toMatchObject({ variable_column_name: "attr", value_column_name: "val" });
        expect(rootOf(people().melt("id", undefined, "k", "v")).unpivot.values).toBeUndefined();
        expect(rootOf(people().toDF("a", "b", "c")).to_df.column_names).toEqual(["a", "b", "c"]);
        expect(rootOf(people().alias("p")).subquery_alias.alias).toBe("p");
        expect(rootOf(people().offset(3)).offset.offset).toBe(3);
        expect(rootOf(people().transpose("id")).transpose.index_columns).toHaveLength(1);
        const tail = ProtoDFAlg.tail(ProtoDFAlg.relation("parquet", "/data/people.parquet"), 2);
        expect(wire({ root: tail }).root.tail.limit).toBe(2);
    });

    it("encodes withColumns, crossJoin and both repartition forms", () => {
        const withColumns = rootOf(people().withColumns({ one: lit(1), shout: call("upper", [col("name")]) }));
        expect(withColumns.with_columns.aliases.map((alias: any) => alias.name)).toEqual([["one"], ["shout"]]);
        const crossed = rootOf(people().crossJoin(people()));
        expect(crossed.join.join_type).toBe("JOIN_TYPE_CROSS");
        expect(crossed.join.join_condition).toBeNull();
        const hashed = rootOf(people().repartition(4, "id")).repartition_by_expression;
        expect(hashed.num_partitions).toBe(4);
        expect(hashed.partition_exprs[0].expr_type).toBe("unresolved_attribute");
        const ranged = rootOf(people().repartitionByRange("id", col("age").desc())).repartition_by_expression;
        expect(ranged.partition_exprs.map((expr: any) => expr.sort_order.direction)).toEqual([
            "SORT_DIRECTION_ASCENDING",
            "SORT_DIRECTION_DESCENDING",
        ]);
    });

    it("encodes WriteOperationV2 commands", () => {
        const command = people()
            .writeTo("db.people")
            .using("delta")
            .options({ a: "1" })
            .tableProperty("p", "v")
            .partitionedBy("age")
            .clusterBy("name")
            .buildCommand("overwrite", col("age").gt(30));
        const decoded = wire({ command }).command;
        expect(decoded.command_type).toBe("write_operation_v2");
        expect(decoded.write_operation_v2).toMatchObject({
            table_name: "db.people",
            provider: "delta",
            options: { a: "1" },
            table_properties: { p: "v" },
            mode: "MODE_OVERWRITE",
            clustering_columns: ["name"],
        });
        expect(decoded.write_operation_v2.overwrite_condition.unresolved_function.function_name).toBe(">");
        expect(decoded.write_operation_v2.partitioning_columns).toHaveLength(1);
    });

    it("encodes MergeIntoTable commands and their actions", () => {
        const command = people()
            .alias("s")
            .mergeInto("t", col("t.id").eq(col("s.id")))
            .whenMatched(col("s.age").gt(1)).update({ name: col("s.name") })
            .whenNotMatched().insertAll()
            .whenNotMatchedBySource().delete()
            .withSchemaEvolution()
            .buildCommand();
        const decoded = wire({ command }).command.merge_into_table_command;
        expect(decoded.target_table_name).toBe("t");
        expect(decoded.with_schema_evolution).toBe(true);
        expect(decoded.match_actions[0].merge_action).toMatchObject({
            action_type: "ACTION_TYPE_UPDATE",
            assignments: [{
                key: { unresolved_attribute: { unparsed_identifier: "name" } },
                value: { unresolved_attribute: { unparsed_identifier: "s.name" } },
            }],
        });
        expect(decoded.match_actions[0].merge_action.condition.unresolved_function.function_name).toBe(">");
        expect(decoded.not_matched_actions[0].merge_action.action_type).toBe("ACTION_TYPE_INSERT_STAR");
        expect(decoded.not_matched_by_source_actions[0].merge_action.action_type).toBe("ACTION_TYPE_DELETE");
    });
});
