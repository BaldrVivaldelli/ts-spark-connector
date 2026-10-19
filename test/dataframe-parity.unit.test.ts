import * as arrow from "apache-arrow";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession, col, lit, schema } from "../src";
import type { LiteralValue } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { JoinType } from "../src/engine/sparkConnectEnums";
import { DataFrameNaFunctions, DataFrameStatFunctions } from "../src/read/dataFrameFunctions";
import { ReadChainedDataFrame } from "../src/read/readChainedDataFrame";

const session = SparkSession.builder().getOrCreate();
const People = schema({ id: "long", name: "string?", age: "int?", score: "double?" });
const people = () => session.read.readWith(People, "parquet", "/data/people.parquet");
const lax = () => session.read.parquet("/data/lax.parquet");

type ProtoObject = Record<string, any>;

function proto(df: { toProtoJSON(): string }): ProtoObject {
    return JSON.parse(df.toProtoJSON()) as ProtoObject;
}

function arrowResponse(columns: Record<string, ArrayLike<unknown>>): Record<string, unknown> {
    const table = arrow.tableFromArrays(columns as Record<string, any>) as arrow.Table;
    return { arrow_batch: { data: Buffer.from(arrow.tableToIPC(table, "stream")) } };
}

function arrowTableResponse(table: arrow.Table): Record<string, unknown> {
    return { arrow_batch: { data: Buffer.from(arrow.tableToIPC(table, "stream")) } };
}

/** Captures the ExecutePlan requests and answers each with the given responses. */
function mockExecute(...responses: Array<Record<string, unknown>>) {
    return vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue(responses as never);
}

function mockAnalyze(response: Record<string, unknown>) {
    return vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue(response as never);
}

function executedRoot(spy: ReturnType<typeof mockExecute>, call = 0): ProtoObject {
    return (spy.mock.calls[call]![0] as ProtoObject).plan.root;
}

afterEach(() => vi.restoreAllMocks());

describe("df.na", () => {
    it("fills one value everywhere, a subset, or a per-column mapping", () => {
        expect(proto(people().fillna(0)).fill_na).toMatchObject({ cols: [], values: [{ long: "0" }] });
        expect(proto(people().na.fill(1.5, "score")).fill_na).toMatchObject({
            cols: ["score"],
            values: [{ double: 1.5 }],
        });
        expect(proto(people().fillna("n/a", ["name"])).fill_na).toMatchObject({
            cols: ["name"],
            values: [{ string: "n/a" }],
        });
        expect(proto(lax().fillna({ age: 0, name: "?", ok: true, big: 7n })).fill_na).toMatchObject({
            cols: ["age", "name", "ok", "big"],
            values: [{ long: "0" }, { string: "?" }, { boolean: true }, { long: "7" }],
        });
    });

    it("rejects fill values Spark cannot use", () => {
        expect(() => people().fillna(Number.NaN)).toThrow(RangeError);
        expect(() => people().fillna(null as never)).toThrow(/must be a string, number, boolean or bigint/);
        expect(() => people().fillna({})).toThrow(/at least one column/);
        expect(() => lax().fillna({ a: 1 }, "a")).toThrow(/no subset/);
        expect(() => lax().fillna({ "": 1 })).toThrow(TypeError);
        expect(() => lax().fillna({ a: Infinity })).toThrow(RangeError);
        expect(() => people().fillna(0, [""] as never)).toThrow(TypeError);
    });

    it("drops rows by how, threshold and subset", () => {
        expect(proto(people().dropna()).drop_na).toEqual({ input: expect.any(Object), cols: [] });
        expect(proto(people().dropna("any")).drop_na.min_non_nulls).toBeUndefined();
        expect(proto(people().na.drop("all")).drop_na.min_non_nulls).toBe(1);
        expect(proto(people().dropna("all", 2, ["name", "age"])).drop_na).toMatchObject({
            cols: ["name", "age"],
            min_non_nulls: 2,
        });
        expect(proto(people().dropna(undefined, 0)).drop_na.min_non_nulls).toBe(0);
        expect(() => people().dropna("some" as never)).toThrow(RangeError);
        expect(() => people().dropna("any", -1)).toThrow(RangeError);
        expect(() => people().dropna("any", 1.5)).toThrow(RangeError);
    });

    it("replaces scalars, lists and mappings with doubles for numbers", () => {
        expect(proto(people().replace(1, 2)).replace).toMatchObject({
            cols: [],
            replacements: [{ old_value: { double: 1 }, new_value: { double: 2 } }],
        });
        expect(proto(people().na.replace(["a", "b"], "z", "name")).replace).toMatchObject({
            cols: ["name"],
            replacements: [
                { old_value: { string: "a" }, new_value: { string: "z" } },
                { old_value: { string: "b" }, new_value: { string: "z" } },
            ],
        });
        expect(proto(people().replace([1n, 2], [10, null])).replace.replacements).toEqual([
            { old_value: { double: 1 }, new_value: { double: 10 } },
            { old_value: { double: 2 }, new_value: { null: { null: {} } } },
        ]);
        expect(proto(lax().replace({ yes: "y", no: null }, ["answer"])).replace).toMatchObject({
            cols: ["answer"],
            replacements: [
                { old_value: { string: "yes" }, new_value: { string: "y" } },
                { old_value: { string: "no" }, new_value: { null: { null: {} } } },
            ],
        });
        expect(proto(lax().na.replace(new Map([[true, false]]))).replace.replacements).toEqual([
            { old_value: { boolean: true }, new_value: { boolean: false } },
        ]);
    });

    it("rejects mixed kinds, uneven lists and unsupported values", () => {
        expect(() => people().replace(1, "one")).toThrow(/cannot mix numeric and string/);
        expect(() => people().replace([1, 2], [3])).toThrow(RangeError);
        expect(() => (people().replace as (...a: unknown[]) => unknown)(1)).toThrow(/replacement value or a mapping/);
        expect(() => people().replace(Number.POSITIVE_INFINITY, 1)).toThrow(/finite/);
        expect(() => people().replace({} as never, 1 as never)).toThrow(TypeError);
        expect(() => people().replace(Symbol("x") as never, 1)).toThrow(/must be strings, numbers/);
    });
});

describe("df.stat relations", () => {
    it("builds crosstab and freqItems as untyped relations", () => {
        const crosstab = people().stat.crosstab("name", "age");
        expect(proto(crosstab).crosstab).toMatchObject({ col1: "name", col2: "age" });
        expect(proto(people().freqItems(["name", "age"])).freq_items).toEqual({
            input: expect.any(Object),
            cols: ["name", "age"],
        });
        expect(proto(people().stat.freqItems(["name"], 0.25)).freq_items.support).toBe(0.25);
        expect(() => people().freqItems([])).toThrow(/at least one column/);
        expect(() => people().freqItems(["name"], 0)).toThrow(RangeError);
        expect(() => people().freqItems(["name"], 1.5)).toThrow(RangeError);
        expect(() => people().crosstab("" as never, "age")).toThrow(TypeError);
        expect(() => people().crosstab("name", "" as never)).toThrow(TypeError);
    });

    it("samples strata from records and maps with a fixed seed", () => {
        const fromRecord = proto(people().sampleBy("name", { a: 0.5, b: 1 }, 7)).sample_by;
        expect(fromRecord).toMatchObject({
            col: { unresolved_attribute: { unparsed_identifier: "name" } },
            fractions: [
                { stratum: { string: "a" }, fraction: 0.5 },
                { stratum: { string: "b" }, fraction: 1 },
            ],
            seed: 7,
        });
        const fromMap = people().stat.sampleBy(col("age"), new Map<LiteralValue, number>([[1, 0.1], [2n, 0.2]]));
        const sampled = proto(fromMap).sample_by;
        expect(sampled.fractions).toEqual([
            { stratum: { integer: 1 }, fraction: 0.1 },
            { stratum: { long: "2" }, fraction: 0.2 },
        ]);
        expect(Number.isSafeInteger(sampled.seed)).toBe(true);
        expect(proto(fromMap).sample_by.seed).toBe(sampled.seed);
        expect(() => people().sampleBy("name", {})).toThrow(/at least one stratum/);
        expect(() => people().sampleBy("name", { a: 2 })).toThrow(RangeError);
        expect(() => people().sampleBy("name", { a: 0.5 }, 0.5)).toThrow(RangeError);
        expect(() => people().sampleBy("" as never, { a: 0.5 })).toThrow(TypeError);
    });

    it("executes corr and cov as scalar doubles", async () => {
        const execute = mockExecute(arrowResponse({ corr: Float64Array.from([0.75]) }));
        await expect(people().corr("age", "score")).resolves.toBe(0.75);
        expect(executedRoot(execute).corr).toMatchObject({ col1: "age", col2: "score", method: "pearson" });

        execute.mockResolvedValue([arrowResponse({ cov: Float64Array.from([2.5]) })] as never);
        await expect(people().stat.cov("age", "score")).resolves.toBe(2.5);
        expect(executedRoot(execute, 1).cov).toMatchObject({ col1: "age", col2: "score" });

        execute.mockResolvedValue([arrowResponse({ corr: Float64Array.from([0.1]) })] as never);
        await expect(people().stat.corr("age", "score", "pearson")).resolves.toBe(0.1);
    });

    it("rejects unsupported correlation methods and malformed scalar results", async () => {
        await expect(people().corr("age", "score", "spearman" as never)).rejects.toThrow(/pearson/);
        await expect(people().corr("" as never, "score")).rejects.toThrow(TypeError);
        await expect(people().corr("age", "" as never)).rejects.toThrow(TypeError);
        await expect(people().cov("" as never, "score")).rejects.toThrow(TypeError);
        await expect(people().cov("age", "" as never)).rejects.toThrow(TypeError);

        mockExecute();
        await expect(people().cov("age", "score")).rejects.toThrow(/expected one result row/);
        vi.restoreAllMocks();
        mockExecute(arrowResponse({ cov: ["x"] }));
        await expect(people().cov("age", "score")).rejects.toThrow(/expected a double result/);
    });

    it("executes approxQuantile for one or several columns", async () => {
        const quantiles = arrow.vectorFromArray(
            [[[1, 2], [3, 4]]],
            new arrow.List(new arrow.Field("element", new arrow.List(new arrow.Field("element", new arrow.Float64(), true)), true)),
        );
        const execute = mockExecute(arrowTableResponse(new arrow.Table({ approx_quantile: quantiles })));
        await expect(people().approxQuantile(["age", "score"], [0.5, 0.9], 0.01)).resolves.toEqual([[1, 2], [3, 4]]);
        expect(executedRoot(execute).approx_quantile).toMatchObject({
            cols: ["age", "score"],
            probabilities: [0.5, 0.9],
            relative_error: 0.01,
        });
        await expect(people().stat.approxQuantile("age", [0.5, 0.9], 0)).resolves.toEqual([1, 2]);
        await expect(people().stat.approxQuantile(["age"], [0.5], 0)).resolves.toEqual([[1, 2], [3, 4]]);
    });

    it("validates approxQuantile arguments and result shape", async () => {
        await expect(people().approxQuantile([], [0.5], 0)).rejects.toThrow(/at least one column/);
        await expect(people().approxQuantile("age", [], 0)).rejects.toThrow(/at least one probability/);
        await expect(people().approxQuantile("age", [1.5], 0)).rejects.toThrow(RangeError);
        await expect(people().approxQuantile("age", [0.5], -1)).rejects.toThrow(RangeError);
        mockExecute(arrowResponse({ approx_quantile: [1] }));
        await expect(people().approxQuantile("age", [0.5], 0)).rejects.toThrow(/list of quantile lists/);
    });
});

describe("reshaping relations", () => {
    it("unpivots with explicit or implicit value columns", () => {
        expect(proto(people().unpivot("id", ["age", col("score")], "attr", "val")).unpivot).toMatchObject({
            ids: [{ unresolved_attribute: { unparsed_identifier: "id" } }],
            values: {
                values: [
                    { unresolved_attribute: { unparsed_identifier: "age" } },
                    { unresolved_attribute: { unparsed_identifier: "score" } },
                ],
            },
            variable_column_name: "attr",
            value_column_name: "val",
        });
        const melted = proto(people().melt([col("id")], undefined, "attr", "val")).unpivot;
        expect(melted.values).toBeUndefined();
        expect(proto(people().unpivot("id", "age", "k", "v")).unpivot.values.values).toHaveLength(1);
        expect(() => people().unpivot("id", [], "k", "v")).toThrow(/omit them/);
        expect(() => people().unpivot("id", "age", "", "v")).toThrow(TypeError);
        expect(() => people().unpivot("id", "age", "k", " ")).toThrow(TypeError);
        expect(() => people().unpivot("" as never, "age", "k", "v")).toThrow(TypeError);
    });

    it("renames positionally with toDF and qualifies with alias", () => {
        expect(proto(people().toDF("a", "b", "c", "d")).to_df.column_names).toEqual(["a", "b", "c", "d"]);
        expect(() => people().toDF("a", "a")).toThrow(/duplicate/);
        expect(() => people().toDF("")).toThrow(TypeError);
        expect(proto(people().alias("p")).subquery_alias).toMatchObject({ alias: "p" });
        expect(() => people().alias(" ")).toThrow(TypeError);
    });

    it("self-joins through aliases with qualified column references", () => {
        const left = lax().alias("a");
        const right = lax().alias("b");
        const joined = proto(left.join(right, col("a.id").eq(col("b.parent_id"))));
        expect(joined.join.left.subquery_alias.alias).toBe("a");
        expect(joined.join.right.subquery_alias.alias).toBe("b");
        expect(joined.join.join_condition.unresolved_function.arguments[0].unresolved_attribute.unparsed_identifier)
            .toBe("a.id");
    });

    it("offsets, transposes and adds several columns at once", () => {
        expect(proto(people().offset(5)).offset).toMatchObject({ offset: 5 });
        expect(() => people().offset(-1)).toThrow(RangeError);
        expect(proto(people().transpose()).transpose.index_columns).toEqual([]);
        expect(proto(people().transpose("id")).transpose.index_columns).toHaveLength(1);
        expect(proto(people().transpose(col("id"))).transpose.index_columns).toHaveLength(1);
        expect(() => people().transpose("" as never)).toThrow(TypeError);

        expect(proto(lax().withColumns({ one: lit(1), two: col("x") })).with_columns.aliases).toEqual([
            { expr: { literal: { integer: 1 } }, name: ["one"] },
            { expr: { unresolved_attribute: { unparsed_identifier: "x" } }, name: ["two"] },
        ]);
        expect(() => lax().withColumns({})).toThrow(/at least one column/);
        expect(() => lax().withColumns({ " ": lit(1) })).toThrow(TypeError);
    });

    it("adds typed columns and validates the factory result lazily", () => {
        const typed = people().withColumns(c => ({ older: c.age.plus(1), label: c.name }));
        expect(proto(typed).with_columns.aliases.map((alias: ProtoObject) => alias.name[0])).toEqual(["older", "label"]);
        const empty = people().withColumns(() => ({}));
        expect(() => empty.toProtoJSON()).toThrow(/at least one column/);
        const blank = people().withColumns(c => ({ "": c.age }));
        expect(() => blank.toProtoJSON()).toThrow(TypeError);
    });

    it("cross-joins and keeps the streaming flavour of either side", () => {
        const crossed = people().crossJoin(lax());
        expect(proto(crossed).join).toMatchObject({ join_type: JoinType.JOIN_TYPE_CROSS });
        expect(proto(crossed).join.join_condition).toBeUndefined();
        const streaming = lax().crossJoin(session.readStream("rate"));
        expect(streaming.isStreamingDataFrame()).toBe(true);
    });
});

describe("partitioning by expression", () => {
    it("hash-partitions by columns with or without a count", () => {
        expect(proto(people().repartition("id", col("age"))).repartition_by_expression).toEqual({
            input: expect.any(Object),
            partition_exprs: [
                { unresolved_attribute: { unparsed_identifier: "id" } },
                { unresolved_attribute: { unparsed_identifier: "age" } },
            ],
        });
        expect(proto(people().repartition(8, "id")).repartition_by_expression.num_partitions).toBe(8);
    });

    it("keeps the round-robin and coalescing forms", () => {
        expect(proto(people().repartition(4)).repartition).toMatchObject({ num_partitions: 4, shuffle: true });
        expect(proto(people().repartition(4, false)).repartition).toMatchObject({ num_partitions: 4, shuffle: false });
        expect(proto(people().repartition(4, undefined)).repartition.shuffle).toBe(true);
    });

    it("rejects malformed repartition calls", () => {
        const call = (...args: unknown[]) => (people().repartition as (...a: unknown[]) => unknown)(...args);
        expect(() => call()).toThrow(/partition count or columns/);
        expect(() => call(4, true, "id")).toThrow(/takes no columns/);
        expect(() => call(0, "id")).toThrow(RangeError);
        expect(() => call("id", true)).toThrow(/names or column expressions/);
        expect(() => call("id", col("x").asc())).toThrow(/names or column expressions/);
        expect(() => call("id", 4)).toThrow(/names or column expressions/);
        expect(() => call("")).toThrow(TypeError);
        expect(() => call({})).toThrow(/names, column expressions or sort keys/);
    });

    it("range-partitions with ascending defaults and explicit sort keys", () => {
        const ranged = proto(people().repartitionByRange(3, "id", col("age").desc("nullsFirst")))
            .repartition_by_expression;
        expect(ranged.num_partitions).toBe(3);
        expect(ranged.partition_exprs).toEqual([
            {
                sort_order: {
                    child: { unresolved_attribute: { unparsed_identifier: "id" } },
                    direction: "SORT_DIRECTION_ASCENDING",
                    null_ordering: "SORT_NULLS_FIRST",
                },
            },
            {
                sort_order: {
                    child: { unresolved_attribute: { unparsed_identifier: "age" } },
                    direction: "SORT_DIRECTION_DESCENDING",
                    null_ordering: "SORT_NULLS_FIRST",
                },
            },
        ]);
        expect(proto(people().repartitionByRange(col("score"))).repartition_by_expression.num_partitions)
            .toBeUndefined();
        expect(() => people().repartitionByRange()).toThrow(/at least one column/);
        expect(() => people().repartitionByRange(0, "id")).toThrow(RangeError);
    });
});

describe("row actions", () => {
    it("counts with a global count(1) aggregate", async () => {
        const execute = mockExecute(arrowResponse({ count: BigInt64Array.from([42n]) }));
        await expect(people().count()).resolves.toBe(42n);
        expect(executedRoot(execute).aggregate).toMatchObject({
            grouping_expressions: [],
            aggregate_expressions: [{ alias: { name: ["count"] } }],
        });
    });

    it("rejects a count that is not a long", async () => {
        mockExecute(arrowResponse({ count: Int32Array.from([1]) }));
        await expect(people().count()).rejects.toThrow(/expected a long result/);
    });

    it("takes, heads and checks emptiness through limit", async () => {
        const row = { id: BigInt64Array.from([1n]), name: ["ada"], age: Int32Array.from([36]), score: Float64Array.from([1.5]) };
        const execute = mockExecute(arrowResponse(row));
        await expect(people().first()).resolves.toEqual({ id: 1n, name: "ada", age: 36, score: 1.5 });
        expect(executedRoot(execute).limit.limit).toBe(1);
        await expect(people().head()).resolves.toMatchObject({ id: 1n });
        await expect(people().head(3)).resolves.toHaveLength(1);
        expect(executedRoot(execute, 2).limit.limit).toBe(3);
        await expect(people().take(1)).resolves.toHaveLength(1);
        await expect(people().isEmpty()).resolves.toBe(false);

        execute.mockResolvedValue([] as never);
        await expect(people().isEmpty()).resolves.toBe(true);
        await expect(people().first()).resolves.toBeUndefined();
    });

    it("tails through the Tail relation", async () => {
        const execute = mockExecute(arrowResponse({ id: BigInt64Array.from([9n]), name: ["z"], age: Int32Array.from([1]), score: Float64Array.from([0]) }));
        await expect(people().tail(1)).resolves.toEqual([{ id: 9n, name: "z", age: 1, score: 0 }]);
        expect(executedRoot(execute).tail).toMatchObject({ limit: 1 });
        await expect(people().tail(-1)).rejects.toThrow(RangeError);
    });
});

describe("AnalyzePlan introspection", () => {
    const structSchema = {
        schema: {
            schema: {
                kind: "struct",
                struct: {
                    fields: [
                        { name: "id", data_type: { kind: "long", long: {} }, nullable: false },
                        { name: "tags", data_type: { kind: "array", array: { element_type: { kind: "string", string: { collation: "" } } } }, nullable: true },
                    ],
                },
            },
        },
    };

    it("reads the schema, columns and dtypes", async () => {
        const analyze = mockAnalyze(structSchema);
        await expect(people().schema()).resolves.toEqual({
            fields: [
                { name: "id", dataType: "bigint", nullable: false },
                { name: "tags", dataType: "array<string>", nullable: true },
            ],
        });
        expect(analyze.mock.calls[0]![0]).toMatchObject({
            client_type: "ts-spark-connector",
            schema: { plan: { root: expect.objectContaining({ read: expect.any(Object) }) } },
        });
        await expect(people().columns()).resolves.toEqual(["id", "tags"]);
        await expect(people().dtypes()).resolves.toEqual([["id", "bigint"], ["tags", "array<string>"]]);
    });

    it("prints and returns the tree string", async () => {
        const analyze = mockAnalyze({ tree_string: { tree_string: "root\n |-- id: long" } });
        await expect(people().treeString()).resolves.toBe("root\n |-- id: long");
        expect(analyze.mock.calls[0]![0]).toMatchObject({ tree_string: { plan: expect.any(Object) } });
        expect((analyze.mock.calls[0]![0] as ProtoObject).tree_string.level).toBeUndefined();
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
        await people().printSchema(2);
        expect(log).toHaveBeenCalledWith("root\n |-- id: long");
        expect((analyze.mock.calls[1]![0] as ProtoObject).tree_string.level).toBe(2);
        await expect(people().treeString(0)).rejects.toThrow(RangeError);
    });

    it("answers the boolean, list and hash introspection requests", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockImplementation(async request => {
            if ("is_local" in request) return { is_local: { is_local: true } };
            if ("is_streaming" in request) return { is_streaming: { is_streaming: false } };
            if ("input_files" in request) return { input_files: { files: ["/data/a.parquet"] } };
            if ("same_semantics" in request) return { same_semantics: { result: true } };
            return { semantic_hash: { result: 123 } };
        });
        await expect(people().isLocal()).resolves.toBe(true);
        await expect(people().isStreaming()).resolves.toBe(false);
        await expect(people().inputFiles()).resolves.toEqual(["/data/a.parquet"]);
        await expect(people().sameSemantics(people().limit(1))).resolves.toBe(true);
        const sameRequest = analyze.mock.calls[3]![0] as ProtoObject;
        expect(sameRequest.same_semantics.target_plan.root.read).toBeDefined();
        expect(sameRequest.same_semantics.other_plan.root.limit).toBeDefined();
        await expect(people().semanticHash()).resolves.toBe(123);
    });

    it("treats a missing file list as empty and rejects a missing result", async () => {
        mockAnalyze({ input_files: {} });
        await expect(people().inputFiles()).resolves.toEqual([]);
        vi.restoreAllMocks();
        mockAnalyze({ explain: {} });
        await expect(people().isLocal()).rejects.toThrow(/missing is_local result/);
    });

    it("applies pending persist actions before analysing", async () => {
        const analyze = vi.spyOn(sparkGrpcClient, "analyze").mockImplementation(async request =>
            "persist" in request ? {} : { is_local: { is_local: false } }
        );
        await expect(people().cache().isLocal()).resolves.toBe(false);
        expect(analyze.mock.calls.map(([request]) => Object.keys(request).find(key => key === "persist" || key === "is_local")))
            .toEqual(["persist", "is_local"]);
    });
});

describe("na / stat accessors", () => {
    it("are bound to the DataFrame they came from", () => {
        expect(people().na).toBeInstanceOf(DataFrameNaFunctions);
        expect(people().stat).toBeInstanceOf(DataFrameStatFunctions);
        expect(people().na.fill(0)).toBeInstanceOf(ReadChainedDataFrame);
    });
});

describe("plan inspection of the new relations", () => {
    it("serializes every relation through the logical-plan and trace printers", () => {
        const df = lax()
            .fillna(0, "a")
            .dropna("all", undefined, "a")
            .replace(1, 2, "a")
            .sampleBy("k", { x: 0.5 }, 3)
            .alias("t")
            .offset(1)
            .repartition(2, "a")
            .repartitionByRange("a")
            .crossJoin(lax())
            .unpivot("a", ["b"], "k", "v")
            .toDF("a", "k", "v")
            .withColumns({ w: lit(1) })
            .transpose("a");
        const logical = df.toSparkLogicalPlanJSON();
        for (const type of ["FillNa", "DropNa", "ReplaceNa", "SampleBy", "SubqueryAlias", "Offset", "RepartitionByExpression", "CrossJoin", "Unpivot", "ToDF", "Transpose"]) {
            expect(logical).toContain(`"type": "${type}"`);
        }
        const trace = df.toClientASTMermaid();
        for (const label of ["fillNa[1](a)", "dropNa(min=1)(a)", "replaceNa[1](a)", "sampleBy[1](seed=3)", "alias(t)", "offset(1)", "repartition[1](2)", "repartitionByRange[1]", "join(CROSS)", "unpivot(k,v;ids[1];values[1])", "toDF(a,k,v)", "withColumns(w)", "transpose[1]"]) {
            expect(trace).toContain(label);
        }
        const stats = lax().crosstab("a", "b").toClientASTJSON() + lax().freqItems(["a"], 0.5).toClientASTJSON();
        expect(stats).toContain("crosstab(a,b)");
        expect(stats).toContain("freqItems(a;support=0.5)");
    });
});
