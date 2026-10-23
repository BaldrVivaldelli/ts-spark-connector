import { afterEach, describe, expect, it, vi } from "vitest";
import { Observation, SparkSession, UnsupportedServerVersionError, col, lit } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { asSparkConnectError } from "../src/client/errors";
import { inspectPlan, preparePlanForServer } from "../src/client/serverCompatibility";
import { observedMetricsOf } from "../src/read/observation";
import { functions as F } from "../src";
import { ProtoExprAlg } from "../src/engine/compilerRead";

type ProtoObject = Record<string, any>;

function session(version: string): SparkSession {
    vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
    vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version } });
    return SparkSession.builder().getOrCreate();
}

function sentPlan(spy: { mock: { calls: unknown[][] } }, index = 0): ProtoObject {
    return (spy.mock.calls[index]![0] as ProtoObject).plan;
}

describe("plan inspection", () => {
    it("names each feature newer than Spark 3.5 once, ignoring unset fields", () => {
        const inspection = inspectPlan({
            project: {
                input: { lateral_join: { left: {}, right: { transpose: { input: {} } } } },
                expressions: [
                    { cast: { eval_mode: "EVAL_MODE_TRY" } },
                    { cast: { eval_mode: "EVAL_MODE_TRY" } },
                ],
            },
            write_operation_v2: { clustering_columns: [] },
            aggregate: { grouping_sets: [] },
        });
        expect(inspection.requirements.map(requirement => requirement.feature).sort())
            .toEqual(["lateralJoin()", "transpose()", "tryCast()"]);
        expect(inspection.requirements.every(requirement => requirement.minimum === "4.0.0")).toBe(true);
        expect(inspectPlan({ cast: { eval_mode: 0 } }).requirements).toEqual([]);
        expect(inspectPlan({ checkpoint_command: null }).requirements).toEqual([]);
        expect(inspectPlan({ aggregate: { grouping_sets: [{ grouping_set: [] }] } }).requirements)
            .toEqual([{ minimum: "4.0.0", feature: "groupingSets()" }]);
        expect(inspectPlan({ write_operation_v2: { clustering_columns: ["day"] } }).requirements)
            .toEqual([{ minimum: "4.0.0", feature: "clusterBy()" }]);
    });

    it("does not walk into binary payloads", () => {
        const data = Object.assign(Buffer.from("payload"), { transpose: {} });
        const typed = Object.assign(new Uint8Array(2), { as_of_join: {} });
        expect(inspectPlan({ local_relation: { data, typed } }).requirements).toEqual([]);
        expect(inspectPlan(Object.assign(Object.create(null), { merge_into_table_command: {} })).requirements)
            .toEqual([{ minimum: "4.0.0", feature: "mergeInto()" }]);
    });

    it("collects the aliases of every CollectMetrics node and spots listColumns", () => {
        const inspection = inspectPlan({
            collect_metrics: {
                name: "outer",
                metrics: [{ alias: { expr: {}, name: ["rows"] } }, { unresolved_function: {} }],
                input: {
                    collect_metrics: { name: "inner", metrics: [{ alias: { name: [] } }], input: { catalog: { list_columns: {} } } },
                },
            },
        });
        expect([...inspection.metricNames]).toEqual([["outer", ["rows", undefined]], ["inner", [undefined]]]);
        expect(inspection.listsColumns).toBe(true);
        expect(inspectPlan({ collect_metrics: { metrics: [] } }).metricNames.size).toBe(0);
        expect(inspectPlan({ collect_metrics: null }).metricNames.size).toBe(0);
        expect(inspectPlan({ catalog: { list_tables: {} } }).listsColumns).toBe(false);
        expect(inspectPlan({ catalog: null }).listsColumns).toBe(false);
    });
});

describe("plans checked against the server version", () => {
    afterEach(() => vi.restoreAllMocks());

    it("rejects a 4.0 relation on Spark 3.5 before ExecutePlan", async () => {
        const spark = session("3.5.6");
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const people = spark.sql("SELECT 1 AS id");

        const error = await people.lateralJoin(spark.sql("SELECT 2 AS id")).collect().catch(e => e);
        expect(error).toBeInstanceOf(UnsupportedServerVersionError);
        expect(error).toMatchObject({
            feature: "lateralJoin()",
            requiredVersion: "4.0.0",
            serverVersion: "3.5.6",
        });
        await expect(people.select(col("id").tryCast("int")).collect())
            .rejects.toMatchObject({ feature: "tryCast()" });
        expect(execute).not.toHaveBeenCalled();
    });

    it("asks for the version only when a plan needs it", async () => {
        const spark = session("4.0.4");
        const analyze = vi.mocked(sparkGrpcClient.analyze);
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);

        await spark.sql("SELECT 1 AS id").collect();
        expect(analyze).not.toHaveBeenCalled();
        await spark.sql("SELECT 1 AS id").select(col("id").tryCast("int")).collect();
        expect(analyze).toHaveBeenCalledOnce();
        expect(sentPlan(execute, 1).root.project.expressions[0].cast.eval_mode).toBe("EVAL_MODE_TRY");
    });

    it("adds isCluster = false to listColumns on servers before 4.0 only", async () => {
        const old = session("3.5.6");
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        await old.catalog.listColumns("people").filter(col("nullable")).collect();
        const filter = sentPlan(execute).root.filter;
        expect(filter.input.project).toEqual({
            input: expect.objectContaining({ catalog: { list_columns: { table_name: "people" } } }),
            expressions: [
                { unresolved_star: {} },
                { alias: { expr: { literal: { boolean: false } }, name: ["isCluster"] } },
            ],
        });

        execute.mockClear();
        const current = session("4.0.4");
        await current.catalog.listColumns("people").collect();
        expect(sentPlan(execute).root).toMatchObject({ catalog: { list_columns: { table_name: "people" } } });
    });

    it("wraps only the listColumns relations of a plan", async () => {
        const spark = session("3.5.6");
        const plan = {
            project: {
                input: { set_op: { left_input: { catalog: { list_columns: {} } }, right_input: { catalog: { list_tables: {} } } } },
                expressions: [{ unresolved_attribute: { unparsed_identifier: "name" } }],
            },
        };
        const { plan: adapted } = await preparePlanForServer(spark, plan);
        expect(adapted.project.input.set_op.left_input).toHaveProperty("project.input.catalog.list_columns");
        expect(adapted.project.input.set_op.right_input).toEqual({ catalog: { list_tables: {} } });
        expect(adapted.project.expressions).toEqual(plan.project.expressions);
    });

    it("leaves plans without versioned fields untouched", async () => {
        const spark = session("3.5.6");
        const plan = { sql: { query: "SELECT 1" } };
        await expect(preparePlanForServer(spark, plan)).resolves.toEqual({ plan, metricNames: new Map() });
    });
});

describe("observed metrics from servers without metric keys", () => {
    afterEach(() => vi.restoreAllMocks());

    it("names the values with the plan's aliases, or by position when unaliased", () => {
        const response = {
            observed_metrics: [{ name: "stats", values: [{ long: "3" }, { string: "y" }, { long: "6" }] }],
        };
        expect(observedMetricsOf(response, new Map([["stats", ["rows", undefined]]]))).toEqual([
            { name: "stats", values: { rows: 3n, _2: "y", _3: 6n } },
        ]);
        expect(observedMetricsOf(response)).toEqual([{ name: "stats", values: { _1: 3n, _2: "y", _3: 6n } }]);
        expect(observedMetricsOf(
            { observed_metrics: [{ name: "stats", keys: ["n"], values: [{ long: "1" }] }] },
            new Map([["stats", ["rows"]]]),
        )).toEqual([{ name: "stats", values: { n: 1n } }]);
    });

    it("delivers 3.5 metrics to the observation under their aliases", async () => {
        const spark = session("3.5.6");
        const stats = new Observation("stats");
        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([
            { observed_metrics: [{ name: "stats", values: [{ long: "2" }, { string: "z" }] }] },
        ]);

        await spark.sql("SELECT 1 AS id").observe(stats, F.count(lit(1)).alias("rows"), F.max("id")).collect();
        await expect(stats.get()).resolves.toEqual({ rows: 2n, _2: "z" });
    });
});

describe("error class of servers without ErrorInfo classes", () => {
    it("reads the class from the message prefix, after the structured sources", () => {
        const failure = (details: string, errorClass?: string) => asSparkConnectError(Object.assign(new Error(details), {
            code: 13,
            details,
            errorInfo: {
                reason: "org.apache.spark.sql.catalyst.ExtendedAnalysisException",
                metadata: errorClass === undefined ? {} : { errorClass },
            },
        }), "ExecutePlan");

        expect(failure("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column ... cannot be resolved."))
            .toMatchObject({ errorClass: "UNRESOLVED_COLUMN.WITH_SUGGESTION" });
        expect(failure("[TABLE_OR_VIEW_NOT_FOUND] ...", "FROM_METADATA"))
            .toMatchObject({ errorClass: "FROM_METADATA" });
        expect(failure("Plain failure without a class."))
            .toMatchObject({ errorClass: "org.apache.spark.sql.catalyst.ExtendedAnalysisException" });
        expect(failure("[not a class] text"))
            .toMatchObject({ errorClass: "org.apache.spark.sql.catalyst.ExtendedAnalysisException" });
    });
});

describe("mode()", () => {
    it("sends the deterministic flag only when given, so Spark 3.5 accepts mode(col)", () => {
        const args = (builder: ReturnType<typeof F.mode>) =>
            (builder.build(ProtoExprAlg) as ProtoObject).unresolved_function.arguments;
        expect(args(F.mode("x"))).toEqual([{ unresolved_attribute: { unparsed_identifier: "x" } }]);
        expect(args(F.mode("x", true))).toEqual([
            { unresolved_attribute: { unparsed_identifier: "x" } },
            { literal: { boolean: true } },
        ]);
    });
});
