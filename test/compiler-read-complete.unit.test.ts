import { afterEach, describe, expect, it, vi } from "vitest";
import type { SparkSession } from "../src/client/session";
import { SparkConnectExecutor } from "../src/client/sparkConnectExecutor";
import {
    applyPendingAnalyzeActions,
    getPendingAnalyzeActions,
    programToProtobufRoot,
    ProtoDFAlg,
    ProtoExec,
    ProtoExprAlg,
    type ProtoExpr,
    type ProtoRel,
} from "../src/engine/compilerRead";

const input = ProtoDFAlg.relation("parquet", "/input");
const column = ProtoExprAlg.col("value");
const literal = ProtoExprAlg.lit(1);

describe("complete protobuf read compiler", () => {
    afterEach(() => vi.restoreAllMocks());

    it("executes every expression constructor", () => {
        expect(ProtoExprAlg.col("value", 3)).toMatchObject({
            unresolved_attribute: { unparsed_identifier: "value", plan_id: 3 },
        });
        expect(ProtoExprAlg.lit(null)).toHaveProperty("literal.null");
        expect(ProtoExprAlg.lit(true)).toHaveProperty("literal.boolean", true);
        expect(ProtoExprAlg.lit("x")).toHaveProperty("literal.string", "x");
        expect(ProtoExprAlg.bin("+", literal, literal)).toHaveProperty(
            "unresolved_function.function_name",
            "+",
        );
        expect(ProtoExprAlg.logical("AND", literal, literal)).toHaveProperty(
            "unresolved_function.function_name",
            "AND",
        );
        expect(ProtoExprAlg.alias(literal, "alias")).toHaveProperty("alias.name", ["alias"]);
        expect(ProtoExprAlg.call("sum", [literal])).toHaveProperty(
            "unresolved_function.function_name",
            "sum",
        );
        expect(ProtoExprAlg.sortKey(column, "asc", "nullsFirst"))
            .toHaveProperty("sort_key_marker.nulls", "nullsFirst");
        expect(ProtoExprAlg.star()).toEqual({ unresolved_star: {} });
        expect(ProtoExprAlg.isNull(column)).toHaveProperty(
            "unresolved_function.function_name",
            "isnull",
        );
        expect(ProtoExprAlg.isNotNull(column)).toHaveProperty(
            "unresolved_function.function_name",
            "isnotnull",
        );
        expect(ProtoExprAlg.coalesce([column, literal])).toHaveProperty(
            "unresolved_function.function_name",
            "coalesce",
        );
        expect(ProtoExprAlg.explode(column)).toHaveProperty(
            "unresolved_function.function_name",
            "explode",
        );
        expect(ProtoExprAlg.posexplode(column)).toHaveProperty(
            "unresolved_function.function_name",
            "posexplode",
        );
        expect(ProtoExprAlg.getField(column, "nested")).toHaveProperty(
            "unresolved_function.function_name",
            "getfield",
        );
        expect(ProtoExprAlg.map_keys(column)).toHaveProperty(
            "unresolved_function.function_name",
            "map_keys",
        );
        expect(ProtoExprAlg.map_values(column)).toHaveProperty(
            "unresolved_function.function_name",
            "map_values",
        );
        expect(ProtoExprAlg.elementAt(column, column)).toHaveProperty(
            "unresolved_function.arguments.1",
            column,
        );
        const runtimeElementAt = ProtoExprAlg.elementAt as unknown as (
            value: ProtoExpr,
            key: unknown,
        ) => ProtoExpr;
        expect(runtimeElementAt(column, "key")).toHaveProperty(
            "unresolved_function.arguments.1.literal.string",
            "key",
        );
        expect(ProtoExprAlg.getItem(column, column)).toHaveProperty(
            "unresolved_function.arguments.1",
            column,
        );
        expect(ProtoExprAlg.getItem(column, 2)).toHaveProperty(
            "unresolved_function.arguments.1.literal.integer",
            2,
        );
        expect(ProtoExprAlg.getItem(column, "key")).toHaveProperty(
            "unresolved_function.arguments.1.literal.string",
            "key",
        );
        expect(ProtoExprAlg.split(column, column)).toHaveProperty(
            "unresolved_function.arguments.1",
            column,
        );
        expect(ProtoExprAlg.split(column, ",")).toHaveProperty(
            "unresolved_function.arguments.1.literal.string",
            ",",
        );
        expect(ProtoExprAlg.from_json(column, "value INT")).toHaveProperty(
            "unresolved_function.function_name",
            "from_json",
        );
        expect(ProtoExprAlg.to_json(column)).toHaveProperty(
            "unresolved_function.function_name",
            "to_json",
        );
    });

    it("rejects malformed cases and unsafe window boundaries", () => {
        const runtimeCaseWhen = ProtoExprAlg.caseWhen as unknown as (
            branches: Array<{ when: ProtoExpr; then: ProtoExpr }>,
            otherwise: ProtoExpr | null,
        ) => ProtoExpr;
        expect(() => runtimeCaseWhen([], null)).toThrow(/otherwise/);
        const sparse = new Array(1) as Array<{ when: ProtoExpr; then: ProtoExpr }>;
        expect(() => ProtoExprAlg.caseWhen(sparse, literal)).toThrow(/sparse/);
        expect(() => ProtoExprAlg.win(literal, {
            partitionBy: [],
            orderBy: [],
            frame: {
                type: "range",
                start: {
                    type: "ValuePreceding",
                    value: Number.MAX_SAFE_INTEGER + 1,
                },
                end: { type: "CurrentRow" },
            },
        })).toThrow(/safe integers/);
        expect(ProtoExprAlg.win(literal, {
            partitionBy: [],
            orderBy: [],
        })).not.toHaveProperty("window.frame_spec");
    });

    it("compiles special relations and rejects plural table/SQL sources", () => {
        expect(ProtoDFAlg.relation("table", "catalog.table")).toMatchObject({
            read: { named_table: { unparsed_identifier: "catalog.table", options: {} } },
        });
        expect(ProtoDFAlg.relation("sql", "SELECT 1")).toEqual({
            sql: { query: "SELECT 1" },
        });
        expect(ProtoDFAlg.relation("csv", ["/a", "/b"], undefined, "id INT"))
            .toMatchObject({
                read: {
                    data_source: {
                        paths: ["/a", "/b"],
                        options: {},
                        schema: "id INT",
                    },
                },
            });
        expect(() => ProtoDFAlg.relation("table", ["/a"])).toThrow(/one identifier/);
        expect(() => ProtoDFAlg.relation("sql", ["SELECT 1"])).toThrow(/one query/);
    });

    it("preserves deferred actions through plan IDs and nested/cyclic plans", () => {
        const cached = ProtoDFAlg.cache(input);
        const persisted = ProtoDFAlg.persist(cached, undefined);
        const planned = ProtoDFAlg.withPlanId!(persisted, 7);
        expect(planned).toHaveProperty("common.plan_id", 7);
        expect(getPendingAnalyzeActions(planned)).toHaveLength(2);
        expect(getPendingAnalyzeActions(ProtoDFAlg.withPlanId!(input, 1))).toEqual([]);

        const cyclic: { child?: unknown; same?: unknown } = { child: planned };
        cyclic.same = cyclic;
        expect(getPendingAnalyzeActions(cyclic)).toHaveLength(2);
        expect(getPendingAnalyzeActions(null)).toEqual([]);
        expect(getPendingAnalyzeActions("primitive")).toEqual([]);
    });

    it("covers plain and marked sorts plus all column-name extraction forms", () => {
        expect(ProtoDFAlg.orderBy(input, [{
            expr: column,
            direction: "desc",
            nulls: "nullsLast",
        }])).toHaveProperty("sort.order.0.direction", "SORT_DIRECTION_DESCENDING");
        const legacy = {
            unresolvedAttribute: { unparsedIdentifier: ["nested", "value"] },
        } as ProtoExpr;
        expect(ProtoDFAlg.dropDuplicates(input, [legacy]))
            .toHaveProperty("deduplicate.column_names", ["nested.value"]);
        expect(ProtoDFAlg.dropDuplicates(input)).toHaveProperty(
            "deduplicate.all_columns_as_keys",
            true,
        );
        expect(ProtoDFAlg.dropDuplicates(input, [])).toHaveProperty(
            "deduplicate.all_columns_as_keys",
            true,
        );
        expect(() => ProtoDFAlg.dropDuplicates(input, [
            { unresolved_attribute: { unparsed_identifier: "" } },
        ])).toThrow(/plain column/);
        expect(() => ProtoDFAlg.dropDuplicates(input, [literal]))
            .toThrow(/plain column/);
    });

    it("executes remaining dataframe transformations and validations", () => {
        expect(ProtoDFAlg.withColumnsRenamed(input, { old: "new" }))
            .toHaveProperty("with_columns_renamed.rename_columns_map.old", "new");
        expect(ProtoDFAlg.describe!(input, [])).toHaveProperty("describe.cols", []);
        expect(ProtoDFAlg.describe!(input, [column])).toHaveProperty(
            "describe.cols",
            ["value"],
        );
        expect(() => ProtoDFAlg.describe!(input, [literal])).toThrow(/plain column/);
        expect(ProtoDFAlg.summary!(input, [ProtoExprAlg.col("count")], [column]))
            .toHaveProperty("summary.statistics", ["count"]);
        expect(() => ProtoDFAlg.summary!(input, [literal], []))
            .toThrow(/metrics must be plain/);

        expect(ProtoDFAlg.hint!(input, "mixed", [
            column,
            "text",
            1,
            true,
            2n,
            null,
        ])).toHaveProperty("hint.parameters", expect.any(Array));
        expect(ProtoDFAlg.hint!(input, "empty")).toHaveProperty("hint.parameters", []);
        expect(() => ProtoDFAlg.hint!(
            input,
            "invalid",
            [Symbol("unsupported")],
        )).toThrow(/unsupported hint parameter/i);
        expect(ProtoDFAlg.sample!(input, 0, 1, undefined, undefined, undefined))
            .not.toHaveProperty("sample.with_replacement");
        expect(ProtoDFAlg.sample!(input, 0, 1, true, 4, true)).toMatchObject({
            sample: {
                with_replacement: true,
                seed: 4,
                deterministic_order: true,
            },
        });
        expect(() => ProtoDFAlg.withWatermark!(input, literal, "1 minute"))
            .toThrow(/plain event-time/);
        const runtimeJoin = ProtoDFAlg.join as unknown as (
            left: ProtoRel,
            right: ProtoRel,
            condition: ProtoExpr,
            joinType: null,
        ) => ProtoRel;
        expect(runtimeJoin(input, input, literal, null)).toHaveProperty("join.join_type");
    });

    it("applies actions and delegates collect/explain through one executor", async () => {
        const runAnalyzeAction = vi.fn(async () => undefined);
        const execute = vi.fn(async () => [{ value: 1 }]);
        const explain = vi.fn(async () => "explain text");
        const executor = {
            runAnalyzeAction,
            execute,
            explain,
        } as unknown as SparkConnectExecutor;
        vi.spyOn(SparkConnectExecutor, "for").mockReturnValue(executor);
        const session = {} as SparkSession;
        const root = ProtoDFAlg.unpersist(ProtoDFAlg.cache(input), true);

        await expect(applyPendingAnalyzeActions(root, session)).resolves.toBe(executor);
        expect(runAnalyzeAction).toHaveBeenCalledTimes(2);
        await expect(ProtoExec.collect(root, session)).resolves.toEqual([{ value: 1 }]);
        await expect(ProtoExec.explain!(root, session, undefined as never))
            .resolves.toBe("explain text");
        await expect(ProtoExec.explain!(root, session, "extended"))
            .resolves.toBe("explain text");
        expect(execute).toHaveBeenCalledWith(root);
        expect(explain).toHaveBeenCalledWith(root, "simple");
        expect(explain).toHaveBeenCalledWith(root, "extended");
        expect(programToProtobufRoot(root)).toEqual({ plan: { root } });
    });
});
