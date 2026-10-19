import { afterEach, describe, expect, it, vi } from "vitest";
import { ProtoDFAlg, unparsedDataType } from "../src/engine/compilerRead";
import { ReadChainedDataFrame } from "../src/read/readChainedDataFrame";
import { STORAGE_LEVEL_NAMES, toProtoStorageLevel } from "../src/client/storageLevel";
import { sparkGrpcClient } from "../src/client/sparkClient";
import { SparkSession, schema } from "../src";
import type { CatalogOperation } from "../src";

const session = SparkSession.builder()
    .config("spark.connect.url", "sc://localhost:15002")
    .getOrCreate();

const memoryOnly = { use_disk: false, use_memory: true, use_off_heap: false, deserialized: true, replication: 1 };

describe("catalog parity: proto compilation", () => {
    const cases: Array<[CatalogOperation, Record<string, unknown>]> = [
        [{ method: "listFunctions" }, { list_functions: {} }],
        [{ method: "listFunctions", dbName: "default", pattern: "up*" }, { list_functions: { db_name: "default", pattern: "up*" } }],
        [{ method: "getDatabase", dbName: "default" }, { get_database: { db_name: "default" } }],
        [{ method: "getTable", tableName: "people" }, { get_table: { table_name: "people" } }],
        [{ method: "getTable", tableName: "people", dbName: "default" }, { get_table: { table_name: "people", db_name: "default" } }],
        [{ method: "getFunction", functionName: "upper" }, { get_function: { function_name: "upper" } }],
        [{ method: "getFunction", functionName: "upper", dbName: "default" }, { get_function: { function_name: "upper", db_name: "default" } }],
        [{ method: "createTable", tableName: "t" }, { create_table: { table_name: "t", options: {} } }],
        [
            {
                method: "createTable",
                tableName: "t",
                path: "/data/t",
                source: "parquet",
                description: "people",
                schema: "id INT, name STRING",
                options: { mergeSchema: "true" },
            },
            {
                create_table: {
                    table_name: "t",
                    path: "/data/t",
                    source: "parquet",
                    description: "people",
                    schema: { unparsed: { data_type_string: "id INT, name STRING" } },
                    options: { mergeSchema: "true" },
                },
            },
        ],
        [{ method: "createExternalTable", tableName: "t", path: "/data/t" }, { create_external_table: { table_name: "t", path: "/data/t", options: {} } }],
        [{ method: "createExternalTable", tableName: "t" }, { create_external_table: { table_name: "t", options: {} } }],
        [
            { method: "createExternalTable", tableName: "t", path: "/data/t", source: "csv", schema: "a INT", options: { header: "true" } },
            { create_external_table: { table_name: "t", path: "/data/t", source: "csv", schema: { unparsed: { data_type_string: "a INT" } }, options: { header: "true" } } },
        ],
        [{ method: "cacheTable", tableName: "t" }, { cache_table: { table_name: "t" } }],
        [{ method: "cacheTable", tableName: "t", storageLevel: "memory_only" }, { cache_table: { table_name: "t", storage_level: memoryOnly } }],
        [{ method: "uncacheTable", tableName: "t" }, { uncache_table: { table_name: "t" } }],
        [{ method: "isCached", tableName: "t" }, { is_cached: { table_name: "t" } }],
        [{ method: "clearCache" }, { clear_cache: {} }],
        [{ method: "refreshTable", tableName: "t" }, { refresh_table: { table_name: "t" } }],
        [{ method: "refreshByPath", path: "/data" }, { refresh_by_path: { path: "/data" } }],
        [{ method: "recoverPartitions", tableName: "t" }, { recover_partitions: { table_name: "t" } }],
    ];

    it.each(cases)("compiles %j", (op, expected) => {
        expect(ProtoDFAlg.catalog(op)).toEqual({ catalog: expected });
    });

    it("rejects unknown storage levels and accepts every documented name in any case", () => {
        expect(() => ProtoDFAlg.catalog({ method: "cacheTable", tableName: "t", storageLevel: "FAST" }))
            .toThrow(/Unsupported storage level "FAST"/);
        expect(STORAGE_LEVEL_NAMES).toContain("MEMORY_AND_DISK");
        for (const name of STORAGE_LEVEL_NAMES) {
            expect(toProtoStorageLevel(name.toLowerCase())).toEqual(toProtoStorageLevel(name));
        }
        expect(toProtoStorageLevel("OFF_HEAP").use_off_heap).toBe(true);
        expect(unparsedDataType("struct<a:int>")).toEqual({ unparsed: { data_type_string: "struct<a:int>" } });
    });
});

describe("catalog parity: client surface", () => {
    afterEach(() => vi.restoreAllMocks());

    function protoOf(df: { toProtoJSON(): string }): unknown {
        return JSON.parse(df.toProtoJSON());
    }

    it("builds listFunctions and create-table plans from the public options", () => {
        expect(protoOf(session.catalog.listFunctions())).toEqual({ catalog: { list_functions: {} } });
        expect(protoOf(session.catalog.listFunctions("default", "up*"))).toEqual({
            catalog: { list_functions: { db_name: "default", pattern: "up*" } },
        });
        expect(session.catalog.listFunctions()).toBeInstanceOf(ReadChainedDataFrame);

    });

    it("creates tables eagerly, once, and returns the new table as a DataFrame", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const People = schema({ id: "int", name: "string?" });

        const people = await session.catalog.createTable("people", {
            path: "/data/people",
            source: "parquet",
            schema: People,
            description: "folks",
            options: { mergeSchema: "true" },
        });
        expect(execute).toHaveBeenCalledTimes(1);
        expect((execute.mock.calls[0]![0].plan as { root: unknown }).root).toEqual({
            limit: {
                limit: 0,
                input: {
                    catalog: {
                        create_table: {
                            table_name: "people",
                            path: "/data/people",
                            source: "parquet",
                            description: "folks",
                            schema: { unparsed: { data_type_string: "id INT NOT NULL, name STRING" } },
                            options: { mergeSchema: "true" },
                        },
                    },
                },
            },
        });
        // Actions on the result read the table; they never re-send the creation.
        expect(people).toBeInstanceOf(ReadChainedDataFrame);
        expect(protoOf(people)).toEqual({ read: { named_table: { unparsed_identifier: "people", options: {} } } });
        await people.collectRaw();
        await people.collectRaw();
        expect(execute).toHaveBeenCalledTimes(3);
        expect((execute.mock.calls[2]![0].plan as { root: unknown }).root).toEqual(protoOf(people));

        const bare = await session.catalog.createTable("bare");
        expect((execute.mock.calls[3]![0].plan as { root: { limit: { input: unknown } } }).root.limit.input)
            .toEqual({ catalog: { create_table: { table_name: "bare", options: {} } } });
        expect(protoOf(bare)).toMatchObject({ read: { named_table: { unparsed_identifier: "bare" } } });

        const ext = await session.catalog.createExternalTable("ext", { path: "/data/ext", schema: "a INT" });
        expect((execute.mock.calls[4]![0].plan as { root: { limit: { input: unknown } } }).root.limit.input).toEqual({
            catalog: { create_external_table: { table_name: "ext", path: "/data/ext", schema: { unparsed: { data_type_string: "a INT" } }, options: {} } },
        });
        expect(protoOf(ext)).toMatchObject({ read: { named_table: { unparsed_identifier: "ext" } } });
    });

    it("validates create-table options before any RPC", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan");
        await expect(session.catalog.createTable("")).rejects.toThrow(/tableName/);
        await expect(session.catalog.createTable("t", { path: " " })).rejects.toThrow(/path/);
        await expect(session.catalog.createTable("t", { source: "" })).rejects.toThrow(/source/);
        await expect(session.catalog.createTable("t", { schema: " " })).rejects.toThrow(/schema/);
        await expect(session.catalog.createTable("t", { description: "" })).rejects.toThrow(/description/);
        await expect(session.catalog.createTable("t", { options: { "": "x" } })).rejects.toThrow(/option name/);
        await expect(session.catalog.createTable("t", { options: { k: 1 as never } })).rejects.toThrow(/option "k" must be a string/);
        await expect(session.catalog.createExternalTable("", { path: "/p" })).rejects.toThrow(/tableName/);
        expect(execute).not.toHaveBeenCalled();
    });

    it("unwraps single-row results and rejects an empty one", async () => {
        const collect = vi.spyOn(ReadChainedDataFrame.prototype, "collect");
        collect.mockResolvedValueOnce([{ name: "default", catalog: "spark_catalog", description: null, locationUri: "file:/w" }] as never);
        expect(await session.catalog.getDatabase("default")).toEqual({
            name: "default", catalog: "spark_catalog", description: null, locationUri: "file:/w",
        });
        collect.mockResolvedValueOnce([{ name: "people", isTemporary: true }] as never);
        expect(await session.catalog.getTable("people")).toMatchObject({ name: "people", isTemporary: true });
        collect.mockResolvedValueOnce([{ name: "people", isTemporary: false }] as never);
        expect(await session.catalog.getTable("people", "default")).toMatchObject({ isTemporary: false });
        collect.mockResolvedValueOnce([{ name: "upper", className: "org.apache.spark.sql.catalyst.expressions.Upper" }] as never);
        expect(await session.catalog.getFunction("upper", "default")).toMatchObject({ name: "upper" });
        collect.mockResolvedValueOnce([{ name: "lower", className: "Lower" }] as never);
        expect(await session.catalog.getFunction("lower")).toMatchObject({ name: "lower" });
        collect.mockRestore();

        // An empty Arrow result (the plan really runs here) is an error, not undefined.
        vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        await expect(session.catalog.getTable("missing")).rejects.toThrow(/getTable expected a single row but received none/);
    });

    it("runs cache and refresh operations for their effect, and unwraps isCached", async () => {
        const raw = vi.spyOn(ReadChainedDataFrame.prototype, "collectRaw").mockResolvedValue([] as never);
        await expect(session.catalog.cacheTable("t")).resolves.toBeUndefined();
        await expect(session.catalog.cacheTable("t", "DISK_ONLY")).resolves.toBeUndefined();
        await expect(session.catalog.uncacheTable("t")).resolves.toBeUndefined();
        await expect(session.catalog.clearCache()).resolves.toBeUndefined();
        await expect(session.catalog.refreshTable("t")).resolves.toBeUndefined();
        await expect(session.catalog.refreshByPath("/data")).resolves.toBeUndefined();
        await expect(session.catalog.recoverPartitions("t")).resolves.toBeUndefined();
        expect(raw).toHaveBeenCalledTimes(7);

        vi.spyOn(ReadChainedDataFrame.prototype, "collect").mockResolvedValue([{ cached: true }] as never);
        expect(await session.catalog.isCached("t")).toBe(true);
    });

    it.each([
        ["getDatabase", () => session.catalog.getDatabase("")],
        ["getTable", () => session.catalog.getTable(" ")],
        ["getTable dbName", () => session.catalog.getTable("t", "")],
        ["getFunction", () => session.catalog.getFunction("")],
        ["getFunction dbName", () => session.catalog.getFunction("f", " ")],
        ["cacheTable", () => session.catalog.cacheTable("")],
        ["cacheTable storageLevel", () => session.catalog.cacheTable("t", "")],
        ["uncacheTable", () => session.catalog.uncacheTable(" ")],
        ["isCached", () => session.catalog.isCached("")],
        ["refreshTable", () => session.catalog.refreshTable("")],
        ["refreshByPath", () => session.catalog.refreshByPath(" ")],
        ["recoverPartitions", () => session.catalog.recoverPartitions("")],
    ])("%s rejects an empty name before touching the network", async (_label, call) => {
        await expect(call()).rejects.toThrow(TypeError);
    });

    it.each([
        ["listFunctions dbName", () => session.catalog.listFunctions(" ")],
        ["listFunctions pattern", () => session.catalog.listFunctions("db", "")],
    ])("%s rejects an empty name synchronously", (_label, call) => {
        expect(call).toThrow(TypeError);
    });
});
