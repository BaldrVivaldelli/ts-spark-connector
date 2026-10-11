import { afterEach, describe, expect, it, vi } from "vitest";
import { ProtoDFAlg } from "../src/engine/compilerRead";
import { SparkDFAlg } from "../src/read/readDataFrameInterpreter";
import { TraceDFAlg } from "../src/trace/trace";
import { ReadChainedDataFrame } from "../src/read/readChainedDataFrame";
import { SparkSession } from "../src";
import type { CatalogOperation } from "../src";

const session = SparkSession.builder()
    .config("spark.connect.url", "sc://localhost:15002")
    .getOrCreate();

describe("catalog proto compilation", () => {
    const cases: Array<[CatalogOperation, Record<string, unknown>]> = [
        [{ method: "currentCatalog" }, { current_catalog: {} }],
        [{ method: "setCurrentCatalog", catalogName: "spark_catalog" }, { set_current_catalog: { catalog_name: "spark_catalog" } }],
        [{ method: "listCatalogs" }, { list_catalogs: {} }],
        [{ method: "listCatalogs", pattern: "spark*" }, { list_catalogs: { pattern: "spark*" } }],
        [{ method: "currentDatabase" }, { current_database: {} }],
        [{ method: "setCurrentDatabase", dbName: "default" }, { set_current_database: { db_name: "default" } }],
        [{ method: "listDatabases" }, { list_databases: {} }],
        [{ method: "listDatabases", pattern: "def*" }, { list_databases: { pattern: "def*" } }],
        [{ method: "listTables" }, { list_tables: {} }],
        [{ method: "listTables", dbName: "default", pattern: "t*" }, { list_tables: { db_name: "default", pattern: "t*" } }],
        [{ method: "listColumns", tableName: "people" }, { list_columns: { table_name: "people" } }],
        [{ method: "listColumns", tableName: "people", dbName: "default" }, { list_columns: { table_name: "people", db_name: "default" } }],
        [{ method: "databaseExists", dbName: "default" }, { database_exists: { db_name: "default" } }],
        [{ method: "tableExists", tableName: "people" }, { table_exists: { table_name: "people" } }],
        [{ method: "tableExists", tableName: "people", dbName: "default" }, { table_exists: { table_name: "people", db_name: "default" } }],
        [{ method: "functionExists", functionName: "upper" }, { function_exists: { function_name: "upper" } }],
        [{ method: "functionExists", functionName: "upper", dbName: "default" }, { function_exists: { function_name: "upper", db_name: "default" } }],
        [{ method: "dropTempView", viewName: "v" }, { drop_temp_view: { view_name: "v" } }],
        [{ method: "dropGlobalTempView", viewName: "v" }, { drop_global_temp_view: { view_name: "v" } }],
    ];

    it.each(cases)("compiles %j", (op, expected) => {
        expect(ProtoDFAlg.catalog(op)).toEqual({ catalog: expected });
    });

    it("keeps optional proto3 fields absent instead of sending empty strings", () => {
        const rel = ProtoDFAlg.catalog({ method: "listTables" }) as { catalog: { list_tables: Record<string, unknown> } };
        expect(Object.keys(rel.catalog.list_tables)).toEqual([]);
    });
});

describe("catalog client AST", () => {
    it("exposes the operation in the logical plan", () => {
        const op: CatalogOperation = { method: "listColumns", tableName: "people" };
        expect(SparkDFAlg.catalog(op)).toEqual({ type: "Catalog", operation: op });
    });

    it("labels the trace node with the catalog method", () => {
        const node = TraceDFAlg.catalog({ method: "tableExists", tableName: "people" });
        expect(JSON.stringify(node)).toContain("catalog(tableExists)");
    });

    it("round-trips through the DataFrame plan printers", () => {
        const plan = session.catalog.plan({ method: "listTables", dbName: "default" });
        expect(JSON.parse(plan.toProtoJSON())).toMatchObject({
            catalog: { list_tables: { db_name: "default" } },
        });
        expect(JSON.parse(plan.toSparkLogicalPlanJSON())).toMatchObject({
            type: "Catalog",
            operation: { method: "listTables", dbName: "default" },
        });
        expect(plan.toClientASTMermaid()).toContain("catalog(listTables)");
    });
});

describe("catalog list DataFrames", () => {
    function protoOf(df: { toProtoJSON(): string }): unknown {
        return JSON.parse(df.toProtoJSON());
    }

    it("builds list plans with and without their optional filters", () => {
        expect(protoOf(session.catalog.listCatalogs())).toEqual({ catalog: { list_catalogs: {} } });
        expect(protoOf(session.catalog.listCatalogs("sp*"))).toEqual({ catalog: { list_catalogs: { pattern: "sp*" } } });
        expect(protoOf(session.catalog.listDatabases())).toEqual({ catalog: { list_databases: {} } });
        expect(protoOf(session.catalog.listDatabases("def*"))).toEqual({ catalog: { list_databases: { pattern: "def*" } } });
        expect(protoOf(session.catalog.listTables())).toEqual({ catalog: { list_tables: {} } });
        expect(protoOf(session.catalog.listTables("default", "t*"))).toEqual({
            catalog: { list_tables: { db_name: "default", pattern: "t*" } },
        });
        expect(protoOf(session.catalog.listColumns("people"))).toEqual({
            catalog: { list_columns: { table_name: "people" } },
        });
        expect(protoOf(session.catalog.listColumns("people", "default"))).toEqual({
            catalog: { list_columns: { table_name: "people", db_name: "default" } },
        });
    });

    it("returns composable DataFrames", () => {
        expect(session.catalog.listTables()).toBeInstanceOf(ReadChainedDataFrame);
        expect(session.catalog.listTables().limit(1)).toBeInstanceOf(ReadChainedDataFrame);
    });
});

describe("catalog execution unwrapping", () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    function mockCollect(rows: Array<Record<string, unknown>>) {
        return vi
            .spyOn(ReadChainedDataFrame.prototype, "collect")
            .mockResolvedValue(rows as never);
    }

    it("unwraps string scalars positionally", async () => {
        mockCollect([{ current_catalog: "spark_catalog" }]);
        expect(await session.catalog.currentCatalog()).toBe("spark_catalog");
        mockCollect([{ anything: "default" }]);
        expect(await session.catalog.currentDatabase()).toBe("default");
    });

    it("unwraps boolean scalars for every existence and drop operation", async () => {
        mockCollect([{ exists: true }]);
        expect(await session.catalog.databaseExists("default")).toBe(true);
        expect(await session.catalog.tableExists("people")).toBe(true);
        expect(await session.catalog.tableExists("people", "default")).toBe(true);
        expect(await session.catalog.functionExists("upper")).toBe(true);
        expect(await session.catalog.functionExists("upper", "default")).toBe(true);
        expect(await session.catalog.dropTempView("v")).toBe(true);
        expect(await session.catalog.dropGlobalTempView("v")).toBe(true);
    });

    it("rejects an empty result instead of fabricating a value", async () => {
        mockCollect([]);
        await expect(session.catalog.currentDatabase()).rejects.toThrow(
            /expected a single string result but received undefined/,
        );
    });

    it("rejects a result of the wrong type", async () => {
        mockCollect([{ exists: "yes" }]);
        await expect(session.catalog.tableExists("people")).rejects.toThrow(
            /expected a single boolean result but received string/,
        );
    });

    it("runs set-current operations for their effect only", async () => {
        const raw = vi
            .spyOn(ReadChainedDataFrame.prototype, "collectRaw")
            .mockResolvedValue([] as never);
        await expect(session.catalog.setCurrentDatabase("default")).resolves.toBeUndefined();
        await expect(session.catalog.setCurrentCatalog("spark_catalog")).resolves.toBeUndefined();
        expect(raw).toHaveBeenCalledTimes(2);
    });
});

describe("catalog argument validation", () => {
    it.each([
        ["setCurrentCatalog", () => session.catalog.setCurrentCatalog("")],
        ["setCurrentDatabase", () => session.catalog.setCurrentDatabase(" ")],
        ["databaseExists", () => session.catalog.databaseExists("")],
        ["tableExists", () => session.catalog.tableExists("")],
        ["functionExists", () => session.catalog.functionExists(" ")],
        ["dropTempView", () => session.catalog.dropTempView("")],
        ["dropGlobalTempView", () => session.catalog.dropGlobalTempView(" ")],
    ])("%s rejects an empty name before touching the network", async (_label, call) => {
        await expect(call()).rejects.toThrow(TypeError);
    });

    it.each([
        ["listColumns", () => session.catalog.listColumns("")],
        ["listTables pattern", () => session.catalog.listTables("db", " ")],
        ["listDatabases pattern", () => session.catalog.listDatabases(" ")],
    ])("%s rejects an empty name synchronously", (_label, call) => {
        expect(call).toThrow(TypeError);
    });
});
