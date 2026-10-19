import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import { SparkSession } from "./session";
import { DFProgram } from "../algebra/read";
import { CatalogOperation } from "../types/catalog";
import { DeclaredSchema, InferSchema, SchemaDef, arrayType, schema } from "../schema/schema";
import { UnknownSchema } from "../schema/schema-model";

/**
 * Row shape of `catalog.listCatalogs()`.
 * @public
 */
export const CatalogEntryRow = schema({
    name: "string",
    description: "string?",
});

/**
 * Row shape of `catalog.listDatabases()`. Every column except `name` is
 * conservatively nullable: the server fills them in catalog-dependent ways.
 * @public
 */
export const CatalogDatabaseRow = schema({
    name: "string",
    catalog: "string?",
    description: "string?",
    locationUri: "string?",
});

/**
 * Row shape of `catalog.listTables()`.
 * @public
 */
export const CatalogTableRow = schema({
    name: "string",
    catalog: "string?",
    namespace: arrayType("string?", true),
    description: "string?",
    tableType: "string?",
    isTemporary: "boolean",
});

/**
 * Row shape of `catalog.listColumns()`.
 * @public
 */
export const CatalogColumnRow = schema({
    name: "string",
    description: "string?",
    dataType: "string",
    nullable: "boolean",
    isPartition: "boolean",
    isBucket: "boolean",
    isCluster: "boolean",
});

/**
 * Row shape of `catalog.listFunctions()` and `catalog.getFunction()`.
 * @public
 */
export const CatalogFunctionRow = schema({
    name: "string",
    catalog: "string?",
    namespace: arrayType("string?", true),
    description: "string?",
    className: "string",
    isTemporary: "boolean",
});

/** @public */
export type CatalogEntry = InferSchema<typeof CatalogEntryRow.def>;
/** @public */
export type CatalogFunction = InferSchema<typeof CatalogFunctionRow.def>;

/**
 * Options of `catalog.createTable()` and `catalog.createExternalTable()`,
 * mirroring PySpark's keyword arguments.
 * @public
 */
export type CreateTableOptions = {
    /** Data location; required for external tables. */
    path?: string;
    /** Data source format, e.g. `parquet` (Spark's default source when omitted). */
    source?: string;
    /** Table schema: a `schema({...})` declaration or a DDL string. Inferred from the data when omitted. */
    schema?: DeclaredSchema<SchemaDef> | string;
    /** Table comment (`createTable` only). */
    description?: string;
    /** Data source options. */
    options?: Readonly<Record<string, string>>;
};
/** @public */
export type CatalogDatabase = InferSchema<typeof CatalogDatabaseRow.def>;
/** @public */
export type CatalogTable = InferSchema<typeof CatalogTableRow.def>;
/** @public */
export type CatalogColumn = InferSchema<typeof CatalogColumnRow.def>;

function requireName(value: string, label: string): string {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${label} must be a non-empty string.`);
    }
    return value;
}

function optionalName(value: string | undefined, label: string): string | undefined {
    return value === undefined ? undefined : requireName(value, label);
}

/** Validates the fields `createTable` and `createExternalTable` share. */
function createTableFields(label: string, options: CreateTableOptions): {
    path?: string;
    source?: string;
    schema?: string;
    options?: Readonly<Record<string, string>>;
} {
    const path = optionalName(options.path, `${label} path`);
    const source = optionalName(options.source, `${label} source`);
    const schema = options.schema === undefined
        ? undefined
        : options.schema instanceof DeclaredSchema
            ? options.schema.toDDL()
            : requireName(options.schema, `${label} schema`);
    if (options.options !== undefined) {
        for (const [key, value] of Object.entries(options.options)) {
            requireName(key, `${label} option name`);
            if (typeof value !== "string") throw new TypeError(`${label} option ${JSON.stringify(key)} must be a string.`);
        }
    }
    return {
        ...(path !== undefined ? { path } : {}),
        ...(source !== undefined ? { source } : {}),
        ...(schema !== undefined ? { schema } : {}),
        ...(options.options !== undefined ? { options: { ...options.options } } : {}),
    };
}

/**
 * Server-side catalog metadata for a Spark Connect session, mirroring
 * `spark.catalog`. List operations return ordinary DataFrames, so they
 * compose with `filter`/`select`/`collect` like any other relation; scalar
 * operations execute immediately and unwrap their single-value result.
 *
 * @public
 */
export class SparkCatalog<R = unknown, E = unknown, G = unknown> {
    constructor(private readonly session: SparkSession) {}

    /** Name of the current default catalog. */
    async currentCatalog(): Promise<string> {
        return this.scalar<string>({ method: "currentCatalog" }, "string");
    }

    /** Sets the current default catalog. */
    async setCurrentCatalog(catalogName: string): Promise<void> {
        await this.execute({
            method: "setCurrentCatalog",
            catalogName: requireName(catalogName, "setCurrentCatalog() catalogName"),
        });
    }

    /** Catalogs available in this session, optionally filtered by pattern. */
    listCatalogs(pattern?: string): ReadChainedDataFrame<CatalogEntry, R, E, G> {
        const match = optionalName(pattern, "listCatalogs() pattern");
        return this.list(
            { method: "listCatalogs", ...(match !== undefined ? { pattern: match } : {}) },
            CatalogEntryRow,
        );
    }

    /** Name of the current default database. */
    async currentDatabase(): Promise<string> {
        return this.scalar<string>({ method: "currentDatabase" }, "string");
    }

    /** Sets the current default database. */
    async setCurrentDatabase(dbName: string): Promise<void> {
        await this.execute({
            method: "setCurrentDatabase",
            dbName: requireName(dbName, "setCurrentDatabase() dbName"),
        });
    }

    /** Databases available in this session, optionally filtered by pattern. */
    listDatabases(pattern?: string): ReadChainedDataFrame<CatalogDatabase, R, E, G> {
        const match = optionalName(pattern, "listDatabases() pattern");
        return this.list(
            { method: "listDatabases", ...(match !== undefined ? { pattern: match } : {}) },
            CatalogDatabaseRow,
        );
    }

    /** Tables and views in a database (the current one when omitted). */
    listTables(dbName?: string, pattern?: string): ReadChainedDataFrame<CatalogTable, R, E, G> {
        const db = optionalName(dbName, "listTables() dbName");
        const match = optionalName(pattern, "listTables() pattern");
        return this.list(
            {
                method: "listTables",
                ...(db !== undefined ? { dbName: db } : {}),
                ...(match !== undefined ? { pattern: match } : {}),
            },
            CatalogTableRow,
        );
    }

    /** Columns of a table or view. */
    listColumns(tableName: string, dbName?: string): ReadChainedDataFrame<CatalogColumn, R, E, G> {
        const db = optionalName(dbName, "listColumns() dbName");
        return this.list(
            {
                method: "listColumns",
                tableName: requireName(tableName, "listColumns() tableName"),
                ...(db !== undefined ? { dbName: db } : {}),
            },
            CatalogColumnRow,
        );
    }

    /** Whether a database exists. */
    async databaseExists(dbName: string): Promise<boolean> {
        return this.scalar<boolean>(
            { method: "databaseExists", dbName: requireName(dbName, "databaseExists() dbName") },
            "boolean",
        );
    }

    /** Whether a table or view exists, in the current database when `dbName` is omitted. */
    async tableExists(tableName: string, dbName?: string): Promise<boolean> {
        const db = optionalName(dbName, "tableExists() dbName");
        return this.scalar<boolean>(
            {
                method: "tableExists",
                tableName: requireName(tableName, "tableExists() tableName"),
                ...(db !== undefined ? { dbName: db } : {}),
            },
            "boolean",
        );
    }

    /** Whether a function exists, in the current database when `dbName` is omitted. */
    async functionExists(functionName: string, dbName?: string): Promise<boolean> {
        const db = optionalName(dbName, "functionExists() dbName");
        return this.scalar<boolean>(
            {
                method: "functionExists",
                functionName: requireName(functionName, "functionExists() functionName"),
                ...(db !== undefined ? { dbName: db } : {}),
            },
            "boolean",
        );
    }

    /** Drops a temporary view; resolves `true` when the view existed. */
    async dropTempView(viewName: string): Promise<boolean> {
        return this.scalar<boolean>(
            { method: "dropTempView", viewName: requireName(viewName, "dropTempView() viewName") },
            "boolean",
        );
    }

    /** Drops a global temporary view; resolves `true` when the view existed. */
    async dropGlobalTempView(viewName: string): Promise<boolean> {
        return this.scalar<boolean>(
            { method: "dropGlobalTempView", viewName: requireName(viewName, "dropGlobalTempView() viewName") },
            "boolean",
        );
    }

    /** Functions registered in a database (the current one when omitted), optionally filtered by pattern. */
    listFunctions(dbName?: string, pattern?: string): ReadChainedDataFrame<CatalogFunction, R, E, G> {
        const db = optionalName(dbName, "listFunctions() dbName");
        const match = optionalName(pattern, "listFunctions() pattern");
        return this.list(
            {
                method: "listFunctions",
                ...(db !== undefined ? { dbName: db } : {}),
                ...(match !== undefined ? { pattern: match } : {}),
            },
            CatalogFunctionRow,
        );
    }

    /** Metadata of one database; rejects when it does not exist. */
    async getDatabase(dbName: string): Promise<CatalogDatabase> {
        return this.single<CatalogDatabase>(
            { method: "getDatabase", dbName: requireName(dbName, "getDatabase() dbName") },
            CatalogDatabaseRow,
        );
    }

    /** Metadata of one table or view; rejects when it does not exist. */
    async getTable(tableName: string, dbName?: string): Promise<CatalogTable> {
        const db = optionalName(dbName, "getTable() dbName");
        return this.single<CatalogTable>(
            {
                method: "getTable",
                tableName: requireName(tableName, "getTable() tableName"),
                ...(db !== undefined ? { dbName: db } : {}),
            },
            CatalogTableRow,
        );
    }

    /** Metadata of one function; rejects when it does not exist. */
    async getFunction(functionName: string, dbName?: string): Promise<CatalogFunction> {
        const db = optionalName(dbName, "getFunction() dbName");
        return this.single<CatalogFunction>(
            {
                method: "getFunction",
                functionName: requireName(functionName, "getFunction() functionName"),
                ...(db !== undefined ? { dbName: db } : {}),
            },
            CatalogFunctionRow,
        );
    }

    /**
     * Creates a table from a data source (an external table when `path` is
     * given, a managed one otherwise) and returns it as a DataFrame.
     */
    createTable(tableName: string, options: CreateTableOptions = {}): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.plan({
            method: "createTable",
            tableName: requireName(tableName, "createTable() tableName"),
            ...createTableFields("createTable()", options),
            ...(options.description !== undefined
                ? { description: requireName(options.description, "createTable() description") }
                : {}),
        });
    }

    /** Creates an external table backed by `path` and returns it as a DataFrame. */
    createExternalTable(tableName: string, options: CreateTableOptions = {}): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        return this.plan({
            method: "createExternalTable",
            tableName: requireName(tableName, "createExternalTable() tableName"),
            ...createTableFields("createExternalTable()", options),
        });
    }

    /** Caches a table in memory (or at the given storage level, e.g. `DISK_ONLY`). */
    async cacheTable(tableName: string, storageLevel?: string): Promise<void> {
        await this.execute({
            method: "cacheTable",
            tableName: requireName(tableName, "cacheTable() tableName"),
            ...(storageLevel !== undefined
                ? { storageLevel: requireName(storageLevel, "cacheTable() storageLevel") }
                : {}),
        });
    }

    /** Removes a table from the cache. */
    async uncacheTable(tableName: string): Promise<void> {
        await this.execute({ method: "uncacheTable", tableName: requireName(tableName, "uncacheTable() tableName") });
    }

    /** Whether a table is currently cached. */
    async isCached(tableName: string): Promise<boolean> {
        return this.scalar<boolean>(
            { method: "isCached", tableName: requireName(tableName, "isCached() tableName") },
            "boolean",
        );
    }

    /** Removes every cached table from the cache. */
    async clearCache(): Promise<void> {
        await this.execute({ method: "clearCache" });
    }

    /** Invalidates cached metadata and data of a table. */
    async refreshTable(tableName: string): Promise<void> {
        await this.execute({ method: "refreshTable", tableName: requireName(tableName, "refreshTable() tableName") });
    }

    /** Invalidates cached metadata and data of every table under a path. */
    async refreshByPath(path: string): Promise<void> {
        await this.execute({ method: "refreshByPath", path: requireName(path, "refreshByPath() path") });
    }

    /** Recovers the partitions of a partitioned table from its directory layout. */
    async recoverPartitions(tableName: string): Promise<void> {
        await this.execute({
            method: "recoverPartitions",
            tableName: requireName(tableName, "recoverPartitions() tableName"),
        });
    }

    /**
     * The catalog operation as an uncollected DataFrame plan, for inspection
     * through `toProtoJSON()` and the client AST printers.
     */
    plan(op: CatalogOperation): ReadChainedDataFrame<UnknownSchema, R, E, G> {
        const prog: DFProgram<R, E, G> = DF => DF.catalog(op);
        return new ReadChainedDataFrame<UnknownSchema, R, E, G>(prog, this.session);
    }

    // ---------- interno ----------

    private list<D extends SchemaDef>(
        op: CatalogOperation,
        declared: DeclaredSchema<D>,
    ): ReadChainedDataFrame<InferSchema<D>, R, E, G> {
        const prog: DFProgram<R, E, G> = DF => DF.catalog(op);
        return new ReadChainedDataFrame<InferSchema<D>, R, E, G>(prog, this.session, false, declared.def);
    }

    /** Collects the single row of a get-style operation, rejecting an empty result. */
    private async single<Row>(op: CatalogOperation, declared: DeclaredSchema<SchemaDef>): Promise<Row> {
        // Untyped at the TypeScript level (the row type is the caller's) but
        // still decoded and validated against the declared runtime schema.
        const prog: DFProgram<R, E, G> = DF => DF.catalog(op);
        const [row] = await new ReadChainedDataFrame<UnknownSchema, R, E, G>(prog, this.session, false, declared.def)
            .collect();
        if (row === undefined) {
            throw new TypeError(`catalog.${op.method} expected a single row but received none.`);
        }
        return row as Row;
    }

    /** Runs an operation executed only for its server-side effect. */
    private async execute(op: CatalogOperation): Promise<void> {
        await this.plan(op).collectRaw();
    }

    /**
     * Collects a single-row, single-column catalog result. The column is read
     * positionally: the server names it after the operation, not after a
     * stable contract.
     */
    private async scalar<T extends string | boolean>(
        op: CatalogOperation,
        expected: "string" | "boolean",
    ): Promise<T> {
        const rows = await this.plan(op).collect();
        const first = rows[0];
        const value = first === undefined ? undefined : Object.values(first)[0];
        if (typeof value !== expected) {
            throw new TypeError(
                `catalog.${op.method} expected a single ${expected} result but received ${typeof value}.`,
            );
        }
        return value as T;
    }
}
