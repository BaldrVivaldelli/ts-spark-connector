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

/** @public */
export type CatalogEntry = InferSchema<typeof CatalogEntryRow.def>;
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
