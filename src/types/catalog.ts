/**
 * Catalog metadata operations supported by `session.catalog`.
 *
 * Each operation maps one-to-one to a case of the Spark Connect `Catalog`
 * relation (`catalog.proto`): the operation executes server-side and its
 * result comes back as ordinary Arrow rows. List-style operations surface as
 * DataFrames; scalar operations (`tableExists`, `currentDatabase`, ...) are
 * collected and unwrapped by `SparkCatalog`.
 *
 * @public
 */
export type CatalogOperation =
    | { readonly method: "currentCatalog" }
    | { readonly method: "setCurrentCatalog"; readonly catalogName: string }
    | { readonly method: "listCatalogs"; readonly pattern?: string }
    | { readonly method: "currentDatabase" }
    | { readonly method: "setCurrentDatabase"; readonly dbName: string }
    | { readonly method: "listDatabases"; readonly pattern?: string }
    | { readonly method: "listTables"; readonly dbName?: string; readonly pattern?: string }
    | { readonly method: "listColumns"; readonly tableName: string; readonly dbName?: string }
    | { readonly method: "databaseExists"; readonly dbName: string }
    | { readonly method: "tableExists"; readonly tableName: string; readonly dbName?: string }
    | { readonly method: "functionExists"; readonly functionName: string; readonly dbName?: string }
    | { readonly method: "dropTempView"; readonly viewName: string }
    | { readonly method: "dropGlobalTempView"; readonly viewName: string }
    | { readonly method: "listFunctions"; readonly dbName?: string; readonly pattern?: string }
    | { readonly method: "getDatabase"; readonly dbName: string }
    | { readonly method: "getTable"; readonly tableName: string; readonly dbName?: string }
    | { readonly method: "getFunction"; readonly functionName: string; readonly dbName?: string }
    | {
        readonly method: "createTable";
        readonly tableName: string;
        readonly path?: string;
        readonly source?: string;
        readonly description?: string;
        /** Table schema as a Spark DDL string, e.g. `id INT, name STRING`. */
        readonly schema?: string;
        readonly options?: Readonly<Record<string, string>>;
    }
    | {
        readonly method: "createExternalTable";
        readonly tableName: string;
        readonly path?: string;
        readonly source?: string;
        readonly schema?: string;
        readonly options?: Readonly<Record<string, string>>;
    }
    | { readonly method: "cacheTable"; readonly tableName: string; readonly storageLevel?: string }
    | { readonly method: "uncacheTable"; readonly tableName: string }
    | { readonly method: "isCached"; readonly tableName: string }
    | { readonly method: "clearCache" }
    | { readonly method: "refreshTable"; readonly tableName: string }
    | { readonly method: "refreshByPath"; readonly path: string }
    | { readonly method: "recoverPartitions"; readonly tableName: string };

/**
 * Name of a supported catalog operation.
 * @public
 */
export type CatalogMethod = CatalogOperation["method"];
