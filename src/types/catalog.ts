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
    | { readonly method: "dropGlobalTempView"; readonly viewName: string };

/**
 * Name of a supported catalog operation.
 * @public
 */
export type CatalogMethod = CatalogOperation["method"];
