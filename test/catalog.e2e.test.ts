import { afterAll, describe, expect, it } from "vitest";
import { SparkSession } from "../src";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";

function connectedBuilder() {
    const builder = SparkSession.builder().config("spark.connect.url", connectUrl);
    if (connectUrl.startsWith("scs://")) {
        builder.enableTLS({
            trustStorePath: caPath,
            serverNameOverride: serverName,
        });
    }
    return builder;
}

const session = connectedBuilder().getOrCreate();
afterAll(async () => session.close());

const VIEW = "catalog_e2e_people";

describe("catalog metadata", () => {
    it("reports the default catalog and database", async () => {
        expect(await session.catalog.currentCatalog()).toBe("spark_catalog");
        expect(await session.catalog.currentDatabase()).toBe("default");
        expect(await session.catalog.databaseExists("default")).toBe(true);
        expect(await session.catalog.databaseExists("catalog_e2e_missing_db")).toBe(false);
    });

    it("lists catalogs and databases", async () => {
        const catalogs = await session.catalog.listCatalogs().collect();
        expect(catalogs.map(row => row.name)).toContain("spark_catalog");

        const databases = await session.catalog.listDatabases().collect();
        expect(databases.map(row => row.name)).toContain("default");

        const filtered = await session.catalog.listDatabases("no_database_matches_this").collect();
        expect(filtered).toEqual([]);
    });

    it("observes a temporary view through its whole lifecycle", async () => {
        await session
            .sql(`CREATE OR REPLACE TEMPORARY VIEW ${VIEW} AS SELECT 1 AS id, 'ada' AS name`)
            .collectRaw();

        expect(await session.catalog.tableExists(VIEW)).toBe(true);

        const tables = await session.catalog.listTables().collect();
        const view = tables.find(row => row.name === VIEW);
        expect(view).toBeDefined();
        expect(view!.isTemporary).toBe(true);

        const columns = await session.catalog.listColumns(VIEW).collect();
        expect(columns.map(column => [column.name, column.dataType])).toEqual([
            ["id", "int"],
            ["name", "string"],
        ]);
        expect(columns.every(column => !column.isPartition && !column.isBucket && !column.isCluster)).toBe(true);

        expect(await session.catalog.dropTempView(VIEW)).toBe(true);
        expect(await session.catalog.tableExists(VIEW)).toBe(false);
        expect(await session.catalog.dropTempView(VIEW)).toBe(false);
    });

    it("answers function existence", async () => {
        expect(await session.catalog.functionExists("upper")).toBe(true);
        expect(await session.catalog.functionExists("catalog_e2e_missing_fn")).toBe(false);
    });

    it("keeps setCurrentDatabase effective within the session", async () => {
        await session.catalog.setCurrentDatabase("default");
        expect(await session.catalog.currentDatabase()).toBe("default");
    });
});
