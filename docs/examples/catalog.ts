import { withExampleSession } from "./_session.js";

void withExampleSession(async spark => {
    await spark
        .sql("CREATE OR REPLACE TEMPORARY VIEW example_people AS SELECT 1 AS id, 'ada' AS name")
        .collectRaw();

    console.log("current catalog:", await spark.catalog.currentCatalog());
    console.log("current database:", await spark.catalog.currentDatabase());

    await spark.catalog.listTables().show();
    await spark.catalog.listColumns("example_people").show();

    console.log("view exists:", await spark.catalog.tableExists("example_people"));
    console.log("dropped:", await spark.catalog.dropTempView("example_people"));
    console.log("view exists:", await spark.catalog.tableExists("example_people"));
});
