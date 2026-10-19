import { arrayType, decimalType, mapType, schema, structType } from "ts-spark-connector";
import { withExampleSession } from "./_session.js";

void withExampleSession(async spark => {
    // range(): one `id` column of longs, like spark.range in PySpark.
    console.log("range:", await spark.range(1, 10, 3).collect());

    // Local rows with nested values travel as an Arrow LocalRelation.
    const Orders = schema({
        id: "int",
        amount: decimalType(10, 2),
        tags: arrayType("string"),
        counts: mapType("string", "long"),
        addr: structType({ city: "string", zip: "int" }),
    });
    // Declare the rows before the call: TypeScript checks them against the
    // schema's inferred row type without re-deriving it from the literal.
    const rows = [
        { id: 1, amount: "1452.00", tags: ["new", "gift"], counts: new Map([["x", 7n]]), addr: { city: "Rosario", zip: 2000 } },
        { id: 2, amount: "-0.50", tags: [], counts: new Map(), addr: { city: null, zip: null } },
    ];
    const orders = spark.createDataFrame(rows, Orders);
    await orders.show();

    // A second, independent session against the same server: temporary views
    // created in one are invisible to the other.
    const sandbox = spark.newSession();
    try {
        await sandbox.sql("CREATE OR REPLACE TEMP VIEW scratch AS SELECT 1 AS id").collectRaw();
        console.log("sandbox sees scratch:", await sandbox.catalog.tableExists("scratch"));
        console.log("main sees scratch:", await spark.catalog.tableExists("scratch"));
    } finally {
        await sandbox.close();
    }

    // Catalog lookups and cache management.
    await spark.catalog.listFunctions(undefined, "upper").show();
    console.log("getFunction:", await spark.catalog.getFunction("upper"));
    console.log("getDatabase:", await spark.catalog.getDatabase("default"));

    const people = await spark.catalog.createTable("example_people_ext", {
        path: "/data/people.tsv",
        source: "csv",
        schema: schema({ id: "int", name: "string" }),
        options: { header: "true", delimiter: "\t" },
    });
    await people.show();
    await spark.catalog.cacheTable("example_people_ext");
    console.log("cached:", await spark.catalog.isCached("example_people_ext"));
    await spark.catalog.clearCache();
    console.log("cached after clearCache:", await spark.catalog.isCached("example_people_ext"));
    await spark.sql("DROP TABLE IF EXISTS example_people_ext").collectRaw();

    // Jars uploaded with addArtifact() join this session's classpath, so
    // Java UDFs from them can be registered and called from SQL:
    //   await spark.addArtifact("./udfs.jar");
    //   await spark.udf.registerJavaFunction("shout", "io.example.Shout", "string");
    //   await spark.sql("SELECT shout('hi')").show();
    console.log("server version:", await spark.version());
});
