import { withExampleSession } from "./_session.js";
import { col, lit, schema } from "ts-spark-connector";

const Customers = schema({
    id: "long",
    name: "string",
    tier: "string",
});

const table = "default.example_customers";

void withExampleSession(async spark => {
    await spark.sql(`DROP TABLE IF EXISTS ${table}`).collectRaw();

    // DataFrameWriterV2: create a Delta table from local rows.
    await spark
        .createDataFrame(
            [
                { id: 1n, name: "ada", tier: "gold" },
                { id: 2n, name: "grace", tier: "silver" },
            ],
            Customers,
        )
        .writeTo(table)
        .using("delta")
        .create();

    // MERGE INTO: update matches, insert new rows, flag customers missing from the feed.
    await spark
        .createDataFrame(
            [
                { id: 2n, name: "grace", tier: "gold" },
                { id: 3n, name: "linus", tier: "bronze" },
            ],
            Customers,
        )
        .alias("feed")
        .mergeInto(table, col("example_customers.id").eq(col("feed.id")))
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .whenNotMatchedBySource().update({ tier: lit("inactive") })
        .merge();

    await spark.table(table).orderBy("id").show();
});
