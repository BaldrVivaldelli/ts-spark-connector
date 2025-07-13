import { spark } from "./spark/session";
import { col } from "./engine/column";

(async () => {
    const df = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    const result = await df
        .select(
            col("name").alias("full_name"),
            col("age"),
            col("country"),
        )
        .withColumn(
            "net_income",
            col("age").gt(18).and(col("country").eq("Argentina"))
        )
        .show();
})();
