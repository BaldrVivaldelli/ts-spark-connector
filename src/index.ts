import { spark } from "./spark/session";
import { col } from "./engine/column";

(async () => {
    const df = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    df
        .select("name", "age", "country")
        .filter(col("name").eq("Alice").or(col("country").eq("Argentina")))
        .withColumn("is_adult", col("age").gte(18))
        .withColumn("greeting", col("name").eq("Alice").alias("greeting")) // solo a modo demostrativo
        .show();

    const rows = await df
        .select("name", "age")
        .withColumn("is_senior", col("age").gt(65))
        .collect();

    console.log("Collected Rows:", rows);
})();
