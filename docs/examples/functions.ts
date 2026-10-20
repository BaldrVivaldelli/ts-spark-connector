import { Window, col, expr, functions as F, lit } from "ts-spark-connector";
import { withExampleSession } from "./_session.js";

void withExampleSession(async spark => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/purchases.tsv");

    // Column methods, the PySpark-style library and higher-order functions.
    const enriched = purchases
        .withColumn("tag_list", F.split("tags", ","))
        .withColumn("is_electronics", F.array_contains("tag_list", "electronics"))
        .withColumn("shout_tags", F.transform("tag_list", tag => F.upper(tag)))
        .withColumn("price_band", col("amount").between(100, 1000).cast("string"))
        .withColumn("with_tax", expr("round(amount * 1.21D, 2)"));

    // Aggregations: the whole library is available inside groupBy().agg().
    const perUser = enriched
        .groupBy("user_id")
        .agg({
            orders: F.count("*"),
            spent: F.sum("amount"),
            products: F.array_sort(F.collect_list("product")),
            biggest: F.max_by("product", "amount"),
        });

    // Window functions: rank each purchase within its user.
    const ranked = enriched
        .select(
            col("user_id"),
            col("product"),
            col("amount"),
            F.row_number().over(Window.partitionBy("user_id").orderBy(col("amount").desc())).alias("rank"),
            F.sum("amount").over(Window.partitionBy("user_id")).alias("user_total"),
        )
        .filter("rank = 1");

    await perUser.orderBy("user_id").show();
    await ranked.orderBy("user_id").show();
    await enriched
        .select(col("product"), col("shout_tags"), col("price_band"), col("with_tax"), lit("EUR").alias("currency"))
        .show();
});
