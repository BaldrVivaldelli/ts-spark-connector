import { withExampleSession } from "./_session.js";

void withExampleSession(async spark => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/purchases.tsv");

    const result = purchases
        .groupBy("user_id")
        .agg({
            total_spent: "sum(amount)",
            purchases_count: "count(product)"
        });

    await result.show();
});
