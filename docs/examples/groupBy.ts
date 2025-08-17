import { createSparkSession } from "../../src/client/session";

async function main() {
    const spark = createSparkSession("example-groupby-session");

    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    const result = purchases
        .groupBy("user_id")
        .agg({
            total_spent: "sum(amount)",
            purchases_count: "count(product)"
        });

    await result.show();
}

console.log(main())