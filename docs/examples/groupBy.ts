import { createSparkSession } from "../../src/spark/session";
import { col } from "../../src/engine/column";

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

    result.show();
}

main().catch(console.error);
