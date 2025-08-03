import { spark } from "./spark/session";
import { col } from "./engine/column";

(async () => {
    // Leemos el people.tsv (con columna id)
    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    // Leemos purchases.tsv (con columna user_id)
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    people
        .join(purchases, col("id").eq(col("user_id")))
        .select("name", "product", "amount")
        .filter(col("amount").gt(100))
        .groupBy("name")
        .agg({
            total_spent: "sum(amount)",
            items_purchased: "count(product)"
        })
        .show();
})();
