import {spark} from "./spark/session";
import {col} from "./engine/column";

(async () => {
    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

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

    purchases
        .select("user_id", "amount")
        .sort("user_id", col("amount").ascNullsFirst())
        .limit(5)
        .show();

    purchases
        .groupBy("user_id")
        .agg({total_spent: "sum(amount)"})
        .orderBy(col("total_spent").descNullsLast())
        .limit(10)
        .show();
    purchases
        .dropDuplicates("user_id", "product")
        .show();

    people
        .select("country")
        .distinct()
        .show();
    purchases
        .withColumnRenamed("user_id", "customer_id")
        .select("customer_id", "product", "amount")
        .limit(3)
        .show();

    const p2024 = purchases.filter(col("year").eq(2024));
    const p2025 = purchases.filter(col("year").eq(2025));
    p2024.union(p2025).limit(5).show();

    p2024.unionByName(p2025) // mapea a set_op(UNION, is_all:false)
        .limit(5)
        .show();
})();
