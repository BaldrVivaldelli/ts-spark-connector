import {spark} from "./client/session";
import {coalesce, col, isNotNull, isNull, when} from "./engine/column";


(async () => {
    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

/*
    await people
        .join(purchases, col("id").eq(col("user_id")))
        .select("name", "product", "amount")
        .filter(col("amount").gt(100))
        .groupBy("name")
        .agg({
            total_spent: "sum(amount)",
            items_purchased: "count(product)"
        })
        .show();

    await purchases
        .select("user_id", "amount")
        .sort("user_id", col("amount").ascNullsFirst())
        .limit(5)
        .show();

    await purchases
        .groupBy("user_id")
        .agg({total_spent: "sum(amount)"})
        .orderBy(col("total_spent").descNullsLast())
        .limit(10)
        .show();
    await purchases
        .dropDuplicates("user_id", "product")
        .show();

    await people
        .select("country")
        .distinct()
        .show();
    await purchases
        .withColumnRenamed("user_id", "customer_id")
        .select("customer_id", "product", "amount")
        .limit(3)
        .show();

    const p2024 = purchases.filter(col("year").eq(2024));
    const p2025 = purchases.filter(col("year").eq(2025));
    await p2024.union(p2025).limit(5).show();

    await p2024.unionByName(p2025)
        .limit(5)
        .show();

    await purchases
        .withColumn("amount_x10", col("amount").gte(10))
        .select("user_id", "product", "amount_x10")
        .limit(3)
        .show();
    await purchases.withColumn(
        "spending_category",
        when(col("amount").gt(1000), "VIP")
            .when(col("amount").gt(500), "Premium")
            .when(col("amount").gt(100), "Regular")
            .otherwise("Low")
    ).show()

    await people
        .select("id", "name", "age")    // incluir 'age' si lo vas a usar en el filter
        .filter(col("age").isNull())
        .limit(5)
        .show();
    await people
        .select("id", "name", "age")    // incluir 'age' si lo vas a usar en el filter
        .filter(col("age").isNotNull())
        .limit(5)
        .show();


    await purchases
        .coalesce("year", "amount", 0)
        .select("user_id", "product", "amount")
        .limit(5)
        .show();
    await people
        .filter(isNull(col("age")))
        .select("id", "name", "age")
        .limit(5)
        .show();
    await people
        .filter(isNotNull(col("age")))
        .select("id", "name", "age")
        .limit(5)
        .show();

    await purchases
        .select("user_id", "product", "amount")
        .write
        .mode("overwrite")
        .save("/data/dest/purchases_summary");
*/

    await purchases.write.parquet().save("/data/dest/purchases_parquet");
    await purchases.write
        .csv()
        .option("header", true)
        .save("/data/dest/purchases_csv");
    await purchases.write.json().save("/data/dest/purchases_json");
    await purchases.write.mode("overwrite").saveAsTable("purchases_tbl");
    await purchases.write
        .partitionBy("year")
        .parquet()
        .mode("overwrite")
        .save("/data/dest/purchases_by_year");
    await purchases.write
        .bucketBy(2, "user_id")
        .sortBy("product")
        .parquet()
        .mode("overwrite")
        .saveAsTable ("purchases_bucketed");
    const topSpenders = purchases
        .groupBy("user_id")
        .agg({ total_spent: "sum(amount)" })
        .orderBy(col("total_spent").descNullsLast());

    await topSpenders.write
        .parquet()
        .mode("overwrite")
        .save("/data/dest/top_spenders");

})();
