// Main entry point - export public API
export { SparkSession } from "./client/session";
export {
    col, isNull, isNotNull, when, lit,
    explode, posexplode, split, to_json, from_json, struct
} from "./engine/column";

// Example usage (commented out for production, uncomment for development/examples)
/*
import {col, from_json, isNotNull, isNull, lit, posexplode, split, struct, to_json, when} from "./engine/column";
import {SparkSession} from "./client/session";

(async () => {
    const session = SparkSession.builder()
        .withAuth({type: "token", token: "my-token"}) // opcional
        .enableTLS({
            keyStorePath: "./spark-server/certs/keystore.p12",
            keyStorePassword: "password",
            trustStorePath: "./spark-server/certs/cert.crt",
            trustStorePassword: "password"
        })
        .getOrCreate();

    const people = session.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    const purchases = session.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");




    await purchases
        .withColumn("tags_arr", split(col("tags"), lit(",")))  // dividir en array
        .select(
            col("user_id"),
            posexplode(col("tags_arr"))
        )
        .show();


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
        .saveAsTable("purchases_bucketed");
    const topSpenders = purchases
        .groupBy("user_id")
        .agg({total_spent: "sum(amount)"})
        .orderBy(col("total_spent").descNullsLast());

    await topSpenders.write
        .parquet()
        .mode("overwrite")
        .save("/data/dest/top_spenders");
    await purchases.write
        .orc()
        .mode("overwrite")
        .save("/data/dest/purchases_orc");
    await purchases.write
        .avro()
        .option("compression", "snappy")  // ejemplo de opción específica
        .mode("append")
        .save("/data/dest/purchases_avro");
    await people
        .describe(["id", "name", "age", "country", "year"])
        .show();
    await people
        .summary(["count", "min", "50%", "75%", "max"], ["age"])
        .show();
    await purchases
        .withColumn("jsonified", to_json(struct(col("product")))) //TODO: en una version futura voy a hacerlo sintacticamente mejor
        .withColumn("parsed", from_json(col("jsonified"), "struct<product:string>")) //TODO: en una version futura voy a hacerlo sintacticamente mejor
        .select("user_id", "product", "jsonified", "parsed")
        .limit(5)
        .show();
    await purchases
        .coalescePartitions(3)        // <-- 🧊 reduce particiones sin shuffle
        .select("tags", "product") // <-- 🧹 selecciona columnas deseadas
        .limit(5)
        .show();
    await purchases
        .repartition(4) // cambia el número de particiones a 4 (con shuffle)
        .select("tags", "product")
        .limit(5)
        .show();
    session.sql("SELECT * FROM purchases_tbl").show();

    const clicks =
        session.readStream<any, any, any>("rate", { rowsPerSecond: "2" })
            .select("value", "timestamp");

    const clickPurchases =
        clicks
            .join(
                purchases.select("user_id", "product", "amount"),
                col("value").eq(col("user_id")), // ajustá si tu columna stream != user_id
                "LEFT"
            )
            .groupBy("value")
            .agg({
                last_seen: "max(timestamp)",
                spent: "sum(amount)"
            });


})();
*/
