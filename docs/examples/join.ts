import { col } from "ts-spark-connector";
import { withExampleSession } from "./_session";

void withExampleSession(async spark => {
    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/people.tsv");

    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/purchases.tsv");

    const result = people
        .join(purchases, col("id").eq(col("user_id")))
        .select("name", "product", "amount")
        .filter(col("amount").gt(100));

    await result.show();
});
