import { col } from "ts-spark-connector";
import { withExampleSession } from "./_session";

void withExampleSession(async spark => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/purchases.tsv");

    await purchases
        .select("user_id", "amount")
        .sort("user_id", col("amount").ascNullsFirst())
        .limit(5)
        .show();
});
