import { col } from "ts-spark-connector";
import { withExampleSession } from "./_session";

void withExampleSession(async spark => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/purchases.tsv");

    await purchases
        .groupBy("user_id")
        .agg({ total_spent: "sum(amount)" })
        .orderBy(col("total_spent").descNullsLast())
        .limit(10)
        .show();
});
