import { spark, col } from "../../src";

(async () => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    purchases
        .select("user_id", "amount")
        .sort("user_id", col("amount").ascNullsFirst())
        .limit(5)
        .show();
})();
