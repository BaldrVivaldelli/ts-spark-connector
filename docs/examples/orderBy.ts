import {col} from "../../src/engine/column";
import {spark} from "../../src/spark/session";

(async () => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    purchases
        .groupBy("user_id")
        .agg({ total_spent: "sum(amount)" })
        .orderBy(col("total_spent").descNullsLast())
        .limit(10)
        .show();
})();
