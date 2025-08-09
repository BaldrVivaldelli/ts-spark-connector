import {spark} from "../../src/spark/session";

(async () => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    purchases
        .dropDuplicates("user_id", "product")
        .show();
})();
