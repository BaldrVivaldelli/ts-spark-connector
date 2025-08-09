import {spark} from "../../src/spark/session";
import {col} from "../../src/engine/column";

(async () => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    const p2024 = purchases.filter(col("year").eq(2024));
    const p2025 = purchases.filter(col("year").eq(2025));

    p2024.union(p2025).limit(5).show();
})();
