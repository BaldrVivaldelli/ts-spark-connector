import { col } from "ts-spark-connector";
import { withExampleSession } from "./_session.js";

void withExampleSession(async spark => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/purchases.tsv");

    const p2024 = purchases.filter(col("year").eq(2024));
    const p2025 = purchases.filter(col("year").eq(2025));

    await p2024.union(p2025).limit(5).show();
});
