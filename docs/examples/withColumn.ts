import {createSparkSession} from "../../src/client/session";
import {col, when} from "../../src/engine/column";

async function main() {
    const spark = createSparkSession("example-withcolumn-session");

    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    const purchasesWithCategory = purchases.withColumn(
        "spending_category",
        when(col("amount").gt(1000), "VIP")
            .when(col("amount").gt(500), "Premium")
            .when(col("amount").gt(100), "Regular")
            .otherwise("Low")
    );

    await purchasesWithCategory.show();
}

console.log(main())