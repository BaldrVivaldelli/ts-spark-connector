import { createSparkSession, col } from "../../src";

async function main() {
    const spark = createSparkSession();

    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/purchases.tsv");

    const result = people
        .join(purchases, col("id").eq(col("user_id")))
        .select("name", "product", "amount")
        .filter(col("amount").gt(100));

    await result.show();
}
console.log(main())