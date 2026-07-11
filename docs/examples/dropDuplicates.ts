import { withExampleSession } from "./_session";

void withExampleSession(async spark => {
    const purchases = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/purchases.tsv");

    await purchases
        .dropDuplicates("user_id", "product")
        .show();
});
