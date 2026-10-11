import { withExampleSession } from "./_session.js";
import { col, schema } from "ts-spark-connector";

const Purchases = schema({
    id: "long",
    product: "string",
    amount: "double",
});

const path = "/data/dest/examples/delta_purchases";

void withExampleSession(async spark => {
    const purchases = spark.createDataFrame(
        [
            { id: 1n, product: "keyboard", amount: 120.5 },
            { id: 2n, product: "mouse", amount: 45.0 },
        ],
        Purchases,
    );

    await purchases.write().format("delta").mode("overwrite").save(path);

    await spark.read
        .format("delta")
        .load(path)
        .filter(col("amount").gt(100))
        .show();
});
