import { withExampleSession } from "./_session.js";
import { col, schema } from "ts-spark-connector";

const Purchases = schema({
    id: "long",
    product: "string",
    amount: "double",
    boughtAt: "timestamp?",
});

void withExampleSession(async spark => {
    const purchases = spark.createDataFrame(
        [
            { id: 1n, product: "keyboard", amount: 120.5, boughtAt: "2026-01-31T12:30:00.000Z" },
            { id: 2n, product: "mouse", amount: 45.0, boughtAt: null },
            { id: 3n, product: "monitor", amount: 310.0, boughtAt: "2026-02-01T09:00:00.000Z" },
        ],
        Purchases,
    );

    await purchases
        .filter(col("amount").gt(100))
        .select("product", "amount")
        .show();
});
