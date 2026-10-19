import { withExampleSession } from "./_session.js";
import { Observation, col, schema, functions as F } from "ts-spark-connector";

const Sales = schema({ region: "string", product: "string", amount: "int" });

void withExampleSession(async spark => {
    const sales = spark.createDataFrame(
        [
            { region: "north", product: "apples", amount: 10 },
            { region: "north", product: "pears", amount: 4 },
            { region: "south", product: "apples", amount: 7 },
        ],
        Sales,
    );

    // Subtotals per region plus a grand total (the null region row).
    await sales.rollup("region").agg({ total: "sum(amount)" }).orderBy("region").show();

    // One column per product; Spark collects the distinct products first.
    await sales.groupBy("region").pivot("product").agg({ total: "sum(amount)" }).show();

    // Metrics computed while the action runs, without a second pass.
    const stats = new Observation("sales");
    const rows = await sales
        .observe(stats, F.count(F.lit(1)).alias("rows"), F.sum("amount").alias("total"))
        .collect();
    console.log(`${rows.length} rows, observed:`, await stats.get());

    // Materialize once, reuse many times.
    const checkpointed = await sales.localCheckpoint();
    console.log("apples:", await checkpointed.filter(col("product").eq("apples")).count());
    console.log("storage level:", (await checkpointed.storageLevel()).name);

    // Every row as a JSON document, and a reconciled schema with casts.
    await sales.toJSON().show(3, 0);
    await sales.to("amount STRING, region STRING").show();
});
