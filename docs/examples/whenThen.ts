import { createSparkSession } from "../../src/client/session";
import { col } from "../../src/engine/column";

async function main() {
    const spark = createSparkSession("example-withcolumn-session");

    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    const result = people
        .withColumn("is_adult", col("age").gte(18))
        .withColumn("greeting", col("name").eq("Alice").alias("greeting_flag"))
        .select("name", "age", "is_adult", "greeting");

    await result.show();
}

console.log(main())