import { col } from "ts-spark-connector";
import { withExampleSession } from "./_session";

void withExampleSession(async spark => {
    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/people.tsv");

    const result = people
        .withColumn("is_adult", col("age").gte(18))
        .withColumn("greeting", col("name").eq("Alice").alias("greeting_flag"))
        .select("name", "age", "is_adult", "greeting");

    await result.show();
});
