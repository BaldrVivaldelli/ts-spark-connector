import { withExampleSession } from "./_session.js";

void withExampleSession(async spark => {
    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/people.tsv");

    await people
        .select("country")
        .distinct()
        .show();
});
