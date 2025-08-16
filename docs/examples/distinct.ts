import {spark} from "../../src/client/session";

(async () => {
    const people = spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .csv("/data/people.tsv");

    people
        .select("country")
        .distinct()
        .show();
})();