import { spark } from "./spark/session";

(async () => {
    const df = spark.read.option("delimiter", "\t").option("header", "true")
        .csv("/data/people.tsv");
    //Aca boludeo con filtrar y o obtener algo. Puedo hacerlo mejor al filter
    //const result = await df.filter(df.col("age").gt(30)).select("name", "country").show();
    const result = await df.show();
    console.log("Resultado:", result);
})();
