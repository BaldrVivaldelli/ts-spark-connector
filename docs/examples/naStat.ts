import { withExampleSession } from "./_session.js";
import { schema } from "ts-spark-connector";

const Readings = schema({
    sensor: "string",
    temperature: "double?",
    humidity: "double?",
});

void withExampleSession(async spark => {
    const readings = spark.createDataFrame(
        [
            { sensor: "a", temperature: 21.5, humidity: 40 },
            { sensor: "a", temperature: null, humidity: 42 },
            { sensor: "b", temperature: 25.0, humidity: null },
            { sensor: "b", temperature: 26.5, humidity: 55 },
        ],
        Readings,
    );

    // Null handling: fill per column, or drop incomplete rows.
    await readings.na.fill({ temperature: 0, humidity: 0 }).show();
    console.log("complete rows:", await readings.dropna().count());

    // Statistics over the filled data.
    const filled = readings.fillna(0);
    console.log("corr(temperature, humidity):", await filled.stat.corr("temperature", "humidity"));
    console.log("median temperature:", await filled.approxQuantile("temperature", [0.5], 0));
    await readings.crosstab("sensor", "humidity").show();

    // Long format: one row per sensor and measure.
    await readings.unpivot("sensor", ["temperature", "humidity"], "measure", "value").show();

    console.log(await readings.treeString());
});
