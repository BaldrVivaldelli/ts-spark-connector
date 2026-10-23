// Starts an Amazon Athena for Apache Spark session, runs a SQL query and a
// small DataFrame pipeline, and terminates the session on close.
//
// Needs AWS credentials and these variables:
//   AWS_REGION
//   TS_SPARK_AWS_ATHENA_WORKGROUP (a workgroup with the Apache Spark 3.5 engine)
//
// It bills while the session runs. Type-checked in CI, never run there.
import { col, functions as F, schema } from "ts-spark-connector";
import { athenaSession } from "ts-spark-connector/aws";

type ExampleEnvironment = Record<string, string | undefined>;

function requireEnv(name: string): string {
    const env = (globalThis as { process?: { env?: ExampleEnvironment } }).process?.env ?? {};
    const value = env[name];
    if (!value) {
        throw new Error(`Set ${name} before running this example (see docs/examples/README.md).`);
    }
    return value;
}

const Sales = schema({
    country: "string",
    product: "string",
    amount: "double",
});

async function main(): Promise<void> {
    const region = requireEnv("AWS_REGION");
    const workGroup = requireEnv("TS_SPARK_AWS_ATHENA_WORKGROUP");

    const { spark, sessionId } = await athenaSession({
        workGroup,
        region,
        // Safety net in case this process dies before close() runs.
        idleTimeoutMinutes: 15,
    });
    try {
        console.log(`Connected to Athena session ${sessionId}`);

        await spark.sql("SELECT 1 AS one, current_timestamp() AS now").show();

        const sales = spark.createDataFrame(
            [
                { country: "AR", product: "keyboard", amount: 120.5 },
                { country: "AR", product: "mouse", amount: 45.0 },
                { country: "BR", product: "monitor", amount: 310.0 },
                { country: "BR", product: "keyboard", amount: 99.9 },
            ],
            Sales,
        );

        await sales
            .filter(col("amount").gt(50))
            .groupBy("country")
            .agg({ orders: F.count("*"), revenue: F.sum("amount") })
            .orderBy("country")
            .show();
    } finally {
        // Also terminates the Athena session, which ends its billing.
        await spark.close();
    }
}

void main();
