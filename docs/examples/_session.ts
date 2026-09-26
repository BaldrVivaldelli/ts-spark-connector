import { SparkSession } from "ts-spark-connector";

type ExampleEnvironment = Record<string, string | undefined>;

function environment(): ExampleEnvironment {
    return (globalThis as { process?: { env?: ExampleEnvironment } }).process?.env ?? {};
}

/** Creates the same environment-driven TLS session used by the Docker E2E gate. */
export function createExampleSession(): SparkSession {
    const env = environment();
    const connectUrl = env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
    const builder = SparkSession.builder().config("spark.connect.url", connectUrl);
    if (connectUrl.startsWith("scs://")) {
        builder.enableTLS({
            trustStorePath: env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt",
            serverNameOverride: env.SPARK_TLS_SERVER_NAME ?? "spark-connect",
        });
    }
    return builder.getOrCreate();
}

/** Runs an example and always releases its remote session and channel. */
export async function withExampleSession(run: (spark: SparkSession) => Promise<void>): Promise<void> {
    const spark = createExampleSession();
    try {
        await run(spark);
    } finally {
        await spark.close();
    }
}
