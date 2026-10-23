// Keeps one EMR Serverless session warm across invocations of a short-lived
// process such as an AWS Lambda function. The first invocation starts the
// session; later ones attach to it by id. Each invocation closes only its own
// connection, and idleTimeoutMinutes stops the session when traffic stops.
//
// Trade-off: queries after the first skip the session start-up wait, but the
// session bills until the idle timeout expires, even when nothing queries it.
// Pick a timeout that matches how often the function is called.
//
// Needs AWS credentials and these variables:
//   AWS_REGION
//   TS_SPARK_AWS_EMR_APPLICATION_ID
//   TS_SPARK_AWS_EMR_EXECUTION_ROLE_ARN
//
// Type-checked in CI, never run there.
import { emrServerlessSession } from "ts-spark-connector/aws";

type ExampleEnvironment = Record<string, string | undefined>;

function requireEnv(name: string): string {
    const env = (globalThis as { process?: { env?: ExampleEnvironment } }).process?.env ?? {};
    const value = env[name];
    if (!value) {
        throw new Error(`Set ${name} before running this example (see docs/examples/README.md).`);
    }
    return value;
}

// Module scope survives between invocations while the runtime stays warm.
let warmSessionId: string | undefined;

async function connect() {
    const options = {
        applicationId: requireEnv("TS_SPARK_AWS_EMR_APPLICATION_ID"),
        executionRoleArn: requireEnv("TS_SPARK_AWS_EMR_EXECUTION_ROLE_ARN"),
        region: requireEnv("AWS_REGION"),
        // The session outlives close(); this timeout is what stops it.
        idleTimeoutMinutes: 10,
        terminateOnClose: false,
    };
    if (warmSessionId !== undefined) {
        try {
            return (await emrServerlessSession({ ...options, sessionId: warmSessionId })).spark;
        } catch {
            // The session hit its idle timeout or failed: start a new one.
            // A real error (credentials, permissions) fails the start too.
            warmSessionId = undefined;
        }
    }
    const connection = await emrServerlessSession(options);
    warmSessionId = connection.sessionId;
    return connection.spark;
}

/** Lambda-style entry point: counts the rows of a generated range. */
export async function handler(): Promise<{ sessionId: string | undefined; count: number }> {
    const spark = await connect();
    try {
        const count = await spark.sql("SELECT id FROM range(100)").count();
        return { sessionId: warmSessionId, count: Number(count) };
    } finally {
        // Closes this connection only; the AWS session keeps running.
        await spark.close();
    }
}
