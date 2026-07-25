import { afterAll, describe, expect, it } from "vitest";
import { SparkConnectError, SparkSession } from "../src";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";

function connectedBuilder() {
    const builder = SparkSession.builder().config("spark.connect.url", connectUrl);
    if (connectUrl.startsWith("scs://")) {
        builder.enableTLS({
            trustStorePath: caPath,
            serverNameOverride: serverName,
        });
    }
    return builder;
}

const session = connectedBuilder().getOrCreate();
afterAll(async () => session.close());

describe("production reliability (E2E)", () => {
    it("round-trips decimal, timestamp, array, struct and map values through Arrow", async () => {
        const rows = await session.sql(`
            SELECT
              CAST(123.45 AS DECIMAL(10, 2)) AS amount,
              TIMESTAMP '2024-01-02 03:04:05' AS instant,
              array(1, 2, 3) AS values,
              named_struct('count', 2, 'label', 'ok') AS nested,
              map('x', 7) AS labels
        `).collect() as Array<{
            amount: string;
            instant: string;
            values: number[];
            nested: { count: number; label: string };
            labels: Map<string, number>;
        }>;

        expect(rows).toHaveLength(1);
        expect(rows[0]).toMatchObject({
            amount: "123.45",
            instant: "2024-01-02T03:04:05.000Z",
            values: [1, 2, 3],
            nested: { count: 2, label: "ok" },
        });
        expect(rows[0]?.labels).toEqual(new Map([["x", 7]]));
    });

    it("preserves structured remote SQL failures", async () => {
        const operation = session.sql(
            "SELECT definitely_missing_column FROM VALUES (1)",
        ).collect();

        await expect(operation).rejects.toBeInstanceOf(SparkConnectError);
        await expect(operation).rejects.toMatchObject({
            operation: "ExecutePlan",
        });
    });

    it("honors a signal that is already aborted before an RPC", async () => {
        const controller = new AbortController();
        controller.abort("cancelled by E2E");
        const abortedSession = connectedBuilder()
            .withAbortSignal(controller.signal)
            .getOrCreate();

        await expect(abortedSession.sql("SELECT 1").collect()).rejects.toMatchObject({
            name: "AbortError",
        });
        await abortedSession.close().catch(() => undefined);
    });

    it.runIf(connectUrl.startsWith("scs://"))(
        "rejects a TLS server-name mismatch",
        async () => {
            const invalidTlsSession = SparkSession.builder()
                .config("spark.connect.url", connectUrl)
                .enableTLS({
                    trustStorePath: caPath,
                    serverNameOverride: "invalid.spark-connect.test",
                })
                .withRpcTimeout(5_000)
                .getOrCreate();

            await expect(invalidTlsSession.sql("SELECT 1").collect()).rejects.toThrow();
            await invalidTlsSession.close().catch(() => undefined);
        },
        30_000,
    );
});
