import { setTimeout as sleep } from "node:timers/promises";
import { describe, expect, it, vi } from "vitest";
import { AWS_PROXY_AUTH_HEADER, ManagedSessionDriver, openManagedSession } from "../src/aws/managedSession";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";

// The AWS helpers share one lifecycle: wait for the session, connect over TLS
// with a token in the x-aws-proxy-auth header, renew the token before it
// expires and stop the session on close(). The services themselves cannot run
// here, so a driver stands in for them and points at the E2E server, which
// ignores the header; the RPCs, the renewal and the close hook are real.
describe("AWS session lifecycle against Spark (E2E)", () => {
    it.runIf(connectUrl.startsWith("scs://"))(
        "runs queries through the token provider, renews the token and stops the session on close()",
        async () => {
            const tokens: string[] = [];
            const expiries: number[] = [];
            const driver: ManagedSessionDriver = {
                service: "Local",
                start: async () => "local-1",
                status: async () => ({ state: "IDLE", ready: true, ended: false }),
                // 40 s of validity leaves 10 s before the 30 s refresh window
                // opens: the token used to connect serves the first query,
                // and the first RPC after the window opens renews it.
                endpoint: async () => {
                    tokens.push(`token-${tokens.length + 1}`);
                    expiries.push(Date.now() + 40_000);
                    return { url: connectUrl, token: tokens.at(-1)!, expiresAt: new Date(expiries.at(-1)!) };
                },
                terminate: vi.fn(async () => undefined),
                dispose: vi.fn(),
            };
            const { spark, sessionId } = await openManagedSession(driver, {
                configure: builder => builder.enableTLS({ trustStorePath: caPath, serverNameOverride: serverName }),
            });
            expect(sessionId).toBe("local-1");
            expect(AWS_PROXY_AUTH_HEADER).toBe("x-aws-proxy-auth");

            expect(await spark.sql("SELECT 1 AS one").collect()).toEqual([{ one: 1 }]);
            expect(tokens).toEqual(["token-1"]);
            await sleep(expiries[0]! - 30_000 - Date.now() + 100);
            expect(await spark.range(5).count()).toBe(5n);
            expect(tokens).toEqual(["token-1", "token-2"]);

            await spark.close();
            expect(driver.terminate).toHaveBeenCalledWith("local-1");
            expect(driver.dispose).toHaveBeenCalledOnce();
        },
    );
});
