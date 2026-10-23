import { inspect } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession } from "../src";
import { assertNoConnectionParameters } from "../src/client/sparkClient";
import { printableAddress, redactForTelemetry } from "../src/client/telemetry";

// Credentials must never reach output a person or a log collector can read:
// error messages, printed objects, JSON and telemetry.
const SECRET = "FAKE-SECRET-1234";
const leaks = (text: string) => text.includes(SECRET);

afterEach(() => {
    vi.unstubAllEnvs();
});

describe("connection strings with parameters", () => {
    it("are rejected without repeating the URL", () => {
        const url = `sc://spark.example:443/;use_ssl=true;x-aws-proxy-auth=${SECRET}`;
        const error = (() => {
            try {
                SparkSession.builder().config("spark.connect.url", url).getOrCreate();
            } catch (caught) {
                return caught as Error;
            }
            throw new Error("getOrCreate() accepted a URL with parameters");
        })();
        expect(error).toBeInstanceOf(TypeError);
        expect(error.message).toMatch(/URL parameters \(after ';'\) are not supported/);
        expect(error.message).toContain('{ type: "provider", provider: async () => ({ headers: { "x-aws-proxy-auth": token } }) }');
        expect(leaks(error.message) || leaks(String(error.stack))).toBe(false);

        expect(() => assertNoConnectionParameters("sc://spark.example:15002")).not.toThrow();
    });

    it("are rejected when they come from SPARK_CONNECT_URL, before any connection", async () => {
        vi.stubEnv("SPARK_CONNECT_URL", `sc://127.0.0.1:1/;token=${SECRET}`);
        const spark = SparkSession.builder().getOrCreate();
        const error = await spark.version().then(
            () => { throw new Error("version() resolved"); },
            (caught: unknown) => caught as Error,
        );
        expect(error.message).toMatch(/URL parameters \(after ';'\) are not supported/);
        expect(leaks(error.message) || leaks(String(error.stack))).toBe(false);
        await spark.close();
    });
});

describe("printed sessions and settings", () => {
    it("show the address but no credentials or configuration values", async () => {
        const builder = SparkSession.builder()
            .config("spark.connect.url", "scs://spark.example:443")
            .config("spark.hadoop.fs.s3a.secret.key", SECRET)
            .withAuth({ type: "token", token: SECRET });
        expect(inspect(builder)).toBe("SparkSessionBuilder { address: 'scs://spark.example:443' }");
        expect(inspect(SparkSession.builder())).toBe("SparkSessionBuilder { address: undefined }");

        const spark = builder.getOrCreate();
        const printed = inspect(spark, { depth: 10 });
        expect(printed).toMatch(/^SparkSession \{\s+sessionId: '[0-9a-f-]{36}',\s+address: 'scs:\/\/spark\.example:443'\s+\}$/);
        expect(leaks(printed)).toBe(false);

        const config = spark.getConnectionConfig();
        expect(config.auth).toEqual({ type: "token", token: SECRET });
        expect(leaks(inspect(config, { depth: 10 }))).toBe(false);
        expect(leaks(JSON.stringify(config))).toBe(false);
        expect(JSON.parse(JSON.stringify(config))).toMatchObject({
            address: "scs://spark.example:443",
            auth: "[REDACTED]",
            sessionConfig: { "spark.hadoop.fs.s3a.secret.key": "[REDACTED]" },
        });
        // The redaction members are not enumerable: copies stay plain settings.
        expect(Object.keys(config)).not.toContain("toJSON");
        await spark.close();
    });

    it("cut addresses before their parameters", () => {
        expect(printableAddress(`sc://spark.example/;token=${SECRET}`)).toBe("sc://spark.example/");
        expect(printableAddress(undefined)).toBeUndefined();
    });
});

describe("telemetry", () => {
    it("redacts tokens in connection-string parameters", () => {
        expect(redactForTelemetry(`sc://h:443/;use_ssl=true;x-aws-proxy-auth=${SECRET};token=${SECRET}`))
            .toBe("sc://h:443/;use_ssl=true;x-aws-proxy-auth=[REDACTED];token=[REDACTED]");
    });
});
