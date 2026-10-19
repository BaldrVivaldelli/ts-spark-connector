import { afterAll, describe, expect, it } from "vitest";
import { SparkSession, UnsupportedServerVersionError } from "../src";

const connectUrl = process.env.SPARK_CONNECT_URL ?? "scs://localhost:15002";
const caPath = process.env.SPARK_TLS_CA ?? "./spark-server/certs/ca.crt";
const serverName = process.env.SPARK_TLS_SERVER_NAME ?? "spark-connect";
/** Set by the CI matrix and docker-compose.test.yml; absent for ad-hoc runs. */
const expectedVersion = process.env.SPARK_VERSION;

function connectedBuilder() {
    const builder = SparkSession.builder().config("spark.connect.url", connectUrl);
    if (connectUrl.startsWith("scs://")) {
        builder.enableTLS({ trustStorePath: caPath, serverNameOverride: serverName });
    }
    return builder;
}

const session = connectedBuilder().getOrCreate();
afterAll(async () => session.close());

function atLeast(version: string, major: number, minor: number): boolean {
    const [maj, min] = version.split(".").map(Number);
    return maj! > major || (maj === major && min! >= minor);
}

describe("server version and version-gated features (E2E)", () => {
    it("reports the server's Spark version", async () => {
        const version = await session.version();
        expect(version).toMatch(/^\d+\.\d+\.\d+/);
        if (expectedVersion) expect(version).toBe(expectedVersion);
    });

    it("clones a session on 4.1+ and refuses older servers before any RPC", async () => {
        const version = await session.version();
        if (!atLeast(version, 4, 1)) {
            const error = await session.cloneSession().catch(e => e);
            expect(error).toBeInstanceOf(UnsupportedServerVersionError);
            expect(error).toMatchObject({ requiredVersion: "4.1.0", serverVersion: version });
            return;
        }

        await session.conf.set("spark.sql.shuffle.partitions", 7);
        await session.sql("CREATE OR REPLACE TEMP VIEW clone_source AS SELECT 1 AS id").collect();

        const clone = await session.cloneSession();
        try {
            expect(clone.getSessionId()).not.toBe(session.getSessionId());
            // State copied at clone time ...
            expect(await clone.sql("SELECT id FROM clone_source").collect()).toEqual([{ id: 1 }]);
            await expect(clone.conf.get("spark.sql.shuffle.partitions")).resolves.toBe("7");

            // ... and independent afterwards, in both directions.
            await clone.sql("DROP VIEW clone_source").collect();
            await clone.conf.set("spark.sql.shuffle.partitions", 11);
            expect(await session.sql("SELECT id FROM clone_source").collect()).toEqual([{ id: 1 }]);
            await expect(session.conf.get("spark.sql.shuffle.partitions")).resolves.toBe("7");
            await session.sql("CREATE OR REPLACE TEMP VIEW only_source AS SELECT 2 AS id").collect();
            await expect(clone.catalog.tableExists("only_source")).resolves.toBe(false);
        } finally {
            await clone.close();
            await session.conf.unset("spark.sql.shuffle.partitions");
        }
    });
});
