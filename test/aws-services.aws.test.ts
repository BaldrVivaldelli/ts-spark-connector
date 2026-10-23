import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import * as Athena from "@aws-sdk/client-athena";
import * as EmrServerless from "@aws-sdk/client-emr-serverless";
import * as Glue from "@aws-sdk/client-glue";
import { functions as F, schema } from "../src";
import { athenaSession, emrServerlessSession, glueSession } from "../src/aws";
import type { AwsSparkConnection } from "../src/aws";

// Opt-in suite that runs the three AWS helpers against real sessions of EMR
// Serverless, AWS Glue and Amazon Athena for Apache Spark. It is not part of
// `npm test`; run it with `npm run test:aws`.
//
// It needs AWS credentials, the resources of infra/aws-e2e (deploy the stack
// and read its outputs) and these variables. A service's tests run only when
// all of its variables are set; otherwise they skip.
//   AWS_REGION                              all services
//   TS_SPARK_AWS_EMR_APPLICATION_ID         EMR Serverless
//   TS_SPARK_AWS_EMR_EXECUTION_ROLE_ARN     EMR Serverless
//   TS_SPARK_AWS_GLUE_ROLE_ARN              Glue
//   TS_SPARK_AWS_ATHENA_WORKGROUP           Athena
//
// It bills while sessions run. Every session a test starts is stopped in
// afterEach even when the test fails, and the idle timeouts below are a second
// safety net.

const region = process.env.AWS_REGION ?? "";
const emrApplicationId = process.env.TS_SPARK_AWS_EMR_APPLICATION_ID ?? "";
const emrExecutionRoleArn = process.env.TS_SPARK_AWS_EMR_EXECUTION_ROLE_ARN ?? "";
const glueRoleArn = process.env.TS_SPARK_AWS_GLUE_ROLE_ARN ?? "";
const athenaWorkGroup = process.env.TS_SPARK_AWS_ATHENA_WORKGROUP ?? "";

const IDLE_TIMEOUT_MINUTES = 15;
const ENDED_POLL_MS = 5_000;
const ENDED_WAIT_MS = 5 * 60_000;

/** What the shared tests need from one service, besides its helper. */
type SessionService = {
    /** Starts a session through the helper. */
    start(): Promise<AwsSparkConnection>;
    /** Attaches to a session through the same helper, with no start options. */
    attach(sessionId: string, extra?: { startTimeoutMs?: number }): Promise<AwsSparkConnection>;
    /** Reads the session state straight from the SDK. */
    state(sessionId: string): Promise<string | undefined>;
    /** Stops the session through the SDK, bypassing the helper. */
    stop(sessionId: string): Promise<unknown>;
    isEnded(state: string | undefined): boolean;
    /** Destroys the SDK client created for state() and stop(). */
    destroy(): void;
};

function emrService(): SessionService {
    const client = new EmrServerless.EMRServerlessClient({ region });
    const ended = new Set(["TERMINATING", "TERMINATED", "FAILED"]);
    return {
        start: () => emrServerlessSession({
            applicationId: emrApplicationId,
            executionRoleArn: emrExecutionRoleArn,
            region,
            idleTimeoutMinutes: IDLE_TIMEOUT_MINUTES,
        }),
        attach: (sessionId, extra) => emrServerlessSession({ applicationId: emrApplicationId, region, sessionId, ...extra }),
        state: async sessionId => {
            const { session } = await client.send(
                new EmrServerless.GetSessionCommand({ applicationId: emrApplicationId, sessionId }),
            );
            return session?.state;
        },
        stop: sessionId => client.send(
            new EmrServerless.TerminateSessionCommand({ applicationId: emrApplicationId, sessionId }),
        ),
        isEnded: state => state !== undefined && ended.has(state),
        destroy: () => client.destroy(),
    };
}

function glueService(): SessionService {
    const client = new Glue.GlueClient({ region });
    const ended = new Set(["STOPPING", "STOPPED", "FAILED", "TIMEOUT"]);
    return {
        start: () => glueSession({
            role: glueRoleArn,
            region,
            idleTimeoutMinutes: IDLE_TIMEOUT_MINUTES,
            workerType: "G.1X",
            numberOfWorkers: 2,
        }),
        attach: (sessionId, extra) => glueSession({ region, sessionId, ...extra }),
        state: async sessionId => {
            const { Session: session } = await client.send(new Glue.GetSessionCommand({ Id: sessionId }));
            return session?.Status;
        },
        stop: sessionId => client.send(new Glue.StopSessionCommand({ Id: sessionId })),
        isEnded: state => state !== undefined && ended.has(state),
        destroy: () => client.destroy(),
    };
}

function athenaService(): SessionService {
    const client = new Athena.AthenaClient({ region });
    const ended = new Set(["TERMINATING", "TERMINATED", "FAILED"]);
    return {
        start: () => athenaSession({ workGroup: athenaWorkGroup, region, idleTimeoutMinutes: IDLE_TIMEOUT_MINUTES }),
        attach: (sessionId, extra) => athenaSession({ region, sessionId, ...extra }),
        state: async sessionId => {
            const { Status: status } = await client.send(new Athena.GetSessionStatusCommand({ SessionId: sessionId }));
            return status?.State;
        },
        stop: sessionId => client.send(new Athena.TerminateSessionCommand({ SessionId: sessionId })),
        isEnded: state => state !== undefined && ended.has(state),
        destroy: () => client.destroy(),
    };
}

const Sales = schema({ country: "string", product: "string", amount: "double" });

/** Registers the tests every service shares. Call it inside a describe. */
function defineServiceTests(create: () => SessionService): void {
    let service: SessionService;
    // Everything a test opened, so afterEach can clean up after a failure.
    let startedSessionIds: string[];
    let connections: AwsSparkConnection[];

    beforeEach(() => {
        service = create();
        startedSessionIds = [];
        connections = [];
    });

    afterEach(async () => {
        try {
            await Promise.allSettled(connections.map(connection => connection.spark.close()));
            for (const sessionId of startedSessionIds) {
                try {
                    if (service.isEnded(await service.state(sessionId))) continue;
                } catch {
                    // Unknown state: stop it anyway.
                }
                try {
                    await service.stop(sessionId);
                } catch {
                    // Nothing more to do; the idle timeout will end it.
                }
            }
        } finally {
            service.destroy();
        }
    });

    async function waitUntilEnded(sessionId: string): Promise<string | undefined> {
        const deadline = Date.now() + ENDED_WAIT_MS;
        let state = await service.state(sessionId);
        while (!service.isEnded(state) && Date.now() < deadline) {
            await sleep(ENDED_POLL_MS);
            state = await service.state(sessionId);
        }
        return state;
    }

    it("starts a session, runs queries and stops the session on close()", async () => {
        const first = await service.start();
        startedSessionIds.push(first.sessionId);
        connections.push(first);
        const { spark, sessionId } = first;

        expect(await spark.version()).toMatch(/^3\.5\./);
        expect(await spark.sql("SELECT 1 AS one").collect()).toEqual([{ one: 1 }]);
        expect(await spark.range(1000).count()).toBe(1000n);

        const sales = spark.createDataFrame(
            [
                { country: "AR", product: "keyboard", amount: 120.5 },
                { country: "AR", product: "mouse", amount: 45.0 },
                { country: "BR", product: "monitor", amount: 310.0 },
            ],
            Sales,
        );
        const totals = await sales
            .groupBy("country")
            .agg({ orders: F.count("*"), revenue: F.sum("amount") })
            .orderBy("country")
            .collect();
        expect(totals).toEqual([
            { country: "AR", orders: 2n, revenue: 165.5 },
            { country: "BR", orders: 1n, revenue: 310 },
        ]);

        // A second connection to the running session leaves it running on close().
        const second = await service.attach(sessionId);
        connections.push(second);
        expect(second.sessionId).toBe(sessionId);
        expect(await second.spark.sql("SELECT 2 AS two").collect()).toEqual([{ two: 2 }]);
        await second.spark.close();
        expect(service.isEnded(await service.state(sessionId))).toBe(false);

        // The first connection started the session, so its close() stops it.
        await spark.close();
        const finalState = await waitUntilEnded(sessionId);
        expect(service.isEnded(finalState), `session ${sessionId} is ${finalState}`).toBe(true);
    });

    it("rejects attaching to a session that does not exist", async () => {
        // Starts nothing, so it costs nothing.
        await expect(service.attach(randomUUID(), { startTimeoutMs: 30_000 })).rejects.toThrow();
    });
}

describe.runIf(region !== "" && emrApplicationId !== "" && emrExecutionRoleArn !== "")(
    "EMR Serverless session (real AWS)",
    () => defineServiceTests(emrService),
);

describe.runIf(region !== "" && glueRoleArn !== "")(
    "AWS Glue session (real AWS)",
    () => defineServiceTests(glueService),
);

describe.runIf(region !== "" && athenaWorkGroup !== "")(
    "Amazon Athena session (real AWS)",
    () => defineServiceTests(athenaService),
);
