import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock, MockInstance } from "vitest";
import * as Athena from "@aws-sdk/client-athena";
import * as EmrServerless from "@aws-sdk/client-emr-serverless";
import * as Glue from "@aws-sdk/client-glue";
import { SparkSession } from "../src";
import { AwsSparkSessionError, athenaSession, emrServerlessSession, glueSession } from "../src/aws";
import {
    AWS_PROXY_AUTH_HEADER,
    DEFAULT_AWS_RETRY,
    ManagedSessionDriver,
    SessionStatus,
    connectAddress,
    loadSdk,
    openManagedSession,
    required,
} from "../src/aws/managedSession";
import type { SparkCredentialProvider } from "../src";

type Sent = { name: string; input: Record<string, unknown> };

const IN_ONE_HOUR = () => new Date(Date.now() + 3_600_000);
const ready: SessionStatus = { state: "IDLE", ready: true, ended: false };
const starting: SessionStatus = { state: "STARTING", ready: false, ended: false };

/** A driver whose operations are mocks; overrides are mocks as well. */
type FakeDriver = { service: string } & {
    [K in Exclude<keyof ManagedSessionDriver, "service">]: Mock<ManagedSessionDriver[K]>;
};

function fakeDriver(overrides: Partial<ManagedSessionDriver> = {}): FakeDriver {
    return {
        service: "Fake",
        start: vi.fn(async () => "s-1"),
        status: vi.fn(async () => ready),
        endpoint: vi.fn(async () => ({ url: "https://spark.example.aws", token: "t-1", expiresAt: IN_ONE_HOUR() })),
        terminate: vi.fn(async () => undefined),
        dispose: vi.fn(),
        ...overrides,
    } as FakeDriver;
}

function providerOf(spark: SparkSession): SparkCredentialProvider {
    const auth = spark.getConnectionConfig().auth as { type: string; provider: SparkCredentialProvider };
    expect(auth.type).toBe("provider");
    return auth.provider;
}

/** A client whose send() answers by command name and records what it was sent. */
function fakeClient(answers: Record<string, unknown | ((input: Record<string, unknown>) => unknown)>) {
    const sent: Sent[] = [];
    const client = {
        send: vi.fn(async (command: { constructor: { name: string }; input: Record<string, unknown> }) => {
            const name = command.constructor.name;
            sent.push({ name, input: command.input });
            const answer = answers[name];
            if (answer === undefined) throw new Error(`unexpected ${name}`);
            return typeof answer === "function" ? (answer as (input: Record<string, unknown>) => unknown)(command.input) : answer;
        }),
    };
    return { client, sent, names: () => sent.map(entry => entry.name) };
}

/** A gRPC UNAVAILABLE failure, as a proxy returns it while the session's server starts. */
const unavailable = () => Object.assign(new Error("upstream connect error"), { code: 14 });

// No Spark server answers here: the helpers' readiness probe gets a version
// unless a test says otherwise.
let version: MockInstance<SparkSession["version"]>;
beforeEach(() => {
    version = vi.spyOn(SparkSession.prototype, "version").mockResolvedValue("3.5.6");
});
afterEach(() => vi.restoreAllMocks());

describe("AWS session plumbing", () => {
    it("turns the endpoints the services report into TLS Spark Connect addresses", () => {
        expect(connectAddress("https://abc.emr-serverless.us-east-1.amazonaws.com")).toBe("scs://abc.emr-serverless.us-east-1.amazonaws.com:443");
        expect(connectAddress(" sc://glue.example/ ")).toBe("scs://glue.example:443");
        expect(connectAddress("spark.example:8443/;use_ssl=true")).toBe("scs://spark.example:8443");
        expect(() => connectAddress("://")).toThrow(/Unexpected Spark Connect endpoint/);
    });

    it("names the SDK package a helper needs when it is not installed", async () => {
        const missing = Object.assign(new Error("Cannot find module"), { code: "MODULE_NOT_FOUND" });
        await expect(loadSdk("@aws-sdk/client-glue", "glueSession()", () => Promise.reject(missing)))
            .rejects.toMatchObject({ message: "glueSession() needs @aws-sdk/client-glue; install it next to ts-spark-connector.", cause: missing });
        const esmMissing = Object.assign(new Error("Cannot find package"), { code: "ERR_MODULE_NOT_FOUND" });
        await expect(loadSdk("x", "x()", () => Promise.reject(esmMissing))).rejects.toThrow(/x\(\) needs x/);
        const broken = new Error("syntax error in the SDK");
        await expect(loadSdk("x", "x()", () => Promise.reject(broken))).rejects.toBe(broken);
        await expect(loadSdk("x", "x()", () => Promise.reject(null))).rejects.toBeNull();
        await expect(loadSdk("x", "x()", async () => "module")).resolves.toBe("module");
    });

    it("reports a response field the service left out", () => {
        expect(required("v", "Fake", "token")).toBe("v");
        for (const absent of [undefined, null, ""]) {
            expect(() => required(absent as string | undefined, "Fake", "token"))
                .toThrow(new AwsSparkSessionError("Fake returned no token.", { service: "Fake" }));
        }
    });
});

describe("managed session lifecycle", () => {
    it("starts, waits, connects with the session token and stops the session on close()", async () => {
        const driver = fakeDriver({
            status: vi.fn<(id: string) => Promise<SessionStatus>>()
                .mockResolvedValueOnce(starting)
                .mockResolvedValue(ready),
        });
        const { spark, sessionId } = await openManagedSession(driver, {
            pollIntervalMs: 1,
            configure: builder => builder.withRpcTimeout(1_234),
        });

        expect(sessionId).toBe("s-1");
        expect(driver.status).toHaveBeenCalledTimes(2);
        expect(spark.getConnectionConfig()).toMatchObject({
            address: "scs://spark.example.aws:443",
            rpcTimeoutMs: 1_234,
            retry: DEFAULT_AWS_RETRY,
        });
        const custom = await openManagedSession(fakeDriver(), { configure: builder => builder.withRetry({ maxRetries: 0 }) });
        expect(custom.spark.getConnectionConfig().retry).toEqual({ maxRetries: 0 });
        await custom.spark.close();

        const provider = providerOf(spark);
        const first = await provider();
        expect(first.headers).toEqual({ [AWS_PROXY_AUTH_HEADER]: "t-1" });
        expect(first.expiresAt).toBeGreaterThan(Date.now());
        expect(driver.endpoint).toHaveBeenCalledOnce();
        driver.endpoint.mockResolvedValueOnce({ url: "https://spark.example.aws", token: "t-2", expiresAt: IN_ONE_HOUR() });
        expect((await provider()).headers).toEqual({ [AWS_PROXY_AUTH_HEADER]: "t-2" });

        expect(driver.terminate).not.toHaveBeenCalled();
        await spark.close();
        expect(driver.terminate).toHaveBeenCalledWith("s-1");
        expect(driver.dispose).toHaveBeenCalledOnce();
    });

    it("fetches a new token when the one used to connect is about to expire, and passes tokens without expiry", async () => {
        const driver = fakeDriver({
            endpoint: vi.fn(async () => ({ url: "h", token: "t-old", expiresAt: new Date(Date.now() + 5_000) })),
        });
        const { spark } = await openManagedSession(driver, {});
        driver.endpoint.mockResolvedValueOnce({ url: "h", token: "t-new" });
        expect(await providerOf(spark)()).toEqual({ headers: { [AWS_PROXY_AUTH_HEADER]: "t-new" } });

        const noExpiry = fakeDriver({ endpoint: vi.fn(async () => ({ url: "h", token: "t-1" })) });
        const attached = await openManagedSession(noExpiry, { sessionId: "s-9" });
        expect(await providerOf(attached.spark)()).toEqual({ headers: { [AWS_PROXY_AUTH_HEADER]: "t-1" } });
        expect(noExpiry.endpoint).toHaveBeenCalledOnce();
    });

    it("attaches to a running session and leaves it running unless told otherwise", async () => {
        const driver = fakeDriver();
        const { spark, sessionId } = await openManagedSession(driver, { sessionId: "s-7" });
        expect(sessionId).toBe("s-7");
        expect(driver.start).not.toHaveBeenCalled();
        await spark.close();
        expect(driver.terminate).not.toHaveBeenCalled();
        expect(driver.dispose).toHaveBeenCalledOnce();

        const owned = fakeDriver();
        const again = await openManagedSession(owned, { sessionId: "s-7", terminateOnClose: true });
        await again.spark.close();
        expect(owned.terminate).toHaveBeenCalledWith("s-7");

        const kept = fakeDriver();
        await (await openManagedSession(kept, { terminateOnClose: false })).spark.close();
        expect(kept.terminate).not.toHaveBeenCalled();
    });

    it("validates its options before starting anything", async () => {
        const driver = fakeDriver();
        await expect(openManagedSession(driver, { startTimeoutMs: 0 })).rejects.toThrow(/startTimeoutMs must be a positive integer/);
        await expect(openManagedSession(driver, { pollIntervalMs: 1.5 })).rejects.toThrow(/pollIntervalMs must be a positive integer/);
        await expect(openManagedSession(driver, { sessionId: " " })).rejects.toThrow(/sessionId must be a non-empty string/);
        expect(driver.start).not.toHaveBeenCalled();
    });

    it("stops a session it started that ended while being waited for", async () => {
        const driver = fakeDriver({
            status: vi.fn(async () => ({ state: "FAILED", ready: false, ended: true, reason: "no capacity" })),
        });
        const error = await openManagedSession(driver, {}).catch(e => e);
        expect(error).toBeInstanceOf(AwsSparkSessionError);
        expect(error).toMatchObject({
            message: "Fake session s-1 is FAILED: no capacity",
            service: "Fake",
            sessionId: "s-1",
            state: "FAILED",
        });
        expect(driver.terminate).toHaveBeenCalledWith("s-1");
        expect(driver.dispose).toHaveBeenCalledOnce();

        const attached = fakeDriver({ status: vi.fn(async () => ({ state: "TERMINATED", ready: false, ended: true })) });
        await expect(openManagedSession(attached, { sessionId: "s-2" })).rejects.toThrow("Fake session s-2 is TERMINATED.");
        expect(attached.terminate).not.toHaveBeenCalled();
    });

    it("gives up after startTimeoutMs and on abort, stopping what it started", async () => {
        const slow = fakeDriver({ status: vi.fn(async () => starting) });
        await expect(openManagedSession(slow, { startTimeoutMs: 5, pollIntervalMs: 1 }))
            .rejects.toMatchObject({ message: "Fake session s-1 was not ready after 5 ms (state STARTING).", state: "STARTING" });
        expect(slow.terminate).toHaveBeenCalledOnce();

        const cancelled = new AbortController();
        cancelled.abort(new Error("caller gave up"));
        const early = fakeDriver();
        await expect(openManagedSession(early, { signal: cancelled.signal })).rejects.toThrow("caller gave up");
        expect(early.status).not.toHaveBeenCalled();
        expect(early.terminate).toHaveBeenCalledOnce();

        const midway = new AbortController();
        const waiting = fakeDriver({
            status: vi.fn(async () => {
                midway.abort();
                return starting;
            }),
        });
        await expect(openManagedSession(waiting, { signal: midway.signal, pollIntervalMs: 60_000 }))
            .rejects.toMatchObject({ name: "AbortError" });
        expect(waiting.terminate).toHaveBeenCalledOnce();
    });

    it("waits until the session's Spark Connect endpoint answers", async () => {
        version.mockRejectedValueOnce(unavailable()).mockRejectedValueOnce(unavailable());
        const driver = fakeDriver();
        const { spark } = await openManagedSession(driver, { pollIntervalMs: 1 });
        expect(version).toHaveBeenCalledTimes(3);
        expect(driver.terminate).not.toHaveBeenCalled();
        await spark.close();
        expect(driver.terminate).toHaveBeenCalledOnce();
    });

    it("gives up on an endpoint that keeps refusing clients, closing the client and stopping the session", async () => {
        const refused = unavailable();
        version.mockRejectedValue(refused);
        const close = vi.spyOn(SparkSession.prototype, "close");
        const driver = fakeDriver();
        const error = await openManagedSession(driver, { startTimeoutMs: 5, pollIntervalMs: 1 }).catch(e => e);
        expect(error).toBeInstanceOf(AwsSparkSessionError);
        expect(error).toMatchObject({
            message: "Fake session s-1 is ready but its Spark Connect endpoint did not answer within 5 ms.",
            sessionId: "s-1",
            cause: refused,
        });
        expect(close).toHaveBeenCalledOnce();
        expect(driver.terminate).toHaveBeenCalledWith("s-1");
        expect(driver.dispose).toHaveBeenCalledOnce();

        // A client that fails to close does not hide why the startup failed.
        close.mockRejectedValueOnce(new Error("channel already gone"));
        await expect(openManagedSession(fakeDriver(), { startTimeoutMs: 5, pollIntervalMs: 1 }))
            .rejects.toThrow(/did not answer within 5 ms/);
    });

    it("stops waiting for the endpoint on other errors and on abort", async () => {
        const denied = Object.assign(new Error("PERMISSION_DENIED"), { code: 7 });
        version.mockRejectedValueOnce(denied);
        const driver = fakeDriver();
        await expect(openManagedSession(driver, {})).rejects.toBe(denied);
        expect(driver.terminate).toHaveBeenCalledOnce();

        version.mockRejectedValueOnce(null);
        await expect(openManagedSession(fakeDriver(), {})).rejects.toBeNull();

        const midway = new AbortController();
        version.mockImplementation(async () => {
            midway.abort(new Error("caller gave up"));
            throw unavailable();
        });
        const waiting = fakeDriver();
        await expect(openManagedSession(waiting, { signal: midway.signal, pollIntervalMs: 60_000 }))
            .rejects.toMatchObject({ name: "AbortError" });
        expect(waiting.terminate).toHaveBeenCalledOnce();
    });

    it("says so when a session that failed to start cannot be stopped either", async () => {
        const failure = new Error("endpoint unavailable");
        const driver = fakeDriver({
            endpoint: vi.fn(async () => { throw failure; }),
            terminate: vi.fn(async () => { throw new Error("access denied"); }),
        });
        const error = await openManagedSession(driver, {}).catch(e => e);
        expect(error).toBeInstanceOf(AwsSparkSessionError);
        expect(error.message).toBe(
            "Fake session s-1 could not be stopped after it failed to start (access denied); it keeps running until its idle timeout.",
        );
        expect(error.cause).toBe(failure);
        expect(driver.dispose).toHaveBeenCalledOnce();

        const odd = fakeDriver({ terminate: vi.fn(async () => { throw "quota"; }) });
        await expect(openManagedSession(odd, { configure: () => { throw failure; } }))
            .rejects.toThrow(/could not be stopped after it failed to start \(quota\)/);
    });

    it("releases its resources when the session cannot even be started", async () => {
        const failure = new Error("throttled");
        const driver = fakeDriver({ start: vi.fn(async () => { throw failure; }) });
        await expect(openManagedSession(driver, {})).rejects.toBe(failure);
        expect(driver.dispose).toHaveBeenCalledOnce();
        expect(driver.terminate).not.toHaveBeenCalled();
    });

    it("rejects close() when stopping the session fails, after closing the client", async () => {
        const driver = fakeDriver({ terminate: vi.fn(async () => { throw new Error("still running"); }) });
        const { spark } = await openManagedSession(driver, {});
        await expect(spark.close()).rejects.toThrow("still running");
        expect(() => spark.read).toThrow(/closed/i);
        expect(driver.dispose).toHaveBeenCalledOnce();
        await expect(spark.close()).resolves.toBeUndefined();
        expect(driver.terminate).toHaveBeenCalledOnce();
    });
});

describe("emrServerlessSession", () => {
    it("drives the EMR Serverless session API", async () => {
        const { client, sent, names } = fakeClient({
            StartSessionCommand: { sessionId: "emr-1" },
            GetSessionCommand: { session: { state: "STARTED" } },
            GetSessionEndpointCommand: {
                endpoint: "https://emr-1.example.amazonaws.com",
                authToken: "emr-token",
                authTokenExpiresAt: IN_ONE_HOUR(),
            },
            TerminateSessionCommand: {},
        });
        const { spark, sessionId } = await emrServerlessSession({
            applicationId: "app-1",
            executionRoleArn: "arn:aws:iam::1:role/r",
            client,
            name: "nightly",
            idleTimeoutMinutes: 15,
            configurationOverrides: { runtimeConfiguration: [{ classification: "spark-defaults", properties: { "spark.x": "1" } }] },
            tags: { team: "data" },
        });
        expect(sessionId).toBe("emr-1");
        expect(spark.getConnectionConfig().address).toBe("scs://emr-1.example.amazonaws.com:443");
        expect((await providerOf(spark)()).headers).toEqual({ [AWS_PROXY_AUTH_HEADER]: "emr-token" });
        await spark.close();

        expect(names()).toEqual(["StartSessionCommand", "GetSessionCommand", "GetSessionEndpointCommand", "TerminateSessionCommand"]);
        expect(sent[0]!.input).toEqual({
            applicationId: "app-1",
            executionRoleArn: "arn:aws:iam::1:role/r",
            name: "nightly",
            idleTimeoutMinutes: 15,
            configurationOverrides: { runtimeConfiguration: [{ classification: "spark-defaults", properties: { "spark.x": "1" } }] },
            tags: { team: "data" },
        });
        for (const entry of sent.slice(1)) expect(entry.input).toEqual({ applicationId: "app-1", sessionId: "emr-1" });
    });

    it("classifies EMR Serverless session states", async () => {
        const failed = fakeClient({
            StartSessionCommand: { sessionId: "emr-2" },
            GetSessionCommand: { session: { state: "FAILED", stateDetails: "role cannot be assumed" } },
            TerminateSessionCommand: {},
        });
        await expect(emrServerlessSession({ applicationId: "app", executionRoleArn: "r", client: failed.client }))
            .rejects.toThrow("EMR Serverless session emr-2 is FAILED: role cannot be assumed");

        const unknown = fakeClient({ GetSessionCommand: {} });
        await expect(emrServerlessSession({ applicationId: "app", sessionId: "s", client: unknown.client, startTimeoutMs: 1 }))
            .rejects.toThrow("(state UNKNOWN)");

        const noId = fakeClient({ StartSessionCommand: {} });
        await expect(emrServerlessSession({ applicationId: "app", executionRoleArn: "r", client: noId.client }))
            .rejects.toThrow("EMR Serverless returned no session id.");
    });

    it("needs the role only to start a session, and creates its own client otherwise", async () => {
        await expect(emrServerlessSession({ applicationId: "app" } as never)).rejects.toThrow(/executionRoleArn must be a non-empty string/);
        await expect(emrServerlessSession({ executionRoleArn: "r" } as never)).rejects.toThrow(/applicationId must be a non-empty string/);

        const regions: unknown[] = [];
        vi.spyOn(EmrServerless.EMRServerlessClient.prototype, "send").mockImplementation(async function (this: EmrServerless.EMRServerlessClient) {
            regions.push(await this.config.region());
            throw new Error("offline");
        });
        const destroy = vi.spyOn(EmrServerless.EMRServerlessClient.prototype, "destroy");
        await expect(emrServerlessSession({ applicationId: "app", sessionId: "s", region: "eu-west-1" })).rejects.toThrow("offline");
        vi.stubEnv("AWS_REGION", "sa-east-1");
        await expect(emrServerlessSession({ applicationId: "app", sessionId: "s" })).rejects.toThrow("offline");
        vi.unstubAllEnvs();
        expect(regions).toEqual(["eu-west-1", "sa-east-1"]);
        expect(destroy).toHaveBeenCalledTimes(2);
    });
});

describe("glueSession", () => {
    it("creates a SPARK_CONNECT session and stops it on close()", async () => {
        const { client, sent, names } = fakeClient({
            CreateSessionCommand: (input: Record<string, unknown>) => ({ Session: { Id: input.Id } }),
            GetSessionCommand: { Session: { Status: "READY" } },
            GetSessionEndpointCommand: {
                SparkConnect: { Url: "sc://glue-1.example", AuthToken: "glue-token", AuthTokenExpirationTime: IN_ONE_HOUR() },
            },
            StopSessionCommand: {},
        });
        const { spark, sessionId } = await glueSession({ role: "arn:aws:iam::1:role/glue", client });
        expect(sessionId).toMatch(/^ts-spark-connector-[0-9a-f-]{36}$/);
        expect(spark.getConnectionConfig().address).toBe("scs://glue-1.example:443");
        expect((await providerOf(spark)()).headers).toEqual({ [AWS_PROXY_AUTH_HEADER]: "glue-token" });
        await spark.close();

        expect(names()).toEqual(["CreateSessionCommand", "GetSessionCommand", "GetSessionEndpointCommand", "StopSessionCommand"]);
        expect(sent[0]!.input).toEqual({
            Id: sessionId,
            Role: "arn:aws:iam::1:role/glue",
            Command: { Name: "glueetl" },
            GlueVersion: "5.1",
            SessionType: "SPARK_CONNECT",
            DefaultArguments: { "--language": "python" },
        });
        expect(sent[1]!.input).toEqual({ Id: sessionId });
        expect(sent[2]!.input).toEqual({ SessionId: sessionId });
        expect(sent[3]!.input).toEqual({ Id: sessionId });
    });

    it("forwards the session settings", async () => {
        const { client, sent } = fakeClient({
            CreateSessionCommand: { Session: { Id: "mine" } },
            GetSessionCommand: { Session: { Status: "READY" } },
            GetSessionEndpointCommand: { SparkConnect: { Url: "h", AuthToken: "t" } },
        });
        await glueSession({
            role: "r",
            id: "mine",
            glueVersion: "5.2",
            workerType: "G.1X",
            numberOfWorkers: 4,
            idleTimeoutMinutes: 30,
            timeoutMinutes: 120,
            defaultArguments: { "--language": "scala", "--enable-glue-datacatalog": "true" },
            connections: ["vpc"],
            securityConfiguration: "sec",
            description: "etl",
            tags: { team: "data" },
            client,
            terminateOnClose: false,
        });
        expect(sent[0]!.input).toMatchObject({
            Id: "mine",
            GlueVersion: "5.2",
            WorkerType: "G.1X",
            NumberOfWorkers: 4,
            IdleTimeout: 30,
            Timeout: 120,
            DefaultArguments: { "--language": "scala", "--enable-glue-datacatalog": "true" },
            Connections: { Connections: ["vpc"] },
            SecurityConfiguration: "sec",
            Description: "etl",
            Tags: { team: "data" },
        });
    });

    it("reports Glue failures and missing fields", async () => {
        await expect(glueSession({})).rejects.toThrow(/role must be a non-empty string/);
        const failed = fakeClient({
            CreateSessionCommand: { Session: { Id: "g" } },
            GetSessionCommand: { Session: { Status: "TIMEOUT", ErrorMessage: "provisioning took too long" } },
            StopSessionCommand: {},
        });
        await expect(glueSession({ role: "r", client: failed.client })).rejects.toThrow("AWS Glue session g is TIMEOUT: provisioning took too long");
        const unknown = fakeClient({ GetSessionCommand: {} });
        await expect(glueSession({ sessionId: "g", client: unknown.client, startTimeoutMs: 1 })).rejects.toThrow("(state UNKNOWN)");
        const noEndpoint = fakeClient({ GetSessionCommand: { Session: { Status: "READY" } }, GetSessionEndpointCommand: {} });
        await expect(glueSession({ sessionId: "g", client: noEndpoint.client })).rejects.toThrow("AWS Glue returned no Spark Connect endpoint.");
        const noId = fakeClient({ CreateSessionCommand: {} });
        await expect(glueSession({ role: "r", client: noId.client })).rejects.toThrow("AWS Glue returned no session id.");

        const destroy = vi.spyOn(Glue.GlueClient.prototype, "destroy");
        vi.spyOn(Glue.GlueClient.prototype, "send").mockRejectedValue(new Error("offline"));
        await expect(glueSession({ sessionId: "g", region: "us-east-2" })).rejects.toThrow("offline");
        await expect(glueSession({ sessionId: "g" })).rejects.toThrow("offline");
        expect(destroy).toHaveBeenCalledTimes(2);
    });
});

describe("athenaSession", () => {
    it("starts an Athena Spark session and terminates it on close()", async () => {
        const { client, sent, names } = fakeClient({
            StartSessionCommand: { SessionId: "ath-1" },
            GetSessionStatusCommand: { Status: { State: "IDLE" } },
            GetSessionEndpointCommand: { EndpointUrl: "https://ath-1.example", AuthToken: "ath-token", AuthTokenExpirationTime: IN_ONE_HOUR() },
            TerminateSessionCommand: {},
        });
        const { spark, sessionId } = await athenaSession({ workGroup: "spark", client, tags: { team: "data" } });
        expect(sessionId).toBe("ath-1");
        expect(spark.getConnectionConfig().address).toBe("scs://ath-1.example:443");
        expect((await providerOf(spark)()).headers).toEqual({ [AWS_PROXY_AUTH_HEADER]: "ath-token" });
        await spark.close();

        expect(names()).toEqual(["StartSessionCommand", "GetSessionStatusCommand", "GetSessionEndpointCommand", "TerminateSessionCommand"]);
        expect(sent[0]!.input).toMatchObject({
            WorkGroup: "spark",
            EngineConfiguration: {},
            Tags: [{ Key: "team", Value: "data" }],
        });
        for (const entry of sent.slice(1)) expect(entry.input).toEqual({ SessionId: "ath-1" });
    });

    it("forwards the engine configuration and reports Athena failures", async () => {
        const configured = fakeClient({
            StartSessionCommand: { SessionId: "a" },
            GetSessionStatusCommand: { Status: { State: "BUSY" } },
            GetSessionEndpointCommand: { EndpointUrl: "h", AuthToken: "t" },
        });
        await athenaSession({
            workGroup: "spark",
            engineConfiguration: { MaxConcurrentDpus: 20, SparkProperties: { "spark.x": "1" } },
            executionRole: "role",
            idleTimeoutMinutes: 20,
            description: "adhoc",
            client: configured.client,
            terminateOnClose: false,
        });
        expect(configured.sent[0]!.input).toEqual({
            WorkGroup: "spark",
            EngineConfiguration: { MaxConcurrentDpus: 20, SparkProperties: { "spark.x": "1" } },
            ExecutionRole: "role",
            SessionIdleTimeoutInMinutes: 20,
            Description: "adhoc",
        });

        await expect(athenaSession({})).rejects.toThrow(/workGroup must be a non-empty string/);
        const failed = fakeClient({
            StartSessionCommand: { SessionId: "a" },
            GetSessionStatusCommand: { Status: { State: "FAILED", StateChangeReason: "workgroup has no Spark engine" } },
            TerminateSessionCommand: {},
        });
        await expect(athenaSession({ workGroup: "w", client: failed.client }))
            .rejects.toThrow("Amazon Athena session a is FAILED: workgroup has no Spark engine");
        const unknown = fakeClient({ GetSessionStatusCommand: {} });
        await expect(athenaSession({ sessionId: "a", client: unknown.client, startTimeoutMs: 1 })).rejects.toThrow("(state UNKNOWN)");
        const noId = fakeClient({ StartSessionCommand: {} });
        await expect(athenaSession({ workGroup: "w", client: noId.client })).rejects.toThrow("Amazon Athena returned no session id.");

        const destroy = vi.spyOn(Athena.AthenaClient.prototype, "destroy");
        vi.spyOn(Athena.AthenaClient.prototype, "send").mockRejectedValue(new Error("offline"));
        await expect(athenaSession({ sessionId: "a", region: "us-west-2" })).rejects.toThrow("offline");
        await expect(athenaSession({ sessionId: "a" })).rejects.toThrow("offline");
        expect(destroy).toHaveBeenCalledTimes(2);
    });
});

describe("SparkSession close hooks", () => {
    it("run once after the session closed, and the first failure rejects close()", async () => {
        const spark = SparkSession.builder().getOrCreate();
        const order: string[] = [];
        spark.addCloseHook(async () => {
            order.push(`first sees closed=${(() => { try { void spark.read; return false; } catch { return true; } })()}`);
            throw new Error("first failed");
        });
        spark.addCloseHook(async () => {
            order.push("second");
            throw new Error("second failed");
        });
        await expect(spark.close()).rejects.toThrow("first failed");
        expect(order).toEqual(["first sees closed=true", "second"]);
        await spark.close();
        expect(order).toHaveLength(2);
        expect(() => spark.addCloseHook(async () => undefined)).toThrow(/closed/i);
    });
});
