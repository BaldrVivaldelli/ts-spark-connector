import { setTimeout as sleep } from "timers/promises";
import { DEFAULT_REFRESH_SKEW_MS } from "../client/credentialProvider";
import { SparkSession } from "../client/session";
import type { SparkCredentialProvider, SparkSessionBuilder } from "../client/session";

/**
 * An AWS SDK v3 client of the service a helper drives, such as
 * `new EMRServerlessClient({ region })`. Only its `send()` is used.
 * @public
 */
export type AwsSdkClient = { send(command: never): Promise<unknown> };

/**
 * Options shared by the AWS helpers (`emrServerlessSession`, `glueSession`,
 * `athenaSession`).
 * @public
 */
export type AwsSparkSessionOptions = {
    /** Attach to this running session instead of starting a new one. */
    sessionId?: string;
    /**
     * Stop the AWS session when `spark.close()` runs, which ends its billing.
     * Defaults to `true` for a session this call started and `false` for one
     * it attached to.
     */
    terminateOnClose?: boolean;
    /**
     * How long to wait for the session to become ready and its Spark Connect
     * endpoint to answer; 10 minutes by default.
     */
    startTimeoutMs?: number;
    /** Delay between session status checks while waiting; 2 seconds by default. */
    pollIntervalMs?: number;
    /** Stops waiting for the session. A session this call started is terminated. */
    signal?: AbortSignal;
    /**
     * Further builder configuration (timeouts, logger, user, Spark configs),
     * applied before the session is created. The address and authentication
     * are already set, and transient failures are retried 3 times; call
     * `withRetry()` here to change that.
     */
    configure?: (builder: SparkSessionBuilder) => void;
};

/**
 * A Spark session connected to an AWS-managed Spark Connect endpoint.
 * @public
 */
export type AwsSparkConnection = {
    /**
     * The connected session. Its `close()` also stops the AWS session when
     * `terminateOnClose` applies.
     */
    readonly spark: SparkSession;
    /** Id of the AWS session, to attach to it again later with `sessionId`. */
    readonly sessionId: string;
};

/**
 * An AWS session that failed to start, ended while being waited for, did not
 * become ready in time, or could not be stopped after such a failure.
 * @public
 */
export class AwsSparkSessionError extends Error {
    /** The AWS service, e.g. `EMR Serverless`. */
    readonly service: string;
    /** The AWS session involved, once the service assigned one. */
    readonly sessionId?: string;
    /** The last session state the service reported. */
    readonly state?: string;

    constructor(
        message: string,
        options: { service: string; sessionId?: string; state?: string; cause?: unknown },
    ) {
        super(message, options.cause === undefined ? undefined : { cause: options.cause });
        this.name = "AwsSparkSessionError";
        this.service = options.service;
        this.sessionId = options.sessionId;
        this.state = options.state;
    }
}

/** @internal Where a session listens and the token that admits a client, as the service reports them. */
export type SessionEndpoint = { url: string; token: string; expiresAt?: Date };

/** @internal A session state, classified by the service's helper. */
export type SessionStatus = { state: string; ready: boolean; ended: boolean; reason?: string };

/** @internal What a helper tells the shared session lifecycle about its service. */
export type ManagedSessionDriver = {
    service: string;
    start(): Promise<string>;
    status(sessionId: string): Promise<SessionStatus>;
    endpoint(sessionId: string): Promise<SessionEndpoint>;
    terminate(sessionId: string): Promise<void>;
    /** Releases resources the helper created, such as its own SDK client. */
    dispose(): void;
};

export const DEFAULT_START_TIMEOUT_MS = 10 * 60_000;
export const DEFAULT_POLL_INTERVAL_MS = 2_000;
/** gRPC status the services' proxies return while the session's server does not accept clients yet. */
const GRPC_UNAVAILABLE = 14;
/**
 * Retries for transient failures. The services' proxies drop requests now and
 * then (Glue's once answered a running query with a plain-text 502); a query
 * then resumes through ReattachExecute instead of running again.
 */
export const DEFAULT_AWS_RETRY = { maxRetries: 3, initialBackoffMs: 500, maxBackoffMs: 5_000 } as const;
/** Header that carries the session token; PySpark puts the same name in its remote URL. */
export const AWS_PROXY_AUTH_HEADER = "x-aws-proxy-auth";

/** @internal Rejects a missing or blank required option. */
export function requireText(value: unknown, label: string): string {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${label} must be a non-empty string.`);
    }
    return value;
}

/** @internal A field of a service response that must be present. */
export function required<T>(value: T | undefined, service: string, field: string): T {
    if (value === undefined || value === null || value === "") {
        throw new AwsSparkSessionError(`${service} returned no ${field}.`, { service });
    }
    return value;
}

function positiveInteger(value: number | undefined, fallback: number, label: string): number {
    if (value === undefined) return fallback;
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw new RangeError(`${label} must be a positive integer.`);
    }
    return value;
}

/**
 * @internal The Spark Connect address of an endpoint URL. The services
 * report `https://host` or `sc://host` without a port; their endpoints
 * listen on 443 with TLS.
 */
export function connectAddress(endpoint: string): string {
    const match = /^(?:[a-z][a-z0-9+.-]*:\/\/)?([^/:;?#\s]+)(?::(\d+))?/i.exec(endpoint.trim());
    if (!match) throw new TypeError(`Unexpected Spark Connect endpoint ${JSON.stringify(endpoint)}.`);
    return `scs://${match[1]}:${match[2] ?? "443"}`;
}

/**
 * @internal Loads an optional AWS SDK client package, naming it when it is
 * not installed.
 */
export async function loadSdk<T>(packageName: string, helper: string, load: () => Promise<T>): Promise<T> {
    try {
        return await load();
    } catch (error) {
        const code = (error as { code?: unknown } | null)?.code;
        if (code === "MODULE_NOT_FOUND" || code === "ERR_MODULE_NOT_FOUND") {
            throw new Error(`${helper} needs ${packageName}; install it next to ts-spark-connector.`, { cause: error });
        }
        throw error;
    }
}

async function waitUntilReady(
    driver: ManagedSessionDriver,
    sessionId: string,
    deadline: number,
    timeoutMs: number,
    pollIntervalMs: number,
    signal?: AbortSignal,
): Promise<void> {
    for (;;) {
        signal?.throwIfAborted();
        const status = await driver.status(sessionId);
        if (status.ready) return;
        if (status.ended) {
            throw new AwsSparkSessionError(
                `${driver.service} session ${sessionId} is ${status.state}${status.reason ? `: ${status.reason}` : "."}`,
                { service: driver.service, sessionId, state: status.state },
            );
        }
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
            throw new AwsSparkSessionError(
                `${driver.service} session ${sessionId} was not ready after ${timeoutMs} ms (state ${status.state}).`,
                { service: driver.service, sessionId, state: status.state },
            );
        }
        await sleep(Math.min(pollIntervalMs, remaining), undefined, signal ? { signal } : undefined);
    }
}

/**
 * Waits until the session's Spark Connect server answers. A service can
 * report a session ready before its server accepts clients: Glue's proxy
 * answers UNAVAILABLE for about 25 seconds after the session is READY.
 */
async function waitUntilAnswering(
    driver: ManagedSessionDriver,
    sessionId: string,
    spark: SparkSession,
    deadline: number,
    timeoutMs: number,
    pollIntervalMs: number,
    signal?: AbortSignal,
): Promise<void> {
    for (;;) {
        signal?.throwIfAborted();
        try {
            await spark.version();
            return;
        } catch (error) {
            if ((error as { code?: unknown } | null)?.code !== GRPC_UNAVAILABLE) throw error;
            const remaining = deadline - Date.now();
            if (remaining <= 0) {
                throw new AwsSparkSessionError(
                    `${driver.service} session ${sessionId} is ready but its Spark Connect endpoint ` +
                    `did not answer within ${timeoutMs} ms.`,
                    { service: driver.service, sessionId, cause: error },
                );
            }
            await sleep(Math.min(pollIntervalMs, remaining), undefined, signal ? { signal } : undefined);
        }
    }
}

/**
 * Credentials for the session's RPCs. The token fetched to connect is used
 * first while it has more than the refresh window left; later calls ask the
 * service for a new one, which the connector does before each expiry.
 */
function tokenProvider(
    driver: ManagedSessionDriver,
    sessionId: string,
    initial: SessionEndpoint,
): SparkCredentialProvider {
    let pending: SessionEndpoint | undefined = initial;
    return async () => {
        const fresh = pending !== undefined
            && (pending.expiresAt === undefined || pending.expiresAt.getTime() - DEFAULT_REFRESH_SKEW_MS > Date.now());
        const endpoint = fresh ? pending! : await driver.endpoint(sessionId);
        pending = undefined;
        return {
            headers: { [AWS_PROXY_AUTH_HEADER]: endpoint.token },
            ...(endpoint.expiresAt === undefined ? {} : { expiresAt: endpoint.expiresAt.getTime() }),
        };
    };
}

/** Stops a session this call started after its startup failed; the original failure is rethrown. */
async function abandon(driver: ManagedSessionDriver, sessionId: string, failure: unknown): Promise<never> {
    try {
        await driver.terminate(sessionId);
    } catch (terminateError) {
        throw new AwsSparkSessionError(
            `${driver.service} session ${sessionId} could not be stopped after it failed to start ` +
            `(${terminateError instanceof Error ? terminateError.message : String(terminateError)}); ` +
            "it keeps running until its idle timeout.",
            { service: driver.service, sessionId, cause: failure },
        );
    }
    throw failure;
}

/**
 * @internal Starts or attaches to a managed session, waits until it accepts
 * clients and connects a SparkSession to it whose `close()` stops it.
 */
export async function openManagedSession(
    driver: ManagedSessionDriver,
    options: AwsSparkSessionOptions,
): Promise<AwsSparkConnection> {
    const timeoutMs = positiveInteger(options.startTimeoutMs, DEFAULT_START_TIMEOUT_MS, "startTimeoutMs");
    const pollIntervalMs = positiveInteger(options.pollIntervalMs, DEFAULT_POLL_INTERVAL_MS, "pollIntervalMs");
    const attached = options.sessionId !== undefined;
    if (attached) requireText(options.sessionId, "sessionId");
    const terminateOnClose = options.terminateOnClose ?? !attached;

    let sessionId: string;
    try {
        sessionId = options.sessionId ?? await driver.start();
    } catch (error) {
        driver.dispose();
        throw error;
    }
    try {
        const deadline = Date.now() + timeoutMs;
        await waitUntilReady(driver, sessionId, deadline, timeoutMs, pollIntervalMs, options.signal);
        const initial = await driver.endpoint(sessionId);
        const builder = SparkSession.builder()
            .config("spark.connect.url", connectAddress(initial.url))
            .withAuth({ type: "provider", provider: tokenProvider(driver, sessionId, initial) })
            .withRetry(DEFAULT_AWS_RETRY);
        options.configure?.(builder);
        const spark = builder.getOrCreate();
        try {
            await waitUntilAnswering(driver, sessionId, spark, deadline, timeoutMs, pollIntervalMs, options.signal);
        } catch (error) {
            try {
                await spark.close();
            } catch {
                // The startup failure is what the caller needs to see.
            }
            throw error;
        }
        spark.addCloseHook(async () => {
            try {
                if (terminateOnClose) await driver.terminate(sessionId);
            } finally {
                driver.dispose();
            }
        });
        return { spark, sessionId };
    } catch (error) {
        try {
            if (!attached) await abandon(driver, sessionId, error);
            throw error;
        } finally {
            driver.dispose();
        }
    }
}
