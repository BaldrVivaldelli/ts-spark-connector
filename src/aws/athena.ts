import type * as Athena from "@aws-sdk/client-athena";
import {
    AwsSdkClient,
    AwsSparkConnection,
    AwsSparkSessionOptions,
    loadSdk,
    openManagedSession,
    required,
    requireText,
} from "./managedSession";

/**
 * Options of {@link athenaSession}. Starting a session needs `workGroup`;
 * attaching to one with `sessionId` does not.
 * @public
 */
export type AthenaSessionOptions = AwsSparkSessionOptions & {
    /** A workgroup with the Apache Spark engine (version 3.5). */
    workGroup?: string;
    /** `EngineConfiguration` of StartSession (DPU sizes, `SparkProperties`, ...); empty by default. */
    engineConfiguration?: Record<string, unknown>;
    executionRole?: string;
    idleTimeoutMinutes?: number;
    description?: string;
    tags?: Record<string, string>;
    /** An `AthenaClient`; by default one is created for `region` with the default credential chain. */
    client?: AwsSdkClient;
    region?: string;
};

const SERVICE = "Amazon Athena";
const READY = new Set(["IDLE", "BUSY"]);
const ENDED = new Set(["FAILED", "TERMINATING", "TERMINATED"]);

/**
 * Connects to an Amazon Athena for Apache Spark session, starting one unless
 * `sessionId` names a running session. Needs `@aws-sdk/client-athena`.
 * @public
 */
export async function athenaSession(options: AthenaSessionOptions): Promise<AwsSparkConnection> {
    const workGroup = options.sessionId === undefined ? requireText(options.workGroup, "workGroup") : options.workGroup;
    const sdk = await loadSdk("@aws-sdk/client-athena", "athenaSession()", () => import("@aws-sdk/client-athena"));
    const owned = options.client === undefined;
    const client = (options.client
        ?? new sdk.AthenaClient(options.region === undefined ? {} : { region: options.region })
    ) as Athena.AthenaClient;

    return openManagedSession({
        service: SERVICE,
        async start() {
            const response = await client.send(new sdk.StartSessionCommand({
                WorkGroup: workGroup,
                EngineConfiguration: (options.engineConfiguration ?? {}) as Athena.EngineConfiguration,
                ExecutionRole: options.executionRole,
                SessionIdleTimeoutInMinutes: options.idleTimeoutMinutes,
                Description: options.description,
                Tags: options.tags === undefined
                    ? undefined
                    : Object.entries(options.tags).map(([Key, Value]) => ({ Key, Value })),
            }));
            return required(response.SessionId, SERVICE, "session id");
        },
        async status(sessionId) {
            const { Status: status } = await client.send(new sdk.GetSessionStatusCommand({ SessionId: sessionId }));
            const state = status?.State ?? "UNKNOWN";
            return { state, ready: READY.has(state), ended: ENDED.has(state), reason: status?.StateChangeReason };
        },
        async endpoint(sessionId) {
            const response = await client.send(new sdk.GetSessionEndpointCommand({ SessionId: sessionId }));
            return {
                url: required(response.EndpointUrl, SERVICE, "endpoint"),
                token: required(response.AuthToken, SERVICE, "auth token"),
                expiresAt: response.AuthTokenExpirationTime,
            };
        },
        async terminate(sessionId) {
            await client.send(new sdk.TerminateSessionCommand({ SessionId: sessionId }));
        },
        dispose() {
            if (owned) client.destroy();
        },
    }, options);
}
