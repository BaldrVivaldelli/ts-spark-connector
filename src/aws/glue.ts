import crypto from "crypto";
import type * as Glue from "@aws-sdk/client-glue";
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
 * Options of {@link glueSession}. Starting a session needs `role`; attaching
 * to one with `sessionId` does not.
 * @public
 */
export type GlueSessionOptions = AwsSparkSessionOptions & {
    /** IAM role ARN the session runs as. */
    role?: string;
    /** Id of the session to create; a random `ts-spark-connector-<uuid>` by default. */
    id?: string;
    /** Glue version with Spark Connect sessions; `5.1` by default. */
    glueVersion?: string;
    workerType?: string;
    numberOfWorkers?: number;
    idleTimeoutMinutes?: number;
    timeoutMinutes?: number;
    /**
     * Session arguments, merged over `{"--language": "python"}`, which AWS's
     * Spark Connect example passes.
     */
    defaultArguments?: Record<string, string>;
    /** Glue connections the session uses. */
    connections?: string[];
    securityConfiguration?: string;
    description?: string;
    tags?: Record<string, string>;
    /** A `GlueClient`; by default one is created for `region` with the default credential chain. */
    client?: AwsSdkClient;
    region?: string;
};

const SERVICE = "AWS Glue";
const ENDED = new Set(["FAILED", "TIMEOUT", "STOPPING", "STOPPED"]);

/**
 * Connects to an AWS Glue interactive session of type `SPARK_CONNECT`,
 * creating one unless `sessionId` names a running session. Needs
 * `@aws-sdk/client-glue`.
 * @public
 */
export async function glueSession(options: GlueSessionOptions): Promise<AwsSparkConnection> {
    const role = options.sessionId === undefined ? requireText(options.role, "role") : options.role;
    const sdk = await loadSdk("@aws-sdk/client-glue", "glueSession()", () => import("@aws-sdk/client-glue"));
    const owned = options.client === undefined;
    const client = (options.client
        ?? new sdk.GlueClient(options.region === undefined ? {} : { region: options.region })
    ) as Glue.GlueClient;

    return openManagedSession({
        service: SERVICE,
        async start() {
            const response = await client.send(new sdk.CreateSessionCommand({
                Id: options.id ?? `ts-spark-connector-${crypto.randomUUID()}`,
                Role: role,
                Command: { Name: "glueetl" },
                GlueVersion: options.glueVersion ?? "5.1",
                SessionType: "SPARK_CONNECT",
                WorkerType: options.workerType as Glue.WorkerType | undefined,
                NumberOfWorkers: options.numberOfWorkers,
                IdleTimeout: options.idleTimeoutMinutes,
                Timeout: options.timeoutMinutes,
                DefaultArguments: { "--language": "python", ...options.defaultArguments },
                Connections: options.connections === undefined ? undefined : { Connections: options.connections },
                SecurityConfiguration: options.securityConfiguration,
                Description: options.description,
                Tags: options.tags,
            }));
            return required(response.Session?.Id, SERVICE, "session id");
        },
        async status(sessionId) {
            const { Session: session } = await client.send(new sdk.GetSessionCommand({ Id: sessionId }));
            const state = session?.Status ?? "UNKNOWN";
            return { state, ready: state === "READY", ended: ENDED.has(state), reason: session?.ErrorMessage };
        },
        async endpoint(sessionId) {
            const { SparkConnect: endpoint } = await client.send(new sdk.GetSessionEndpointCommand({ SessionId: sessionId }));
            return {
                url: required(endpoint?.Url, SERVICE, "Spark Connect endpoint"),
                token: required(endpoint?.AuthToken, SERVICE, "auth token"),
                expiresAt: endpoint?.AuthTokenExpirationTime,
            };
        },
        async terminate(sessionId) {
            await client.send(new sdk.StopSessionCommand({ Id: sessionId }));
        },
        dispose() {
            if (owned) client.destroy();
        },
    }, options);
}
