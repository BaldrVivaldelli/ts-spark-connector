import type * as EmrServerless from "@aws-sdk/client-emr-serverless";
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
 * Options of {@link emrServerlessSession}. Starting a session needs
 * `executionRoleArn`; attaching to one with `sessionId` does not.
 * @public
 */
export type EmrServerlessSessionOptions = AwsSparkSessionOptions & {
    /** Application created with `--interactive-configuration '{"sessionEnabled": true}'` (emr-7.13.0 or later). */
    applicationId: string;
    /** IAM role the session runs as. */
    executionRoleArn?: string;
    /** An `EMRServerlessClient`; by default one is created for `region` with the default credential chain. */
    client?: AwsSdkClient;
    region?: string;
    name?: string;
    idleTimeoutMinutes?: number;
    /** `configurationOverrides` of StartSession, e.g. Spark properties under the `spark-defaults` classification. */
    configurationOverrides?: Record<string, unknown>;
    tags?: Record<string, string>;
};

const SERVICE = "EMR Serverless";
const READY = new Set(["STARTED", "IDLE", "BUSY"]);
const ENDED = new Set(["FAILED", "TERMINATING", "TERMINATED"]);

/**
 * Connects to an EMR Serverless Spark Connect session, starting one unless
 * `sessionId` names a running session. Needs `@aws-sdk/client-emr-serverless`.
 *
 * ```ts
 * const { spark } = await emrServerlessSession({ applicationId, executionRoleArn, region: "us-east-1" });
 * try {
 *     await spark.sql("SELECT 1 AS one").show();
 * } finally {
 *     await spark.close(); // also terminates the EMR Serverless session
 * }
 * ```
 * @public
 */
export async function emrServerlessSession(options: EmrServerlessSessionOptions): Promise<AwsSparkConnection> {
    const applicationId = requireText(options.applicationId, "applicationId");
    const executionRoleArn = options.sessionId === undefined
        ? requireText(options.executionRoleArn, "executionRoleArn")
        : options.executionRoleArn;
    const sdk = await loadSdk(
        "@aws-sdk/client-emr-serverless",
        "emrServerlessSession()",
        () => import("@aws-sdk/client-emr-serverless"),
    );
    const owned = options.client === undefined;
    const client = (options.client
        ?? new sdk.EMRServerlessClient(options.region === undefined ? {} : { region: options.region })
    ) as EmrServerless.EMRServerlessClient;

    return openManagedSession({
        service: SERVICE,
        async start() {
            const response = await client.send(new sdk.StartSessionCommand({
                applicationId,
                executionRoleArn,
                name: options.name,
                idleTimeoutMinutes: options.idleTimeoutMinutes,
                configurationOverrides: options.configurationOverrides as EmrServerless.SessionConfigurationOverrides | undefined,
                tags: options.tags,
            }));
            return required(response.sessionId, SERVICE, "session id");
        },
        async status(sessionId) {
            const { session } = await client.send(new sdk.GetSessionCommand({ applicationId, sessionId }));
            const state = session?.state ?? "UNKNOWN";
            return { state, ready: READY.has(state), ended: ENDED.has(state), reason: session?.stateDetails };
        },
        async endpoint(sessionId) {
            const response = await client.send(new sdk.GetSessionEndpointCommand({ applicationId, sessionId }));
            return {
                url: required(response.endpoint, SERVICE, "endpoint"),
                token: required(response.authToken, SERVICE, "auth token"),
                expiresAt: response.authTokenExpiresAt,
            };
        },
        async terminate(sessionId) {
            await client.send(new sdk.TerminateSessionCommand({ applicationId, sessionId }));
        },
        dispose() {
            if (owned) client.destroy();
        },
    }, options);
}
