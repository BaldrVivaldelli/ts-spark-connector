import os from "node:os";
import { validateRetryConfig } from "./retry";
import type {
    AuthConfig,
    SessionConfigMap,
    SessionConfigValue,
    SparkConnectionConfig,
    SparkUserContext,
    TLSConfig,
} from "./session";

export type AuthDraft = {
    type?: AuthConfig["type"];
    username?: string;
    password?: string;
    token?: string;
};

export type TlsDraft = TLSConfig & {
    enabled?: boolean;
};

export const LEGACY_AUTH_KEYS = new Set<string>([
    "spark.auth.type",
    "spark.auth.username",
    "spark.auth.password",
    "spark.auth.token",
]);

export const LEGACY_TLS_KEYS = new Set<string>([
    "spark.ssl.enabled",
    "spark.connect.grpc.ssl.enabled",
    "spark.ssl.keyStore",
    "spark.ssl.keyStorePassword",
    "spark.ssl.trustStore",
    "spark.ssl.trustStorePassword",
    "spark.ssl.certChain",
    "spark.ssl.privateKey",
    "spark.ssl.serverNameOverride",
]);

const STRIPPED_SESSION_CONFIG_KEYS = new Set<string>([
    ...LEGACY_AUTH_KEYS,
    "spark.ssl.keyStore",
    "spark.ssl.keyStorePassword",
    "spark.ssl.trustStore",
    "spark.ssl.trustStorePassword",
    "spark.ssl.certChain",
    "spark.ssl.privateKey",
    "spark.ssl.serverNameOverride",
]);

const CONNECTION_ONLY_SESSION_KEYS = new Set<string>([
    "spark.connect.url",
    "spark.connect.address",
    "SPARK_CONNECT_URL",
    "spark.connect.userId",
    "spark.connect.userName",
    "user_id",
    "user_name",
    ...LEGACY_AUTH_KEYS,
    ...LEGACY_TLS_KEYS,
]);

const DEFAULT_SPARK_CONNECT_ADDRESS = "sc://localhost:15002";
const MAX_GRPC_MESSAGE_BYTES = 2_147_483_647;
const CANONICAL_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function isRemoteSparkConfig(key: string): boolean {
    return !CONNECTION_ONLY_SESSION_KEYS.has(key)
        && !key.startsWith("spark.connect.header.");
}

function configuredAddress(
    config: SparkConnectionConfig | undefined,
    sessionConfig: SessionConfigMap,
): string {
    if (config?.address?.trim()) return config.address.trim();
    for (const key of ["spark.connect.url", "spark.connect.address", "SPARK_CONNECT_URL"]) {
        const value = sessionConfig[key];
        if (value != null && String(value).trim()) return String(value).trim();
    }
    return process.env.SPARK_CONNECT_URL?.trim() || DEFAULT_SPARK_CONNECT_ADDRESS;
}

function validateGrpcMessageLimit(name: string, value?: number): void {
    if (value !== undefined
        && (!Number.isSafeInteger(value) || value <= 0 || value > MAX_GRPC_MESSAGE_BYTES)) {
        throw new RangeError(`${name} must be a positive integer no greater than ${MAX_GRPC_MESSAGE_BYTES}.`);
    }
}

export function validateSessionId(sessionId: string): string {
    if (!CANONICAL_UUID.test(sessionId)) {
        throw new TypeError("sessionId must be a canonical UUID string.");
    }
    return sessionId;
}

export function defaultUserContext(): SparkUserContext {
    let username = process.env.USER ?? process.env.USERNAME ?? "ts-spark-connector";
    try {
        username = os.userInfo().username || username;
    } catch {
        // Keep the environment or constant fallback.
    }

    return {
        user_id: username,
        user_name: username,
    };
}

export function cloneSessionConfig(sessionConfig?: SessionConfigMap): SessionConfigMap {
    return { ...(sessionConfig ?? {}) };
}

export function cloneAuth(auth?: AuthConfig): AuthConfig | undefined {
    return auth ? { ...auth } : undefined;
}

export function cloneTls(tls?: TLSConfig): TLSConfig | undefined {
    return tls ? { ...tls } : undefined;
}

function getTrimmedConfigString(
    sessionConfig: SessionConfigMap,
    key: string,
): string | undefined {
    const value = sessionConfig[key];
    if (value == null) return undefined;
    const text = String(value).trim();
    return text ? text : undefined;
}

export function isEnabledConfigValue(value: SessionConfigValue | undefined): boolean {
    return String(value).toLowerCase() === "true";
}

function readLegacyAuthConfig(sessionConfig: SessionConfigMap): AuthConfig | undefined {
    const authType = getTrimmedConfigString(sessionConfig, "spark.auth.type");
    if (authType === "token") {
        const token = getTrimmedConfigString(sessionConfig, "spark.auth.token");
        return token ? { type: "token", token } : undefined;
    }
    if (authType === "basic") {
        const username = getTrimmedConfigString(sessionConfig, "spark.auth.username");
        const password = getTrimmedConfigString(sessionConfig, "spark.auth.password");
        return username && password
            ? { type: "basic", username, password }
            : undefined;
    }
    return undefined;
}

function readLegacyTlsConfig(sessionConfig: SessionConfigMap): TLSConfig | undefined {
    const enabled =
        isEnabledConfigValue(sessionConfig["spark.ssl.enabled"]) ||
        isEnabledConfigValue(sessionConfig["spark.connect.grpc.ssl.enabled"]);
    const tls: TLSConfig = {
        keyStorePath: getTrimmedConfigString(sessionConfig, "spark.ssl.keyStore"),
        keyStorePassword: getTrimmedConfigString(sessionConfig, "spark.ssl.keyStorePassword"),
        trustStorePath: getTrimmedConfigString(sessionConfig, "spark.ssl.trustStore"),
        trustStorePassword: getTrimmedConfigString(sessionConfig, "spark.ssl.trustStorePassword"),
        certChainPath: getTrimmedConfigString(sessionConfig, "spark.ssl.certChain"),
        privateKeyPath: getTrimmedConfigString(sessionConfig, "spark.ssl.privateKey"),
        serverNameOverride: getTrimmedConfigString(sessionConfig, "spark.ssl.serverNameOverride"),
    };
    const hasAnyTlsField = Object.values(tls).some(
        value => typeof value === "string" && value.length > 0,
    );
    return enabled || hasAnyTlsField ? tls : undefined;
}

function stripSensitiveConnectionConfig(sessionConfig: SessionConfigMap): SessionConfigMap {
    const sanitized = cloneSessionConfig(sessionConfig);
    for (const key of STRIPPED_SESSION_CONFIG_KEYS) delete sanitized[key];
    return sanitized;
}

export function normalizeConnectionConfig(
    config?: SparkConnectionConfig,
): SparkConnectionConfig {
    validateRetryConfig(config?.retry);
    if (config?.logger !== undefined && typeof config.logger !== "function") {
        throw new TypeError("logger must be a function.");
    }
    if (config?.metrics !== undefined && typeof config.metrics !== "function") {
        throw new TypeError("metrics must be a function.");
    }
    if (config?.rpcTimeoutMs !== undefined
        && (!Number.isSafeInteger(config.rpcTimeoutMs) || config.rpcTimeoutMs <= 0)) {
        throw new RangeError("rpcTimeoutMs must be a positive safe integer.");
    }
    validateGrpcMessageLimit("grpcMaxReceiveMessageBytes", config?.grpcMaxReceiveMessageBytes);
    validateGrpcMessageLimit("grpcMaxSendMessageBytes", config?.grpcMaxSendMessageBytes);

    const rawSessionConfig = cloneSessionConfig(config?.sessionConfig);
    const auth = cloneAuth(config?.auth) ?? readLegacyAuthConfig(rawSessionConfig);
    const tls = cloneTls(config?.tls) ?? readLegacyTlsConfig(rawSessionConfig);
    return {
        ...(config ?? {}),
        address: configuredAddress(config, rawSessionConfig),
        auth,
        tls,
        sessionConfig: stripSensitiveConnectionConfig(rawSessionConfig),
    };
}

export function syncAuthFromDraft(authDraft: AuthDraft): AuthConfig | undefined {
    if (authDraft.type === "token") {
        return authDraft.token ? { type: "token", token: authDraft.token } : undefined;
    }
    if (authDraft.type === "basic" && authDraft.username && authDraft.password) {
        return {
            type: "basic",
            username: authDraft.username,
            password: authDraft.password,
        };
    }
    return undefined;
}

export function syncTlsFromDraft(tlsDraft: TlsDraft): TLSConfig | undefined {
    const { enabled, ...tls } = tlsDraft;
    const hasAnyTlsField = Object.values(tls).some(
        value => typeof value === "string" && value.length > 0,
    );
    return enabled || hasAnyTlsField ? tls : undefined;
}
