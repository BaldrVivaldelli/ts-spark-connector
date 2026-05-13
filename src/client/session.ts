import crypto from "crypto";
import os from "node:os";
import { DataFrameReaderTF } from "../read/dataFrameReaderTF";
import { SessionAlgebra } from "./sessionAlgebra";
import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import { DFProgram, StreamingMark } from "../algebra/read";
import { SqlCap } from "../algebra/read/batch-capabilities";
import { StreamingReadCap } from "../algebra/read/streaming-capabilities";
import { DataFrameWriterTF } from "../write/dataFrameWriterTF";
import { WStream } from "../algebra/write";
import { StreamWriterAlg } from "../algebra/write/dataframe";

export type TLSConfig = {
    keyStorePath?: string;
    keyStorePassword?: string;
    trustStorePath?: string;
    trustStorePassword?: string;
    certChainPath?: string;
    privateKeyPath?: string;
    serverNameOverride?: string;
};

export type AuthConfig =
    | { type: "basic"; username: string; password: string }
    | { type: "token"; token: string };

export type SparkUserContext = {
    user_id: string;
    user_name?: string;
};

export type SessionConfigValue = string | number | boolean;
export type SessionConfigMap = Record<string, SessionConfigValue>;

export type SparkConnectionConfig = {
    address?: string;
    auth?: AuthConfig;
    tls?: TLSConfig;
    sessionConfig?: SessionConfigMap;
};

type AuthDraft = {
    type?: AuthConfig["type"];
    username?: string;
    password?: string;
    token?: string;
};

type TlsDraft = TLSConfig & {
    enabled?: boolean;
};

const LEGACY_AUTH_KEYS = new Set<string>([
    "spark.auth.type",
    "spark.auth.username",
    "spark.auth.password",
    "spark.auth.token",
]);

const LEGACY_TLS_KEYS = new Set<string>([
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

function defaultUserContext(): SparkUserContext {
    let username = process.env.USER ?? process.env.USERNAME ?? "ts-spark-connector";
    try {
        username = os.userInfo().username || username;
    } catch {
        // noop: dejamos el fallback de env o constante.
    }

    return {
        user_id: username,
        user_name: username,
    };
}

function cloneSessionConfig(sessionConfig?: SessionConfigMap): SessionConfigMap {
    return { ...(sessionConfig ?? {}) };
}

function cloneAuth(auth?: AuthConfig): AuthConfig | undefined {
    if (!auth) return undefined;
    return auth.type === "basic" ? { ...auth } : { ...auth };
}

function cloneTls(tls?: TLSConfig): TLSConfig | undefined {
    return tls ? { ...tls } : undefined;
}

function getTrimmedConfigString(sessionConfig: SessionConfigMap, key: string): string | undefined {
    const value = sessionConfig[key];
    if (value == null) return undefined;

    const text = String(value).trim();
    return text ? text : undefined;
}

function isEnabledConfigValue(value: SessionConfigValue | undefined): boolean {
    return String(value).toLowerCase() === "true";
}

function readLegacyAuthConfigFromSessionConfig(sessionConfig: SessionConfigMap): AuthConfig | undefined {
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

function readLegacyTlsConfigFromSessionConfig(sessionConfig: SessionConfigMap): TLSConfig | undefined {
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

    const hasAnyTlsField = Object.values(tls).some(value => typeof value === "string" && value.length > 0);
    if (!enabled && !hasAnyTlsField) {
        return undefined;
    }

    return tls;
}

function stripSensitiveConnectionConfig(sessionConfig: SessionConfigMap): SessionConfigMap {
    const sanitized = cloneSessionConfig(sessionConfig);
    for (const key of STRIPPED_SESSION_CONFIG_KEYS) {
        delete sanitized[key];
    }
    return sanitized;
}

function normalizeConnectionConfig(config?: SparkConnectionConfig): SparkConnectionConfig {
    const rawSessionConfig = cloneSessionConfig(config?.sessionConfig);
    const auth = cloneAuth(config?.auth) ?? readLegacyAuthConfigFromSessionConfig(rawSessionConfig);
    const tls = cloneTls(config?.tls) ?? readLegacyTlsConfigFromSessionConfig(rawSessionConfig);

    return {
        ...(config ?? {}),
        auth,
        tls,
        sessionConfig: stripSensitiveConnectionConfig(rawSessionConfig),
    };
}

function syncAuthFromDraft(authDraft: AuthDraft): AuthConfig | undefined {
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

function syncTlsFromDraft(tlsDraft: TlsDraft): TLSConfig | undefined {
    const { enabled, ...tls } = tlsDraft;
    const hasAnyTlsField = Object.values(tls).some(value => typeof value === "string" && value.length > 0);
    if (!enabled && !hasAnyTlsField) {
        return undefined;
    }
    return tls;
}

export class SparkSession implements SessionAlgebra {
    private readonly sessionId: string;
    private userContext: SparkUserContext;
    private connectionConfig: SparkConnectionConfig;

    constructor(
        sessionId?: string,
        opts?: {
            userContext?: Partial<SparkUserContext>;
            connectionConfig?: SparkConnectionConfig;
        }
    ) {
        this.sessionId = sessionId ?? crypto.randomUUID();
        this.userContext = {
            ...defaultUserContext(),
            ...(opts?.userContext ?? {}),
        };
        this.connectionConfig = normalizeConnectionConfig(opts?.connectionConfig);
    }

    readStream<R, E, G>(
        format: string,
        options?: Record<string, string>
    ): ReadChainedDataFrame<R, E, G, StreamingReadCap<R> & StreamingMark<R>, unknown> {
        return ReadChainedDataFrame.readStream<R, E, G>(format, this, options);
    }

    writeStream<
        R = unknown,
        E = unknown,
        G = unknown,
        CDF = unknown,
        CEX = unknown
    >(
        df: ReadChainedDataFrame<R, E, G, CDF & StreamingMark<R>, CEX>
    ): DataFrameWriterTF<R, E, G, WStream, CDF & StreamingMark<R>, CEX, StreamWriterAlg<R>> {
        return df.writeStream();
    }

    sql<R = unknown, E = unknown, G = unknown>(
        query: string
    ): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown> {
        const prog: DFProgram<R, E, G, SqlCap<R>, unknown> = (DF) => DF.sql(query);
        return new ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>(prog, this);
    }

    table<R = unknown, E = unknown, G = unknown>(
        name: string
    ): ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown> {
        const prog: DFProgram<R, E, G, SqlCap<R>, unknown> = (DF) => DF.sql(`SELECT * FROM ${name}`);
        return new ReadChainedDataFrame<R, E, G, SqlCap<R>, unknown>(prog, this);
    }

    static builder(): SparkSessionBuilder {
        return new SparkSessionBuilder();
    }

    get read(): DataFrameReaderTF {
        return new DataFrameReaderTF(this);
    }

    getSessionId(): string {
        return this.sessionId;
    }

    getUserContext(): SparkUserContext {
        return { ...this.userContext };
    }

    setUserContext(context: Partial<SparkUserContext>) {
        const next: Partial<SparkUserContext> = {};
        if (typeof context.user_id === "string" && context.user_id.trim()) {
            next.user_id = context.user_id;
        }
        if (typeof context.user_name === "string" && context.user_name.trim()) {
            next.user_name = context.user_name;
        }
        this.userContext = {
            ...this.userContext,
            ...next,
        };
    }

    getConnectionConfig(): SparkConnectionConfig {
        return {
            ...this.connectionConfig,
            auth: cloneAuth(this.connectionConfig.auth),
            tls: cloneTls(this.connectionConfig.tls),
            sessionConfig: cloneSessionConfig(this.connectionConfig.sessionConfig),
        };
    }

    setConnectionConfig(config: SparkConnectionConfig) {
        this.connectionConfig = normalizeConnectionConfig(config);
    }

    getSessionConfig(): SessionConfigMap {
        return cloneSessionConfig(this.connectionConfig.sessionConfig);
    }
}

export function createSparkSession(sessionId?: string): SparkSession {
    return new SparkSession(sessionId);
}

class SparkSessionBuilder {
    private configMap: SessionConfigMap = {};
    private auth?: AuthConfig;
    private readonly authDraft: AuthDraft = {};
    private tls?: TLSConfig;
    private readonly tlsDraft: TlsDraft = {};
    private userContext: Partial<SparkUserContext> = {};

    config(key: string, value: SessionConfigValue): this {
        this.applyReservedConnectionDrafts(key, value);

        if (!this.isSensitiveReservedConfigKey(key)) {
            this.configMap[key] = value;
        }

        if (key === "spark.connect.userId" || key === "user_id") {
            this.userContext.user_id = String(value);
        }
        if (key === "spark.connect.userName" || key === "user_name") {
            this.userContext.user_name = String(value);
        }

        return this;
    }

    configs(configs: SessionConfigMap): this {
        Object.entries(configs).forEach(([key, value]) => this.config(key, value));
        return this;
    }

    user(context: Partial<SparkUserContext>): this {
        this.userContext = {
            ...this.userContext,
            ...context,
        };
        return this;
    }

    enableTLS(tls: TLSConfig): this {
        this.tls = { ...tls };
        this.tlsDraft.enabled = true;
        Object.assign(this.tlsDraft, tls);
        this.configMap["spark.ssl.enabled"] = true;
        return this;
    }

    withAuth(auth: AuthConfig): this {
        this.auth = { ...auth };
        this.resetAuthDraft(auth);
        return this;
    }

    withAuthAndTLS(auth: AuthConfig, tls: TLSConfig): this {
        return this.withAuth(auth).enableTLS(tls);
    }

    getOrCreate(): SparkSession {
        const session = new SparkSession(undefined, {
            userContext: this.userContext,
            connectionConfig: {
                address: this.readConfiguredAddress(),
                auth: this.auth,
                tls: this.tls,
                sessionConfig: { ...this.configMap },
            },
        });
        return session;
    }

    private isSensitiveReservedConfigKey(key: string): boolean {
        return LEGACY_AUTH_KEYS.has(key)
            || (LEGACY_TLS_KEYS.has(key) && key !== "spark.ssl.enabled" && key !== "spark.connect.grpc.ssl.enabled");
    }

    private applyReservedConnectionDrafts(key: string, value: SessionConfigValue) {
        this.applyAuthDraftKey(key, value);
        this.applyTlsDraftKey(key, value);
    }

    private applyAuthDraftKey(key: string, value: SessionConfigValue) {
        const text = String(value);
        switch (key) {
            case "spark.auth.type":
                this.authDraft.type = text === "token" ? "token" : text === "basic" ? "basic" : undefined;
                break;
            case "spark.auth.username":
                this.authDraft.username = text;
                break;
            case "spark.auth.password":
                this.authDraft.password = text;
                break;
            case "spark.auth.token":
                this.authDraft.token = text;
                break;
            default:
                return;
        }

        this.auth = syncAuthFromDraft(this.authDraft);
    }

    private applyTlsDraftKey(key: string, value: SessionConfigValue) {
        const text = String(value);
        switch (key) {
            case "spark.ssl.enabled":
            case "spark.connect.grpc.ssl.enabled":
                this.tlsDraft.enabled = isEnabledConfigValue(value);
                break;
            case "spark.ssl.keyStore":
                this.tlsDraft.keyStorePath = text;
                break;
            case "spark.ssl.keyStorePassword":
                this.tlsDraft.keyStorePassword = text;
                break;
            case "spark.ssl.trustStore":
                this.tlsDraft.trustStorePath = text;
                break;
            case "spark.ssl.trustStorePassword":
                this.tlsDraft.trustStorePassword = text;
                break;
            case "spark.ssl.certChain":
                this.tlsDraft.certChainPath = text;
                break;
            case "spark.ssl.privateKey":
                this.tlsDraft.privateKeyPath = text;
                break;
            case "spark.ssl.serverNameOverride":
                this.tlsDraft.serverNameOverride = text;
                break;
            default:
                return;
        }

        this.tls = syncTlsFromDraft(this.tlsDraft);
    }

    private resetAuthDraft(auth: AuthConfig) {
        this.authDraft.type = auth.type;
        this.authDraft.username = auth.type === "basic" ? auth.username : undefined;
        this.authDraft.password = auth.type === "basic" ? auth.password : undefined;
        this.authDraft.token = auth.type === "token" ? auth.token : undefined;
    }

    private readConfiguredAddress(): string | undefined {
        for (const key of ["spark.connect.url", "spark.connect.address", "SPARK_CONNECT_URL"]) {
            const value = this.configMap[key];
            if (value != null && String(value).trim()) {
                return String(value);
            }
        }
        return undefined;
    }
}

export const spark = createSparkSession();
