import crypto from "crypto";
import { DataFrameReaderTF } from "../read/dataFrameReaderTF";
import { SessionAlgebra } from "./sessionAlgebra";
import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import { StreamingMark } from "../algebra/read";
import { SqlCap } from "../algebra/read/batch-capabilities";
import { StreamingReadCap } from "../algebra/read/streaming-capabilities";
import { DataFrameWriterTF } from "../write/dataFrameWriterTF";
import { WStream } from "../algebra/write";
import { StreamWriterAlg } from "../algebra/write/dataframe";
import { UnknownSchema } from "../schema/schema-model";
import { validateRetryConfig } from "./retry";
import { SPARK_CLIENT_CACHE_IDENTITY, sparkGrpcClient } from "./sparkClient";
import {
    AuthDraft,
    LEGACY_AUTH_KEYS,
    LEGACY_TLS_KEYS,
    TlsDraft,
    cloneAuth,
    cloneSessionConfig,
    cloneTls,
    defaultUserContext,
    isEnabledConfigValue,
    isRemoteSparkConfig,
    normalizeConnectionConfig,
    syncAuthFromDraft,
    syncTlsFromDraft,
    validateSessionId,
} from "./sessionConfig";

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

export type RetryEvent = {
    /** One-based retry attempt number (the initial call is not a retry). */
    attempt: number;
    /** Delay selected for this retry after jitter, in milliseconds. */
    delayMs: number;
    /** The transient error that caused the retry. */
    error: unknown;
};

export type RetryConfig = {
    /** Maximum number of retry attempts after the initial call (0 disables retries). */
    maxRetries?: number;
    /** Delay before the first retry, in milliseconds. */
    initialBackoffMs?: number;
    /** Upper bound for any single backoff delay, in milliseconds. */
    maxBackoffMs?: number;
    /** Multiplier applied to the backoff after each attempt. */
    backoffMultiplier?: number;
    /** Called immediately before each retry delay. Throwing aborts the retry loop. */
    onRetry?: (event: RetryEvent) => void;
};

export type SparkTelemetryEvent = {
    name: string;
    level: "debug" | "info" | "warn" | "error";
    timestamp: string;
    attributes: Record<string, unknown>;
};

/** Opt-in sink for structured, redacted Spark Connect lifecycle events. */
export type SparkLogger = (event: Readonly<SparkTelemetryEvent>) => void;

export type SparkMetric = {
    name: string;
    kind: "counter" | "histogram";
    value: number;
    unit: "count" | "milliseconds";
    timestamp: string;
    attributes: Record<string, unknown>;
};

/** Opt-in metrics sink. Observer failures never affect Spark operations. */
export type SparkMetricObserver = (metric: Readonly<SparkMetric>) => void | PromiseLike<void>;

export type SparkUserContext = {
    user_id: string;
    user_name?: string;
};

export type SessionConfigValue = string | number | boolean;
export type SessionConfigMap = Record<string, SessionConfigValue>;

export type SparkConnectionConfig = {
    address?: string;
    auth?: AuthConfig;
    /**
     * Permit Basic/Bearer credentials over a plaintext channel. Disabled by
     * default; only enable this for trusted local development networks.
     */
    allowInsecureAuth?: boolean;
    /** Per-RPC deadline in milliseconds. */
    rpcTimeoutMs?: number;
    /** Cancels RPCs and retry backoff when aborted. */
    signal?: AbortSignal;
    /** Maximum serialized gRPC response size. Defaults to 128 MiB. */
    grpcMaxReceiveMessageBytes?: number;
    /** Maximum serialized gRPC request size. Defaults to 128 MiB. */
    grpcMaxSendMessageBytes?: number;
    tls?: TLSConfig;
    retry?: RetryConfig;
    logger?: SparkLogger;
    metrics?: SparkMetricObserver;
    sessionConfig?: SessionConfigMap;
};

export class SparkSession implements SessionAlgebra {
    private readonly sessionId: string;
    private userContext: SparkUserContext;
    private connectionConfig: SparkConnectionConfig;
    private initialConfigApplied = false;
    private initialConfigPromise?: Promise<void>;
    private remoteTouched = false;
    private closed = false;
    private closePromise?: Promise<void>;
    private clientRetained = false;
    private clientCacheIdentity?: string;
    private serverSideSessionId?: string;
    private invalidatedError?: Error;
    private readonly runtimeConfig: SparkRuntimeConfig;

    constructor(
        sessionId?: string,
        opts?: {
            userContext?: Partial<SparkUserContext>;
            connectionConfig?: SparkConnectionConfig;
        }
    ) {
        this.sessionId = validateSessionId(sessionId ?? crypto.randomUUID());
        this.userContext = {
            ...defaultUserContext(),
            ...(opts?.userContext ?? {}),
        };
        this.connectionConfig = normalizeConnectionConfig(opts?.connectionConfig);
        this.runtimeConfig = new SparkRuntimeConfig(this);
    }

    readStream<R, E, G>(
        format: string,
        options?: Record<string, string>
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, StreamingReadCap<R> & StreamingMark<R>, unknown> {
        return ReadChainedDataFrame.readStream<R, E, G>(format, this, options);
    }

    writeStream<
        R = unknown,
        E = unknown,
        G = unknown,
        CDF = unknown,
        CEX = unknown
    >(
        df: ReadChainedDataFrame<UnknownSchema, R, E, G, CDF & StreamingMark<R>, CEX>
    ): DataFrameWriterTF<R, E, G, WStream, CDF & StreamingMark<R>, CEX, StreamWriterAlg<R>> {
        return df.writeStream();
    }

    sql<R = unknown, E = unknown, G = unknown>(
        query: string
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown> {
        return new DataFrameReaderTF<R, E, G>(this).sql(query);
    }

    table<R = unknown, E = unknown, G = unknown>(
        name: string
    ): ReadChainedDataFrame<UnknownSchema, R, E, G, SqlCap<R>, unknown> {
        return new DataFrameReaderTF<R, E, G>(this).table(name);
    }

    static builder(): SparkSessionBuilder {
        return new SparkSessionBuilder();
    }

    get read(): DataFrameReaderTF {
        this.assertOpen();
        return new DataFrameReaderTF(this);
    }

    /** Remote Spark SQL/runtime configuration for this Connect session. */
    get conf(): SparkRuntimeConfig {
        this.assertOpen();
        return this.runtimeConfig;
    }

    getSessionId(): string {
        return this.sessionId;
    }

    getUserContext(): SparkUserContext {
        return { ...this.userContext };
    }

    setUserContext(context: Partial<SparkUserContext>) {
        this.assertOpen();
        if (this.remoteTouched) {
            throw new Error("Cannot change the Spark user after this session has used remote state.");
        }
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
        return this.attachClientCacheIdentity({
            ...this.connectionConfig,
            auth: cloneAuth(this.connectionConfig.auth),
            tls: cloneTls(this.connectionConfig.tls),
            retry: this.connectionConfig.retry ? { ...this.connectionConfig.retry } : undefined,
            sessionConfig: cloneSessionConfig(this.connectionConfig.sessionConfig),
        });
    }

    setConnectionConfig(config: SparkConnectionConfig) {
        this.assertOpen();
        if (this.remoteTouched) {
            throw new Error("Cannot change the Spark connection after this session has used remote state.");
        }
        this.connectionConfig = normalizeConnectionConfig(config);
        this.initialConfigApplied = false;
        this.initialConfigPromise = undefined;
    }

    getSessionConfig(): SessionConfigMap {
        return cloneSessionConfig(this.connectionConfig.sessionConfig);
    }

    /** @internal Ensures builder-provided Spark configs reach the remote session once. */
    async ensureRemoteConfigApplied(): Promise<void> {
        this.assertOpen();
        if (this.initialConfigApplied) return;
        if (this.initialConfigPromise) return this.initialConfigPromise;

        const pairs = Object.entries(this.connectionConfig.sessionConfig ?? {})
            .filter(([key]) => isRemoteSparkConfig(key))
            .map(([key, value]) => ({ key, value: String(value) }));

        if (pairs.length === 0) {
            this.initialConfigApplied = true;
            return;
        }

        this.initialConfigPromise = this.runConfigOperation({ set: { pairs, silent: false } })
            .then(() => {
                this.initialConfigApplied = true;
            })
            .finally(() => {
                this.initialConfigPromise = undefined;
            });
        return this.initialConfigPromise;
    }

    /** @internal Validates response identity and records the first server session id. */
    observeServerSideSessionId(response: unknown): void {
        const carrier = response as {
            session_id?: unknown;
            sessionId?: unknown;
            server_side_session_id?: unknown;
            serverSideSessionId?: unknown;
        } | null;
        const responseSessionId = carrier?.session_id ?? carrier?.sessionId;
        if (responseSessionId !== undefined && responseSessionId !== this.sessionId) {
            throw this.invalidate(
                `Spark Connect returned session_id ${String(responseSessionId)}; expected ${this.sessionId}.`
            );
        }

        const id = carrier?.server_side_session_id ?? carrier?.serverSideSessionId;
        if (typeof id !== "string" || !id) return;
        if (this.serverSideSessionId && this.serverSideSessionId !== id) {
            throw this.invalidate(
                `Spark Connect server session changed from ${this.serverSideSessionId} to ${id}.`
            );
        }
        this.serverSideSessionId = id;
    }

    /** @internal Marks the session unusable when Spark reports stale remote state. */
    observeRemoteError(error: unknown): void {
        const remote = error as {
            errorClass?: unknown;
            errorInfo?: { reason?: unknown; metadata?: Record<string, unknown> };
        } | null;
        const errorClass = remote?.errorClass
            ?? remote?.errorInfo?.metadata?.errorClass
            ?? remote?.errorInfo?.reason;
        if (errorClass === "INVALID_HANDLE.SESSION_CHANGED") {
            this.invalidate("Spark Connect reported that the server-side session changed.", error);
        }
    }

    /** @internal Server-side session id to attach to subsequent requests when known. */
    getServerSideSessionId(): string | undefined {
        return this.serverSideSessionId;
    }

    /** Releases remote session state and closes the cached gRPC channel. Idempotent. */
    async close(): Promise<void> {
        if (this.closed) return;
        if (this.closePromise) return this.closePromise;
        const config = this.getConnectionConfigUnchecked();
        const cleanupConfig = { ...config, signal: undefined };

        this.closePromise = (async () => {
            if (this.remoteTouched && !this.invalidatedError) {
                try {
                    const response = await sparkGrpcClient.releaseSession({
                        session_id: this.sessionId,
                        user_context: this.getUserContext(),
                        client_type: "ts-spark-connector",
                    }, cleanupConfig);
                    this.observeServerSideSessionId(response);
                } catch (error) {
                    this.observeRemoteError(error);
                    if (!this.invalidatedError) throw error;
                    // The server has already rejected this stale identity. There
                    // is no valid remote session left to release, so continue
                    // with deterministic local channel cleanup.
                }
            }
            this.closed = true;
            if (this.clientRetained) {
                sparkGrpcClient.close(config);
                this.clientRetained = false;
                this.clientCacheIdentity = undefined;
            }
        })().finally(() => {
            this.closePromise = undefined;
        });
        return this.closePromise;
    }

    /** Alias matching Spark clients that call session shutdown `stop()`. */
    stop(): Promise<void> {
        return this.close();
    }

    /** @internal Executes a raw Config operation and tracks session identity. */
    async runConfigOperation(operation: Record<string, unknown>): Promise<Record<string, unknown>> {
        this.assertOpen();
        this.markRemoteTouched();
        let response: Record<string, unknown>;
        try {
            response = await sparkGrpcClient.config({
                session_id: this.sessionId,
                user_context: this.getUserContext(),
                client_type: "ts-spark-connector",
                ...(this.serverSideSessionId
                    ? { client_observed_server_side_session_id: this.serverSideSessionId }
                    : {}),
                operation,
            }, this.getConnectionConfigUnchecked());
        } catch (error) {
            this.observeRemoteError(error);
            throw error;
        }
        this.observeServerSideSessionId(response);
        return response;
    }

    /** @internal Marks an execution RPC as having created/used remote state. */
    markRemoteTouched(): void {
        this.assertOpen();
        if (!this.clientRetained) {
            this.clientCacheIdentity = sparkGrpcClient.retain(this.getConnectionConfigUnchecked());
            this.clientRetained = true;
        }
        this.remoteTouched = true;
    }

    /** @internal Mirrors successful runtime config mutations in the local snapshot. */
    updateLocalSessionConfig(key: string, value: SessionConfigValue | undefined): void {
        const sessionConfig = cloneSessionConfig(this.connectionConfig.sessionConfig);
        if (value === undefined) delete sessionConfig[key];
        else sessionConfig[key] = value;
        this.connectionConfig = { ...this.connectionConfig, sessionConfig };
    }

    private assertOpen(): void {
        if (this.invalidatedError) throw this.invalidatedError;
        if (this.closed) throw new Error("SparkSession is closed.");
        if (this.closePromise) throw new Error("SparkSession is closing.");
    }

    private invalidate(message: string, cause?: unknown): Error {
        if (!this.invalidatedError) {
            const error = new Error(message) as Error & { cause?: unknown };
            error.name = "SparkSessionInvalidError";
            error.cause = cause;
            this.invalidatedError = error;

            // A stale server identity can never become usable again. Drop this
            // session's channel ownership immediately so callers are not
            // required to remember close() after the failing RPC. Do not send
            // ReleaseSession: the server identity is precisely what is no
            // longer trustworthy.
            if (this.clientRetained) {
                const config = this.getConnectionConfigUnchecked();
                this.clientRetained = false;
                this.clientCacheIdentity = undefined;
                try {
                    sparkGrpcClient.close(config);
                } catch {
                    // Local transport cleanup must not hide the integrity error.
                }
            }
        }
        return this.invalidatedError;
    }

    private getConnectionConfigUnchecked(): SparkConnectionConfig {
        return this.attachClientCacheIdentity({
            ...this.connectionConfig,
            auth: cloneAuth(this.connectionConfig.auth),
            tls: cloneTls(this.connectionConfig.tls),
            retry: this.connectionConfig.retry ? { ...this.connectionConfig.retry } : undefined,
            sessionConfig: cloneSessionConfig(this.connectionConfig.sessionConfig),
        });
    }

    private attachClientCacheIdentity(config: SparkConnectionConfig): SparkConnectionConfig {
        if (!this.clientCacheIdentity) return config;
        (config as SparkConnectionConfig & {
            [SPARK_CLIENT_CACHE_IDENTITY]?: string;
        })[SPARK_CLIENT_CACHE_IDENTITY] = this.clientCacheIdentity;
        return config;
    }
}

type ConfigPair = { key?: string; value?: string };

/** Spark Connect runtime configuration facade backed by the Config RPC. */
export class SparkRuntimeConfig {
    /** @internal */ constructor(private readonly session: SparkSession) {}

    async set(key: string, value: SessionConfigValue): Promise<void> {
        const normalizedKey = this.validateKey(key);
        await this.session.ensureRemoteConfigApplied();
        await this.session.runConfigOperation({
            set: { pairs: [{ key: normalizedKey, value: String(value) }], silent: false },
        });
        this.session.updateLocalSessionConfig(normalizedKey, value);
    }

    async get(key: string, defaultValue?: SessionConfigValue): Promise<string> {
        const normalizedKey = this.validateKey(key);
        await this.session.ensureRemoteConfigApplied();
        const response = defaultValue === undefined
            ? await this.session.runConfigOperation({ get: { keys: [normalizedKey] } })
            : await this.session.runConfigOperation({
                get_with_default: {
                    pairs: [{ key: normalizedKey, value: String(defaultValue) }],
                },
            });
        const value = this.pairs(response).find(pair => pair.key === normalizedKey)?.value;
        if (value === undefined) {
            throw new Error(`Spark config "${normalizedKey}" was not returned by the server.`);
        }
        return value;
    }

    async getOption(key: string): Promise<string | undefined> {
        const normalizedKey = this.validateKey(key);
        await this.session.ensureRemoteConfigApplied();
        const response = await this.session.runConfigOperation({
            get_option: { keys: [normalizedKey] },
        });
        return this.pairs(response).find(pair => pair.key === normalizedKey)?.value;
    }

    async getAll(prefix?: string): Promise<Record<string, string>> {
        await this.session.ensureRemoteConfigApplied();
        const response = await this.session.runConfigOperation({
            get_all: prefix === undefined ? {} : { prefix },
        });
        return Object.fromEntries(
            this.pairs(response)
                .filter((pair): pair is Required<ConfigPair> => !!pair.key && pair.value !== undefined)
                .map(pair => [pair.key, pair.value])
        );
    }

    async unset(key: string): Promise<void> {
        const normalizedKey = this.validateKey(key);
        await this.session.ensureRemoteConfigApplied();
        await this.session.runConfigOperation({ unset: { keys: [normalizedKey] } });
        this.session.updateLocalSessionConfig(normalizedKey, undefined);
    }

    async isModifiable(key: string): Promise<boolean> {
        const normalizedKey = this.validateKey(key);
        await this.session.ensureRemoteConfigApplied();
        const response = await this.session.runConfigOperation({
            is_modifiable: { keys: [normalizedKey] },
        });
        return this.pairs(response).find(pair => pair.key === normalizedKey)?.value === "true";
    }

    private validateKey(key: string): string {
        const normalized = key.trim();
        if (!normalized) throw new TypeError("Spark config key must be a non-empty string.");
        if (!isRemoteSparkConfig(normalized)) {
            throw new Error(`"${normalized}" is a connection setting, not a remote Spark config.`);
        }
        return normalized;
    }

    private pairs(response: Record<string, unknown>): ConfigPair[] {
        return Array.isArray(response.pairs) ? response.pairs as ConfigPair[] : [];
    }
}

export function createSparkSession(sessionId?: string): SparkSession {
    return new SparkSession(sessionId);
}

export class SparkSessionBuilder {
    private configMap: SessionConfigMap = {};
    private auth?: AuthConfig;
    private insecureAuthAllowed = false;
    private rpcTimeoutMs?: number;
    private signal?: AbortSignal;
    private readonly authDraft: AuthDraft = {};
    private tls?: TLSConfig;
    private readonly tlsDraft: TlsDraft = {};
    private retry?: RetryConfig;
    private logger?: SparkLogger;
    private metrics?: SparkMetricObserver;
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

    /**
     * Explicitly allow Basic/Bearer credentials on a plaintext `sc://`
     * connection. Prefer `scs://` or `enableTLS()` outside local development.
     */
    allowInsecureAuth(allow = true): this {
        this.insecureAuthAllowed = allow;
        return this;
    }

    /** Sets a deadline applied independently to every Spark Connect RPC. */
    withRpcTimeout(timeoutMs: number): this {
        if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
            throw new RangeError("rpcTimeoutMs must be a positive safe integer.");
        }
        this.rpcTimeoutMs = timeoutMs;
        return this;
    }

    /** Associates an AbortSignal with RPCs and retry waits created by this session. */
    withAbortSignal(signal: AbortSignal): this {
        this.signal = signal;
        return this;
    }

    withRetry(retry: RetryConfig): this {
        validateRetryConfig(retry);
        this.retry = { ...retry };
        return this;
    }

    /** Enables structured transport telemetry. No logger is installed by default. */
    withLogger(logger: SparkLogger): this {
        if (typeof logger !== "function") throw new TypeError("logger must be a function.");
        this.logger = logger;
        return this;
    }

    /** Enables redacted RPC counters and duration histograms. */
    withMetrics(observer: SparkMetricObserver): this {
        if (typeof observer !== "function") throw new TypeError("metrics observer must be a function.");
        this.metrics = observer;
        return this;
    }

    withAuthAndTLS(auth: AuthConfig, tls: TLSConfig): this {
        return this.withAuth(auth).enableTLS(tls);
    }

    /**
     * Creates a new lazy client session. The name mirrors Spark's builder API,
     * but this connector has no process-global active-session registry: no RPC
     * is opened and no existing `SparkSession` is reused here.
     */
    getOrCreate(): SparkSession {
        const session = new SparkSession(undefined, {
            userContext: this.userContext,
            connectionConfig: {
                address: this.readConfiguredAddress(),
                auth: this.auth,
                allowInsecureAuth: this.insecureAuthAllowed,
                rpcTimeoutMs: this.rpcTimeoutMs,
                signal: this.signal,
                tls: this.tls,
                retry: this.retry,
                logger: this.logger,
                metrics: this.metrics,
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
