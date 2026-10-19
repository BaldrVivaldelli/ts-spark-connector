import crypto from "crypto";
import { DataFrameReaderTF } from "../read/dataFrameReaderTF";
import { SparkCatalog } from "./catalog";
import { SessionAlgebra } from "./sessionAlgebra";
import { ReadChainedDataFrame } from "../read/readChainedDataFrame";
import { DFProgram, StreamingMark } from "../algebra/read";
import { SqlCap } from "../algebra/read/batch-capabilities";
import { DeclaredSchema, InferSchema, SchemaDef, schema } from "../schema/schema";
import { encodeLocalRelation } from "../typed/arrow-encode";
import { StreamingReadCap } from "../algebra/read/streaming-capabilities";
import { DataFrameWriterTF } from "../write/dataFrameWriterTF";
import { WStream } from "../algebra/write";
import { StreamWriterAlg } from "../algebra/write/dataframe";
import { UnknownSchema } from "../schema/schema-model";
import { validateRetryConfig } from "./retry";
import { SPARK_CLIENT_CACHE_IDENTITY, sparkGrpcClient } from "./sparkClient";
import { cleanupConnectionConfig } from "./rpcLifecycle";
import {
    CACHE_ARTIFACT_PREFIX,
    artifactRequests,
    cachedArtifactRequests,
    uploadedArtifactNames,
    type ArtifactKind,
    type ArtifactUpload,
} from "./artifacts";
import { SparkUdfRegistration } from "./udf";
import { StreamingQueryManager } from "./streamingQueryManager";
import { compareSparkVersions, UnsupportedServerVersionError } from "./serverVersion";
import {
    assertOperationId,
    assertValidTag,
    ExecutionOptions,
    ResolvedExecution,
    resolveExecution,
} from "./executionOptions";
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
    validateExecuteTimeout,
    validateSessionId,
} from "./sessionConfig";

/** @public */
export type TLSConfig = {
    keyStorePath?: string;
    keyStorePassword?: string;
    trustStorePath?: string;
    trustStorePassword?: string;
    certChainPath?: string;
    privateKeyPath?: string;
    serverNameOverride?: string;
};

/**
 * Credentials resolved by a {@link SparkCredentialProvider} for outgoing RPCs.
 * @public
 */
export type SparkCredentials = {
    /**
     * Headers attached to each Spark Connect call, such as
     * `{ authorization: "Bearer <jwt>" }`. Names are lowercased; values must
     * not contain CR, LF, or NUL.
     */
    headers: Record<string, string>;
    /**
     * Epoch milliseconds at which these credentials stop being valid. Until
     * then a refresh failure falls back to the previous result. When omitted,
     * the result is held only briefly to absorb per-query RPC bursts, so a
     * provider that does its own caching stays authoritative.
     */
    expiresAt?: number;
};

/**
 * Supplies credentials on demand. Called before RPCs whose cached credentials
 * are absent or within the refresh window, and never more than once
 * concurrently per provider.
 * @public
 */
export type SparkCredentialProvider = () => SparkCredentials | Promise<SparkCredentials>;

/**
 * AWS credentials forwarded to the Spark job as S3A Hadoop configuration.
 * These authenticate the job's `s3a://` reads and writes on the server, not
 * the client's gRPC channel; use {@link AuthConfig} for the latter.
 * @public
 */
export type S3Credentials = {
    accessKeyId: string;
    secretAccessKey: string;
    /** Marks the credentials as temporary (STS/AssumeRole) when present. */
    sessionToken?: string;
};

/** @public */
export type AuthConfig =
    | { type: "basic"; username: string; password: string }
    | { type: "token"; token: string }
    | {
        type: "provider";
        provider: SparkCredentialProvider;
        /** Refresh lead time before `expiresAt`, in milliseconds. Defaults to 30000. */
        refreshSkewMs?: number;
    };

/** @public */
export type RetryEvent = {
    /** One-based retry attempt number (the initial call is not a retry). */
    attempt: number;
    /** Delay selected for this retry after jitter, in milliseconds. */
    delayMs: number;
    /** The transient error that caused the retry. */
    error: unknown;
};

/** @public */
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

/** @public */
export type SparkTelemetryEvent = {
    name: string;
    level: "debug" | "info" | "warn" | "error";
    timestamp: string;
    attributes: Record<string, unknown>;
};

/**
 * Opt-in sink for structured, redacted Spark Connect lifecycle events.
 * @public
 */
export type SparkLogger = (event: Readonly<SparkTelemetryEvent>) => void;

/** @public */
export type SparkMetric = {
    name: string;
    kind: "counter" | "histogram";
    value: number;
    unit: "count" | "milliseconds";
    timestamp: string;
    attributes: Record<string, unknown>;
};

/**
 * Opt-in metrics sink. Observer failures never affect Spark operations.
 * @public
 */
export type SparkMetricObserver = (metric: Readonly<SparkMetric>) => void | PromiseLike<void>;

/** @public */
export type SparkUserContext = {
    user_id: string;
    user_name?: string;
};

/** @public */
export type SessionConfigValue = string | number | boolean;
/** @public */
export type SessionConfigMap = Record<string, SessionConfigValue>;

/** @public */
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
    /**
     * Wall-clock budget in milliseconds for one query execution: the
     * ExecutePlan stream plus every ReattachExecute that continues it. When
     * set, it replaces `rpcTimeoutMs` for execution streams while unary RPCs
     * keep `rpcTimeoutMs`; `Infinity` lets executions run without a client
     * deadline. Unset, every execution stream gets its own `rpcTimeoutMs`.
     */
    executeTimeoutMs?: number;
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

/**
 * State of one operation as reported by `session.getStatus()` (Spark 4.2+).
 * @public
 */
export type OperationState =
    | "UNSPECIFIED"
    | "UNKNOWN"
    | "RUNNING"
    | "TERMINATING"
    | "SUCCEEDED"
    | "FAILED"
    | "CANCELLED";

/** @public */
export type OperationStatus = {
    operationId: string;
    state: OperationState;
};

const OPERATION_STATES: readonly OperationState[] = [
    "UNSPECIFIED", "UNKNOWN", "RUNNING", "TERMINATING", "SUCCEEDED", "FAILED", "CANCELLED",
];

function operationState(value: unknown): OperationState {
    // enums arrive as their proto names ("OPERATION_STATE_RUNNING"); the
    // numeric form only if a loader was configured differently.
    const name = typeof value === "number"
        ? OPERATION_STATES[value]
        : String(value ?? "").replace(/^OPERATION_STATE_/, "");
    return OPERATION_STATES.includes(name as OperationState) ? name as OperationState : "UNKNOWN";
}

/** Schema of `session.range()`: Spark names the column `id` and never makes it null. */
const RANGE_SCHEMA = schema({ id: "long" });

/**
 * Row shape of `session.range()`.
 * @public
 */
export type RangeRow = { id: bigint };

/** Validates a `range()` bound and promotes it to the int64 the relation carries. */
function rangeBound(label: string, value: number | bigint): bigint {
    if (typeof value === "bigint") {
        if (value < -(2n ** 63n) || value > 2n ** 63n - 1n) {
            throw new RangeError(`${label} is outside Spark's signed int64 range.`);
        }
        return value;
    }
    if (!Number.isSafeInteger(value)) {
        throw new TypeError(`${label} must be a safe integer or a bigint.`);
    }
    return BigInt(value);
}
/** gRPC status UNAVAILABLE: the server cannot be reached, so further cleanup RPCs are pointless. */
const GRPC_UNAVAILABLE = 14;

/** @public */
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
    private readonly tags = new Set<string>();
    private serverVersion?: Promise<string>;
    private readonly cachedRelationIds = new Set<string>();
    private streamingQueryManager?: StreamingQueryManager;
    /** Connection config as given to the builder, before runtime `conf.set()` mirrors into it. */
    private builderConnectionConfig: SparkConnectionConfig;

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
        this.builderConnectionConfig = this.connectionConfig;
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

    /**
     * Builds a DataFrame from local rows. The declared schema is the single
     * source of truth: it types the rows at compile time, drives the Arrow
     * encoding, and travels to Spark as the `LocalRelation` schema. Value
     * representations are the ones `collect()` returns (`bigint` for `long`,
     * `"YYYY-MM-DD"` for `date`, ISO-8601 strings for `timestamp`), so
     * `createDataFrame(await df.collect(), Schema)` round-trips.
     *
     * Decimals are strings, arrays are arrays, maps are `Map` instances
     * (plain objects are accepted for string keys) and structs are objects;
     * nested values may be null, as in the rows Spark returns.
     */
    createDataFrame<D extends SchemaDef, R = unknown, E = unknown, G = unknown>(
        rows: ReadonlyArray<InferSchema<D>>,
        declared: DeclaredSchema<D>,
    ): ReadChainedDataFrame<InferSchema<D>, R, E, G> {
        this.assertOpen();
        if (!Array.isArray(rows)) {
            throw new TypeError("createDataFrame() rows must be an array.");
        }
        if (!(declared instanceof DeclaredSchema)) {
            throw new TypeError("createDataFrame() requires a schema declared with schema({...}).");
        }
        const data = rows.length === 0
            ? undefined
            : encodeLocalRelation(rows as ReadonlyArray<Record<string, unknown>>, declared.def);
        const ddl = declared.toDDL();
        const rowCount = rows.length;
        const prog: DFProgram<R, E, G> = DF => DF.localRelation(ddl, data, rowCount);
        return new ReadChainedDataFrame<InferSchema<D>, R, E, G>(prog, this, false, declared.def);
    }

    /**
     * Spark's `range`: a DataFrame with one `id` column of longs from `start`
     * (0 when only `end` is given) up to, excluding, `end`, stepping by
     * `step`. Bounds accept numbers or bigints; `numPartitions` splits the
     * range server-side.
     */
    range<R = unknown, E = unknown, G = unknown>(end: number | bigint): ReadChainedDataFrame<RangeRow, R, E, G>;
    range<R = unknown, E = unknown, G = unknown>(
        start: number | bigint,
        end: number | bigint,
        step?: number | bigint,
        numPartitions?: number,
    ): ReadChainedDataFrame<RangeRow, R, E, G>;
    range<R = unknown, E = unknown, G = unknown>(
        startOrEnd: number | bigint,
        end?: number | bigint,
        step: number | bigint = 1,
        numPartitions?: number,
    ): ReadChainedDataFrame<RangeRow, R, E, G> {
        this.assertOpen();
        const bounds = end === undefined
            ? { start: undefined, end: rangeBound("range() end", startOrEnd) }
            : { start: rangeBound("range() start", startOrEnd), end: rangeBound("range() end", end) };
        const stride = rangeBound("range() step", step);
        if (stride === 0n) throw new RangeError("range() step must not be 0.");
        if (numPartitions !== undefined && (!Number.isSafeInteger(numPartitions) || numPartitions < 1)) {
            throw new RangeError("range() numPartitions must be a positive integer.");
        }
        const prog: DFProgram<R, E, G> = DF => DF.range(bounds.start, bounds.end, stride, numPartitions);
        return new ReadChainedDataFrame<RangeRow, R, E, G>(prog, this, false, RANGE_SCHEMA.def);
    }

    /**
     * A new, independent session against the same server with the same
     * connection settings and user. Nothing is sent: the server creates its
     * state on first use. Unlike `cloneSession()`, no temporary views or
     * runtime configuration are copied. Session tags are not copied either.
     */
    newSession(): SparkSession {
        this.assertOpen();
        // Runtime `conf.set()` calls are mirrored into connectionConfig; the
        // child must start from what the builder configured, not from them.
        return new SparkSession(undefined, {
            userContext: this.userContext,
            connectionConfig: this.builderConnectionConfig,
        });
    }

    static builder(): SparkSessionBuilder {
        return new SparkSessionBuilder();
    }

    get read(): DataFrameReaderTF {
        this.assertOpen();
        return new DataFrameReaderTF(this);
    }

    /** Server-side catalog metadata (databases, tables, columns, temp views). */
    get catalog(): SparkCatalog {
        this.assertOpen();
        return new SparkCatalog(this);
    }

    /** Remote Spark SQL/runtime configuration for this Connect session. */
    get conf(): SparkRuntimeConfig {
        this.assertOpen();
        return this.runtimeConfig;
    }

    /** The streaming queries of this session: Spark's `StreamingQueryManager`. */
    get streams(): StreamingQueryManager {
        this.assertOpen();
        return this.streamingQueryManager ??= new StreamingQueryManager(this);
    }

    /** @internal Forwards a query-started event to the listeners of `streams`. */
    notifyStreamingQueryStarted(eventJson: string): void {
        this.streamingQueryManager?.dispatchQueryStarted(eventJson);
    }

    getSessionId(): string {
        return this.sessionId;
    }

    /**
     * Adds a tag to every operation this session starts from now on, so
     * `interruptTag(tag)` can cancel them as a group. Executions already in
     * flight keep the tags they started with.
     */
    addTag(tag: string): void {
        assertValidTag(tag);
        this.tags.add(tag);
    }

    /** Stops tagging new operations with `tag`. */
    removeTag(tag: string): void {
        assertValidTag(tag);
        this.tags.delete(tag);
    }

    /** Tags attached to operations started by this session. */
    getTags(): string[] {
        return [...this.tags];
    }

    /** Removes every session tag. */
    clearTags(): void {
        this.tags.clear();
    }

    /** Interrupts every running operation of this session; resolves to their ids. */
    interruptAll(): Promise<string[]> {
        return this.runInterrupt({ interrupt_type: "INTERRUPT_TYPE_ALL" });
    }

    /** Interrupts the running operations tagged with `tag`; resolves to their ids. */
    interruptTag(tag: string): Promise<string[]> {
        try {
            assertValidTag(tag);
        } catch (error) {
            return Promise.reject(error);
        }
        return this.runInterrupt({ interrupt_type: "INTERRUPT_TYPE_TAG", operation_tag: tag });
    }

    /**
     * Interrupts one running operation. Pass the `operationId` given to the
     * action's `ExecutionOptions`; resolves to the ids actually interrupted.
     */
    interruptOperation(operationId: string): Promise<string[]> {
        try {
            assertOperationId(operationId);
        } catch (error) {
            return Promise.reject(error);
        }
        return this.runInterrupt({
            interrupt_type: "INTERRUPT_TYPE_OPERATION_ID",
            operation_id: operationId,
        });
    }

    /**
     * Version of the Spark Connect server, e.g. `"4.1.3"`. Fetched once with
     * AnalyzePlan and cached for the lifetime of the session.
     */
    async version(): Promise<string> {
        this.assertOpen();
        this.serverVersion ??= this.fetchServerVersion().catch(error => {
            this.serverVersion = undefined;
            throw error;
        });
        return this.serverVersion;
    }

    /**
     * @internal Rejects with `UnsupportedServerVersionError`, before the
     * feature's own RPC, when the server is older than `minimum`.
     */
    async requireServerVersion(minimum: string, feature: string): Promise<string> {
        const actual = await this.version();
        if (compareSparkVersions(actual, minimum) < 0) {
            throw new UnsupportedServerVersionError(feature, minimum, actual);
        }
        return actual;
    }

    /**
     * Creates an independent copy of this session on the server (Spark
     * Connect 4.1+): temporary views, runtime configuration and other session
     * state are copied once, and later changes on either side stay local to
     * it. The clone uses the same connection settings; session tags are
     * client-side and are not copied.
     */
    async cloneSession(): Promise<SparkSession> {
        await this.requireServerVersion("4.1.0", "cloneSession()");
        const newSessionId = crypto.randomUUID();
        let response: Record<string, unknown>;
        try {
            response = await sparkGrpcClient.cloneSession({
                session_id: this.sessionId,
                user_context: this.getUserContext(),
                client_type: "ts-spark-connector",
                ...(this.serverSideSessionId
                    ? { client_observed_server_side_session_id: this.serverSideSessionId }
                    : {}),
                new_session_id: newSessionId,
            }, this.getConnectionConfigUnchecked());
        } catch (error) {
            this.observeRemoteError(error);
            throw error;
        }
        this.observeServerSideSessionId({
            session_id: response.session_id,
            server_side_session_id: response.server_side_session_id,
        });
        if (response.new_session_id !== newSessionId) {
            throw new Error(
                `CloneSession returned session id ${String(response.new_session_id)}; expected ${newSessionId}.`
            );
        }
        const clone = new SparkSession(newSessionId, {
            userContext: this.userContext,
            connectionConfig: this.connectionConfig,
        });
        clone.adoptClonedServerSession(response.new_server_side_session_id);
        return clone;
    }

    /**
     * Uploads a local file to the server for this session: a `.jar` joins the
     * session classpath (then `udf.registerJavaFunction()` can use its
     * classes); other files need `kind` `"file"` or `"archive"`. Resolves with
     * the server-side artifact name, e.g. `jars/udfs.jar`.
     */
    async addArtifact(localPath: string, kind?: ArtifactKind): Promise<string> {
        const [name] = await this.addArtifacts({ path: localPath, ...(kind !== undefined ? { kind } : {}) });
        return name!;
    }

    /**
     * Uploads several local files in one `AddArtifacts` stream. Small files
     * are batched, large ones chunked, and every chunk is CRC-checked by the
     * server. Resolves with the server-side artifact names, in order; a CRC
     * failure rejects with `ArtifactUploadError`, which lists the names that
     * did and did not make it. Uploads are bounded like executions (see
     * `withExecuteTimeout()`) and cancelled by the session's abort signal.
     */
    async addArtifacts(...uploads: Array<string | ArtifactUpload>): Promise<string[]> {
        this.assertOpen();
        if (uploads.length === 0) throw new TypeError("addArtifacts() requires at least one file.");
        const normalized: ArtifactUpload[] = uploads.map(upload => typeof upload === "string" ? { path: upload } : upload);
        return this.uploadArtifacts(artifactRequests(this.requestBase(), normalized));
    }

    /**
     * Stores a blob on the server under its SHA-256 (`cache/<hash>`), like
     * PySpark's `cache_artifact`, and resolves with the hash; a blob the
     * server already holds is not sent again. Cached blobs are the one
     * artifact kind the server reports through `isArtifactCached()`.
     */
    async cacheArtifact(data: Uint8Array | string): Promise<string> {
        this.assertOpen();
        if (typeof data !== "string" && !(data instanceof Uint8Array)) {
            throw new TypeError("cacheArtifact() takes a Uint8Array or a string.");
        }
        const bytes = typeof data === "string" ? Buffer.from(data, "utf8") : data;
        if (bytes.byteLength === 0) throw new TypeError("cacheArtifact() requires a non-empty blob.");
        const { hash, requests } = cachedArtifactRequests(this.requestBase(), bytes);
        // Same blob, same hash: skip the upload when the server still has it.
        if (await this.isArtifactCached(hash)) return hash;
        const [name] = await this.uploadArtifacts(requests);
        if (name !== `${CACHE_ARTIFACT_PREFIX}${hash}`) {
            throw new Error(`AddArtifacts confirmed ${String(name)}; expected ${CACHE_ARTIFACT_PREFIX}${hash}.`);
        }
        return hash;
    }

    /** Whether a blob stored with `cacheArtifact()` is still cached on the server. */
    async isArtifactCached(hash: string): Promise<boolean> {
        if (typeof hash !== "string" || !/^[0-9a-f]{64}$/i.test(hash)) {
            throw new TypeError("isArtifactCached() takes the SHA-256 hex hash returned by cacheArtifact().");
        }
        const name = `${CACHE_ARTIFACT_PREFIX}${hash.toLowerCase()}`;
        return (await this.artifactStatus(name))[name] === true;
    }

    /**
     * Raw `ArtifactStatus` lookup by server-side name. Spark answers only for
     * blobs stored by hash (`cache/<hash>`, see `cacheArtifact()`); jars and
     * files always report `false` even after a successful upload.
     */
    async artifactStatus(...names: string[]): Promise<Record<string, boolean>> {
        this.assertOpen();
        if (names.length === 0) throw new TypeError("artifactStatus() requires at least one artifact name.");
        for (const name of names) {
            if (typeof name !== "string" || !name.trim()) throw new TypeError("Artifact names must be non-empty strings.");
        }
        this.markRemoteTouched();
        let response: Record<string, unknown>;
        try {
            response = await sparkGrpcClient.artifactStatus({ ...this.requestBase(), names }, this.getConnectionConfigUnchecked());
        } catch (error) {
            this.observeRemoteError(error);
            throw error;
        }
        this.observeServerSideSessionId(response);
        const statuses = (response.statuses ?? {}) as Record<string, { exists?: boolean } | undefined>;
        return Object.fromEntries(names.map(name => [name, statuses[name]?.exists === true]));
    }

    /** Registration of server-side (Java) user-defined functions. */
    get udf(): SparkUdfRegistration {
        this.assertOpen();
        return new SparkUdfRegistration(this);
    }

    /**
     * States of this session's operations (Spark Connect 4.2+), for the given
     * operation ids or, when none are given, every operation the server still
     * tracks. Pair it with `ExecutionOptions.operationId` to follow a query.
     */
    async getStatus(operationIds: readonly string[] = []): Promise<OperationStatus[]> {
        for (const id of operationIds) assertOperationId(id);
        await this.requireServerVersion("4.2.0", "getStatus()");
        let response: Record<string, unknown>;
        try {
            response = await sparkGrpcClient.getStatus({
                ...this.requestBase(),
                operation_status: { operation_ids: [...operationIds] },
            }, this.getConnectionConfigUnchecked());
        } catch (error) {
            this.observeRemoteError(error);
            throw error;
        }
        this.observeServerSideSessionId(response);
        const statuses = response.operation_statuses ?? response.operationStatuses;
        return (Array.isArray(statuses) ? statuses : []).map(status => {
            const entry = status as { operation_id?: unknown; operationId?: unknown; state?: unknown };
            return { operationId: String(entry.operation_id ?? entry.operationId ?? ""), state: operationState(entry.state) };
        });
    }

    private async uploadArtifacts(requests: Iterable<Record<string, unknown>>): Promise<string[]> {
        this.markRemoteTouched();
        let response: Record<string, unknown>;
        try {
            response = await sparkGrpcClient.addArtifacts(requests, this.getConnectionConfigUnchecked());
        } catch (error) {
            this.observeRemoteError(error);
            throw error;
        }
        this.observeServerSideSessionId(response);
        return uploadedArtifactNames(response);
    }

    /** Identity fields every session-scoped request repeats. */
    private requestBase(): Record<string, unknown> {
        return {
            session_id: this.sessionId,
            user_context: this.getUserContext(),
            client_type: "ts-spark-connector",
            ...(this.serverSideSessionId
                ? { client_observed_server_side_session_id: this.serverSideSessionId }
                : {}),
        };
    }

    /** @internal Remembers a checkpointed relation so `close()` releases it on the server. */
    trackCachedRelation(relationId: string): void {
        this.cachedRelationIds.add(relationId);
    }

    /** @internal Forgets a relation about to be released; false when it was released already. */
    untrackCachedRelation(relationId: string): boolean {
        return this.cachedRelationIds.delete(relationId);
    }

    /** @internal Snapshots per-execution controls when an action starts. */
    resolveExecution(options?: ExecutionOptions): ResolvedExecution {
        return resolveExecution(this.tags, options);
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
        this.builderConnectionConfig = this.connectionConfig;
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
        // "" is proto3's unset string, not a foreign session.
        if (responseSessionId !== undefined && responseSessionId !== "" && responseSessionId !== this.sessionId) {
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

    /**
     * Releases remote session state and closes the cached gRPC channel.
     * Idempotent. Local state is released even when the server cannot be
     * reached: the session is closed afterwards and the ReleaseSession failure
     * is rethrown so the caller knows remote state may linger until Spark's
     * session timeout.
     */
    async close(): Promise<void> {
        if (this.closed) return;
        if (this.closePromise) return this.closePromise;
        const config = this.getConnectionConfigUnchecked();
        const cleanupConfig = cleanupConnectionConfig(config);

        this.closePromise = (async () => {
            let releaseFailure: { error: unknown } | undefined;
            // The listener bus holds a long-running execution; stop following
            // it before the session that owns it is released.
            if (this.streamingQueryManager) await this.streamingQueryManager.shutdown();
            if (this.remoteTouched && !this.invalidatedError) {
                // Only when something was checkpointed: the extra await would
                // otherwise delay ReleaseSession by a microtask for nothing.
                if (this.cachedRelationIds.size > 0) await this.releaseCachedRelations(cleanupConfig);
                try {
                    const response = await sparkGrpcClient.releaseSession({
                        session_id: this.sessionId,
                        user_context: this.getUserContext(),
                        client_type: "ts-spark-connector",
                    }, cleanupConfig);
                    this.observeServerSideSessionId(response);
                } catch (error) {
                    this.observeRemoteError(error);
                    // A stale identity has no remote session left to release;
                    // any other failure is reported after local cleanup.
                    if (!this.invalidatedError) releaseFailure = { error };
                }
            }
            this.closed = true;
            if (this.clientRetained) {
                sparkGrpcClient.close(config);
                this.clientRetained = false;
                this.clientCacheIdentity = undefined;
            }
            if (releaseFailure) throw releaseFailure.error;
        })().finally(() => {
            this.closePromise = undefined;
        });
        return this.closePromise;
    }

    /**
     * Checkpointed relations not released with `releaseCheckpoint()` are
     * released here, as PySpark does when the DataFrame is garbage collected.
     * Best effort and bounded: every release runs at once under the cleanup
     * deadline, the first UNAVAILABLE cancels the rest (ReleaseSession drops
     * the cache anyway), and no failure blocks closing.
     */
    private async releaseCachedRelations(config: SparkConnectionConfig): Promise<void> {
        const relationIds = [...this.cachedRelationIds];
        this.cachedRelationIds.clear();
        const unreachable = new AbortController();
        const releaseConfig = { ...config, signal: unreachable.signal };
        await Promise.all(relationIds.map(relationId => sparkGrpcClient.executePlan({
            plan: { command: { remove_cached_remote_relation_command: { relation: { relation_id: relationId } } } },
            session_id: this.sessionId,
            user_context: this.getUserContext(),
            client_type: "ts-spark-connector",
            operation_id: crypto.randomUUID(),
            ...(this.serverSideSessionId
                ? { client_observed_server_side_session_id: this.serverSideSessionId }
                : {}),
        }, releaseConfig).catch(error => {
            if ((error as { code?: unknown } | null)?.code === GRPC_UNAVAILABLE) unreachable.abort(error);
        })));
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

    private async fetchServerVersion(): Promise<string> {
        this.markRemoteTouched();
        let response: Record<string, unknown>;
        try {
            response = await sparkGrpcClient.analyze({
                session_id: this.sessionId,
                user_context: this.getUserContext(),
                client_type: "ts-spark-connector",
                ...(this.serverSideSessionId
                    ? { client_observed_server_side_session_id: this.serverSideSessionId }
                    : {}),
                spark_version: {},
            }, this.getConnectionConfigUnchecked());
        } catch (error) {
            this.observeRemoteError(error);
            throw error;
        }
        this.observeServerSideSessionId(response);
        const version = (response.spark_version as { version?: unknown } | undefined)?.version;
        if (typeof version !== "string" || !version) {
            throw new Error("Invalid AnalyzePlan response: missing spark_version.");
        }
        return version;
    }

    /**
     * A clone already exists on the server with the source's configuration
     * applied, so builder-provided configs must not be re-sent over the
     * values it copied.
     */
    private adoptClonedServerSession(serverSideSessionId: unknown): void {
        this.markRemoteTouched();
        this.initialConfigApplied = true;
        this.observeServerSideSessionId({ session_id: this.sessionId, server_side_session_id: serverSideSessionId });
    }

    private async runInterrupt(fields: Record<string, unknown>): Promise<string[]> {
        this.assertOpen();
        // Nothing can be running on a session that never reached the server.
        if (!this.remoteTouched) return [];
        let response: Record<string, unknown>;
        try {
            response = await sparkGrpcClient.interrupt({
                session_id: this.sessionId,
                user_context: this.getUserContext(),
                client_type: "ts-spark-connector",
                ...(this.serverSideSessionId
                    ? { client_observed_server_side_session_id: this.serverSideSessionId }
                    : {}),
                ...fields,
            }, this.getConnectionConfigUnchecked());
        } catch (error) {
            this.observeRemoteError(error);
            throw error;
        }
        this.observeServerSideSessionId(response);
        const ids = response.interrupted_ids ?? response.interruptedIds;
        return Array.isArray(ids) ? ids.filter((id): id is string => typeof id === "string") : [];
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

/**
 * Spark Connect runtime configuration facade backed by the Config RPC.
 * @public
 */
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

/** @public */
export function createSparkSession(sessionId?: string): SparkSession {
    return new SparkSession(sessionId);
}

const AUTH_SOURCE_CONFLICT_MESSAGE =
    "Authentication is configured through both withAuth() and legacy spark.auth.* keys. " +
    "Use a single source.";

/** @public */
export class SparkSessionBuilder {
    private configMap: SessionConfigMap = {};
    private auth?: AuthConfig;
    private authSource?: "api" | "legacy";
    private s3CredentialsConfigured = false;
    private insecureAuthAllowed = false;
    private rpcTimeoutMs?: number;
    private executeTimeoutMs?: number;
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
        if (this.authSource === "legacy") {
            throw new Error(AUTH_SOURCE_CONFLICT_MESSAGE);
        }
        this.authSource = "api";
        this.auth = { ...auth };
        this.resetAuthDraft(auth);
        return this;
    }

    /**
     * Forwards AWS credentials to the Spark job as `spark.hadoop.fs.s3a.*`
     * session configuration so `s3a://` paths resolve on the server. With a
     * `sessionToken`, S3A's `TemporaryAWSCredentialsProvider` is selected as
     * well, which temporary STS credentials require.
     *
     * The values travel to the server in Config RPCs, so `getOrCreate()`
     * refuses a plaintext channel unless `allowInsecureAuth()` is set.
     */
    withS3Credentials(credentials: S3Credentials): this {
        const { accessKeyId, secretAccessKey, sessionToken } = credentials;
        if (typeof accessKeyId !== "string" || !accessKeyId.trim()
            || typeof secretAccessKey !== "string" || !secretAccessKey.trim()) {
            throw new TypeError("withS3Credentials requires non-empty accessKeyId and secretAccessKey strings.");
        }
        if (sessionToken !== undefined && (typeof sessionToken !== "string" || !sessionToken.trim())) {
            throw new TypeError("withS3Credentials sessionToken must be a non-empty string when provided.");
        }

        this.s3CredentialsConfigured = true;
        this.config("spark.hadoop.fs.s3a.access.key", accessKeyId);
        this.config("spark.hadoop.fs.s3a.secret.key", secretAccessKey);
        if (sessionToken !== undefined) {
            this.config("spark.hadoop.fs.s3a.session.token", sessionToken);
            this.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            );
        }
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

    /**
     * Bounds each query execution as a whole, across ReattachExecute
     * continuations, instead of per stream. Pass `Infinity` to keep a
     * `withRpcTimeout()` deadline for unary RPCs without capping how long a
     * query may run.
     */
    withExecuteTimeout(timeoutMs: number): this {
        validateExecuteTimeout(timeoutMs);
        this.executeTimeoutMs = timeoutMs;
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
        this.assertS3CredentialTransport();
        const session = new SparkSession(undefined, {
            userContext: this.userContext,
            connectionConfig: {
                address: this.readConfiguredAddress(),
                auth: this.auth,
                allowInsecureAuth: this.insecureAuthAllowed,
                rpcTimeoutMs: this.rpcTimeoutMs,
                executeTimeoutMs: this.executeTimeoutMs,
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
        if (!LEGACY_AUTH_KEYS.has(key)) return;
        if (this.authSource === "api") {
            throw new Error(AUTH_SOURCE_CONFLICT_MESSAGE);
        }
        this.authSource = "legacy";

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
            default:
                // Membership was checked above, so this is "spark.auth.token".
                this.authDraft.token = text;
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

    // Mirrors assertSecureAuthTransport, but runs at build time: S3 secrets
    // ride Config RPCs, so a plaintext channel would expose them just like a
    // Bearer header. Raw .config() writes of the same keys stay unguarded as
    // the documented escape hatch.
    private assertS3CredentialTransport(): void {
        if (!this.s3CredentialsConfigured || this.insecureAuthAllowed || this.tls) return;
        const address = this.readConfiguredAddress() ?? process.env.SPARK_CONNECT_URL ?? "";
        if (/^scs:\/\//i.test(address.trim())) return;

        throw new Error(
            "Refusing to send S3 credentials over an insecure Spark Connect channel. " +
            "Use an scs:// address or enableTLS(); for trusted local development only, " +
            "set allowInsecureAuth to true."
        );
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

/** @public */
export const spark = createSparkSession();
