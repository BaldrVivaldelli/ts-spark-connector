import type * as Grpc from "@grpc/grpc-js";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import tls from "node:tls";
import type { AuthConfig, SessionConfigMap, SparkConnectionConfig, TLSConfig } from "./session";
import { resolveRetryConfig, withRetry } from "./retry";
import {
    executeReattachable,
    RpcMessage,
    RpcReadable,
} from "./reattachableExecution";
import { emitTelemetry } from "./telemetry";
import {
    buildCallOptions,
    callUnary,
    callUnaryWithRetry,
    emitExecuteCleanupTelemetry,
    enrichSparkConnectError,
} from "./rpcLifecycle";
export {
    assertRpcResponseSessionIntegrity,
    emitExecuteCleanupTelemetry,
} from "./rpcLifecycle";
import {
    ExecutePlanResponse,
    StreamStartResult,
    StreamingQueryCommandResult,
    buildStreamingQueryCommandRequest,
    extractAwaitTerminationResult,
    extractStreamStart,
    extractStreamingException,
    extractStreamingQueryCommandResult,
    extractStreamingQueryId,
    validateTerminationTimeout,
} from "./streamingProtocol";

export interface StreamingQueryHandle {
    awaitTermination(): Promise<void>;
    awaitTermination(timeoutMs: number): Promise<boolean>;
    stop(): Promise<unknown>;
    name: string;
    /** @internal Latest server identity observed while controlling the query. */
    readonly serverSideSessionId?: string;
}

type AnalyzePlanResponse = RpcMessage & {
    explain?: {
        explain_string?: string;
        explainString?: string;
    };
    explain_string?: string;
    explainString?: string;
};

type UnaryCallback<T> = (error: Error | null, response: T) => void;

interface SparkConnectServiceClient {
    executePlan(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions): RpcReadable<ExecutePlanResponse>;
    reattachExecute(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions): RpcReadable<ExecutePlanResponse>;
    releaseExecute(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions, callback: UnaryCallback<RpcMessage>): Grpc.ClientUnaryCall;
    analyzePlan(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions, callback: UnaryCallback<AnalyzePlanResponse>): Grpc.ClientUnaryCall;
    config(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions, callback: UnaryCallback<RpcMessage>): Grpc.ClientUnaryCall;
    interrupt(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions, callback: UnaryCallback<RpcMessage>): Grpc.ClientUnaryCall;
    releaseSession(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions, callback: UnaryCallback<RpcMessage>): Grpc.ClientUnaryCall;
    fetchErrorDetails(request: RpcMessage, metadata: Grpc.Metadata, options: Grpc.CallOptions, callback: UnaryCallback<RpcMessage>): Grpc.ClientUnaryCall;
    close(): void;
}

interface LoadedSparkConnectProto {
    spark: {
        connect: {
            SparkConnectService: new (
                address: string,
                credentials: Grpc.ChannelCredentials,
                options?: Grpc.ChannelOptions
            ) => SparkConnectServiceClient;
        };
    };
}

const PROTO_ROOT = path.resolve(__dirname, "../../proto");
const BASE_PROTO = path.join(PROTO_ROOT, "spark/connect/base.proto");

let grpcModule: typeof import("@grpc/grpc-js") | undefined;
let loadedProto: LoadedSparkConnectProto | undefined;

function grpcRuntime(): typeof import("@grpc/grpc-js") {
    return grpcModule ??= require("@grpc/grpc-js") as typeof import("@grpc/grpc-js");
}

function sparkConnectProto(): LoadedSparkConnectProto {
    if (loadedProto) return loadedProto;
    const protoLoader = require("@grpc/proto-loader") as typeof import("@grpc/proto-loader");
    const packageDefinition = protoLoader.loadSync([BASE_PROTO], {
        includeDirs: [PROTO_ROOT],
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    });
    loadedProto = grpcRuntime().loadPackageDefinition(packageDefinition) as unknown as LoadedSparkConnectProto;
    return loadedProto;
}

type ClientContext = {
    client: SparkConnectServiceClient;
    metadata: Grpc.Metadata;
};

const clientCache = new Map<string, SparkConnectServiceClient>();
const clientReferences = new Map<string, number>();
const clientCacheFingerprintKey = crypto.randomBytes(32);
/** @internal Pins a live SparkSession to the credential snapshot it retained. */
export const SPARK_CLIENT_CACHE_IDENTITY: unique symbol = Symbol("sparkClientCacheIdentity");
export const DEFAULT_GRPC_MAX_MESSAGE_BYTES = 128 * 1024 * 1024;
const MAX_GRPC_MESSAGE_BYTES = 2_147_483_647;

function getConfiguredAddress(config?: SparkConnectionConfig): string {
    if (config?.address?.trim()) {
        return config.address.trim();
    }

    const map = config?.sessionConfig ?? {};
    for (const key of ["spark.connect.url", "spark.connect.address", "SPARK_CONNECT_URL"]) {
        const value = map[key];
        if (value != null && String(value).trim()) {
            return String(value).trim();
        }
    }

    return process.env.SPARK_CONNECT_URL?.trim() || "sc://localhost:15002";
}

function normalizeSparkConnectAddress(rawAddress: string): string {
    return rawAddress
        .replace(/^scs?:\/\//i, "")
        .replace(/\/+$/, "");
}

function resolveConfiguredAddress(config?: SparkConnectionConfig): string {
    return normalizeSparkConnectAddress(getConfiguredAddress(config));
}

function readFileIfPresent(filePath?: string): Buffer | undefined {
    if (!filePath) return undefined;
    const resolvedPath = path.resolve(filePath);
    if (!fs.existsSync(resolvedPath)) {
        throw new Error(`TLS file not found: ${resolvedPath}`);
    }
    return fs.readFileSync(resolvedPath);
}

function isPkcs12Keystore(keyStorePath?: string): boolean {
    return !!keyStorePath && /\.(p12|pfx)$/i.test(keyStorePath);
}

function isJksStore(storePath?: string): boolean {
    return !!storePath && /\.jks$/i.test(storePath);
}

function validateTlsStoreFormats(tlsConfig?: TLSConfig): void {
    if (!tlsConfig) return;
    if (isJksStore(tlsConfig.keyStorePath) || isJksStore(tlsConfig.trustStorePath)) {
        throw new Error(
            "Java JKS stores are not supported. Use a .p12/.pfx keyStorePath for client keys " +
            "and a PEM trustStorePath for CA certificates."
        );
    }
    if (tlsConfig.keyStorePath && !isPkcs12Keystore(tlsConfig.keyStorePath)) {
        throw new Error(
            "TLS keyStorePath supports only PKCS#12 (.p12/.pfx). " +
            "For PEM client credentials, use certChainPath and privateKeyPath."
        );
    }
    if (isPkcs12Keystore(tlsConfig.trustStorePath)) {
        throw new Error(
            `TLS trustStorePath "${tlsConfig.trustStorePath}" must be a PEM CA certificate; ` +
            "PKCS#12 trust stores are not supported."
        );
    }
    const hasPemCertificate = !!tlsConfig.certChainPath;
    const hasPemPrivateKey = !!tlsConfig.privateKeyPath;
    if (hasPemCertificate !== hasPemPrivateKey) {
        throw new Error(
            "PEM mutual TLS requires both certChainPath and privateKeyPath."
        );
    }
    if (tlsConfig.keyStorePath && (hasPemCertificate || hasPemPrivateKey)) {
        throw new Error(
            "Configure either a PKCS#12 keyStorePath or PEM certChainPath/privateKeyPath, not both."
        );
    }
}

// grpc-js channel credentials accept PEM material directly via createSsl, but
// not Java-style PKCS#12 keystores (.p12/.pfx). Node's stdlib has no public
// PKCS#12 parser, however tls.createSecureContext() understands the `pfx`
// option natively (via the bundled OpenSSL), and grpc-js can build credentials
// from a SecureContext. Chaining the two lets us support PKCS#12 keystores
// without any extra dependency.
function buildPkcs12Credentials(tlsConfig: TLSConfig): Grpc.ChannelCredentials {
    // Called only after isPkcs12Keystore(keyStorePath) succeeds; the reader
    // either returns the file or throws a precise not-found error.
    const pfx = readFileIfPresent(tlsConfig.keyStorePath)!;

    // The CA used to verify the server. trustStorePath is expected to be a PEM
    // certificate; a PKCS#12 truststore is not supported here.
    const ca = readFileIfPresent(tlsConfig.trustStorePath);

    let secureContext;
    try {
        secureContext = tls.createSecureContext({
            pfx,
            passphrase: tlsConfig.keyStorePassword,
            ...(ca ? { ca } : {}),
        });
    } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        throw new Error(
            `Failed to load PKCS#12 keystore "${tlsConfig.keyStorePath}". ` +
            "Check that keyStorePassword is correct and the file is a valid .p12/.pfx. " +
            `Underlying error: ${reason}`,
            { cause: error },
        );
    }

    return grpcRuntime().credentials.createFromSecureContext(secureContext);
}

function isTlsEnabled(config?: SparkConnectionConfig): boolean {
    const tlsConfig = config?.tls ?? readLegacyTlsConfig(config?.sessionConfig);
    return Boolean(
        tlsConfig || /^scs:\/\//i.test(getConfiguredAddress(config))
    );
}

export function buildChannelCredentials(config?: SparkConnectionConfig): Grpc.ChannelCredentials {
    if (!isTlsEnabled(config)) {
        return grpcRuntime().credentials.createInsecure();
    }

    const tlsConfig = config?.tls ?? readLegacyTlsConfig(config?.sessionConfig);
    validateTlsStoreFormats(tlsConfig);

    if (tlsConfig && isPkcs12Keystore(tlsConfig.keyStorePath)) {
        return buildPkcs12Credentials(tlsConfig);
    }

    const rootCert = readFileIfPresent(tlsConfig?.trustStorePath);
    const privateKey = readFileIfPresent(tlsConfig?.privateKeyPath);
    const certChain = readFileIfPresent(tlsConfig?.certChainPath);

    return grpcRuntime().credentials.createSsl(rootCert, privateKey, certChain);
}

function getSessionConfigString(sessionConfig: SessionConfigMap, key: string): string | undefined {
    const value = sessionConfig[key];
    if (value == null) return undefined;
    const text = String(value).trim();
    return text ? text : undefined;
}

function readLegacyTlsConfig(sessionConfig?: SessionConfigMap): TLSConfig | undefined {
    if (!sessionConfig) return undefined;
    const enabled =
        sessionConfig["spark.ssl.enabled"] === true ||
        sessionConfig["spark.ssl.enabled"] === "true" ||
        sessionConfig["spark.connect.grpc.ssl.enabled"] === true ||
        sessionConfig["spark.connect.grpc.ssl.enabled"] === "true";
    const tlsConfig: TLSConfig = {
        keyStorePath: getSessionConfigString(sessionConfig, "spark.ssl.keyStore"),
        keyStorePassword: getSessionConfigString(sessionConfig, "spark.ssl.keyStorePassword"),
        trustStorePath: getSessionConfigString(sessionConfig, "spark.ssl.trustStore"),
        trustStorePassword: getSessionConfigString(sessionConfig, "spark.ssl.trustStorePassword"),
        certChainPath: getSessionConfigString(sessionConfig, "spark.ssl.certChain"),
        privateKeyPath: getSessionConfigString(sessionConfig, "spark.ssl.privateKey"),
        serverNameOverride: getSessionConfigString(sessionConfig, "spark.ssl.serverNameOverride"),
    };

    const hasTlsMaterial = Object.values(tlsConfig).some(value => value !== undefined);
    return enabled || hasTlsMaterial ? tlsConfig : undefined;
}

function readLegacyAuthConfig(sessionConfig?: SessionConfigMap): AuthConfig | undefined {
    if (!sessionConfig) return undefined;

    const authType = getSessionConfigString(sessionConfig, "spark.auth.type");
    if (authType === "token") {
        const token = getSessionConfigString(sessionConfig, "spark.auth.token");
        return token ? { type: "token", token } : undefined;
    }
    if (authType === "basic") {
        const username = getSessionConfigString(sessionConfig, "spark.auth.username");
        const password = getSessionConfigString(sessionConfig, "spark.auth.password");
        return username && password
            ? { type: "basic", username, password }
            : undefined;
    }

    return undefined;
}

function getPlaintextAuthorizationScheme(config?: SparkConnectionConfig): "Basic" | "Bearer" | undefined {
    const auth = config?.auth ?? readLegacyAuthConfig(config?.sessionConfig);
    if (auth?.type === "basic") return "Basic";
    if (auth?.type === "token") return "Bearer";

    for (const [key, value] of Object.entries(config?.sessionConfig ?? {})) {
        if (!key.startsWith("spark.connect.header.")) continue;
        const headerName = key.slice("spark.connect.header.".length).trim();
        if (headerName.toLowerCase() !== "authorization") continue;
        const match = /^\s*(Basic|Bearer)\s+/i.exec(String(value));
        if (match?.[1]?.toLowerCase() === "basic") return "Basic";
        if (match?.[1]?.toLowerCase() === "bearer") return "Bearer";
    }

    return undefined;
}

/** @internal Guard invoked before a channel is created or reused. */
export function assertSecureAuthTransport(config?: SparkConnectionConfig): void {
    const scheme = getPlaintextAuthorizationScheme(config);
    if (!scheme || isTlsEnabled(config) || config?.allowInsecureAuth === true) {
        return;
    }

    throw new Error(
        `Refusing to send ${scheme} credentials over an insecure Spark Connect channel. ` +
        "Use an scs:// address or enableTLS(); for trusted local development only, " +
        "set allowInsecureAuth to true."
    );
}

function buildMetadata(config?: SparkConnectionConfig): Grpc.Metadata {
    const metadata = new (grpcRuntime().Metadata)();
    const auth = config?.auth ?? readLegacyAuthConfig(config?.sessionConfig);

    if (auth?.type === "token") {
        metadata.set("authorization", `Bearer ${auth.token}`);
    } else if (auth?.type === "basic") {
        const encoded = Buffer.from(`${auth.username}:${auth.password}`, "utf8").toString("base64");
        metadata.set("authorization", `Basic ${encoded}`);
    }

    const sessionConfig = config?.sessionConfig ?? {};
    for (const [key, value] of Object.entries(sessionConfig)) {
        if (!key.startsWith("spark.connect.header.")) continue;
        const headerName = key.slice("spark.connect.header.".length).trim();
        if (!headerName || value == null) continue;
        metadata.set(headerName, String(value));
    }

    return metadata;
}

function resolveGrpcMessageLimit(name: string, value?: number): number {
    const resolved = value ?? DEFAULT_GRPC_MAX_MESSAGE_BYTES;
    if (!Number.isSafeInteger(resolved) || resolved <= 0 || resolved > MAX_GRPC_MESSAGE_BYTES) {
        throw new RangeError(`${name} must be a positive integer no greater than ${MAX_GRPC_MESSAGE_BYTES}.`);
    }
    return resolved;
}

export function buildChannelOptions(config?: SparkConnectionConfig): Grpc.ChannelOptions {
    const tls = config?.tls ?? readLegacyTlsConfig(config?.sessionConfig);
    return {
        "grpc.max_receive_message_length": resolveGrpcMessageLimit(
            "grpcMaxReceiveMessageBytes",
            config?.grpcMaxReceiveMessageBytes
        ),
        "grpc.max_send_message_length": resolveGrpcMessageLimit(
            "grpcMaxSendMessageBytes",
            config?.grpcMaxSendMessageBytes
        ),
        ...(tls?.serverNameOverride
            ? {
                "grpc.ssl_target_name_override": tls.serverNameOverride,
                "grpc.default_authority": tls.serverNameOverride,
            }
            : {}),
    };
}

/** @internal Stable channel identity; authentication metadata is intentionally excluded. */
export function getClientCacheKey(config?: SparkConnectionConfig): string {
    const pinnedIdentity = (config as SparkConnectionConfig & {
        [SPARK_CLIENT_CACHE_IDENTITY]?: string;
    } | undefined)?.[SPARK_CLIENT_CACHE_IDENTITY];
    if (pinnedIdentity) return pinnedIdentity;

    const tls = config?.tls ?? readLegacyTlsConfig(config?.sessionConfig);
    validateTlsStoreFormats(tls);
    const secure = isTlsEnabled(config);
    const configuredAddress = getConfiguredAddress(config);
    const scheme = /^scs:\/\//i.test(configuredAddress)
        ? "scs"
        : (/^sc:\/\//i.test(configuredAddress) ? "sc" : "bare");
    const fingerprintFile = (filePath?: string): string | undefined => {
        if (!filePath) return undefined;
        const resolved = path.resolve(filePath);
        const fingerprint = crypto.createHmac("sha256", clientCacheFingerprintKey)
            .update(resolved)
            .update("\0");
        try {
            fingerprint.update(fs.readFileSync(resolved));
        } catch (error) {
            const code = (error as { code?: string }).code;
            if (code !== "ENOENT") throw error;
            fingerprint.update("[MISSING]");
        }
        return fingerprint.digest("hex");
    };
    const tlsFingerprint = tls
        ? crypto.createHmac("sha256", clientCacheFingerprintKey).update(JSON.stringify({
            keyStoreMaterial: fingerprintFile(tls.keyStorePath),
            keyStorePassword: tls.keyStorePassword,
            trustStoreMaterial: fingerprintFile(tls.trustStorePath),
            trustStorePassword: tls.trustStorePassword,
            certChainMaterial: fingerprintFile(tls.certChainPath),
            privateKeyMaterial: fingerprintFile(tls.privateKeyPath),
            serverNameOverride: tls.serverNameOverride,
        })).digest("hex")
        : undefined;
    const grpcMaxReceiveMessageBytes = resolveGrpcMessageLimit(
        "grpcMaxReceiveMessageBytes",
        config?.grpcMaxReceiveMessageBytes
    );
    const grpcMaxSendMessageBytes = resolveGrpcMessageLimit(
        "grpcMaxSendMessageBytes",
        config?.grpcMaxSendMessageBytes
    );

    return JSON.stringify({
        address: resolveConfiguredAddress(config),
        scheme,
        secure,
        tlsFingerprint,
        grpcMaxReceiveMessageBytes,
        grpcMaxSendMessageBytes,
    });
}

function getClientContext(config?: SparkConnectionConfig): ClientContext {
    assertSecureAuthTransport(config);
    const cacheKey = getClientCacheKey(config);
    let client = clientCache.get(cacheKey);

    if (!client) {
        const SparkConnectService = sparkConnectProto().spark.connect.SparkConnectService;
        client = new SparkConnectService(
            resolveConfiguredAddress(config),
            buildChannelCredentials(config),
            buildChannelOptions(config)
        );
        clientCache.set(cacheKey, client);
    }

    return {
        client,
        metadata: buildMetadata(config),
    };
}

/** @internal Number of live SparkSession owners for a cached channel identity. */
export function getClientReferenceCount(config?: SparkConnectionConfig): number {
    return clientReferences.get(getClientCacheKey(config)) ?? 0;
}

function retainClient(config?: SparkConnectionConfig): string {
    const cacheKey = getClientCacheKey(config);
    clientReferences.set(cacheKey, (clientReferences.get(cacheKey) ?? 0) + 1);
    return cacheKey;
}

function releaseClient(config?: SparkConnectionConfig): void {
    const cacheKey = getClientCacheKey(config);
    const references = clientReferences.get(cacheKey) ?? 0;
    if (references > 1) {
        clientReferences.set(cacheKey, references - 1);
        return;
    }
    clientReferences.delete(cacheKey);
    const client = clientCache.get(cacheKey);
    if (!client) return;
    clientCache.delete(cacheKey);
    client.close();
}

function extractExplainString(response: AnalyzePlanResponse): string | undefined {
    return response.explain?.explain_string
        ?? response.explain?.explainString
        ?? response.explain_string
        ?? response.explainString;
}

export const sparkGrpcClient = {
    /**
     * Incremental ExecutePlan response stream with bounded client-side buffering.
     * Consumers that stop iterating cancel the underlying gRPC call.
     */
    async *executePlanStream(
        request: RpcMessage,
        config?: SparkConnectionConfig
    ): AsyncGenerator<ExecutePlanResponse, void, void> {
        const { client, metadata } = getClientContext(config);
        let latestObservedServerSessionId = request.client_observed_server_side_session_id;
        const cleanupConfig = config ? { ...config, signal: undefined } : undefined;
        const startedAt = Date.now();
        let responseCount = 0;
        emitTelemetry(config, "spark.execute.start", "debug", {
            operationId: request.operation_id,
        });
        try {
            for await (const response of executeReattachable(request, {
                execute: executeRequest => client.executePlan(
                    executeRequest,
                    metadata,
                    buildCallOptions(config)
                ),
                reattach: reattachRequest => {
                    emitTelemetry(config, "spark.execute.reattach", "info", {
                        operationId: reattachRequest.operation_id,
                        lastResponseId: reattachRequest.last_response_id,
                    });
                    return client.reattachExecute(
                        reattachRequest,
                        metadata,
                        buildCallOptions(config)
                    );
                },
                release: releaseRequest => withRetry(
                    () => callUnary(
                        client.releaseExecute.bind(client),
                        releaseRequest,
                        metadata,
                        cleanupConfig,
                        "ReleaseExecute"
                    ),
                    resolveRetryConfig(cleanupConfig)
                ),
            }, {
                retry: resolveRetryConfig(config),
                signal: config?.signal,
                onCleanupError: cleanupError =>
                    emitExecuteCleanupTelemetry(config, request.operation_id, cleanupError),
            })) {
                latestObservedServerSessionId = response.server_side_session_id
                    ?? response.serverSideSessionId
                    ?? latestObservedServerSessionId;
                responseCount += 1;
                yield response;
            }
            emitTelemetry(config, "spark.execute.end", "debug", {
                operationId: request.operation_id,
                responseCount,
                durationMs: Date.now() - startedAt,
            });
        } catch (error) {
            emitTelemetry(config, "spark.execute.error", "error", {
                operationId: request.operation_id,
                responseCount,
                durationMs: Date.now() - startedAt,
                code: (error as { code?: unknown } | null)?.code,
            });
            throw await enrichSparkConnectError(error, "ExecutePlan", {
                ...request,
                ...(latestObservedServerSessionId
                    ? { client_observed_server_side_session_id: latestObservedServerSessionId }
                    : {}),
            }, client, metadata, config);
        }
    },

    async executePlan(request: RpcMessage, config?: SparkConnectionConfig): Promise<ExecutePlanResponse[]> {
        const results: ExecutePlanResponse[] = [];
        for await (const response of this.executePlanStream(request, config)) {
            results.push(response);
        }
        return results;
    },

    async explainWithResponse(
        request: RpcMessage,
        config?: SparkConnectionConfig
    ): Promise<{ explainString: string; response: AnalyzePlanResponse }> {
        const { client, metadata } = getClientContext(config);
        const response = await callUnaryWithRetry(
            client,
            client.analyzePlan.bind(client),
            request,
            metadata,
            config,
            "AnalyzePlan"
        );
        const explainString = extractExplainString(response);
        if (typeof explainString !== "string") {
            throw new Error("Invalid AnalyzePlan response: expected explain string.");
        }
        return { explainString, response };
    },

    async explain(request: RpcMessage, config?: SparkConnectionConfig): Promise<string> {
        return (await this.explainWithResponse(request, config)).explainString;
    },

    /** Executes a non-explain AnalyzePlan operation such as persist/unpersist. */
    async analyze(request: RpcMessage, config?: SparkConnectionConfig): Promise<RpcMessage> {
        const { client, metadata } = getClientContext(config);
        return callUnaryWithRetry(
            client,
            client.analyzePlan.bind(client),
            request,
            metadata,
            config,
            "AnalyzePlan"
        );
    },

    async interrupt(request: RpcMessage, config?: SparkConnectionConfig): Promise<RpcMessage> {
        const { client, metadata } = getClientContext(config);
        return callUnaryWithRetry(
            client,
            client.interrupt.bind(client),
            request,
            metadata,
            config,
            "Interrupt"
        );
    },

    /** Executes Spark Connect's session configuration RPC. */
    async config(request: RpcMessage, config?: SparkConnectionConfig): Promise<RpcMessage> {
        const { client, metadata } = getClientContext(config);
        return callUnaryWithRetry(
            client,
            client.config.bind(client),
            request,
            metadata,
            config,
            "Config"
        );
    },

    /** Releases all server-side state associated with a Spark Connect session. */
    async releaseSession(request: RpcMessage, config?: SparkConnectionConfig): Promise<RpcMessage> {
        const { client, metadata } = getClientContext(config);
        return callUnaryWithRetry(
            client,
            client.releaseSession.bind(client),
            request,
            metadata,
            config,
            "ReleaseSession"
        );
    },

    /** Retrieves structured details for a previously returned Spark error id. */
    async fetchErrorDetails(request: RpcMessage, config?: SparkConnectionConfig): Promise<RpcMessage> {
        const { client, metadata } = getClientContext(config);
        const retryConfig = resolveRetryConfig(config);
        return withRetry(
            () => callUnary(client.fetchErrorDetails.bind(client), request, metadata, config, "FetchErrorDetails"),
            retryConfig,
            { signal: config?.signal }
        );
    },

    /** Retains shared ownership of the channel identity without opening it eagerly. */
    retain(config?: SparkConnectionConfig): string {
        return retainClient(config);
    },

    /** Releases one owner and evicts the channel only after the final owner closes. */
    close(config?: SparkConnectionConfig): void {
        releaseClient(config);
    },

    async executePlanStreaming(request: RpcMessage, config?: SparkConnectionConfig): Promise<StreamingQueryHandle> {
        const responses = await sparkGrpcClient.executePlan(request, config);
        let observedServerSideSessionId = responses.reduce<string | undefined>(
            (latest, response) => response.server_side_session_id
                ?? response.serverSideSessionId
                ?? latest,
            typeof request.client_observed_server_side_session_id === "string"
                ? request.client_observed_server_side_session_id
                : undefined,
        );
        const startResult = responses
            .map(extractStreamStart)
            .find((value): value is StreamStartResult => value !== undefined);
        if (!startResult) {
            throw new Error("Streaming query ended before reporting a start result.");
        }
        const queryId = extractStreamingQueryId(startResult);
        if (!queryId) {
            throw new Error("Invalid WriteStreamOperationStartResult: missing query_id.");
        }
        const queryName = startResult.name
            ?? startResult.query_name
            ?? startResult.queryName
            ?? "";

        const executeQueryCommand = async (command: RpcMessage): Promise<ExecutePlanResponse[]> => {
            const commandRequest = buildStreamingQueryCommandRequest({
                ...request,
                ...(observedServerSideSessionId
                    ? { client_observed_server_side_session_id: observedServerSideSessionId }
                    : {}),
            }, queryId, command);
            const commandResponses = await sparkGrpcClient.executePlan(commandRequest, config);
            for (const response of commandResponses) {
                observedServerSideSessionId = response.server_side_session_id
                    ?? response.serverSideSessionId
                    ?? observedServerSideSessionId;
            }
            return commandResponses;
        };

        const fetchQueryException = async (): Promise<void> => {
            const exceptionResponses = await executeQueryCommand({ exception: true });
            const exceptionResult = exceptionResponses
                .map(extractStreamingQueryCommandResult)
                .map(extractStreamingException)
                .find((value): value is NonNullable<StreamingQueryCommandResult["exception"]> =>
                    value !== undefined
                );
            if (!exceptionResult) {
                throw new Error("Invalid streaming exception response: missing exception result.");
            }
            const message = exceptionResult?.exception_message ?? exceptionResult?.exceptionMessage;
            if (!message) return;
            const error = new Error(message) as Error & {
                errorClass?: string;
                remoteStack?: string;
            };
            error.name = "StreamingQueryError";
            error.errorClass = exceptionResult?.error_class ?? exceptionResult?.errorClass;
            error.remoteStack = exceptionResult?.stack_trace ?? exceptionResult?.stackTrace;
            throw error;
        };

        const awaitTermination = async (timeoutMs?: number): Promise<void | boolean> => {
            if (timeoutMs !== undefined) validateTerminationTimeout(timeoutMs);
            const awaitCommand = timeoutMs === undefined
                ? { await_termination: {} }
                : { await_termination: { timeout_ms: String(timeoutMs) } };
            const terminationResponses = await executeQueryCommand(awaitCommand);
            const result = terminationResponses
                .map(extractStreamingQueryCommandResult)
                .find((value): value is StreamingQueryCommandResult => value !== undefined);
            const terminated = extractAwaitTerminationResult(result);
            if (terminated === undefined) {
                throw new Error("Invalid awaitTermination response: missing termination result.");
            }
            if (terminated) await fetchQueryException();
            if (timeoutMs !== undefined) return terminated;
            if (!terminated) {
                throw new Error("Streaming query awaitTermination returned false without a timeout.");
            }
        };

        return {
            name: queryName,
            get serverSideSessionId() { return observedServerSideSessionId; },
            awaitTermination: awaitTermination as StreamingQueryHandle["awaitTermination"],
            stop: async () => {
                await executeQueryCommand({ stop: true });
            },
        };
    },
};
