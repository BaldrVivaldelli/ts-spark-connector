import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import fs from "node:fs";
import path from "node:path";
import type { AuthConfig, SessionConfigMap, SparkConnectionConfig, TLSConfig } from "./session";

export interface StreamingQueryHandle {
    awaitTermination(): Promise<void>;
    stop(): Promise<unknown>;
    name: string;
}

type RpcMessage = Record<string, unknown>;

type StreamingQueryInstanceId = {
    id?: string;
    run_id?: string;
    runId?: string;
};

type NormalizedStreamingQueryInstanceId = {
    id: string;
    run_id: string;
};

type QueryIdCarrier = {
    query_id?: StreamingQueryInstanceId;
    queryId?: StreamingQueryInstanceId;
};

type StreamStartResult = QueryIdCarrier & {
    name?: string;
    query_name?: string;
    queryName?: string;
};

type StreamingQueryCommandResult = QueryIdCarrier & {
    await_termination?: {
        terminated?: boolean;
    };
    awaitTermination?: {
        terminated?: boolean;
    };
};

type ExecutePlanResponse = RpcMessage & {
    write_stream_operation_start_result?: StreamStartResult;
    writeStreamOperationStartResult?: StreamStartResult;
    streaming_query_command_result?: StreamingQueryCommandResult;
    streamingQueryCommandResult?: StreamingQueryCommandResult;
};

type AnalyzePlanResponse = RpcMessage & {
    explain?: {
        explain_string?: string;
        explainString?: string;
    };
    explain_string?: string;
    explainString?: string;
};

type UnaryCallback<T> = (error: Error | null, response: T) => void;

interface RpcReadable<T> {
    cancel(): void;
    on(event: "data", listener: (response: T) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (error: Error) => void): this;
}

interface SparkConnectServiceClient {
    executePlan(request: RpcMessage, metadata: grpc.Metadata): RpcReadable<ExecutePlanResponse>;
    analyzePlan(request: RpcMessage, metadata: grpc.Metadata, callback: UnaryCallback<AnalyzePlanResponse>): void;
    interrupt(request: RpcMessage, metadata: grpc.Metadata, callback: UnaryCallback<RpcMessage>): void;
}

interface LoadedSparkConnectProto {
    spark: {
        connect: {
            SparkConnectService: new (
                address: string,
                credentials: grpc.ChannelCredentials,
                options?: grpc.ChannelOptions
            ) => SparkConnectServiceClient;
        };
    };
}

const PROTO_ROOT = path.resolve(__dirname, "../../proto");
const BASE_PROTO = path.join(PROTO_ROOT, "spark/connect/base.proto");

const packageDefinition = protoLoader.loadSync([BASE_PROTO], {
    includeDirs: [PROTO_ROOT],
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition) as unknown as LoadedSparkConnectProto;

type ClientContext = {
    client: SparkConnectServiceClient;
    metadata: grpc.Metadata;
};

const clientCache = new Map<string, SparkConnectServiceClient>();

function normalizeSparkConnectAddress(rawAddress?: string): string {
    const configured = rawAddress || process.env.SPARK_CONNECT_URL || "sc://localhost:15002";
    return configured
        .replace(/^scs?:\/\//, "")
        .replace(/\/+$/, "");
}

function resolveConfiguredAddress(config?: SparkConnectionConfig): string {
    if (config?.address) {
        return normalizeSparkConnectAddress(config.address);
    }

    const map = config?.sessionConfig ?? {};
    for (const key of ["spark.connect.url", "spark.connect.address", "SPARK_CONNECT_URL"]) {
        const value = map[key];
        if (value != null && String(value).trim()) {
            return normalizeSparkConnectAddress(String(value));
        }
    }

    return normalizeSparkConnectAddress();
}

function readFileIfPresent(filePath?: string): Buffer | undefined {
    if (!filePath) return undefined;
    const resolvedPath = path.resolve(filePath);
    if (!fs.existsSync(resolvedPath)) {
        throw new Error(`TLS file not found: ${resolvedPath}`);
    }
    return fs.readFileSync(resolvedPath);
}

function isTlsEnabled(config?: SparkConnectionConfig): boolean {
    const raw = config?.sessionConfig ?? {};
    return Boolean(
        config?.tls ||
        raw["spark.ssl.enabled"] === true ||
        raw["spark.ssl.enabled"] === "true" ||
        raw["spark.connect.grpc.ssl.enabled"] === true ||
        raw["spark.connect.grpc.ssl.enabled"] === "true" ||
        config?.address?.startsWith("scs://") ||
        process.env.SPARK_CONNECT_URL?.startsWith("scs://")
    );
}

function buildChannelCredentials(config?: SparkConnectionConfig): grpc.ChannelCredentials {
    if (!isTlsEnabled(config)) {
        return grpc.credentials.createInsecure();
    }

    const tls = config?.tls ?? readLegacyTlsConfig(config?.sessionConfig);
    const rootCert = readFileIfPresent(tls?.trustStorePath);
    const privateKey = readFileIfPresent(tls?.privateKeyPath);
    const certChain = readFileIfPresent(tls?.certChainPath);

    return grpc.credentials.createSsl(rootCert, privateKey, certChain);
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
    if (!enabled) return undefined;

    return {
        keyStorePath: getSessionConfigString(sessionConfig, "spark.ssl.keyStore"),
        keyStorePassword: getSessionConfigString(sessionConfig, "spark.ssl.keyStorePassword"),
        trustStorePath: getSessionConfigString(sessionConfig, "spark.ssl.trustStore"),
        trustStorePassword: getSessionConfigString(sessionConfig, "spark.ssl.trustStorePassword"),
        certChainPath: getSessionConfigString(sessionConfig, "spark.ssl.certChain"),
        privateKeyPath: getSessionConfigString(sessionConfig, "spark.ssl.privateKey"),
        serverNameOverride: getSessionConfigString(sessionConfig, "spark.ssl.serverNameOverride"),
    };
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

function buildMetadata(config?: SparkConnectionConfig): grpc.Metadata {
    const metadata = new grpc.Metadata();
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

function buildChannelOptions(config?: SparkConnectionConfig): grpc.ChannelOptions | undefined {
    const tls = config?.tls ?? readLegacyTlsConfig(config?.sessionConfig);
    if (!tls?.serverNameOverride) return undefined;

    return {
        "grpc.ssl_target_name_override": tls.serverNameOverride,
        "grpc.default_authority": tls.serverNameOverride,
    };
}

function getClientCacheKey(config?: SparkConnectionConfig): string {
    const tls = config?.tls ?? readLegacyTlsConfig(config?.sessionConfig);
    return JSON.stringify({
        address: resolveConfiguredAddress(config),
        tls: tls
            ? {
                trustStorePath: tls.trustStorePath,
                certChainPath: tls.certChainPath,
                privateKeyPath: tls.privateKeyPath,
                serverNameOverride: tls.serverNameOverride,
            }
            : undefined,
    });
}

function getClientContext(config?: SparkConnectionConfig): ClientContext {
    const cacheKey = getClientCacheKey(config);
    let client = clientCache.get(cacheKey);

    if (!client) {
        client = new proto.spark.connect.SparkConnectService(
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

function extractExplainString(response: AnalyzePlanResponse): string | undefined {
    return response.explain?.explain_string
        ?? response.explain?.explainString
        ?? response.explain_string
        ?? response.explainString;
}

function extractStreamStart(response: ExecutePlanResponse): StreamStartResult | undefined {
    return response.write_stream_operation_start_result ?? response.writeStreamOperationStartResult;
}

function extractStreamingQueryCommandResult(response: ExecutePlanResponse): StreamingQueryCommandResult | undefined {
    return response.streaming_query_command_result ?? response.streamingQueryCommandResult;
}

function normalizeStreamingQueryId(
    queryId?: StreamingQueryInstanceId
): NormalizedStreamingQueryInstanceId | undefined {
    if (!queryId) return undefined;

    const id = typeof queryId.id === "string" && queryId.id.trim()
        ? queryId.id
        : undefined;
    const runId = typeof queryId.run_id === "string" && queryId.run_id.trim()
        ? queryId.run_id
        : (typeof queryId.runId === "string" && queryId.runId.trim()
            ? queryId.runId
            : undefined);

    return id && runId
        ? { id, run_id: runId }
        : undefined;
}

function extractStreamingQueryId(
    carrier?: QueryIdCarrier
): NormalizedStreamingQueryInstanceId | undefined {
    return normalizeStreamingQueryId(carrier?.query_id ?? carrier?.queryId);
}

function extractAwaitTerminationResult(
    result?: StreamingQueryCommandResult
): boolean | undefined {
    const awaitTermination = result?.await_termination ?? result?.awaitTermination;
    return typeof awaitTermination?.terminated === "boolean"
        ? awaitTermination.terminated
        : undefined;
}

function buildStreamingQueryCommandRequest(
    request: RpcMessage,
    queryId: NormalizedStreamingQueryInstanceId,
    command: RpcMessage
): RpcMessage {
    const commandRequest: RpcMessage = {
        session_id: request.session_id,
        user_context: request.user_context,
        plan: {
            command: {
                streaming_query_command: {
                    query_id: queryId,
                    ...command,
                },
            },
        },
    };

    const clientType = request.client_type;
    if (typeof clientType === "string" && clientType) {
        commandRequest.client_type = clientType;
    }

    return commandRequest;
}

function callUnary<TResponse>(
    method: (request: RpcMessage, metadata: grpc.Metadata, callback: UnaryCallback<TResponse>) => void,
    request: RpcMessage,
    metadata: grpc.Metadata
): Promise<TResponse> {
    return new Promise((resolve, reject) => {
        method(request, metadata, (error, response) => {
            if (error) {
                reject(error);
                return;
            }
            resolve(response);
        });
    });
}

export const sparkGrpcClient = {
    async executePlan(request: RpcMessage, config?: SparkConnectionConfig): Promise<ExecutePlanResponse[]> {
        const { client, metadata } = getClientContext(config);
        return new Promise((resolve, reject) => {
            const call = client.executePlan(request, metadata);
            const results: ExecutePlanResponse[] = [];

            call.on("data", response => {
                results.push(response);
            });
            call.on("end", () => resolve(results));
            call.on("error", error => reject(error));
        });
    },

    async explain(request: RpcMessage, config?: SparkConnectionConfig): Promise<string> {
        const { client, metadata } = getClientContext(config);
        const response = await callUnary(client.analyzePlan.bind(client), request, metadata);
        const explainString = extractExplainString(response);
        if (typeof explainString !== "string") {
            throw new Error("Invalid AnalyzePlan response: expected explain string.");
        }
        return explainString;
    },

    async interrupt(request: RpcMessage, config?: SparkConnectionConfig): Promise<RpcMessage> {
        const { client, metadata } = getClientContext(config);
        return callUnary(client.interrupt.bind(client), request, metadata);
    },

    async executePlanStreaming(request: RpcMessage, config?: SparkConnectionConfig): Promise<StreamingQueryHandle> {
        const { client, metadata } = getClientContext(config);

        return new Promise((resolve, reject) => {
            const call = client.executePlan(request, metadata);
            let queryName = "";
            let queryId: NormalizedStreamingQueryInstanceId | undefined;
            let settled = false;
            let started = false;

            const makeHandle = (): StreamingQueryHandle => ({
                get name() {
                    return queryName;
                },
                awaitTermination: async () => {
                    if (!queryId) {
                        throw new Error("Streaming query start result did not include query_id.");
                    }

                    try {
                        const responses = await sparkGrpcClient.executePlan(
                            buildStreamingQueryCommandRequest(request, queryId, {
                                await_termination: {},
                            }),
                            config
                        );
                        const result = responses
                            .map(extractStreamingQueryCommandResult)
                            .find((value): value is StreamingQueryCommandResult => value !== undefined);
                        const terminated = extractAwaitTerminationResult(result);
                        if (terminated === false) {
                            throw new Error("Streaming query awaitTermination returned false.");
                        }
                    } finally {
                        call.cancel();
                    }
                },
                stop: async () => {
                    try {
                        if (queryId) {
                            await sparkGrpcClient.executePlan(
                                buildStreamingQueryCommandRequest(request, queryId, {
                                    stop: true,
                                }),
                                config
                            );
                            return;
                        }

                        const operationId = request.operation_id;
                        if (typeof operationId === "string" && operationId) {
                            await sparkGrpcClient.interrupt({
                                session_id: request.session_id,
                                user_context: request.user_context,
                                interrupt_type: "INTERRUPT_TYPE_OPERATION_ID",
                                operation_id: operationId,
                            }, config);
                            return;
                        }

                        throw new Error("Unable to stop streaming query: missing query_id and operation_id.");
                    } finally {
                        call.cancel();
                    }
                },
            });

            call.on("data", response => {
                const startResult = extractStreamStart(response);
                if (!startResult || settled) {
                    return;
                }

                started = true;
                const nextQueryId = extractStreamingQueryId(startResult);
                if (!nextQueryId) {
                    settled = true;
                    reject(new Error("Invalid WriteStreamOperationStartResult: missing query_id."));
                    call.cancel();
                    return;
                }

                queryId = nextQueryId;
                queryName = startResult.name ?? startResult.query_name ?? startResult.queryName ?? queryName;
                settled = true;
                resolve(makeHandle());
            });

            call.on("error", error => {
                if (!settled) {
                    settled = true;
                    reject(error);
                }
            });

            call.on("end", () => {
                if (!settled) {
                    settled = true;
                    reject(new Error(started
                        ? "Streaming query terminated before a handle could be returned."
                        : "Streaming query ended before reporting a start result."));
                }
            });
        });
    },
};
