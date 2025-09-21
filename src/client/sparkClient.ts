import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "node:path";

export interface StreamingQueryHandle {
    awaitTermination(): Promise<void>;
    stop(): Promise<any>;
    name: string;
}
const packageDefinition = protoLoader.loadSync(
    [path.resolve(__dirname, "../../proto/spark/connect/base.proto")],
    {
        includeDirs: ["proto"],
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    }
);

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

// Extract host:port from SPARK_CONNECT_URL environment variable
function getSparkConnectAddress(): string {
    const url = process.env.SPARK_CONNECT_URL || "sc://localhost:15002";
    // Remove sc:// prefix if present
    const address = url.replace(/^sc:\/\//, "");
    return address;
}

// Lazy client creation - only create when first used
let _client: any = null;
function getClient() {
    if (!_client) {
        _client = new proto.spark.connect.SparkConnectService(
            getSparkConnectAddress(),
            grpc.credentials.createInsecure()
        );
    }
    return _client;
}

export const sparkGrpcClient = {
    async executePlan(request: any): Promise<any[]> {
        return new Promise((resolve, reject) => {
            const call = getClient().executePlan(request);

            const results: any[] = [];

            call.on("data", (response: any) => {
                results.push(response);
            });

            call.on("end", () => resolve(results));
            call.on("error", (err: any) => reject(err));
        });
    },
    explain(request: any): Promise<string> {
        return new Promise((resolve, reject) => {
            getClient().explain(request, (err: any, response: any) => {
                if (err) return reject(err);

                if (typeof response?.explain_string === "string") {
                    resolve(response.explain_string);
                } else {
                    reject(new Error("Invalid response: expected explain_string"));
                }
            });
        });
    },
    async executePlanStreaming(request: any): Promise<StreamingQueryHandle> {
        const call = getClient().executePlan(request);

        let streamStarted = false;
        let queryName = "";
        let queryId = "";

        call.on("data", (response: any) => {
            if (response.writeStreamOperationStartResult) {
                streamStarted = true;
                queryName = response.writeStreamOperationStartResult.queryName;
                queryId = response.writeStreamOperationStartResult.id;
                console.log("Stream started:", response.writeStreamOperationStartResult);
            } else {
                console.warn("Unexpected response:", response);
            }
        });

        call.on("error", (err: any) => {
            console.error("Stream error:", err);
        });

        call.on("end", () => {
            console.log("Stream ended.");
        });

        return {
            awaitTermination: () =>
                new Promise<void>((resolve, reject) => {
                    call.on("end", resolve);
                    call.on("error", reject);
                }),
            stop: () => {
                // Implementación opcional si manejás cancelación vía Session
                return getClient().cancelQuery(queryId);

            },
            name: queryName,
        };
    }
};

