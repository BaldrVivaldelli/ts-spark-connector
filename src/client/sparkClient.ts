import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "node:path";

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

const client = new proto.spark.connect.SparkConnectService(
    getSparkConnectAddress(),
    grpc.credentials.createInsecure()
);

export const sparkGrpcClient = {
    async executePlan(request: any): Promise<any[]> {
        console.log("[DEBUG] Request gRPC:", JSON.stringify(request, null, 2));
        return new Promise((resolve, reject) => {
            const call = client.executePlan(request);

            const results: any[] = [];

            call.on("data", (response: any) => {
                results.push(response);
            });

            call.on("end", () => resolve(results));
            call.on("error", (err: any) => reject(err));
        });
    },
    explain(request: any): Promise<string> {
        console.log("[DEBUG] Request gRPC:", JSON.stringify(request, null, 2));
        return new Promise((resolve, reject) => {
            client.explain(request, (err: any, response: any) => {
                if (err) return reject(err);

                if (typeof response?.explain_string === "string") {
                    resolve(response.explain_string);
                } else {
                    reject(new Error("Invalid response: expected explain_string"));
                }
            });
        });
    }
    ,
};
