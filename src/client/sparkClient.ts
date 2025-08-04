import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

const packageDefinition = protoLoader.loadSync(
    ["C:\\Users\\aatv1\\projects\\ts-spark-connector\\proto\\spark\\connect\\base.proto"],
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

const client = new proto.spark.connect.SparkConnectService(
    "localhost:15002",
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
    }
};
