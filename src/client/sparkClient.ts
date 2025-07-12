import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

// 1. Cargamos solo el proto que define el servicio (y que importa al resto)
const packageDefinition = protoLoader.loadSync(
    ["C:\\Users\\aatv1\\projects\\ts-spark-connector\\proto\\spark\\connect\\base.proto"], // 👈 este es el archivo clave
    {
        includeDirs: ["proto"], // 👈 muy importante para que funcione `import "spark/connect/..."`
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    }
);

// 2. Cargamos el paquete
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

// 3. Creamos cliente gRPC
const client = new proto.spark.connect.SparkConnectService(
    "localhost:15002", // o IP si está en contenedor
    grpc.credentials.createInsecure()
);

// 4. Wrapper con lógica para ejecutar plan y capturar resultados
export const sparkGrpcClient = {
    async executePlan(request: any): Promise<any[]> {
        console.log("[DEBUG] Request gRPC:", JSON.stringify(request, null, 2));
        return new Promise((resolve, reject) => {
            const call = client.executePlan(request);

            const results: any[] = [];

            call.on("data", (response: any) => {
                // Aquí deberías decodificar response.arrowBatch
                results.push(response);
            });

            call.on("end", () => resolve(results));
            call.on("error", (err: any) => reject(err));
        });
    }
};
