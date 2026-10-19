import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { SparkSession, UnsupportedServerVersionError, arrayType } from "../src";
import { sparkGrpcClient } from "../src/client/sparkClient";
import type { RpcMessage } from "../src/client/reattachableExecution";

let dir: string;
let jar: string;

beforeAll(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "ts-spark-session-artifacts-"));
    jar = path.join(dir, "udfs.jar");
    fs.writeFileSync(jar, Buffer.from("not really a jar"));
});

afterAll(() => {
    fs.rmSync(dir, { recursive: true, force: true });
});

function session(): SparkSession {
    vi.spyOn(sparkGrpcClient, "close").mockImplementation(() => undefined);
    return SparkSession.builder().config("spark.connect.url", "sc://host:15002").getOrCreate();
}

describe("session.addArtifact(s)", () => {
    afterEach(() => vi.restoreAllMocks());

    it("streams the file with the session identity and returns the server-side names", async () => {
        const streams: RpcMessage[][] = [];
        vi.spyOn(sparkGrpcClient, "addArtifacts").mockImplementation(async requests => {
            const sent = [...requests];
            streams.push(sent);
            return {
                session_id: "",
                server_side_session_id: "server-9",
                artifacts: sent.flatMap(request =>
                    ((request.batch as { artifacts: Array<{ name: string }> }).artifacts).map(({ name }) => ({ name, is_crc_successful: true }))),
            };
        });
        const spark = session();
        spark.observeServerSideSessionId({ server_side_session_id: "server-9" });

        expect(await spark.addArtifact(jar)).toBe("jars/udfs.jar");
        const sent = streams[0]!;
        expect(sent).toHaveLength(1);
        expect(sent[0]).toMatchObject({
            session_id: spark.getSessionId(),
            client_type: "ts-spark-connector",
            client_observed_server_side_session_id: "server-9",
            batch: { artifacts: [{ name: "jars/udfs.jar" }] },
        });
        expect(await spark.addArtifacts({ path: jar, kind: "file", name: "copy.jar" }, jar)).toEqual(["files/copy.jar", "jars/udfs.jar"]);
        expect(await spark.addArtifact(jar, "archive")).toBe("archives/udfs.jar");
    });

    it("rejects CRC failures, bad arguments and closed sessions", async () => {
        vi.spyOn(sparkGrpcClient, "addArtifacts").mockResolvedValue({
            artifacts: [{ name: "jars/udfs.jar", is_crc_successful: false }],
        });
        const spark = session();
        await expect(spark.addArtifact(jar)).rejects.toThrow(/CRC mismatch/);
        await expect(spark.addArtifacts()).rejects.toThrow(/at least one file/);
        await expect(spark.addArtifact(path.join(dir, "config.json"))).rejects.toThrow(/not a jar/);
        vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
        await spark.close();
        await expect(spark.addArtifact(jar)).rejects.toThrow(/closed/);
    });

    it("invalidates the session when the upload reports a changed server identity", async () => {
        const changed = Object.assign(new Error("changed"), { errorClass: "INVALID_HANDLE.SESSION_CHANGED" });
        vi.spyOn(sparkGrpcClient, "addArtifacts").mockRejectedValue(changed);
        const spark = session();
        await expect(spark.addArtifact(jar)).rejects.toBe(changed);
        expect(() => spark.read).toThrow(/server-side session changed/);
    });
});

describe("session.cacheArtifact and isArtifactCached", () => {
    afterEach(() => vi.restoreAllMocks());

    it("stores a blob by hash and checks it through ArtifactStatus", async () => {
        const streams: RpcMessage[][] = [];
        vi.spyOn(sparkGrpcClient, "addArtifacts").mockImplementation(async requests => {
            const sent = [...requests];
            streams.push(sent);
            const name = (sent[0]!.batch as { artifacts: Array<{ name: string }> }).artifacts[0]!.name;
            return { artifacts: [{ name, is_crc_successful: true }] };
        });
        const status = vi.spyOn(sparkGrpcClient, "artifactStatus").mockImplementation(async request => ({
            statuses: Object.fromEntries((request.names as string[]).map(name => [name, { exists: name.endsWith("1".repeat(64)) }])),
        }));
        const spark = session();

        const hash = await spark.cacheArtifact("payload");
        expect(hash).toBe("239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5");
        expect(streams[0]![0]).toMatchObject({ session_id: spark.getSessionId(), batch: { artifacts: [{ name: `cache/${hash}` }] } });
        expect(await spark.cacheArtifact(new Uint8Array([1, 2, 3]))).toMatch(/^[0-9a-f]{64}$/);

        expect(await spark.isArtifactCached("1".repeat(64))).toBe(true);
        expect(await spark.isArtifactCached("A".repeat(64))).toBe(false);
        expect(status.mock.calls.map(call => call[0]!.names)).toEqual([[`cache/${"1".repeat(64)}`], [`cache/${"a".repeat(64)}`]]);
    });

    it("validates its input and the server's confirmation", async () => {
        vi.spyOn(sparkGrpcClient, "addArtifacts").mockResolvedValue({ artifacts: [{ name: "cache/other", is_crc_successful: true }] });
        const spark = session();
        await expect(spark.cacheArtifact(7 as never)).rejects.toThrow(/Uint8Array or a string/);
        await expect(spark.cacheArtifact("")).rejects.toThrow(/non-empty blob/);
        await expect(spark.cacheArtifact("x")).rejects.toThrow(/confirmed cache\/other/);
        await expect(spark.isArtifactCached("not-a-hash")).rejects.toThrow(/SHA-256 hex hash/);
    });
});

describe("session.artifactStatus", () => {
    afterEach(() => vi.restoreAllMocks());

    it("maps every requested name to whether it exists", async () => {
        const status = vi.spyOn(sparkGrpcClient, "artifactStatus").mockResolvedValue({
            statuses: { "jars/udfs.jar": { exists: true }, "files/missing": { exists: false } },
        });
        const spark = session();
        expect(await spark.artifactStatus("jars/udfs.jar", "files/missing", "files/unknown")).toEqual({
            "jars/udfs.jar": true,
            "files/missing": false,
            "files/unknown": false,
        });
        expect(status.mock.calls[0]![0]).toMatchObject({ session_id: spark.getSessionId(), names: ["jars/udfs.jar", "files/missing", "files/unknown"] });
        status.mockResolvedValueOnce({});
        expect(await spark.artifactStatus("jars/none.jar")).toEqual({ "jars/none.jar": false });
        await expect(spark.artifactStatus()).rejects.toThrow(/at least one artifact name/);
        await expect(spark.artifactStatus(" ")).rejects.toThrow(/non-empty strings/);
    });

    it("reports RPC failures through the session", async () => {
        const changed = Object.assign(new Error("changed"), { errorClass: "INVALID_HANDLE.SESSION_CHANGED" });
        vi.spyOn(sparkGrpcClient, "artifactStatus").mockRejectedValue(changed);
        const spark = session();
        await expect(spark.artifactStatus("jars/x.jar")).rejects.toBe(changed);
        expect(() => spark.read).toThrow(/server-side session changed/);
    });
});

describe("session.udf", () => {
    afterEach(() => vi.restoreAllMocks());

    it("registers Java UDFs and UDAFs through a register_function command", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const spark = session();

        await spark.udf.registerJavaFunction("shout", "io.example.Shout", "string");
        await spark.udf.registerJavaFunction("tags", "io.example.Tags", arrayType("string"));
        await spark.udf.registerJavaFunction("untyped", "io.example.Untyped");
        await spark.udf.registerJavaUDAF("summed", "io.example.Summed");

        const commands = execute.mock.calls.map(([request]) => (request.plan as { command: Record<string, unknown> }).command);
        expect(commands[0]).toEqual({
            register_function: {
                function_name: "shout",
                deterministic: true,
                arguments: [],
                java_udf: { class_name: "io.example.Shout", output_type: { unparsed: { data_type_string: "string" } }, aggregate: false },
            },
        });
        expect(commands[1]).toMatchObject({
            register_function: { java_udf: { output_type: { unparsed: { data_type_string: "ARRAY<STRING>" } } } },
        });
        expect(commands[2]).toMatchObject({ register_function: { java_udf: { class_name: "io.example.Untyped", aggregate: false } } });
        expect((commands[2] as { register_function: { java_udf: object } }).register_function.java_udf).not.toHaveProperty("output_type");
        expect(commands[3]).toMatchObject({
            register_function: { function_name: "summed", java_udf: { class_name: "io.example.Summed", aggregate: true } },
        });
        expect((commands[3] as { register_function: { java_udf: object } }).register_function.java_udf).not.toHaveProperty("output_type");
    });

    it("validates names, classes and return types before sending", async () => {
        const execute = vi.spyOn(sparkGrpcClient, "executePlan").mockResolvedValue([]);
        const spark = session();
        await expect(spark.udf.registerJavaFunction("", "io.example.Shout")).rejects.toThrow(/UDF name/);
        await expect(spark.udf.registerJavaFunction("f", " ")).rejects.toThrow(/class name/);
        await expect(spark.udf.registerJavaFunction("f", "io.example.F", "")).rejects.toThrow(/return type/);
        await expect(spark.udf.registerJavaUDAF("f", "")).rejects.toThrow(/class name/);
        expect(execute).not.toHaveBeenCalled();
    });
});

describe("session.getStatus", () => {
    afterEach(() => vi.restoreAllMocks());

    it("refuses servers older than 4.2 before any GetStatus RPC", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.1.3" } });
        const status = vi.spyOn(sparkGrpcClient, "getStatus");
        const spark = session();
        const error = await spark.getStatus().catch(e => e);
        expect(error).toBeInstanceOf(UnsupportedServerVersionError);
        expect(error).toMatchObject({ requiredVersion: "4.2.0", serverVersion: "4.1.3" });
        expect(status).not.toHaveBeenCalled();
    });

    it("maps operation states and validates operation ids", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.2.0" } });
        const status = vi.spyOn(sparkGrpcClient, "getStatus").mockResolvedValue({
            session_id: "",
            operation_statuses: [
                { operation_id: "00112233-4455-4677-8899-aabbccddeeff", state: "OPERATION_STATE_RUNNING" },
                { operation_id: "11112233-4455-4677-8899-aabbccddeeff", state: 4 },
                { operationId: "22112233-4455-4677-8899-aabbccddeeff", state: "OPERATION_STATE_FROZEN" },
                { state: 42 },
                {},
            ],
        });
        const spark = session();
        expect(await spark.getStatus(["00112233-4455-4677-8899-aabbccddeeff"])).toEqual([
            { operationId: "00112233-4455-4677-8899-aabbccddeeff", state: "RUNNING" },
            { operationId: "11112233-4455-4677-8899-aabbccddeeff", state: "SUCCEEDED" },
            { operationId: "22112233-4455-4677-8899-aabbccddeeff", state: "UNKNOWN" },
            { operationId: "", state: "UNKNOWN" },
            { operationId: "", state: "UNKNOWN" },
        ]);
        expect(status.mock.calls[0]![0]).toMatchObject({
            session_id: spark.getSessionId(),
            operation_status: { operation_ids: ["00112233-4455-4677-8899-aabbccddeeff"] },
        });
        status.mockResolvedValueOnce({});
        expect(await spark.getStatus()).toEqual([]);
        await expect(spark.getStatus(["op-1"])).rejects.toThrow(/canonical UUID/);
    });

    it("reports GetStatus failures through the session", async () => {
        vi.spyOn(sparkGrpcClient, "analyze").mockResolvedValue({ spark_version: { version: "4.2.0" } });
        const changed = Object.assign(new Error("changed"), { errorClass: "INVALID_HANDLE.SESSION_CHANGED" });
        vi.spyOn(sparkGrpcClient, "getStatus").mockRejectedValue(changed);
        const spark = session();
        await expect(spark.getStatus()).rejects.toBe(changed);
        expect(() => spark.read).toThrow(/server-side session changed/);
    });
});
