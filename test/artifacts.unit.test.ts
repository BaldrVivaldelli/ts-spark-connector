import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
    ARTIFACT_CHUNK_SIZE,
    ArtifactUploadError,
    artifactName,
    artifactRequests,
    cachedArtifactHash,
    cachedArtifactRequests,
    crc32,
    uploadedArtifactNames,
} from "../src/client/artifacts";

const base = { session_id: "session-1", user_context: { user_id: "u" }, client_type: "test" };
let dir: string;

function write(name: string, bytes: number, fill = 7): string {
    const file = path.join(dir, name);
    fs.writeFileSync(file, Buffer.alloc(bytes, fill));
    return file;
}

beforeAll(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "ts-spark-artifacts-"));
});

afterAll(() => {
    fs.rmSync(dir, { recursive: true, force: true });
});

describe("artifact naming", () => {
    it("derives the server-side name from the kind and the file name", () => {
        expect(artifactName({ path: "/tmp/libs/udfs.JAR" })).toBe("jars/udfs.JAR");
        expect(artifactName({ path: "/tmp/config.json", kind: "file" })).toBe("files/config.json");
        expect(artifactName({ path: "/tmp/model.zip", kind: "archive" })).toBe("archives/model.zip");
        expect(artifactName({ path: "/tmp/a.jar", kind: "file", name: "renamed.jar" })).toBe("files/renamed.jar");
    });

    it("rejects non-jars without a kind, bad names and unknown kinds", () => {
        expect(() => artifactName({ path: "/tmp/config.json" })).toThrow(/not a jar; pass kind "file" or "archive"/);
        expect(() => artifactName({ path: " " })).toThrow(/non-empty string/);
        expect(() => artifactName({ path: "/tmp/a.jar", name: "dir/a.jar" })).toThrow(/bare file name/);
        expect(() => artifactName({ path: "/tmp/a.jar", name: " " })).toThrow(/bare file name/);
        expect(() => artifactName({ path: "/tmp/a.jar", kind: "pyfile" as never })).toThrow(/Unsupported artifact kind/);
    });
});

describe("crc32", () => {
    it("matches the IEEE check value and zlib", async () => {
        expect(crc32(Buffer.from("123456789"))).toBe(0xcbf43926);
        expect(crc32(new Uint8Array())).toBe(0);
        const zlib = await import("node:zlib");
        const sample = Buffer.alloc(70_000).map((_, index) => (index * 31) & 0xff);
        expect(crc32(sample)).toBe(zlib.crc32(sample));
    });
});

describe("artifact requests", () => {
    it("batches small files together and flushes before a batch exceeds the chunk size", () => {
        const a = write("a.jar", 10);
        const b = write("b.txt", 20, 1);
        const c = write("c.jar", ARTIFACT_CHUNK_SIZE - 25, 2);
        const requests = [...artifactRequests(base, [{ path: a }, { path: b, kind: "file" }, { path: c }])];

        expect(requests).toHaveLength(2);
        expect(requests[0]).toMatchObject({ ...base, batch: { artifacts: [{ name: "jars/a.jar" }, { name: "files/b.txt" }] } });
        const batch = (requests[0] as { batch: { artifacts: Array<{ data: { data: Uint8Array; crc: number } }> } }).batch;
        expect(batch.artifacts[0]!.data.data).toEqual(Buffer.alloc(10, 7));
        expect(batch.artifacts[0]!.data.crc).toBe(crc32(Buffer.alloc(10, 7)));
        expect(requests[1]).toMatchObject({ batch: { artifacts: [{ name: "jars/c.jar" }] } });
    });

    it("sends a large file as begin_chunk followed by CRC-checked chunks", () => {
        const size = ARTIFACT_CHUNK_SIZE * 2 + 100;
        const big = write("big.jar", size, 3);
        const small = write("small.jar", 5);
        const requests = [...artifactRequests(base, [{ path: small }, { path: big }, { path: small, name: "again.jar" }])];

        expect(requests.map(request => Object.keys(request).find(key => ["batch", "begin_chunk", "chunk"].includes(key))))
            .toEqual(["batch", "begin_chunk", "chunk", "chunk", "batch"]);
        const begin = (requests[1] as { begin_chunk: Record<string, unknown> }).begin_chunk;
        expect(begin).toMatchObject({ name: "jars/big.jar", total_bytes: size, num_chunks: 3 });
        const initial = begin.initial_chunk as { data: Uint8Array; crc: number };
        expect(initial.data.byteLength).toBe(ARTIFACT_CHUNK_SIZE);
        expect(initial.crc).toBe(crc32(initial.data));
        const last = (requests[3] as { chunk: { data: Uint8Array; crc: number } }).chunk;
        expect(last.data.byteLength).toBe(100);
        expect(last.crc).toBe(crc32(Buffer.alloc(100, 3)));
        expect(requests[4]).toMatchObject({ batch: { artifacts: [{ name: "jars/again.jar" }] } });
    });

    it("sends an exact multiple of the chunk size without an empty trailing chunk", () => {
        const exact = write("exact.jar", ARTIFACT_CHUNK_SIZE * 2, 4);
        const requests = [...artifactRequests(base, [{ path: exact }])];
        expect(requests).toHaveLength(2);
        expect((requests[0] as { begin_chunk: { num_chunks: number } }).begin_chunk.num_chunks).toBe(2);
    });

    it("fails before sending anything when a file is missing or is a directory", () => {
        expect(() => [...artifactRequests(base, [{ path: path.join(dir, "missing.jar") }])]).toThrow(/not found/);
        expect(() => [...artifactRequests(base, [{ path: dir, kind: "file" }])]).toThrow(/not found/);
    });
});

describe("cached artifact requests", () => {
    it("names the blob by its SHA-256 and batches or chunks it like a file", () => {
        const small = Buffer.from("payload");
        const cached = cachedArtifactRequests(base, small);
        expect(cached.hash).toBe(cachedArtifactHash(small));
        expect(cached.hash).toBe("239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5");
        expect([...cached.requests]).toEqual([
            { ...base, batch: { artifacts: [{ name: `cache/${cached.hash}`, data: { data: small, crc: crc32(small) } }] } },
        ]);

        const big = Buffer.alloc(ARTIFACT_CHUNK_SIZE + 1, 2);
        const chunked = [...cachedArtifactRequests(base, big).requests];
        expect(chunked).toHaveLength(2);
        expect((chunked[0] as { begin_chunk: { name: string; num_chunks: number } }).begin_chunk)
            .toMatchObject({ name: `cache/${cachedArtifactHash(big)}`, num_chunks: 2 });
        expect((chunked[1] as { chunk: { data: Uint8Array } }).chunk.data.byteLength).toBe(1);
    });
});

describe("upload summaries", () => {
    it("returns the confirmed names and rejects CRC failures with both lists", () => {
        expect(uploadedArtifactNames({ artifacts: [{ name: "jars/a.jar", is_crc_successful: true }] })).toEqual(["jars/a.jar"]);
        expect(uploadedArtifactNames({})).toEqual([]);
        const error = (() => {
            try {
                uploadedArtifactNames({
                    artifacts: [{ name: "jars/a.jar", isCrcSuccessful: false }, { name: "files/b", is_crc_successful: true }],
                });
            } catch (caught) {
                return caught;
            }
            throw new Error("expected a rejection");
        })();
        expect(error).toBeInstanceOf(ArtifactUploadError);
        expect(error).toMatchObject({
            name: "ArtifactUploadError",
            message: "Spark rejected the upload of jars/a.jar: CRC mismatch.",
            uploaded: ["files/b"],
            failed: ["jars/a.jar"],
        });
    });
});
