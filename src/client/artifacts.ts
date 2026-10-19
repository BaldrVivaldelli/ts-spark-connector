import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { RpcMessage } from "./reattachableExecution";

/**
 * Where Spark places an uploaded artifact: `jar` files join the session's
 * classpath, `file` and `archive` entries are distributed to executors like
 * `spark.files` / `spark.archives`.
 * @public
 */
export type ArtifactKind = "jar" | "file" | "archive";

/**
 * One local file to upload with `session.addArtifacts()`. The kind defaults
 * to `jar` for `.jar` files and must be given otherwise; `name` overrides the
 * file name used on the server (the kind's prefix is still added).
 * @public
 */
export type ArtifactUpload = {
    path: string;
    kind?: ArtifactKind;
    name?: string;
};

/**
 * Artifacts larger than this are sent as a `begin_chunk` followed by
 * `chunk` messages of this size; smaller ones are batched together while
 * the batch stays under it. Same value as the Scala and Python clients.
 */
export const ARTIFACT_CHUNK_SIZE = 32 * 1024;

/** Prefix of blobs stored by content hash (`cacheArtifact`), the only kind `ArtifactStatus` reports on. */
export const CACHE_ARTIFACT_PREFIX = "cache/";

const PREFIXES: Record<ArtifactKind, string> = { jar: "jars", file: "files", archive: "archives" };

const CRC_TABLE = new Uint32Array(256).map((_, n) => {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    return c >>> 0;
});

/** IEEE CRC-32, as `zlib.crc32`; the server verifies every chunk with it. */
export function crc32(bytes: Uint8Array): number {
    let crc = 0xffffffff;
    for (const byte of bytes) crc = CRC_TABLE[(crc ^ byte) & 0xff]! ^ (crc >>> 8);
    return (crc ^ 0xffffffff) >>> 0;
}

/** Server-side name of an artifact, e.g. `jars/udfs.jar` or `files/config.json`. */
export function artifactName(upload: ArtifactUpload): string {
    if (typeof upload.path !== "string" || !upload.path.trim()) {
        throw new TypeError("Artifact path must be a non-empty string.");
    }
    const base = upload.name ?? path.basename(upload.path);
    if (!base.trim() || base.includes("/") || base.includes("\\")) {
        throw new TypeError(`Artifact name ${JSON.stringify(base)} must be a bare file name.`);
    }
    const kind = upload.kind ?? (base.toLowerCase().endsWith(".jar") ? "jar" : undefined);
    if (kind === undefined) {
        throw new TypeError(
            `Artifact ${JSON.stringify(upload.path)} is not a jar; pass kind "file" or "archive" to upload it.`,
        );
    }
    if (!(kind in PREFIXES)) {
        throw new TypeError(`Unsupported artifact kind ${JSON.stringify(kind)}; use "jar", "file" or "archive".`);
    }
    return `${PREFIXES[kind]}/${base}`;
}

/** Content hash that names a cached blob: `cache/<sha256 hex>`, like PySpark's `cache_artifact`. */
export function cachedArtifactHash(data: Uint8Array): string {
    return crypto.createHash("sha256").update(data).digest("hex");
}

type Chunk = { data: Uint8Array; crc: number };

function chunk(data: Uint8Array): Chunk {
    return { data, crc: crc32(data) };
}

/** Bytes to upload under one server-side name, read lazily while the stream is written. */
interface ArtifactSource {
    readonly name: string;
    readonly size: number;
    /** The whole content; only used for artifacts that fit one chunk. */
    readonly bytes: () => Uint8Array;
    /** `ARTIFACT_CHUNK_SIZE` pieces, in order. */
    readonly chunks: () => Iterable<Uint8Array>;
}

/** Reads a file in `ARTIFACT_CHUNK_SIZE` pieces without holding it all in memory. */
function* fileChunks(localPath: string): Generator<Uint8Array> {
    const fd = fs.openSync(localPath, "r");
    try {
        for (;;) {
            const buffer = Buffer.allocUnsafe(ARTIFACT_CHUNK_SIZE);
            const read = fs.readSync(fd, buffer, 0, ARTIFACT_CHUNK_SIZE, null);
            if (read === 0) return;
            yield read === ARTIFACT_CHUNK_SIZE ? buffer : buffer.subarray(0, read);
        }
    } finally {
        fs.closeSync(fd);
    }
}

function fileSource(upload: ArtifactUpload): ArtifactSource {
    const name = artifactName(upload);
    const stats = fs.statSync(upload.path, { throwIfNoEntry: false });
    if (!stats?.isFile()) throw new Error(`Artifact file not found: ${upload.path}`);
    return {
        name,
        size: stats.size,
        bytes: () => fs.readFileSync(upload.path),
        chunks: () => fileChunks(upload.path),
    };
}

function blobSource(name: string, data: Uint8Array): ArtifactSource {
    return {
        name,
        size: data.byteLength,
        bytes: () => data,
        chunks: function* () {
            for (let offset = 0; offset < data.byteLength; offset += ARTIFACT_CHUNK_SIZE) {
                yield data.subarray(offset, offset + ARTIFACT_CHUNK_SIZE);
            }
        },
    };
}

/**
 * Builds the `AddArtifactsRequest` sequence for `uploads`, in order: small
 * artifacts share `batch` messages (flushed before a batch would exceed the
 * chunk size), large ones become `begin_chunk` + `chunk` messages. `base`
 * carries the session identity fields repeated on every message. Names and
 * files are validated here, before any RPC; the bytes are read lazily while
 * the stream is written.
 */
export function artifactRequests(base: RpcMessage, uploads: readonly ArtifactUpload[]): Iterable<RpcMessage> {
    const sources = uploads.map(fileSource);
    return { [Symbol.iterator]: () => generateRequests(base, sources) };
}

/**
 * Requests that store `data` by content hash, as PySpark's `cache_artifact`
 * does; the returned hash identifies the blob in `ArtifactStatus` checks.
 */
export function cachedArtifactRequests(base: RpcMessage, data: Uint8Array): { hash: string; requests: Iterable<RpcMessage> } {
    const hash = cachedArtifactHash(data);
    const source = blobSource(`${CACHE_ARTIFACT_PREFIX}${hash}`, data);
    return { hash, requests: { [Symbol.iterator]: () => generateRequests(base, [source]) } };
}

function* generateRequests(base: RpcMessage, sources: readonly ArtifactSource[]): Generator<RpcMessage> {
    let batch: Array<{ name: string; data: Chunk }> = [];
    let batchBytes = 0;
    const flush = function* (): Generator<RpcMessage> {
        if (batch.length === 0) return;
        yield { ...base, batch: { artifacts: batch } };
        batch = [];
        batchBytes = 0;
    };

    for (const source of sources) {
        if (source.size > ARTIFACT_CHUNK_SIZE) {
            yield* flush();
            const chunks = source.chunks()[Symbol.iterator]();
            const first = chunks.next();
            yield {
                ...base,
                begin_chunk: {
                    name: source.name,
                    total_bytes: source.size,
                    num_chunks: Math.ceil(source.size / ARTIFACT_CHUNK_SIZE),
                    initial_chunk: chunk(first.value as Uint8Array),
                },
            };
            for (let next = chunks.next(); !next.done; next = chunks.next()) {
                yield { ...base, chunk: chunk(next.value) };
            }
            continue;
        }
        if (batchBytes + source.size > ARTIFACT_CHUNK_SIZE) yield* flush();
        batch.push({ name: source.name, data: chunk(source.bytes()) });
        batchBytes += source.size;
    }
    yield* flush();
}

type ArtifactSummary = { name?: string; is_crc_successful?: boolean; isCrcSuccessful?: boolean };

/** Names confirmed by an `AddArtifactsResponse`; rejects when a CRC check failed. */
export function uploadedArtifactNames(response: RpcMessage): string[] {
    const summaries = Array.isArray(response.artifacts) ? response.artifacts as ArtifactSummary[] : [];
    const failed = summaries.filter(summary => (summary.is_crc_successful ?? summary.isCrcSuccessful) === false);
    if (failed.length > 0) {
        throw new Error(
            `Spark rejected the upload of ${failed.map(summary => String(summary.name)).join(", ")}: CRC mismatch.`,
        );
    }
    return summaries.map(summary => String(summary.name));
}
