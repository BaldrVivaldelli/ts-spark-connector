import type * as grpc from "@grpc/grpc-js";

export type SparkConnectErrorOptions = {
    operation: string;
    code?: number;
    details?: string;
    metadata?: grpc.Metadata;
    errorId?: string;
    errorClass?: string;
    errorInfo?: SparkGrpcErrorInfo;
    remoteDetails?: SparkErrorDetails;
    cause?: unknown;
};

/** Structured payload returned by Spark Connect's FetchErrorDetails RPC. */
export type SparkErrorDetails = Record<string, unknown>;

/** google.rpc.ErrorInfo carried inside grpc-status-details-bin. */
export type SparkGrpcErrorInfo = {
    reason?: string;
    domain?: string;
    metadata: Record<string, string>;
};

/** Error raised by a Spark Connect RPC with stable transport context attached. */
export class SparkConnectError extends Error {
    readonly operation: string;
    readonly code?: number;
    readonly details?: string;
    readonly metadata?: grpc.Metadata;
    readonly errorId?: string;
    readonly errorClass?: string;
    readonly errorInfo?: SparkGrpcErrorInfo;
    readonly remoteDetails?: SparkErrorDetails;
    readonly cause?: unknown;

    constructor(message: string, options: SparkConnectErrorOptions) {
        super(message);
        this.name = "SparkConnectError";
        this.operation = options.operation;
        this.code = options.code;
        this.details = options.details;
        this.metadata = options.metadata;
        this.errorId = options.errorId;
        this.errorClass = options.errorClass;
        this.errorInfo = options.errorInfo;
        this.remoteDetails = options.remoteDetails;
        this.cause = options.cause;
    }
}

type GrpcLikeError = Error & {
    code?: number;
    details?: string;
    metadata?: grpc.Metadata;
    errorId?: string;
    error_id?: string;
    errorClass?: string;
    errorInfo?: SparkGrpcErrorInfo;
    __noRetry?: boolean;
};

function metadataErrorId(metadata?: grpc.Metadata): string | undefined {
    if (!metadata) return undefined;
    for (const key of ["error-id", "error_id", "errorid", "spark-connect-error-id"]) {
        const value = metadata.get(key)[0];
        if (typeof value === "string" && value) return value;
        if (Buffer.isBuffer(value) && value.length > 0) return value.toString("utf8");
    }
    return metadataErrorInfo(metadata)?.metadata.errorId;
}

type WireField = { number: number; wireType: number; value: bigint | Buffer };

function readVarint(buffer: Buffer, start: number): [bigint, number] {
    let value = 0n;
    let shift = 0n;
    let offset = start;
    while (offset < buffer.length && shift <= 63n) {
        const byte = buffer[offset++];
        value |= BigInt(byte & 0x7f) << shift;
        if ((byte & 0x80) === 0) return [value, offset];
        shift += 7n;
    }
    throw new Error("Invalid protobuf varint in grpc-status-details-bin.");
}

function protobufFields(buffer: Buffer): WireField[] {
    const fields: WireField[] = [];
    let offset = 0;
    while (offset < buffer.length) {
        const [tag, afterTag] = readVarint(buffer, offset);
        offset = afterTag;
        const fieldNumber = Number(tag >> 3n);
        const wireType = Number(tag & 7n);
        if (fieldNumber < 1) throw new Error("Invalid protobuf field number.");
        if (wireType === 0) {
            const [value, next] = readVarint(buffer, offset);
            fields.push({ number: fieldNumber, wireType, value });
            offset = next;
        } else if (wireType === 2) {
            const [lengthValue, afterLength] = readVarint(buffer, offset);
            const length = Number(lengthValue);
            const end = afterLength + length;
            if (!Number.isSafeInteger(length) || length < 0 || end > buffer.length) {
                throw new Error("Invalid protobuf length in grpc-status-details-bin.");
            }
            fields.push({ number: fieldNumber, wireType, value: buffer.subarray(afterLength, end) });
            offset = end;
        } else if (wireType === 1) {
            offset += 8;
        } else if (wireType === 5) {
            offset += 4;
        } else {
            throw new Error(`Unsupported protobuf wire type ${wireType}.`);
        }
        if (offset > buffer.length) throw new Error("Truncated grpc-status-details-bin.");
    }
    return fields;
}

function stringField(fields: WireField[], fieldNumber: number): string | undefined {
    const field = fields.find(item => item.number === fieldNumber && Buffer.isBuffer(item.value));
    return Buffer.isBuffer(field?.value) ? field.value.toString("utf8") : undefined;
}

function decodeErrorInfo(buffer: Buffer): SparkGrpcErrorInfo {
    const fields = protobufFields(buffer);
    const metadata: Record<string, string> = {};
    for (const entryField of fields.filter(field => field.number === 3 && Buffer.isBuffer(field.value))) {
        const entry = protobufFields(entryField.value as Buffer);
        const key = stringField(entry, 1);
        const value = stringField(entry, 2);
        if (key !== undefined && value !== undefined) metadata[key] = value;
    }
    return {
        reason: stringField(fields, 1),
        domain: stringField(fields, 2),
        metadata,
    };
}

/** Decodes google.rpc.Status -> Any -> google.rpc.ErrorInfo without a runtime protobuf dependency. */
export function decodeGrpcStatusErrorInfo(statusBytes: Buffer): SparkGrpcErrorInfo | undefined {
    for (const detail of protobufFields(statusBytes)
        .filter(field => field.number === 3 && Buffer.isBuffer(field.value))) {
        const anyFields = protobufFields(detail.value as Buffer);
        const typeUrl = stringField(anyFields, 1);
        const value = anyFields.find(field => field.number === 2 && Buffer.isBuffer(field.value))?.value;
        if (typeUrl?.endsWith("/google.rpc.ErrorInfo") && Buffer.isBuffer(value)) {
            return decodeErrorInfo(value);
        }
    }
    return undefined;
}

function metadataErrorInfo(metadata?: grpc.Metadata): SparkGrpcErrorInfo | undefined {
    if (!metadata) return undefined;
    for (const value of metadata.get("grpc-status-details-bin")) {
        try {
            const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), "base64");
            const info = decodeGrpcStatusErrorInfo(bytes);
            if (info) return info;
        } catch {
            // A malformed optional trailer must not hide the original RPC error.
        }
    }
    return undefined;
}

/** @internal Converts raw grpc-js failures without hiding aborts or existing wrappers. */
export function asSparkConnectError(error: unknown, operation: string): Error {
    if (error instanceof SparkConnectError) return error;
    if (error instanceof Error && error.name === "AbortError") return error;

    const source = error instanceof Error ? error as GrpcLikeError : undefined;
    const errorInfo = source?.errorInfo ?? metadataErrorInfo(source?.metadata);
    const details = source?.details;
    const message = details
        ? `${operation} failed: ${details}`
        : `${operation} failed: ${source?.message ?? String(error)}`;
    const wrapped = new SparkConnectError(message, {
        operation,
        code: source?.code,
        details,
        metadata: source?.metadata,
        errorId: source?.errorId ?? source?.error_id
            ?? errorInfo?.metadata.errorId
            ?? metadataErrorId(source?.metadata),
        errorClass: source?.errorClass ?? errorInfo?.metadata.errorClass ?? errorInfo?.reason,
        errorInfo,
        cause: error,
    });
    if (source?.__noRetry) {
        (wrapped as SparkConnectError & { __noRetry?: boolean }).__noRetry = true;
    }
    return wrapped;
}

/** @internal Returns a copy enriched with FetchErrorDetails without losing retry metadata. */
export function attachSparkErrorDetails(error: SparkConnectError, details: SparkErrorDetails): SparkConnectError {
    const enriched = new SparkConnectError(error.message, {
        operation: error.operation,
        code: error.code,
        details: error.details,
        metadata: error.metadata,
        errorId: error.errorId,
        errorClass: error.errorClass,
        errorInfo: error.errorInfo,
        remoteDetails: details,
        cause: error.cause,
    });
    if ((error as SparkConnectError & { __noRetry?: boolean }).__noRetry) {
        (enriched as SparkConnectError & { __noRetry?: boolean }).__noRetry = true;
    }
    return enriched;
}
