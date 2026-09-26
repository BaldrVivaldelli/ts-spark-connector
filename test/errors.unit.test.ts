import { describe, expect, it } from "vitest";
import * as grpc from "@grpc/grpc-js";
import {
    asSparkConnectError,
    attachSparkErrorDetails,
    decodeGrpcStatusErrorInfo,
    SparkConnectError,
} from "../src/client/errors";

function varint(value: number): Buffer {
    const bytes: number[] = [];
    let remaining = value;
    do {
        let byte = remaining & 0x7f;
        remaining = Math.floor(remaining / 128);
        if (remaining > 0) byte |= 0x80;
        bytes.push(byte);
    } while (remaining > 0);
    return Buffer.from(bytes);
}

const field = (number: number, value: Buffer) => Buffer.concat([
    varint((number << 3) | 2),
    varint(value.length),
    value,
]);
const text = (number: number, value: string) => field(number, Buffer.from(value, "utf8"));

describe("SparkConnectError", () => {
    it("preserves gRPC status and operation context", () => {
        const source = Object.assign(new Error("transport failed"), {
            code: 14,
            details: "server unavailable",
            error_id: "error-123",
        });
        const error = asSparkConnectError(source, "ExecutePlan") as SparkConnectError;

        expect(error).toBeInstanceOf(SparkConnectError);
        expect(error).toMatchObject({
            name: "SparkConnectError",
            operation: "ExecutePlan",
            code: 14,
            details: "server unavailable",
            errorId: "error-123",
            cause: source,
        });
    });

    it("does not wrap AbortError or an existing SparkConnectError twice", () => {
        const aborted = Object.assign(new Error("cancelled"), { name: "AbortError" });
        expect(asSparkConnectError(aborted, "Config")).toBe(aborted);

        const existing = new SparkConnectError("failed", { operation: "Config" });
        expect(asSparkConnectError(existing, "Config")).toBe(existing);
    });

    it("extracts a trailer error id and preserves fetched structured details", () => {
        const metadata = new grpc.Metadata();
        metadata.set("error-id", "remote-error-1");
        const wrapped = asSparkConnectError(
            Object.assign(new Error("analysis failed"), { metadata }),
            "AnalyzePlan"
        ) as SparkConnectError;
        const enriched = attachSparkErrorDetails(wrapped, {
            root_error_idx: 0,
            errors: [{ message: "bad column" }],
        });

        expect(enriched.errorId).toBe("remote-error-1");
        expect(enriched.remoteDetails).toMatchObject({
            root_error_idx: 0,
            errors: [{ message: "bad column" }],
        });
        expect(enriched.cause).toBe(wrapped.cause);
    });

    it("decodes Spark 4 google.rpc.ErrorInfo from grpc-status-details-bin", () => {
        const errorIdEntry = Buffer.concat([text(1, "errorId"), text(2, "remote-error-bin")]);
        const errorClassEntry = Buffer.concat([
            text(1, "errorClass"),
            text(2, "INVALID_HANDLE.SESSION_CHANGED"),
        ]);
        const errorInfo = Buffer.concat([
            text(1, "SPARK_CONNECT_ERROR"),
            text(2, "org.apache.spark"),
            field(3, errorIdEntry),
            field(3, errorClassEntry),
        ]);
        const any = Buffer.concat([
            text(1, "type.googleapis.com/google.rpc.ErrorInfo"),
            field(2, errorInfo),
        ]);
        const status = field(3, any);
        const metadata = new grpc.Metadata();
        metadata.set("grpc-status-details-bin", status);

        expect(decodeGrpcStatusErrorInfo(status)).toEqual({
            reason: "SPARK_CONNECT_ERROR",
            domain: "org.apache.spark",
            metadata: {
                errorId: "remote-error-bin",
                errorClass: "INVALID_HANDLE.SESSION_CHANGED",
            },
        });
        const wrapped = asSparkConnectError(
            Object.assign(new Error("session changed"), { metadata }),
            "ExecutePlan",
        ) as SparkConnectError;
        expect(wrapped).toMatchObject({
            errorId: "remote-error-bin",
            errorClass: "INVALID_HANDLE.SESSION_CHANGED",
        });
    });

    it("keeps the original RPC failure when an optional binary trailer is malformed", () => {
        const metadata = new grpc.Metadata();
        metadata.set("grpc-status-details-bin", Buffer.from([0xff]));

        const wrapped = asSparkConnectError(
            Object.assign(new Error("original transport failure"), {
                code: 13,
                details: "original details",
                metadata,
            }),
            "ExecutePlan",
        ) as SparkConnectError;

        expect(wrapped).toMatchObject({
            operation: "ExecutePlan",
            code: 13,
            details: "original details",
            errorInfo: undefined,
        });
    });
});
