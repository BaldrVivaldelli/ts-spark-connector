import { describe, expect, it, vi } from "vitest";
import { SparkSession } from "../src/client/session";
import { emitTelemetry, redactForTelemetry } from "../src/client/telemetry";
import { emitExecuteCleanupTelemetry } from "../src/client/sparkClient";

describe("structured Spark Connect telemetry", () => {
    it("recursively redacts credentials, authorization headers and secret URL values", () => {
        const value = redactForTelemetry({
            auth: { type: "token", token: "top-secret" },
            tls: { keyStorePassword: "changeit", serverNameOverride: "spark" },
            headers: { Authorization: "Bearer abc.def.ghi", requestId: "request-1" },
            url: "https://spark.invalid/connect?token=abc123&mode=test",
            message: "retrying with Basic YWxpY2U6c2VjcmV0",
        });

        expect(value).toEqual({
            auth: "[REDACTED]",
            tls: { keyStorePassword: "[REDACTED]", serverNameOverride: "spark" },
            headers: { Authorization: "[REDACTED]", requestId: "request-1" },
            url: "https://spark.invalid/connect?token=[REDACTED]&mode=test",
            message: "retrying with Basic [REDACTED]",
        });
    });

    it("is opt-in, emits structured events and isolates logger failures", () => {
        const logger = vi.fn();
        emitTelemetry(undefined, "spark.rpc.start", "debug", { rpc: "Config" });
        expect(logger).not.toHaveBeenCalled();

        emitTelemetry({ logger }, "spark.rpc.start", "debug", {
            rpc: "Config",
            token: "must-not-leak",
        });
        expect(logger).toHaveBeenCalledWith(expect.objectContaining({
            name: "spark.rpc.start",
            level: "debug",
            timestamp: expect.any(String),
            attributes: { rpc: "Config", token: "[REDACTED]" },
        }));

        expect(() => emitTelemetry({ logger: () => { throw new Error("observer failed"); } },
            "spark.rpc.end", "debug")).not.toThrow();
    });

    it("is configurable through SparkSession.builder", () => {
        const logger = vi.fn();
        const session = SparkSession.builder().withLogger(logger).getOrCreate();

        expect(session.getConnectionConfig().logger).toBe(logger);
        expect(() => SparkSession.builder().withLogger(null as never)).toThrow(TypeError);
    });

    it("distinguishes post-completion ReleaseExecute cleanup failures", () => {
        const logger = vi.fn();

        emitExecuteCleanupTelemetry(
            { logger },
            "operation-1",
            Object.assign(new Error("release failed"), { code: 14 })
        );

        expect(logger).toHaveBeenCalledWith(expect.objectContaining({
            name: "spark.execute.cleanup_error",
            level: "warn",
            attributes: {
                operationId: "operation-1",
                code: 14,
                errorClass: undefined,
            },
        }));
    });
});
