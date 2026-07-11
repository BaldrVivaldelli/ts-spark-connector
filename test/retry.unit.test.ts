import { describe, expect, it, vi } from "vitest";
import * as grpc from "@grpc/grpc-js";
import {
  isRetryableError,
  markNonRetryable,
  resolveRetryConfig,
  withRetry,
} from "../src/client/retry";
import { SparkSession } from "../src";

const noSleep = { sleep: async () => {}, jitter: (b: number) => b };

function grpcError(code: number): Error & { code: number } {
  const err = new Error(`grpc error ${code}`) as Error & { code: number };
  err.code = code;
  return err;
}

describe("resolveRetryConfig", () => {
  it("disables retries by default", () => {
    expect(resolveRetryConfig().maxRetries).toBe(0);
    expect(resolveRetryConfig({}).maxRetries).toBe(0);
  });

  it("applies valid provided values", () => {
    const cfg = resolveRetryConfig({ retry: { maxRetries: 5, initialBackoffMs: 50 } });
    expect(cfg.maxRetries).toBe(5);
    expect(cfg.initialBackoffMs).toBe(50);
  });

  it.each([
    ["negative retries", { maxRetries: -1 }],
    ["fractional retries", { maxRetries: 1.5 }],
    ["non-finite retries", { maxRetries: Number.POSITIVE_INFINITY }],
    ["negative initial backoff", { initialBackoffMs: -1 }],
    ["fractional initial backoff", { initialBackoffMs: 1.5 }],
    ["non-finite maximum backoff", { maxBackoffMs: Number.NaN }],
    ["timer overflow", { maxBackoffMs: 2_147_483_648 }],
    ["shrinking multiplier", { backoffMultiplier: 0.5 }],
    ["non-finite multiplier", { backoffMultiplier: Number.POSITIVE_INFINITY }],
  ])("rejects %s", (_name, retry) => {
    expect(() => resolveRetryConfig({ retry })).toThrow(/retry\./i);
  });

  it("rejects an initial backoff larger than its maximum", () => {
    expect(() => resolveRetryConfig({
      retry: { initialBackoffMs: 500, maxBackoffMs: 100 },
    })).toThrow(/initialBackoffMs.*less than or equal/i);
  });

  it("rejects invalid values at the session builder boundary", () => {
    expect(() => SparkSession.builder().withRetry({ maxRetries: 1.5 }))
      .toThrow(/retry\.maxRetries/i);
  });
});

describe("isRetryableError", () => {
  it("treats UNAVAILABLE / DEADLINE_EXCEEDED / RESOURCE_EXHAUSTED / ABORTED as retryable", () => {
    for (const code of [
      grpc.status.UNAVAILABLE,
      grpc.status.DEADLINE_EXCEEDED,
      grpc.status.RESOURCE_EXHAUSTED,
      grpc.status.ABORTED,
    ]) {
      expect(isRetryableError(grpcError(code))).toBe(true);
    }
  });

  it("does not retry INVALID_ARGUMENT / UNAUTHENTICATED / NOT_FOUND", () => {
    for (const code of [
      grpc.status.INVALID_ARGUMENT,
      grpc.status.UNAUTHENTICATED,
      grpc.status.NOT_FOUND,
    ]) {
      expect(isRetryableError(grpcError(code))).toBe(false);
    }
  });

  it("never retries an error marked non-retryable, even if its code is retryable", () => {
    const err = grpcError(grpc.status.UNAVAILABLE);
    markNonRetryable(err);
    expect(isRetryableError(err)).toBe(false);
  });

  it("does not retry plain errors without a code", () => {
    expect(isRetryableError(new Error("boom"))).toBe(false);
  });
});

describe("withRetry", () => {
  it("returns the result without retrying on success", async () => {
    let calls = 0;
    const result = await withRetry(async () => { calls++; return 42; }, resolveRetryConfig(), noSleep);
    expect(result).toBe(42);
    expect(calls).toBe(1);
  });

  it("retries up to maxRetries on transient errors, then succeeds", async () => {
    let calls = 0;
    const cfg = resolveRetryConfig({ retry: { maxRetries: 3, initialBackoffMs: 10 } });
    const result = await withRetry(async () => {
      calls++;
      if (calls < 3) throw grpcError(grpc.status.UNAVAILABLE);
      return "ok";
    }, cfg, noSleep);

    expect(result).toBe("ok");
    expect(calls).toBe(3);
  });

  it("gives up after maxRetries and rethrows the last error", async () => {
    let calls = 0;
    const cfg = resolveRetryConfig({ retry: { maxRetries: 2, initialBackoffMs: 10 } });
    await expect(
      withRetry(async () => { calls++; throw grpcError(grpc.status.UNAVAILABLE); }, cfg, noSleep)
    ).rejects.toThrow(/grpc error/);
    expect(calls).toBe(3); // initial + 2 retries
  });

  it("does not retry non-retryable errors", async () => {
    let calls = 0;
    const cfg = resolveRetryConfig({ retry: { maxRetries: 5, initialBackoffMs: 10 } });
    await expect(
      withRetry(async () => { calls++; throw grpcError(grpc.status.INVALID_ARGUMENT); }, cfg, noSleep)
    ).rejects.toThrow();
    expect(calls).toBe(1);
  });

  it("applies exponential backoff with the configured multiplier and cap", async () => {
    const delays: number[] = [];
    const cfg = resolveRetryConfig({
      retry: { maxRetries: 4, initialBackoffMs: 100, backoffMultiplier: 2, maxBackoffMs: 350 },
    });
    let calls = 0;
    await expect(
      withRetry(async () => { calls++; throw grpcError(grpc.status.UNAVAILABLE); }, cfg, {
        sleep: async (ms) => { delays.push(ms); },
        jitter: (b) => b, // disable jitter to assert exact values
      })
    ).rejects.toThrow();

    // 100, 200, 350 (capped from 400), 350 (capped)
    expect(delays).toEqual([100, 200, 350, 350]);
  });

  it("reports each retry through onRetry with a one-based attempt and selected delay", async () => {
    const events: Array<{ attempt: number; delayMs: number; error: unknown }> = [];
    const transient = grpcError(grpc.status.UNAVAILABLE);
    const cfg = resolveRetryConfig({
      retry: {
        maxRetries: 2,
        initialBackoffMs: 25,
        onRetry: event => events.push(event),
      },
    });
    let calls = 0;

    await expect(withRetry(async () => {
      calls += 1;
      throw transient;
    }, cfg, noSleep)).rejects.toBe(transient);

    expect(events).toEqual([
      { attempt: 1, delayMs: 25, error: transient },
      { attempt: 2, delayMs: 50, error: transient },
    ]);
  });

  it("cancels an in-progress backoff through AbortSignal", async () => {
    const controller = new AbortController();
    const cfg = resolveRetryConfig({
      retry: { maxRetries: 2, initialBackoffMs: 10_000 },
    });
    const promise = withRetry(
      async () => { throw grpcError(grpc.status.UNAVAILABLE); },
      cfg,
      { signal: controller.signal, jitter: value => value },
    );

    controller.abort("test cancellation");
    await expect(promise).rejects.toMatchObject({ name: "AbortError" });
  });

  it("does not start an attempt when already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    const operation = vi.fn(async () => "never");

    await expect(withRetry(operation, resolveRetryConfig(), { signal: controller.signal }))
      .rejects.toMatchObject({ name: "AbortError" });
    expect(operation).not.toHaveBeenCalled();
  });
});
