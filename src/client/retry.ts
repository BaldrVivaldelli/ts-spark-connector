import type { RetryConfig, SparkConnectionConfig } from "./session";

const MAX_TIMER_DELAY_MS = 2_147_483_647;

function noopOnRetry(): void {
    // Intentionally empty default observer.
}

export const DEFAULT_RETRY: Required<RetryConfig> = {
    maxRetries: 0,
    initialBackoffMs: 200,
    maxBackoffMs: 10_000,
    backoffMultiplier: 2,
    onRetry: noopOnRetry,
};

// gRPC status codes that typically indicate a transient failure worth retrying.
// UNAVAILABLE: connection issues / server not ready.
// DEADLINE_EXCEEDED: timeout that may succeed on retry.
// RESOURCE_EXHAUSTED: backpressure / rate limiting.
// ABORTED: transient contention.
export const RETRYABLE_STATUS_CODES = new Set<number>([
    14, // UNAVAILABLE
    4,  // DEADLINE_EXCEEDED
    8,  // RESOURCE_EXHAUSTED
    10, // ABORTED
]);

function assertSafeIntegerInRange(name: string, value: number, max: number): void {
    if (!Number.isSafeInteger(value) || value < 0 || value > max) {
        throw new RangeError(`${name} must be a safe integer between 0 and ${max}; received ${String(value)}.`);
    }
}

/** Validates retry values before they can reach timers or retry loops. */
export function validateRetryConfig(retry: RetryConfig = {}): void {
    if (retry.maxRetries !== undefined) {
        assertSafeIntegerInRange("retry.maxRetries", retry.maxRetries, Number.MAX_SAFE_INTEGER);
    }
    if (retry.initialBackoffMs !== undefined) {
        assertSafeIntegerInRange("retry.initialBackoffMs", retry.initialBackoffMs, MAX_TIMER_DELAY_MS);
    }
    if (retry.maxBackoffMs !== undefined) {
        assertSafeIntegerInRange("retry.maxBackoffMs", retry.maxBackoffMs, MAX_TIMER_DELAY_MS);
    }
    if (retry.backoffMultiplier !== undefined
        && (!Number.isFinite(retry.backoffMultiplier) || retry.backoffMultiplier < 1)) {
        throw new RangeError(
            `retry.backoffMultiplier must be a finite number greater than or equal to 1; ` +
            `received ${String(retry.backoffMultiplier)}.`
        );
    }
    if (retry.onRetry !== undefined && typeof retry.onRetry !== "function") {
        throw new TypeError("retry.onRetry must be a function.");
    }

    const initialBackoffMs = retry.initialBackoffMs ?? DEFAULT_RETRY.initialBackoffMs;
    const maxBackoffMs = retry.maxBackoffMs ?? DEFAULT_RETRY.maxBackoffMs;
    if (initialBackoffMs > maxBackoffMs) {
        throw new RangeError(
            "retry.initialBackoffMs must be less than or equal to retry.maxBackoffMs."
        );
    }
}

export function resolveRetryConfig(config?: SparkConnectionConfig): Required<RetryConfig> {
    const retry = config?.retry ?? {};
    validateRetryConfig(retry);

    const resolved: Required<RetryConfig> = {
        maxRetries: retry.maxRetries ?? DEFAULT_RETRY.maxRetries,
        initialBackoffMs: retry.initialBackoffMs ?? DEFAULT_RETRY.initialBackoffMs,
        maxBackoffMs: retry.maxBackoffMs ?? DEFAULT_RETRY.maxBackoffMs,
        backoffMultiplier: retry.backoffMultiplier ?? DEFAULT_RETRY.backoffMultiplier,
        onRetry: retry.onRetry ?? DEFAULT_RETRY.onRetry,
    };

    return resolved;
}

export function isRetryableError(error: unknown): boolean {
    if ((error as { __noRetry?: boolean } | null)?.__noRetry) {
        return false;
    }
    const code = (error as { code?: number } | null)?.code;
    return typeof code === "number" && RETRYABLE_STATUS_CODES.has(code);
}

/** Marks an error so withRetry will never retry it, regardless of status code. */
export function markNonRetryable(error: unknown): void {
    if (error && typeof error === "object") {
        (error as { __noRetry?: boolean }).__noRetry = true;
    }
}

export function abortError(reason?: unknown): Error {
    const error = new Error(reason === undefined ? "The operation was aborted." : String(reason));
    error.name = "AbortError";
    return error;
}

export function throwIfAborted(signal?: AbortSignal): void {
    if (signal?.aborted) throw abortError(signal.reason);
}

function defaultDelay(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
        throwIfAborted(signal);
        const timer = setTimeout(() => {
            signal?.removeEventListener("abort", onAbort);
            resolve();
        }, ms);
        const onAbort = () => {
            clearTimeout(timer);
            signal?.removeEventListener("abort", onAbort);
            reject(abortError(signal?.reason));
        };
        signal?.addEventListener("abort", onAbort, { once: true });
    });
}

export type RetryWaitHooks = {
    sleep?: (ms: number) => Promise<void>;
    jitter?: (backoff: number) => number;
    signal?: AbortSignal;
};

/**
 * Waits before a numbered retry, applying the same retryability, backoff,
 * jitter, observer and cancellation rules as `withRetry`.
 */
export async function waitBeforeRetry(
    error: unknown,
    retryNumber: number,
    retryConfig: Required<RetryConfig>,
    hooks: RetryWaitHooks = {}
): Promise<void> {
    if (!Number.isSafeInteger(retryNumber) || retryNumber < 1) {
        throw new RangeError("retryNumber must be a positive safe integer.");
    }
    throwIfAborted(hooks.signal);
    if (retryNumber > retryConfig.maxRetries || !isRetryableError(error)) {
        throw error;
    }

    const exponential = retryConfig.initialBackoffMs
        * Math.pow(retryConfig.backoffMultiplier, retryNumber - 1);
    const cappedBackoff = Math.min(exponential, retryConfig.maxBackoffMs);
    const delayMs = (hooks.jitter ?? ((backoff: number) => Math.random() * backoff))(cappedBackoff);
    retryConfig.onRetry({ attempt: retryNumber, delayMs, error });

    if (hooks.sleep) {
        await hooks.sleep(delayMs);
        throwIfAborted(hooks.signal);
    } else {
        await defaultDelay(delayMs, hooks.signal);
    }
}

export type RetryHooks = {
    /** Override the delay implementation (used in tests to avoid real timers). */
    sleep?: (ms: number) => Promise<void>;
    /** Override jitter selection (defaults to full jitter via Math.random). */
    jitter?: (backoff: number) => number;
    /** Cancels both the next attempt and the default backoff timer. */
    signal?: AbortSignal;
};

/**
 * Retries an idempotent operation on transient gRPC errors using exponential
 * backoff with full jitter. Non-retryable errors propagate immediately.
 */
export async function withRetry<T>(
    operation: () => Promise<T>,
    retryConfig: Required<RetryConfig>,
    hooks: RetryHooks = {}
): Promise<T> {
    const sleep = hooks.sleep
        ? async (ms: number) => {
            throwIfAborted(hooks.signal);
            await hooks.sleep!(ms);
            throwIfAborted(hooks.signal);
        }
        : (ms: number) => defaultDelay(ms, hooks.signal);
    const jitter = hooks.jitter ?? ((backoff: number) => Math.random() * backoff);

    let attempt = 0;
    let backoff = retryConfig.initialBackoffMs;

    for (;;) {
        throwIfAborted(hooks.signal);
        try {
            return await operation();
        } catch (error) {
            if (attempt >= retryConfig.maxRetries || !isRetryableError(error)) {
                throw error;
            }
            const cappedBackoff = Math.min(backoff, retryConfig.maxBackoffMs);
            const delayMs = jitter(cappedBackoff);
            retryConfig.onRetry({ attempt: attempt + 1, delayMs, error });
            await sleep(delayMs);
            backoff = Math.min(backoff * retryConfig.backoffMultiplier, retryConfig.maxBackoffMs);
            attempt += 1;
        }
    }
}
