import crypto from "crypto";
import { CANONICAL_UUID } from "./sessionConfig";

/**
 * Per-execution controls accepted by DataFrame actions such as `collect()`.
 * @public
 */
export type ExecutionOptions = {
    /**
     * Cancels only this execution. The stream is cancelled and the operation is
     * released, which interrupts it on the server; unlike the session-wide
     * `withAbortSignal()`, the session stays usable afterwards.
     */
    signal?: AbortSignal;
    /**
     * Spark operation tags for this execution, in addition to the session's
     * tags. `session.interruptTag(tag)` interrupts every running operation
     * that carries the tag.
     */
    tags?: readonly string[];
    /**
     * Canonical UUID to use as the Spark operation id instead of a random one,
     * so the caller can target the execution with `session.interruptOperation()`.
     */
    operationId?: string;
};

/**
 * Execution controls snapshotted when an action starts, so tags added or
 * removed afterwards do not leak into an execution already in flight.
 * @internal
 */
export type ResolvedExecution = {
    signal?: AbortSignal;
    tags: string[];
    operationId: string;
};

/** Mirrors Spark Connect's server-side tag validation. */
export function assertValidTag(tag: unknown): asserts tag is string {
    if (typeof tag !== "string") {
        throw new TypeError("Spark Connect tags must be strings.");
    }
    if (tag === "") {
        throw new TypeError("Spark Connect tags cannot be empty strings.");
    }
    if (tag.includes(",")) {
        throw new TypeError(`Spark Connect tag ${JSON.stringify(tag)} cannot contain ','.`);
    }
}

export function assertOperationId(operationId: unknown): asserts operationId is string {
    if (typeof operationId !== "string" || !CANONICAL_UUID.test(operationId)) {
        throw new TypeError("operationId must be a canonical UUID string.");
    }
}

/** Validates caller options and merges them with the session's tags. */
export function resolveExecution(
    sessionTags: Iterable<string>,
    options: ExecutionOptions = {},
): ResolvedExecution {
    if (options === null || typeof options !== "object") {
        throw new TypeError("Execution options must be an object.");
    }
    const { signal, tags = [], operationId } = options;
    if (signal !== undefined && !(signal instanceof AbortSignal)) {
        throw new TypeError("Execution option signal must be an AbortSignal.");
    }
    if (!Array.isArray(tags)) {
        throw new TypeError("Execution option tags must be an array of strings.");
    }
    tags.forEach(assertValidTag);
    if (operationId !== undefined) assertOperationId(operationId);
    return {
        ...(signal ? { signal } : {}),
        tags: [...new Set([...sessionTags, ...tags])],
        operationId: operationId ?? crypto.randomUUID(),
    };
}

/** Combines the session-wide signal with the per-execution one. */
export function combineSignals(
    sessionSignal: AbortSignal | undefined,
    executionSignal: AbortSignal | undefined,
): AbortSignal | undefined {
    if (!sessionSignal || !executionSignal) return sessionSignal ?? executionSignal;
    return AbortSignal.any([sessionSignal, executionSignal]);
}
