import type {
    SparkConnectionConfig,
    SparkMetric,
    SparkTelemetryEvent,
} from "./session";

const REDACTED = "[REDACTED]";
const SENSITIVE_KEY = /(?:auth(?:orization)?|token|password|secret|credential|private.?key|api.?key)/i;
const AUTHORIZATION_VALUE = /\b(Bearer|Basic)\s+[^\s,;]+/gi;
const SECRET_QUERY_VALUE = /([?&](?:token|password|secret|api[_-]?key)=)[^&#\s]*/gi;

function redactString(value: string): string {
    return value
        .replace(AUTHORIZATION_VALUE, "$1 [REDACTED]")
        .replace(SECRET_QUERY_VALUE, "$1[REDACTED]");
}

function redactValue(value: unknown, seen: WeakSet<object>): unknown {
    if (typeof value === "string") return redactString(value);
    if (value == null || typeof value !== "object") return value;
    if (Buffer.isBuffer(value)) return `[Buffer ${value.length} bytes]`;
    if (seen.has(value)) return "[Circular]";
    seen.add(value);

    if (Array.isArray(value)) return value.map(item => redactValue(item, seen));
    const output: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(value)) {
        output[key] = SENSITIVE_KEY.test(key)
            ? REDACTED
            : redactValue(nested, seen);
    }
    return output;
}

function observeSafely<T>(
    observer: ((value: T) => unknown) | undefined,
    value: T,
): void {
    if (!observer) return;
    try {
        const result = observer(value);
        if (result && typeof (result as PromiseLike<unknown>).then === "function") {
            Promise.resolve(result).catch(() => undefined);
        }
    } catch {
        // Observability is intentionally isolated from transport control flow.
    }
}

/** @internal Recursively removes credentials before an object reaches a logger. */
export function redactForTelemetry(value: unknown): unknown {
    return redactValue(value, new WeakSet<object>());
}

/** @internal Emits best-effort telemetry; an observer can never fail the RPC. */
export function emitTelemetry(
    config: SparkConnectionConfig | undefined,
    name: string,
    level: SparkTelemetryEvent["level"],
    attributes: Record<string, unknown> = {}
): void {
    if (!config?.logger && !config?.metrics) return;
    const timestamp = new Date().toISOString();
    const redactedAttributes = redactForTelemetry(attributes) as Record<string, unknown>;
    const event: SparkTelemetryEvent = {
        name,
        level,
        timestamp,
        attributes: redactedAttributes,
    };

    observeSafely(config.logger, event);
    if (config.metrics) {
        const counter: SparkMetric = {
            name: `${name}.count`,
            kind: "counter",
            value: 1,
            unit: "count",
            timestamp,
            attributes: redactedAttributes,
        };
        observeSafely(config.metrics, counter);

        const durationMs = redactedAttributes.durationMs;
        if (typeof durationMs === "number" && Number.isFinite(durationMs) && durationMs >= 0) {
            const duration: SparkMetric = {
                name: `${name}.duration`,
                kind: "histogram",
                value: durationMs,
                unit: "milliseconds",
                timestamp,
                attributes: redactedAttributes,
            };
            observeSafely(config.metrics, duration);
        }
    }
}
