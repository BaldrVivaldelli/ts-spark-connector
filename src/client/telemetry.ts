import { inspect } from "node:util";
import type { InspectOptions } from "node:util";
import type {
    SparkConnectionConfig,
    SparkMetric,
    SparkTelemetryEvent,
} from "./session";

const REDACTED = "[REDACTED]";
const SENSITIVE_KEY = /(?:auth(?:orization)?|token|password|secret|credential|private.?key|api.?key|access.?key)/i;
const AUTHORIZATION_VALUE = /\b(Bearer|Basic)\s+[^\s,;]+/gi;
// `;` covers Spark Connect connection strings (`sc://host/;token=…`), the
// form AWS documents for PySpark with `x-aws-proxy-auth`.
const SECRET_QUERY_VALUE = /([?&;](?:token|password|secret|api[_-]?key|x-aws-proxy-auth)=)[^&;#\s]*/gi;

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

/**
 * @internal A Spark Connect address cut before its `;` parameters, which can
 * carry tokens, for messages and printed objects.
 */
export function printableAddress(address: string | undefined): string | undefined {
    return address?.split(";", 1)[0];
}

/**
 * @internal Makes `config` print and serialize without credentials
 * (`console.log`, `util.inspect`, `JSON.stringify`) while its fields stay
 * readable. The added members are not enumerable, so copies and equality
 * checks do not see them.
 */
export function redactWhenPrinted<T extends object>(config: T): T {
    const redacted = () => redactForTelemetry({ ...config });
    Object.defineProperties(config, {
        toJSON: { value: redacted },
        [inspect.custom]: {
            value: (_depth: number, options: InspectOptions) => inspect(redacted(), options),
        },
    });
    return config;
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
