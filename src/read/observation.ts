import crypto from "crypto";

/**
 * Named metrics collected while an action runs over a DataFrame that called
 * `observe(observation, ...metrics)`. Spark evaluates the aggregate
 * expressions over the rows the plan produces and reports the values once
 * per action, so `get()` resolves after the action that ran them.
 *
 * ```ts
 * const stats = new Observation("stats");
 * const rows = await df.observe(stats, F.count(F.lit(1)).alias("rows")).collect();
 * const { rows: observed } = await stats.get();
 * ```
 * @public
 */
export class Observation {
    readonly name: string;
    private latest?: Record<string, unknown>;
    private waiters: Array<(values: Record<string, unknown>) => void> = [];

    /** `name` defaults to a unique `observation_<uuid>`. */
    constructor(name?: string) {
        if (name !== undefined && (typeof name !== "string" || name.trim() === "")) {
            throw new TypeError("Observation name must be a non-empty string.");
        }
        this.name = name ?? `observation_${crypto.randomUUID()}`;
    }

    /**
     * The metrics of the last action that ran this observation, or of the next
     * one when none has run yet. Each key is the metric's column alias.
     */
    get(): Promise<Record<string, unknown>> {
        if (this.latest !== undefined) return Promise.resolve({ ...this.latest });
        return new Promise(resolve => {
            this.waiters.push(resolve);
        });
    }

    /** The last metrics received, or `undefined` before any action ran. */
    get values(): Record<string, unknown> | undefined {
        return this.latest === undefined ? undefined : { ...this.latest };
    }

    /** @internal Called by the session when an ExecutePlan response carries this observation. */
    record(values: Record<string, unknown>): void {
        this.latest = values;
        const waiting = this.waiters;
        this.waiters = [];
        for (const resolve of waiting) resolve({ ...values });
    }
}

/** @internal One `ObservedMetrics` message of an ExecutePlanResponse, decoded. */
export type ObservedMetrics = {
    name: string;
    values: Record<string, unknown>;
};

type ProtoLiteral = Record<string, unknown> & { literal_type?: string };

/**
 * The literal kinds Spark may put in observed metrics, in the order used to
 * detect which one is set when the response carries no `literal_type` oneof
 * marker (proto-loader adds it for real responses).
 */
const LITERAL_KINDS = [
    "null", "boolean", "byte", "short", "integer", "long", "float", "double",
    "decimal", "string", "date", "timestamp", "timestamp_ntz", "binary", "array", "map",
] as const;

function literalKind(literal: ProtoLiteral): string | undefined {
    if (typeof literal.literal_type === "string") return literal.literal_type;
    return LITERAL_KINDS.find(kind => literal[kind] !== undefined && literal[kind] !== null);
}

function epochDays(days: number): string {
    return new Date(days * 86_400_000).toISOString().slice(0, 10);
}

function epochMicros(micros: unknown): string {
    return new Date(Number(BigInt(String(micros)) / 1000n)).toISOString();
}

/** @internal Decodes an `Expression.Literal` the way `collect()` represents the same value. */
export function literalValue(input: unknown): unknown {
    const literal = (input ?? {}) as ProtoLiteral;
    const kind = literalKind(literal);
    switch (kind) {
        case "null":
            return null;
        case "boolean":
            return literal.boolean;
        case "byte":
        case "short":
        case "integer":
        case "float":
        case "double":
            return Number(literal[kind]);
        case "long":
            return BigInt(String(literal.long));
        case "decimal":
            return String((literal.decimal as { value?: unknown } | undefined)?.value ?? "");
        case "string":
            return String(literal.string);
        case "date":
            return epochDays(Number(literal.date));
        case "timestamp":
        case "timestamp_ntz":
            return epochMicros(literal[kind]);
        case "binary":
            return literal.binary;
        case "array": {
            const elements = (literal.array as { elements?: unknown[] } | undefined)?.elements ?? [];
            return elements.map(literalValue);
        }
        case "map": {
            const map = literal.map as { keys?: unknown[]; values?: unknown[] } | undefined;
            const keys = (map?.keys ?? []).map(literalValue);
            const values = (map?.values ?? []).map(literalValue);
            return new Map(keys.map((key, index) => [key, values[index]]));
        }
        default:
            return literal;
    }
}

/** @internal Extracts and decodes the observed metrics carried by an ExecutePlan response. */
export function observedMetricsOf(response: unknown): ObservedMetrics[] {
    const carrier = response as { observed_metrics?: unknown; observedMetrics?: unknown } | null;
    const metrics = carrier?.observed_metrics ?? carrier?.observedMetrics;
    if (!Array.isArray(metrics)) return [];
    return metrics.flatMap(metric => {
        const { name, keys, values } = (metric ?? {}) as { name?: unknown; keys?: unknown; values?: unknown };
        if (typeof name !== "string" || !name) return [];
        const keyList = Array.isArray(keys) ? keys.map(String) : [];
        const valueList = Array.isArray(values) ? values : [];
        return [{
            name,
            values: Object.fromEntries(valueList.map((value, index) => [keyList[index] ?? `_${index + 1}`, literalValue(value)])),
        }];
    });
}
