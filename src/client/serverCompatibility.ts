import { compareSparkVersions } from "./serverVersion";
import type { SparkSession } from "./session";

type VersionedField = { minimum: string; feature: string };

/**
 * Plan fields of the vendored (newest) protocol that older Spark Connect
 * servers do not define. A server rejects an unknown relation or command with
 * an opaque "Expected ... to be set", and silently drops an unknown field of a
 * message it already knows: `tryCast()` would become a plain cast, which wraps
 * on overflow instead of returning null. Plans that carry one of these fields
 * are checked against the server version before they are sent; any other plan
 * costs no extra round trip.
 */
const VERSIONED_FIELDS: Readonly<Record<string, VersionedField>> = {
    as_of_join: { minimum: "4.0.0", feature: "asOfJoin()" },
    lateral_join: { minimum: "4.0.0", feature: "lateralJoin()" },
    transpose: { minimum: "4.0.0", feature: "transpose()" },
    grouping_sets: { minimum: "4.0.0", feature: "groupingSets()" },
    eval_mode: { minimum: "4.0.0", feature: "tryCast()" },
    clustering_columns: { minimum: "4.0.0", feature: "clusterBy()" },
    checkpoint_command: { minimum: "4.0.0", feature: "checkpoint() and localCheckpoint()" },
    merge_into_table_command: { minimum: "4.0.0", feature: "mergeInto()" },
    streaming_query_listener_bus_command: { minimum: "4.0.0", feature: "streaming query listeners" },
};

/** Spark 4.0 added `isCluster` to the rows of `Catalog.listColumns`. */
const CLUSTER_COLUMN_MINIMUM = "4.0.0";

/** @internal What the executor needs to know about a plan before sending it. */
export type PlanInspection = {
    requirements: VersionedField[];
    /**
     * Alias of each metric of every CollectMetrics node, by observation name.
     * Servers before 4.0 report observed metrics without their names.
     */
    metricNames: Map<string, Array<string | undefined>>;
    listsColumns: boolean;
};

function isPlainNode(value: unknown): value is Record<string, unknown> | unknown[] {
    if (Array.isArray(value)) return true;
    if (value === null || typeof value !== "object") return false;
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
}

/** An unset proto3 field is absent, `false`, empty or zero; any other value is sent. */
function isSet(value: unknown): boolean {
    if (value === undefined || value === null || value === false || value === "" || value === 0) return false;
    if (Array.isArray(value)) return value.length > 0;
    return true;
}

function aliasOf(metric: unknown): string | undefined {
    const name = (metric as { alias?: { name?: unknown } } | null)?.alias?.name;
    return Array.isArray(name) && typeof name[0] === "string" ? name[0] : undefined;
}

/** @internal Walks a plan once, without recursing into binary payloads such as local relation data. */
export function inspectPlan(plan: unknown): PlanInspection {
    const requirements: VersionedField[] = [];
    const seen = new Set<string>();
    const metricNames = new Map<string, Array<string | undefined>>();
    let listsColumns = false;
    const pending: unknown[] = [plan];
    while (pending.length > 0) {
        const node = pending.pop();
        if (!isPlainNode(node)) continue;
        if (Array.isArray(node)) {
            pending.push(...node);
            continue;
        }
        for (const [key, value] of Object.entries(node)) {
            const versioned = VERSIONED_FIELDS[key];
            if (versioned && !seen.has(key) && isSet(value)) {
                seen.add(key);
                requirements.push(versioned);
            }
            if (key === "collect_metrics") {
                const { name, metrics } = (value ?? {}) as { name?: unknown; metrics?: unknown };
                if (typeof name === "string" && Array.isArray(metrics)) metricNames.set(name, metrics.map(aliasOf));
            }
            if (key === "catalog" && (value as { list_columns?: unknown } | null)?.list_columns !== undefined) {
                listsColumns = true;
            }
            pending.push(value);
        }
    }
    return { requirements, metricNames, listsColumns };
}

/** Wraps every `listColumns` relation so it reports `isCluster = false`, as servers before 4.0 omit it. */
function addClusterColumn(node: unknown): unknown {
    if (!isPlainNode(node)) return node;
    if (Array.isArray(node)) return node.map(addClusterColumn);
    const copy = Object.fromEntries(Object.entries(node).map(([key, value]) => [key, addClusterColumn(value)]));
    if ((node.catalog as { list_columns?: unknown } | undefined)?.list_columns === undefined) return copy;
    return {
        project: {
            input: copy,
            expressions: [
                { unresolved_star: {} },
                { alias: { expr: { literal: { boolean: false } }, name: ["isCluster"] } },
            ],
        },
    };
}

/**
 * @internal Rejects a plan the server cannot run faithfully with
 * `UnsupportedServerVersionError`, and adapts the parts an older server
 * answers in an older shape. Only plans that need it ask for the server
 * version (fetched once per session).
 */
export async function preparePlanForServer<P>(
    session: SparkSession,
    plan: P,
): Promise<{ plan: P; metricNames: PlanInspection["metricNames"] }> {
    const inspection = inspectPlan(plan);
    for (const { minimum, feature } of inspection.requirements) {
        await session.requireServerVersion(minimum, feature);
    }
    if (inspection.listsColumns && compareSparkVersions(await session.version(), CLUSTER_COLUMN_MINIMUM) < 0) {
        return { plan: addClusterColumn(plan) as P, metricNames: inspection.metricNames };
    }
    return { plan, metricNames: inspection.metricNames };
}
