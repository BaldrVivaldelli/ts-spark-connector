import { normalizeArrowValue } from "../typed/arrow-rows";

let arrowModule: typeof import("apache-arrow") | undefined;

function arrow(): typeof import("apache-arrow") {
    return arrowModule ??= require("apache-arrow") as typeof import("apache-arrow");
}

export type ArrowPrintOptions = {
    /** Maximum number of rows to print across all Arrow batches. */
    maxRows?: number;
    /** Maximum displayed width per cell. Set to 0 to disable truncation. */
    truncate?: number;
};

/**
 * Renders one decoded value the way Spark's own `show()` does: arrays as
 * `[a, b]`, structs as `{a, b}`, maps as `{k -> v}` and binaries as hex
 * bytes. Scalars keep the representation `collect()` returns (decimal
 * strings, ISO dates and timestamps, bigint longs).
 */
function formatValue(value: unknown): string {
    if (value == null) return "null";
    if (Array.isArray(value)) return `[${value.map(formatValue).join(", ")}]`;
    if (value instanceof Map) {
        return `{${[...value].map(([key, item]) => `${formatValue(key)} -> ${formatValue(item)}`).join(", ")}}`;
    }
    if (value instanceof Uint8Array) {
        return `[${Array.from(value, byte => byte.toString(16).toUpperCase().padStart(2, "0")).join(" ")}]`;
    }
    if (typeof value === "object") {
        return `{${Object.values(value as Record<string, unknown>).map(formatValue).join(", ")}}`;
    }
    return String(value);
}

function displayValue(text: string, truncate: number): string {
    if (truncate <= 0 || text.length <= truncate) return text;
    if (truncate === 1) return "…";
    return `${text.slice(0, truncate - 1)}…`;
}

/**
 * Pretty-prints a bounded number of rows from one or more Arrow IPC batches.
 * Headers are emitted once even when Spark splits the result into many batches.
 */
export function printArrowResults(
    buffers: Buffer[],
    options: ArrowPrintOptions = {}
): void {
    const maxRows = options.maxRows ?? 20;
    const truncate = options.truncate ?? 20;
    if (!Number.isInteger(maxRows) || maxRows < 0) {
        throw new RangeError("maxRows must be a non-negative integer.");
    }
    if (!Number.isInteger(truncate) || truncate < 0) {
        throw new RangeError("truncate must be a non-negative integer.");
    }

    let columns: string[] | undefined;
    const rows: string[][] = [];

    for (const buf of buffers) {
        if (rows.length >= maxRows) break;
        const table = arrow().tableFromIPC(buf);
        const batchColumns = table.schema.fields.map(field => field.name);
        columns ??= batchColumns;
        const vectors = columns.map(name => table.getChild(name));

        const remaining = maxRows - rows.length;
        const rowCount = Math.min(table.numRows, remaining);
        for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            rows.push(vectors.map(vector => {
                // A batch without the column (never produced by Spark, but
                // tolerated) prints as null.
                const value = vector ? normalizeArrowValue(vector.get(rowIndex), vector.type) : undefined;
                return displayValue(formatValue(value), truncate);
            }));
        }
    }

    if (!columns || rows.length === 0) {
        console.log("(no rows)");
        return;
    }

    const colWidths = columns.map((column, columnIndex) =>
        Math.max(column.length, ...rows.map(row => row[columnIndex]!.length))
    );
    console.log(columns.map((name, index) => name.padEnd(colWidths[index]!)).join(" | "));
    console.log(colWidths.map(width => "-".repeat(width)).join("-+-"));
    for (const row of rows) {
        console.log(row.map((value, index) => value.padEnd(colWidths[index]!)).join(" | "));
    }
}
