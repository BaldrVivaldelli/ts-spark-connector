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

function displayValue(value: unknown, truncate: number): string {
    const text = String(value ?? "null");
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
            rows.push(vectors.map(vector => displayValue(vector?.get(rowIndex), truncate)));
        }
    }

    if (!columns || rows.length === 0) {
        console.log("(no rows)");
        return;
    }

    const colWidths = columns.map((column, columnIndex) =>
        Math.max(column.length, ...rows.map(row => row[columnIndex]?.length ?? 0))
    );
    console.log(columns.map((name, index) => name.padEnd(colWidths[index])).join(" | "));
    console.log(colWidths.map(width => "-".repeat(width)).join("-+-"));
    for (const row of rows) {
        console.log(row.map((value, index) => value.padEnd(colWidths[index])).join(" | "));
    }
}
