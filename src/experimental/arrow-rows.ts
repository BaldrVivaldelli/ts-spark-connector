/**
 * EXPERIMENTAL — decode Spark Connect Arrow batches into plain row objects.
 *
 * The untyped API only has `printArrowResults` (display) and `collectRaw`
 * (raw responses). To return typed rows from `TypedDataFrame.collect()` we need
 * to materialize the Arrow IPC buffers into `{ column: value }` objects.
 */

import * as arrow from "apache-arrow";

type ArrowBatchResponse = { arrow_batch?: { data?: Buffer } };

/** Extracts the Arrow IPC buffers from raw Spark Connect ExecutePlan responses. */
export function arrowBuffersFromResponses(responses: unknown[]): Buffer[] {
    return responses
        .filter((r): r is ArrowBatchResponse => !!(r as ArrowBatchResponse)?.arrow_batch?.data)
        .map(r => (r as ArrowBatchResponse).arrow_batch!.data as Buffer);
}

/**
 * Decodes Arrow IPC buffers into an array of row objects. Each row is a plain
 * object keyed by column name. BigInt values (Spark longs) are normalized to
 * `number` when they fit safely, otherwise left as `bigint`.
 */
export function rowsFromArrowBuffers<Row>(buffers: Buffer[]): Row[] {
    const rows: Row[] = [];

    for (const buf of buffers) {
        const table = arrow.tableFromIPC(buf);
        if (table.numRows === 0) continue;

        const fields = table.schema.fields as Array<{ name: string }>;
        const names = fields.map(f => f.name);
        const vectors = names.map(name => table.getChild(name)!);

        for (let r = 0; r < table.numRows; r++) {
            const row: Record<string, unknown> = {};
            for (let c = 0; c < names.length; c++) {
                row[names[c]] = normalizeValue(vectors[c].get(r));
            }
            rows.push(row as Row);
        }
    }

    return rows;
}

function normalizeValue(value: unknown): unknown {
    if (typeof value === "bigint") {
        return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
            ? Number(value)
            : value;
    }
    return value;
}
