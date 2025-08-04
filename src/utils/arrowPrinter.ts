import * as arrow from "apache-arrow";

export function printArrowResults(buffers: Buffer[]) {
    for (const buf of buffers) {
        const table = arrow.tableFromIPC(buf);

        if (table.numRows === 0) {
            console.log("(no rows)");
            continue;
        }

        const fields = table.schema.fields;
        const columns = fields.map(f => f.name);

        const colVectors = columns.map(name => table.getChild(name)!);

        const colWidths = colVectors.map((vec, i) =>
            Math.max(columns[i].length, ...Array.from({ length: table.numRows }, (_, row) => {
                const val = vec.get(row);
                return String(val ?? "null").length;
            }))
        );

        const header = columns.map((name, i) => name.padEnd(colWidths[i])).join(" | ");
        console.log(header);
        console.log(colWidths.map(w => "-".repeat(w)).join("-+-"));

        for (let i = 0; i < table.numRows; i++) {
            const row = colVectors.map((vec, j) => {
                const val = vec.get(i);
                return String(val ?? "null").padEnd(colWidths[j]);
            });
            console.log(row.join(" | "));
        }
    }
}
