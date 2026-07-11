import { afterEach, describe, expect, it, vi } from "vitest";
import * as arrow from "apache-arrow";
import { printArrowResults } from "../src/utils/arrowPrinter";

function ipc(columns: Record<string, unknown[]>): Buffer {
    const table = arrow.tableFromArrays(columns);
    return Buffer.from(arrow.tableToIPC(table, "stream"));
}

describe("printArrowResults", () => {
    afterEach(() => vi.restoreAllMocks());

    it("prints one header and respects the global row bound across batches", () => {
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);

        printArrowResults([
            ipc({ id: [1, 2], name: ["alice", "bob"] }),
            ipc({ id: [3, 4], name: ["carol", "dave"] }),
        ], { maxRows: 3, truncate: 20 });

        const lines = log.mock.calls.map(args => String(args[0]));
        expect(lines).toHaveLength(5); // header, separator, three rows
        expect(lines.filter(line => line.includes("id") && line.includes("name"))).toHaveLength(1);
        expect(lines.join("\n")).toContain("carol");
        expect(lines.join("\n")).not.toContain("dave");
    });

    it("truncates cells and handles empty results", () => {
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
        printArrowResults([ipc({ value: ["abcdefgh"] })], { maxRows: 20, truncate: 5 });
        expect(log.mock.calls.map(args => String(args[0])).join("\n")).toContain("abcd…");

        log.mockClear();
        printArrowResults([], { maxRows: 20 });
        expect(log).toHaveBeenCalledWith("(no rows)");
    });

    it("rejects invalid bounds", () => {
        expect(() => printArrowResults([], { maxRows: -1 })).toThrow(RangeError);
        expect(() => printArrowResults([], { truncate: 1.5 })).toThrow(RangeError);
    });
});
