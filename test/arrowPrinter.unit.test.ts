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

describe("printArrowResults display rules", () => {
    afterEach(() => vi.restoreAllMocks());

    it("renders nulls, collapses one-character truncation to an ellipsis and bounds rows", () => {
        const buffer = ipc({ id: [1, 2], value: [null, "long-value"] });
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);

        printArrowResults([buffer], { maxRows: 1, truncate: 1 });
        let lines = log.mock.calls.map(args => String(args[0]));
        expect(lines).toHaveLength(3);
        expect(lines[2]).toContain("…");
        expect(lines[2]).not.toContain("null");

        log.mockClear();
        printArrowResults([buffer, buffer], { maxRows: 2, truncate: 0 });
        lines = log.mock.calls.map(args => String(args[0]));
        expect(lines).toHaveLength(4);
        expect(lines[2]).toContain("null");
        expect(lines[3]).toContain("long-value");
    });
});

describe("printArrowResults value formatting", () => {
    afterEach(() => vi.restoreAllMocks());

    it("prints decimals, temporals, longs and nested values like collect() decodes them", () => {
        const entryField = new arrow.Field(
            "entries",
            new arrow.Struct([
                new arrow.Field("key", new arrow.Utf8(), false),
                new arrow.Field("value", new arrow.Int32(), true),
            ]),
            false,
        );
        const table = new arrow.Table({
            amount: arrow.vectorFromArray(
                [arrow.util.BN.decimal(new Uint32Array([145200, 0, 0, 0]))],
                new arrow.Decimal(2, 10, 128),
            ),
            day: arrow.vectorFromArray([new Date("2024-01-02T00:00:00Z")], new arrow.DateDay()),
            instant: arrow.vectorFromArray([new Date("2024-01-02T03:04:05Z")], new arrow.TimestampMillisecond()),
            big: arrow.vectorFromArray([9007199254740993n], new arrow.Int64()),
            values: arrow.vectorFromArray([[1, null]], new arrow.List(new arrow.Field("item", new arrow.Int32()))),
            nested: arrow.vectorFromArray(
                [{ count: 2, label: "ok" }],
                new arrow.Struct([
                    new arrow.Field("count", new arrow.Int32()),
                    new arrow.Field("label", new arrow.Utf8()),
                ]),
            ),
            labels: arrow.vectorFromArray([new Map([["x", 7]])], new arrow.Map_(entryField, false)),
            raw: arrow.vectorFromArray([new Uint8Array([0xde, 0xad, 0x01])], new arrow.Binary()),
        });
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);

        printArrowResults([Buffer.from(arrow.tableToIPC(table, "stream"))], { truncate: 0 });

        const row = String(log.mock.calls[2]?.[0]);
        const cells = row.split(" | ").map(cell => cell.trim());
        expect(cells).toEqual([
            "1452.00",
            "2024-01-02",
            "2024-01-02T03:04:05.000Z",
            "9007199254740993",
            "[1, null]",
            "{2, ok}",
            "{x -> 7}",
            "[DE AD 01]",
        ]);
    });

    it("prints null for a column missing from a later batch", () => {
        const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
        printArrowResults([ipc({ id: [1], name: ["alice"] }), ipc({ id: [2] })], { truncate: 0 });
        const lines = log.mock.calls.map(args => String(args[0]));
        expect(lines[3]).toMatch(/^2\s+\| null/);
    });
});
