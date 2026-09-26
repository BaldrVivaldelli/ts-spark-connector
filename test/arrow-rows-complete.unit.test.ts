import * as arrow from "apache-arrow";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
    arrowBuffersFromResponses,
    rowsFromArrowBuffers,
} from "../src/typed/arrow-rows";
import type { FieldSpec, SchemaDef } from "../src/schema/schema";

const arrowRuntime = require("apache-arrow") as typeof import("apache-arrow");

function tableBuffer(fields: arrow.Field[]): Buffer {
    const table = new arrow.Table(new arrow.Schema(fields));
    return Buffer.from(arrow.tableToIPC(table, "stream"));
}

function field(name: string, type: arrow.DataType, nullable = false): arrow.Field {
    return new arrow.Field(name, type, nullable);
}

function expectSchemaAccepted(actual: arrow.Field[], expected: SchemaDef): void {
    expect(rowsFromArrowBuffers([tableBuffer(actual)], expected)).toEqual([]);
}

function expectTypeRejected(actual: arrow.DataType, expected: FieldSpec): void {
    expect(() => rowsFromArrowBuffers(
        [tableBuffer([field("value", actual)])],
        { value: expected },
    )).toThrow(/schema mismatch/i);
}

describe("complete Arrow row schema behavior", () => {
    afterEach(() => vi.restoreAllMocks());

    it("filters nullish and empty Arrow response shapes", () => {
        const data = Buffer.from("ipc");
        expect(arrowBuffersFromResponses([
            null,
            undefined,
            {},
            { arrow_batch: null },
            { arrow_batch: {} },
            { arrow_batch: { data } },
        ])).toEqual([data]);
    });

    it("accepts every primitive token and dictionary strings", () => {
        expectSchemaAccepted([
            field("int", new arrow.Int32()),
            field("long", new arrow.Int64()),
            field("float", new arrow.Float32()),
            field("double", new arrow.Float64()),
            field("string", new arrow.Utf8()),
            field("large", new arrow.LargeUtf8()),
            field("boolean", new arrow.Bool()),
            field("date", new arrow.DateDay()),
            field("timestamp", new arrow.TimestampMillisecond()),
            field("dictionary", new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32())),
        ], {
            int: "int",
            long: "long",
            float: "float",
            double: "double",
            string: "string",
            large: "string",
            boolean: "boolean",
            date: "date",
            timestamp: "timestamp",
            dictionary: "string",
        });
    });

    it("rejects every primitive mismatch and unknown runtime token", () => {
        for (const token of [
            "int",
            "long",
            "float",
            "double",
            "string",
            "boolean",
            "date",
            "timestamp",
        ] as const) {
            expectTypeRejected(new arrow.Binary(), token);
        }
        expectTypeRejected(
            new arrow.Binary(),
            "unsupported" as unknown as FieldSpec,
        );
        expectTypeRejected(new arrow.Uint32(), "int");
        expectTypeRejected(new arrow.Int16(), "int");
        expectTypeRejected(new arrow.Uint64(), "long");
        expectTypeRejected(new arrow.Float64(), "float");
        expectTypeRejected(new arrow.Float32(), "double");
    });

    it("validates exact column order, count and nullability", () => {
        expect(() => rowsFromArrowBuffers(
            [tableBuffer([field("actual", new arrow.Int32())])],
            { expected: "int" },
        )).toThrow(/expected columns.*expected.*received.*actual/i);
        expect(() => rowsFromArrowBuffers(
            [tableBuffer([
                field("first", new arrow.Int32()),
                field("second", new arrow.Int32()),
            ])],
            { first: "int" },
        )).toThrow(/expected columns/i);
        expect(() => rowsFromArrowBuffers(
            [tableBuffer([field("value", new arrow.Int32(), true)])],
            { value: "int" },
        )).toThrow(/non-null/i);
        expectSchemaAccepted(
            [field("value", new arrow.Int32(), true)],
            { value: "int?" },
        );
    });

    it("accepts decimals, lists, maps and nested structs", () => {
        const list = new arrow.List(field("item", new arrow.Int32(), true));
        const fixedList = new arrow.FixedSizeList(
            2,
            field("item", new arrow.Int32(), true),
        );
        const entries = field("entries", new arrow.Struct([
            field("key", new arrow.Utf8()),
            field("value", new arrow.Int32(), true),
        ]));
        const map = new arrow.Map_(entries);
        const struct = new arrow.Struct([
            field("name", new arrow.Utf8()),
            field("scores", list, true),
        ]);
        expectSchemaAccepted([
            field("decimal", new arrow.Decimal(2, 10, 128)),
            field("list", list),
            field("fixed", fixedList),
            field("map", map),
            field("struct", struct),
        ], {
            decimal: { kind: "decimal", precision: 10, scale: 2 },
            list: { kind: "array", element: "int?" },
            fixed: { kind: "array", element: "int?" },
            map: { kind: "map", key: "string", value: "int?" },
            struct: {
                kind: "struct",
                fields: {
                    name: "string",
                    scores: { kind: "array", element: "int?", nullable: true },
                },
            },
        });
    });

    it("rejects malformed or mismatched complex declarations", () => {
        expectTypeRejected(
            new arrow.Decimal(2, 10, 128),
            { kind: "decimal", precision: 11, scale: 2 },
        );
        expectTypeRejected(new arrow.Int32(), {
            kind: "decimal",
            precision: 10,
            scale: 2,
        });
        expectTypeRejected(new arrow.Int32(), {
            kind: "array",
            element: "int",
        });
        expectTypeRejected(
            new arrow.List(field("item", new arrow.Utf8())),
            { kind: "array", element: "int" },
        );
        expectTypeRejected(new arrow.Int32(), {
            kind: "map",
            key: "string",
            value: "int",
        });

        const entries = field("entries", new arrow.Struct([
            field("key", new arrow.Utf8()),
            field("value", new arrow.Int32()),
        ]));
        expectTypeRejected(new arrow.Map_(entries), {
            kind: "map",
            key: "int",
            value: "int",
        });
        expectTypeRejected(new arrow.Map_(entries), {
            kind: "map",
            key: "string",
            value: "string",
        });
        expectTypeRejected(new arrow.Int32(), {
            kind: "struct",
            fields: { value: "int" },
        });
        expectTypeRejected(new arrow.Struct([
            field("other", new arrow.Int32()),
        ]), {
            kind: "struct",
            fields: { value: "int" },
        });
        expectTypeRejected(new arrow.Struct([
            field("value", new arrow.Utf8()),
        ]), {
            kind: "struct",
            fields: { value: "int" },
        });
    });

    it("normalizes null, temporal, negative/zero-scale decimal, fixed-list and map values", () => {
        const values = [
            null,
            0,
            0,
            "-5",
            "12",
            [1, null],
            { value: 2 },
            new Map([["a", 3]]),
        ];
        const entry = field("entries", new arrow.Struct([
            field("key", new arrow.Utf8()),
            field("value", new arrow.Int32()),
        ]));
        const fields = [
            field("nullable", new arrow.Int32(), true),
            field("day", new arrow.DateDay()),
            field("instant", new arrow.TimestampMillisecond()),
            field("negative", new arrow.Decimal(2, 10, 128)),
            field("unscaled", new arrow.Decimal(0, 10, 128)),
            field("fixed", new arrow.FixedSizeList(2, field("item", new arrow.Int32(), true))),
            field("nested", new arrow.Struct([field("value", new arrow.Int32())])),
            field("mapped", new arrow.Map_(entry)),
        ];
        const fakeTable = {
            schema: { fields },
            numRows: 1,
            getChildAt(index: number) {
                return { get: () => values[index] };
            },
        };
        vi.spyOn(arrowRuntime, "tableFromIPC").mockReturnValue(
            fakeTable as unknown as arrow.Table,
        );

        expect(rowsFromArrowBuffers([Buffer.alloc(0)])).toEqual([{
            nullable: null,
            day: "1970-01-01",
            instant: "1970-01-01T00:00:00.000Z",
            negative: "-0.05",
            unscaled: "12",
            fixed: [1, null],
            nested: { value: 2 },
            mapped: new Map([["a", 3]]),
        }]);
    });

    it("reports missing vectors from a malformed decoded table", () => {
        const fakeTable = {
            schema: { fields: [field("value", new arrow.Int32())] },
            numRows: 1,
            getChildAt: () => undefined,
        };
        vi.spyOn(arrowRuntime, "tableFromIPC").mockReturnValue(
            fakeTable as unknown as arrow.Table,
        );
        expect(() => rowsFromArrowBuffers([Buffer.alloc(0)]))
            .toThrow(/missing column vector at index 0/i);
    });
});
