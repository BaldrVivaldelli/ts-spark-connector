import { describe, expect, it } from "vitest";
import { ProtoDFAlg } from "../src/engine/compilerRead";
import { SparkDFAlg } from "../src/read/readDataFrameInterpreter";
import { TraceDFAlg } from "../src/trace/trace";
import { encodeLocalRelation } from "../src/typed/arrow-encode";
import { rowsFromArrowBuffers } from "../src/typed/arrow-rows";
import { SparkSession, arrayType, decimalType, mapType, schema, structType } from "../src";

const session = SparkSession.builder()
    .config("spark.connect.url", "sc://localhost:15002")
    .getOrCreate();

const Events = schema({
    id: "long",
    score: "double",
    ratio: "float?",
    count: "int",
    name: "string",
    active: "boolean?",
    day: "date",
    at: "timestamp?",
});

const rows = [
    {
        id: 1n,
        score: 0.5,
        ratio: 0.25,
        count: 7,
        name: "ada",
        active: true,
        day: "2026-01-31",
        at: "2026-01-31T12:30:45.123Z",
    },
    {
        id: 9007199254740993n,
        score: -1.5,
        ratio: null,
        count: -3,
        name: "grace",
        active: null,
        day: "1969-12-31",
        at: null,
    },
];

describe("encodeLocalRelation", () => {
    it("round-trips through the connector's own Arrow decoder", () => {
        const buffer = Buffer.from(encodeLocalRelation(rows, Events.def));
        const decoded = rowsFromArrowBuffers([buffer], Events.def);
        expect(decoded).toEqual([{ ...rows[0], ratio: 0.25 }, rows[1]]);
    });

    it("stays in a single record batch beyond the arrow chunking threshold", () => {
        const Numbers = schema({ n: "int" });
        const many = Array.from({ length: 4321 }, (_v, n) => ({ n }));
        const buffer = Buffer.from(encodeLocalRelation(many, Numbers.def));
        const decoded = rowsFromArrowBuffers<{ n: number }>([buffer], Numbers.def);
        expect(decoded).toHaveLength(4321);
        expect(decoded[4320]).toEqual({ n: 4320 });
    });

    it("accepts safe-integer numbers for long columns", () => {
        const Longs = schema({ id: "long" });
        const buffer = Buffer.from(encodeLocalRelation([{ id: 41 }], Longs.def));
        expect(rowsFromArrowBuffers([buffer], Longs.def)).toEqual([{ id: 41n }]);
    });

    it("rejects an empty schema", () => {
        expect(() => encodeLocalRelation([], {})).toThrow(/at least one column/);
    });

    it("round-trips decimal, array, map and struct columns through the decoder", () => {
        const Complex = schema({
            amount: decimalType(10, 2),
            tags: arrayType("string"),
            counts: mapType("string", "long"),
            byId: mapType("int", "string", true),
            addr: structType({ city: "string", zip: "int" }),
            people: arrayType(structType({ name: "string", age: "int" }), true),
        });
        const rows = [
            {
                amount: "1452.00",
                tags: ["a", null, "c"],
                counts: new Map<string, bigint | null>([["x", 7n], ["y", null]]),
                byId: new Map<number, string | null>([[1, "one"]]),
                addr: { city: "Rosario", zip: 2000 },
                people: [{ name: "ada", age: 36 }, null, { name: "grace", age: null }],
            },
            {
                amount: "-0.5",
                tags: [],
                counts: new Map(),
                byId: null,
                addr: { city: null, zip: null },
                people: null,
            },
        ];
        const buffer = Buffer.from(encodeLocalRelation(rows, Complex.def));
        expect(rowsFromArrowBuffers(buffer ? [buffer] : [], Complex.def)).toEqual([
            {
                amount: "1452.00",
                tags: ["a", null, "c"],
                counts: new Map([["x", 7n], ["y", null]]),
                byId: new Map([[1, "one"]]),
                addr: { city: "Rosario", zip: 2000 },
                people: [{ name: "ada", age: 36 }, null, { name: "grace", age: null }],
            },
            {
                amount: "-0.50",
                tags: [],
                counts: new Map(),
                byId: null,
                addr: { city: null, zip: null },
                people: null,
            },
        ]);
    });

    it("accepts bigint decimals, plain objects for string-keyed maps and missing struct fields", () => {
        const Def = schema({
            amount: decimalType(5, 0),
            labels: mapType("string", "int"),
            addr: structType({ city: "string", zip: "int" }),
        });
        const buffer = Buffer.from(encodeLocalRelation([
            { amount: 42n, labels: { a: 1, b: null }, addr: { city: "Salta" } },
        ], Def.def));
        expect(rowsFromArrowBuffers([buffer], Def.def)).toEqual([
            { amount: "42", labels: new Map([["a", 1], ["b", null]]), addr: { city: "Salta", zip: null } },
        ]);
    });

    it("keeps trailing fractional zeros beyond the scale but rejects other excess digits", () => {
        const Def = schema({ v: decimalType(5, 2) });
        const buffer = Buffer.from(encodeLocalRelation([{ v: "12.500" }, { v: "-0.100000" }], Def.def));
        expect(rowsFromArrowBuffers([buffer], Def.def)).toEqual([{ v: "12.50" }, { v: "-0.10" }]);
        expect(() => encodeLocalRelation([{ v: "12.505" }], Def.def)).toThrow(/at most 2 decimals/);
    });

    it.each([
        [decimalType(10, 2), "12.345", /at most 2 decimals/],
        [decimalType(4, 2), "123.45", /within 4 digits/],
        [decimalType(10, 2), "abc", /DECIMAL\(10,2\) string/],
        [decimalType(10, 2), ".", /DECIMAL\(10,2\) string/],
        [decimalType(10, 2), "", /DECIMAL\(10,2\) string/],
        [decimalType(10, 2), 12.5, /DECIMAL\(10,2\) string/],
        [arrayType("int"), "nope", /an array/],
        [arrayType("int"), new Map(), /received Map\(0\)/],
        [arrayType("int"), [1, "x"], /row 0\[1\]: expected a 32-bit integer/],
        [mapType("int", "string"), { 1: "a" }, /expected a Map,/],
        [mapType("string", "int"), 7, /a Map or a plain object/],
        [mapType("string", "int"), new Map([[null, 1]]), /non-null map key/],
        [mapType("string", "int"), new Map([["k", "v"]]), /row 0\["k"\]: expected a 32-bit integer/],
        [mapType("int", "string"), new Map([["k", "v"]]), /key "k": expected a 32-bit integer/],
        [structType({ a: "int" }), [1], /an object with the struct's fields/],
        [structType({ a: "int" }), { a: "x" }, /row 0\.a: expected a 32-bit integer/],
    ])("rejects a bad complex value: %o", (spec, value, message) => {
        expect(() => encodeLocalRelation([{ v: value }], { v: spec as never })).toThrow(message);
    });

    it("reports a missing non-nullable column as undefined", () => {
        expect(() => encodeLocalRelation([{}], { name: "string" })).toThrow(/received undefined/);
    });

    it("rejects an unknown field spec kind", () => {
        expect(() => encodeLocalRelation([], { v: { kind: "tuple" } as never })).toThrow(/does not support tuple columns/);
    });

    it("rejects a row that is not an object", () => {
        expect(() => encodeLocalRelation([null as never], Events.def)).toThrow(/row 0 is not an object/);
    });

    it("rejects a null in a non-nullable column naming column and row", () => {
        const People = schema({ name: "string" });
        expect(() => encodeLocalRelation([{ name: null }], People.def)).toThrow(
            /"name" row 0: expected a non-null value/,
        );
    });

    it.each([
        ["int", { v: 1.5 }, /32-bit integer/],
        ["int", { v: 2147483648 }, /32-bit integer/],
        ["int", { v: "1" }, /32-bit integer/],
        ["long", { v: "1" }, /bigint/],
        ["long", { v: 1.5 }, /bigint/],
        ["double", { v: "x" }, /a number/],
        ["float", { v: true }, /a number/],
        ["string", { v: 7 }, /a string/],
        ["boolean", { v: "true" }, /a boolean/],
        ["date", { v: "31/01/2026" }, /date string/],
        ["date", { v: "2026-02-30" }, /valid calendar date/],
        ["timestamp", { v: 123 }, /ISO-8601/],
        ["timestamp", { v: "not a time" }, /parseable ISO-8601/],
    ] as const)("rejects a bad %s value", (token, row, message) => {
        const Def = schema({ v: token });
        expect(() => encodeLocalRelation([row as never], Def.def)).toThrow(message);
    });

    it("reports bigint values in errors without JSON.stringify choking", () => {
        const Ints = schema({ v: "int" });
        expect(() => encodeLocalRelation([{ v: 1n as never }], Ints.def)).toThrow(/received 1n/);
    });
});

describe("localRelation plan compilation", () => {
    it("compiles schema-only relations without a data field", () => {
        expect(ProtoDFAlg.localRelation("`a` INT NOT NULL", undefined, 0)).toEqual({
            local_relation: { schema: "`a` INT NOT NULL" },
        });
    });

    it("carries the Arrow bytes when rows exist", () => {
        const data = new Uint8Array([1, 2, 3]);
        expect(ProtoDFAlg.localRelation("`a` INT", data, 3)).toEqual({
            local_relation: { schema: "`a` INT", data },
        });
    });

    it("exposes schema and row count, never bytes, in the client AST", () => {
        expect(SparkDFAlg.localRelation("`a` INT", new Uint8Array([1]), 5)).toEqual({
            type: "LocalRelation",
            schema: "`a` INT",
            rowCount: 5,
        });
        expect(JSON.stringify(TraceDFAlg.localRelation("`a` INT", undefined, 5))).toContain("localRelation[5]");
    });
});

describe("session.createDataFrame", () => {
    it("summarises the Arrow payload in toProtoJSON", () => {
        const proto = JSON.parse(session.createDataFrame(rows, Events).toProtoJSON()) as {
            local_relation: { schema: string; data: string };
        };
        expect(proto.local_relation.schema).toContain("id BIGINT NOT NULL");
        expect(proto.local_relation.data).toMatch(/^<\d+ bytes of Arrow IPC data>$/);
    });

    it("compiles zero rows to a schema-only LocalRelation", () => {
        const proto = JSON.parse(session.createDataFrame([], Events).toProtoJSON()) as {
            local_relation: Record<string, unknown>;
        };
        expect(proto.local_relation.data).toBeUndefined();
        expect(proto.local_relation.schema).toContain("day DATE NOT NULL");
    });

    it("shows row counts in the client plan printers", () => {
        const df = session.createDataFrame(rows, Events);
        expect(df.toClientASTMermaid()).toContain("localRelation[2]");
        expect(JSON.parse(df.toSparkLogicalPlanJSON())).toMatchObject({
            type: "LocalRelation",
            rowCount: 2,
        });
    });

    it("validates its arguments", () => {
        expect(() => session.createDataFrame("rows" as never, Events)).toThrow(/rows must be an array/);
        expect(() => session.createDataFrame([], { def: {} } as never)).toThrow(/schema declared with schema/);
    });
});
