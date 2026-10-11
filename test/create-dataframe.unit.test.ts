import { describe, expect, it } from "vitest";
import { ProtoDFAlg } from "../src/engine/compilerRead";
import { SparkDFAlg } from "../src/read/readDataFrameInterpreter";
import { TraceDFAlg } from "../src/trace/trace";
import { encodeLocalRelation } from "../src/typed/arrow-encode";
import { rowsFromArrowBuffers } from "../src/typed/arrow-rows";
import { SparkSession, arrayType, schema } from "../src";

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

    it("rejects complex column specs with the column name", () => {
        expect(() => encodeLocalRelation([], { tags: arrayType("string") })).toThrow(
            /does not support array columns yet.*"tags"/,
        );
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
