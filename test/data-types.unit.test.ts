import { describe, expect, it } from "vitest";
import { sparkDataTypeString, sparkStructType } from "../src/read/dataTypes";

const kind = (name: string, message: Record<string, unknown> = {}) => ({ kind: name, [name]: message });

describe("Spark DataType simple strings", () => {
    it.each([
        ["null", "void"],
        ["binary", "binary"],
        ["boolean", "boolean"],
        ["byte", "tinyint"],
        ["short", "smallint"],
        ["integer", "int"],
        ["long", "bigint"],
        ["float", "float"],
        ["double", "double"],
        ["date", "date"],
        ["timestamp", "timestamp"],
        ["timestamp_ntz", "timestamp_ntz"],
        ["calendar_interval", "interval"],
        ["variant", "variant"],
    ])("maps %s to %s", (name, expected) => {
        expect(sparkDataTypeString(kind(name))).toBe(expected);
    });

    it("renders parameterised and collated types", () => {
        expect(sparkDataTypeString(kind("decimal", { precision: 12, scale: 3 }))).toBe("decimal(12,3)");
        expect(sparkDataTypeString(kind("decimal"))).toBe("decimal(10,0)");
        expect(sparkDataTypeString(kind("string", { collation: "" }))).toBe("string");
        expect(sparkDataTypeString(kind("string", { collation: "UTF8_BINARY" }))).toBe("string");
        expect(sparkDataTypeString(kind("string", { collation: "UNICODE_CI" }))).toBe("string collate UNICODE_CI");
        expect(sparkDataTypeString(kind("string"))).toBe("string");
        expect(sparkDataTypeString(kind("char", { length: 3 }))).toBe("char(3)");
        expect(sparkDataTypeString(kind("var_char", { length: 9 }))).toBe("varchar(9)");
    });

    it("names interval ranges like Spark", () => {
        expect(sparkDataTypeString(kind("year_month_interval", { start_field: 0, end_field: 1 }))).toBe("interval year to month");
        expect(sparkDataTypeString(kind("year_month_interval", { start_field: 1, end_field: 1 }))).toBe("interval month");
        expect(sparkDataTypeString(kind("year_month_interval"))).toBe("interval year to month");
        expect(sparkDataTypeString(kind("day_time_interval", { start_field: 1, end_field: 2 }))).toBe("interval hour to minute");
        expect(sparkDataTypeString(kind("day_time_interval"))).toBe("interval day to second");
        expect(sparkDataTypeString(kind("day_time_interval", { start_field: 7, end_field: 9 }))).toBe("interval 7 to 9");
    });

    it("nests arrays, maps and structs", () => {
        const struct = kind("struct", {
            fields: [
                { name: "a", data_type: kind("integer"), nullable: false },
                { name: "b", data_type: kind("array", { element_type: kind("string") }), nullable: true },
            ],
        });
        expect(sparkDataTypeString(kind("map", { key_type: kind("string"), value_type: struct })))
            .toBe("map<string,struct<a:int,b:array<string>>>");
        expect(sparkDataTypeString(kind("struct", {}))).toBe("struct<>");
    });

    it("unwraps user-defined and unparsed types", () => {
        expect(sparkDataTypeString(kind("udt", { type: "udt", sql_type: kind("double") }))).toBe("double");
        expect(sparkDataTypeString(kind("udt", { type: "point" }))).toBe("point");
        expect(sparkDataTypeString(kind("unparsed", { data_type_string: "geometry" }))).toBe("geometry");
    });

    it("finds the populated member when the virtual oneof field is absent", () => {
        expect(sparkDataTypeString({ long: {}, integer: null })).toBe("bigint");
        expect(sparkDataTypeString({ array: { element_type: { integer: {} } } })).toBe("array<int>");
    });

    it("rejects malformed or unknown types", () => {
        expect(() => sparkDataTypeString(undefined)).toThrow(/data type is missing/);
        expect(() => sparkDataTypeString({})).toThrow(/no type kind is set/);
        expect(() => sparkDataTypeString({ kind: "time", time: {} })).toThrow(/Unsupported Spark DataType kind: time/);
        expect(() => sparkDataTypeString({ kind: "array" })).toThrow(/array is missing/);
        expect(() => sparkDataTypeString(kind("struct", { fields: [null] }))).toThrow(/struct field is missing/);
    });
});

describe("Spark struct schemas", () => {
    it("decodes top-level fields", () => {
        expect(sparkStructType(kind("struct", {
            fields: [
                { name: "id", data_type: kind("long"), nullable: false },
                { name: "name", data_type: kind("string"), nullable: true },
            ],
        }))).toEqual({
            fields: [
                { name: "id", dataType: "bigint", nullable: false },
                { name: "name", dataType: "string", nullable: true },
            ],
        });
    });

    it("rejects a non-struct schema", () => {
        expect(() => sparkStructType(kind("long"))).toThrow(/Expected a struct schema but Spark returned bigint/);
        expect(() => sparkStructType(null)).toThrow(/schema is missing/);
    });
});
