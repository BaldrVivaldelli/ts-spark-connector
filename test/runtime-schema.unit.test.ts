import { describe, expect, it } from "vitest";
import {
    assertRenameMapping,
    assertUniqueColumnNames,
    dropRuntimeSchema,
    joinRuntimeSchema,
    renameRuntimeSchema,
    schemaDefsEqual,
    selectRuntimeSchema,
    statisticsRuntimeSchema,
} from "../src/read/runtimeSchema";
import { structType } from "../src/schema/schema";

// The runtime schema travels next to the phantom `S` so Arrow decoding can
// check what the types promised. These helpers decide what survives each
// relational step; the type-level mirror lives in test/typed/*.type.test.ts.

describe("runtime schema helpers", () => {
    const left = { id: "int", nested: structType({ value: "string" }) } as const;
    const right = { label: "string" } as const;

    it("compares, selects, drops and derives statistics schemas", () => {
        expect(schemaDefsEqual()).toBe(true);
        expect(schemaDefsEqual(left)).toBe(false);
        expect(schemaDefsEqual(left, { ...left })).toBe(true);
        expect(schemaDefsEqual(left, right)).toBe(false);

        expect(selectRuntimeSchema(undefined, ["id"])).toBeUndefined();
        expect(selectRuntimeSchema(left, ["missing"])).toBeUndefined();
        expect(selectRuntimeSchema(left, ["id", "id"])).toBeUndefined();
        expect(selectRuntimeSchema(left, ["id"])).toEqual({ id: "int" });
        expect(dropRuntimeSchema(undefined, ["id"])).toBeUndefined();
        expect(dropRuntimeSchema(left, ["id"])).toEqual({ nested: left.nested });
        expect(statisticsRuntimeSchema(left, ["missing"])).toBeUndefined();
        expect(statisticsRuntimeSchema(left, ["id"])).toEqual({
            summary: "string",
            id: "string?",
        });
    });

    it("applies join nullability per join type and drops the schema on collisions", () => {
        expect(joinRuntimeSchema(undefined, right, "INNER")).toBeUndefined();
        expect(joinRuntimeSchema(left, { id: "int" }, "INNER")).toBeUndefined();
        expect(joinRuntimeSchema(left, right, "LEFT_SEMI")).toBe(left);
        expect(joinRuntimeSchema(left, right, "LEFT_ANTI")).toBe(left);
        expect(joinRuntimeSchema(left, right, "INNER")).toEqual({ ...left, ...right });
        expect(joinRuntimeSchema(left, right, "LEFT")).toMatchObject({ label: "string?" });
        expect(joinRuntimeSchema(left, right, "RIGHT")).toMatchObject({
            id: "int?",
            nested: { ...left.nested, nullable: true },
        });
        expect(joinRuntimeSchema(left, right, "FULL")).toMatchObject({
            id: "int?",
            label: "string?",
        });
        expect(joinRuntimeSchema({ id: "int?" }, right, "RIGHT")).toMatchObject({
            id: "int?",
        });
    });

    it("validates renames and duplicate names", () => {
        expect(() => assertRenameMapping({ "": "id" })).toThrow();
        expect(() => assertRenameMapping({ id: "" })).toThrow();
        expect(() => assertRenameMapping({ id: "same", label: "same" })).toThrow();
        expect(() => assertUniqueColumnNames("select", ["id", "id"])).toThrow();
        expect(renameRuntimeSchema(undefined, { id: "new_id" })).toBeUndefined();
        expect(() => renameRuntimeSchema(left, { missing: "x" })).toThrow(/missing column/);
        expect(() => renameRuntimeSchema(left, { nested: "id" })).toThrow();
        expect(renameRuntimeSchema(left, { id: "new_id" })).toEqual({
            new_id: "int",
            nested: left.nested,
        });
    });
});
