import { describe, expect, it } from "vitest";
import {
    arrayType,
    decimalType,
    mapType,
    schema,
    structType,
    type FieldSpec,
} from "../src/schema/schema";

describe("complex schema descriptors", () => {
    it("records nullability only when requested", () => {
        expect(decimalType(10, 2, true)).toMatchObject({ nullable: true });
        expect(decimalType(10, 2)).not.toHaveProperty("nullable");
        expect(arrayType("int", true)).toMatchObject({ nullable: true });
        expect(arrayType("int")).not.toHaveProperty("nullable");
        expect(mapType("string", "int", true)).toMatchObject({ nullable: true });
        expect(mapType("string", "int")).not.toHaveProperty("nullable");
        expect(structType({ id: "int" }, true)).toMatchObject({ nullable: true });
        expect(structType({ id: "int" })).not.toHaveProperty("nullable");
    });

    it("refuses to render DDL for an unknown descriptor kind", () => {
        const invalid = schema({
            broken: { kind: "unsupported" } as unknown as FieldSpec,
        });
        expect(() => invalid.toDDL()).toThrow(/Unsupported Spark schema descriptor/);
    });
});
