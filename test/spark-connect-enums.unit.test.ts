import { describe, expect, it } from "vitest";
import {
    ExplainMode,
    toProtoExplainMode,
    toProtoGroupType,
    toProtoJoinType,
    toProtoNullsOrder,
    toProtoSetOpType,
    toProtoSortDirection,
} from "../src/engine/sparkConnectEnums";

// Pins the numeric / string enum values the connector puts on the wire; the
// proto descriptor is loaded with `enums: String` for sort fields and numeric
// values elsewhere, so both encodings matter.

describe("Spark Connect enum mapping", () => {
    it("maps join, group and set-operation defaults to their proto values", () => {
        expect(toProtoJoinType("inner")).toBe(1);
        expect(toProtoJoinType("LEFT")).toBe(3);
        expect(() => toProtoJoinType("invalid" as unknown as "inner")).toThrow();
        expect(toProtoGroupType()).toBe(1);
        expect(() => toProtoGroupType("invalid" as unknown as "groupby")).toThrow();
        expect(toProtoSetOpType()).toBe(2);
        expect(toProtoSetOpType("intersect")).toBe(1);
        expect(toProtoSetOpType("except")).toBe(3);
        expect(() => toProtoSetOpType("invalid" as unknown as "union")).toThrow();
    });

    it("emits Spark's default null ordering when none is given", () => {
        expect(toProtoSortDirection("asc")).toBe("SORT_DIRECTION_ASCENDING");
        expect(toProtoSortDirection("desc")).toBe("SORT_DIRECTION_DESCENDING");
        expect(toProtoNullsOrder("nullsFirst", "desc")).toBe("SORT_NULLS_FIRST");
        expect(toProtoNullsOrder("nullsLast", "asc")).toBe("SORT_NULLS_LAST");
        expect(toProtoNullsOrder(undefined, "asc")).toBe("SORT_NULLS_FIRST");
        expect(toProtoNullsOrder(undefined, "desc")).toBe("SORT_NULLS_LAST");
    });

    it("maps every explain mode to its proto enum", () => {
        expect(toProtoExplainMode("simple")).toBe(ExplainMode.EXPLAIN_MODE_SIMPLE);
        expect(toProtoExplainMode("extended")).toBe(ExplainMode.EXPLAIN_MODE_EXTENDED);
        expect(toProtoExplainMode("codegen")).toBe(ExplainMode.EXPLAIN_MODE_CODEGEN);
        expect(toProtoExplainMode("cost")).toBe(ExplainMode.EXPLAIN_MODE_COST);
        expect(toProtoExplainMode("formatted")).toBe(ExplainMode.EXPLAIN_MODE_FORMATTED);
        expect(() => toProtoExplainMode("invalid" as unknown as "simple")).toThrow();
    });
});
