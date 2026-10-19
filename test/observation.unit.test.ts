import { describe, expect, it } from "vitest";
import { Observation, literalValue, observedMetricsOf } from "../src/read/observation";

describe("Observation", () => {
    it("names itself uniquely when no name is given and validates explicit names", () => {
        const anonymous = new Observation();
        expect(anonymous.name).toMatch(/^observation_[0-9a-f-]{36}$/);
        expect(new Observation().name).not.toBe(anonymous.name);
        expect(new Observation("stats").name).toBe("stats");
        expect(() => new Observation("")).toThrow(/non-empty string/);
        expect(() => new Observation("  ")).toThrow(/non-empty string/);
        expect(() => new Observation(5 as never)).toThrow(/non-empty string/);
    });

    it("resolves pending readers when metrics arrive and hands out copies afterwards", async () => {
        const observation = new Observation("stats");
        expect(observation.values).toBeUndefined();
        const pending = observation.get();
        observation.record({ rows: 3n, total: 1.5 });
        await expect(pending).resolves.toEqual({ rows: 3n, total: 1.5 });

        const later = await observation.get();
        later.rows = 0n;
        expect(observation.values).toEqual({ rows: 3n, total: 1.5 });
        const snapshot = observation.values!;
        snapshot.rows = 1n;
        expect(observation.values).toEqual({ rows: 3n, total: 1.5 });

        observation.record({ rows: 4n });
        await expect(observation.get()).resolves.toEqual({ rows: 4n });
    });

    it("tracks overlapping actions and settles pending readers without metrics", async () => {
        const observation = new Observation("stats");
        observation.begin();
        observation.begin();
        const pending = observation.get();
        observation.record({ rows: 1n });
        observation.settle();
        // Still in flight for the second action: readers wait for it.
        const waiting = observation.get();
        observation.settle();
        await expect(pending).resolves.toEqual({ rows: 1n });
        await expect(waiting).resolves.toEqual({ rows: 1n });

        const silent = new Observation("silent");
        silent.begin();
        const unanswered = silent.get();
        silent.settle();
        await expect(unanswered).rejects.toThrow(/without Spark reporting the metrics of observation "silent"/);
        await expect(silent.get()).rejects.toThrow(/without Spark reporting/);

        const failed = new Observation("failed");
        failed.begin();
        failed.settle(new Error("boom"));
        await expect(failed.get()).rejects.toThrow("boom");
    });
});

describe("literalValue", () => {
    it("decodes every scalar kind the way collect() represents it", () => {
        expect(literalValue({ literal_type: "null", null: {} })).toBeNull();
        expect(literalValue({ literal_type: "boolean", boolean: true })).toBe(true);
        expect(literalValue({ literal_type: "byte", byte: 7 })).toBe(7);
        expect(literalValue({ literal_type: "short", short: 8 })).toBe(8);
        expect(literalValue({ literal_type: "integer", integer: 9 })).toBe(9);
        expect(literalValue({ literal_type: "long", long: "9007199254740993" })).toBe(9007199254740993n);
        expect(literalValue({ literal_type: "float", float: 1.5 })).toBe(1.5);
        expect(literalValue({ literal_type: "double", double: 2.5 })).toBe(2.5);
        expect(literalValue({ literal_type: "decimal", decimal: { value: "12.34", precision: 4, scale: 2 } })).toBe("12.34");
        expect(literalValue({ literal_type: "decimal", decimal: {} })).toBe("");
        expect(literalValue({ literal_type: "string", string: "x" })).toBe("x");
        expect(literalValue({ literal_type: "date", date: 19724 })).toBe("2024-01-02");
        expect(literalValue({ literal_type: "timestamp", timestamp: "1704164645000000" })).toBe("2024-01-02T03:04:05.000Z");
        expect(literalValue({ literal_type: "timestamp_ntz", timestamp_ntz: "1704164645000000" })).toBe("2024-01-02T03:04:05.000Z");
        const bytes = new Uint8Array([1, 2]);
        expect(literalValue({ literal_type: "binary", binary: bytes })).toBe(bytes);
    });

    it("decodes arrays and maps recursively and falls back to the raw literal", () => {
        expect(literalValue({
            literal_type: "array",
            array: { elements: [{ literal_type: "integer", integer: 1 }, { literal_type: "null", null: {} }] },
        })).toEqual([1, null]);
        expect(literalValue({ literal_type: "array", array: {} })).toEqual([]);
        expect(literalValue({
            literal_type: "map",
            map: { keys: [{ literal_type: "string", string: "a" }], values: [{ literal_type: "long", long: "2" }] },
        })).toEqual(new Map([["a", 2n]]));
        expect(literalValue({ literal_type: "map", map: {} })).toEqual(new Map());
        const struct = { literal_type: "struct", struct: {} };
        expect(literalValue(struct)).toBe(struct);
        expect(literalValue(undefined)).toEqual({});
    });

    it("detects the set kind when the oneof marker is missing", () => {
        expect(literalValue({ long: "5" })).toBe(5n);
        expect(literalValue({ null: {} })).toBeNull();
        expect(literalValue({ string: "s" })).toBe("s");
        expect(literalValue({})).toEqual({});
    });
});

describe("observedMetricsOf", () => {
    it("pairs keys with decoded values and ignores malformed entries", () => {
        expect(observedMetricsOf(undefined)).toEqual([]);
        expect(observedMetricsOf({ observed_metrics: "nope" })).toEqual([]);
        expect(observedMetricsOf({
            observed_metrics: [
                {
                    name: "stats",
                    keys: ["rows", "total"],
                    values: [{ literal_type: "long", long: "3" }, { literal_type: "double", double: 1.5 }],
                },
                { name: "", keys: [], values: [] },
                null,
                { name: "short", values: [{ literal_type: "integer", integer: 1 }] },
                { name: "novals" },
            ],
        })).toEqual([
            { name: "stats", values: { rows: 3n, total: 1.5 } },
            { name: "short", values: { _1: 1 } },
            { name: "novals", values: {} },
        ]);
        expect(observedMetricsOf({
            observedMetrics: [{ name: "camel", keys: ["k"], values: [{ literal_type: "string", string: "v" }] }],
        })).toEqual([{ name: "camel", values: { k: "v" } }]);
    });
});
