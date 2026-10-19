import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import * as Root from "../src";
import { ProtoExprAlg } from "../src/engine/compilerRead";
import { col } from "../src/engine/column";
import * as F from "../src/functions";
import * as Typed from "../src/typed/functions";
import { FUNCTION_CATALOG } from "./fixtures/function-catalog";
import { callFunction, library, sampleCall } from "./fixtures/function-samples";

// Every generated function is called through the public `functions` object,
// with only its required arguments and with every argument, and the Spark
// Connect expression it produces is checked against the catalog entry.

describe("generated function library", () => {
    it("exports every catalog entry through `functions`", () => {
        expect(FUNCTION_CATALOG.length).toBeGreaterThan(400);
        for (const fn of FUNCTION_CATALOG) expect(typeof library[fn.name], fn.name).toBe("function");
    });

    it.each(FUNCTION_CATALOG.map(fn => [fn.name, fn] as const))(
        "%s sends its Spark name and arguments in order",
        (_name, fn) => {
            for (const includeOptional of [false, true]) {
                const { values, protos } = sampleCall(fn, includeOptional);
                expect(callFunction(fn, values).build(ProtoExprAlg)).toEqual({
                    unresolved_function: { function_name: fn.sql, arguments: protos },
                });
            }
        },
    );

    it("only uses functions registered in Spark 4.0.4", () => {
        // `SHOW FUNCTIONS` of the oldest supported line; the E2E suite checks
        // the live server of every matrix version the same way.
        const registry = new Set(
            readFileSync(path.join(__dirname, "fixtures", "spark-4.0.4-functions.txt"), "utf8")
                .split("\n")
                .filter(Boolean),
        );
        // `product` is resolved by the Spark Connect planner, not the registry.
        const plannerOnly = new Set(["product"]);
        const missing = FUNCTION_CATALOG
            .map(fn => fn.sql)
            .filter(name => !registry.has(name) && !plannerOnly.has(name));
        expect(missing).toEqual([]);
    });

    it("rejects an omitted optional argument followed by a given one", () => {
        expect(() => F.unix_timestamp(undefined, "yyyy-MM-dd")).toThrow(
            "unix_timestamp(): an optional argument was omitted before one that was given.",
        );
        expect(F.unix_timestamp().build(ProtoExprAlg)).toEqual({
            unresolved_function: { function_name: "unix_timestamp", arguments: [] },
        });
    });

    it("accepts expressions wherever a literal is expected", () => {
        expect(F.lpad("name", col("width"), "*").build(ProtoExprAlg)).toEqual({
            unresolved_function: {
                function_name: "lpad",
                arguments: [
                    { unresolved_attribute: { unparsed_identifier: "name" } },
                    { unresolved_attribute: { unparsed_identifier: "width" } },
                    { literal: { string: "*" } },
                ],
            },
        });
    });

    it("exports `typeof` under its Spark name", () => {
        expect(F.typeof("x").build(ProtoExprAlg)).toHaveProperty("unresolved_function.function_name", "typeof");
        expect(Root.typeof).toBe(F.typeof);
    });

    it("keeps the root's original helpers where a library name collides", () => {
        // Typed, schema-aware functions stay at the root...
        expect(Root.abs).toBe(Typed.abs);
        expect(Root.upper).toBe(Typed.upper);
        // ...while `functions` holds the PySpark-style library version.
        expect(Root.functions.abs).toBe(F.abs);
        expect(Root.functions.abs).not.toBe(Root.abs);
        // Names without a collision are exported at the root as well.
        expect(Root.collect_list).toBe(F.collect_list);
        expect(Root.functions.when).toBe(Root.when);
    });
});
