import { execFileSync } from "node:child_process";
import path from "node:path";
import { describe, expect, it } from "vitest";

// src/functions/generated.ts and its test fixture are generated from
// scripts/functions/catalog.mjs; a catalog edit without regeneration fails here.
describe("generated function library", () => {
    it("is up to date with the catalog", () => {
        const output = execFileSync(
            process.execPath,
            [path.join(__dirname, "..", "scripts", "generate-functions.mjs"), "--check"],
            { encoding: "utf8" },
        );
        expect(output).toMatch(/^Function library is up to date \(\d+ generated functions\)\.$/m);
    });
});
