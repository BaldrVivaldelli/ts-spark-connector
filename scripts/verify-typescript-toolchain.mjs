import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);
const executable = name => join(
    root,
    "node_modules",
    ".bin",
    process.platform === "win32" ? `${name}.cmd` : name,
);
const version = name => {
    const result = spawnSync(executable(name), ["--version"], {
        cwd: root,
        encoding: "utf8",
    });
    assert.equal(result.status, 0, result.stderr || result.error?.message);
    return result.stdout.trim();
};
const manifest = name => JSON.parse(readFileSync(require.resolve(`${name}/package.json`), "utf8"));

const nativeVersion = version("tsc");
const nativeManifest = manifest("@typescript/native");
const compatibilityManifest = manifest("typescript");

assert.match(nativeVersion, /^Version 7\./, "tsc must resolve to the TypeScript 7 native compiler");
assert.match(nativeManifest.version, /^7\./, "@typescript/native must resolve to TypeScript 7");
assert.match(compatibilityManifest.version, /^6\./, "typescript must expose the TypeScript 6 API for tooling");
assert.ok(existsSync(executable("tsc6")), "the TypeScript 6 compatibility compiler is missing");

process.stdout.write(
    `TypeScript toolchain verified: ${nativeVersion}; tooling API ${compatibilityManifest.version}.\n`,
);
