import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const directory = join(root, "proto", "spark", "connect");
const sparkVersion = "4.0.4";
const expected = {
    "base.proto": "e523f95a4124fed8129e3f082aae20befe12db10170426880a165b160e7b1996",
    "catalog.proto": "f4e4211e4b1b0905c7b1e1b05bb008e2c0354b1dec971188cb2d76ab654699da",
    "commands.proto": "d934c9fb02911866c38d4e7fe08b280a802e1f7deb2cc4d9524c2f904d4d4c53",
    "common.proto": "cb508096334b83f61e2020d7929e7b357647d67858d29fa7fe2aa8bcb8f5652a",
    "example_plugins.proto": "73c74dc164cc6c78ef7ce362ce2e0e0efa5b02aeaf1dce118e817f06a80b1291",
    "expressions.proto": "2ce3849fda7d48546b1ef5ea2ba30cede6a6348582bbe6ef8b14df57ff25cb9a",
    "ml.proto": "0f5372140417fb4888075c52416ab0ebcce410c895ccc4f92c684f2dd10141b6",
    "ml_common.proto": "237628488246b193c2711d05b2657d640b5e044c4bcffd786dcd2ae56224e8ba",
    "relations.proto": "8a82ffc93dabb34fcef721dd14842e808280926745cc487d320d3f40367caab8",
    "types.proto": "78d41d565a51bbf826cfc427b891e5e84e3b9c80a3876500ffcaeea4204d8256",
};

const actualFiles = readdirSync(directory).filter(name => name.endsWith(".proto")).sort();
assert.deepEqual(actualFiles, Object.keys(expected).sort(),
    `Vendored proto file set differs from Apache Spark v${sparkVersion}`);

for (const [name, digest] of Object.entries(expected)) {
    const actual = createHash("sha256").update(readFileSync(join(directory, name))).digest("hex");
    assert.equal(actual, digest, `${name} differs from Apache Spark v${sparkVersion}`);
}

process.stdout.write(`Spark Connect v${sparkVersion} proto manifest verified.\n`);
