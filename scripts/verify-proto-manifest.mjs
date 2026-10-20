import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const directory = join(root, "proto", "spark", "connect");
const sparkVersion = "4.2.0";
const expected = {
    "base.proto": "86d49472e8c905245b5007f19a3ed9cea740140376b5515c6034fbd2a2f6d31f",
    "catalog.proto": "c0f655f0edc9c83b2229da28233dbf2802bd8d8f91035942e53f22bfdeb94fd5",
    "commands.proto": "923a4fddf21120e8d5f6ed304fbfd7ed8aa5885c7548ed1b909db56e0f12496d",
    "common.proto": "1c111676e247370259273863e2b37247b913f94221c6f1f4a609fe037169e783",
    "example_plugins.proto": "73c74dc164cc6c78ef7ce362ce2e0e0efa5b02aeaf1dce118e817f06a80b1291",
    "expressions.proto": "03a0995ca94aa8c87af64462e6926eade335732e6edf95e2a0625496c39d1eb5",
    "ml.proto": "7d61c9098d25dd8b1c3484222d8298e6891a02366c59f454e24dd90993422a13",
    "ml_common.proto": "237628488246b193c2711d05b2657d640b5e044c4bcffd786dcd2ae56224e8ba",
    "pipelines.proto": "6624415fc3dd80834eb7fa14e6427f5af0ec41ea80280380a0645df13190f98e",
    "relations.proto": "cf6b233f07d9fb4c1b708ac74260b3885b2cbbed1ee5a6b139e58423fd44c60d",
    "types.proto": "192e4c1af57e81247e946813f37c886af0ed201d9305e2350e9a228030ea0b8b",
};

const actualFiles = readdirSync(directory).filter(name => name.endsWith(".proto")).sort();
assert.deepEqual(actualFiles, Object.keys(expected).sort(),
    `Vendored proto file set differs from Apache Spark v${sparkVersion}`);

for (const [name, digest] of Object.entries(expected)) {
    const actual = createHash("sha256").update(readFileSync(join(directory, name))).digest("hex");
    assert.equal(actual, digest, `${name} differs from Apache Spark v${sparkVersion}`);
}

process.stdout.write(`Spark Connect v${sparkVersion} proto manifest verified.\n`);
