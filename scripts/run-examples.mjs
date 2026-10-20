import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import {
    existsSync,
    mkdirSync,
    mkdtempSync,
    readdirSync,
    rmSync,
    symlinkSync,
    writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const examplesDirectory = join(root, "docs", "examples");
const available = readdirSync(examplesDirectory)
    .filter(name => name.endsWith(".ts") && !name.startsWith("_"))
    .map(name => name.slice(0, -3))
    .sort();
const requested = process.argv.slice(2);
const selected = requested.length === 0 ? available : requested;

for (const name of selected) {
    assert.match(name, /^[a-zA-Z][a-zA-Z0-9_-]*$/, `Invalid example name: ${name}`);
    assert.ok(available.includes(name), `Unknown example "${name}". Available: ${available.join(", ")}`);
}

const workspace = mkdtempSync(join(tmpdir(), "ts-spark-connector-examples-"));
const compiler = join(
    root,
    "node_modules",
    ".bin",
    process.platform === "win32" ? "tsc.cmd" : "tsc",
);

// Each example runs once per module format, so a live session is opened through
// both package entry points: CommonJS output loads the "require" entry
// (dist/index.js) and ES module output loads the "import" entry
// (dist/index.mjs), type-checked against dist/index.d.mts.
const formats = [
    { label: "CommonJS", directory: "cjs", compilerOptions: [], packageType: undefined },
    {
        label: "an ES module",
        directory: "esm",
        compilerOptions: ["--module", "ESNext", "--moduleResolution", "Bundler"],
        packageType: "module",
    },
];

try {
    for (const format of formats) {
        const outDir = join(workspace, format.directory);
        const compilation = spawnSync(compiler, [
            "--project",
            join(root, "tsconfig.examples.json"),
            "--noEmit",
            "false",
            "--declaration",
            "false",
            "--declarationMap",
            "false",
            ...format.compilerOptions,
            "--outDir",
            outDir,
        ], {
            cwd: root,
            stdio: "inherit",
        });
        assert.equal(compilation.status, 0, compilation.error?.message ?? `Example compilation failed (${format.label})`);
        if (format.packageType) {
            writeFileSync(join(outDir, "package.json"), `${JSON.stringify({ type: format.packageType })}\n`);
        }
    }

    const nodeModules = join(workspace, "node_modules");
    mkdirSync(nodeModules, { recursive: true });
    const packageLink = join(nodeModules, "ts-spark-connector");
    if (!existsSync(packageLink)) {
        symlinkSync(root, packageLink, process.platform === "win32" ? "junction" : "dir");
    }

    for (const name of selected) {
        for (const format of formats) {
            process.stdout.write(`\nRunning docs/examples/${name}.ts as ${format.label}\n`);
            execFileSync(process.execPath, [join(workspace, format.directory, `${name}.js`)], {
                cwd: root,
                env: process.env,
                stdio: "inherit",
            });
        }
    }
} finally {
    rmSync(workspace, { recursive: true, force: true });
}
