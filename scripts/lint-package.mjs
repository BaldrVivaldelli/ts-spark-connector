import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const workspace = mkdtempSync(join(tmpdir(), "ts-spark-connector-package-lint-"));

function run(command, args, options = {}) {
    execFileSync(command, args, {
        cwd: root,
        stdio: "inherit",
        ...options,
    });
}

try {
    run(process.platform === "win32" ? "npm.cmd" : "npm", [
        "pack",
        "--silent",
        "--pack-destination",
        workspace,
    ], {
        env: {
            ...process.env,
            npm_config_cache: join(workspace, "npm-cache"),
        },
    });

    const archives = readdirSync(workspace).filter(name => name.endsWith(".tgz"));
    assert.equal(archives.length, 1, "npm pack must produce exactly one archive");
    const archive = join(workspace, archives[0]);

    run(process.execPath, [
        join(root, "node_modules", "publint", "src", "cli.js"),
        archive,
        "--strict",
    ]);
    run(process.execPath, [
        join(root, "node_modules", "@arethetypeswrong", "cli", "dist", "index.js"),
        archive,
        "--profile",
        "node16",
    ]);
} finally {
    rmSync(workspace, { recursive: true, force: true });
}
