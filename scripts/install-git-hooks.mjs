// Points this clone's git hooks at .githooks/, so the pre-commit secret scan
// runs for everyone who installs dependencies. Called from npm's prepare
// script; it never fails an install: outside a git checkout (a packed tarball,
// a dependency install) there is nothing to set up.
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const git = args => spawnSync("git", args, { cwd: root, encoding: "utf8" });

const inside = git(["rev-parse", "--show-toplevel"]);
if (inside.status !== 0 || resolve(inside.stdout.trim()) !== root || !existsSync(join(root, ".githooks"))) {
    process.exit(0);
}
if (git(["config", "--local", "--get", "core.hooksPath"]).stdout.trim() === ".githooks") {
    process.exit(0);
}
const set = git(["config", "--local", "core.hooksPath", ".githooks"]);
if (set.status === 0) {
    process.stdout.write("Git hooks enabled: commits are scanned for secrets (.githooks/pre-commit).\n");
} else {
    process.stderr.write(`Could not enable the git hooks; run: git config core.hooksPath .githooks\n${set.stderr}\n`);
}
