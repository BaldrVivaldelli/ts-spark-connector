// The changelog preset and semantic-release's writer are separate packages with
// no enforced version relationship, so an incompatible pair installs cleanly and
// passes every test. It only fails inside generateNotes, which runs exclusively
// on a commit that actually releases — meaning the break reaches main, and the
// publish is what discovers it. This renders the preset up front instead.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { generateNotes } from "@semantic-release/release-notes-generator";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageJson = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
const repositoryUrl = packageJson.repository?.url ?? "https://example.invalid/pkg.git";

const generatorConfig = (packageJson.release?.plugins ?? [])
    .filter(plugin => Array.isArray(plugin) && plugin[0] === "@semantic-release/release-notes-generator")
    .map(plugin => plugin[1])[0];
assert.ok(generatorConfig?.preset, "release config does not configure a release-notes-generator preset");

// Exercises the commit types the release rules can emit, including the breaking
// footer whose template helper is the one that actually goes missing.
const commits = [
    {
        hash: "0".repeat(40),
        message: "feat(scope)!: rename an exported option\n\nBREAKING CHANGE: the old option name is gone.",
    },
    { hash: "1".repeat(40), message: "feat(scope): add an option" },
    { hash: "2".repeat(40), message: "fix(scope): stop dropping a value" },
    { hash: "3".repeat(40), message: "perf(scope): avoid a redundant pass" },
];

const notes = await generateNotes(generatorConfig, {
    cwd: process.cwd(),
    options: { repositoryUrl },
    lastRelease: { gitTag: "v0.0.0", version: "0.0.0" },
    nextRelease: { gitTag: "v1.0.0", version: "1.0.0", type: "major" },
    commits,
    logger: { log: () => {} },
});

assert.ok(notes?.trim(), "the preset rendered empty release notes");
assert.match(notes, /BREAKING CHANGES/, "the preset did not render the breaking-changes section");
assert.match(notes, /the old option name is gone/, "the preset dropped the breaking-change description");
assert.match(notes, /add an option/, "the preset dropped a feature entry");
assert.match(notes, /stop dropping a value/, "the preset dropped a fix entry");

process.stdout.write(
    `Changelog preset "${generatorConfig.preset}" renders release notes (${notes.trim().split("\n").length} lines).\n`,
);
