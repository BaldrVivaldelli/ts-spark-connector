import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import process from "node:process";

const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const packageName = process.env.CANARY_PACKAGE ?? "ts-spark-connector";
const channel = process.env.CANARY_CHANNEL ?? "next";

// This gate also used to require the canary to have existed for a number of hours.
// That measured wall-clock and nothing else: it read no issue, download or
// observation signal, so it could not tell a healthy canary from a broken one. The
// one race it did prevent, a stable publish overtaking a canary publish, is now
// prevented structurally by the single release concurrency group. What remains is
// the check that carries weight: that the stable candidate contains the commit the
// canary was built from, so the observation that just passed describes this code.

const capture = (command, args) => {
    const result = spawnSync(command, args, { encoding: "utf8" });
    if (result.status !== 0) {
        const detail = result.stderr || result.stdout || result.error?.message || `${command} failed`;
        throw new Error(detail.trim());
    }
    return result.stdout;
};

const metadata = JSON.parse(capture(npm, [
    "view",
    `${packageName}@${channel}`,
    "version",
    "gitHead",
    "--json",
]));
const timeline = JSON.parse(capture(npm, ["view", packageName, "time", "--json"]));
const publishedAt = timeline[metadata.version];
assert.ok(publishedAt, `npm did not return a publication time for ${metadata.version}`);
assert.match(metadata.version, /-next\.\d+$/, `${metadata.version} is not a next canary`);
assert.match(metadata.gitHead, /^[0-9a-f]{40}$/i, "canary does not expose a valid gitHead");

const ancestry = spawnSync("git", ["merge-base", "--is-ancestor", metadata.gitHead, "HEAD"], {
    encoding: "utf8",
});
assert.equal(
    ancestry.status,
    0,
    `Canary commit ${metadata.gitHead} is not an ancestor of the stable candidate`,
);

const ageHours = (Date.now() - Date.parse(publishedAt)) / 3_600_000;

const markdown = [
    `## Stable release canary gate — PASS`,
    "",
    `- Canary: \`${metadata.version}\``,
    `- Published: ${publishedAt}`,
    `- Age at release: ${ageHours.toFixed(2)} hours (reported, not enforced)`,
    `- Git lineage: \`${metadata.gitHead}\` is included in the stable candidate`,
    "",
].join("\n");
if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
}
process.stdout.write(markdown);
