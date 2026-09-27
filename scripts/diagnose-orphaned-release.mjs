import { spawnSync } from "node:child_process";
import { appendFileSync, readFileSync } from "node:fs";
import process from "node:process";

// semantic-release creates and pushes the git tag before it runs the publish
// plugins, so a publish that fails leaves a tag behind for a version that never
// reached the registry. It reads tags to decide what is already released, so it
// then skips that version for good: this is how v1.12.0 ended up as a tag and a
// changelog entry with no package on npm.
//
// This only reports. It never deletes, because the registry answers 404 for a
// version it has accepted but not finished processing -- 2.0.0 looked absent for
// three minutes -- and deleting on that signal would destroy a good release.

const logPath = process.argv[2] ?? "semantic-release.log";
const packageName = process.env.RELEASE_PACKAGE ?? "ts-spark-connector";
const attempts = Number(process.env.REGISTRY_SETTLE_ATTEMPTS ?? 15);
const delayMs = Number(process.env.REGISTRY_SETTLE_DELAY_MS ?? 20_000);
const npm = process.platform === "win32" ? "npm.cmd" : "npm";

const sleep = milliseconds => {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
};

const capture = (command, args) => spawnSync(command, args, { encoding: "utf8" });

let log = "";
try {
    // Matching the escape character is the whole point: the log is ANSI-coloured.
    // eslint-disable-next-line no-control-regex
    log = readFileSync(logPath, "utf8").replaceAll(/\u001b\[[0-9;]*m/gu, "");
} catch {
    // semantic-release can fail before the log exists.
}

const version = log.match(/Created tag v(\S+)/u)?.[1]
    ?? log.match(/The next release version is (\S+)/u)?.[1];

const lines = [];
const say = line => lines.push(line);
let annotation;

if (!version) {
    say("## Orphaned release check — nothing to undo");
    say("");
    say("semantic-release failed before it settled on a version, so it created no tag.");
} else {
    const tag = `v${version}`;
    const remote = capture("git", ["ls-remote", "--tags", "origin", `refs/tags/${tag}`]);
    const tagOnRemote = remote.status === 0 && remote.stdout.trim() !== "";

    let onRegistry = false;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
        const view = capture(npm, ["view", `${packageName}@${version}`, "version"]);
        if (view.status === 0 && view.stdout.trim() !== "") {
            onRegistry = true;
            break;
        }
        if (attempt < attempts) {
            process.stderr.write(
                `${packageName}@${version} is not on the registry yet `
                + `(${attempt}/${attempts}); waiting ${delayMs / 1_000}s for it to settle\n`,
            );
            sleep(delayMs);
        }
    }
    const waited = (((attempts - 1) * delayMs) / 60_000).toFixed(1);

    if (tagOnRemote && onRegistry) {
        say(`## Orphaned release check — ${version} is published`);
        say("");
        say(`\`${packageName}@${version}\` is on the registry and \`${tag}\` exists, so the publish`);
        say("landed and whatever failed came after it. **Do not delete the tag** — the release is real.");
    } else if (tagOnRemote) {
        annotation = `${tag} was pushed but ${packageName}@${version} never reached the registry. `
            + `semantic-release will skip ${version} until the tag is deleted.`;
        say(`## Orphaned release check — ${tag} is orphaned`);
        say("");
        say(`\`${tag}\` is on the remote but \`${packageName}@${version}\` never appeared on the`);
        say(`registry, checked over ${waited} minutes. semantic-release reads tags to decide what is`);
        say(`already released, so it will skip ${version} on every later run until the tag is gone.`);
        say("");
        say("To let the pipeline retry this version:");
        say("");
        say("```sh");
        say(`git push origin :refs/tags/${tag}`);
        say(`gh release delete ${tag} --yes`);
        say("```");
        say("");
        const release = capture("gh", ["release", "view", tag, "--json", "tagName"]);
        say(release.status === 0
            ? `A GitHub release for \`${tag}\` exists, so delete that too.`
            : `A GitHub release for \`${tag}\` could not be confirmed; run the delete anyway,`
              + " it is a no-op when there is nothing to remove.");
    } else if (onRegistry) {
        annotation = `${packageName}@${version} is published but ${tag} is missing from the remote.`;
        say(`## Orphaned release check — ${version} is published without its tag`);
        say("");
        say(`\`${packageName}@${version}\` is on the registry but \`${tag}\` is not on the remote, so`);
        say(`semantic-release still treats ${version} as unreleased. Push the tag to correct that.`);
    } else {
        say("## Orphaned release check — nothing to undo");
        say("");
        say(`Neither \`${tag}\` nor \`${packageName}@${version}\` exists, so the run failed before it`);
        say("changed anything outside this job.");
    }
}

const summary = `${lines.join("\n")}\n`;
process.stdout.write(summary);
if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
}
if (annotation) {
    process.stdout.write(`::error title=Release left something behind::${annotation}\n`);
}
