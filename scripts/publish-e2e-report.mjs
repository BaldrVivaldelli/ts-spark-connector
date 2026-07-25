import assert from "node:assert/strict";
import {
    appendFileSync,
    existsSync,
    mkdirSync,
    readFileSync,
    writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import process from "node:process";

const sparkVersion = process.env.SPARK_VERSION;
assert.ok(sparkVersion, "SPARK_VERSION is required");

const outputDirectory = resolve(process.env.E2E_ARTIFACT_DIR ?? "artifacts/e2e");
const junitPath = resolve(
    process.env.E2E_JUNIT_PATH ?? `${outputDirectory}/spark-${sparkVersion}.xml`,
);
const e2eOutcome = process.env.E2E_TEST_OUTCOME ?? "unknown";
const exampleOutcome = process.env.E2E_EXAMPLE_OUTCOME ?? "unknown";

const attributes = text => Object.fromEntries(
    [...text.matchAll(/([A-Za-z][A-Za-z0-9_-]*)="([^"]*)"/g)]
        .map(match => [match[1], match[2]]),
);

let tests;
if (existsSync(junitPath)) {
    const junit = readFileSync(junitPath, "utf8");
    const suite = junit.match(/<testsuites\b([^>]*)>/);
    if (suite) {
        const values = attributes(suite[1]);
        tests = {
            total: Number(values.tests ?? 0),
            failures: Number(values.failures ?? 0),
            errors: Number(values.errors ?? 0),
            skipped: Number(values.skipped ?? 0),
            durationSeconds: Number(values.time ?? 0),
        };
    }
}

const successful = e2eOutcome === "success"
    && exampleOutcome === "success"
    && tests !== undefined
    && tests.failures === 0
    && tests.errors === 0;
const repository = process.env.GITHUB_REPOSITORY;
const runId = process.env.GITHUB_RUN_ID;
const runUrl = repository && runId
    ? `https://github.com/${repository}/actions/runs/${runId}`
    : undefined;
const report = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    status: successful ? "passed" : "failed",
    sparkVersion,
    nodeVersion: process.version,
    commit: process.env.GITHUB_SHA,
    runUrl,
    outcomes: {
        e2e: e2eOutcome,
        executableExample: exampleOutcome,
    },
    tests,
};

mkdirSync(outputDirectory, { recursive: true });
const stem = `${outputDirectory}/spark-${sparkVersion}`;
const markdown = [
    `## Spark ${sparkVersion} E2E — ${successful ? "PASS" : "FAIL"}`,
    "",
    "| Signal | Result |",
    "| --- | --- |",
    `| E2E suite | ${e2eOutcome} |`,
    `| Executable package example | ${exampleOutcome} |`,
    `| Tests | ${tests?.total ?? "report unavailable"} |`,
    `| Failures / errors | ${tests ? `${tests.failures} / ${tests.errors}` : "report unavailable"} |`,
    `| Duration | ${tests ? `${tests.durationSeconds.toFixed(3)} s` : "report unavailable"} |`,
    `| Node | ${process.version} |`,
    `| Commit | ${process.env.GITHUB_SHA ?? "local"} |`,
    runUrl ? `| Workflow | [Open run](${runUrl}) |` : undefined,
    "",
    `Artifacts: \`spark-${sparkVersion}.xml\`, \`spark-${sparkVersion}.json\` and this report.`,
    "",
].filter(line => line !== undefined).join("\n");

writeFileSync(`${stem}.json`, `${JSON.stringify(report, null, 2)}\n`);
writeFileSync(`${stem}.md`, markdown);
if (process.env.GITHUB_STEP_SUMMARY) {
    mkdirSync(dirname(process.env.GITHUB_STEP_SUMMARY), { recursive: true });
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
}

process.stdout.write(markdown);
