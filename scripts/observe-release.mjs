import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
    appendFileSync,
    mkdirSync,
    mkdtempSync,
    readFileSync,
    rmSync,
    writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const specifier = process.argv[2] ?? "ts-spark-connector@latest";
const outputDirectory = resolve(root, process.argv[3] ?? "artifacts/release");
const workspace = mkdtempSync(join(tmpdir(), "ts-spark-connector-observe-"));
const consumer = join(workspace, "consumer");
const npm = process.platform === "win32" ? "npm.cmd" : "npm";

const run = (command, args, options = {}) => {
    const result = spawnSync(command, args, {
        cwd: root,
        encoding: "utf8",
        ...options,
    });
    if (result.status !== 0) {
        const detail = result.stderr || result.stdout || result.error?.message || `${command} failed`;
        throw new Error(detail.trim());
    }
    return result.stdout;
};

// registry.npmjs.org sits behind a CDN, so a packument read moments after a publish
// can still come from a stale edge cache and 404 on the brand new dist-tag.
const retryableRegistryFailure =
    /E404|No match found|ETIMEDOUT|ECONNRESET|ENOTFOUND|EAI_AGAIN|socket hang up|registry returned 5\d\d/iu;
const registryAttempts = Number(process.env.CANARY_REGISTRY_ATTEMPTS ?? 8);
assert.ok(
    Number.isInteger(registryAttempts) && registryAttempts >= 1,
    "CANARY_REGISTRY_ATTEMPTS must be a positive integer",
);

const sleep = milliseconds => {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
};

const runAgainstRegistry = (command, args, options = {}) => {
    for (let attempt = 1; ; attempt += 1) {
        try {
            return run(command, args, options);
        } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            if (attempt >= registryAttempts || !retryableRegistryFailure.test(detail)) {
                throw error;
            }
            const backoff = Math.min(2 ** attempt * 1_000, 15_000);
            process.stderr.write(
                `Registry read failed (attempt ${attempt}/${registryAttempts}); retrying in ${backoff / 1_000}s\n`,
            );
            sleep(backoff);
        }
    }
};

const tagSeparator = specifier.lastIndexOf("@");
const packageName = tagSeparator > 0 ? specifier.slice(0, tagSeparator) : specifier;

const metadata = JSON.parse(runAgainstRegistry(npm, [
    "view",
    specifier,
    "version",
    "dist.integrity",
    "dist.shasum",
    "gitHead",
    "--json",
]));

// Install the exact version the registry just resolved; re-resolving the dist-tag
// would expose the install to the same stale-cache race.
const pinnedSpecifier = `${packageName}@${metadata.version}`;

const report = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    status: "failed",
    specifier,
    version: metadata.version,
    gitHead: metadata.gitHead,
    integrity: metadata["dist.integrity"],
    shasum: metadata["dist.shasum"],
    nodeVersion: process.version,
    commit: process.env.GITHUB_SHA,
    checks: {
        registryMetadata: true,
        installedVersion: false,
        commonJsImport: false,
        esmImport: false,
        typescript7Declarations: false,
        productionAudit: false,
    },
};

let failure;
try {
    mkdirSync(consumer, { recursive: true });
    runAgainstRegistry(npm, [
        "install",
        "--prefix",
        consumer,
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        pinnedSpecifier,
    ]);

    const installedManifest = JSON.parse(readFileSync(
        join(consumer, "node_modules", "ts-spark-connector", "package.json"),
        "utf8",
    ));
    assert.equal(installedManifest.version, metadata.version);
    report.checks.installedVersion = true;

    writeFileSync(join(consumer, "smoke.cjs"), `
const assert = require("node:assert/strict");
const connector = require("ts-spark-connector");
assert.equal(typeof connector.SparkSession, "function");
assert.equal(typeof connector.col, "function");
`);
    run(process.execPath, [join(consumer, "smoke.cjs")], { cwd: consumer });
    report.checks.commonJsImport = true;

    writeFileSync(join(consumer, "smoke.mjs"), `
import assert from "node:assert/strict";
import { SparkSession, col } from "ts-spark-connector";
assert.equal(typeof SparkSession, "function");
assert.equal(typeof col, "function");
`);
    run(process.execPath, [join(consumer, "smoke.mjs")], { cwd: consumer });
    report.checks.esmImport = true;

    writeFileSync(join(consumer, "consumer.mts"), `
import { SparkSession, col } from "ts-spark-connector";
const sessionId: string = SparkSession.builder().getOrCreate().getSessionId();
void sessionId;
void col("id");
`);
    writeFileSync(join(consumer, "tsconfig.json"), `${JSON.stringify({
        compilerOptions: {
            module: "NodeNext",
            moduleResolution: "NodeNext",
            target: "ES2022",
            strict: true,
            noEmit: true,
            skipLibCheck: false,
        },
        include: ["consumer.mts"],
    }, null, 2)}\n`);
    const compiler = join(
        root,
        "node_modules",
        "@typescript",
        "native",
        "bin",
        "tsc",
    );
    run(compiler, ["--project", join(consumer, "tsconfig.json")], { cwd: consumer });
    report.checks.typescript7Declarations = true;

    const audit = JSON.parse(run(npm, [
        "audit",
        "--prefix",
        consumer,
        "--omit=dev",
        "--json",
    ], { cwd: consumer }));
    const vulnerabilities = audit.metadata?.vulnerabilities?.total ?? 0;
    assert.equal(vulnerabilities, 0, `${metadata.version} has ${vulnerabilities} production vulnerabilities`);
    report.checks.productionAudit = true;
    report.status = "passed";
} catch (error) {
    failure = error;
    report.error = error instanceof Error ? error.message : String(error);
} finally {
    mkdirSync(outputDirectory, { recursive: true });
    const reportPath = join(outputDirectory, "release-observation.json");
    const markdownPath = join(outputDirectory, "release-observation.md");
    const rows = Object.entries(report.checks)
        .map(([name, passed]) => `| ${name} | ${passed ? "PASS" : "FAIL"} |`);
    const runUrl = process.env.GITHUB_REPOSITORY && process.env.GITHUB_RUN_ID
        ? `https://github.com/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`
        : undefined;
    const markdown = [
        `## Release ${report.version} — ${report.status.toUpperCase()}`,
        "",
        `Package: \`${specifier}\``,
        "",
        "| Check | Result |",
        "| --- | --- |",
        ...rows,
        "",
        `Integrity: \`${report.integrity ?? "unavailable"}\``,
        runUrl ? `Workflow: [open run](${runUrl})` : undefined,
        report.error ? `Error: \`${report.error.replaceAll("`", "'")}\`` : undefined,
        "",
    ].filter(line => line !== undefined).join("\n");
    writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
    writeFileSync(markdownPath, markdown);
    if (process.env.GITHUB_STEP_SUMMARY) {
        appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
    }
    rmSync(workspace, { recursive: true, force: true });
}

if (failure) {
    throw failure;
}
process.stdout.write(`Release ${report.version} passed registry, runtime, types and audit checks.\n`);
