// Scans for secrets with gitleaks, pinned to one version for the pre-commit
// hook and for CI so both apply the same rules (.gitleaks.toml).
//
//   node scripts/gitleaks.mjs staged       what `git commit` is about to record
//   node scripts/gitleaks.mjs history      every commit reachable from HEAD
//   node scripts/gitleaks.mjs dir <path>   files on disk, e.g. an unpacked tarball
//
// It runs a gitleaks binary from PATH when there is one, and otherwise the
// official image, pinned by digest, through Docker. With neither it fails: a
// scan that silently does not run would let a secret through. Findings are
// always redacted, so the scan itself never prints a secret.
import { spawnSync } from "node:child_process";
import { dirname, relative, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const GITLEAKS_VERSION = "8.30.1";
const GITLEAKS_IMAGE =
    "ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f";
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function usage() {
    process.stderr.write("Usage: node scripts/gitleaks.mjs staged | history | dir <path>\n");
    process.exit(2);
}

function scanArguments(mode, target) {
    const common = ["--config", ".gitleaks.toml", "--redact", "--no-banner", "--exit-code", "1"];
    if (mode === "staged") return ["git", "--pre-commit", "--staged", ...common, "."];
    if (mode === "history") return ["git", ...common, "."];
    if (mode === "dir" && target) return ["dir", ...common, target];
    return usage();
}

function available(command, args) {
    const probe = spawnSync(command, args, { stdio: "ignore" });
    return probe.error === undefined && probe.status === 0;
}

const [mode, target] = process.argv.slice(2);
const args = scanArguments(mode, target);
let result;
if (available("gitleaks", ["version"])) {
    result = spawnSync("gitleaks", args, { cwd: root, stdio: "inherit" });
} else if (available("docker", ["version", "--format", "{{.Server.Version}}"])) {
    // The repository is mounted at /repo; a scanned directory must live inside it.
    const dockerArgs = mode === "dir"
        ? args.map(arg => (arg === target ? `/repo/${relative(root, resolve(target))}` : arg))
        : args;
    const user = typeof process.getuid === "function" ? ["--user", `${process.getuid()}:${process.getgid()}`] : [];
    result = spawnSync("docker", [
        "run", "--rm", ...user, "-e", "HOME=/tmp",
        "-v", `${root}:/repo`, "-w", "/repo",
        GITLEAKS_IMAGE, ...dockerArgs,
    ], { stdio: "inherit" });
} else {
    process.stderr.write(
        `Secret scan blocked: install gitleaks ${GITLEAKS_VERSION} (https://github.com/gitleaks/gitleaks#installing) ` +
        "or start Docker, then try again. See CONTRIBUTING.md, Keeping secrets out of the repository.\n"
    );
    process.exit(1);
}

if (result.error) {
    process.stderr.write(`Secret scan could not run: ${result.error.message}\n`);
    process.exit(1);
}
if (result.status !== 0) {
    process.stderr.write(
        "Secret scan failed. Remove the secret (and rotate it if it was real), or, for a public test " +
        "fixture, exempt that exact value in .gitleaks.toml. Do not bypass the check with --no-verify.\n"
    );
}
process.exit(result.status ?? 1);
