#!/usr/bin/env node
// Maintainer helper: keep the npm granular access token in the OS keyring
// (the same Secret Service that `gh` uses) and run npm with it on demand, so
// no token lives in ~/.npmrc, in .env files or in the shell history.
//
//   node scripts/npm-auth.mjs login [--expires YYYY-MM-DD]
//   node scripts/npm-auth.mjs status
//   node scripts/npm-auth.mjs run <npm arguments...>
//   node scripts/npm-auth.mjs ci-secret [--repo owner/name]
//   node scripts/npm-auth.mjs logout
//
// Tokens are created on npmjs.com (there is no API for it): Access Tokens →
// Generate New Token → Granular Access Token, scoped to the packages the task
// needs, read-only unless publishing, expiring in 90 days at most.

import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";

const REGISTRY = "registry.npmjs.org";
const TOKEN_URL = "https://www.npmjs.com/settings/~/tokens/granular-access-tokens/new";
// Overridable so the test suite can use a throwaway keyring entry.
const SERVICE = process.env.NPM_AUTH_SERVICE ?? "npm";
const LOOKUP = ["service", SERVICE, "registry", REGISTRY];

function fail(message) {
    process.stderr.write(`npm-auth: ${message}\n`);
    process.exit(1);
}

function run(command, args, options = {}) {
    const result = spawnSync(command, args, { encoding: "utf8", ...options });
    if (result.error?.code === "ENOENT") fail(`${command} is not installed.`);
    return result;
}

function requireSecretTool() {
    const probe = run("secret-tool", ["--help"]);
    if (probe.status !== 0 && probe.error) {
        fail("secret-tool (libsecret) is required; it stores the token in the keyring gh already uses.");
    }
}

function readToken() {
    const result = run("secret-tool", ["lookup", ...LOOKUP]);
    const token = result.stdout.trim();
    return result.status === 0 && token ? token : undefined;
}

function readAttribute(name) {
    // secret-tool search prints "attribute.<name> = <value>" lines.
    const result = run("secret-tool", ["search", ...LOOKUP]);
    const match = new RegExp(`^attribute\\.${name} = (.*)$`, "m").exec(result.stdout);
    return match?.[1]?.trim() || undefined;
}

/** Runs npm with the token in a private, temporary user config. */
function npmWithToken(token, args, options = {}) {
    const dir = mkdtempSync(join(tmpdir(), "npm-auth-"));
    const userconfig = join(dir, "npmrc");
    try {
        writeFileSync(userconfig, `//${REGISTRY}/:_authToken=${token}\n`, { mode: 0o600 });
        return run("npm", args, {
            ...options,
            env: { ...process.env, NPM_CONFIG_USERCONFIG: userconfig },
        });
    } finally {
        rmSync(dir, { recursive: true, force: true });
    }
}

function whoami(token) {
    const result = npmWithToken(token, ["whoami", "--registry", `https://${REGISTRY}`]);
    return result.status === 0 ? result.stdout.trim() : undefined;
}

function daysUntil(date) {
    return Math.ceil((new Date(`${date}T00:00:00Z`).getTime() - Date.now()) / 86_400_000);
}

function readIfPresent(file) {
    try {
        return readFileSync(file, "utf8");
    } catch {
        return undefined;
    }
}

function warnAboutStrayTokens() {
    const npmrc = join(homedir(), ".npmrc");
    const text = readIfPresent(npmrc);
    if (text !== undefined && /_authToken\s*=/.test(text)) {
        process.stderr.write(
            `npm-auth: ${npmrc} still contains an _authToken line; npm would use it before the keyring. ` +
            "Delete that line so only this helper authenticates.\n",
        );
    }
}

function option(args, name) {
    const index = args.indexOf(name);
    if (index === -1) return undefined;
    const value = args[index + 1];
    if (!value) fail(`${name} needs a value.`);
    args.splice(index, 2);
    return value;
}

function login(args) {
    requireSecretTool();
    const expires = option(args, "--expires");
    if (expires !== undefined) {
        if (!/^\d{4}-\d{2}-\d{2}$/.test(expires) || Number.isNaN(Date.parse(expires))) {
            fail("--expires takes a date as YYYY-MM-DD.");
        }
        if (daysUntil(expires) > 90) fail("npm caps granular tokens at 90 days; pick an earlier date.");
    }
    process.stdout.write(
        "Create a granular access token at\n" +
        `  ${TOKEN_URL}\n` +
        "scoped to the packages this task needs (read-only unless you publish),\n" +
        "then paste it below. The prompt does not echo.\n",
    );
    const store = run("secret-tool", [
        "store", `--label=npm granular token (${REGISTRY})`,
        ...LOOKUP,
        ...(expires ? ["expires", expires] : []),
    ], { stdio: "inherit" });
    if (store.status !== 0) fail("the token was not stored.");

    const token = readToken();
    if (!token) fail("the keyring returned no token after storing it.");
    const user = whoami(token);
    if (!user) {
        run("secret-tool", ["clear", ...LOOKUP]);
        fail("npm rejected that token (whoami failed); it was removed from the keyring.");
    }
    process.stdout.write(`Stored. npm sees you as ${user}${expires ? `; the token expires ${expires}` : ""}.\n`);
    warnAboutStrayTokens();
}

function status() {
    requireSecretTool();
    warnAboutStrayTokens();
    const token = readToken();
    if (!token) {
        process.stdout.write("No npm token in the keyring. Run: node scripts/npm-auth.mjs login\n");
        return;
    }
    const user = whoami(token);
    const expires = readAttribute("expires");
    const expiry = expires === undefined
        ? "expiry not recorded"
        : daysUntil(expires) < 0
            ? `expired ${expires}`
            : `expires ${expires} (${daysUntil(expires)} days)`;
    process.stdout.write(user
        ? `Token in keyring: valid, npm user ${user}, ${expiry}.\n`
        : `Token in keyring: npm rejects it (${expiry}). Run login again.\n`);
    if (!user) process.exit(1);
}

function runNpm(args) {
    requireSecretTool();
    if (args.length === 0) fail("run needs npm arguments, e.g. run dist-tag ls ts-spark-connector");
    const token = readToken();
    if (!token) fail("no token in the keyring; run login first.");
    const result = npmWithToken(token, args, { stdio: "inherit", encoding: undefined });
    process.exit(result.status ?? 1);
}

function ciSecret(args) {
    requireSecretTool();
    const repo = option(args, "--repo");
    const token = readToken();
    if (!token) fail("no token in the keyring; run login first.");
    if (!whoami(token)) fail("npm rejects the stored token; run login again before updating CI.");
    // gh reads the secret from stdin, so the value never appears in a process list.
    const result = run("gh", ["secret", "set", "NPM_TOKEN", ...(repo ? ["--repo", repo] : [])], {
        input: token,
        stdio: ["pipe", "inherit", "inherit"],
    });
    if (result.status !== 0) fail("gh secret set failed.");
    process.stdout.write("GitHub Actions secret NPM_TOKEN updated from the keyring token.\n");
}

function logout() {
    requireSecretTool();
    if (!readToken()) {
        process.stdout.write("No npm token in the keyring; nothing to remove.\n");
        return;
    }
    const result = run("secret-tool", ["clear", ...LOOKUP]);
    if (result.status !== 0) fail("secret-tool clear failed.");
    process.stdout.write("Token removed from the keyring. Revoke it on npmjs.com as well if it is no longer needed.\n");
}

const [command, ...rest] = process.argv.slice(2);
switch (command) {
    case "login": login(rest); break;
    case "status": status(); break;
    case "run": runNpm(rest); break;
    case "ci-secret": ciSecret(rest); break;
    case "logout": logout(); break;
    default:
        process.stdout.write(
            "usage: node scripts/npm-auth.mjs <login [--expires YYYY-MM-DD] | status | run <npm args...> | ci-secret [--repo owner/name] | logout>\n",
        );
        process.exit(command === undefined || command === "--help" || command === "-h" ? 0 : 1);
}
