import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
    existsSync,
    copyFileSync,
    mkdirSync,
    mkdtempSync,
    readFileSync,
    readdirSync,
    renameSync,
    rmSync,
    symlinkSync,
    writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const workspace = mkdtempSync(join(tmpdir(), "ts-spark-connector-package-"));
const staleMarker = join(root, "dist", "__stale_package_smoke__.js");

const run = (command, args, options = {}) => {
    const result = spawnSync(command, args, {
        cwd: root,
        stdio: "inherit",
        ...options,
    });
    assert.equal(result.status, 0, result.error?.message ?? `${command} failed`);
};

try {
    const packDir = join(workspace, "pack");
    const nodeModulesDir = join(workspace, "node_modules");
    mkdirSync(packDir, { recursive: true });
    mkdirSync(nodeModulesDir, { recursive: true });
    mkdirSync(dirname(staleMarker), { recursive: true });

    // A clean prepack must remove this file before npm creates the tarball.
    writeFileSync(staleMarker, "throw new Error('stale build output was packaged');\n");
    run(process.platform === "win32" ? "npm.cmd" : "npm", [
        "pack",
        "--silent",
        "--pack-destination",
        packDir,
    ], {
        env: {
            ...process.env,
            npm_config_cache: join(workspace, "npm-cache"),
        },
    });

    const archives = readdirSync(packDir).filter((name) => name.endsWith(".tgz"));
    assert.equal(archives.length, 1, "npm pack must produce exactly one tarball");

    run("tar", ["-xzf", join(packDir, archives[0]), "-C", nodeModulesDir]);

    const extractedPackage = join(nodeModulesDir, "package");
    const installedPackage = join(nodeModulesDir, "ts-spark-connector");
    assert.ok(existsSync(join(extractedPackage, "dist", "index.js")), "tarball is missing dist/index.js");
    assert.ok(existsSync(join(extractedPackage, "dist", "index.d.ts")), "tarball is missing dist/index.d.ts");
    assert.ok(existsSync(join(extractedPackage, "dist", "index.mjs")), "tarball is missing the ES module entry dist/index.mjs");
    assert.ok(existsSync(join(extractedPackage, "dist", "index.d.mts")), "tarball is missing dist/index.d.mts");
    assert.ok(existsSync(join(extractedPackage, "proto", "spark", "connect", "base.proto")), "tarball is missing Spark proto files");
    assert.ok(existsSync(join(extractedPackage, "MIGRATION.md")), "tarball is missing the migration guide");
    assert.ok(existsSync(join(extractedPackage, "docs", "examples", "join.ts")), "tarball is missing executable examples");
    assert.ok(existsSync(join(extractedPackage, "NOTICE")), "tarball is missing NOTICE for vendored Spark protocol files");
    assert.ok(!existsSync(join(extractedPackage, "dist", "__stale_package_smoke__.js")), "prepack did not clean stale dist output");
    assert.ok(!existsSync(join(extractedPackage, "src")), "tarball must not include source files");
    assert.ok(!existsSync(join(extractedPackage, "test")), "tarball must not include tests");
    renameSync(extractedPackage, installedPackage);

    // Expose only declared production dependencies to the extracted package.
    // Symlinks keep this smoke fully offline while still catching imports from
    // undeclared devDependencies.
    const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
    for (const dependency of Object.keys(manifest.dependencies ?? {})) {
        const source = join(root, "node_modules", ...dependency.split("/"));
        const destination = join(nodeModulesDir, ...dependency.split("/"));
        assert.ok(existsSync(source), `production dependency is not installed: ${dependency}`);
        mkdirSync(dirname(destination), { recursive: true });
        symlinkSync(source, destination, process.platform === "win32" ? "junction" : "dir");
    }

    const consumerDir = join(workspace, "consumer");
    mkdirSync(consumerDir, { recursive: true });

    const examplesDir = join(consumerDir, "examples");
    mkdirSync(examplesDir, { recursive: true });
    for (const example of readdirSync(join(root, "docs", "examples")).filter(name => name.endsWith(".ts"))) {
        const source = readFileSync(join(root, "docs", "examples", example), "utf8");
        assert.ok(
            !source.includes('from "../../src"') && !source.includes("from '../../src'"),
            `${example} imports repository source instead of the package`,
        );
        copyFileSync(join(root, "docs", "examples", example), join(examplesDir, example));
    }

    writeFileSync(join(consumerDir, "consumer.cjs"), `
const assert = require("node:assert/strict");
const connector = require("ts-spark-connector");
assert.equal(typeof connector.SparkSession, "function");
assert.equal(typeof connector.col, "function");
assert.equal(typeof connector.SparkSession.builder().getOrCreate().getSessionId(), "string");
const loaded = Object.keys(require.cache);
assert.equal(loaded.some(path => path.includes("apache-arrow")), false, "Arrow loaded during plan-only import");
assert.equal(loaded.some(path => path.includes("@grpc/grpc-js")), false, "gRPC loaded during plan-only import");
assert.equal(loaded.some(path => path.includes("@grpc/proto-loader")), false, "proto-loader loaded during plan-only import");
`);

    // The ES module entry must be a real ES module that hands out the CommonJS
    // build's own objects: same classes for instanceof, same lazy loading.
    writeFileSync(join(consumerDir, "consumer.mjs"), `
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import connector, { SparkSession, col, typeof as typeOf } from "ts-spark-connector";
assert.match(import.meta.resolve("ts-spark-connector"), /\\/dist\\/index\\.mjs$/);
assert.equal(typeof SparkSession, "function");
assert.equal(typeof col, "function");
assert.equal(typeof typeOf, "function");
assert.equal(typeof SparkSession.builder().getOrCreate().getSessionId(), "string");
const require = createRequire(import.meta.url);
const loaded = Object.keys(require.cache);
assert.equal(loaded.some(path => path.includes("apache-arrow")), false, "Arrow loaded during plan-only import");
assert.equal(loaded.some(path => path.includes("@grpc/grpc-js")), false, "gRPC loaded during plan-only import");
assert.equal(loaded.some(path => path.includes("@grpc/proto-loader")), false, "proto-loader loaded during plan-only import");
const commonJs = require("ts-spark-connector");
assert.equal(connector, commonJs, "default export is not the CommonJS module");
assert.equal(SparkSession, commonJs.SparkSession, "ESM and CJS consumers get different classes");
const namespace = await import("ts-spark-connector");
for (const name of Object.keys(commonJs).filter(key => key !== "__esModule")) {
    assert.equal(namespace[name], commonJs[name], \`ES module entry lacks \${name}\`);
}
`);

    writeFileSync(join(consumerDir, "consumer.mts"), `
import connector, { SparkSession, col, typeof as typeOf } from "ts-spark-connector";
const sessionId: string = SparkSession.builder().getOrCreate().getSessionId();
const viaDefault: typeof SparkSession = connector.SparkSession;
void sessionId;
void viaDefault;
void col("id");
void typeOf("id");
`);

    writeFileSync(join(consumerDir, "consumer.cts"), `
import connector = require("ts-spark-connector");
const sessionId: string = connector.SparkSession.builder().getOrCreate().getSessionId();
void sessionId;
void connector.col("id");
`);

    writeFileSync(join(consumerDir, "tsconfig.json"), `${JSON.stringify({
        compilerOptions: {
            module: "NodeNext",
            moduleResolution: "NodeNext",
            target: "ES2020",
            strict: true,
            noEmit: true,
            skipLibCheck: false,
        },
        include: ["consumer.mts", "consumer.cts", "examples/**/*.ts"],
    }, null, 2)}\n`);

    run(process.execPath, [join(consumerDir, "consumer.cjs")], { cwd: consumerDir });
    run(process.execPath, [join(consumerDir, "consumer.mjs")], { cwd: consumerDir });
    run(process.execPath, [
        join(root, "node_modules", "@typescript", "native", "bin", "tsc"),
        "--project",
        join(consumerDir, "tsconfig.json"),
    ], { cwd: consumerDir });

    process.stdout.write("Package smoke passed for CJS, ESM, and TypeScript declarations (ESM and CJS).\n");
} finally {
    rmSync(staleMarker, { force: true });
    rmSync(workspace, { recursive: true, force: true });
}
