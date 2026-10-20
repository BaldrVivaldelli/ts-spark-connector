// Rebuilds test/fixtures/udf/ts-spark-e2e-udf.jar, the Java UDF the session
// E2E suite uploads with addArtifact() and registers with
// udf.registerJavaFunction(). It compiles against a stub of Spark's UDF1
// interface (same binary signature; not packaged) inside a JDK container, so
// no local Java toolchain is needed:
//
//   node scripts/build-udf-fixture.mjs
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const fixture = join(root, "test", "fixtures", "udf");
const jar = join(fixture, "ts-spark-e2e-udf.jar");
const image = "eclipse-temurin:17-jdk";

const work = mkdtempSync(join(tmpdir(), "ts-spark-udf-"));
try {
    mkdirSync(join(work, "classes"));
    // --release 17 matches the server's JRE line; the stub stays out of the jar
    // because only the io/ tree is packaged.
    const script = [
        "set -eu",
        "javac --release 17 -d /work/classes " +
            "/fixture/stub/org/apache/spark/sql/api/java/UDF1.java " +
            "/fixture/src/io/tsspark/e2e/Shout.java",
        "cd /work/classes && jar --create --file /work/udf.jar --no-manifest io",
    ].join(" && ");
    execFileSync("docker", [
        "run", "--rm",
        // Run as the invoking user so the work directory stays deletable.
        "--user", `${process.getuid?.() ?? 0}:${process.getgid?.() ?? 0}`,
        "-v", `${fixture}:/fixture:ro`,
        "-v", `${work}:/work`,
        image, "sh", "-c", script,
    ], { stdio: "inherit" });
    const bytes = readFileSync(join(work, "udf.jar"));
    writeFileSync(jar, bytes);
    process.stdout.write(`Wrote ${jar} (${bytes.length} bytes)\n`);
} finally {
    rmSync(work, { recursive: true, force: true });
}
