# ts-spark-connector

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).
It builds Spark logical plans in TypeScript and executes them against a Spark Connect server.

**Documentation:** [API reference](https://baldrvivaldelli.github.io/ts-spark-connector/) ·
[Architecture and contributor guide](docs/ARCHITECTURE.md) ·
[PySpark parity tracker](docs/PARITY.md) · [Migration guide](MIGRATION.md)

## What is solid today

- Fluent DataFrame-style API in TypeScript
- Client-side plan inspection in JSON / Mermaid / proto JSON
- Spark Connect execution over gRPC
- Batch reads, projections, filters, joins, aggregates, sorting, limits, unions, temp views, and writes
- Structured Streaming reads/writes with query handles, termination timeouts, stop, watermark, triggers, and output modes

## Installation

```bash
npm i ts-spark-connector
```

## Quick start

```ts
import { SparkSession, col } from "ts-spark-connector";

const session = SparkSession.builder()
  .config("spark.connect.url", "scs://localhost:15002")
  .enableTLS({ trustStorePath: "./spark-server/certs/ca.crt" })
  .getOrCreate();

const people = session.read
  .option("delimiter", "\t")
  .option("header", "true")
  .csv("/data/people.tsv");

const purchases = session.read
  .option("delimiter", "\t")
  .option("header", "true")
  .csv("/data/purchases.tsv");

await people
  .join(purchases, col("id").eq(col("user_id")), "left")
  .select("name", "product", "amount")
  .filter(col("amount").gt(100))
  .show();

await session.close();
```

`getOrCreate()` is lazy and creates a new client session; it does not contact
Spark or reuse a process-global active session. Call `close()`/`stop()` when the
session is no longer needed so remote state and the shared gRPC channel can be
released.

## Schema-aware DataFrames

`readWith` sends the same declared schema to Spark and carries it through the
TypeScript API. Spark `BIGINT` is exposed as `bigint`; dates and timestamps as
ISO strings; decimals as exact strings; arrays, maps, and structs as recursive
JavaScript collections. Because Spark DDL does not preserve nested nullability,
array elements, map values, and struct fields are typed conservatively as
possibly null. File sources (CSV, JSON, Parquet, ORC, Avro) always report
their columns as nullable, so a column declared non-null is verified against
the data instead: `collect()` decodes it as long as no row carries a null and
throws a schema mismatch naming the column otherwise.

```ts
import {
  SparkSession, arrayType, decimalType, mapType, schema, structType,
} from "ts-spark-connector";

const Events = schema({
  id: "long",
  occurredAt: "timestamp",
  amount: decimalType(18, 4),
  tags: arrayType("string"),
  attributes: mapType("string", "string?"),
  source: structType({ name: "string", version: "int?" }),
});

const events = session.read.readWith(Events, "parquet", "/data/events");
const rows = await events.select("id", "amount", "tags").collect();
// rows: Array<{ id: bigint; amount: string; tags: Array<string | null> }>
```

## Local data

`createDataFrame` turns local rows into a Spark DataFrame through the Connect
`LocalRelation`: the declared schema types the rows at compile time, drives the
Arrow IPC encoding, and travels to Spark as the relation's schema. Value
representations are exactly the ones `collect()` returns, so
`createDataFrame(await df.collect(), Schema)` round-trips without loss —
`long` stays `bigint` at any magnitude.

```ts
import { SparkSession, col, schema } from "ts-spark-connector";

const People = schema({ id: "long", name: "string", joined: "date?" });

const people = session.createDataFrame(
  [
    { id: 1n, name: "ada", joined: "2026-01-31" },
    { id: 2n, name: "grace", joined: null },
  ],
  People,
);

await people.filter(col("id").gt(1)).show();
```

Rows are validated during encoding: a `null` in a non-nullable column or a
value that does not match its token fails with the column and row in the
message, before anything reaches the network, including nested values inside
arrays, maps and structs (`decimalType`, `arrayType`, `mapType`,
`structType`). An
empty row array produces an empty, correctly-typed relation.

## Column expressions and functions

Column methods follow PySpark: a bare string on the right-hand side of a method
is a literal (`col("country").eq("AR")`), while a bare string passed where a
column is expected is a column name.

```ts
import { col, expr, functions as F } from "ts-spark-connector";

purchases
  .filter(col("amount").between(100, 1000).and(col("product").isin("Monitor", "Chair")))
  .withColumn("total", col("amount").multiply(1.21).cast("decimal(10,2)"))
  .withColumn("first_tag", F.split("tags", ",").getItem(0))  // 0-based, like PySpark
  .withColumn("label", expr("upper(product) || '!'"));       // parsed by Spark
```

SQL text is accepted wherever PySpark accepts it: `df.selectExpr("id", "amount
* 2 AS doubled")`, `df.filter("amount > 100")` and its alias `df.where(...)`.

The `functions` object mirrors `pyspark.sql.functions` (about 430 functions,
named as in PySpark). Arguments that are literals in practice (patterns,
formats, delimiters, sizes) take JS values; every other argument takes a column
or a column name. Higher-order functions take JS callbacks:

```ts
import { Window, col, functions as F } from "ts-spark-connector";

const perUser = purchases
  .withColumn("tags", F.split("tags", ","))
  .withColumn("tags", F.transform("tags", (tag, i) => F.concat_ws(":", i.cast("string"), F.upper(tag))))
  .groupBy("user_id")
  .agg({
    orders: F.count("*"),
    products: F.array_sort(F.collect_list("product")),
    p50: F.percentile_approx("amount", 0.5),
  });

const ranked = purchases.select(
  col("user_id"),
  F.row_number().over(Window.partitionBy("user_id").orderBy(col("amount").desc())).alias("rank"),
  F.sum("amount").over(Window.partitionBy("user_id").orderBy("year").rowsBetween(-Infinity, 0)).alias("running"),
);
```

Every function is also exported from the package root under the same name,
except where the root already exports a helper with that name: `abs`, `concat`,
`length`, `lower`, `round` and `upper` at the root are the schema-aware typed
functions, and `coalesce`, `explode`, `from_json`, `map_keys`, `map_values`,
`posexplode`, `split`, `struct` and `to_json` keep their original signatures.
Use `functions.<name>` for the library version. Schema-aware
`groupBy().agg(a => [...])` adds typed `first`, `last`, `collectList`,
`collectSet`, `stddev`, `variance`, `approxCountDistinct` and `countIf` to the
existing `count`, `countDistinct`, `sum`, `avg`, `min` and `max`.

The library is generated from `scripts/functions/catalog.mjs`
(`npm run functions:generate`); the E2E suite has every Spark server in the
matrix resolve each catalog function with its required and its full argument
list.

## Plan inspection

```ts
const df = purchases
  .select("user_id", "product", "amount")
  .filter(col("amount").gt(100))
  .orderBy(col("user_id").descNullsLast());

console.log(df.toClientASTJSON());
console.log(df.toClientASTMermaid());
console.log(df.toSparkLogicalPlanJSON());
console.log(df.toProtoJSON());
```

## Explain plans

```ts
const explain = await purchases
  .filter(col("amount").gt(100))
  .explain("formatted");

console.log(explain);
```

## Spark Connect server

The repo includes a version-parameterized Spark 4.x Docker setup under
`spark-server/`. It defaults to 4.0.4, builds every version covered by the E2E
matrix (4.0.0, 4.0.4, 4.1.3 and 4.2.0) through `SPARK_VERSION` and
`SPARK_AVRO_SHA256`, starts a TLS-enabled endpoint, and uses the development CA
shown in the quick start.

```bash
docker compose up --build
```

For isolated local debugging, a separate plaintext profile is available:

```bash
docker compose -f docker-compose.yml -f docker-compose.plain.yml up --build
```

Use `sc://localhost:15002` with that profile and never send credentials over
it. See [`spark-server/README.md`](spark-server/README.md) for certificate
renewal and profile details.

## TLS / auth

```ts
const session = SparkSession.builder()
  .withAuth({ type: "token", token: "my-token" })
  .config("spark.connect.url", "scs://spark.example.com:15002")
  .enableTLS({
    trustStorePath: "./path/to/ca.crt"
  })
  .getOrCreate();
```

### Rotating credentials

A static token breaks as soon as it expires. For short-lived credentials
(OIDC/JWT, AWS Cognito or STS, Databricks OAuth) pass a provider instead, and
the connector resolves credentials per RPC:

```ts
const session = SparkSession.builder()
  .config("spark.connect.url", "scs://spark.example.com:15002")
  .enableTLS({ trustStorePath: "./path/to/ca.crt" })
  .withAuth({
    type: "provider",
    async provider() {
      const { token, expiresInSeconds } = await mintJwt();
      return {
        headers: { authorization: `Bearer ${token}` },
        expiresAt: Date.now() + expiresInSeconds * 1000,
      };
    },
  })
  .getOrCreate();
```

The connector caches the result until `expiresAt` minus a refresh window
(`refreshSkewMs`, 30s by default) and collapses concurrent refreshes into one
provider call. The window is also a resilience buffer: if a refresh fails while
the previous credential is still valid, the connector keeps using it, reports a
`spark.auth.refresh_fallback` telemetry warning, and retries on later RPCs —
the failure only surfaces once the credential has truly expired. Omit
`expiresAt` when the provider already caches — for example the AWS SDK; such
results are held for at most one second, to absorb the burst of RPCs a single
query produces, and are never used as a fallback:

```ts
import { fromNodeProviderChain } from "@aws-sdk/credential-providers";

const credentials = fromNodeProviderChain();

const provider = async () => {
  const { accessKeyId, secretAccessKey, sessionToken, expiration } = await credentials();
  return {
    // Your own signing step, e.g. SigV4 over the gRPC request, or an
    // exchange of these credentials for a gateway token.
    headers: await signHeaders({ accessKeyId, secretAccessKey, sessionToken }),
    ...(expiration ? { expiresAt: expiration.getTime() } : {}),
  };
};
```

The connector ships no AWS dependency: a provider is any function returning
headers, so SigV4 signing, IAM role assumption, or a token exchange all stay on
your side.

Rotating a credential does not rebuild the gRPC channel — authentication is
deliberately excluded from the channel cache identity.

### S3 credentials for the job

Channel auth (above) authenticates *you* to the Spark Connect server. For the
*job* to read `s3a://` paths, the server needs AWS credentials as Hadoop
configuration — a different concern with its own helper:

```ts
const session = SparkSession.builder()
  .config("spark.connect.url", "scs://spark.example.com:15002")
  .enableTLS({ trustStorePath: "./path/to/ca.crt" })
  .withS3Credentials({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
    sessionToken: process.env.AWS_SESSION_TOKEN, // optional (STS/AssumeRole)
  })
  .getOrCreate();

const events = session.read.parquet("s3a://my-bucket/events/");
```

The helper writes `spark.hadoop.fs.s3a.access.key` / `secret.key` /
`session.token` and, when a `sessionToken` is present, selects S3A's
`TemporaryAWSCredentialsProvider` — the pairing temporary STS credentials
require. Because these values travel to the server in Config RPCs,
`getOrCreate()` refuses a plaintext channel unless `.allowInsecureAuth()` is
set (same policy as Bearer/Basic). Setting the raw `spark.hadoop.fs.s3a.*`
keys through `.config()` still works and bypasses the guard — that is the
deliberate escape hatch, not the recommended path. Telemetry redacts all
three values by key name.

Notes:

- Token, basic, and provider credentials are sent as gRPC metadata.
- Provider header names are lowercased; values containing CR, LF, or NUL are rejected, and a failing provider surfaces its error to the RPC caller.
- A `spark.connect.header.*` key whose name collides with a header produced by the configured authentication (`token`, `basic`, or `provider`) is rejected as a configuration error; disjoint static headers combine with it.
- `withAuth()` and the legacy `spark.auth.*` config keys are mutually exclusive; configuring both throws.
- Hoist your provider to a single instance: caching, refresh coalescing, and fallback are keyed by the provider's function identity, so a closure recreated per session never hits its cache.
- Basic, Bearer, and provider credentials are refused on plaintext `sc://` connections by default. Existing local-development setups can opt in explicitly with `.allowInsecureAuth()`, but TLS is recommended anywhere outside a trusted local network.
- TLS uses grpc-js channel credentials.
- Server-auth TLS works with a CA / root certificate via `trustStorePath`.
- Optional PEM client cert/key paths are supported through `certChainPath` and `privateKeyPath`.
- Java-style PKCS#12 keystores (`.p12` / `.pfx`) are supported via `keyStorePath` + `keyStorePassword`. They are loaded through Node's `tls.createSecureContext` and bound to the gRPC channel, so no external dependency or manual PEM conversion is required. The CA (`trustStorePath`) must still be a PEM certificate.
- The checked-in CA and keystore are public development fixtures. Regenerate and validate them with `./spark-server/certs/generate-dev-certs.sh`; never use them in production.

## Examples

Ready-to-run examples live under `docs/examples/`. They read the same
`SPARK_CONNECT_URL`, `SPARK_TLS_CA`, and `SPARK_TLS_SERVER_NAME` variables as
the E2E setup, compile with TypeScript 7, wait for their work, and close the
session. After starting the bundled server, run one example or the full set:

```bash
npm run examples:run -- join
npm run examples:run
```

See [`docs/examples/README.md`](docs/examples/README.md) for the complete setup.
Upgrade guidance and the release flow live in [`MIGRATION.md`](MIGRATION.md).

## Connection resiliency

Transient gRPC failures (server not ready, timeouts, backpressure) can be retried
automatically with exponential backoff and jitter. Retries are disabled by default.

```ts
const session = SparkSession.builder()
  .withRetry({
    maxRetries: 3,
    initialBackoffMs: 200,
    maxBackoffMs: 10_000,
    backoffMultiplier: 2,
    onRetry: ({ attempt, delayMs }) => {
      console.warn(`Spark retry ${attempt} in ${delayMs}ms`);
    },
  })
  .getOrCreate();
```

Notes:

- Only transient status codes are retried: `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`.
- Retry counts and delays must be non-negative safe integers; delays are limited to Node's maximum timer duration, the multiplier must be finite and at least `1`, and the initial delay cannot exceed the maximum delay. Invalid configurations throw instead of being silently clamped.
- `explain` and `interrupt` are unary and idempotent, so they are always safe to retry.
- Every `executePlan` has a stable operation UUID and requests reattachable execution. Recovery uses `ReattachExecute` from the last response id and never resends the original read, SQL, DDL, or write plan; `ReleaseExecute` cleans server buffers afterward.
- `withRpcTimeout(ms)` sets per-RPC deadlines and `withAbortSignal(signal)` cancels RPCs and retry waits. The session-wide signal is sticky: once it aborts, every later RPC of that session rejects. To cancel a single query, use the per-execution options below.
- By default each ExecutePlan/ReattachExecute stream gets its own `rpcTimeoutMs` deadline, so a query that streams for longer than that fails. `withExecuteTimeout(ms)` instead bounds the whole execution, across every reattach and retry, while unary RPCs keep `rpcTimeoutMs`; `withExecuteTimeout(Infinity)` keeps short unary deadlines without capping query time.
- Spark closes every reattachable response stream after `spark.connect.execute.reattachable.senderMaxStreamDuration` (two minutes by default), even while a long job has not produced a row yet. The client follows such idle streams for as long as the job runs; only streams that end empty immediately, which indicates a broken server, are limited.
- Cleanup RPCs (`ReleaseExecute`, `ReleaseSession`, `FetchErrorDetails`) never use the caller's signal and are bounded by a 10 s deadline (or `rpcTimeoutMs`, when shorter), so an unreachable server cannot hold an aborted query or `close()` open.
- `close()` always releases local state. When `ReleaseSession` fails, the session is still closed and its channel released; the failure is rethrown so the caller knows the server may keep the session until its idle timeout.
- Provider credentials (`withAuth({ type: "provider" })`) are resolved again for every ReattachExecute and ReleaseExecute, so a query can outlive the token it started with.
- gRPC send and receive limits default to 128 MiB, avoiding grpc-js's small default for normal Arrow batches; advanced callers can override the validated `grpcMaxReceiveMessageBytes` / `grpcMaxSendMessageBytes` connection options before the first RPC.
- Client session ids and operation ids are UUIDs. If the compatibility helper `createSparkSession(id)` is given an explicit id, it must be a canonical UUID.

### Cancelling and tagging queries

Actions accept per-execution options. Aborting the `signal` cancels only that
query: the stream is cancelled and the operation is released, which interrupts
the job on the server, and the session stays usable.

```ts
const controller = new AbortController();
setTimeout(() => controller.abort("too slow"), 30_000);
const rows = await df.collect({ signal: controller.signal });
```

`collect`, `collectRaw`, `toRows`, `toArrowBatches` and `streamRaw` take the
same options. Spark operation tags group executions so another part of the
program can interrupt them, and `operationId` (a canonical UUID) lets the
caller target one execution:

```ts
session.addTag("nightly-etl");              // every operation started from now on
const report = big.collect({ tags: ["report"] });
await session.interruptTag("report");      // resolves to the interrupted operation ids
session.removeTag("nightly-etl");

const id = crypto.randomUUID();
const pending = other.collect({ operationId: id });
await session.interruptOperation(id);
await session.interruptAll();              // everything this session is running
```

Session tags are read when an action starts, so later `addTag`/`removeTag`
calls do not affect executions already in flight. Batch writes carry the
session tags as well. An interrupted execution rejects with a
`SparkConnectError` from Spark.

RPC failures are wrapped as `SparkConnectError` with the operation, gRPC code,
Spark error class and error id when available. Spark 4's binary
`grpc-status-details-bin` trailer is decoded and `FetchErrorDetails` enriches
the error without hiding the original failure if enrichment itself fails.

Structured telemetry is opt-in and recursively redacts authorization, token,
password, secret, credential and private-key fields before invoking the sink:

```ts
const observed = SparkSession.builder()
  .withLogger(event => myLogger.log(event.level, event.name, event.attributes))
  .getOrCreate();
```

Runtime Spark configuration is backed by the Connect `Config` RPC:

```ts
await session.conf.set("spark.sql.shuffle.partitions", 8);
const value = await session.conf.get("spark.sql.shuffle.partitions");
await session.conf.unset("spark.sql.shuffle.partitions");
```

## Catalog metadata

`session.catalog` mirrors `spark.catalog` over the Connect `Catalog` relation.
List operations are ordinary DataFrames — they compose with `filter`, `select`,
`collect` and `show` — and their rows are typed through the same declared-schema
machinery as `readWith`:

```ts
const tables = await session.catalog.listTables().collect();
// tables: Array<{ name: string; isTemporary: boolean; ... }>

await session.catalog.listColumns("people").show();

if (await session.catalog.tableExists("people")) {
  console.log(await session.catalog.currentDatabase());
}

await session.catalog.dropTempView("people_view");
```

Scalar operations (`currentCatalog`, `currentDatabase`, `databaseExists`,
`tableExists`, `functionExists`, `dropTempView`, `dropGlobalTempView`,
`setCurrentCatalog`, `setCurrentDatabase`, `isCached`) execute immediately and
unwrap their single result; `getDatabase`, `getTable` and `getFunction` resolve
to one typed row and reject when the object does not exist. `listFunctions`
lists registered functions. Tables are created from data sources with
`createTable(name, { path, source, schema, description, options })` (external
when `path` is given) or `createExternalTable`, both returning the new table
as a DataFrame; `cacheTable(name, storageLevel?)`, `uncacheTable`, `clearCache`,
`refreshTable`, `refreshByPath` and `recoverPartitions` manage caches and
metadata:

```ts
const people = session.catalog.createTable("people", {
  path: "/data/people.tsv",
  source: "csv",
  schema: schema({ id: "int", name: "string" }),
  options: { header: "true", delimiter: "\t" },
});
await people.show();
await session.catalog.cacheTable("people", "MEMORY_ONLY");
console.log(await session.catalog.isCached("people")); // true
```

Catalog plans print through the same inspection surface as any other
DataFrame (`toProtoJSON`, Mermaid, client AST).

## Sessions, artifacts and Java UDFs

`session.range(end)` / `session.range(start, end, step?, numPartitions?)`
builds Spark's `range` relation, typed as `{ id: bigint }`.
`session.newSession()` returns an independent session with the same
connection settings and user and no server round trip; unlike
`cloneSession()` (Spark 4.1+), nothing is copied: temporary views and runtime
configuration start empty.

Local files are shipped to the server with `addArtifact(path, kind?)` and
`addArtifacts(...uploads)`: a `.jar` joins the session's classpath, other
files need `kind: "file"` or `"archive"`. Uploads stream through the
`AddArtifacts` RPC (small files batched, large ones chunked, every chunk
CRC-checked by the server) and resolve with the server-side names.
`cacheArtifact(bytes)` stores a blob by its SHA-256 (PySpark's
`cache_artifact`) and `isArtifactCached(hash)` checks it; those hashed blobs
are the only artifacts Spark's `ArtifactStatus` reports on, so the raw
`artifactStatus(...names)` answers `false` for jars and files. User-defined functions
have to run on the server, so `session.udf.registerJavaFunction(name,
className, returnType?)` and `registerJavaUDAF` register classes from an
uploaded jar, after which SQL and `call(name, [...])` can use them:

```ts
await session.addArtifact("./build/udfs.jar");
await session.udf.registerJavaFunction("shout", "io.example.Shout", "string");
await session.sql("SELECT shout('hi') AS v").show(); // HI!
```

On Spark 4.2 and later, `session.getStatus(operationIds?)` reports the state
of this session's operations (`RUNNING`, `SUCCEEDED`, `FAILED`, ...); pair it
with `ExecutionOptions.operationId` to follow a query. Older servers reject
it with `UnsupportedServerVersionError` before any RPC.

## Delta Lake and Iceberg

The bundled Spark server bakes in checksum-pinned Delta Lake and Iceberg
runtimes, registers their session extensions, replaces `spark_catalog` with
Delta's catalog, and exposes an Iceberg Hadoop catalog named `iceberg`. Both
formats then work through the ordinary reader/writer surface:

```ts
// Delta: path-based tables, including time travel.
await df.write().format("delta").mode("overwrite").save("/data/dest/events");
const current = await session.read.format("delta").load("/data/dest/events").collect();
const v0 = await session.read
  .format("delta")
  .option("versionAsOf", 0)
  .load("/data/dest/events")
  .collect();

// Iceberg: catalog tables plus metadata tables.
await df.write().format("iceberg").saveAsTable("iceberg.db.events");
const rows = await session.table("iceberg.db.events").collect();
const snapshots = await session.sql("SELECT * FROM iceberg.db.events.snapshots").collect();
```

Delta is exercised by the E2E matrix on every covered Spark version. Iceberg
publishes no Spark 4.2 runtime yet, so its suite runs on the 4.0 and 4.1
servers and skips on 4.2; the 4.2 image carries Delta only. Against your own
Spark Connect server, both formats require the matching jars and catalog
configuration server-side — see `spark-server/Dockerfile` for the exact
pinned artifacts.

## Null handling, statistics and introspection

`df.na` and `df.stat` mirror PySpark; the same methods also exist directly on
the DataFrame (`fillna`, `dropna`, `replace`, `corr`, ...). Null handling keeps
the declared schema, while relations that reshape the data (`crosstab`,
`unpivot`, `toDF`, `transpose`) fall back to the untyped surface.

```ts
const filled = df.na.fill({ age: 0, name: "unknown" });
const complete = df.dropna("any", undefined, ["age"]);
const renamed = df.replace(["n/a", "-"], null, "name");

const r = await df.stat.corr("age", "score");          // number
const [p50, p90] = await df.approxQuantile("age", [0.5, 0.9], 0.01);
const pairs = df.crosstab("country", "year");           // DataFrame
const sample = df.sampleBy("country", { AR: 0.1, US: 0.5 }, 42);

const rows = await df.count();                          // bigint, like every Spark long
const first = await df.first();                        // row or undefined
const last = await df.tail(5);
const fields = await df.schema();                       // { fields: [{ name, dataType, nullable }] }
await df.printSchema();
```

`schema()`, `columns()`, `dtypes()`, `treeString()`, `isLocal()`,
`isStreaming()`, `inputFiles()`, `sameSemantics()` and `semanticHash()` are
answered by Spark's `AnalyzePlan` RPC without running the query.

## Table writes and MERGE INTO

`df.writeTo(table)` is Spark's DataFrameWriterV2, and `df.mergeInto(table,
condition)` builds a `MERGE INTO` command. Qualify the target columns with the
table name and the source columns with an `alias()`:

```ts
await df.writeTo("default.customers").using("delta").partitionedBy("country").create();
await updates.writeTo("default.customers").append();
await updates.writeTo("default.customers").overwrite(col("country").eq("AR"));

await updates
  .alias("s")
  .mergeInto("default.customers", col("customers.id").eq(col("s.id")))
  .whenMatched().updateAll()
  .whenNotMatched().insertAll()
  .whenNotMatchedBySource().delete()
  .merge();
```

Both are verified against the bundled Delta catalog on every E2E matrix
version; see [`docs/examples/merge.ts`](docs/examples/merge.ts).

## Compatibility

The currently verified compatibility targets are:

- Vendored Spark Connect protocol descriptors from 4.2.0, the newest line in
  the matrix. The connector only sends messages an older server understands:
  features that need a newer server are gated on the version it reports and
  fail with `UnsupportedServerVersionError` before any RPC
- Spark Connect servers 4.0.0, 4.0.4, 4.1.3 and 4.2.0 covered by the Docker E2E
  matrix
- Scala 2.13 artifacts on the server image
- Node.js 22 and 24
- TypeScript 7 for compilation; TypeScript 6 API compatibility only for
  tooling that has not migrated to the new compiler API

`session.version()` returns the server's Spark version (fetched once with
AnalyzePlan and cached). Version-gated features:

| Feature | Minimum server |
| --- | --- |
| `session.cloneSession()` — copy the server-side session (temp views, runtime configuration) into an independent new session | 4.1.0 |

```ts
if ((await session.version()).startsWith("4.0")) {
  // cloneSession() would reject with UnsupportedServerVersionError
}
const sandbox = await session.cloneSession();
await sandbox.conf.set("spark.sql.shuffle.partitions", 4); // the original keeps its value
```

Other Spark Connect versions may work, but they are not
currently part of a green compatibility matrix and are therefore not claimed
as supported. Add a version to this list only after its protocol and behavioral
E2E suite passes in CI.

## Current scope

Implemented and exposed in the public API:

- Reads: CSV / JSON / Parquet / ORC / Avro
- Local data: `createDataFrame(rows, schema)` over Arrow `LocalRelation`, including decimal, array, map and struct columns; `session.range()`
- Lakehouse formats against the bundled server: Delta Lake on every matrix version (reads, writes, time travel) and Iceberg on Spark 4.0/4.1 (Hadoop catalog and metadata tables)
- Transformations: `select`, `selectExpr`, `filter`/`where` (expressions or SQL strings), `join`, `crossJoin`, `lateralJoin`, `asOfJoin`, `groupBy().agg()`, `rollup`, `cube`, `groupingSets`, `groupBy().pivot()`, `sort`, `sortWithinPartitions`, `limit`, `offset`, `distinct`, `dropDuplicates`, `dropDuplicatesWithinWatermark`, `withColumn`, `withColumns`, `withColumnRenamed`, `withColumnsRenamed`, `withMetadata`, `toDF`, `to(schema)`, `toJSON`, `alias`, `drop`, `unpivot` / `melt`, `transpose`, `sample`, `randomSplit`, `repartition` (by count or by columns), `repartitionByRange`, `coalescePartitions`; `coalesce(name, ...)` is the column expression helper; `colRegex` and `metadataColumn` build column references
- Expressions: column methods (arithmetic, comparisons, `isin`, `between`, `like`/`rlike`, `cast`/`tryCast`, `getItem`/`getField`, `withField`/`dropFields`, ...), `expr()` SQL fragments, windows (`partitionBy`, `orderBy`, `rowsBetween`, `rangeBetween`) and the PySpark-style `functions` library: aggregate, window, string, date/time, math, array, map, JSON/CSV/XML, variant, URL, hash and higher-order functions with JS lambdas
- Null handling (`df.na`, `fillna`, `dropna`, `replace`) and statistics (`df.stat`: `corr`, `cov`, `crosstab`, `freqItems`, `approxQuantile`, `sampleBy`)
- Actions and introspection: `collect`, `toRows`, `show`, `count`, `first`, `head`, `take`, `tail`, `isEmpty`, `schema`, `columns`, `dtypes`, `printSchema` / `treeString`, `isLocal`, `isStreaming`, `inputFiles`, `sameSemantics`, `semanticHash`, `storageLevel`, `observe` with `Observation`, `checkpoint` / `localCheckpoint`
- Set ops: `union`, `unionByName`, `intersect`, `intersectAll`, `except`, `exceptAll`
- SQL / temp views, including global temp views (`createGlobalTempView`, `createOrReplaceGlobalTempView`)
- Catalog metadata (`session.catalog`): current catalog/database, listing catalogs, databases, tables, columns and functions, `getDatabase`/`getTable`/`getFunction`, existence checks, temp-view drops, `createTable`/`createExternalTable`, cache management (`cacheTable`, `uncacheTable`, `isCached`, `clearCache`) and refreshes (`refreshTable`, `refreshByPath`, `recoverPartitions`)
- Sessions: `newSession()`, `cloneSession()` (4.1+), `version()`, `getStatus()` (4.2+), artifact upload (`addArtifact`, `addArtifacts`, `artifactStatus`) and Java UDF registration (`session.udf`)
- Batch writes plus executable streaming writes with `start()`, `toTable()`, `awaitTermination()` and `stop()`
- Table writes through `writeTo(table)` (DataFrameWriterV2: `create`, `replace`, `createOrReplace`, `append`, `overwrite(condition)`, `overwritePartitions`) and `mergeInto(table, condition)` (`MERGE INTO` with matched / not-matched / not-matched-by-source clauses)
- `explain(...)`

Not exposed as stable public API yet:

- JDBC / Hudi
- MLlib

## Package contract

The package is CommonJS-first. Both `require()` and Node ESM named imports are
smoke-tested from the packed tarball, together with NodeNext declarations and
all `docs/examples`. Only the root import (`ts-spark-connector`) is a supported
export; deep imports are intentionally blocked by `package.json#exports`.

`npm run api:check` compares the `@public` declarations with the committed API
Extractor [report](etc/ts-spark-connector.public.api.md); declarations
documented `@internal` are exported for typing convenience only and are not
part of that contract. `npm run package:lint` validates the
packed tarball with Publint and Are The Types Wrong, and
`npm run test:coverage` enforces the current coverage baseline.

CI publishes a JUnit report, a machine-readable JSON result and a Markdown
summary for every Spark E2E matrix entry. Releases are cut from `main` only:
when a Conventional Commit lands there and the unit (Node.js 22 and 24),
publishable-tarball and Spark E2E jobs are green, semantic-release publishes
the package to npm with provenance and tags the release, and the workflow then
installs that exact version from the registry and re-runs the CommonJS, ESM,
TypeScript 7 declaration and production-audit checks against it. There is no
prerelease channel: `latest` is the only dist-tag that receives releases.

## License

Apache-2.0
