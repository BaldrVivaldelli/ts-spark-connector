# ts-spark-connector

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).
It builds Spark logical plans in TypeScript and executes them against a Spark Connect server.

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
possibly null.

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

The repo includes a Spark 4.0.0 Docker setup under `spark-server/`. It starts a
TLS-enabled endpoint by default and uses the development CA shown in the quick
start.

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

Notes:

- Token and basic auth are sent as gRPC metadata.
- Basic and Bearer credentials are refused on plaintext `sc://` connections by default. Existing local-development setups can opt in explicitly with `.allowInsecureAuth()`, but TLS is recommended anywhere outside a trusted local network.
- TLS uses grpc-js channel credentials.
- Server-auth TLS works with a CA / root certificate via `trustStorePath`.
- Optional PEM client cert/key paths are supported through `certChainPath` and `privateKeyPath`.
- Java-style PKCS#12 keystores (`.p12` / `.pfx`) are supported via `keyStorePath` + `keyStorePassword`. They are loaded through Node's `tls.createSecureContext` and bound to the gRPC channel, so no external dependency or manual PEM conversion is required. The CA (`trustStorePath`) must still be a PEM certificate.
- The checked-in CA and keystore are public development fixtures. Regenerate and validate them with `./spark-server/certs/generate-dev-certs.sh`; never use them in production.

## Examples

Ready-to-run examples live under `docs/examples/`. They read the same
`SPARK_CONNECT_URL`, `SPARK_TLS_CA`, and `SPARK_TLS_SERVER_NAME` variables as
the E2E setup, wait for their work, and close the session. For example, after
starting the bundled server:

```bash
npm run build
node -r ts-node/register docs/examples/join.ts
```

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
- `withRpcTimeout(ms)` sets per-RPC deadlines and `withAbortSignal(signal)` cancels RPCs and retry waits.
- gRPC send and receive limits default to 128 MiB, avoiding grpc-js's small default for normal Arrow batches; advanced callers can override the validated `grpcMaxReceiveMessageBytes` / `grpcMaxSendMessageBytes` connection options before the first RPC.
- Client session ids and operation ids are UUIDs. If the compatibility helper `createSparkSession(id)` is given an explicit id, it must be a canonical UUID.

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

## Compatibility

The currently verified compatibility target is:

- Spark Connect protocol/server 4.0.0 (the pinned Docker E2E target)
- Scala 2.13 artifacts on the server image
- Node.js 18+

Spark 3.5.x and other Spark Connect versions may work, but they are not
currently part of a green compatibility matrix and are therefore not claimed
as supported. Add a version to this list only after its protocol and behavioral
E2E suite passes in CI.

## Current scope

Implemented and exposed in the public API:

- Reads: CSV / JSON / Parquet / ORC / Avro
- Transformations: `select`, `filter`, `join`, `groupBy().agg()`, `sort`, `limit`, `distinct`, `dropDuplicates`, `withColumn`, `withColumnRenamed`, `withColumnsRenamed`, `drop`, `sample`, `randomSplit`, `repartition`, `coalescePartitions`; `coalesce(name, ...)` is the column expression helper
- Set ops: `union`, `unionByName`, `intersect`, `intersectAll`, `except`, `exceptAll`
- SQL / temp views
- Batch writes plus executable streaming writes with `start()`, `toTable()`, `awaitTermination()` and `stop()`
- `explain(...)`

Not exposed as stable public API yet:

- JDBC / Delta / Iceberg / Hudi
- MLlib

## Package contract

The package is CommonJS-first. Both `require()` and Node ESM named imports are
smoke-tested from the packed tarball, together with NodeNext declarations and
all `docs/examples`. Only the root import (`ts-spark-connector`) is a supported
export; deep imports are intentionally blocked by `package.json#exports`.

## License

Apache-2.0
