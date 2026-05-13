# ts-spark-connector

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).
It builds Spark logical plans in TypeScript and executes them against a Spark Connect server.

## What is solid today

- Fluent DataFrame-style API in TypeScript
- Client-side plan inspection in JSON / Mermaid / proto JSON
- Spark Connect execution over gRPC
- Batch reads, projections, filters, joins, aggregates, sorting, limits, unions, temp views, and writes
- Structured Streaming read/write scaffolding, including watermark / trigger / output mode serialization

## Installation

```bash
npm i ts-spark-connector
```

## Quick start

```ts
import { SparkSession, col } from "ts-spark-connector";

const session = SparkSession.builder().getOrCreate();

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

The repo includes a local Docker setup under `spark-server/`.

```bash
docker compose up --build
```

See [`spark-server/README.md`](spark-server/README.md) for details.

## TLS / auth

```ts
const session = SparkSession.builder()
  .withAuth({ type: "token", token: "my-token" })
  .enableTLS({
    trustStorePath: "./spark-server/certs/cert.crt"
  })
  .getOrCreate();
```

Notes:

- Token and basic auth are sent as gRPC metadata.
- TLS uses grpc-js channel credentials.
- Server-auth TLS works with a CA / root certificate via `trustStorePath`.
- Optional PEM client cert/key paths are supported through `certChainPath` and `privateKeyPath`.
- Java-style PKCS#12 keystores are preserved in config, but grpc-js does not consume `.p12` directly.

## Examples

Ready-to-run examples live under `docs/examples/`.

## Compatibility

This repo’s bundled Docker server uses:

- Spark 4.0.0
- Scala 2.13 artifacts on the server image
- Node.js 18+

Other Spark Connect versions may work, but the repository setup is aligned with the Docker image above.

## Current scope

Implemented and exposed in the public API:

- Reads: CSV / JSON / Parquet / ORC / Avro
- Transformations: `select`, `filter`, `join`, `groupBy().agg()`, `sort`, `limit`, `distinct`, `dropDuplicates`, `withColumn`, `withColumnRenamed`, `withColumnsRenamed`, `drop`, `sample`, `randomSplit`, `repartition`, `coalesce`
- Set ops: `union`, `unionByName`
- SQL / temp views
- Batch writes and streaming write plan compilation
- `explain(...)`

Not exposed as stable public API yet:

- `INTERSECT` / `EXCEPT`
- JDBC / Delta / Iceberg / Hudi
- MLlib

## License

Apache-2.0
