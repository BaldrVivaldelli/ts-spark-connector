# ts-spark-connector

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/sql-connect.html).  
Construct Spark logical plans entirely in TypeScript and run them against a Spark Connect server.

## 🚀 Features

- Build Spark logical plans using a fluent, PySpark-style API in TypeScript
- Evaluate transformations locally or stream results via Arrow
- Tagless Final DSL design with support for multiple backends
- Composable, immutable, and strongly typed DataFrame operations
- Column expressions (`col`, `.gt`, `.alias`, `.and`, etc.)
- Compatible with Spark Connect Protobuf and
  `spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer`
- **Set operations** (`UNION`, `INTERSECT`, `EXCEPT`) with `by_name`, `is_all`, and `allow_missing_columns`
- Spark-compatible joins with configurable join types
- Session-aware execution (no global singletons)
- **Plan viz / AST dump**: export client AST to JSON & Mermaid
- **Ready-to-run examples** in `examples/`

## 📦 Installation

```bash
git clone https://github.com/BaldrVivaldelli/ts-spark-connector
cd ts-spark-connector
npm install
```

> You need a running **Spark Connect** server. See [`spark-server/README.md`](spark-server/README.md) for a ready-to-use Docker setup, or run your own server.

## 🧪 Quick Start

```ts
import { SparkSession } from "./src/client/session";
import { col } from "./src/engine/column";

const session = SparkSession.builder()
  // optional: auth / TLS
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
```

### Example: UNION (Set Operation)

```ts
const p2024 = purchases.filter(col("year").eq(2024));
const p2025 = purchases.filter(col("year").eq(2025));

await p2024.union(p2025, { is_all: true, by_name: false })
  .limit(5)
  .show();
```

## 🗺️ Plan viz / AST dump

Inspect the **client-side** plan (before server optimization):

```ts
const df = purchases
  .select("user_id", "product", "amount")
  .filter(col("amount").gt(100))
  .orderBy(col("user_id").descNullsLast());

console.log(df.toClientASTJSON());      // JSON AST
console.log(df.toClientASTMermaid());   // Mermaid diagram
console.log(df.toSparkLogicalPlanJSON());// Client logical plan
console.log(df.toProtoJSON());           // Spark Connect proto
```

> Tip: write these strings to disk (`.mmd`, `.json`) and publish them as CI artifacts.

## 🔐 TLS

```ts
const session = SparkSession.builder()
  .enableTLS({
    keyStorePath: "./certs/keystore.p12",
    keyStorePassword: "password",
    trustStorePath: "./certs/cert.crt",
    trustStorePassword: "password",
  })
  .getOrCreate();
```

## ✅ Compatibility Matrix

| Component        | Supported / Tested                 |
|------------------|------------------------------------|
| Spark Connect    | 3.5.x                              |
| Scala ABI (JAR)  | 2.12 (`spark-connect_2.12`)        |
| Node.js          | 18, 20, 22                         |
| OS               | Linux (CI); macOS (local)          |

> Planned: add CI jobs for macOS/Windows; update table as coverage expands.

## ✅ Feature Matrix

| Feature                                                                | Supported |
|------------------------------------------------------------------------|-----------|
| CSV Reading                                                            | ✅         |
| Filtering                                                              | ✅         |
| Projection / Alias                                                     | ✅         |
| Arrow decoding (`.show()`)                                             | ✅         |
| Column expressions (`col`, `.gt`, `.and`, `.alias`, etc.)              | ✅         |
| DSL abstraction (Tagless Final)                                        | ✅         |
| Joins (configurable types)                                             | ✅         |
| Aggregation (`groupBy().agg({...})`)                                   | ✅         |
| Distinct (`distinct()`, `dropDuplicates(...)`)                         | ✅         |
| Sorting (`orderBy(...)`, `sort(...)`)                                  | ✅         |
| Limit (`limit(n)`)                                                     | ✅         |
| **Set operations** (`UNION`, `INTERSECT`, `EXCEPT`)                    | ✅         |
| Column renaming (`withColumnRenamed(...)`)                             | ✅         |
| Type declarations (`.d.ts`)                                            | ✅         |
| Modular compiler core (backend-agnostic)                               | ✅         |
| Tests (Unit + Integration + E2E)                                       | ✅         |
| **withColumn(...)**                                                    | ✅         |
| **when(...).otherwise(...)**                                           | ✅         |
| **Window functions**                                                   | ✅         |
| **Null handling** (`isNull`, `na.drop/fill/replace`)                   | ✅         |
| **Parquet Reading**                                                    | ✅         |
| **JSON Reading**                                                       | ✅         |
| **DataFrameWriter** (CSV/JSON/Parquet/ORC/Avro)                        | ✅         |
| Write `partitionBy`, `bucketBy`, `sortBy`                              | ✅         |
| **describe()**, `summary()`                                            | ✅         |
| **unionByName(...)**                                                   | ✅         |
| **Complex types** + `explode/posexplode`                               | ✅         |
| **JSON helpers** (`from_json`, `to_json`)                              | ✅         |
| **repartition(...) / coalesce(...)**                                   | ✅         |
| **explain(...)** (`simple/extended/formatted`)                         | ✅         |
| `SparkSession.builder.config(...)`                                     | ✅         |
| Auth/TLS for Spark Connect                                             | ✅         |
| **spark.sql(...)**                                                     | ✅         |
| Temp views (`createOrReplaceTempView`)                                 | ✅         |
| Catalog (`read.table`, `saveAsTable`)                                  | ✅         |
| **Plan viz / AST dump**                                                | ✅         |
| **cache() / persist() / unpersist()**                                  | ⚠️ Limited by Spark Connect |
| **Join hints** (`broadcast`, etc.)                                     | ✅         |
| **sample(...)**, `randomSplit(...)`                                    | ✅         |
| UDF (scalar)                                                           | ⚠️ Limited by Spark Connect |
| **UDAF / Vectorized UDF (Arrow)**                                      | ⚠️ Limited by Spark Connect |
| Structured Streaming                                                   | ✅         |
| Watermark / triggers / output modes                                    | ✅         |
| Lakehouse: Delta/Iceberg/Hudi                                          | ❌         |
| JDBC read/write                                                        | ❌         |
| **MLlib**                                                              | ❌         |

## 📄 License

Apache-2.0
