# ts-spark-connector
🌱 **Status: Alpha – Early growth stage**

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/sql-connect.html).  
Construct Spark logical plans entirely in TypeScript and run them against a Spark Connect server.

## 🚀 Features

- Build Spark logical plans using a fluent, PySpark-style API in TypeScript
- Evaluate transformations locally or stream results via Arrow
- Tagless Final DSL design with support for multiple backends
- Composable, immutable, and strongly typed DataFrame operations
- Supports column expressions (`col`, `.gt`, `.alias`, `.and`, etc.)
- Compatible with Spark Connect Protobuf and `spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer`
- **Supports Spark SetOperations** (`UNION`, `INTERSECT`, `EXCEPT`) with `by_name`, `is_all`, and `allow_missing_columns`
- Support for Spark-compatible joins with configurable join types
- Session-aware execution without relying on global singletons
- Includes **ready-to-run examples** in the `examples/` folder

## 📦 Installation

```bash
git clone https://github.com/your-org/ts-spark-connector
cd ts-spark-connector
npm install
```

## 🔧 Docker Setup

You need a Spark Connect server running. Example `docker-compose.yml`:

```yaml
services:
  spark:
    build: ./spark-server
    ports:
      - "15002:15002"
    volumes:
      - ./example_data:/data
    environment:
      - SPARK_NO_DAEMONIZE=true
```

`spark-server/Dockerfile`:
```Dockerfile
FROM bitnami/spark:latest
USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
CMD ["/entrypoint.sh"]
```

`spark-server/entrypoint.sh`:
```bash
#!/bin/bash
/opt/bitnami/spark/bin/spark-submit   --class org.apache.spark.sql.connect.service.SparkConnectServer   --conf spark.sql.connect.enable=true   --conf spark.sql.connect.grpc.binding=0.0.0.0:15002   /opt/bitnami/spark/jars/spark-connect_2.12-4.0.0.jar
```

Example data in `/example_data`:
- `people.tsv`
- `purchases.tsv`

## 📂 Examples

We include a set of TypeScript scripts in the `examples/` folder:

| File              | Description |
|-------------------|-------------|
| `join.ts`         | Inner join between two datasets |
| `groupBy.ts`      | Aggregations with `groupBy` and `orderBy` |
| `withColumn.ts`   | Adding and renaming columns |
| `union.ts`        | `SetOperation` with UNION queries |
| `distinct.ts`     | Using `distinct()` and `dropDuplicates()` |

Run any example with:

```bash
npx tsx examples/join.ts
```

## 🧪 Quick Usage

```ts
import { spark } from "./src/spark/session";
import { col } from "./src/engine/column";

const people = spark.read
    .option("delimiter", "\t")
    .option("header", "true")
    .csv("/data/people.tsv");

const purchases = spark.read
    .option("delimiter", "\t")
    .option("header", "true")
    .csv("/data/purchases.tsv");

people
  .join(purchases, col("id").eq(col("user_id")), "left")
  .select("name", "product", "amount")
  .filter(col("amount").gt(100))
  .show();
```

### Example: UNION with SetOperation

```ts
const p2024 = purchases.filter(col("year").eq(2024));
const p2025 = purchases.filter(col("year").eq(2025));

p2024.union(p2025, { is_all: true, by_name: false })
  .limit(5)
  .show();
```

## 💡 Column Expressions

```ts
col("age").gt(18)
col("country").eq("AR")
col("age").gt(18).and(col("active").eq(true)).alias("eligible")
```

## 🧠 Tagless Final DSL

```ts
function userQuery<F>(dsl: DataFrameDSL<F>): F {
  return dsl
    .select(["name", "age"])
    .filter("age > 18")
    .withColumn("eligible", col("age").gt(18).and(col("country").eq("AR")));
}
```

## ✅ Status Features & Roadmap

## Legend
- **P0** = Paridad base inmediata
- **P1** = I/O realista
- **P2** = Performance & DX
- **P3** = SQL/Catálogo & control
- **P4** = UDF (scalar / vectorizadas)
- **P5** = Streaming / Lakehouse / JDBC
- **P6** = MLlib
- **—** = ya implementado

## Feature Matrix

| Feature | Supported | Priority |
|---|---|---|
| CSV Reading | ✅ | — |
| Filtering | ✅ | — |
| Projection / Alias | ✅ | — |
| Arrow decoding | ✅ (`.show()` prints tabular output) | — |
| Column expressions | ✅ (`col`, `.gt`, `.and`, `.alias`, etc.) | — |
| DSL abstraction | ✅ Tagless Final | — |
| Join | ✅ Supports all join types | — |
| Aggregation | ✅ (`groupBy().agg({...})`) | — |
| Distinct | ✅ (`distinct()`, `dropDuplicates(...)`) | — |
| Sorting | ✅ (`orderBy(...)`, `sort(...)`) | — |
| Limit & Take | ✅ (`limit(n)`) | — |
| **SetOperation** | ✅ (`UNION`, `INTERSECT`, `EXCEPT`) | — |
| Column renaming | ✅ (`withColumnRenamed(...)`) | — |
| Type declarations | ✅ `.d.ts` published to NPM | — |
| Modular compiler core | ✅ (`engine/` separated from Spark backend) | — |
| NPM Package | ✅ [Published](https://www.npmjs.com/package/ts-spark-connector) | — |
| Tests (Unit + Integration) | 🚧 In progress | **P0** |
| **withColumn(...)** | ❌ Not yet | **P0** |
| **when(...).otherwise(...)** (CASE WHEN) | ❌ Not yet | **P0** |
| **Window functions** (`over`, `partitionBy`, `orderBy`, `rowsBetween`) | ❌ Not yet | **P0** |
| **Null handling** (`na.drop`, `na.fill`, `na.replace`, `isNull`) | ❌ Not yet | **P0** |
| **Parquet Reading** | ❌ Not yet | **P1** |
| **JSON Reading** | ❌ Not yet | **P1** |
| **DataFrameWriter** (CSV/JSON/Parquet/ORC) | ❌ Not yet | **P1** |
| Write `partitionBy`, `bucketBy`, `sortBy` | ❌ Not yet | **P1** |
| **describe()**, `summary()` | ❌ Not yet | **P2** |
| **unionByName(...)** | ❌ Not yet | **P2** |
| **Complex types** (arrays/maps/struct) + `explode/posexplode` | ❌ Not yet | **P2** |
| **JSON helpers** (`from_json`, `to_json`) | ❌ Not yet | **P2** |
| **cache() / persist() / unpersist()** | ❌ Not yet | **P2** |
| **repartition(...) / coalesce(...)** | ❌ Not yet | **P2** |
| **explain(...)** (`simple/extended/formatted`) | ❌ Not yet | **P2** |
| `SparkSession.builder.config(...)` | ❌ Not yet | **P2** |
| Auth/TLS for Spark Connect | ❌ Not yet | **P2** |
| **spark.sql(...)** | ❌ Not yet | **P3** |
| Temp views (`createOrReplaceTempView`) | ❌ Not yet | **P3** |
| Catalog (`read.table`, `saveAsTable`) | ❌ Not yet | **P3** |
| Plan viz / AST dump | ❌ Not yet | **P3** |
| **Join hints** (`broadcast`, `shuffle_replicate_nl`, etc.) | ❌ Not yet | **P3** |
| **sample(...)**, `randomSplit(...)` | ❌ Not yet | **P3** |
| UDF (scalar) | ❌ Not yet | **P4** |
| **UDAF / Vectorized UDF (Arrow)** | ❌ Not yet | **P4** |
| Structured Streaming (`readStream` / `writeStream`) | ❌ Not yet | **P5** |
| Watermark / trigger / output modes | ❌ Not yet | **P5** |
| Lakehouse: Delta/Iceberg/Hudi (`format(...)`) | ❌ Not yet | **P5** |
| JDBC read/write (`format("jdbc")`) | ❌ Not yet | **P5** |
| **MLlib** (Pipelines/Transformers/Estimators básicos) | ❌ Not yet | **P6** |

---

## Roadmap por etapas (P0 → P6)

### P0 — Immediate base parity
**Goal:** match PySpark’s minimum ergonomics for DataFrame/Column.  
**Includes:** `withColumn`, `when/otherwise`, **window functions**, `na.*`, **tests** (unit + integration).  
**Acceptance criteria:**
- ✅ `withColumn` supports adding and replacing columns.
- ✅ `when(...).otherwise(...)` builds conditional expressions in `select`/`withColumn`.
- ✅ Windows: `over(partitionBy(...).orderBy(...).rowsBetween(...))` with aggregations.
- ✅ `na.drop/fill/replace` + `isNull/isNotNull` working.
- ✅ Test suite running in CI against a local Spark Connect.
  **Notes:** Add README examples + an `df.explain()` stub for early debugging if helpful.

### P1 — Realistic I/O
**Goal:** work with common data formats and write results.  
**Includes:** **Parquet/JSON reading**, **DataFrameWriter** (CSV/JSON/Parquet/ORC), `partitionBy/bucketBy/sortBy` (write).  
**Criteria:**
- ✅ `spark.read.parquet/json/csv(...)` with common `options(...)`.
- ✅ `df.write.format(...).mode("overwrite|append").save(...)`.
- ✅ `partitionBy` on write and smoke tests that read back what was written.
  **Notes:** Cover basic schema inference and clear error messages.

### P2 — Performance & DX
**Goal:** partition control, caching, and better debugging.  
**Includes:** `cache/persist/unpersist`, `repartition/coalesce`, **explain(formatted)**, `describe/summary`, `unionByName`, **complex types + explode**, **JSON helpers**, `SparkSession.builder.config`, **Auth/TLS**.  
**Criteria:**
- ✅ Partition changes reflected in the plan.
- ✅ `cache/persist` maintain consistency; `unpersist` cleans up.
- ✅ `explain("formatted")`/`df.explain()` shows a valid plan.
- ✅ `from_json/to_json` + `explode` for arrays/maps/structs.
  **Notes:** Document tuning recommendations and DX examples.

### P3 — SQL/Catalog & fine-grained control
**Goal:** full interop with SQL and catalog.  
**Includes:** **spark.sql(...)**, **temp views**, **catalog** (`read.table`, `saveAsTable`), **plan viz/AST dump**, **join hints**, `sample/randomSplit`.  
**Criteria:**
- ✅ `spark.sql("...")` yields a `DataFrame` equivalent to the DSL.
- ✅ `createOrReplaceTempView` usable from `spark.sql`.
- ✅ `broadcast(df2)`/hints reflected in the plan.
  **Notes:** Optional: command to dump the logical plan in JSON/Protobuf.

### P4 — UDF (User-Defined Functions)
**Goal:** extend transformations with custom logic.  
**Includes:** **Scalar UDF**, **UDAF / vectorized UDF (Arrow)**.  
**Criteria:**
- ✅ Register and use UDFs in the DSL and in `spark.sql`.
- ✅ Vectorized UDF with Arrow works on the happy path and passes interoperability tests.
  **Notes:** Document costs/limitations and best practices.

### P5 — Streaming / Lakehouse / JDBC
**Goal:** enable production pipelines.  
**Includes:** **Structured Streaming** (`readStream`/`writeStream`, **watermark**, **trigger**, **output modes**), **Delta/Iceberg/Hudi** via `format(...)`, **JDBC** read/write.  
**Criteria:**
- ✅ End-to-end streaming example (socket/Kafka → transform → sink).
- ✅ Read/write to Delta/Iceberg/Hudi when the cluster has the required JARs.
- ✅ JDBC tested against a popular database (Postgres/MySQL).

### P6 — MLlib
**Goal:** ML pipelines on DataFrames from TypeScript.  
**Includes:** Pipelines, basic Estimators/Transformers (e.g., `StringIndexer`, `VectorAssembler`, `LogisticRegression`).  
**Criteria:**
- ✅ Train, save, and load models; `transform()` on a DataFrame.
- ✅ Reproducible example (public dataset) and migration guide from PySpark.

---


## 📄 License

Apache-2.0 — This project aims to democratize access to Spark features from the TypeScript ecosystem.
