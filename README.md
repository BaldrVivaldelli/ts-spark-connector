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

## ✅ Status

| Feature               | Supported                                          |
|-----------------------|----------------------------------------------------|
| CSV Reading           | ✅                                                  |
| Filtering             | ✅                                                  |
| Projection / Alias    | ✅                                                  |
| Arrow decoding        | ✅ (`.show()` prints tabular output)               |
| Column expressions    | ✅ (`col`, `.gt`, `.and`, `.alias`, etc.)          |
| DSL abstraction       | ✅ Tagless Final                                    |
| Join                  | ✅ Supports all join types                         |
| Aggregation           | ✅ (`groupBy().agg({...})`)                        |
| Distinct              | ✅ (`distinct()`, `dropDuplicates(...)`)           |
| Sorting               | ✅ (`orderBy(...)`, `sort(...)`)                   |
| Limit & Take          | ✅ (`limit(n)`)                                    |
| **SetOperation**      | ✅ (`UNION`, `INTERSECT`, `EXCEPT`)                 |
| Column renaming       | ✅ (`withColumnRenamed(...)`)                       |
| UDF                   | ❌ Not yet                                         |
| Type declarations     | ✅ `.d.ts` published to NPM                        |
| Tests (Unit + Integration) | 🚧 In progress                                |
| Modular compiler core | ✅ (`engine/` separated from Spark backend)        |
| NPM Package           | ✅ [Published](https://www.npmjs.com/package/ts-spark-connector) |

## 📄 License

Apache-2.0 — This project aims to democratize access to Spark features from the TypeScript ecosystem.
