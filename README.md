# ts-spark-connector

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/sql-connect.html). This project allows you to construct Spark logical plans entirely in TypeScript and run them against a Spark Connect server.

## 🚀 Features

- Build Spark logical plans using a fluent, PySpark-style API in TypeScript
- Evaluate transformations locally or stream results via Arrow
- Tagless Final DSL design with support for multiple backends
- Composable, immutable, and strongly typed DataFrame operations
- Supports column expressions (`col`, `.gt`, `.alias`, `.and`, etc.)
- Compatible with Spark Connect Protobuf and `spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer`


## 📦 Installation

```bash
git clone https://github.com/your-org/ts-spark-connector
cd ts-spark-connector
npm install
```

## 🔧 Docker Setup

You need a Spark Connect server running. Here's a `docker-compose.yml` example:

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

The `spark-server` Dockerfile should start Spark Connect:

```Dockerfile
FROM bitnami/spark:latest
USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
CMD ["/entrypoint.sh"]
```

The `entrypoint.sh`:

```bash
#!/bin/bash
/opt/bitnami/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --conf spark.sql.connect.enable=true \
  --conf spark.sql.connect.grpc.binding=0.0.0.0:15002 \
  /opt/bitnami/spark/jars/spark-connect_2.12-4.0.0.jar
```

Make sure `example_data/people.tsv` exists.

## 🧪 Example Usage

```ts
import { session } from "./src/session";

const df = session.read.csv("/data/people.tsv", { delimiter: "\t", header: "true" });
const result = await df.filter(df.col("age").gt(30)).select("name", "country").collect();
console.log(result);
```

## 💡 Column Expressions
This library supports composable column expressions using a Spark-like DSL:
```ts
col("age").gt(18)
col("country").eq("AR")
col("age").gt(18).and(col("active").eq(true)).alias("eligible")
```

## 🧠 Tagless Final DSL
The internal architecture separates the declarative query description from its interpretation (compilation, debugging, execution, etc.), enabling:

- Static analysis or testable plans

- Reuse across backends (debug, Spark, SQL, etc.)

- DSL reuse without tying to Spark

```ts
function userQuery<F>(dsl: DataFrameDSL<F>): F {
    return dsl
        .select(["name", "age"])
        .filter("age > 18")
        .withColumn("eligible", col("age").gt(18).and(col("country").eq("AR")))
}
```

## ✅ Status

| Feature            | Supported                                |
| ------------------ | ---------------------------------------- |
| CSV Reading        | ✅                                        |
| Filtering          | ✅                                        |
| Projection / Alias | ✅                                        |
| Arrow decoding     | ✅ (`.show()` prints tabular output)      |
| Column expressions | ✅ (`col`, `.gt`, `.and`, `.alias`, etc.) |
| DSL abstraction    | ✅ Tagless Final                          |
| UDF                | ❌                                        |
| Join               | ❌                                        |
| Aggregation        | ✅ (with `groupBy().agg({...})`)          |

## 📄 License

APACHE or the one that is opensource. The main idea is to democracy the access of spark features to TS ecosystem