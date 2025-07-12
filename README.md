# ts-spark-connector

TypeScript client for Apache Spark Connect. This project allows you to construct Spark projects in typescript.

## 🚀 Features

- Collect data via Arrow serialization
- Easy composability via DataFrame-like API
- Fully written in TypeScript

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

## ✅ Status

| Feature        | Supported |
|----------------|-----------|
| CSV Reading    | ✅        |
| Filtering      | ✅        |
| Projection     | ✅        |
| Arrow decoding | ✅ (partial `.show()`) |
| UDF            | ❌        |
| Join           | ❌        |

## 📄 License

APACHE or the one that is opensource. The main idea is to democracy the access of spark features to TS ecosystem