# spark-server (Docker)

This folder contains a minimal **Spark Connect** server setup using Docker.  
Use it to run `ts-spark-connector` locally without installing Spark on your host.

## 🧰 What you get

- A container running **Apache Spark** with the **Spark Connect** server enabled
- gRPC bound on **`:15002`**
- A volume mounted at `/data` for sample datasets
- Simple build & run via `docker compose`

## 📁 Files

```
spark-server/
├─ Dockerfile
├─ entrypoint.sh
└─ docker-compose.yml (example shown below – create at repo root if preferred)
```

## 🧩 docker-compose.yml (example)

Create this file at the repo root (or adapt paths to your layout):

```yaml
services:
  spark:
    build: ./spark-server
    container_name: spark-connect
    ports:
      - "15002:15002"
    environment:
      - SPARK_NO_DAEMONIZE=true
      # optionally set Spark version for the Connect artifact in entrypoint.sh
      - SPARK_VERSION=3.5.1
    volumes:
      - ./example_data:/data
```

> Mount any local folder with TSV/CSV/Parquet files to `/data` so your examples can read them, e.g. `/data/people.tsv` and `/data/purchases.tsv`.

## 🐳 Dockerfile

```Dockerfile
FROM bitnami/spark:latest
USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
CMD ["/entrypoint.sh"]
```

## 🚀 Entrypoint

`entrypoint.sh` starts the Spark Connect server on `0.0.0.0:15002`:

```bash
#!/bin/bash
/opt/bitnami/spark/bin/spark-submit   --class org.apache.spark.sql.connect.service.SparkConnectServer   --conf spark.sql.connect.enable=true   --conf spark.sql.connect.grpc.binding=0.0.0.0:15002   --packages org.apache.spark:spark-connect_2.12:${SPARK_VERSION:-3.5.1}
```

> If you prefer to ship the jar yourself instead of `--packages`, adjust the command accordingly and make sure the artifact is in the container.

## ▶️ Run

From the repo root:

```bash
docker compose up --build
```

You should see logs indicating the Connect server is listening on `0.0.0.0:15002`.

## 🔗 Client connection

By default, `ts-spark-connector` uses the environment variable `SPARK_CONNECT_URL` or falls back to `sc://localhost:15002`:

```bash
export SPARK_CONNECT_URL=sc://localhost:15002
```

In TypeScript you can create a session like:

```ts
import { SparkSession } from "../src/client/session";

const session = SparkSession.builder()
  // .withAuth({ type: "token", token: "my-token" }) // optional
  .getOrCreate();
```

## 🔐 TLS (optional)

This example uses **plain gRPC** for simplicity. If you need TLS, place your certificates in a secure path and enable TLS in your client:

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

> Enabling TLS on the **server** side requires additional Spark configuration not covered in this minimal setup.

## 🧪 Sample data

Place sample data under `./example_data` on your host. It will appear as `/data` in the container:

- `people.tsv`
- `purchases.tsv`

Then you can reference them in your examples, e.g. `session.read.csv("/data/people.tsv")`.

---

Happy Sparking! 🚀
