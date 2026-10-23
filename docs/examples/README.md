# Executable examples

Every TypeScript file in this directory imports the public
`ts-spark-connector` package entry point. The examples are compiled with the
same TypeScript 7 compiler used by the library and then executed as JavaScript.

Start the default TLS-enabled Spark 4.0.4 server from the repository root:

```bash
docker compose up --build --detach --wait spark
```

Run one example:

```bash
npm run examples:run -- join
```

Run the complete example set:

```bash
npm run examples:run
```

The runner accepts the same environment variables as the E2E suite:

- `SPARK_CONNECT_URL`, defaulting to `scs://localhost:15002`
- `SPARK_TLS_CA`, defaulting to `./spark-server/certs/ca.crt`
- `SPARK_TLS_SERVER_NAME`, defaulting to `spark-connect`

Stop the local server afterwards:

```bash
docker compose down --volumes
```

`npm run typecheck:examples` validates every example without requiring a
running Spark server. CI additionally executes `join.ts` against every Spark
version covered by the E2E matrix (3.5.6, 4.0.0, 4.0.4, 4.1.3 and 4.2.0).

## AWS

The `aws/` subdirectory shows the `ts-spark-connector/aws` helpers that start
a Spark Connect session on an AWS service, connect to it and stop it on
`spark.close()`:

- `aws/emrServerless.ts`: Amazon EMR Serverless session, SQL query and a small
  DataFrame aggregation.
- `aws/glue.ts`: the same on an AWS Glue interactive session.
- `aws/athena.ts`: the same on an Amazon Athena for Apache Spark session.
- `aws/reuseSession.ts`: a Lambda-style `handler` that keeps one EMR
  Serverless session warm across invocations and relies on the idle timeout
  to stop it.

They read these variables and fail with a clear message when one is missing:

- `AWS_REGION` (all examples)
- `TS_SPARK_AWS_EMR_APPLICATION_ID` and `TS_SPARK_AWS_EMR_EXECUTION_ROLE_ARN`
  (`emrServerless.ts`, `reuseSession.ts`)
- `TS_SPARK_AWS_GLUE_ROLE_ARN` (`glue.ts`)
- `TS_SPARK_AWS_ATHENA_WORKGROUP` (`athena.ts`)

Credentials come from the AWS SDK's default chain, and each helper needs its
own `@aws-sdk/client-*` package. These files are type-checked in CI but never
run: they need an AWS account and the sessions they start are billed.
`npm run examples:run` only runs the top-level files. To create the resources
the variables point at, see `infra/aws-e2e/`.
