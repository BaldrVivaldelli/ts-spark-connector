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
running Spark server. CI additionally executes `join.ts` against both supported
Spark versions.
