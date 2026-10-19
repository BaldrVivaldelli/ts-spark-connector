# Testing with Docker

This project includes Docker configuration for the behavioral E2E gate. The CI
E2E matrix covers Apache Spark Connect 4.0.0, 4.0.4, 4.1.3 and 4.2.0 over TLS.
The vendored protocol descriptor is the exact Apache Spark 4.2.0 set, the
newest line in the matrix; the older servers verify that version-gated
features stay off and everything else keeps working. `SPARK_VERSION` reaches
the test runner so the suite can assert the version the server reports.

## Prerequisites

- Docker
- Docker Compose
- OpenSSL, only when renewing or checking the development certificates

## Running Tests

Unit and protocol-shape tests do not need Spark and explicitly exclude E2E tests:

```bash
npm test
npm run typecheck:test
npm run test:coverage
npm run api:check
npm run package:lint
npm run typecheck:examples
```

The E2E suite has a separate Vitest configuration. It includes only
`*.e2e.test.ts` files, fails when no E2E test is discovered, and requires a
reachable Spark Connect server plus the `/data` fixtures:

```bash
npm run test:e2e
```

To validate the exact publishable artifact without downloading dependencies,
run `npm run test:package`. It cleans and packs the project, extracts the
tarball locally, and exercises CommonJS, ESM, and TypeScript consumers.

`npm run benchmark` also enforces a deliberately conservative plan-compilation
floor of 1,000 plans/second. Override it with
`SPARK_PLAN_BENCH_MIN_PLANS_PER_SECOND` when maintaining a calibrated runner;
the check is intended to catch catastrophic regressions, not compare machines.

### Option 1: Using npm scripts (Recommended)

```bash
# Build Spark and run the E2E suite over TLS
npm run test:docker

# Clean up Docker containers and volumes after testing
npm run test:docker:cleanup

# View logs from the test containers
npm run test:docker:logs
```

### Option 2: Using Docker Compose directly

```bash
# Build and run tests
docker compose -f docker-compose.test.yml up --build \
  --abort-on-container-exit \
  --exit-code-from test-runner \
  test-runner

# Clean up afterwards
docker compose -f docker-compose.test.yml down -v
```

## How it works

The Docker test setup includes:

1. **Spark server container**: runs the selected Spark version (4.0.0, 4.0.4,
   4.1.3 or 4.2.0 in the E2E matrix) on a loopback-only plaintext backend.
   HAProxy terminates TLS with ALPN `h2` on port 15002.
2. **Test runner container**: trusts only the public development CA and runs
   the E2E suite against Spark.

The test runner waits for the Spark server TCP health check before running. The
E2E assertions, rather than the health check, prove the TLS handshake, protocol
compatibility, rows, ordering, joins, multipath reads, and writes. The
containers communicate over the Compose network using the `spark` hostname.

Each CI matrix job also runs the executable `join` example and publishes:

- the Vitest JUnit document;
- a JSON result containing version, commit and outcomes;
- a Markdown report in the GitHub job summary;
- Spark logs when the suite or example fails.

The downloadable `e2e-spark-4.0.0`, `e2e-spark-4.0.4`, `e2e-spark-4.1.3` and
`e2e-spark-4.2.0` artifacts are retained for 30 days.

## Environment Variables

- `SPARK_CONNECT_URL`: `scs://spark:15002` in the Docker E2E environment.
- `SPARK_TLS_CA`: `/app/spark-server/certs/ca.crt`, the public development CA.
- `SPARK_TLS_SERVER_NAME`: `spark-connect`; this and `spark` are both present
  in the certificate SANs.
- `SPARK_TEST_DEST`: server-visible output root for write/read round trips.
- `SPARK_VERSION`: server/image version; defaults to `4.0.4`. The E2E matrix
  covers `4.0.0`, `4.0.4`, `4.1.3` and `4.2.0`.
- `SPARK_AVRO_SHA256`: SHA-256 of the matching Scala 2.13 Avro artifact
  (`spark-avro_2.13-<version>.jar` on Maven Central). Always set it together
  with `SPARK_VERSION`:

| `SPARK_VERSION` | `SPARK_AVRO_SHA256` |
| --- | --- |
| `4.0.0` | `083fb13d7a1091025b135eff216e2009b42c9724bb8bc597d2f28b1207ef4709` |
| `4.0.4` | `f2862c13564bf6cd78cfdfe7b902ef07e68d1d6a5edd9fde336c0cf0fcdb8c55` |
| `4.1.3` | `83f848dae53cfe511d61d81026c1b08321390b778684552f3ddaa44c02a77b9c` |
| `4.2.0` | `4c186cedc03cbe4ee356a94b32232b347ef48611c3209400b3a77e4fe4abff4f` |

To run the default CI matrix target locally:

```bash
SPARK_VERSION=4.0.4 \
SPARK_AVRO_SHA256=f2862c13564bf6cd78cfdfe7b902ef07e68d1d6a5edd9fde336c0cf0fcdb8c55 \
npm run test:docker
```

To run the newest matrix entry instead:

```bash
SPARK_VERSION=4.2.0 \
SPARK_AVRO_SHA256=4c186cedc03cbe4ee356a94b32232b347ef48611c3209400b3a77e4fe4abff4f \
npm run test:docker
```

## Data and Certificates

- `./example_data` is copied into both images as `/data` for deterministic
  fixtures.
- The Spark image contains `ca.crt`, `cert.crt`, and `keystore.p12`.
- The test runner receives only `ca.crt`; the server private key is not copied
  into the client image.
- The certificates are public development fixtures. They must never be used in
  production or to protect real credentials/data.

Validate the tracked chain, SANs, expiry, and PKCS#12 contents with:

```bash
./spark-server/certs/generate-dev-certs.sh --check
```

Renew it with the same command without `--check`, then commit `ca.crt`,
`cert.crt`, and `keystore.p12` together and rebuild the images. The validator
fails inside the 30-day renewal window. No loose PEM private key is written to
the repository.

## Running E2E from the host

To run the same TLS server while executing Vitest on the host:

```bash
docker compose up --build --detach --wait spark

SPARK_CONNECT_URL=scs://localhost:15002 \
SPARK_TLS_CA=./spark-server/certs/ca.crt \
SPARK_TLS_SERVER_NAME=spark-connect \
SPARK_TEST_DEST=/data/dest/host \
npm run test:e2e

docker compose down --volumes
```

The plaintext override (`docker-compose.plain.yml`) is available for manual,
isolated debugging, but it is intentionally not used by the compatibility
gate. See `spark-server/README.md`.

## Troubleshooting

### Tests failing to connect to Spark

- Check that the Spark container is healthy: `docker compose -f docker-compose.test.yml ps`
- View Spark logs: `docker compose -f docker-compose.test.yml logs spark`
- Validate the CA/keystore: `./spark-server/certs/generate-dev-certs.sh --check`
- Rebuild after renewing certificates; restarting an old container does not
  replace the keystore baked into its image.

### Build failures

- Ensure all dependencies are listed in `package.json`
- Check that TypeScript compilation succeeds: `npm run build`

### Permission issues

- Spark runs as a non-root user. The default Compose file uses a named volume
  for `/data/dest` to avoid host UID mismatches; use the same approach for
  additional writable locations.

## Coverage policy

`npm run test:coverage` enforces 100 % statements, functions, lines and
branches over `src/`, measured by the unit suite only. The gate exists to
surface untested behaviour, not to be satisfied mechanically:

- Reach a branch through the public API, or through a direct unit test of the
  function that owns it, and assert on the result. Assertions such as
  `expect(value).toBeTypeOf("object")` do not count as coverage of behaviour.
- Do not fabricate states the types forbid (for example an
  `as unknown as ProtoWriteRoot` with missing fields) to light up a defensive
  fallback. Remove the fallback and let the type carry the guarantee, or, when
  the guard protects against untyped external input, exercise it with
  type-valid malformed values.
- Interpreter methods that the public API never calls (for example
  `TraceDFAlg.describe`, which the DataFrame emulates client-side) are tested
  directly in the interpreter's own test file.
- Mocks do not cover transport behaviour. Reattach, cancellation, deadlines,
  Arrow decoding of real batches and authentication are only considered
  covered by `npm run test:e2e` against a Spark Connect server.
