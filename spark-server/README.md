# Local Spark Connect server

The repository ships a Dockerized Spark Connect server for development and
end-to-end tests. It defaults to **Apache Spark 4.0.4** and CI also exercises
**4.0.0** for backwards compatibility, always with the matching Scala 2.13
Avro artifact. TLS is enabled by default on port `15002`.

Spark Connect 4.0.x exposes a plaintext gRPC listener. The container therefore
binds Spark only to `127.0.0.1:15003` and uses HAProxy to terminate TLS/ALPN
`h2` on the public `15002` endpoint. The PKCS#12 fixture's private key is
extracted only into the container's mode-`0700` runtime directory and removed
when the entrypoint exits; no loose private key is stored in the repository or
image layer.

Other releases may work, but are not claimed as supported until they have a
green compatibility matrix.

## Files

```text
spark-server/
├── Dockerfile
├── entrypoint.sh                        # Spark + TLS proxy supervisor
├── conf/spark-defaults.conf             # loopback Spark backend
├── conf-plain/spark-defaults.conf       # opt-in plaintext profile
└── certs/
    ├── ca.crt                           # public development CA
    ├── cert.crt                         # public server certificate
    ├── keystore.p12                     # server key + certificate chain
    └── generate-dev-certs.sh            # generation/validation tool
```

The root `docker-compose.yml` mounts sample data at `/data`. The image installs
the version-matched `spark-avro_2.13` artifact at build time so Avro reads do
not depend on an Ivy download when the server starts.

## Start the TLS server

From the repository root:

```bash
docker compose up --build
```

The default uses Spark 4.0.4. To select the backwards-compatibility matrix
entry instead:

```bash
SPARK_VERSION=4.0.0 \
SPARK_AVRO_SHA256=083fb13d7a1091025b135eff216e2009b42c9724bb8bc597d2f28b1207ef4709 \
docker compose up --build
```

Connect using the checked-in development CA:

```ts
import { SparkSession } from "ts-spark-connector";

const session = SparkSession.builder()
  .config("spark.connect.url", "scs://localhost:15002")
  .enableTLS({
    trustStorePath: "./spark-server/certs/ca.crt",
  })
  .getOrCreate();
```

The certificate includes SANs for `localhost`, `127.0.0.1`, `spark`, and
`spark-connect`, so normal local and Compose hostnames do not need a TLS name
override.

## Plain gRPC profile

Plaintext is available only as an explicit local-development override:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.plain.yml \
  up --build
```

Use `sc://localhost:15002` with that profile. Do not send tokens, usernames, or
passwords over it. The client rejects Basic/Bearer credentials on plaintext by
default; `.allowInsecureAuth()` exists only for isolated, trusted development
networks.

The Docker E2E gate always uses the TLS profile.

## Development certificates

The committed certificates are public test fixtures, not secrets. The
PKCS#12 password is `password` and matches `conf/spark-defaults.conf`. Never
trust this CA or reuse this keystore in production, shared infrastructure, or
any environment containing real data or credentials.

Generate a fresh CA, server certificate, and PKCS#12 keystore with:

```bash
./spark-server/certs/generate-dev-certs.sh
```

The script uses a mode-`0700` temporary directory, removes it on exit, and
never writes a loose private PEM key into the repository. It validates the
chain, every required SAN, the PKCS#12 leaf certificate, and a 30-day renewal
window before replacing the tracked artifacts. `keytool` is also used when it
is installed; OpenSSL validation is always performed.

Validate without replacing the current artifacts:

```bash
./spark-server/certs/generate-dev-certs.sh --check
```

The default lifetimes are 10 years for the development CA and 825 days for the
server certificate. They can be changed for a renewal with
`SPARK_DEV_CA_DAYS` and `SPARK_DEV_CERT_DAYS`. After renewal, commit
`ca.crt`, `cert.crt`, and `keystore.p12` together and rebuild both Docker
images. Any running container still has the previous keystore.

The npm package allowlist contains only `dist`, `proto`, the root README, and
the license, so none of these Docker test credentials are published.

## Sample data and output

The checked-in fixtures are available to Spark as:

- `/data/people.tsv`
- `/data/purchases.tsv`

The regular Compose setup mounts the `spark-output` Docker volume at
`/data/dest` for Parquet/CSV/JSON/ORC output. A named volume avoids host UID
permission mismatches while Spark runs as a non-root user. While the service is
running, use `docker compose cp spark:/data/dest ./spark-output` if you need a
host copy. `docker compose down --volumes` deletes the stored output.

## Troubleshooting

- Validate certificates with `./spark-server/certs/generate-dev-certs.sh --check`.
- Check status with `docker compose ps`.
- Inspect the server with `docker compose logs spark`.
- If a renewed certificate is not visible, rebuild the image rather than only
  restarting the existing container.
- A TCP health check proves that the gRPC port is listening; the E2E suite is
  the protocol/TLS behavior check.
