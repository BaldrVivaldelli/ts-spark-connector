# Security policy

## Supported versions

- `3.x` receives security fixes. They land on `main` and ship through the
  regular release flow, so upgrade to the latest `3.x` release to pick them up.
- `2.x` and older majors, including the `2.0.0-next.*` prereleases, do not
  receive fixes.

## Reporting a vulnerability

Do not open a public issue or pull request for a suspected vulnerability.
Report it privately through GitHub's advisory form:

https://github.com/BaldrVivaldelli/ts-spark-connector/security/advisories/new

Include the affected version, the impact as you understand it, and a minimal
reproduction when you have one. The project has a single maintainer, so
response times are best effort: expect an acknowledgement within 7 days, and a
fix or a status update once the report has been reproduced.

## Scope

ts-spark-connector is a client library. The security-relevant surface is what
it sends to a Spark Connect server and what it hands to telemetry:

- Token, basic and provider credentials configured with `withAuth()` are sent
  to the server as gRPC metadata on every RPC.
- `withS3Credentials()` forwards AWS credentials to the server as Spark
  configuration (`spark.hadoop.fs.s3a.*`) in Config RPCs.
- Bearer, basic, provider and S3 credentials are refused on plaintext `sc://`
  connections unless `.allowInsecureAuth()` is set explicitly. Setting the raw
  `spark.hadoop.fs.s3a.*` keys through `.config()` bypasses that guard by
  design.
- The opt-in telemetry sink only receives redacted events: authorization,
  token, password, secret, credential and private-key fields, including the S3
  keys above, are removed before the sink is invoked.
- The TLS certificates and keystore committed under `spark-server/certs` are
  public development fixtures for the bundled Docker server. They are not
  secrets, must never be trusted or reused in production, and their presence
  in the repository is not a vulnerability.

Vulnerabilities in Apache Spark or in a Spark Connect server deployment should
go to the [Apache Spark security team](https://spark.apache.org/security.html).
