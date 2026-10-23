# Vendored Spark Connect protocol

These files are an exact copy of Apache Spark tag `v4.2.0` from:

`sql/connect/common/src/main/protobuf/spark/connect/`

Source: <https://github.com/apache/spark/tree/v4.2.0/sql/connect/common/src/main/protobuf/spark/connect>

The descriptors follow the newest Spark line in the E2E matrix. Older servers
in the matrix (currently 3.5.6, 4.0.0, 4.0.4 and 4.1.3) stay supported because
the connector only emits messages they understand: anything newer is gated on
the version the server reports (`session.requireServerVersion`, and for plans
`src/client/serverCompatibility.ts`).

Run `npm run test:proto` to verify the complete file set and SHA-256 manifest.
Wire-conformance tests load this descriptor with `@grpc/proto-loader` and
round-trip every plan shape emitted by the connector. Update the version,
manifest, Docker image, compatibility docs, and live E2E matrix together.

The files retain their Apache license headers. The package also ships the
repository-level `LICENSE` and `NOTICE`, including the Apache Spark attribution.
