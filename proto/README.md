# Vendored Spark Connect protocol

These files are an exact copy of Apache Spark tag `v4.0.0` from:

`sql/connect/common/src/main/protobuf/spark/connect/`

Source: <https://github.com/apache/spark/tree/v4.0.0/sql/connect/common/src/main/protobuf/spark/connect>

Run `npm run test:proto` to verify the complete file set and SHA-256 manifest.
Wire-conformance tests load this descriptor with `@grpc/proto-loader` and
round-trip every plan shape emitted by the connector. Update the version,
manifest, Docker image, compatibility docs, and live E2E matrix together.

The files retain their Apache license headers. The package also ships the
repository-level `LICENSE` and `NOTICE`, including the Apache Spark attribution.
