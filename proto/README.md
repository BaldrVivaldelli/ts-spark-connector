# Vendored Spark Connect protocol

These files are an exact copy of Apache Spark tag `v4.0.4` from:

`sql/connect/common/src/main/protobuf/spark/connect/`

Source: <https://github.com/apache/spark/tree/v4.0.4/sql/connect/common/src/main/protobuf/spark/connect>

The descriptors are pinned to the oldest fully supported Spark line on purpose:
newer servers covered by the E2E matrix (currently 4.1.3 and 4.2.0, alongside
4.0.0 and 4.0.4) are verified behaviorally against this 4.0.4 set rather than
by re-vendoring their protocol files.

Run `npm run test:proto` to verify the complete file set and SHA-256 manifest.
Wire-conformance tests load this descriptor with `@grpc/proto-loader` and
round-trip every plan shape emitted by the connector. Update the version,
manifest, Docker image, compatibility docs, and live E2E matrix together.

The files retain their Apache license headers. The package also ships the
repository-level `LICENSE` and `NOTICE`, including the Apache Spark attribution.
