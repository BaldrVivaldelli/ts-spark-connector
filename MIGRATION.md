# Migration guide

## 3.0: one authentication source, loud header collisions

Authentication misconfigurations that previous releases resolved silently are
now errors:

- `withAuth()` (or an explicit `auth` in the connection config) can no longer
  be combined with the legacy `spark.auth.*` keys. Configuring both throws;
  previously the legacy keys silently rewrote the explicit configuration.
  Migrate by deleting the legacy keys and keeping `withAuth()`.
- A `spark.connect.header.*` entry whose name collides with a header produced
  by the configured authentication (for example
  `spark.connect.header.authorization` next to `withAuth({ type: "token" })`)
  now throws; previously the static header silently overrode the credential.
  Keep whichever source is intentional and remove the other. Disjoint custom
  headers are unaffected.

Legacy `spark.auth.*` keys used **alone** keep working unchanged.

## 2.x

This guide covers the `2.x` releases after `1.12.x`, including the Spark 4.0.4
baseline and the TypeScript 7 build-tool migration.

## Install

Releases are published to the `latest` dist-tag only:

```bash
npm install ts-spark-connector
```

The `next` dist-tag is no longer updated — it stays frozen at `2.0.0-next.2` —
so replace any `ts-spark-connector@next` pin with `latest`. Pin the exact
version reported by `npm view ts-spark-connector version` when reproducible
installations are required.

## Runtime requirements

- Node.js 22 or 24 is required.
- Spark Connect 4.0.4 is the default and protocol baseline.
- Spark Connect 4.0.0 remains behaviorally tested for backwards compatibility.
- Spark Connect 4.1.3 and 4.2.0 are covered by the E2E matrix; the protocol
  descriptors stay pinned to 4.0.4 and the newer servers are verified
  behaviorally.
- TLS remains the default for the repository-provided Spark server.

Application code using the public package entry point does not need to change
solely because the project itself now builds with TypeScript 7.

## TypeScript 7 for contributors

TypeScript 7 removed the legacy `moduleResolution: "node"`/Node10 mode. Use one
of the explicit modern pairs:

```json
{
  "compilerOptions": {
    "module": "Node16",
    "moduleResolution": "Node16"
  }
}
```

For bundler-owned test code, `module: "Preserve"` with
`moduleResolution: "Bundler"` is also supported. Relative imports intended to
run under Node should include their emitted `.js` extension.

The repository uses TypeScript 7 as `tsc`. TypeScript 7.0 does not expose the
programmatic compiler API needed by `typescript-eslint`, so the official
TypeScript 6 compatibility package remains installed only for that tooling:

```json
{
  "devDependencies": {
    "@typescript/native": "npm:typescript@^7.0.2",
    "typescript": "npm:@typescript/typescript6@^6.0.2"
  }
}
```

Run `npm run check:typescript` to prove that `tsc` resolves to version 7 and
that the compatibility API resolves to version 6.

## Imports and examples

Only the root package export is public:

```ts
import { SparkSession, col } from "ts-spark-connector";
```

Do not migrate to `src`, `dist`, `experimental`, or other deep imports. Typed
schema, aggregation, Arrow-row and expression helpers are exported through the
root entry point.

All examples under `docs/examples` are executable:

```bash
npm run examples:run -- join
npm run examples:run
```

## Release flow

Every release is cut from `main`:

1. A pull request with a Conventional Commit title is merged into `main`.
2. CI runs the unit suite on Node.js 22 and 24, the publishable-tarball checks
   and the Spark Connect E2E matrix for the versions listed under Runtime
   requirements; each Spark job runs the full E2E suite plus the executable
   `join` example and publishes JUnit, JSON and Markdown evidence.
3. When every job is green, semantic-release computes the version from the
   commits, publishes the package to npm with provenance and tags the release.
4. The workflow then installs the exact published version from the registry
   and re-checks the registry metadata, the CommonJS and ESM entry points, the
   TypeScript 7 declarations and the production audit against it.

If any check fails, nothing is published until the fix lands on `main`.

## Public API

`etc/ts-spark-connector.public.api.md` is the API Extractor report that CI
enforces through `npm run api:check`. It lists only the declarations documented
`@public`; those declarations are the semver contract.

Declarations documented `@internal` stay exported from the root entry point for
typing convenience only: the tagless-final algebra (`DFAlg`, `ExprAlg`,
`DFProgram`, the `*Cap` capability interfaces, `WProgram`, `WriteCore`), the
writer plumbing (`WriterBackendImplementation`, `ReadDFAny`, `ReadDFPublic`,
`ReadDFPrivate`, `DefaultDataFrameRepresentation`, `WBatch`, `WStream`), the
schema-transform helpers (`Selected`, `Dropped`, `Renamed`, `WithColumn`,
`Joined*`, `Aggregated`, `Prettify`, `UnionToIntersection`, `IsKnownSchema`,
`UsableColumnKey`, `UsableColumnType`, the `*Schema` result types) and the
typed-column internals (`TypedColumnExprThunk`, `NumericOperand`,
`PromotedNumeric`, `Coalesced`, `Widen`, `ColRef`, `PendingAggregation`,
`makeAggFactory`, `TOKEN_TO_DDL`). They are not covered by semver, they do not
appear in the enforced report and they will be removed from the root entry
point in the next major release. Public signatures may still mention them; you
never need to name them in application code.

Duplicate names kept for compatibility and marked `@deprecated`:

| Deprecated | Use instead | Notes |
| --- | --- | --- |
| `DataFrameExpressionBuilder` | `EBuilder` | Structural `{ build }` shape accepted by the untyped DataFrame overloads; every expression helper already returns an `EBuilder`. |
| `WindowFrameBoundary` | `FrameBoundary` | Same type; the window frame boundary is declared once now. |
| `AggregationExprThunk` | `TypedColumnExprThunk` | Both are `(EX: ExprAlg<E>) => E`; both are `@internal`. |

`ExpressionCaseChain` (returned by `when()`) and `TypedCaseChain` (returned by
`typedWhen()`) are different types, untyped versus schema-aware, and both stay
public.
