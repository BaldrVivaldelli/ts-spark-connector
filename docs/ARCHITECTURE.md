# Architecture and contributor guide

This guide explains how a DataFrame call becomes a Spark Connect request, and
lists what a change needs before it can merge. For the parity status with
PySpark and the open work, see [PARITY.md](PARITY.md).

## From a DataFrame call to a gRPC request

```text
df.filter(col("age").gt(30)).select("name").collect()
        │                                     │
        ▼                                     ▼
 ReadChainedDataFrame                   action: interpret the program
 (composes a DFProgram,                 with the proto interpreter and
  tracks the schema type S)             execute it
        │                                     │
        ▼                                     ▼
 DFProgram = (DF: DFAlg, EX: ExprAlg) => R    ProtoDFAlg / ProtoExprAlg
        │                                     │  (Spark Connect messages)
        │                                     ▼
        │                               SparkConnectExecutor
        │                               (ExecutePlan request)
        │                                     │
        │                                     ▼
        │                               sparkGrpcClient + reattachable
        │                               execution (gRPC, retries, release)
        ▼
 other interpreters of the same program:
   SparkDFAlg  → client-side logical plan JSON (toSparkLogicalPlanJSON)
   TraceDFAlg  → AST JSON / Mermaid (toClientASTJSON, toClientASTMermaid)
```

The DataFrame never builds a data structure itself. Every method appends a
step to a *tagless-final program*: a function from an algebra of operations to
some result type. Interpreting the same program with different algebras gives
the protobuf plan, the inspection formats and the trace, so all of them always
describe the same query.

### Layers

| Layer | Where | Role |
| --- | --- | --- |
| Algebras | `src/algebra/read`, `src/algebra/write` | `ExprAlg<E>` (expressions), `DFAlg<R, E, G>` = `DFCore` + capability interfaces (`CacheCap`, `RepartitionCap`, streaming caps, …), writer algebras. Interfaces only. |
| Untyped builders | `src/engine/column.ts` | `col`, `lit`, `when`, `call`, … return an `EBuilder`: a thunk `build(EX)` over any `ExprAlg`. |
| Typed builders | `src/typed/*` | `TypedColumn`, `NumericColumn`, `Condition`, typed functions and the `AggFactory` used by `agg(f => [...])`. They carry the column type and nullability in TypeScript. |
| DataFrame | `src/read/readChainedDataFrame.ts` | Public surface. `chain`/`chainAs` compose the program; `S` is a phantom schema type, `runtimeSchema` its runtime twin used to decode Arrow rows (`src/read/runtimeSchema.ts`, `src/schema/*`). |
| Interpreters | `src/engine/compilerRead.ts` (proto), `src/read/readDataFrameInterpreter.ts` (logical plan), `src/trace/trace.ts` (trace), `src/write/compilerWrite.ts` (writes) | One implementation of each algebra per output format. |
| Client | `src/client/*` | `SparkSession` and its builder, request building (`sparkConnectExecutor.ts`), gRPC channel cache and RPCs (`sparkClient.ts`), reattachable execution, retries, errors, telemetry, credentials, catalog, per-execution options and server-version gating. |
| Arrow | `src/typed/arrow-rows.ts`, `src/typed/arrow-encode.ts` | Decode result batches into rows; encode `createDataFrame` rows. |
| Protocol | `proto/spark/connect/*.proto` | Vendored Spark 4.2.0 descriptors, loaded at runtime with `@grpc/proto-loader` and `keepCase: true`, so request objects use the proto's snake_case field names. |

## Adding an operation, end to end

The same checklist applies to a DataFrame method, a column function or a
session API; skip the steps that do not apply.

1. **Find the protocol message.** Look it up in `proto/spark/connect/`
   (`relations.proto` for DataFrame operations, `expressions.proto` for
   column expressions, `commands.proto` for writes and DDL, `base.proto` for
   RPCs and AnalyzePlan). Check the [PySpark Connect
   client](https://github.com/apache/spark/tree/master/python/pyspark/sql/connect)
   to see how Spark itself fills it.
2. **Extend the algebra.** Add the operation to `DFCore`, to a capability
   interface in `src/algebra/read/batch-capabilities.ts`, or to `ExprAlg`.
   Most column functions need no new algebra method: `EX.call(name, args)`
   emits an `UnresolvedFunction`, which is how Spark resolves almost every
   SQL function.
3. **Implement it in every interpreter.** `ProtoDFAlg`/`ProtoExprAlg`
   (field names exactly as in the `.proto`), `SparkDFAlg`/`SparkExprAlg` plus
   the `LogicalPlan`/`Expression` variant in `src/engine/logicalPlan.ts`, and
   `TraceDFAlg`/`TraceExprAlg`. TypeScript enforces this: an algebra method
   without an implementation does not compile.
4. **Expose it.** Add the public method to `ReadChainedDataFrame` (or the
   builder, session or writer) using `chain` when the schema is unchanged and
   `chainAs` with a schema transform when it changes. Validate arguments
   eagerly with the helpers in `src/read/dataframeValidation.ts`, so mistakes
   fail at the call site rather than as a remote Spark error.
5. **Gate newer protocol features.** If the message or field only exists on
   Spark 4.1 or newer, call `session.requireServerVersion("4.1.0", "feature()")`
   before sending it. Older servers then fail with
   `UnsupportedServerVersionError` without any RPC.
6. **Test it.**
   - Unit tests that assert the emitted proto (see
     `test/compilerRead.unit.test.ts`).
   - A wire-conformance case in `test/wire-conformance.test.ts` (or
     `test/write-wire-conformance.test.ts`): the plan is serialized with the
     vendored descriptor and decoded again, so a misspelled field, which
     protobuf would silently drop, fails the test.
   - Type tests in `test/typed/*.type.test.ts` when the method has typed
     overloads or changes the schema type.
   - An E2E test against a real server in `test/*.e2e.test.ts` that asserts
     values, not only that the call succeeds.
   - Coverage is enforced at 100 %; cover behaviour, not lines (see the
     coverage policy in [TESTING.md](../TESTING.md)).
7. **Document it.** Run `npm run api:report` and commit the updated
   `etc/ts-spark-connector.public.api.md`; mark new exports `@public` (or
   `@internal`) in TSDoc. Update the README scope, add or extend an example in
   `docs/examples/` (imports from the package root only) and tick the item in
   [PARITY.md](PARITY.md).
8. **Run the gate.** `npm run check`, then the Docker E2E suite
   (`npm run test:docker`, or the per-version commands in
   [TESTING.md](../TESTING.md)) on at least 4.0.4 and 4.2.0.

Commit with Conventional Commits (`feat(dataframe): ...`, `fix(client): ...`);
semantic-release derives the next version and the changelog from them.

## Conventions worth knowing

- **Strings are column names, except on the right of a comparison.**
  `call("upper", ["name"])` reads the column `name`, while
  `col("country").eq("AR")` compares with the literal `"AR"`, as in PySpark.
- **Integers become `integer` literals when they fit in int32**, otherwise
  `long`; non-integers become `double`; `bigint` is always `long`.
- **Actions snapshot their options** (`ExecutionOptions`, session tags) when
  they start, before the first `await`.
- **Plans are interpreted once per action.** Binary operations receive the
  already interpreted left plan from `chainAs`; never re-run `this.prog`
  inside a step, or a chain of N joins costs 2^N interpretations.
