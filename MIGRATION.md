# Migration guide

This guide covers the canary following `1.12.x`, including the Spark 4.0.4
baseline and the TypeScript 7 build-tool migration.

## Install the canary

Use the `next` dist-tag while evaluating the release:

```bash
npm install ts-spark-connector@next
```

Pin the exact version reported by `npm view ts-spark-connector@next version`
when reproducible installations are required. Do not promote a canary to
production solely because it installs: review its published E2E and canary
observation artifacts first.

## Runtime requirements

- Node.js 22 or 24 is required.
- Spark Connect 4.0.4 is the default and protocol baseline.
- Spark Connect 4.0.0 remains behaviorally tested for backwards compatibility.
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

## Canary-to-stable gate

A stable release is allowed only after:

1. CI passes on Node.js 22 and 24.
2. The full E2E suite and an installed-package example pass on Spark 4.0.0 and
   4.0.4.
3. JUnit, JSON and Markdown evidence is published for both Spark jobs.
4. The `next` package passes registry integrity, CJS, ESM, TypeScript 7
   declaration and production-audit checks.
5. The canary has been available for at least 24 hours.
6. The canary's `gitHead` is an ancestor of the stable candidate.

If any signal fails, leave the stable release blocked, fix the issue on
`next`, and publish a newer canary.
