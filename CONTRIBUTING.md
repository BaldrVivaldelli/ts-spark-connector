# Contributing

Thanks for considering a contribution to **ts-spark-connector**!

## Setup

1. Clone the repo and install dependencies:

   ```bash
   git clone https://github.com/BaldrVivaldelli/ts-spark-connector
   cd ts-spark-connector
   npm ci
   ```

## Where to start

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) explains how a DataFrame call
  becomes a Spark Connect request and lists everything a new operation needs,
  from the algebra to the E2E test.
- [docs/PARITY.md](docs/PARITY.md) tracks the PySpark API surface: every
  unchecked item is an open, self-contained contribution. Issues labelled
  `good first issue` point at the smallest ones.
- `npm run docs:api` builds the TypeDoc API reference into `docs-site/`.

## Development

- Code style: TypeScript + ESLint (`npm run lint`)
- Tests: Vitest (`npm test` for unit tests)
- Build: TypeScript 7 (`npm run build`)
- Executable examples: `npm run examples:run -- join`
- Function library: `functions` is generated from `scripts/functions/catalog.mjs`;
  edit the catalog (or `src/functions/special.ts` for hand-written functions)
  and run `npm run functions:generate`

## Testing

### Unit Tests

```bash
npm test
```

### E2E Tests (Recommended: Docker-based)

For complete end-to-end testing with all dependencies:

```bash
# Run all E2E tests in Docker environment
npm run test:docker

# Clean up afterwards
npm run test:docker:cleanup
```

### E2E Tests (Manual Spark Setup)

Alternatively, you can manually start Spark and run tests:

```bash
# Start Spark server
docker compose up -d --build spark

# Run E2E tests
npm run test:e2e

# Clean up
docker compose down
```

📖 **For detailed testing instructions, see [TESTING.md](./TESTING.md)**

The E2E matrix currently covers Spark Connect 4.0.0, 4.0.4, 4.1.3 and 4.2.0
over TLS. Other versions are not claimed as supported until their full behavior
suite is green.

## Maintainer access to npm

npm only issues granular access tokens now (90 days at most, scoped to
packages and permissions). Keep yours out of `~/.npmrc` and out of `.env`
files: `scripts/npm-auth.mjs` stores it in the OS keyring, the same Secret
Service `gh` uses, and runs npm with it only when asked.

```bash
node scripts/npm-auth.mjs login --expires 2026-12-31   # paste the token; it is verified with npm whoami
node scripts/npm-auth.mjs status                        # user, validity and days left
node scripts/npm-auth.mjs run dist-tag ls ts-spark-connector
node scripts/npm-auth.mjs ci-secret                     # copies it into the NPM_TOKEN Actions secret
node scripts/npm-auth.mjs logout
```

Create the token on npmjs.com with the smallest scope the task needs
(read-only unless you publish). Releases from CI should move to npm trusted
publishing (OIDC) rather than depend on a token at all.

## Pull Requests

- Create a branch from `main` and open a PR
- Keep PRs small and focused
- Include tests and docs updates when applicable
- Use a Conventional Commit title for the PR; squash merges use that title as
  the release commit
- Do not edit `CHANGELOG.md` or package versions manually; Semantic Release
  updates both after the tested commit reaches `main`

## Commit Messages

Conventional Commits are required because they determine the next package
version and release notes:

```text
feat(read): add schema-aware JSON reads
fix(transport): release a failed streaming operation
docs: clarify TLS setup
```

Use `feat!:` or a `BREAKING CHANGE:` footer only for intentional breaking
changes. Before opening a PR, run:

```bash
npm run check
npm run test:package
```

`npm run check:typescript` verifies that the native TypeScript 7 compiler is
active. The TypeScript 6 package is retained only as the programmatic API
compatibility layer required by the current `typescript-eslint` release.

Releases are published from `main` only. Once a PR with a Conventional Commit
title is merged and the unit (Node.js 22 and 24), publishable-tarball and
Spark E2E jobs are green, semantic-release publishes the package to npm with
provenance and tags the release; the workflow then installs that exact version
from the registry and verifies it. A red job blocks the publish until a fix
lands on `main`; see the release flow in [MIGRATION.md](./MIGRATION.md).

## Code of Conduct

By participating, you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).
