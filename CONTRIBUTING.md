# Contributing

Thanks for considering a contribution to **ts-spark-connector**!

## Setup

1. Clone the repo and install dependencies:

   ```bash
   git clone https://github.com/BaldrVivaldelli/ts-spark-connector
   cd ts-spark-connector
   npm ci
   ```

   `npm ci` also enables the repository's git hooks (`core.hooksPath`
   `.githooks`), whose pre-commit hook scans every commit for secrets; see
   below.

## Keeping secrets out of the repository

The repository, its pull requests and its CI logs are public, and a secret
that reaches a commit stays in the history even after a later commit deletes
it. Several layers stop one before it gets there:

- **Pre-commit hook.** `.githooks/pre-commit` runs
  [gitleaks](https://github.com/gitleaks/gitleaks) on the staged changes and
  blocks the commit when it finds a token, key or password. It uses a
  `gitleaks` binary when one is installed (the hook pins version 8.30.1, see
  `scripts/gitleaks.mjs`) and the official Docker image otherwise; with
  neither it blocks the commit rather than skip the scan. Install gitleaks
  with `brew install gitleaks`, `mise use -g gitleaks@8.30.1` or a
  [release binary](https://github.com/gitleaks/gitleaks/releases).
- **Full scan.** `npm run secrets:scan` checks the whole history, for
  example before making a fork or a branch public.
- **CI.** The `Secret scan` job checks the history and the contents of the
  npm tarball on every pull request and push to `main`; a release cannot
  publish until it passes.
- **GitHub push protection** rejects pushes that contain known token formats.

A finding is never bypassed with `git commit --no-verify`. Remove the secret
and, if it was real, revoke it first: it was exposed the moment it was
written to disk next to the repository. A test fixture that must look like a
credential uses a documented public placeholder (AWS's `111122223333`,
`AKIAIOSFODNN7EXAMPLE`) and, only if gitleaks flags it, gets its exact value
exempted in `.gitleaks.toml`, never a whole file. AWS account ids, ARNs,
resource ids and work email addresses are not secrets to gitleaks, but they
are sensitive here too: [AGENTS.MD](AGENTS.MD#secrets-and-sensitive-data)
lists what to keep out of commits, pull requests and logs.

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

The E2E matrix currently covers Spark Connect 3.5.6, 4.0.0, 4.0.4, 4.1.3 and
4.2.0 over TLS. Other versions are not claimed as supported until their full behavior
suite is green.

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
provenance (through npm trusted publishing, so there is no npm token to manage)
and tags the release; the workflow then installs that exact version
from the registry and verifies it. A red job blocks the publish until a fix
lands on `main`; see the release flow in [MIGRATION.md](./MIGRATION.md).

## Code of Conduct

By participating, you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).
