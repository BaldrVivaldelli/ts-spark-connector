# Contributing

Thanks for considering a contribution to **ts-spark-connector**!

## Setup

1. Clone the repo and install dependencies:

   ```bash
   git clone https://github.com/BaldrVivaldelli/ts-spark-connector
   cd ts-spark-connector
   npm ci
   ```

## Development

- Code style: TypeScript + ESLint (`npm run lint`)
- Tests: Vitest (`npm test` for unit tests)
- Build: TypeScript 7 (`npm run build`)
- Executable examples: `npm run examples:run -- join`

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

The E2E gate currently covers Spark Connect 4.0.0 and 4.0.4 over TLS. Other
versions are not claimed as supported until their full behavior suite is green.

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

Canaries are published from `next`. Do not promote one to `main` until both
Spark E2E artifacts are green and the registry observation has completed its
24-hour soak; see [MIGRATION.md](./MIGRATION.md).

## Code of Conduct

By participating, you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).
