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

- Code style: TypeScript + ESLint + Prettier
- Tests: Vitest (`npm test` for unit tests)
- Build: `npm run build`

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
npx vitest run "test/**/*.e2e.test.ts"

# Clean up
docker compose down
```

📖 **For detailed testing instructions, see [TESTING.md](./TESTING.md)**

## Pull Requests

- Create a branch from `main` and open a PR
- Keep PRs small and focused
- Include tests and docs updates when applicable
- Update `CHANGELOG.md`

## Commit Messages

Use clear, descriptive messages. If you prefer conventional commits, we accept them.

## Code of Conduct

By participating, you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).
