# Contributing

Thanks for considering a contribution to **ts-spark-connector**!

## Setup

1. Clone the repo and install dependencies:

   ```bash
   git clone https://github.com/BaldrVivaldelli/ts-spark-connector
   cd ts-spark-connector
   npm ci
   ```

2. (Optional) Start a local Spark Connect server (see `spark-server/README.md`) and export:

   ```bash
   export SPARK_CONNECT_URL=sc://localhost:15002
   ```

## Development

- Code style: TypeScript + ESLint + Prettier
- Tests: Vitest (`npm test`); E2E require a running Spark Connect server
- Build: `npm run build`

Run E2E tests:

```bash
docker compose up -d --build spark
npx vitest run "test/**/*.e2e.test.ts"
```

## Pull Requests

- Create a branch from `main` and open a PR
- Keep PRs small and focused
- Include tests and docs updates when applicable
- Update `CHANGELOG.md`

## Commit Messages

Use clear, descriptive messages. If you prefer conventional commits, we accept them.

## Code of Conduct

By participating, you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).
