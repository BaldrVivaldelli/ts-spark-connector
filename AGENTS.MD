# AGENTS.md — ts-spark-connector

## What this repo is
TypeScript client for Spark Connect. Prefer small, backwards-compatible changes.

## Setup (local)
- Node: 18/20/22 (CI uses Linux)
- Install:
    - `npm ci`

## Spark Connect server (required for integration/e2e)
Use the repo Docker setup:
- `cd spark-server`
- `docker compose up -d`

## How to run
- Build: `npm run build`
- Unit tests: `npm test`
- E2E tests (need the Docker Spark server up): `npm run test:e2e`
- Examples live in `docs/examples/` (prefer updating/adding an example when adding a feature)

## Coding rules
- Keep public APIs stable; avoid breaking changes unless necessary and documented.
- Prefer adding/adjusting unit + integration tests when changing DataFrame ops or column expressions.
- When editing README examples, imports should be from the package name (not `./src/...`).

## Compatibility
- Target Spark Connect 4.0.x (the bundled Docker server and CI run Spark 4.0.0) unless explicitly expanding support.

## Release process
- Repo uses semantic-release. Use Conventional Commits:
    - `fix: ...` / `feat: ...` / `docs: ...` etc.
- Do not manually bump versions in commits unless the release tooling requires it.
