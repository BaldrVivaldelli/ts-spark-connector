# AGENTS.md — ts-spark-connector

## What this repo is
TypeScript client for Spark Connect. Prefer small, backwards-compatible changes.

## Setup (local)
- Node: 22/24 (CI uses Linux)
- Install:
    - `npm ci`

## Spark Connect server (required for integration/e2e)
Run Compose from the repository root:
- TLS (default): `docker compose up -d --build spark`
- Plain local override: `docker compose -f docker-compose.yml -f docker-compose.plain.yml up -d --build spark`

The plaintext profile is for isolated local debugging only. Do not use auth
credentials with it. Docker E2E always uses TLS.

## How to run
- Build: `npm run build`
- Verify TypeScript 7: `npm run check:typescript`
- Tests: `npm test`
- Test typecheck: `npm run typecheck:test`
- E2E against an already-running server: `npm run test:e2e`
- Full Docker E2E: `npm run test:docker`
- Publishable tarball smoke test: `npm run test:package`
- Examples live in `docs/examples/` (prefer updating/adding an example when adding a feature)
- Run examples with `npm run examples:run -- <name>`; they compile with
  TypeScript 7 and import only the public package entry point.

## Coding rules
- Keep public APIs stable; avoid breaking changes unless necessary and documented.
- Prefer adding/adjusting unit + integration tests when changing DataFrame ops or column expressions.
- When editing README examples, imports should be from the package name (not `./src/...`).

## Compatibility
- Spark Connect protocol descriptors remain pinned to 4.0.4; E2E covers server
  versions 4.0.0 and 4.0.4.
- Do not claim another version until its full behavior suite is green in the CI
  compatibility matrix.
- TypeScript 7 is the build compiler. The aliased TypeScript 6 package exists
  only for tools that still require the legacy programmatic compiler API.

## Development TLS fixtures
- Renew: `./spark-server/certs/generate-dev-certs.sh`
- Validate: `./spark-server/certs/generate-dev-certs.sh --check`
- Commit `ca.crt`, `cert.crt`, and `keystore.p12` together after renewal.
- Never add loose private PEM keys or OpenSSL intermediates to the repo.
- The committed CA/keystore/password are development-only public fixtures and
  must never be reused in production.

## Release process
- Repo uses semantic-release. Use Conventional Commits:
    - `fix: ...` / `feat: ...` / `docs: ...` etc.
- Do not manually bump versions in commits unless the release tooling requires it.
