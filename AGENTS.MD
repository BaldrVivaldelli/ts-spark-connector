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
The default container terminates TLS with HAProxy on port 15002 and keeps
Spark's plaintext backend loopback-only on port 15003.

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
- Spark Connect protocol descriptors are vendored from 4.2.0, the newest line
  in the E2E matrix (server versions 3.5.6, 4.0.0, 4.0.4, 4.1.3 and 4.2.0).
  Anything that needs a newer server than 3.5 must be gated so older servers
  fail with `UnsupportedServerVersionError` before the request is sent: plan
  fields in `VERSIONED_FIELDS` (`src/client/serverCompatibility.ts`), other
  calls with `session.requireServerVersion(minimum, feature)`.
- Do not claim another version until its full behavior suite is green in the CI
  compatibility matrix.
- TypeScript 7 is the build compiler. The aliased TypeScript 6 package exists
  only for tools that still require the legacy programmatic compiler API.

## Secrets and sensitive data
The repository, its pull requests and its CI logs are public. Treat as
sensitive, and keep out of commits, PR and issue text, CI logs, test output
and anything you print while working:
- Credentials: Spark Connect tokens, `x-aws-proxy-auth` session tokens, AWS
  access keys and session tokens, npm and GitHub tokens, passwords, private
  keys.
- AWS identifiers: account ids, ARNs (they embed the account id), resource
  and session ids, SSO start URLs, profile names, work email addresses.
  Redact them (`XXXXXXXXXXXX`) when output has to be shown.

Rules:
- Fixtures use AWS's documentation placeholders (account `111122223333`, key
  `AKIAIOSFODNN7EXAMPLE`) and obviously fake tokens, never real values.
- Credentials reach the connector only through `withAuth()` (custom headers
  through a `provider`), never through URLs, error messages or log lines.
  Anything that prints or serializes connection settings goes through
  `redactForTelemetry` / `redactWhenPrinted` (`src/client/telemetry.ts`); add a
  case to `test/secret-redaction.unit.test.ts` for any new path that formats
  settings or errors.
- In CI, AWS identifiers are repository secrets (masked in logs), never
  variables, and no step echoes them.
- When running against a real AWS account, pass ids through environment
  variables read from stack outputs without echoing them, and delete local
  logs that captured them.
- Every commit passes the gitleaks pre-commit hook (`.githooks/pre-commit`,
  enabled by `npm ci`), and CI's `Secret scan` job gates releases. Never
  commit with `--no-verify`; `npm run secrets:scan` scans the full history.
  Details in CONTRIBUTING.md, "Keeping secrets out of the repository".

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
- `main` is the only release branch: once the unit (Node 22/24),
  publishable-tarball and Spark E2E jobs are green on a push to `main`,
  semantic-release publishes to npm with provenance and tags the release, then
  `scripts/observe-release.mjs` installs the exact published version and
  re-checks it. `latest` is the only dist-tag that receives releases.
