# Testing

The root build exposes a default test aggregate plus package-scoped and
special-purpose test tiers.

## Default Tests

Run the default test suite from the repository root:

```sh
zig build test
```

`zig build test` depends on the default package aggregates:

- `zig build antfly-test`
- `zig build inference-test`

The default aggregate is intended to be the normal local and CI confidence
target. It does not fetch external corpora and does not run benchmark or soak
targets.

## CI Tiers

Pull-request CI runs fast required checks:

- `zig build check-snowball`
- `zig build unit-test`
- `zig build -Doptimize=ReleaseFast install -Dedition=full`
- shared release-binary smoke checks
- `e2e-base`
- TLA checks when relevant files change

Merge queue and `main` push CI also run the default aggregate:

```sh
zig build test
```

Nightly/manual validation should use broader checks such as `e2e-full`,
conformance, and soak suites. This workflow currently wires `e2e-full`; the
other broad suites are not required for every merge.

## Package Aggregates

Run only the Antfly default aggregate:

```sh
zig build antfly-test
```

`antfly-test` includes:

- `unit-test`
- `sim-test`
- `integration-test`
- `recall-test`
- the default recall harness over `testdata/vectorsets`
- `chaos-test`

Run only the inference package tests:

```sh
zig build inference-test
```

`inference-test` delegates to `pkg/inference` by running `zig build test` in that
package with the root build's relevant backend options forwarded.

## Antfly Tiers

Run the hermetic unit and focused integration bucket:

```sh
zig build unit-test
```

`unit-test` is where default, no-fetch Antfly, storage, and shared-library unit
coverage belongs. Focused aliases such as `lib-json-test`, `db-test`, and
`wal-test` remain available for narrower iteration, but broad module suites are
wired into `unit-test` once.

Run mocked-time and modeled simulation checks:

```sh
zig build sim-test
```

Run focused real HTTP and public API integration checks:

```sh
zig build integration-test
```

Run bounded generated chaos campaigns:

```sh
zig build chaos-test
```

Run recall checks:

```sh
zig build recall-test
zig build recall-harness
```

## Conformance And Soak

Fetch and run the conformance suites:

```sh
zig build conformance-test
```

`conformance-test` is intentionally outside `zig build test` and may download
or refresh external corpora under local paths such as `/tmp`. It keeps
successful corpus output quiet. Use the suite-specific run-only steps when you
want verbose per-fixture output, or when you want to avoid fetches and use
already present local corpora:

```sh
zig build lib-toon-conformance-run
zig build lib-image-conformance-run
zig build lib-audio-conformance-run
zig build image-jpeg-seed-corpora-e2e-run
```

The suite-specific fetch and fetch-and-run steps remain available:

```sh
zig build lib-toon-conformance-fetch
zig build lib-toon-conformance
zig build lib-image-conformance-fetch
zig build lib-image-conformance
zig build lib-audio-conformance-fetch
zig build lib-audio-conformance
```

Run long-running soak aggregates:

```sh
zig build soak-test
```

`soak-test` is intentionally outside `zig build test`. Use it for deeper local
or scheduled validation, not as the default edit-compile-test loop.

## Python E2E

The Python end-to-end suites live under `e2e/` and are run with `uv`.
They are separate from `zig build test`.

The required CI E2E tier is `e2e-base`: all E2E tests except tests marked as
external-service, model, browser, or slow integration coverage. Run the same
base tier locally with the release binaries:

```sh
zig build -Doptimize=ReleaseFast install -Dedition=full

ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/antfly pytest -q \
  -m "not objectstore_integration and not swarm_integration and not real_model and not postgres_integration and not slow" \
  e2e/antfly

ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/inference pytest -q \
  -m "not slow and not multimodal and not model_integration and not browser_integration" \
  e2e/inference
```

The GitHub `e2e-base` job uses shared scripts so the same path can be run
locally:

```sh
scripts/ci/zig-e2e-base-linux.sh
```

Pass pytest selectors after the script name to run a focused Antfly E2E loop.
Termite E2E is skipped for focused runs unless `RUN_TERMITE_E2E=1` is set:

```sh
SKIP_BUILD=1 scripts/ci/zig-e2e-base-linux.sh \
  e2e/antfly/test_auth.py -k test_stateful_auth_enforces_table_permissions
```

To reproduce the Linux amd64 CI environment locally from macOS or another host,
build and run the CI image:

```sh
docker buildx build --platform linux/amd64 -f zig/Dockerfile.ci \
  -t antfly-zig-ci:local --load .

docker run --rm --platform linux/amd64 \
  -v "$PWD":/workspace \
  -w /workspace \
  antfly-zig-ci:local
```

Focused Docker runs use the same script arguments:

```sh
docker run --rm --platform linux/amd64 \
  -v "$PWD":/workspace \
  -w /workspace \
  antfly-zig-ci:local \
  scripts/ci/zig-e2e-base-linux.sh e2e/antfly/test_auth.py -k auth
```

Useful script controls:

- `SKIP_BUILD=1`: reuse existing `zig/zig-out/bin/antfly`.
- `RUN_TERMITE_E2E=0`: skip Termite base E2E in the default run.
- `ANTFLY_E2E_VENV` and `TERMITE_E2E_VENV`: override the script-managed
  virtualenv paths, which default under `/tmp` so Docker runs do not rewrite
  project-local `.venv` directories.
- `ANTFLY_CI_ZIG_TARGET`, `ANTFLY_CI_ZIG_CPU`, and `ANTFLY_CI_ZIG_OPTIMIZE`:
  override the binary build target, CPU, or optimization mode.

Unmarked E2E tests are expected to be safe for `e2e-base`. Mark tests that
require PostgreSQL, object stores, real model weights, browsers/WebGPU, or
long-running scenarios so they stay in `e2e-full`.

Run the Antfly product E2E suite:

```sh
zig build install -Dedition=full
ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/antfly pytest -q e2e/antfly
```

The Antfly fixtures can also target an already-running service:

```sh
ANTFLY_SERVERLESS_URL=http://127.0.0.1:8080 uv run --project e2e/antfly pytest -q e2e/antfly
ANTFLY_STATEFUL_URL=http://127.0.0.1:8080 uv run --project e2e/antfly pytest -q e2e/antfly/test_schema_migration.py
```

Useful Antfly E2E variants:

```sh
uv run --project e2e/antfly pytest -q e2e/antfly/test_quickstart.py
uv run --project e2e/antfly pytest -q e2e/antfly/test_transactions.py
uv run --project e2e/antfly pytest -q e2e/antfly/test_backup_restore.py
uv run --project e2e/antfly pytest -q e2e/antfly/test_backup_restore.py -m objectstore_integration
```

Common Antfly E2E environment variables:

- `ANTFLY_BIN`: local Antfly binary to auto-start, usually `./zig-out/bin/antfly`.
- `ANTFLY_SERVERLESS_URL`: existing serverless API endpoint.
- `ANTFLY_STATEFUL_URL`: existing stateful API endpoint.
- `ANTFLY_STATEFUL_API_ROOT`: stateful API root override; use `/db/v1` for Go Antfly.
- `ANTFLY_E2E_PRESERVE_ROOT=1`: preserve per-test data roots for debugging.
- `ANTFLY_E2E_ALLOW_REAL_MODEL_DOWNLOAD=1`: allow real model downloads for model-backed tests.

Object-store backup E2E tests are opt-in and skip when their envs are absent.
For S3-compatible backends:

```sh
export OBJECTSTORE_S3_INTEGRATION=1
export OBJECTSTORE_S3_TEST_BUCKET=my-test-bucket
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
```

For GCS:

```sh
export OBJECTSTORE_GCS_INTEGRATION=1
export OBJECTSTORE_GCS_TEST_BUCKET=my-test-bucket
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export GOOGLE_CLOUD_PROJECT=my-project
```

Run the inference product E2E suite:

```sh
zig build install -Dedition=inference
ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/inference pytest -q e2e/inference
```

The inference fixtures can also target an already-running service:

```sh
ANTFLY_INFERENCE_URL=http://127.0.0.1:8080 uv run --project e2e/inference pytest -q e2e/inference
ANTFLY_INFERENCE_URL=https://inference.example.com ANTFLY_INFERENCE_TOKEN=... uv run --project e2e/inference pytest -q e2e/inference
```

Common inference E2E environment variables:

## TypeScript Components

Run the focused components checks through the same helper used for local CI
debugging:

```sh
scripts/ci/ts-components.sh
```

Pass Vitest selectors after the script name when narrowing a failure:

```sh
scripts/ci/ts-components.sh src/Listener.test.tsx -t config
```

Common inference E2E environment variables:

- `ANTFLY_BIN`: local Antfly binary to auto-start via `antfly inference run`.
- `ANTFLY_INFERENCE_URL`: existing inference API endpoint.
- `ANTFLY_INFERENCE_TOKEN`: bearer token for remote inference endpoints.
- `ANTFLY_INFERENCE_MODELS_DIR`: model directory override.
- `ANTFLY_INFERENCE_DOWNLOAD=1`: allow model downloads through `antfly inference pull` when tests request unavailable models.
- `RUN_LARGE_MODEL_TESTS=1`: opt into large-model tests.

Many E2E tests skip cleanly when required binaries, services, local PostgreSQL,
remote object stores, or model files are unavailable.

E2E marker policy:

- `postgres_integration`: requires local PostgreSQL.
- `objectstore_integration`: requires S3 or GCS credentials and buckets.
- `real_model` or `model_integration`: requires local or downloadable model weights.
- `swarm_integration`: requires a local Antfly swarm plus live inference model support.
- `browser_integration`: requires a browser or WebGPU runtime.
- `slow`: too long-running for required E2E base CI.

## Focused Steps

The build still exposes focused steps for narrow iteration. Examples:

```sh
zig build lib-httpx-test
zig build lib-metadata-test
zig build lsm-backend-test
zig build persistent-test
zig build db-test
zig build sparse-test
```

List all available steps with:

```sh
zig build --help
```
