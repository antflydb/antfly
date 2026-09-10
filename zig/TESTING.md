# Testing

The root build exposes a default test aggregate plus package-scoped and
special-purpose test tiers.

## Build ownership

`build.zig` resolves public options and connects owner constructors. Substantial
library definitions live in `lib/<owner>/build_support.zig`. Antfly's runtime,
embedded artifacts, benchmarks, generators, and test suites live in
`pkg/antfly/build/`; inference constructors live in `pkg/inference/build/` and
serve both package and root entrypoints. Small library definitions stay inline.

Give library constructors an explicit owner path and compatible dependency modules.
Constructors return artifacts and runs; entrypoints publish target names and connect
aggregates. Native and WASM configurations remain separate. Runtime archive boundaries, link order, and
test selections belong to their owners; moving a definition does not change them.

The unified browser runtime in `pkg/antfly/build/wasm.zig` owns a fixed WASM32
ReleaseSafe configuration, including HTTPX, JSON, OpenAPI, and storage modules.
Native optimization, target, and storage flags do not configure those modules.
Native and WASM OpenAPI modules use one wiring constructor with separate module
instances; SentencePiece modules share the same generated source output.

Python process-lifecycle checks run only when Zig's executor resolver identifies a
native target. Foreign Linux/macOS fixtures still compile, and the build summary
reports those process checks as skipped. Ordinary Zig test executables keep Zig's
normal foreign-execution and emulator handling. Run the process suites natively
on Linux/macOS to qualify signal, process-group, and cancellation behavior.

OpenAPI generators run on the host and write formatted Zig files into declared
cache outputs. `make openapi-generate` (from `zig/`) synchronizes those outputs and
the joined public schema into the checked-in trees, including removal of obsolete
generated files. `make openapi-check` compares them without modifying source files.
Both use the normal Zig cache; source files, generator options, Python dependency
locks, and the schema tree supply the inputs. No Git-derived cache key is needed.
Schema joins resolve references to deterministic owner paths and report the files
they read through depfiles. Missing required schemas fail the join; generated
bundles and same-named files elsewhere cannot substitute for them.
The HTTP API embeds the source schemas through a dedicated module with tracked
`@embedFile` inputs. The API kernel and HTTP-serving test/benchmark roots attach
that module explicitly; shared imports and build options carry no schema inputs.
Editing an embedded schema leaves the CLI, distributed, serverless, and inference
runtime archives cached.

Runtime dependency and cache contracts are checked in CI with
`python3 -m unittest tools.test_runtime_cache` from `zig/`, with Zig and uv on
`PATH` and `zig/deps/snowball` initialized (`git submodule update --init
zig/deps/snowball` from the repository root). The fixtures call the real root and
standalone inference compositions and replace expensive runtime, test, benchmark,
and WASM entry bodies. They
keep external modules, options, generated assets, backend inputs, and final link
edges; the inference probe also loads the real inference module. The checks cover:

- All six served schemas invalidate only the API kernel; the other archives can
  build with a schema missing.
- The remote CLI has no transitive tokenizer, storage-engine, or inference
  implementation imports. Tokenizer data and generator changes invalidate their
  local consumers while the CLI remains cached.
- Audio carries no inference options. GPU identities belong to enabled backends;
  CPU builds configure and compile with the disabled Metal `.m` and CUDA `.cu`
  kernel source files absent.
  Enabled fingerprints match an independent implementation of the qualification
  identity format and change when their source inputs change.
- ONNX headers and libraries are dependencies of their consuming modules. A
  missing installation fails those compile/link steps; root and standalone
  inference help and unrelated library targets still work with ONNX enabled.
- Production runtime archives have no transitive VOPR imports. Editing or removing
  VOPR source leaves all five archives cached, while the actual API simulation
  test recompiles on an edit and fails when its dependency is missing. Test
  constructors receive VOPR explicitly; the full Antfly package retains its
  public simulation exports.
- Release versions live in a small `lib/build_info` object. Archives see only a
  stable accessor module; final links attach the object only for version consumers.
  Tests use stable test metadata. Version-only changes leave the five runtime
  archives cached while the resulting binary reports the updated version. The
  unchanged dataset generator compiles, runs, and stays cached across version
  changes; a training-command probe still links and reports the updated version.
  ABI constants remain at their interface declarations.
- Storage options do not invalidate the CLI or inference archives. A real shared
  implementation edit does invalidate its consumers and changes linked behavior;
  an unchanged rebuild reuses compilation and generated outputs.
- Host generators stay cached across product targets and optimization settings.
  HTTPX runtime edits do not invalidate the OpenAPI compiler.
- Every published native artifact's runtime imports match its target and
  optimization profile in both build entrypoints, including benchmark-specific
  profiles. Compiled audio and linalg probes verify Debug and ReleaseFast reach
  the actual library modules and stay cached across version changes. Graph checks
  also cover foreign targets and a separate PDF optimization override.
- WASM runtime imports retain their target and ReleaseSafe profile. Compiled
  HTTPX/JSON probes and the WASM artifact remain cached when native target,
  optimization, or storage options change.
- Schema joins detect removed and restored inputs, new references, and retargeted
  symlinks. Warm outputs match a fresh join; unrelated files do not invalidate it.
- SQL and Snowball checks and regeneration reuse the same final formatted
  outputs. Checks report drift without repairing source files; neither operation
  rewrites cached generator outputs. Schema and Python codegen input changes
  select these generated-source and cache checks in CI.

Tokenizer constructors and SentencePiece generation live under `lib/tokenizer`.
Entrypoints supply the same compatible tokenizer modules to inference and Antfly;
the inference owner receives the modules rather than reconstructing them. GPU
source identities use cached host generation with declared file arguments,
including their original ordered bundle hashing format. There is no eager source
hashing or separately maintained dependency inventory.

Zig still reads relative `.zig` imports inside disabled branches. Consequently,
edits to the Metal/CUDA Zig implementations can invalidate the CPU inference
archive, even though no GPU identity generator runs. Removing that dependency
requires separate backend modules with shared inference types; the cache contract
above applies to the kernel source inputs and build graph, not every source file
inside the inference module.

These checks establish cache boundaries; they do not substitute for production
compilation or runtime tests. For timing comparisons, hold Zig version, target,
optimization, backend flags, job limit, and cache policy fixed. Measure cold,
unchanged, and representative edit builds separately, recording compiler step
status, elapsed/CPU time, and peak RSS. A smaller import graph primarily improves
incremental invalidation; the expensive runtime code still dominates clean builds.

## Default Tests

Run the default test suite from the repository root:

```sh
make zig-test
```

`zig build test` depends on the default package aggregates:

- `zig build lib-test`
- `zig build antfly-test`
- `zig build inference-test`
- `zig build inference-finetune-test`

The default aggregate is intended to be the normal local and CI confidence
target. It does not fetch external corpora and does not run benchmark or soak
targets.

## CI Tiers

Pull-request CI runs fast required checks:

- `zig build check-snowball`
- `make zig-unit-test`
- `zig build -Doptimize=ReleaseFast antfly`
- shared release-binary smoke checks
- `e2e-base`
- TLA checks when relevant files change

Merge queue and `main` push CI also run the default aggregate:

```sh
make zig-test
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

- `antfly-unit-test`
- `vopr-test`
- `antfly-integration-test`
- `antfly-recall-test`
- the default recall harness over `testdata/vectorsets`
- `antfly-chaos-test`

Run only the inference package tests:

```sh
zig build inference-test
```

`inference-test` uses the same constructors as `zig build test` inside
`pkg/inference`. Inference and platform checks participate directly in the root
build graph, so the root scheduler, memory budget, and build runner govern them.
Root inference targets use the root's resolved backend options and shared modules;
package-local commands retain the standalone defaults. Runtime arguments follow
`--`, and package-relative fixtures run with an explicit package working directory.

Root inference installs use `zig/zig-out` (or the root `--prefix`). Package-local
installs continue to use `pkg/inference/zig-out`. The root default install remains
Antfly and its assets; constructing inference targets does not build or install
the standalone inference CLI.

## Antfly Tiers

Run the hermetic unit and focused integration bucket:

```sh
make zig-unit-test
```

`antfly-unit-test` owns Antfly unit and focused integration coverage. `lib-test`
owns the existing default standalone library checks, including platform tests.
The repository's `make zig-unit-test` gate runs `lib-test antfly-unit-test
inference-test inference-finetune-test`, preserving its previous coverage.
Use `zig build antfly-unit-test` for Antfly alone. All `zig build` commands here
run from `zig/`; `lib-*` names refer to code in `zig/lib/`, and `antfly-*` names
follow the subsystem path beneath `pkg/antfly/src/`.

`antfly-storage-db-test` uses the focused DB root, whose inventory contains all
former full-root DB and result-shape selections plus its own unique cases. It
runs that union once. Focused subsystem suites remain available for iteration.

Enable progress labels on the same run nodes with `-Dtest-progress=true`; this
changes neither the selected tests nor scheduling. Regressions formerly exclusive
to the progress shortcut now belong to the metadata and data-storage suites.

The single `antfly-storage-db-enrichment-test` compiles the former selections once.
Narrow its curated suite with runtime filters; unmatched selections fail:

```sh
zig build antfly-storage-db-enrichment-test -- --test-filter "split cutover"
zig build antfly-storage-db-enrichment-test -- --list-tests
```

The aggregate Make targets reserve 20% memory headroom and use the patched Zig
0.16 build runner so ready steps retain their declared RSS reservations. Set
`ANTFLY_ZIG_MAX_RSS` to an explicit byte count to override the detected budget.
From the `zig/` directory, the equivalent targets are `make test` and
`make unit-test`.

Run fast deterministic VOPR checks, including production HTTP on `VoprIo`:

```sh
zig build vopr-test
```

Run focused real HTTP and public API integration checks:

```sh
zig build antfly-integration-test
```

Run bounded generated chaos campaigns:

```sh
zig build antfly-chaos-test
```

Run recall checks:

```sh
zig build antfly-storage-vectorindex-recall-test
zig build recall-harness && ./zig-out/bin/recall_harness
```

## Conformance And Soak

Conformance targets fetch missing external fixtures, reuse cached corpora, and run
the suite. Setup failures fail the target. These suites remain opt-in; ordinary
library tests do not download external corpora.

```sh
zig build conformance-test
zig build lib-toon-conformance
zig build lib-image-conformance
zig build lib-audio-conformance
```

Library suite names follow `lib-<library>-conformance`, alongside
`lib-<library>-test` and `lib-<library>-bench`.

Fixtures are cached under `/tmp` by default. Use
`-Dconformance-fixtures=/absolute/path` to choose another cache directory.
For offline runs, disable fixture fetching explicitly (missing fixtures fail):

```sh
zig build conformance-test -Dconformance-fetch=false -Dconformance-fixtures=/absolute/path
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
zig build -Doptimize=ReleaseFast antfly

ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/antfly pytest -q \
  -m "not objectstore_integration and not standalone_integration and not real_model and not postgres_integration and not slow" \
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
Inference E2E is skipped for focused runs unless `RUN_INFERENCE_E2E=1` is set:

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
- `RUN_INFERENCE_E2E=0`: skip inference base E2E in the default run.
- `ANTFLY_E2E_VENV` and `ANTFLY_INFERENCE_E2E_VENV`: override the script-managed
  virtualenv paths, which default under `/tmp` so Docker runs do not rewrite
  project-local `.venv` directories.
- `ANTFLY_CI_ZIG_TARGET`, `ANTFLY_CI_ZIG_CPU`, and `ANTFLY_CI_ZIG_OPTIMIZE`:
  override the binary build target, CPU, or optimization mode.

Unmarked E2E tests are expected to be safe for `e2e-base`. Mark tests that
require PostgreSQL, object stores, real model weights, browsers/WebGPU, or
long-running scenarios so they stay in `e2e-full`.

Run the Antfly product E2E suite:

```sh
zig build antfly
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
zig build antfly
ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/inference pytest -q e2e/inference
```

The inference fixtures can also target an already-running service:

```sh
ANTFLY_INFERENCE_URL=http://127.0.0.1:8080 uv run --project e2e/inference pytest -q e2e/inference
ANTFLY_INFERENCE_URL=https://inference.example.com ANTFLY_INFERENCE_TOKEN=... uv run --project e2e/inference pytest -q e2e/inference
```

Common inference E2E environment variables:

- `ANTFLY_BIN`: local Antfly binary to auto-start via `antfly inference run`.
- `ANTFLY_INFERENCE_URL`: existing inference API endpoint.
- `ANTFLY_INFERENCE_TOKEN`: bearer token for remote inference endpoints.
- `ANTFLY_INFERENCE_MODELS_DIR`: model directory override.
- `ANTFLY_INFERENCE_DOWNLOAD=1`: allow model downloads through `antfly
  inference pull` when tests request unavailable models.
- `RUN_LARGE_MODEL_TESTS=1`: opt into large-model tests.
- `RUN_MULTIMODAL_GENERATOR_TESTS=1`: require the shipped multimodal generator
  to be advertised, loadable, and able to complete live image and audio
  inference.
- `RUN_CLIPCLAP_CONTRACT_TESTS=1`: require the published ClipClap GGUF pair to
  pass strict server admission and complete live image and audio embedding.

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

Many E2E tests skip cleanly when required binaries, services, local PostgreSQL,
remote object stores, or model files are unavailable.

E2E marker policy:

- `postgres_integration`: requires local PostgreSQL.
- `objectstore_integration`: requires S3 or GCS credentials and buckets.
- `real_model` or `model_integration`: requires local or downloadable model weights.
- `standalone_integration`: requires a local Antfly standalone plus live inference model support.
- `browser_integration`: requires a browser or WebGPU runtime.
- `slow`: too long-running for required E2E base CI.

## Focused Steps

The build still exposes focused steps for narrow iteration. Examples:

```sh
zig build lib-httpx-test
zig build antfly-metadata-test
zig build lsm-backend-test
zig build persistent-test
zig build antfly-storage-db-test
zig build sparse-test
```

List all available steps with:

```sh
zig build --help
```

## DB analytics and planner coverage

Algebraic behavior uses the normal DB, API, metadata, and graph owner suites.
`antfly-storage-db-test` includes the planner-ownership regression, and
`antfly-unit-test` retains the dynamic-template/cardinality-cache selections.
There are no separate algebraic test or guardrail targets. Benchmark sweeps use
`scripts/run_db_query_matrix.py --suite analytics`; see [BENCHMARKS.md](BENCHMARKS.md#analytics-comparisons).
