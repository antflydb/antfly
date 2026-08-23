# antfly-zig

`antfly-zig` is the Zig monorepo for AntflyDB and the inference
runtime. The repository contains product packages, shared libraries, benchmark
harnesses, compatibility suites, and Python end-to-end tests that exercise the
same checked-in source tree.

## Repository Layout

```text
pkg/
  antfly/            AntflyDB server, API, metadata, storage, search, raft
  antfly-client/     Zig client package
  antfly-embedded/   Embedded Antfly package and WASM smoke surface
  inference/         Inference runtime, OpenAPI server, tools, web UI
  inference-client/  Zig inference client package

lib/
  audio/             Shared audio decode and PCM boundary
  image/             Shared image decode/encode/preprocess boundary
  raft/              Reusable raft library
  httpx/             HTTP client/server helpers
  objectstore/       Object storage abstraction
  openapi/           OpenAPI code generator
  vectorindex/       Vector search primitives
  ...                Other shared Zig libraries used by pkg/*

bench/
  full_text/         Full-text indexing/query/codec benchmarks
  graph/             Graph-pattern latency and demand-working-set benchmarks
  storage/           LMDB, LSM, WAL, replay, open, and storage-path benches
  vectors/           Dense, HBC, RaBitQ, recall, and sparse vector benches
  baselines/         Checked benchmark baseline JSONL outputs

e2e/
  antfly/            Antfly product-level pytest suite
  inference/         Inference product-level pytest suite

compat/              Shared compatibility corpus and Go comparison harnesses
specs/               Formal specifications and model-checking inputs
scripts/             Repository tooling scripts
tools/               Developer tools
testdata/            Shared checked-in fixture data
```

Root-level Markdown files are design and operating notes for active AntflyDB
areas. Library-specific design docs live next to their libraries, for example
`lib/image/IMAGE.md` and `lib/audio/AUDIO.md`. Inference-specific design docs
currently live under `pkg/inference/`.

## Build Requirements

- Zig `0.16.0` or newer.
- `uv` for Python e2e suites and repository helper scripts.
- Optional native runtime dependencies for some inference features, such as MLX,
  ONNX Runtime, FFmpeg, or platform GPU support. The build detects available
  local support and exposes flags such as `-Dmlx=...`, `-Dmetal=...`, and
  `-Donnx=...`.

## Common Builds

```sh
zig build
make test
zig build antfly
./zig-out/bin/antfly --help
```

The inference runtime also has a package-local build file. From the repository root, use the
delegated root steps when possible:

```sh
zig build inference-run
zig build inference-test
zig build inference-wasm
zig build inference-bench-linalg
zig build inference-bench-audio
```

For package-local inference work:

```sh
cd pkg/inference
zig build -Dshared-lib-root=../..
zig build test -Dshared-lib-root=../..
```

## Tests

The default Zig test target runs unit, simulation, chaos, and checked recall
coverage:

```sh
make test
```

Focused targets are useful while iterating:

```sh
make unit-test
zig build lib-db-test
zig build lib-storage-test
zig build lib-metadata-test
zig build lib-image-test
zig build lib-audio-test
zig build lib-raft-sim-test
zig build inference-test
```

The Make targets run aggregate tests with the patched Zig 0.16 scheduler and an
RSS budget of 80% of the detected cgroup or host memory. Set
`ANTFLY_ZIG_MAX_RSS` to an explicit byte count when a smaller local budget is
needed. From the repository root, use `make zig-test` or `make zig-unit-test`.

The Python e2e suites are split by product:

```sh
scripts/ci/zig-antfly-e2e-pytest.sh e2e/antfly
uv run --project e2e/inference pytest -q e2e/inference
```

The Antfly runner uses up to four pytest workers, schedules historically slow
independent tests first, and keeps shared fixtures on one worker. Antfly process
and cluster work has a separate concurrency budget of two. Set
`ANTFLY_E2E_WORKERS` and `ANTFLY_E2E_PROCESS_SLOTS` to tune those limits, or set
`ANTFLY_E2E_WORKERS=1` for a sequential debugging run. Test duration history is
stored under `e2e/antfly/.pytest_cache` locally and in the ARC cache in CI.
Session-scoped process fixtures retain their process slot for the lifetime of
the worker; transient function- and module-scoped fixtures release theirs after
their queued work completes.
The scheduler infers process ownership and fixture scope for the standard E2E
fixtures. Custom fixtures can place
`@e2e_resource("antfly_process")` from `e2e_scheduler` immediately below
`@pytest.fixture`; the fixture's effective scope determines its isolation
boundary, including indirect parametrization scope overrides. Package-scoped
fixtures stay on one worker for their package, while a session-scoped fixture
automatically retains a process slot for the worker's lifetime. Inferred
session identities are fixture-definition-specific; add
`lifetime="session", identity="shared_runtime"` on wrapper fixtures that share a
longer-lived process. Tests that launch a process directly can use
`@pytest.mark.e2e_resource("antfly_process")`. Tests with unusual sharing
requirements can use
`@pytest.mark.e2e_isolation("test")` or `@pytest.mark.e2e_isolation("module")`.

Some e2e tests start local binaries from `zig-out/bin`; build the relevant
binary first when running those tests directly:

```sh
zig build antfly
(cd pkg/inference && zig build -Dshared-lib-root=../..)
```

Model-backed inference tests may require local model fixtures or environment
configuration. The suite keeps those tests skippable when the required assets
are not present.

## Benchmarks

Benchmark sources are grouped by domain, while build step names stay stable:

```sh
zig build search-bench-build
zig build graph-pattern-bench-build
zig build graph-pattern-bench -- --mode exact --fanout 10000 --target-degree 100000
zig build graph-pattern-bench -- --mode generic --fanout 10000 --target-degree 100000
zig build text-segment-write-bench
zig build lsm-backend-bench
zig build wal-bench
zig build dense-stack-bench-build
zig build hbc-read-bench
zig build json-bench
zig build regex-bench
```

Run each graph-pattern mode in a fresh process. The JSON output reports p50,
p95, and p99 latency and query-allocation high-water marks. Process RSS is an
OS high-water mark; use `--warmup 0 --samples 1` for a cold-query RSS comparison.

The `search-benchmark-game/engines/antfly-zig` directory is an adapter for the
external `search-benchmark-game` harness. It delegates to the root
`search-bench-build` step.

## Generated Code

OpenAPI-generated Zig sources and Snowball-generated Zig stemmers are checked
in where they are part of normal builds. Regeneration is wired through build
steps and scripts rather than ad-hoc editing. Keep generated output, caches,
downloaded models, and local runtime state out of commits unless the repository
explicitly tracks them.

Common generated/local-output directories include:

```text
zig-out/
.zig-cache/
.zig-global-cache/
.pytest_cache/
e2e/*/.venv/
pkg/inference/.debug/
```

## Development Notes

- Prefer adding shared, reusable code under `lib/` and product-specific code
  under `pkg/antfly` or `pkg/inference`.
- Keep package and library README files local when they explain how to use that
  component directly.
- Keep long-lived design docs near the subsystem they describe. Move stale
  planning docs into current design/status docs instead of adding more
  top-level files.
- Preserve build step names when moving benchmark or test sources; scripts and
  compatibility harnesses depend on those names.
