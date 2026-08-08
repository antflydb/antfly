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

go/pkg/antfly/lib/
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
See [DOCUMENTATION.md](DOCUMENTATION.md) for a curated index of first-party
Markdown docs in this tree.

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
zig build test
zig build install -Dedition=full
zig build antfly -- --help
```

The inference runtime also has a package-local build file. From the repository root, use the
delegated root steps when possible:

```sh
zig build inference-run
zig build inference-test
zig build inference-wasm
zig build bench-linalg
zig build bench-audio
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
zig build test
```

Focused targets are useful while iterating:

```sh
zig build unit-test
zig build lib-db-test
zig build lib-storage-test
zig build lib-metadata-test
zig build lib-image-test
zig build lib-audio-test
zig build lib-raft-sim-test
zig build inference-test
```

The Python e2e suites are split by product:

```sh
uv run --project e2e/antfly pytest -q e2e/antfly
uv run --project e2e/inference pytest -q e2e/inference
```

Some e2e tests start local binaries from `zig-out/bin`; build the relevant
binary first when running those tests directly:

```sh
zig build install -Dedition=full
(cd pkg/inference && zig build -Dshared-lib-root=../..)
```

Model-backed inference tests may require local model fixtures or environment
configuration. The suite keeps those tests skippable when the required assets
are not present.

## Benchmarks

Benchmark sources are grouped by domain, while build step names stay stable:

```sh
zig build search-bench-build
zig build text-segment-write-bench
zig build lsm-backend-bench
zig build relational-read-bench
zig build relational-write-fast-path-bench
zig build relational-index-maintenance-bench
zig build public-query-guardrail
zig build wal-bench
zig build dense-stack-bench-build
zig build hbc-read-bench
zig build json-bench
zig build regex-bench
```

`relational-read-bench` emits schema-versioned JSONL. Schema v4 adds a
data-driven equality/prefix/range predicate matrix, the declared and observed
candidate structure, planner capability/admission fields, and an
`ordered_range` mode alongside document, base-row, scalar, ordered-tuple, and
algebraic modes.
It flushes a `relational_read_bench_case_start` row before each measured case,
so interrupted large-row runs identify the unfinished matrix cell.
`relational_read_bench_result` rows include `plan_class`, per-query latency,
planner counters, retained and peak-buffered candidate rows/bytes, and
covering-payload counters so ordered-tuple stream, doc-set, and base-scan plans
can be compared directly. Relational status bands are deterministically
interleaved across row keys so bounded base scans are not accidentally favored
by contiguous fixture data:

```sh
zig build relational-read-bench -- --samples 1 --rows 10000 --repeats 3 --batch-size 10000 --limit 100
zig build relational-read-bench -- --mode ordered_tuple --rows 10000 --repeats 3 --batch-size 10000 --limit 100
zig build relational-read-bench -- --mode ordered_tuple --selectivity high --shape ordered_page --total-mode none --rows 10000 --repeats 3 --batch-size 10000 --limit 100
zig build relational-read-bench -- --predicate-shape all --rows 10000 --repeats 3 --batch-size 10000 --limit 100
zig build relational-index-maintenance-bench -- --docs 10000 --batch-size 10000 --samples 3
zig build public-query-guardrail -Doptimize=ReleaseFast -- --relational-rows-matrix --mode local --docs 10000 --k 100 --batch-size 10000 --queries 1 --repeats 10 --warmup 2 --search-threads 1 --dims 1
```

`relational-index-maintenance-bench` is table-driven across nullable scalar,
nullable ordered-tuple, and mixed multi-index schemas. It records allocation,
peak-live-memory, staged mutation/byte, cleanup, and rebuild metrics for insert,
unchanged and changed overwrite, null transitions, and delete operations.

The relational `public-query-guardrail` matrix uses independent, identically
loaded databases for direct DB and real loopback HTTP measurements. Its 18
schema-versioned cases cover base, ordered-tuple, and algebraic access at low
and high selectivity with no-total pages, exact pages, and exact count-only
requests. It asserts row/total/profile parity, including planner work counters,
and reports average/minimum/maximum latency. Algebraic path observation remains
enabled for correct path artifacts while lazy materialization is disabled so
adaptive backfill is not charged to one query surface.

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

- Prefer adding shared, reusable code under `go/pkg/antfly/lib/` and product-specific code
  under `pkg/antfly` or `pkg/inference`.
- Keep package and library README files local when they explain how to use that
  component directly.
- Keep long-lived design docs near the subsystem they describe. Move stale
  planning docs into current design/status docs instead of adding more
  top-level files.
- Preserve build step names when moving benchmark or test sources; scripts and
  compatibility harnesses depend on those names.
