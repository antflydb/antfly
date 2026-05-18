# antfly-zig

`antfly-zig` is the Zig monorepo for AntflyDB and the Termite inference
runtime. The repository contains product packages, shared libraries, benchmark
harnesses, compatibility suites, and Python end-to-end tests that exercise the
same checked-in source tree.

## Repository Layout

```text
pkg/
  antfly/            AntflyDB server, API, metadata, storage, search, raft
  antfly-client/     Zig client package
  antfly-embedded/   Embedded Antfly package and WASM smoke surface
  termite/           Termite inference runtime, OpenAPI server, tools, web UI
  termite-client/    Zig Termite client package

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
  storage/           LMDB, LSM, WAL, replay, open, and storage-path benches
  vectors/           Dense, HBC, RaBitQ, recall, and sparse vector benches
  baselines/         Checked benchmark baseline JSONL outputs

e2e/
  antfly/            Antfly product-level pytest suite
  termite/           Termite product-level pytest suite

compat/              Shared compatibility corpus and Go comparison harnesses
specs/               Formal specifications and model-checking inputs
scripts/             Repository tooling scripts
tools/               Developer tools
testdata/            Shared checked-in fixture data
```

Root-level Markdown files are design and operating notes for active AntflyDB
areas. Library-specific design docs live next to their libraries, for example
`lib/image/IMAGE.md` and `lib/audio/AUDIO.md`. Termite-specific design docs
currently live under `pkg/termite/` and `pkg/termite/docs/`.

## Build Requirements

- Zig `0.16.0` or newer.
- `uv` for Python e2e suites and repository helper scripts.
- Optional native runtime dependencies for some Termite features, such as MLX,
  ONNX Runtime, FFmpeg, or platform GPU support. The build detects available
  local support and exposes flags such as `-Dmlx=...`, `-Dmetal=...`, and
  `-Donnx=...`.

## Common Builds

```sh
zig build
zig build test
zig build install-antfly
zig build antfly -- --help
```

Termite also has a package-local build file. From the repository root, use the
delegated root steps when possible:

```sh
zig build termite-run
zig build termite-test
zig build termite-wasm
zig build termite-bench-linalg
zig build termite-bench-audio
```

For package-local Termite work:

```sh
cd pkg/termite
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
zig build termite-test
```

The Python e2e suites are split by product:

```sh
uv run --project e2e/antfly pytest -q e2e/antfly
uv run --project e2e/termite pytest -q e2e/termite
```

Some e2e tests start local binaries from `zig-out/bin`; build the relevant
binary first when running those tests directly:

```sh
zig build install-antfly
(cd pkg/termite && zig build -Dshared-lib-root=../..)
```

Model-backed Termite tests may require local model fixtures or environment
configuration. The suite keeps those tests skippable when the required assets
are not present.

## Benchmarks

Benchmark sources are grouped by domain, while build step names stay stable:

```sh
zig build search-bench-build
zig build text-segment-write-bench
zig build lsm-backend-bench
zig build wal-bench
zig build dense-stack-bench-build
zig build hbc-read-bench
zig build json-bench
zig build regex-bench
```

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
pkg/termite/.debug/
```

## Development Notes

- Prefer adding shared, reusable code under `lib/` and product-specific code
  under `pkg/antfly` or `pkg/termite`.
- Keep package and library README files local when they explain how to use that
  component directly.
- Keep long-lived design docs near the subsystem they describe. Move stale
  planning docs into current design/status docs instead of adding more
  top-level files.
- Preserve build step names when moving benchmark or test sources; scripts and
  compatibility harnesses depend on those names.
