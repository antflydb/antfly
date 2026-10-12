# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Worktrees and Build Caches

Work in a git worktree under `.worktrees/<name>` created from `origin/main`. Keep build caches inside that worktree, never in a temporary `--cache-dir`, and run `make gc` after long build sessions. Details are in @AGENTS.md.

## Project Overview

Antfly is a distributed key-value store and vector search engine with a Zig server runtime. It provides hybrid search capabilities combining full-text search (BM25) with vector similarity search, supporting multimodal data (images, audio, video) and various embedding models.

The Go tree contains client SDKs, bindings, the operator, proxies, and supporting libraries. Multiple independent Go modules exist under `go/pkg/` (`sdk`, `embedded`, `operator`, `proxy`, `docsaf`, `evalaf`, `evalaf/plugins/antfly`, `genkit/antfly`, `genkit/openrouter`, `memoryaf`; see `GO_MODULES` in the Makefile), and each must be built from within its own directory with `GOWORK=off` (the Makefiles set this). The Antfly and inference servers live under `zig/`.

## Go Version

Use Go 1.26 with SIMD experiment enabled for hardware SIMD acceleration:

```bash
# Build with SIMD
GOEXPERIMENT=simd go build ./...

# Test with SIMD
GOEXPERIMENT=simd go test ./...

# Run benchmarks
GOEXPERIMENT=simd go test -bench=. -benchmem ./...
```

The Makefile defines `GO := GOWORK=off GOEXPERIMENT=simd go` for convenience. All `make` targets use this automatically.

## Architecture

See `docs/architecture.mdx` for full details.

**Multi-Raft Design**: Separate consensus groups for metadata (cluster topology, schemas) and storage (one per shard).

**Key Components**:
- `zig/pkg/antfly/src/metadata/`: Metadata server coordinating cluster operations
- `zig/pkg/antfly/src/data/`: Storage nodes handling data shards and queries
- `zig/pkg/antfly/src/raft/`: Raft consensus and transport
- `zig/pkg/antfly/src/storage/`: Server-side storage integration (enrichment, backups, kernel ownership, shard splits)
- `zig/pkg/antfly-embedded/src/storage/`: Native storage engine (LSM tree, indexes, transactions), shared with embedded Lite
- `zig/pkg/antfly-client/`, `zig/pkg/antfly-server-api/`: Client library and server API types
- `zig/pkg/inference/`: Local inference runtime
- `docs/`: Hand-written documentation (synced into colony/frontend/apps/www-antfly at build time)

**Data Organization**:
- **Shards**: Horizontal partitions by key range
- **Tables**: Multiple shards with configurable replication
- **Indexes**: `full_text` (BM25), `embeddings` (dense, sparse, and late-interaction vectors), `graph` (traversal and pathfinding), `algebraic` (aggregates and materializations), `relational` (ordered composite keys over typed rows). Enrichments (chunking, embeddings, extraction, summaries) are declared inline on the index config; see `specs/openapi/antfly/indexes.yaml`

**Storage**: LSM storage + Raft consensus, with separate runtime paths for provisioned, serverless, and embedded Lite deployments.

## Commands

```bash
make build              # antfarm frontend + Zig antfly binary (copied to ./antfly)
make generate           # After OpenAPI changes (Zig modules, joined openapi.yaml, Go/TS/Python/Rust SDKs, antfarm)
make gc                 # Garbage-collect build caches across all worktrees
```

## Testing

**Zig tests and E2E suites**:

```bash
make zig-test
cd zig && make unit-test
uv run --project zig/e2e/antfly pytest -q
```

**Long-running tests** (E2E, evals, `-race`) should write output to a file:

```bash
cd zig && make test > /tmp/test.log 2>&1
uv run --project zig/e2e/antfly pytest -q > /tmp/test.log 2>&1
cd go/pkg/sdk && go test -race -v ./... > /tmp/test.log 2>&1
```

## Running Antfly

```bash
make build
./antfly standalone
```

**Antfly inference**: ML service for embeddings/chunking/reranking, enabled by default in standalone mode. Models auto-discovered from `~/.antfly/inference/models/`.

**Model Registry**: Pull models with `antfly inference pull <owner/name[:variant]>` (HuggingFace; GGUF/safetensors/ONNX variants) or chat with generative models via `antfly inference chat <model>` (friendly aliases like `gemma4-e2b` auto-pull). Reranker ONNX export: `scripts/export_reranker_to_onnx_static.py`.

## API Development

**Code generation**: The Go SDK uses oapi-codegen with `cfg.yaml` configs under `go/pkg/sdk/`; the Zig modules and the TS, Python, and Rust SDKs have their own generators, all driven by `make generate`. Key setting: optional fields use `omitzero` instead of pointers (`prefer-skip-optional-pointer-with-omitzero: true`).

**Adding endpoints**:
1. Update the relevant spec under `specs/openapi/`
2. Run `make generate` (regenerates Zig types, joins the public `openapi.yaml`, regenerates the SDKs and docs)
3. Implement the handler under `zig/pkg/antfly/src/api/`

**Client SDKs**: Auto-generated in `go/pkg/sdk/`, `ts/packages/sdk/`, `py/packages/sdk/`, and `rs/crates/sdk/`.

## Release Tags

Release tags:

- `v*` — Zig runtime archives, CLI packages, and container images
- `go/pkg/operator/v*` — integrated Antfly operator container build
- `py/antfly/sdk/v*`, `ts/antfly/sdk/v*` — Python and TypeScript SDK packages
- `ts/antfly/components/v*`, `ts/antfly/design-system/v*`, `ts/antfly/web/v*` — TypeScript UI packages

See `RELEASE.md` for the release pipeline.

## Secrets Management

Never store credentials in config. Use `${secret:...}` keystore or env vars. See `docs/reference/secrets.md`.

## Common Patterns

**Schema Extensions** (`x-antfly-*`): Custom OpenAPI annotations for indexing (`x-antfly-types`, `x-antfly-index`, `x-antfly-include-in-all`).

**Leader-Only Work**: Only the active Raft leader may run reconciliation and background mutation work; keep ownership transitions explicit and test them through the Zig simulation harnesses.
