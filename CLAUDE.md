# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Antfly is a distributed key-value store and vector search engine with a Zig server runtime. It provides hybrid search capabilities combining full-text search (BM25) with vector similarity search, supporting multimodal data (images, audio, video) and various embedding models.

The Go tree contains client SDKs, bindings, the operator, proxies, and supporting libraries. Multiple independent Go modules exist under `go/`, and each must be built from within its own directory. The Antfly and inference servers live under `zig/`.

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

The Makefile defines `GO := GOEXPERIMENT=simd go` for convenience. All `make` targets use this automatically.

## Architecture

See `docs/architecture.mdx` for full details.

**Multi-Raft Design**: Separate consensus groups for metadata (cluster topology, schemas) and storage (one per shard).

**Key Components**:
- `zig/pkg/antfly/src/metadata/`: Metadata server coordinating cluster operations
- `zig/pkg/antfly/src/data/`: Storage nodes handling data shards and queries
- `zig/pkg/antfly/src/raft/`: Raft consensus and transport
- `zig/pkg/antfly/src/storage/`: Storage engines, indexes, and transactions
- `zig/pkg/inference/`: Local inference runtime
- `docs/`: Hand-written documentation (synced into colony/frontend/apps/www-antfly at build time)

**Data Organization**:
- **Shards**: Horizontal partitions by key range (`common.Range`)
- **Tables**: Multiple shards with configurable replication
- **Indexes**: `bleve` (full-text BM25), `embeddingindex` (vector), `remote` (proxy), enrichers (embeddings/summaries)

**Storage**: LSM storage + Raft consensus, with separate runtime paths for provisioned, serverless, and embedded Lite deployments.

## Commands

```bash
make build              # All binaries + frontend + codegen
make generate           # After OpenAPI/proto changes (SDKs, docs, protobufs)
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

**Code generation**: OpenAPI specs use oapi-codegen with `cfg.yaml` configs. Look up the `cfg.yaml` next to any `openapi.yaml` or `api.yaml` for generation settings. Key setting: optional fields use `omitzero` instead of pointers (`prefer-skip-optional-pointer-with-omitzero: true`).

**Adding endpoints**:
1. Update the relevant spec under `specs/openapi/`
2. Run `make generate`
3. Implement handler

**Client SDKs**: Auto-generated in `go/pkg/sdk/`, `ts/packages/sdk/`, `py/packages/sdk/`, and `rs/crates/sdk/`.

## Release Tags

Release tags:

- `v*` — Zig runtime archives, CLI packages, and container images
- `go/pkg/operator/v*` — integrated Antfly operator container build
## Secrets Management

Never store credentials in config. Use `${secret:...}` keystore or env vars. See `docs/secrets.md`.

## Common Patterns

**Schema Extensions** (`x-antfly-*`): Custom OpenAPI annotations for indexing (`x-antfly-types`, `x-antfly-index`, `x-antfly-include-in-all`).

**Leader-Only Work**: Only the active Raft leader may run reconciliation and background mutation work; keep ownership transitions explicit and test them through the Zig simulation harnesses.
