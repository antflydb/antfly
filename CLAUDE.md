# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Antfly is a distributed key-value store and vector search engine built on etcd's Raft consensus library. It provides hybrid search capabilities combining full-text search (BM25) with vector similarity search, supporting multimodal data (images, audio, video) and various embedding models.

Multiple independent Go modules exist (`e2e/`, `pkg/operator/`, `pkg/client/`, `pkg/libaf/`, etc.) — each must be built from within its own directory. CLI subcommands (query, table, load, etc.) are registered directly on the root command. Git submodule: `termite/`.

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
- `src/metadata/`: Metadata server coordinating cluster operations
- `src/store/`: Storage nodes handling data shards, queries, and indexes
- `src/raft/`: Raft consensus wrapping etcd/raft
- `src/tablemgr/`: Table and shard management
- `lib/multirafthttp/`: HTTP transport for multi-raft communication
- `docs/`: Hand-written documentation (synced into colony/frontend/apps/www-antfly at build time)

**Data Organization**:
- **Shards**: Horizontal partitions by key range (`common.Range`)
- **Tables**: Multiple shards with configurable replication
- **Indexes**: `bleve` (full-text BM25), `embeddingindex` (vector), `remote` (proxy), enrichers (embeddings/summaries)

**Index System**: Registry pattern in `src/store/indexes/indexes.go`. All indexes implement `Index` interface; enrichable indexes implement `EnrichableIndex` for async embedding generation.

**Storage**: Pebble (RocksDB successor) + Raft consensus. Each shard has its own Pebble instance.

## Commands

```bash
make build              # All binaries + frontend + codegen
make generate           # After OpenAPI/proto changes (SDKs, docs, protobufs)
```

## Testing

**E2E tests** with ONNX+XLA (downloads deps and models on first run):

```bash
make e2e                            # Run all E2E tests
make e2e E2E_TEST=TestName          # Run specific test
make e2e E2E_TIMEOUT=30m            # Custom timeout (default: 30m)
```

**Long-running tests** (E2E, evals, `-race`) should write output to a file:

```bash
RUN_TXN_TESTS=true go test -v ./e2e -run DistributedTransaction -timeout 5m > /tmp/test.log 2>&1
RUN_AUTOSCALING_TESTS=true go test -v ./e2e -run Autoscaling -timeout 20m > /tmp/test.log 2>&1
RUN_EVAL_TESTS=true go test -v ./... -timeout 10m > /tmp/test.log 2>&1
go test -race -v ./... > /tmp/test.log 2>&1
```

## Running Antfly

```bash
go run ./cmd/antfly swarm              # Single-node dev
```

**Termite**: ML service for embeddings/chunking/reranking, enabled by default in swarm mode. Models auto-discovered from `./models/`.

**Model Registry**: Export HuggingFace models to ONNX via `scripts/export_model_to_registry.py`. Supports embedders, rerankers, chunkers, and multimodal (CLIP).

## API Development

**Code generation**: OpenAPI specs use oapi-codegen with `cfg.yaml` configs. Look up the `cfg.yaml` next to any `openapi.yaml` or `api.yaml` for generation settings. Key setting: optional fields use `omitzero` instead of pointers (`prefer-skip-optional-pointer-with-omitzero: true`).

**Adding endpoints**:
1. Update `src/metadata/api.yaml` (or component's `openapi.yaml`)
2. Run `make generate`
3. Implement handler

**Client SDKs**: Auto-generated in `pkg/client/`, `ts/`, `py/`.

## Release Tags

Tags follow Go module conventions and trigger CI:

- `v*` — root module release + container build
- `pkg/operator/v*` — operator container build

## Secrets Management

Never store credentials in config. Use `${secret:...}` keystore or env vars. See `docs/secrets.md`.

## Common Patterns

**Schema Extensions** (`x-antfly-*`): Custom OpenAPI annotations for indexing (`x-antfly-types`, `x-antfly-index`, `x-antfly-include-in-all`).

**Leader-Only Work**: Use `LeaderFactory` pattern with `atomic.Bool` flag. Only Raft leader runs background jobs. See `src/store/db.go`.
