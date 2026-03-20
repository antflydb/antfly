# Contributing to Antfly

## Repository Layout

```
cmd/antfly/          CLI entry point (cobra subcommands: swarm, store, metadata, query, load, etc.)
src/
  metadata/          Metadata server (API, Raft, schema management, retrieval agents)
  store/             Storage nodes (shards, indexes, queries, transactions)
  mcp/               MCP server
  a2a/               A2A protocol adapter
  usermgr/           User management and authentication
  raft/              Raft consensus wrapper around etcd/raft
  tablemgr/          Table and shard lifecycle management
lib/
  multirafthttp/     HTTP transport for multi-raft communication
  ai/                LLM provider abstraction (Ollama, OpenAI, Bedrock, Google, etc.)
  websearch/         Web search providers (Google, Bing, Serper, Tavily)
  types/             Shared types
pkg/
  client/            Go SDK
  libaf/             Shared library (JSON, embeddings, reranking, logging, S3)
  docsaf/            Content ingestion (filesystem, web crawl, git, S3)
  evalaf/            LLM/RAG evaluation framework
  genkit/            Firebase Genkit plugins (antfly, openrouter)
  operator/          Kubernetes operator
ts/
  packages/sdk/      TypeScript SDK (@antfly/sdk)
  packages/components/  React component library (@antfly/components)
  packages/termite-sdk/ Termite TypeScript SDK
  apps/antfarm/      Web dashboard (React + Vite)
py/                  Python SDK
rs/
  pgaf/              PostgreSQL extension (Rust/pgrx)
  antfly-client/     Generated Rust SDK (shared types with pgaf)
termite/             ML inference service (git submodule)
e2e/                 End-to-end tests
configs/             Example configuration files
devops/              Kubernetes manifests (minikube, etc.)
scripts/             Build and utility scripts
docs/                Hand-written documentation (synced to docs site at build time)
```

## Prerequisites

- **Go 1.26+** — the Makefile sets `GOEXPERIMENT=simd` automatically
- **Node.js 18+** and **pnpm** — for TypeScript SDK, React components, and Antfarm dashboard
- **[goreman](https://github.com/mattn/goreman)** — optional, for running a distributed cluster locally

## Makefile Targets

Run `make help` for the full list. Key targets:

| Target | Description |
|--------|-------------|
| `make build` | Build the `antfly` binary (includes Antfarm frontend and code generation) |
| `make generate` | Regenerate all code: OpenAPI types, Go/TS/Python SDKs, Termite, docs |
| `make lint` | Run linters across Go (root + all submodules), Termite, and TypeScript |
| `make tidy` | Run `go mod tidy` across the root module and Go submodules |
| `make tidy-check` | Verify `go.mod`/`go.sum` are already tidy across the root module and Go submodules |
| `make e2e` | Run E2E tests with ONNX+XLA (downloads deps on first run) |
| `make e2e E2E_TEST=TestName` | Run a specific E2E test |
| `make build-omni` | Build with all ML backends (ONNX + XLA) |
| `make build-antfarm` | Build the Antfarm dashboard and Termite dashboard |
| `make build-docs` | Join and lint OpenAPI specs with Redocly |
| `make update-deps` | Update Go dependencies across all modules |
| `make license-headers` | Add ELv2 license headers to core files |
| `make license-check` | Verify license headers are present |

### Operator targets

| Target | Description |
|--------|-------------|
| `make operator-build` | Build the antfly-operator binary |
| `make operator-test` | Run operator tests |
| `make operator-lint` | Lint operator code |
| `make operator-docker-build` | Build operator Docker image |

### Minikube targets

| Target | Description |
|--------|-------------|
| `make minikube-start` | Start Minikube with ingress, metrics, and registry |
| `make minikube-deploy` | Build and deploy to Minikube |
| `make minikube-status` | Show pods, services, and deployments |
| `make minikube-restart` | Delete and recreate Minikube |

## Running Locally

### Swarm Mode (Single Process)

Runs metadata, storage, and [Termite](https://github.com/antflydb/termite) together:

```bash
go run ./cmd/antfly swarm
```

Dashboard at `http://localhost:8080`. Termite auto-discovers models from `~/.termite/models/`.

### Distributed Mode (Multi-Process)

Starts 5 storage nodes, 3 metadata nodes, Termite, and a load balancer:

```bash
make build && goreman -f Procfile start
```

Clean up after:

```bash
make cleanup-goreman
```

### Docker

```bash
# Standard build
docker build -t antfly .

# Omni build (ONNX + XLA backends)
docker build -f Dockerfile.omni -t antfly:omni .
```

## Multi-Module Structure

The repository contains multiple independent Go modules (no `go.work`). Each must be built and tested from its own directory:

| Module | Directory |
|--------|-----------|
| Root | `.` |
| E2E tests | `e2e/` |
| Go SDK | `pkg/client/` |
| Operator | `pkg/operator/` |
| libaf | `pkg/libaf/` |
| docsaf | `pkg/docsaf/` |
| evalaf | `pkg/evalaf/` |
| evalaf antfly plugin | `pkg/evalaf/plugins/antfly/` |
| Genkit plugin | `pkg/genkit/antfly/` |
| Genkit OpenRouter | `pkg/genkit/openrouter/` |
| Termite | `termite/` (submodule, has own Makefile) |

`make generate`, `make lint`, and `make update-deps` iterate over all submodules automatically.
`make tidy` and `make tidy-check` do too.

## Dependency Hygiene

`make build` and `make generate` may auto-run `make tidy` as part of keeping the multi-module workspace buildable. CI separately runs `make tidy-check` so dependency drift is still surfaced explicitly.

If you want the same check locally at commit time, install the repository hook path once:

```bash
make install-git-hooks
```

That enables `.githooks/pre-commit`, which runs `make tidy-check` when staged changes include Go sources or Go module files.

## Testing

```bash
# Unit tests (root module)
GOEXPERIMENT=simd go test ./...

# Specific submodule
cd pkg/client && go test ./...

# Race detector (redirect output for long runs)
go test -race -v ./... > /tmp/test.log 2>&1

# E2E tests (downloads ONNX/XLA deps and models on first run)
make e2e

# Specific E2E test with custom timeout
make e2e E2E_TEST=TestQuickstart E2E_TIMEOUT=15m

# E2E tests gated behind env vars
RUN_TXN_TESTS=true go test -v ./e2e -run DistributedTransaction -timeout 5m > /tmp/test.log 2>&1
RUN_AUTOSCALING_TESTS=true go test -v ./e2e -run Autoscaling -timeout 20m > /tmp/test.log 2>&1
RUN_EVAL_TESTS=true go test -v ./... -timeout 10m > /tmp/test.log 2>&1
```

### TypeScript

```bash
cd ts && pnpm install && pnpm test
cd ts && pnpm run lint
```

### Python

```bash
cd py && uv sync && uv run pytest
```

### Rust (pgaf)

```bash
cd rs/pgaf && make test       # Unit tests
cd rs/pgaf && make test-e2e   # E2E (requires running Antfly server)
```

## Code Generation

After changing OpenAPI specs (`src/metadata/api.yaml`, `src/usermgr/api.yaml`, `src/store/db/indexes/openapi.yaml`) or protobuf definitions:

```bash
make generate
```

This runs:
1. Redocly join + lint on OpenAPI specs
2. `go generate ./...` across root and all submodules (oapi-codegen)
3. Termite code generation
4. TypeScript SDK generation (`@antfly/sdk`, `@antfly/termite-sdk`)
5. Python SDK generation

Look for `cfg.yaml` next to any `openapi.yaml` for oapi-codegen settings. Key convention: optional fields use `omitzero` instead of pointers.

## Configuration

Example configs live in `configs/`:

| File | Description |
|------|-------------|
| `config-no-tls.yaml` | Local development without TLS |
| `config-tls.yaml` | TLS-enabled configuration |
| `config-s3-example.yaml` | S3 storage backend |
| `config-s3-minio-local.yaml` | Local MinIO for S3 testing |
| `config-secrets-example.yaml` | Secrets / keystore usage |

## Releasing

Tags follow Go module conventions and trigger CI:

- `v*` — root module release + container build
- `pkg/operator/v*` — operator container build

```bash
export GITHUB_TOKEN=...
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
go tool goreleaser release --clean
```

## License

Core Antfly code is licensed under [Elastic License 2.0 (ELv2)](LICENSE). Packages under `pkg/`, `ts/`, `py/`, `rs/`, and `termite/` are Apache 2.0 — check individual LICENSE files.
