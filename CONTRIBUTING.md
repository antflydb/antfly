# Contributing to Antfly

## Repository Layout

```
zig/pkg/antfly/      Antfly server, CLI, storage, Raft, and embedded runtime
zig/pkg/inference/   Local inference runtime
zig/e2e/             Zig runtime end-to-end suites
go/pkg/
  embedded/          Go binding for local databases and inference
  sdk/               Go SDK
  docsaf/            Content ingestion (filesystem, web crawl, git, S3)
  evalaf/            LLM/RAG evaluation framework
  genkit/            Firebase Genkit plugins (antfly, openrouter)
  memoryaf/          Memory-focused Antfly helpers
ts/
  packages/sdk/      TypeScript SDK (@antfly/sdk)
  packages/embedded/    TypeScript embedded binding (@antfly/embedded)
  packages/components/  React component library (@antfly/components)
  apps/antfarm/      Web dashboard (React + Vite)
py/packages/sdk/     Python SDK
py/packages/embedded/  Python embedded binding (antfly-embedded)
rs/
  crates/postgres/      PostgreSQL extension (antfly-postgres, Rust/pgrx)
  crates/sdk/           Generated Rust SDK (shared types with antfly-postgres)
  crates/embedded/      Rust embedded binding (antfly-embedded)
  crates/embedded-sys/  Raw libantfly C ABI declarations (antfly-embedded-sys)
configs/             Example configuration files
devops/              Kubernetes manifests (minikube, etc.)
scripts/             Build and utility scripts
docs/                Hand-written documentation (synced to docs site at build time)
```

## Prerequisites

- **Zig 0.16** — required for the server runtime
- **Go 1.26+** — required for retained Go SDKs and tools
- **Python 3.11+ and uv** — required for Python SDK and repository tooling
- **Rust** — required for the Rust SDK and extensions; CI pins the exact version
- **Node.js and pnpm versions pinned in `ts/package.json`** — for reproducible TypeScript SDK and Antfarm artifacts; `make generate` uses Volta automatically when available

Versions for Python, Go, Zig, Rust, the macOS cross-compilation SDK, and the
publishing npm client are recorded in `scripts/ci/toolchain-policy.json`;
workflows consume them through the shared
`.github/actions/load-toolchain-policy` action. The `SDK CI` workflow runs
repository formatting, all SDKs and their in-repository consumers, Antfarm,
and release-script validation from the same change classifier.

## Makefile Targets

Run `make help` for the full list. Key targets:

| Target | Description |
|--------|-------------|
| `make build` | Build the `antfly` binary with the committed Antfarm assets |
| `make generate` | Join the public `openapi.yaml` and regenerate OpenAPI types, Go/TS/Python/Rust SDKs, and the embedded Antfarm dashboard |
| `make fmt` | Format Zig, Go, Python, TypeScript, and Rust sources |
| `make fmt-check` | Verify repository formatting without changing files |
| `make repository-check` | Run formatting, SDK, memoryaf, and release-tooling checks serially |
| `make lint` | Run linters across retained Go modules and TypeScript |
| `make tidy` | Run `go mod tidy` across retained Go modules |
| `make tidy-check` | Verify retained Go modules are tidy |
| `make zig-test` | Run the Zig test aggregate (`make zig-<target>` runs any `zig/Makefile` target) |
| `make tla-check` | Model-check the TLA+ specs (`make tla-<target>` runs any TLA+ target) |
| `make gc` | Garbage-collect build caches across all worktrees |
| `make update-deps` | Update Go dependencies across all modules |
| `make license-headers` | Add first-party license headers |
| `make license-check` | Verify first-party license headers are present |

### Minikube targets

| Target | Description |
|--------|-------------|
| `make minikube-start` | Start Minikube and deploy `devops/minikube` |
| `make minikube-deploy` | Load the published `antfly` image and apply `devops/minikube` |
| `make minikube-status` | Show Antfly pods, services, and StatefulSets |
| `make minikube-restart` | Delete and recreate Minikube |
| `make minikube-delete` | Delete the Minikube cluster |

## Running Locally

### Standalone Mode (Single Process)

Runs metadata, storage, and Antfly inference together:

```bash
make build
./antfly standalone
```

Dashboard at `http://localhost:8080`. Antfly inference auto-discovers models from `~/.antfly/inference/models/`.

## Multi-Module Structure

The repository contains multiple independent Go modules (no `go.work`). Each must be built and tested from its own directory:

| Module | Directory |
|--------|-----------|
| Antfly embedded binding | `go/pkg/embedded/` |
| Go SDK | `go/pkg/sdk/` |
| docsaf | `go/pkg/docsaf/` |
| evalaf | `go/pkg/evalaf/` |
| evalaf antfly plugin | `go/pkg/evalaf/plugins/antfly/` |
| Genkit plugin | `go/pkg/genkit/antfly/` |
| Genkit OpenRouter | `go/pkg/genkit/openrouter/` |
| Memory helpers | `go/pkg/memoryaf/` |

`make generate`, `make lint`, and `make update-deps` iterate over all submodules automatically.
`make tidy` and `make tidy-check` do too.

## Dependency Hygiene

`make generate` auto-runs `make tidy` as part of keeping the multi-module workspace buildable. CI separately runs `make tidy-check` so dependency drift is still surfaced explicitly.

If you want the same check locally before pushing, install the repository hooks once:

```bash
make install-git-hooks
```

That enables two hooks:

- `.githooks/pre-commit` formats staged Zig, Go, Python, Rust, and TypeScript workspace files with the tools used by `make fmt`. Only formatting changes are added to the index; unrelated unstaged edits stay unstaged. If formatting overlaps an unstaged edit, the hook stops without applying any changes. Run `make fmt`, then review and stage the desired hunks before retrying.
- `.githooks/pre-push` runs `make tidy-check` when pushed changes include Go sources or Go module files.

Install the development tools and TypeScript dependencies before using the hooks. Missing tools or formatter failures stop the commit with an error. Use `git commit --no-verify` to bypass hooks when needed; CI continues to check formatting independently.

## Testing

```bash
# Zig unit and aggregate tests
make zig-test
cd zig && make unit-test

# Specific submodule
cd go/pkg/sdk && go test ./...

# Race detector (redirect output for long runs)
go test -race -v ./... > /tmp/test.log 2>&1

# Zig runtime E2E tests
uv run --project zig/e2e/antfly pytest -q
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

### Rust (antfly-postgres)

```bash
cd rs/crates/postgres && make test       # Unit tests
cd rs/crates/postgres && make test-e2e   # E2E (requires running Antfly server)
```

## Code Generation

After changing OpenAPI specs under `specs/openapi/` or inference runtime specs:

```bash
make generate
```

This runs:
1. Redocly join + lint on OpenAPI specs
2. Zig OpenAPI modules (`make zig-openapi-generate`)
3. `go generate ./...` across retained Go modules (oapi-codegen)
4. TypeScript SDK generation (`@antfly/sdk`)
5. Python SDK generation
6. Antfarm dashboard generation with the pinned Node and pnpm toolchain

Look for `cfg.yaml` next to any `openapi.yaml` for oapi-codegen settings. Key convention: optional fields use `omitzero` instead of pointers.

## Configuration

Example configs live in `configs/`:

| File | Description |
|------|-------------|
| `config-no-tls.json` | Local development without TLS |
| `config-tls.json` | TLS-enabled configuration |
| `config-s3-example.json` | Serverless S3 storage with named connections and multi-bucket lanes |
| `config-s3-minio-local.json` | Serverless object storage on local MinIO |
| `config-secrets-example.json` | Secrets / keystore usage |

## Releasing

Release tags:

- `v*` — Zig runtime archives, CLI packages, and container images

Colony owns the Kubernetes operator, hosted proxies, and operator release tags
and image publishing. They are developed and tested in that repository.

See [RELEASE.md](RELEASE.md) for the Zig release pipeline.

## License

All first-party Antfly code is Apache-2.0, including the database server,
orchestration, embedded engine, inference, CLI, SDKs, and language bindings.
See [LICENSING.md](LICENSING.md) for source ownership and third-party notices.

## Contribution rights and inbound license

By submitting a contribution, you confirm that you have the right to submit it
under Apache-2.0. Contributions containing third-party material must retain its
original notices and comply with its license. `Signed-off-by` trailers are optional.
