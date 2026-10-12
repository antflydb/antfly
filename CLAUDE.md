# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Worktrees and Build Caches

Work in a git worktree under `.worktrees/<name>` created from `origin/main`. Keep build caches inside that worktree, never in a temporary `--cache-dir`, and run `make gc` after long build sessions. Details are in @AGENTS.md.

## Project Overview

What Antfly is, install options, and features are in `README.md`. Where things live:

- `zig/`: the Antfly server (`zig/pkg/antfly`), storage engine shared with embedded Lite (`zig/pkg/antfly-embedded`), and inference runtime (`zig/pkg/inference`)
- `go/`: SDK, embedded bindings, operator, proxy, and supporting libraries. Each module in `GO_MODULES` (Makefile) is independent; build it from its own directory with `GOWORK=off GOEXPERIMENT=simd go` (Make targets set this)
- `ts/`, `py/`, `rs/`: TypeScript packages and apps, Python SDK, Rust SDK
- `specs/openapi/`: API source of truth; `openapi.yaml` at the root is generated
- `docs/`: hand-written documentation, synced into colony/frontend/apps/www-antfly at build time

Read `docs/architecture.mdx` for the Raft, storage, and index design, and `specs/openapi/antfly/indexes.yaml` for index types and options. Don't rely on memory for either; both change often.

## Commands

```bash
make build              # Zig antfly binary, copied to ./antfly (run ./antfly standalone)
make generate           # After OpenAPI or antfarm changes (Zig modules, joined openapi.yaml, Go/TS/Python/Rust SDKs, antfarm build)
make gc                 # Garbage-collect build caches across all worktrees
```

## Testing

```bash
make zig-test
cd zig && make unit-test
uv run --project zig/e2e/antfly pytest -q
```

Long-running tests (E2E, evals, `-race`) should write output to a file:

```bash
cd zig && make test > /tmp/test.log 2>&1
uv run --project zig/e2e/antfly pytest -q > /tmp/test.log 2>&1
cd go/pkg/sdk && GOEXPERIMENT=simd go test -race -v ./... > /tmp/test.log 2>&1
```

## API Development

1. Update the relevant spec under `specs/openapi/`
2. Run `make generate`
3. Implement the handler under `zig/pkg/antfly/src/api/`

Client SDKs in `go/pkg/sdk/`, `ts/packages/sdk/`, `py/packages/sdk/`, and `rs/crates/sdk/` wrap generated API clients; edit the hand-written wrappers, never generated files (`*generated*`, `oapi/`, `public-api.d.ts`). The Go SDK's oapi-codegen settings live in `go/pkg/sdk/**/cfg.yaml` (optional fields use `omitzero`, not pointers).

Custom `x-antfly-*` OpenAPI annotations drive indexing and SQL typing; see `specs/openapi/antfly/metadata.yaml`.

## Rules

- Never store credentials in config. Use the `${secret:...}` keystore or env vars (`docs/reference/secrets.md`).
- Only the active Raft leader may run reconciliation and background mutation work. Keep ownership transitions explicit and test them through the Zig simulation harnesses.
- Releases are tag-driven; see `RELEASE.md` before creating or changing tags.
