# Antfarm

Antfarm is the Antfly dashboard: a React + Vite app served by the `antfly`
binary at `http://localhost:8080` in standalone mode, and deployable separately
for hosted environments. It provides playgrounds for search, RAG, chat,
knowledge graphs, embeddings, reranking, chunking, extraction, OCR,
transcription, and evals, plus table, index, and connection management.

See [ANTFARM.md](ANTFARM.md) for the information architecture and product
direction.

## Development

From the `ts/` workspace root:

```bash
pnpm install
pnpm --filter antfarm dev         # Vite dev server
pnpm --filter antfarm build       # sync command index, typecheck, build
pnpm --filter antfarm test        # vitest unit project
pnpm --filter antfarm typecheck
```

The command palette index is generated from the page sources. Run
`pnpm --filter antfarm generate` after adding or renaming a page; `build` and
`typecheck` fail if the checked-in index is stale.

## Build into the server

The repo-level `make build-antfarm` target builds the production assets that
the Zig server embeds. `make build` runs it automatically.
