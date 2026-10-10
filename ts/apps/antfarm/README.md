# Antfarm

Antfarm is the Antfly dashboard: a React + Vite app served by the `antfly`
binary at `http://localhost:8080` in standalone mode, and deployable separately
for hosted environments. It provides playgrounds for search, RAG, chat,
knowledge graphs, embeddings, reranking, chunking, extraction, OCR,
transcription, and evals, plus table, index, and connection management.

See [ANTFARM.md](ANTFARM.md) for the information architecture and product
direction.

## Runtime profiles

Models & Runtime includes a device card and a selectable model radar profile.
Choose the runtime connection, then use **Set up device** to enter its name,
chip, CPU cores, memory, and bandwidth. Hardware details are user-configured;
the browser does not identify the inference host automatically.

Use **Record measurements** for a model's quantization, evaluation score,
decode/prefill throughput, peak memory, tested context, and measurement source.
Unknown values remain empty. **Values & chart scales** explains the radar's
fixed scales; memory headroom uses configured RAM and is not a serving admission
check. Profiles stay in browser storage, isolated by endpoint and connection.
Changing hardware clears that connection's model measurements.

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
