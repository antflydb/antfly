# Antfarm Command Data

`command-definitions.json` is the source of truth for command palette entries.
The palette renders from this file directly.

`command-index.json` is generated data for semantic command search. It contains
only definitions marked with `"semantic": true` plus their precomputed
embeddings.

When changing commands:

1. Edit `command-definitions.json`.
2. Run `pnpm --filter antfarm generate:commands`.
3. If the script reports missing embeddings, refresh embeddings for the changed
   semantic commands before committing.

`pnpm --filter antfarm typecheck` and `pnpm --filter antfarm build` both verify
that `command-index.json` is synced with the definitions.
