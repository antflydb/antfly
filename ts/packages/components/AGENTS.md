# @antfly/components — Agent Notes

Unstyled React components for Antfly search UIs: query boxes, autosuggest, facets, results, pagination, Retrieval Agent answers, and chat. Ships a baseline stylesheet (`@antfly/components/styles`) with stable `react-af-*` class names. Public API and usage are in `README.md`; workspace conventions are in `ts/AGENTS.md`.

## Architecture

- **Shared state**: `Antfly.tsx` creates a reducer-backed context (`SharedContext.tsx`, `SharedContextProvider.tsx`). Every interactive component registers itself as a widget in `SharedState.widgets`; see the widget type in `SharedContext.tsx` for its fields.
- **Query coordination**: `Listener.tsx` collects widget configurations, builds queries with the helpers in `utils.ts` (`conjunctsFrom`, `disjunctsFrom`, `toTermQueries`), sends one debounced `multiquery()` through `@antfly/sdk`, and distributes results back to widgets.
- **Widget isolation**: autosuggest widgets see only their own queries; root query widgets (QueryBox, standalone Autosuggest) exclude other root queries but include facet filters; facets see all non-autosuggest queries. Preserve this when changing `Listener.tsx` so autocomplete never affects main results.
- **Tables**: a widget's `table` prop overrides the `Antfly` default (`resolveTable()` in `utils.ts`).
- **Streaming**: `streamAnswer()` in `utils.ts` streams Retrieval Agent events and returns an `AbortController`; `useAnswerStream` and `useChatStream` wrap it.
- **Citations**: `citations.ts` parses and replaces `[doc_id 1, 2]` and `[1, 2]` citations.
- **URL state**: `toUrlQueryString()` / `fromUrlQueryString()` sync widget state with the URL.

## Workflow

- New component: add it in `src/` with a colocated `*.test.tsx`, export it from `src/index.ts`, and add a story under `stories/`.
- Tests use Vitest + jsdom with MSW for API mocks (`vitest.setup.ts`).
- Before committing: `pnpm test`, `pnpm lint`, `pnpm typecheck`.
- React, React DOM, and `@antfly/sdk` are peer dependencies; never bundle them.
- Released by tag `ts/antfly/components/v<semver>`; bump `package.json` `version` first.
