# Antfly React Components

`@antfly/components` is a React component library for building search interfaces with Antfly. It provides declarative components for query boxes, autosuggest, facets, results, pagination, answers, and chat. It ships a baseline stylesheet (`@antfly/components/styles`) with stable `react-af-*` class names that consumers can override.

## Testing

- Uses Vitest with jsdom for testing React components
- Uses Mock Service Worker (MSW) for mocking API requests in tests
- Tests are located alongside component files (e.g., `Autosuggest.test.tsx`)
- Test setup is in `vitest.setup.ts`
- Run specific test file: `vitest src/Autosuggest.test.tsx`
- Run specific test: `vitest -t "test name pattern"`
- Run tests with UI: `vitest --ui`
- Run tests in watch mode: `vitest` (no arguments)

## Architecture

### State Management

The library uses a **centralized state system** via React Context:

- **`SharedContext.tsx`**: Defines the state shape and actions
- **`SharedContextProvider.tsx`**: Provides the context to components
- **`Antfly.tsx`**: Root component that initializes the context with a reducer

All components (QueryBox, Facet, Results, etc.) register themselves as **widgets** in the shared state. The `Listener` component coordinates queries by:
1. Collecting widget configurations
2. Building queries from widget state
3. Executing multi-query requests via `multiquery()`
4. Distributing results back to widgets

### Widget System

Each interactive component is a "widget" that registers in `SharedState.widgets` Map with:
- `id`: Unique identifier
- `needsQuery`: Whether it needs query results
- `needsConfiguration`: Whether it contributes to the query
- `isFacet`: Whether it's a facet component
- `rootQuery`: Whether it's a root query widget (QueryBox, Autosuggest)
- `isAutosuggest`: Whether it's an autosuggest widget (for isolation)
- `wantResults`: Whether it wants full results
- `wantFacets`: Whether it wants facet data
- `query`: The query it contributes
- `semanticQuery`: Semantic/vector search query text
- `isSemantic`: Whether semantic search is enabled
- `value`: Current selected/input value
- `submittedAt`: Timestamp of last submission
- `table`: Optional table override (single or multi-table)
- `filterQuery`: Query to constrain results
- `exclusionQuery`: Query to exclude matches
- `facetOptions`: Facet configurations for the widget
- `isLoading`: Loading state
- `configuration`: Component config (fields, size, etc.)
- `result`: Query results specific to this widget (data, facetData, total, error)

### Query Building

Queries are constructed from widget states using helper functions in `utils.ts`:
- `conjunctsFrom()`: Combines multiple queries with AND logic
- `disjunctsFrom()`: Combines queries with OR logic
- `toTermQueries()`: Creates term queries for facet selections

The library uses Antfly's query DSL (match, conjuncts, disjuncts) and communicates via the TypeScript SDK (`@antfly/sdk`).

### URL State Synchronization

- `toUrlQueryString()`: Serializes widget state to URL query params
- `fromUrlQueryString()`: Deserializes URL params back to state
- Supports browser back/forward navigation

## Key Components

### Search Components
- **Antfly**: Root provider component that initializes client and context
- **QueryBox**: Text search input with debouncing; the root query widget for search and Retrieval Agent queries
- **Autosuggest**: Search box with autocomplete suggestions (standalone or nested in QueryBox), with `AutosuggestResults` and `AutosuggestFacets` for composable dropdowns
- **Facet**: Faceted navigation with term aggregations
- **Results**: Render search results with customizable item rendering (`itemsPerPage`)
- **Pagination**: Page navigation controls
- **ActiveFilters**: Display and remove active filters (`items` prop)

### Answer and Chat Components
- **AnswerResults**: Retrieval Agent results with streaming reasoning, answers, citations, and an optional `generator`
- **AnswerFeedback**: Collect user ratings and comments on AI-generated answers
- **ChatBar**, **ChatInput**, **ChatMessages**: Multi-turn chat over the Retrieval Agent, coordinated through `ChatContext`

### Hooks
- **useSearchHistory**: Manage search history with localStorage persistence (max results configurable, 0 to disable)
- **useAnswerStream**: Stream Retrieval Agent responses with state management (answer, reasoning, classification, hits, follow-up questions)
- **useChatStream**: Stream multi-turn chat responses
- **useCitations**: Parse and render citations in Retrieval Agent responses (supports `[doc_id X]` and `[X]` formats)
- **useSharedContext**, **useAnswerResultsContext**, **useAutosuggestContext**, **useChatContext**: Access the shared widget state and per-feature contexts

### Internal Components
- **Listener**: Internal component that coordinates queries (not typically used directly)
- **CustomWidget**: Extensibility point for custom search widgets

## SDK Integration

The library depends on `@antfly/sdk` (from `ts/` directory) which provides:
- `AntflyClient`: HTTP client for Antfly API
- Type definitions for queries and responses
- Multi-query support via `multiquery()`
- Streaming support for the Retrieval Agent

Client initialization happens in `Antfly` component via `initializeAntflyClient()`. The client is stored as a singleton accessible via `getAntflyClient()`.

### Streaming the Retrieval Agent

The library provides the `streamAnswer()` utility (in `utils.ts`) for streaming Retrieval Agent responses:

```typescript
streamAnswer(url, request, headers, {
  onClassification: (data) => { /* handle query classification */ },
  onHit: (hit) => { /* handle search hit */ },
  onReasoning: (chunk) => { /* handle reasoning chunk */ },
  onGeneration: (chunk) => { /* handle generation chunk */ },
  onFollowup: (question) => { /* handle follow-up */ },
  onComplete: () => { /* handle completion */ }
});
```

It returns an `AbortController` for canceling the stream. `useAnswerStream` and `useChatStream` wrap it for component use.

## Build Output

The library is built as ES modules and CommonJS:
- `dist/main.js`: ES module format (`import`)
- `dist/main.cjs`: CommonJS format (`require`)
- `dist/index.d.ts`: TypeScript declarations
- `dist/components.css`: baseline stylesheet, exported as `@antfly/components/styles`
- `./adapters` subpath export for framework adapters

React, React DOM, and `@antfly/sdk` are peer dependencies (not bundled).

## Publishing

Published to npm as `@antfly/components` through the monorepo's tag-triggered
workflow (`ts/antfly/components/v<semver>`). The package includes:
- Main entry: `dist/main.js`
- Types entry: `dist/index.d.ts`
- Source maps for debugging

## Component Development Workflow

1. Create component in `src/` with corresponding `.test.tsx` file
2. Add component to `src/index.ts` exports
3. Create story in `stories/` for documentation
4. Run `pnpm storybook` to develop interactively
5. Run tests with `pnpm test`
6. Run `pnpm lint` and `pnpm typecheck` before committing

## Important Architecture Patterns

### Widget Isolation (Autosuggest vs. Search)

The `Listener` component (src/Listener.tsx:154-193) implements widget isolation to prevent query interference:

- **Autosuggest widgets** are completely isolated - they only see their own queries
- **Root query widgets** (QueryBox, and Autosuggest when standalone) exclude other root queries but include facet filters
- **Facet widgets** see all non-autosuggest queries

This prevents autocomplete dropdowns from affecting main search results and vice versa.

### Multi-Table Support

Widgets support a `table` prop to override the default table from `Antfly`:
- Single table: `table="products"`
- Priority: `widget.table` > `Antfly.table`
- Resolved via `resolveTable()` in utils.ts:144-154

### Citation Handling (RAG/Retrieval Agent)

The `citations.ts` module provides utilities for parsing and rendering citations in AI responses:

```typescript
// Parse citations from text
const citations = parseCitations(text); // Finds [doc_id X] or [X] patterns

// Replace with custom rendering
const rendered = replaceCitations(text, {
  renderCitation: (ids, allIds) => renderAsSequentialLinks(ids, allIds)
});

// Extract cited document IDs
const citedIds = getCitedDocumentIds(summary);
const citedHits = hits.filter(hit => citedIds.includes(hit._id));
```

Supports formats: `[doc_id 1, 2]` and shorthand `[1, 2]`

### Query Debouncing

The `Listener` batches rapid widget updates with a 15ms debounce (src/Listener.tsx:448) to minimize network calls when multiple widgets update simultaneously.
