# Antfly React Components

[![Version](https://img.shields.io/npm/v/@antfly/components.svg)](https://npmjs.org/package/@antfly/components)
[![Downloads](https://img.shields.io/npm/dt/@antfly/components.svg)](https://npmjs.org/package/@antfly/components)
[![License](https://img.shields.io/npm/l/@antfly/components.svg)](https://github.com/antflydb/antfly/tree/main/ts/LICENSE)

UI components for React + Antfly. Create search applications using declarative components.
## Usage
**[Documentation about Antfly](https://docs.antfly.io).**

```jsx
const MySearchComponent = () => (
  <Antfly url="http://<antfly_url>/table/movies">
    <SearchBox id="mainSearch" fields={["title"]} />
    <Autosuggest id="autosuggest" fields={["title"]} returnFields={["title"]} />
    <Facet id="actors" fields={["actors"]} />
    <Facet id="releasedYear" fields={["releasedYear"]} />
    <Results
      id="results"
      items={data =>
        // Map on result hits and display whatever you want.
        data.map(item => <MyCardItem key={item._id} source={item._source} />)
      }
    />
  </Antfly>
);
```

## Hooks

React Antfly provides hooks for common search and RAG functionality:

### useSearchHistory

Manage search history with localStorage persistence.

```jsx
import { useSearchHistory } from '@antfly/components';

function MyComponent() {
  const { history, isReady, saveSearch, clearHistory } = useSearchHistory(10);

  // Save a search result
  saveSearch({
    query: "how does raft work",
    timestamp: Date.now(),
    summary: "Raft is a consensus algorithm...",
    hits: [...],
    citations: [{ id: "doc1", score: 0.95 }]
  });

  // Clear all history
  clearHistory();
}
```

### useAnswerStream

Stream Answer Agent responses with state management.

```jsx
import { useAnswerStream } from '@antfly/components';

function MyComponent() {
  const {
    answer,
    reasoning,
    classification,
    hits,
    followUpQuestions,
    isStreaming,
    error,
    startStream,
    stopStream,
    reset
  } = useAnswerStream();

  // Start streaming
  startStream({
    url: 'http://localhost:8080/api/v1',
    request: {
      query: 'how does raft work',
      tables: ['docs']
    },
    headers: { 'X-API-Key': 'key' }
  });
}
```

### useCitations

Parse and render citations in RAG/Answer Agent responses.

```jsx
import { useCitations } from '@antfly/components';

function MyComponent() {
  const {
    parseCitations,
    highlightCitations,
    extractCitationUrls,
    renderAsMarkdown,
    renderAsSequential
  } = useCitations();

  // Parse citations from answer text
  const citations = parseCitations("See docs [resource_id 1, 2]");

  // Get IDs of cited resources
  const citedIds = extractCitationUrls(answer);
  const citedHits = hits.filter(hit => citedIds.includes(hit._id));
}
```

## Install

```bash
pnpm add @antfly/components
# or
npm install @antfly/components
```

## Develop

### Backend Setup

Before using the React components, you need to create a table in your Antfly instance with the appropriate schema. Here's an example using the Antfly CLI to create the "example" table used in our storybook:

```bash
antfly table create --table example \
  --schema "$(cat storybook-schema.json)" \
  --index '{
    "name": "tico_embeddings",
    "field": "TICO",
    "embedder": {
      "provider": "ollama",
      "model": "nomic-embed-text"
    }
  }'
```

The schema is defined in [`storybook-schema.json`](./storybook-schema.json).

This creates a table with:
- **x-antfly-include-in-all**: Enables the `_all` field for cross-field search across TICO, AUTR, and DESC
- **TICO** (title) and **AUTR** (author): Multi-type fields with full-text search, keyword faceting (`AUTR__keyword`, `TICO__keyword`), and autocomplete (`AUTR__2gram`, `TICO__2gram`)
- **DOMN** (domain), **DESC** (description): Text fields for categorization and content
- **DMIS**, **DMAJ**: Date fields for sorting
- **tico_embeddings**: Vector index for semantic search using Ollama embeddings

You can then load sample data from `storybook-testdata.jsonl` using:

```bash
antfly load --table example --file storybook-testdata.jsonl --id-field _id
```

### Run Storybook

```bash
pnpm storybook
```

## Main features

- Released under **Apache-2.0 licence**.
- Each component is built with React and is **customisable**. Not too much extra features nor magic.
- It comes with **no style** so it's the developers responsibility to implement their own.
- **35.32KB gzipped** for the whole lib, compatible with old browsers: >0.03% usage.

## Documentation

- **[Answer Feedback](docs/feedback.md)** - Collect user ratings and comments on AI-generated answers

## Acknowledgements

This work is based off the awesome work by [react-elasticsearch](https://github.com/betagouv/react-elasticsearch).


## Contributing

Open issues and PRs here: https://github.com/antflydb/antfly
