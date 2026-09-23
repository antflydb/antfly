# TOON Format for RAG Document Rendering

## Overview

The retrieval agent renders retrieved documents into its generation prompt as **TOON (Token-Oriented Object Notation)** by default. TOON carries the same structure as JSON with fewer punctuation tokens, so more documents fit in the model's context for the same cost.

## What is TOON?

TOON is a compact, human-readable encoding of JSON data designed for passing structured data to Large Language Models.

- **Key-value lines**: `key: value`, with nested objects indented beneath their key
- **Inline primitive arrays**: `tags[3]: ai,search,ml`, with the length in brackets
- **Tabular arrays**: arrays of uniform objects declare their fields once and list one row per line

**Simple object:**
```
title: Introduction to Vector Search
author: Jane Doe
tags[3]: ai,search,ml
```

**Tabular data:**
```
users[2]{id,name}:
  1,Ada
  2,Bob
```

## Usage in Antfly

### Default Behavior

When a retrieval agent request has a generation step and no `document_renderer`, each document's source fields are encoded as TOON in the prompt. Retrieval metadata such as tree position is stated separately, before the document.

```bash
curl -X POST http://127.0.0.1:8080/db/v1/agents/retrieval \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is vector search?",
    "queries": [{
      "table": "documents",
      "semantic_search": "What is vector search?"
    }],
    "generator": {"provider": "openai", "model": "gpt-4o"},
    "steps": {"generation": {}}
  }'
```

### Custom Document Rendering

Set `document_renderer` on the retrieval agent request to render each document with a Handlebars template instead. It requires `steps.generation`. The template is rendered once per hit against:

- `this.id`: the document id
- `this.score`: the hit's relevance score
- `this.fields`: the document's source fields

```json
{
  "query": "What is vector search?",
  "queries": [{"table": "documents", "semantic_search": "What is vector search?"}],
  "generator": {"provider": "openai", "model": "gpt-4o"},
  "steps": {"generation": {}},
  "document_renderer": "Title: {{{this.fields.title}}}\n{{encodeToon this.fields}}"
}
```

Values in `{{...}}` are HTML-escaped; use triple braces (`{{{...}}}`) for raw text. Queries (`/db/v1/tables/{table}/query`) do not generate text and reject `document_renderer`.

A template that fails to parse, or passes invalid `encodeToon` options, is rejected with a `400` before retrieval runs.

### Template Helpers

`encodeToon` encodes any value as TOON:

```handlebars
{{encodeToon this.fields}}
{{encodeToon this.fields indent=4}}
{{encodeToon this.fields delimiter="tab"}}
```

- `indent`: spaces per nesting level, at least 1 (default 2)
- `delimiter`: separator for array values and table rows: `comma` (default), `tab`, or `pipe`

The template helpers `scrubHtml`, `eq`, and `media` are also available.

## Token Reduction

Savings depend on document structure. Flat documents with short values save little over plain key-value text; uniform arrays of objects, which TOON writes as tables, save the most compared with JSON.

## Implementation Details

The TOON encoder lives in `zig/lib/toon/` and is checked against the TOON specification conformance suite. Prompt rendering and the `encodeToon` helper live in `zig/pkg/antfly/src/api/document_renderer.zig`.

## See Also

- [TOON Specification](https://github.com/toon-format/spec)
- [Support Answer Agent guide](guides/support-answer-agent.mdx)
