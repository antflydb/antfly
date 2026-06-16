# Antfarm Experience Design

This note describes a simpler information architecture for Antfarm. The goal is
to make the dashboard feel like a focused data retrieval workspace instead of a
collection of unrelated management pages and playgrounds.

## Product Direction

Antfarm should be organized around the user's data and retrieval workflow:

1. What data do I have?
2. How do I add or shape that data?
3. How do I search, ask questions, and evaluate quality?
4. What models and runtime capabilities support those flows?
5. How do I operate and administer the deployment?

The current implementation exposes many low-level capabilities directly in the
sidebar. That makes the app powerful, but it asks users to understand internal
product boundaries before they can choose a task. The new design should keep the
same functionality available while moving advanced or diagnostic tools closer to
the workflows that need them.

## Top-Level Navigation

Use a short, stable sidebar:

```text
Tables
Retrieval
Ingest
Models
Cluster
Admin
```

`Admin` should only be visible to users with administrative permissions. Raw
inference tools should not compete with data retrieval tasks in the primary data
workspace.

## Tables

Tables are the home base of the application.

The tables list should show:

- Table name and description
- Health or migration status
- Document/data count when available
- Index count and coverage summary
- Storage usage
- Primary action: open the table

Opening a table should land on a useful overview rather than a narrow technical
section. A table page can use local tabs:

```text
Overview | Schema | Indexes | Data
```

The overview should answer:

- Is this table usable?
- Does it have data?
- Does it have indexes?
- What can I do next?

Primary table actions should be:

- Upload data
- Create index
- Search
- Ask

Schema, indexes, upload, and manual document creation should be table-local
controls. They should not need permanent global sidebar entries.

## Retrieval

Retrieval is the umbrella for working with indexed data. It should collect the
current search, chat/RAG, graph, and eval surfaces under one mental model.

Use either a single `Retrieval` page with tabs or a sidebar item that expands to:

```text
Search
Ask
Evaluate
Graph
```

`Search` and `Ask` should feel like two modes of the same retrieval system, not
separate products.

Search should focus on:

- Query input
- Ranked results
- Filters and facets
- Scores and result inspection
- Query JSON as an advanced escape hatch

Ask should focus on:

- Chat or question answering over the selected table
- Citations
- Retrieval trace or reasoning trace
- Generation model and retrieval strategy as settings

Evaluate should remain in the product for now. Evals are the bridge between
"retrieval works" and "retrieval can be trusted." Place it under Retrieval as
`Evaluate`, below Search and Ask. If the flow is still experimental or requires
setup, make that clear in the page and keep advanced configuration collapsed.

Graph should appear when the selected table has graph-capable schema or graph
indexes. If no graph capability exists, the page should explain what is missing
and offer the next setup action.

Chunking, embedding, and reranking should not be primary destinations in the data
workspace. They should appear as settings, traces, or diagnostics inside Search,
Ask, and Evaluate.

Examples:

- Search can expose reranker selection as an advanced option.
- Ask can expose retrieval strategy, top-k, and generation model.
- Evaluate can compare retrieval configurations.
- Traces can show chunking, embedding, and reranking behavior when relevant.

## Ingest

Ingest should cover getting data into Antfly:

```text
Upload
Manual Entry
Artifacts
Reprocess
```

`Document Builder` is likely too technical as a primary label. If it remains,
prefer `Manual Entry` in user-facing navigation and keep the implementation name
internal.

Component Builder should not be a primary navigation item for now. It is more of
a developer/demo/export feature than a core database workflow. If retained, make
it a secondary action inside Search, such as `Build search UI`.

## Models

Models should describe the runtime and model capabilities that power the rest of
the product. This area can include raw model tools, but they should be presented
as a lab or utility surface rather than as peer workflows to data retrieval.

Suggested structure:

```text
Runtime
Model Directory
Tools
```

Tools can include:

- Chunk text
- Extract entities or structured data
- Read images
- Transcribe audio
- Rewrite text
- Embed text
- Rerank text

The distinction should be clear:

- Retrieval is for working with indexed table data.
- Models is for checking and experimenting with model capabilities directly.

## Cluster

Cluster should stay operational and focused:

- Health
- Nodes and shards
- Storage
- Backups
- Maintenance

This area should not contain product exploration flows.

## Admin

Admin should be permission-gated and contain:

- Users
- Secrets
- API or runtime configuration

Theme, density, and other personal display preferences should live in settings
rather than occupying persistent header space.

## Header

The global header should be quieter.

Keep:

- Command palette trigger
- Page-specific primary actions when needed

Move into settings:

- Theme
- Density
- API endpoint configuration

Show generator or model controls only on pages where generation is part of the
current workflow.

## Command Palette

The command palette should become the universal escape hatch for navigation and
task discovery. It should include all primary and secondary destinations with
user-language synonyms.

Useful commands include:

- Create table
- Upload data
- Create index
- Search table
- Ask questions
- Run eval
- View retrieval trace
- Manage models
- Add secret
- Manage users
- Check cluster health
- Transcribe audio
- Read image
- Extract structured data

Labels in the command palette should match the sidebar and page titles. It
should not introduce a third taxonomy.

## Empty And First-Run States

First-run should be guided as a short path:

```text
Create table -> Add data -> Create index -> Search or ask
```

Empty states should explain the next action that unlocks the page. For example:

- No tables: create a table.
- Empty table: upload data or add a document manually.
- No retrieval index: create a full-text or vector index.
- No eval sets: create or import an eval set from successful questions.
- No graph capability: add graph schema or graph index.

Avoid generic messages like "configure settings above" when the page can point
to a specific next step.

## Migration Strategy

This can be implemented incrementally without deleting existing pages.

1. Rename and regroup sidebar items around `Tables`, `Retrieval`, `Ingest`,
   `Models`, `Cluster`, and `Admin`.
2. Add a table overview page and move schema, indexes, upload, and manual entry
   into table-local tabs.
3. Create a Retrieval surface that groups Search, Ask, Evaluate, and Graph.
4. Remove Component Builder from primary navigation and expose it as a secondary
   Search action if needed.
5. Move raw inference playgrounds under Models or Tools.
6. Align the command palette with the new labels and include every destination.
7. Tighten first-run and empty states around the setup path.

The resulting mental model should be:

```text
Tables: what data do I have?
Ingest: how do I add data?
Retrieval: how do I search, ask, and evaluate it?
Models: what AI/runtime capabilities power it?
Cluster/Admin: how do I operate it?
```
