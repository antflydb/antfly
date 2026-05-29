# Antfly as an Agentic Memory Layer over BigQuery/Snowflake

## Context

Warehouses (BigQuery, Snowflake) hold the enterprise's structured truth: CRM rows,
product catalogs, ticket logs, financial records. They are terrible as an
agent's working memory. Agents want:

- per-conversation and per-user memory
- low-latency hybrid (BM25 + vector + graph) retrieval
- typed entities with canonical identity
- durable feedback and overrides
- streaming retrieval with tool use
- a tuning surface that can improve over time

Antfly already provides the substrate for this:

- `pkg/memoryaf` — agent-facing memory handler, with embeddings + graph index
- `pkg/docsaf` — document processing (PDF/HTML/OOXML/etc.) with chunking, OCR,
  and entity enrichment, plus source adapters (S3, web, filesystem, git, GDrive)
- Antfly core — tables, hybrid indexes, graph with typed edges, retrieval
  agent with streaming tool use
- Termite — rerankers, embedders, chunkers, NER, relation extraction

The missing piece is the warehouse bridge: a managed way to pull
agent-relevant slices of BigQuery/Snowflake into Antfly, keep them in sync,
route agent traffic through memoryaf on top of that corpus, and close the
loop with reinforcement learning that tunes extraction and graph-index
behavior from real agent outcomes.

This doc sketches that bridge.

## Design Goal

Treat the warehouse as the **system of record for structured data** and
Antfly as the **system of record for agent interaction** — memory, evidence,
canonical entities, edges, feedback, and tuned retrieval policy.

Agents never query BigQuery/Snowflake directly. They query memoryaf + Antfly
tables that are continuously hydrated from warehouse slices through the
docsaf pipeline. Warehouse rows become Antfly documents; extracted entities
become canonical records in an entity table; agent outcomes become training
signal for extractor and index tuning.

## Architecture Layers

```
┌──────────────────────────────────────────────────────────────────┐
│  Agent Runtime (chat UI, SDK, tool-calling apps)                 │
│  - retrieval agent (POST /agents/retrieval, streaming)           │
│  - memoryaf MCP/HTTP surface (pkg/memoryaf)                      │
└──────────────▲──────────────────────────────▲────────────────────┘
               │ read/write memory            │ tool: search corpus
               │                              │
┌──────────────┴──────────────────────────────┴────────────────────┐
│  Antfly (GKE / Cloud Run, per-tenant or shared)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ memory       │  │ corpus_*     │  │ entities     │           │
│  │ (memoryaf)   │  │ (docsaf)     │  │ (canonical)  │           │
│  │ embeddings+  │  │ embeddings+  │  │ embeddings+  │           │
│  │ graph        │  │ bm25+graph   │  │ entity_graph │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│           ▲                ▲                   ▲                 │
│           │ writes         │ ingest            │ resolver        │
│           │                │                   │                 │
│  ┌────────┴────────────────┴───────────────────┴──────────┐     │
│  │  Enrichment + resolver pipeline (leader-only jobs)     │     │
│  │  - embedding enrichers                                 │     │
│  │  - docsaf entity extractor → evidence outbox           │     │
│  │  - entity resolver (namespaced, leader-owned)          │     │
│  │  - RL policy service (see §RL Loop)                    │     │
│  └────────────────────────────────────────────────────────┘     │
└──────────────▲─────────────────────────────────▲────────────────┘
               │                                 │
┌──────────────┴────────────────┐  ┌─────────────┴────────────────┐
│  Warehouse Bridge (GKE job)   │  │  Termite (ML inference)      │
│  - BQ Storage Read API        │  │  - embedders                 │
│  - Snowflake Streams / CDC    │  │  - rerankers                 │
│  - view-scoped "contexts"     │  │  - GLiNER NER                │
│  - row → docsaf document      │  │  - REBEL relation extraction │
│  - scheduled + webhook pull   │  │  - chunkers                  │
└──────────────▲────────────────┘  └──────────────────────────────┘
               │
        ┌──────┴────────┐
        │  BigQuery     │
        │  Snowflake    │
        │  (GCP tenant) │
        └───────────────┘
```

## Components

### 1. Warehouse Bridge (new)

A new package, roughly `pkg/warehouseaf`, that defines a **context** — a
named, versioned slice of a warehouse that maps to one Antfly table.

Config shape:

```yaml
contexts:
  support_tickets:
    source:
      kind: bigquery
      project: acme-prod
      dataset: support
      view: agent_ready_tickets_v3      # view owned by data team
      auth: ${secret:gcp_sa_key}
    incremental:
      mode: append_timestamp            # append_timestamp | cdc | full
      cursor: updated_at
      watermark_table: support_tickets_watermark
    target:
      antfly_table: corpus_support
      batch_size: 1000
      sync_level: write
    document:
      key_template: "ticket:{id}"
      fields:
        title:      "{subject}"
        content:    "{body}\n\n{resolution_notes}"
        metadata:   "{priority, product, customer_segment}"
        updated_at: "{updated_at}"
      docsaf:
        processor: wholefile            # or markdown/html if body is formatted
        entity_extraction: true
    lifecycle:
      retention: 180d
      tombstone_on_delete: true
```

Responsibilities:

- **Pull**: BigQuery Storage Read API (Arrow batches) or Snowflake streams;
  scheduled (cron, Cloud Scheduler) or webhook-driven (Pub/Sub on warehouse
  events).
- **Transform**: render each row through `document.fields` templates,
  optionally pipe through `pkg/docsaf` for unstructured text fields (PDF
  attachments referenced by URL, rich HTML bodies, etc.).
- **Upsert**: write to the configured Antfly table using the existing
  `client.BatchRequest` path, keyed by `key_template`.
- **Tombstone**: honor warehouse deletions via CDC or watermark reconciliation
  so orphan documents don't pollute retrieval.
- **Backfill**: explicit, budgeted (per CLAUDE.md: "explicit budgeted backfill
  unless opted in").

Auth: GCP workload identity for BQ; Snowflake key-pair via `${secret:...}`.
Never store warehouse credentials in Antfly config structs.

The bridge is the **only** code that talks to the warehouse. Antfly core and
the agent runtime never do. That keeps the blast radius small and the
tenancy model simple.

### 2. Corpus Tables (docsaf-loaded)

One or more Antfly tables per context, configured with:

- `full_text_v0` index over `title + content`
- `embeddings` index with chunked content (Termite chunker)
- `graph` index with `entity_extraction` enabled (see
  `work-log/planned/entity-relationship-extraction.md`)

Docsaf is the ingestion adapter. For warehouse rows that carry references to
unstructured content (e.g., `attachment_url`), the bridge resolves them
through `pkg/docsaf/source_*` before writing.

### 3. Entities Table (canonical)

Separate `entities` table per tenant or per business domain, per the entity
extraction plan. Cross-source identity resolution lives here:

- a support ticket mentioning "Acme" and a CRM account named "Acme Corp"
  both resolve to the same canonical `entity:acme:organization:01hz...`
- resolver is leader-only, namespace-scoped, and consumes the evidence
  outbox written by corpus-table enrichers

This is where the knowledge graph actually accumulates value over time.

### 4. Memory Tables (memoryaf)

`pkg/memoryaf` already creates memory tables with embedding + graph indexes.
The warehouse architecture doesn't change memoryaf's surface; it adds two
integration points:

- **Memory ↔ entity links**: when an agent stores a memory referencing a
  canonical entity, the memory-table graph index writes a cross-table edge
  (`memory:m-42 -> entities/entity-acme`, `type: mentions`). This makes
  memory retrievable from the entity side ("everything this agent knows
  about Acme") using the same batched cross-table hydration the entity plan
  already specifies.
- **Corpus as memory context**: retrieval agent tools can be configured to
  search memory and corpus tables in the same reasoning step, with a shared
  graph expansion over entities. The agent doesn't need to know which table
  a fact came from — only that it resolves to an entity it already knows.

### 5. Agent Runtime

The retrieval agent (`POST /agents/retrieval`) is the single entry point.
Tools are declared per agent configuration; typical tools for this
architecture:

- `search_memory` — memoryaf query
- `search_corpus(table=...)` — hybrid search over a corpus table
- `lookup_entity(name, type?)` — canonical entity search
- `expand_entity(id)` — graph traversal one-hop from a canonical entity
- `write_memory(namespace, content, entities?)` — memoryaf write

SSE events (`step_started`/`step_completed` from
`work-log/planned/retrieval-agent-tool-use.md`) already give the frontend
per-step observability. That same telemetry is the training signal for §6.

## The RL Loop

This is the piece that makes the system get better over time. Three things
are tunable:

1. **Extractor behavior** — which entity/relation labels to extract,
   confidence thresholds, which fields to extract from, which chunker to use.
2. **Graph index policy** — edge type weights, traversal depth, which
   relationship types to surface in retrieval.
3. **Ontology** — which entity types and relationship types to keep,
   promote, or retire based on actual agent usage.

### Signal Sources

The retrieval agent's SSE stream already emits the observable data we need:

- **Step-level**: which tool was called, latency, status, hits returned,
  which hits were cited in the final generation.
- **Outcome-level**: user rating on the response, follow-up question (a
  follow-up on the same entity is weak negative signal), explicit
  thumbs-up/down, regeneration.
- **Entity-level**: which canonical entities were hydrated, which were
  cited, which were ignored despite being retrieved.
- **Feedback API**: the same override records the entity resolver already
  consumes (`must_merge`, `must_not_merge`, `mention_belongs_to`,
  `attribute_verified`, `suppress_evidence`).

These are all durable Antfly records. No external telemetry store needed.

### Policy Service

A new leader-only job, `pkg/tuneaf` (or a module under memoryaf), runs a
policy-gradient or contextual-bandit loop over the signal table:

```
state     = { query_embedding, available_tools, recent_entities, tenant }
action    = { tool_sequence, retrieval_params (k, weights), expand_depth,
              extractor_config, edge_type_weights }
reward    = f(citation_hit, user_rating, follow_up_rate, latency_penalty,
              override_disagreement)
```

Start with the simplest thing that works:

- **Bandit over retrieval params** per (tenant, tool, query_type). Tunes
  BM25 vs embedding weight, top-k, rerank-k, and graph-expansion depth.
- **Offline extractor sweeps** — periodic budgeted backfills with
  candidate extractor configs (different labels, thresholds, models),
  scored on whether they produce mentions whose resolved entities later
  get cited in agent responses. Winning configs are promoted to the
  table's live `entity_extraction` config.
- **Ontology pressure** — edge types and entity types that are never
  traversed in successful retrievals get flagged for demotion; types that
  are consistently cited get boosted. Human-in-the-loop via the dashboard;
  never auto-retire schema.

Policy updates go through the same transform API as human feedback
(`kind: "graph"` transforms plus a new `kind: "policy"` for tuning records).
That keeps the audit trail uniform.

### Tuning Dashboard

The dashboard surface (Antfarm, built on `@antfly/components`) exposes:

- per-tool reward curves
- per-entity-type extraction precision/recall against human feedback
- ontology health: type usage, orphan rates, merge/split rates
- override diff: current policy vs. last N overrides
- "promote candidate config" button (writes the transform that swaps in
  the new `entity_extraction` block)

## GCP Deployment

- **Antfly**: GKE Autopilot, antfly-operator manages the StatefulSet.
  Persistent disks for Pebble; GCS for AFB portable backups.
- **Termite**: GKE node pool with GPU for embedders/rerankers; termite
  operator manages scaling. CPU-only pool for GLiNER/REBEL if latency is
  acceptable.
- **Warehouse Bridge**: Cloud Run job (scheduled) + Pub/Sub subscriber
  (for real-time CDC). Workload identity into BigQuery; Snowflake via
  private Secret Manager-stored key pair.
- **Networking**: Antfly is internal-only; the retrieval agent sits behind
  colony searchaf for customer-facing traffic, or behind the customer's
  own IAP for in-tenant deployments.
- **Tenancy**: one Antfly cluster per tenant for strong isolation, or one
  shared cluster with per-tenant tables + row-level auth (see
  `work-log/planned/row-level-auth.md`).

## Phasing

1. **Warehouse bridge MVP**: BigQuery-only, append-timestamp incremental,
   no CDC, no deletes. Writes to one corpus table. Tests via `e2e/`.
2. **Docsaf integration**: corpus rows run through docsaf processors for
   unstructured fields; entity extraction opt-in per context.
3. **Entity table + resolver**: plug into the entity-extraction plan.
   Cross-source identity resolution between two corpus tables.
4. **Memory ↔ entity links**: memoryaf writes cross-table graph edges to
   canonical entities. Retrieval tool set expanded.
5. **Telemetry table**: structured storage of retrieval agent traces,
   citations, feedback events.
6. **Bandit tuning**: simplest first — retrieval-parameter bandit per
   (tenant, tool).
7. **Extractor sweeps**: offline config candidates, scored on downstream
   citation lift.
8. **Ontology pressure + dashboard**: human-in-the-loop promotion of
   schema changes.
9. **Snowflake + CDC**: second warehouse backend, real-time updates.

## Open Questions

- **Tenancy vs shared resolver**: one entity resolver per tenant is simple
  but duplicates work when multiple tenants have overlapping public
  entities (companies, people). Is there a shared "public entities"
  namespace that tenant namespaces can alias into? Leaning: not in v1.
- **Warehouse schema drift**: column renames in the source view break
  document templates silently. Need a schema validator on each pull or a
  contract-test job that the data team signs off on.
- **Reward attribution**: a helpful answer often comes from a blend of
  memory + corpus + entity hydration. Naive credit assignment is noisy.
  Start with Shapley-style per-tool attribution on cited passages only,
  not the whole response.
- **Extractor cost at scale**: warehouse contexts can be very large. The
  bridge should support "extraction sampling" — extract entities from a
  sampled percentage, evaluate, then budget a full backfill.
- **PII**: warehouse rows routinely contain PII. The bridge needs a
  field-level redaction/masking config before writing to Antfly, and
  memoryaf needs a namespace-level policy that forbids certain entity
  types from being persisted to memory.

## Related Work

- `work-log/planned/entity-relationship-extraction.md` — the canonical
  entity lifecycle this design sits on top of.
- `work-log/planned/retrieval-agent-tool-use.md` — SSE step events that
  provide the training signal.
- `work-log/planned/row-level-auth.md` — needed for shared-cluster tenancy.
- `pkg/memoryaf/README.md` — current memoryaf surface and MCP server.
- `pkg/docsaf/` — existing document processors and entity enricher.
