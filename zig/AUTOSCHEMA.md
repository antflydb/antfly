# AutoSchema Knowledge Graphs

Schema-free knowledge graph construction inside the autograph pipeline:
LLM-driven triple extraction over entities *and events*, followed by
conceptualization ("schema induction") that grows an emergent `is_a` taxonomy
with no predefined ontology.

Based on **AutoSchemaKG: Autonomous Knowledge Graph Construction through
Dynamic Schema Induction from Web-Scale Corpora** (HKUST-KnowComp),
arXiv:2505.23628 — https://arxiv.org/abs/2505.23628. Reference implementation
(MIT): https://github.com/HKUST-KnowComp/AutoSchemaKG.

Investigated 2026-09-19 against `origin/main` at `d194bf7d0`. This is a
proposal; the extraction and resolution layers are expressible with shipped
machinery, while conceptualizer context, seeded PageRank, and retrieval-agent
wiring require the engine changes listed under "Engine Gaps".

## Why

AutoSchemaKG's results that matter for Antfly:

- **Events carry most of the signal.** Entity+event graphs reached ~95% on
  knowledge probes versus ~70% for entity-only graphs. Antfly's autograph today
  extracts entity-entity relations only.
- **An 8B model suffices for extraction.** LLaMA-3-8B-class extraction matched
  or beat pipelines using 70B models, so the `antfly` provider's local models
  are in range.
- **Concept edges improve retrieval.** Conceptualization edges (1B+ in their
  ATLAS-Wiki graph) provide alternative traversal pathways and retrieval
  targets when literal triples are ambiguous; combined with personalized
  PageRank retrieval (HippoRAG2-style, damping 0.9) they produced 12-18%
  multi-hop QA gains.
- **No schema authoring.** Concept induction achieved ~95% semantic alignment
  with human-crafted schemas with zero manual ontology work.

## Goals

- Express the paper's three extraction passes (entity-entity, entity-event,
  event-event) as autograph configuration using the existing `generator`
  producer with forced tool calling.
- Model events as ordinary documents in an `events` table via the shipped
  resolution/promotion pipeline, exactly as entities are modeled today.
- Add a conceptualization layer as a *recursive* autograph: enrichment and
  graph index configured on the `entities`/`events` tables themselves,
  promoting concept phrases into a `concepts` table and emitting `is_a` edges.
- Enable query-seeded personalized PageRank over the enriched graph and let
  the retrieval agent orchestrate it.

## Non-Goals

- No new node model. `GraphNodeModel` stays `{document, external}`
  (`pkg/antfly/src/storage/db/catalog/index_manager.zig`); entities, events,
  and concepts are ordinary documents in dedicated tables.
- No new artifact format. All stages emit the existing `extraction_graph`
  shape consumed by `runtimeGraphWritesFromArtifactValueAlloc`.
- No LLM-backed entity resolver. Deterministic key-minting first, the existing
  matcher-scorer for fuzzy resolution later (`RESOLUTION.md` non-goal holds).
- GLiNER2 (`extractor` producer) remains the default graph extractor. The LLM
  path is per-index opt-in: three generation calls per chunk is orders of
  magnitude more compute than one ONNX forward pass.

## Data Flow

```text
document write
  -> chunker (existing)
  -> generator enrichments kg_ee_v1 / kg_ev_v1 / kg_vv_v1   [config]
       forced tool call -> extraction_graph artifacts
  -> resolver/promoter (existing runtime)                   [config]
       entities table:  person/ada_lovelace
       events table:    event/<hash>
  -> graph materializer + replay (existing, unchanged)
       doc    --mentions-->        entities/...
       entity --works_at-->        entity
       entity --participates_in--> event
       event  --because-->         event
  -> promoter upsert to entities/events triggers second-order enrichment:
     conceptualize_v1 (generator)                           [config]
       -> concept phrases artifact
       -> resolver promotes to concepts table
       -> entity/event --is_a--> concepts/...
```

Query path: query entities/concepts resolved to seed nodes -> seeded
personalized PageRank (engine gap 2) -> `graph_metric_rerank` blend (shipped)
-> subgraph + evidence chunks -> existing RAG answer generation.

## Stage 1: LLM Triple Extraction

Three `generator` asset enrichments, one per extraction pass, following the
forced-tool-call pattern proven in `examples/epstein/main.go`
(`tool_output: "arguments"`, pinned `tool_choice`, `additionalProperties:
false`, enum-constrained relation types). Each emits one `extraction_graph`
artifact; the graph index merges them via its ordered `sources` array (up to
64 sources, existing precedence semantics from `GRAPH.md`).

```jsonc
// kg_ee_v1 — entity-entity
{
  "name": "kg_ee_v1",
  "kind": "asset",
  "producer_json": {
    "type": "generator",
    "config": {
      "provider": "antfly",
      "model": "gemma4-e2b",
      "prompt": "Summarize all important entities and the relations between them ...",
      "tool_output": "arguments",
      "tool_choice": { "type": "function", "function": { "name": "emit_graph" } },
      "tools": [{ "function": { "name": "emit_graph", "parameters": {
        "type": "object", "additionalProperties": false,
        "required": ["entities", "relations"],
        "properties": {
          "entities": { "type": "array", "items": { "type": "object",
            "required": ["id", "label", "text"],
            "properties": {
              "id": { "type": "string" },
              "label": { "type": "string" },
              "text": { "type": "string" } } } },
          "relations": { "type": "array", "items": { "type": "object",
            "required": ["type", "source", "target"],
            "properties": {
              "type": { "type": "string" },
              "source": { "type": "string" },
              "target": { "type": "string" },
              "evidence": { "type": "string" } } } }
        } } } }]
    }
  }
}
```

The other two passes differ only in prompt and schema constraints:

- **kg_ev_v1 (entity-event)**: events are emitted as entity items with
  `label: "event"` and `text` set to the normalized simple sentence
  ("Sam plays with his dog"); relations are `participates_in` from entity to
  event. The paper's prompt: identify events and the entities participating
  in them.
- **kg_vv_v1 (event-event)**: relation `type` is enum-constrained to
  `["before", "after", "concurrent", "because", "as_result"]` — the tool
  schema is the guardrail, same trick as epstein's relation labels.

Extraction prompts should be ported from the reference implementation's
prompt set (MIT-licensed) and evaluated per corpus before freezing.

Optional verification: the paper filters extracted triples for faithfulness.
"Is this triple entailed by its evidence span?" is textbook NLI
(premise = evidence, hypothesis = verbalized triple) and runs on the existing
NLI classification pipeline (`pkg/inference/src/pipelines/classification.zig`)
— no generative call needed. Typed-decision models (issue #810) are a later
drop-in for the same slot.

## Stage 2: Entities And Events Via Resolution

Events need no new node model: they flow through the shipped
resolver/promoter runtime (`storage/db/resolution_runtime.zig`,
`promotion_runtime.zig`) exactly as entities do, routed by label.

```jsonc
{
  "name": "knowledge_graph",
  "type": "graph",
  "sources": [
    { "artifact_name": "kg_ee_v1", "format": "extraction_graph",
      "mention_edge_type": "mentions" },
    { "artifact_name": "kg_ev_v1", "format": "extraction_graph",
      "mention_edge_type": "mentions" },
    { "artifact_name": "kg_vv_v1", "format": "extraction_graph" }
  ],
  "resolvers": [
    { "labels": ["person", "org", "place", "work"],
      "table": "entities",
      "key_template": "{{lower _entity.label}}/{{slug _entity.text}}" },
    { "labels": ["event"],
      "table": "events",
      "key_template": "event/{{hash _entity.text}}" }
  ]
}
```

Field names in the `resolvers` block follow the shapes exercised in
`e2e/antfly/test_resolution.py`; exact routing-by-label syntax needs API
review if it does not already exist. Everything downstream — durable edge
artifacts, generation binding, visibility inheritance, replay, split/merge —
is the existing autograph machinery, unchanged.

Fuzzy resolution ("A. Lovelace" vs "Ada Lovelace") upgrades later by swapping
the deterministic resolver for the matcher-scorer configuration with
confidence fusion (`catalog/resolver_catalog.zig`), per `RESOLUTION.md`.

## Stage 3: Conceptualization As A Recursive Autograph

Schema induction is the same autograph pattern applied to the `entities` and
`events` tables themselves. Because promoter upserts are normal document
writes, they trigger enrichments configured on those tables.

```jsonc
// enrichment ON the entities table
{
  "name": "conceptualize_v1",
  "kind": "asset",
  "producer_json": {
    "type": "generator",
    "config": {
      "provider": "antfly",
      "model": "gemma4-e2b",
      "prompt": "Given this entity and its context, produce 3 or more concept phrases at increasing levels of abstraction ...",
      "tool_output": "arguments",
      "tool_choice": { "type": "function", "function": { "name": "emit_concepts" } },
      "tools": [{ "function": { "name": "emit_concepts", "parameters": {
        "type": "object", "additionalProperties": false,
        "required": ["entities", "relations"],
        "properties": {
          "entities": { "type": "array", "minItems": 3, "items": {
            "type": "object",
            "properties": {
              "id": { "type": "string" },
              "label": { "const": "concept" },
              "text": { "type": "string" } } } },
          "relations": { "type": "array", "items": { "type": "object",
            "properties": {
              "type": { "const": "is_a" },
              "source": { "type": "string" },
              "target": { "type": "string" } } } }
        } } } }]
    }
  }
}

// graph index ON the entities table
{
  "name": "taxonomy",
  "type": "graph",
  "sources": [
    { "artifact_name": "conceptualize_v1", "format": "extraction_graph" }
  ],
  "resolvers": [
    { "labels": ["concept"],
      "table": "concepts",
      "key_template": "{{slug _entity.text}}" }
  ]
}
```

Properties that fall out of key-templating with zero new machinery:

- Identical phrases from different entities converge on the same concept
  document — the emergent taxonomy is deduplication by canonical key.
  "Black Mountain College" links to `concepts/college`, `concepts/school`,
  `concepts/liberal-arts-college`.
- Concept documents are ordinary documents: embeddable, full-text searchable,
  hydratable, and human-curatable (merging two concepts is a document edit).
- Concept-to-concept abstraction (`concepts/college --is_a-->
  concepts/institution`) is one more recursion of the same pattern on the
  `concepts` table, bounded by a depth/no-op guard.

V1 conceptualizes from the promoted document's own fields (canonical name,
label, aliases, provenance evidence snippets). The paper additionally samples
graph neighbors for context ("Black Mountain College" + "started by John
Andrew Rice" -> better abstractions); that requires engine gap 1 below.

## Retrieval

Shipped today: global centrality metrics (`graph/metrics.zig` — PageRank,
degree, eigenvector, HITS) with query-level projection, `where_metric`
filtering, ordering, and `graph_metric_rerank` blending a published metric
into search-hit scores (`specs/openapi/antfly/metadata.yaml`,
`graph/metric_rerank.zig`).

Missing for HippoRAG2/AutoSchemaKG-style retrieval:

1. **Query-seeded personalized PageRank.** `pageRankAlloc` accepts a
   warm-start seed for global recompute (`graph/warm_start.zig`) but not a
   per-query teleport vector. Proposal: a seeded variant where teleport mass
   is restricted to seed nodes, exposed on the existing `graph_metric` query
   field, bounded by the existing `work_budget` machinery:

   ```jsonc
   { "graph_metric": {
       "metric": "pagerank",
       "seed_nodes": ["entities/person/ada_lovelace", "concepts/college"],
       "damping": 0.9,
       "freshness": "fresh" } }
   ```

2. **Retrieval-agent orchestration.** `api/retrieval_agent.zig` forwards
   graph queries through `graph_search`/`tree_search` but never invokes
   `graph_metric` or `graph_metric_rerank` itself. The agent should resolve
   query entities/concepts to seed nodes, run seeded PageRank, and apply the
   metric-rerank blend. Concept nodes act as fallback seeds when literal
   entity match fails (the paper's "alternative pathway" role for concepts).

## Engine Gaps

| # | Gap | Status |
|---|-----|--------|
| 1 | Neighbor context for conceptualizer producers | DONE: `neighbor_context` on asset enrichments (`enrichment/neighbor_context.zig`) — deterministic adjacency block in the producer input, participates in skip-state hash; generator/extractor producers only |
| 2 | Seeded personalized PageRank | DONE: personalized teleport kernel (`graph/metrics.zig`), `GraphMetricRead.seed_nodes`+`damping` (max 128 seeds, fresh-only fail-closed, serverless fails closed) |
| 3 | Retrieval-agent seeding/rerank orchestration + wire-layer plumbing (`query_contract`, top-k, `graph_metric_rerank`, distributed fan-out) | in progress |
| 4 | Promoter upserts trigger enrichments on entity/event tables | verified by construction (promotion writes through routed `TableWriteSource`); e2e cascade coverage pending |
| 5 | Resolver routing-by-label | DONE: `labels` on `GraphResolverConfig`; labeled siblings disjoint (admission), catch-alls skip sibling-claimed labels at runtime, `{{ hash }}` key-template helper for event keys |

## Phases

1. **Prototype (config only).** Extend `examples/epstein` with the three
   extraction tool schemas and an `events` resolver; port and evaluate the
   reference prompts against local 8B-class models. Exit criterion: triple
   precision/recall on a labeled sample comparable to the paper's 88%+ F1.
2. **Taxonomy.** Configure the recursive conceptualizer autograph on
   `entities`/`events`; verify the promotion cascade (gap 4); land neighbor
   context (gap 1).
3. **Retrieval.** Seeded PageRank (gap 2) and retrieval-agent wiring (gap 3);
   evaluate multi-hop QA uplift against the BM25+vector baseline.
4. **Native shorthand.** Distill the validated prompt/schema bundle into a
   shorthand producer config (mirroring dense/sparse AKNN shorthand
   provisioning) so users opt in with one config block. Optional cost pass:
   NLI triple verification at build time, typed-decision path pruning (#810)
   at query time.

## Validation

- Tool-call outputs violating the pinned schema are rejected by the existing
  structured-output enforcement; malformed-but-valid-JSON payloads follow the
  paper's repair-and-retry approach in the producer.
- Concept recursion must be depth-bounded; conceptualizing a concept document
  more than N levels (default 1) is a no-op.
- Event key hashing must be stable across replays (normalized text input) so
  re-extraction converges on the same event documents.
- All existing autograph guardrails apply unchanged: source manifests,
  per-source budgets, generation-bound replay, fail-closed hydration.

## Regression Coverage (sketch)

- Three-source graph index merges entity-entity, entity-event, and
  event-event artifacts with declaration-order precedence.
- Event items promote to the `events` table and hydrate cross-table.
- Promoter upsert to `entities` triggers `conceptualize_v1` and materializes
  `is_a` edges to `concepts`.
- Identical concept phrases from distinct entities converge on one concept
  document.
- Seeded PageRank respects `work_budget` and returns deterministic top-k for
  a fixed seed set.
- Retrieval-agent `graph_search` applies metric rerank when seeds resolve and
  falls back to concept seeds when entity match fails.
