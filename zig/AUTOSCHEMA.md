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

Verification (designed, not yet implemented): the paper filters extracted
triples for faithfulness. "Is this triple entailed by its evidence span?" is
textbook NLI (premise = evidence, hypothesis = verbalized triple). The
concrete native design: a new `verifier` asset producer that consumes the
extraction artifact (`source_artifact_name` on an asset enrichment), scores
each relation's (evidence, verbalized triple) pair through the SHIPPED
classification surface — `/ai/v1/extract` with
`schema.classifications[].hypothesis_template` + `options.threshold`; the
NLI cross-encoder in `pkg/inference/src/pipelines/classification.zig` is
only reachable through that endpoint or the standalone bridge, never
directly — and writes a filtered/annotated artifact in the same
extraction_graph shape that the graph index consumes instead. Relation
`confidence` already becomes edge weight at materialization, so
annotate-mode additionally enables query-time `min_weight` filtering for
free. The threshold lives in `producer_json.config` so it participates in
artifact identity and re-derives on change. Blockers, all in the enrichment
layer: asset-consumes-asset is currently accepted at admission but silently
dropped in planning (the `.asset` arm of `validateEnrichmentConfig` never
reads `source_artifact_name`, and the asset planning loop never copies it),
and asset requests have no dependency ordering, so the verifier must either
defer-and-retry off `changed_artifact_keys` or planning must become
topological. Typed-decision models (issue #810) are a later drop-in for the
same slot.

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
    { "table": "entities",
      "key_template": "entity/{{slug _entity.text}}" },
    { "labels": ["event"],
      "table": "events",
      "key_template": "event/{{hash _entity.event_identity}}" }
  ]
}
```

Two identity rules learned the hard way (both shipped):

- **Entity keys are label-free.** The extractor's label is a per-chunk
  guess; baking it into the key splits "Antfly the organization" and
  "Antfly the product" into nodes that can never be joined, and a graph
  whose nodes never converge gives seeded PageRank nothing to concentrate
  on. The label rides the promoted entity document (`entity_type`) and the
  mention, not the key. The accepted trade-off is homonym collapse
  ("washington" the person vs the place), which the matcher-scorer phase
  exists to relitigate — a merged node is visible and curable, a split node
  silently starves retrieval. The `slug` helper also strips English
  possessives so "Epstein's island" and "Epstein island" converge.
- **Event keys compose participants + predicate, because replay-stable is
  necessary but NOT sufficient.** Hashing the normalized sentence is stable
  across replays yet mints a distinct event per wording, so
  `participates_in` mass never accumulates on a shared node across
  documents. `_entity.event_identity` (computed by lib/resolver from the
  artifact's relations) is the sorted slugs of the related non-event
  mentions plus the extractor-asserted `predicate` (main-verb lemma, an
  optional mention field the extraction tool schemas now request),
  degrading to the raw sentence text when either is missing — never weaker
  than the sentence hash it replaces. Remaining coarseness: two genuinely
  different events with identical participants and predicate merge (no
  time bucketing yet).

Post-extraction junk control: `min_confidence` on a resolver floors mention
admission (below-floor mentions mint no key, no mention edge, and relation
endpoints referencing them are withheld). Score-carrying extractors (GLiNER)
should set it; LLM lanes default their mentions to confidence 1.0.

One shape note from implementation: `source_artifact` is required and
singular per resolver, so the events/catch-all pair above is declared per
artifact in practice — a labeled `event` resolver plus a catch-all where an
artifact carries both classes, each with its own `resolution_artifact` (see
`examples/epstein/autoschema.go` for the working four-resolver layout over
three artifacts). Everything downstream — durable edge artifacts, generation
binding, visibility inheritance, replay, split/merge — is the existing
autograph machinery, unchanged.

Relation edges are entity-sourced with document ownership: a relation whose
endpoints reference extraction entities materializes only once resolution
lands, with both endpoints rendered as resolver-minted canonical keys
(`person/ada_lovelace --works_at--> org/antfly`). The graph edge artifact key
embeds the topological source as an optional trailing component while its
leading component remains the producing document, so routing, retirement,
replacement manifests, and split ranges stay owner-scoped
(`GraphEdgeWrite.owner`, `graphEdgeArtifactKeyWithSourceAlloc`). Before
resolution such relations are deliberately absent rather than rendered with
local mention ids; endpoints matching no extraction entity keep the
external-node string passthrough, and legacy inline endpoint objects keep the
document source. Resolved endpoints carry their home table in edge metadata
(`target_table`), the same cross-table tag mention edges use — including
when the source declares a custom metadata template (the tag is prepended
to the rendered object unless the template sets its own). Seeded
personalized PageRank consumes the entity-sourced topology directly (the
kernel reads the raw edge snapshot). Traversal:

- A `target_table` tag naming the index-owning table itself canonicalizes
  to null (`TraversalRules.owning_table`), matching the distributed
  executor's `canonicalGraphNodeTable`, so a self-table tag never stops
  expansion or splits node identity.
- The direct storage entry point (embedded Lite, `traverseEdges`) opts into
  `expand_cross_table_local`: entity-sourced edges are document-owned rows
  in the SAME index, so expanding THROUGH a cross-table node there is a
  same-snapshot single-index read, and an embedded walk crosses
  doc -> entity -> entity/event in one traversal. Node identity stays
  table-qualified for dedup and results.
- The server query executors keep the terminal behavior: the local executor
  still stops at tagged nodes (single-group opt-in is a follow-up — the
  drain plumbing from `requiresDistributedGraphCoordinator` down to
  `TraversalRules` — and a caller can already re-seed a second traversal at
  the returned entity key, which always expands; the autoschema e2e walks
  the chain exactly that way). The DEEP open question for the multi-shard
  entity node model (GRAPH.md): the distributed router routes an
  entity-tagged frontier to the entity TABLE's identically named graph
  index, while the entity's relation edges live in the DOCUMENTS table's
  index scattered by producing document — owner-scoped storage and
  table-scoped routing disagree about where an entity node's edges live,
  and reverse-direction completeness needs a span fanout, not an owner
  route. Also unresolved in the local path engines: `paths.zig` keys its
  visited set by bare key (namespace aliasing the traversal engine already
  fixed via table-qualified identity) and local MATCH readers return empty
  streams for tagged nodes.

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
| 3 | Retrieval-agent seeding/rerank orchestration + wire-layer plumbing | DONE: `seed_nodes`+`damping` on `graph_metric`/`graph_metric_rerank` wire shapes; personalized top-k and rerank readers; serverless and cross-shard personalized requests fail closed (422); the retrieval agent auto-seeds a fresh rerank from literal graph-search start keys and degrades to unseeded when none resolve |
| 4 | Promoter upserts trigger enrichments on entity/event tables | verified by construction (promotion writes through routed `TableWriteSource`); e2e cascade coverage pending |
| 5 | Resolver routing-by-label | DONE: `labels` on `GraphResolverConfig`; labeled siblings disjoint (admission), catch-alls skip sibling-claimed labels at runtime, `{{ hash }}` key-template helper for event keys |
| 6 | Positional (id-less) extractor payloads | DONE: GLiNER2.5 boundary payloads carry no per-entity ids and reference entities via each relation's `entity_index`; `lib/resolver` assigns id-less mentions their decimal array position as the local id, and the graph materializer resolves `entity_index` endpoints through the injected `_entities` resolution map under the same identity (`graphArtifactEntityAtIndex` + runtime mirror). `examples/dogfood` (Lite) and the `kg_gliner_v1` lane in `examples/epstein` build on this |
| 7 | Lite resolver registration | DONE: native Lite handles register a graph config's inline `resolvers` array on `antfly_db_add_index_json` (add/update only, mirroring nested-enrichment registration); resolution runs locally, promotion stays cleanly blocked without a cross-table entity sink and `runUntilIdle` drains around it. AddIndex is all-or-nothing: a rejected admission or partial enrichment/resolver registration restores the pre-call catalog |
| 8 | Convergent identity layer | DONE: label-free `entity/{{ slug _entity.text }}` keys (label rides the document), possessive-stripping slug, compositional `event/{{ hash _entity.event_identity }}` event keys (participants + predicate, computed in lib/resolver, degrades to sentence text), `min_confidence` mention-admission floor on GraphResolverConfig |
| 9 | Cross-table traversal, local | DONE (scoped): self-table `target_table` tags canonicalize away (`TraversalRules.owning_table`); the direct storage traversal (embedded Lite) expands THROUGH cross-table nodes in the same index (`expand_cross_table_local`, identity stays table-qualified); custom metadata templates no longer drop the `target_table` tag. Server-executor opt-in, `paths.zig` identity-keyed visited sets, and the multi-shard entity-node routing model (owner-scoped storage vs table-scoped routing) remain follow-ups — see Stage 2 |
| 10 | Terminal-failure coverage | EXISTS in the engine (durable per-document coverage markers `produced/skipped/terminal_failed`, artifact repair ledger with per-doc `generation_error`, index-status coverage JSON); this branch surfaces it to embedded consumers (capi index stats carry the coverage counters; enrichment stats carry `stalled`/`stall_reason`/`skipped_source_count`) and documents `fatal_error_count` as the durable terminal-request counter. Follow-ups: hoist the server status coverage block beyond `index_type == .embeddings`; artifact-scoped coverage for producers with no consuming index (today: the repair ledger is the answer) |
| 11 | NLI triple verification stage | DESIGNED (Stage 1 section): `verifier` asset producer over the extraction artifact through the shipped `/ai/v1/extract` classification surface; blocked on asset-consumes-asset enrichment plumbing (admission reads it, planning drops it) and producer dependency ordering |

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
