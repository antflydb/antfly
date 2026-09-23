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
  -> generator enrichments kg_ee_v1 / kg_events_v1          [config]
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

Two `generator` asset enrichments, one per extraction pass, following the
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

The second pass differs in prompt and schema constraints:

- **kg_events_v1 (events)**: events are emitted as entity items with
  `label: "event"`, `text` set to the normalized simple sentence ("Sam plays
  with his dog"), and a `predicate` field holding the main verb lemma; the
  participating entities ride the same artifact with `participates_in`
  relations, and the temporal/causal event-event relations are
  enum-constrained to
  `["participates_in", "before", "after", "concurrent", "because",
  "as_result"]` — the tool schema is the guardrail, same trick as epstein's
  relation labels. The paper runs entity-event and event-event as separate
  passes; they are folded into ONE here because compositional event identity
  needs every event mention to carry its participants. A participant-less
  event-event pass would key its events by sentence text, structurally
  disconnected from the participant-keyed nodes where mention and
  `participates_in` mass accumulates — its temporal/causal edges would
  connect nodes the seeded PageRank never reaches.

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
artifact identity and re-derives on change. The enrichment-layer blocker is
now CLOSED: asset-consumes-asset is plumbed end to end. An asset enrichment
with `source_artifact_name` naming another asset reads the upstream's
produced bytes as its source (field/template must be omitted;
media-locator producers — document_extraction, reader, transcriber — are
rejected at admission; existence, self-reference and cycles are validated
in both the API config walk and the catalog validator). Planning emits
asset requests upstream-first, the synchronous batch path overlays pending
same-batch producer writes and deletes, and the async runtime treats a
missing upstream as the normal null-source retire path — the per-document
replay record written when the upstream artifact lands re-plans the
consumer, so chains converge without bespoke retry state. Skip-state is
free: the consumer's source text IS the upstream bytes. What remains for
the verifier is only the `verifier` producer kind itself. Typed-decision
models (issue #810) are a later drop-in for the same slot.

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
    { "artifact_name": "kg_events_v1", "format": "extraction_graph",
      "mention_edge_type": "mentions" }
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
  than the sentence hash it replaces. Participants compose their CANONICAL
  key's final segment, not their raw text: the resolution runtime feeds
  each resolver the sibling resolutions over the same artifact (pending
  same-batch writes overlaid), and a committed resolution artifact
  re-drives its source for the other resolvers, so a matcher-scorer merge
  ("A. Lovelace" into `entity/ada_lovelace`) re-keys the events it
  participates in instead of leaving them pinned to the stale surface-form
  slug. The fan-back terminates because byte-stable recomputes publish no
  new resolution record. Re-keying never orphans: the promoter keeps a
  durable promoted-keys row per resolution artifact and diffs it on
  replay, so the previously promoted document becomes a merged_into
  redirect (the matcher-scorer convention) in the same atomic batch as
  the survivor's upsert; the entity sink's merge transform SETS the
  redirect on the live document and clears stale redirects on live
  promotions. Remaining coarseness: two genuinely different events with
  identical participants and predicate merge (no time bucketing yet).

Post-extraction junk control: `min_confidence` on a resolver floors mention
admission (below-floor mentions mint no key, no mention edge, and relation
endpoints referencing them are withheld). Score-carrying extractors (GLiNER)
should set it; LLM lanes default their mentions to confidence 1.0.

One shape note from implementation: `source_artifact` is required and
singular per resolver, so the events/catch-all pair above is declared per
artifact in practice — a labeled `event` resolver plus a catch-all where an
artifact carries both classes, each with its own `resolution_artifact` (see
`examples/epstein/autoschema.go` for the working three-resolver layout over
two artifacts). Everything downstream — durable edge artifacts, generation
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
- The server query executors now expand too. A single-group graph query
  admitted for local execution carries a complete-snapshot scope from the
  API read source (`graphScopedSearchRequest` sets
  `SearchRequest.graph_owning_table` + `graph_index_complete_snapshot`;
  the scope rides the graph executor vtables down to `TraversalRules`,
  `PathFindOptions`, and `MatchOptions`), so traversal, paths, and MATCH
  walk doc -> entity -> event in one local execution — the same
  single-index justification as the embedded entry points, now proven by
  the group count instead of assumed. The local MATCH edge readers serve a
  tagged node's adjacency by bare key under that scope and canonicalize
  self-table tags (`LocalGraphIndexEdgeReader.canonicalizeTable`,
  mirroring the distributed reader).
- The multi-shard routing disagreement is CLOSED by source-table fanout:
  the distributed coordinator still routes an entity-tagged frontier node
  to the tagged table's identically named index (its own locally-owned
  adjacency) but ALSO fans the node out across the SOURCE table's groups,
  where the entity-sourced edges live as owner-scoped document rows
  (`batchFrontierByGroup`; the weighted shortest-path search mirrors it
  with per-node expand routes). Incoming probes consult the source table's
  groups the same way, because reverse rows are colocated with the
  source-owned edge rows. A tagged table without the index no longer
  silently terminates the node. The hop merge deduplicates by canonical
  {table, key} identity, so overlapping routes collapse.

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
| 8 | Convergent identity layer | DONE: label-free `entity/{{ slug _entity.text }}` keys (label rides the document), possessive-stripping slug, compositional `event/{{ hash _entity.event_identity }}` event keys (participants + predicate, computed in lib/resolver, degrades to sentence text; requires at least one participant so a bare predicate never becomes a corpus-wide hub), `min_confidence` mention-admission floor on GraphResolverConfig. Participants compose their CANONICAL keys: the resolution runtime injects sibling resolutions (same-batch overlay + committed artifacts) and a committed resolution re-drives its source for the other resolvers, so entity merges re-key the events they touch. The entity-event and event-event passes are folded into ONE `kg_events_v1` pass so every event mention carries the participants its identity needs — a participant-less event-event pass would mint sentence-keyed nodes disconnected from the participates_in topology |
| 9 | Cross-table traversal | DONE: self-table `target_table` tags canonicalize away (`TraversalRules.owning_table` locally, `canonicalizeTable` in the local MATCH readers); the direct storage entry points (embedded Lite) and the server executors expand THROUGH cross-table nodes — the API read source proves snapshot completeness for single-group tables and threads the scope down the executor vtables; the distributed coordinator fans an entity-tagged frontier across the SOURCE table's groups (owner-scoped entity-sourced rows) in addition to the tagged table's route, for expansion, weighted paths, and incoming probes. `paths.zig` keys identity by table. See Stage 2 |
| 10 | Coverage beyond embeddings | DONE: asset and chunk producers record produced/skipped outcomes at every terminal point in both runtimes, attributed to their graph and full_text consumers ONLY (a produced chunk says nothing about its dense/sparse consumers, whose embedding lanes settle their own outcomes); the index-status coverage block now serves artifact-sourced graph/full_text indexes (direct-document projections stay coverage-silent instead of eternally pending); capi index stats carry the counters, enrichment stats carry `stalled`/`stall_reason`/`skipped_source_count`, and `fatal_error_count` is the durable terminal-request counter. The autoschema e2e and live run observe the pipeline through this block |
| 11 | NLI triple verification stage | DESIGNED (Stage 1 section): `verifier` asset producer over the extraction artifact through the shipped `/ai/v1/extract` classification surface. The enrichment-layer blocker is CLOSED: asset-consumes-asset is plumbed end to end (admission validates the chain, planning orders upstream-first, both runtimes read upstream bytes as the producer source, replay converges missing-upstream consumers); only the `verifier` producer kind itself remains |

## Phases

1. **Prototype (config only).** Extend `examples/epstein` with the
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
