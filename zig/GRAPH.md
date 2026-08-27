# Graph Indexing Design

## Summary

Graph indexes declare the enrichment inputs they need, then consume the
resulting artifacts through Antfly's existing managed-index replay path.

V1 does not introduce a separate graph recovery protocol or move graph index
rows directly into the Raft state machine. Antfly uses this boundary:

```text
Rafted primary store and replay journal
  -> value-only artifacts
  -> managed-index replay
  -> private graph index stores
```

The graph index owns the dependency on graph inputs. The enrichment pipeline
produces reusable value-only artifacts. Artifacts do not auto-create graph
indexes, and graph indexes do not own artifact rows directly.

For V1, extracted relation artifacts materialize into existing graph edge
artifact rows. The existing graph replay path then applies those graph edge
artifact rows to the graph index. Crash recovery remains Antfly's normal
recovery: primary-store durability, replay journal, enrichment state hashes,
managed-index applied sequence, and reverse-index rebuild from owned outgoing
edges.

## Goals

- Let a graph index declare an enrichment dependency for the artifact it needs.
- Consume extracted relation/entity JSON from `_artifacts`.
- Reuse existing `EnrichmentConfig`, `producer_json`, and asset producer runtime
  shapes.
- Reuse existing graph edge artifact rows as the durable source of graph edge
  truth.
- Reuse existing managed graph replay to update private graph stores.
- Keep graph queries model-free. Queries should never call extractors, readers,
  generators, or transcribers synchronously.
- Keep V1 document-key compatible with current graph query, hydration, identity,
  split, and merge behavior.

## Non-Goals For V1

- No custom graph reconciliation protocol.
- No direct graph-row Raft apply path beyond ordinary primary-store writes.
- No required cross-shard graph projections.
- No global entity graph routing.
- No built-in entity resolution.
- No true multigraph storage unless graph edge keys are extended with a stable
  edge id.

## Current Antfly Shape

The implemented pieces are:

- `_edges` and explicit graph writes are converted into graph edge artifact rows.
- Visible graph edge artifact keys retain the existing logical edge identity:

```text
(doc_key, "graph", index_name, edge_type, target_doc_key)
```

- Managed graph replay watches changed graph edge artifact keys and calls
  `applyGraphMutationsByName`.
- `GraphIndex` stores forward and reverse private graph rows.
- Reverse rows are derived from owned forward rows and can be rebuilt.
- Managed index applied sequence and the replay journal provide catch-up and
  crash recovery.
- Asset enrichments already support model-backed producers through
  `producer_json`.
- Graph configs declare one `source` or up to 64 ordered `sources`, and the
  graph materializer renders their selected artifact values into edge rows.
- A generation-bound manifest per document, index, and artifact source retains
  the complete desired edge keys and payloads. These manifests provide source
  ownership without changing the public graph edge identity.
- Reconciliation applies source order as deterministic precedence and restores
  the next source's retained payload when a winner disappears.

The remaining identity limitation is that visible graph edge keys do not
include `logical_edge_id`, so duplicate relations with the same
source/target/type collapse.

## V1 Data Flow

```text
table/index open
  -> graph config declares source artifact/enrichment dependency
  -> IndexManager ensures shorthand enrichments or validates user-defined ones

document write
  -> enrichment runtime produces _artifacts.<artifact_name>
  -> changed asset artifact key is recorded in replay journal

managed replay
  -> graph materializer reads changed source artifact
  -> renders relation items into graph edge artifact writes/deletes
  -> graph edge artifact changes are durable in the primary store
  -> existing graph replay applies graph edge artifacts to GraphIndex

query
  -> graph query reads GraphIndex private stores
  -> visibility and identity checks remain in the existing query path
```

The important design decision is that graph edge artifact rows are the
authoritative materialized edge state for V1. The private graph index is a
replayable index over those rows.

## Managed Enrichment Dependencies

A graph index may reference a user-defined enrichment or include shorthand
configuration that materializes into a normal enrichment catalog entry. This
matches dense/sparse AKNN behavior: the index depends on an enrichment, and the
enrichment produces artifacts.

The shorthand shape should reuse Antfly's public enrichment config fields:

```json
{
  "name": "relations_graph",
  "type": "graph",
  "source": {
    "artifact": "relations_v1",
    "path": "$.relations[*]",
    "format": "extraction_relation"
  },
  "artifact": {
    "name": "relations_v1",
    "kind": "asset",
    "source": {
      "type": "field",
      "value": "body"
    },
    "content_type": "application/json",
    "producer_json": {
      "type": "extractor",
      "config": {
        "provider": "antfly",
        "model": "relations"
      }
    }
  }
}
```

Rules:

- If the named enrichment already exists with a compatible config, the
  graph index reuses it.
- If the enrichment is missing and shorthand enrichment config is present, table
  open/index install creates the enrichment before graph materialization starts.
- If the enrichment is missing and shorthand config is absent, validation rejects
  the graph index.
- If the same enrichment name exists with incompatible `kind`, `field`,
  `template`, `content_type`, or `producer_json`, validation rejects the table
  config.
- Multiple graph indexes may share one enrichment when the enrichment config is
  identical.
- Enrichment producers remain graph-agnostic. They only write value bytes.

Implementation should mirror existing dense/sparse shorthand provisioning in
`IndexManager.ensureShorthandEnrichments`, adding a `.graph` branch and an
`ensureAssetEnrichment` helper.

Inline graph index enrichment config is only creation shorthand. Once it is in
the catalog, it is a normal enrichment resource. Lifecycle decisions are based on
catalog references, not on who originally created the enrichment:

- Deleting an enrichment is rejected while any index depends on it.
- Deleting an index may remove its shorthand-created enrichment only when no
  other index references that enrichment and the enrichment was not user-defined.
- Updating an enrichment config is rejected while dependent indexes require the
  old config, unless the update is a compatible no-op or an explicit rebuild plan
  updates the dependents.
- Referrers should be derived from current index configs on catalog load/update.
  Cached referrer lists may be exposed for status/UI, but they are not the source
  of truth.

## Source Families

Graph indexes accept direct document edges and artifact-backed edge streams.
The public API keeps those forms distinct instead of using a source-kind
discriminator.

Document field edges:

```json
{
  "name": "doc_graph",
  "type": "graph"
}
```

Documents may write explicit `_edges`; `edge_types[].field` remains the typed
configuration for deriving edges from another document field.

Artifact relation edges:

```json
{
  "name": "relations_graph",
  "type": "graph",
  "source": {
    "artifact": "relations_v1",
    "path": "$.relations[*]",
    "format": "extraction_relation"
  }
}
```

The graph materializer registers dependency interest in the source artifact
name. When that artifact changes for a document, the materializer reads the
artifact value, selects items with `path`, renders edges, and replaces the graph
edge artifact rows for that document/index/source.

`source` is the single-source convenience form. `sources` accepts up to 64
uniquely named artifact streams with deterministic precedence: earlier sources
win edge-identity collisions. Per-source manifests retain ownership so deleting
a winning source restores the next source without a full graph rescan.

## Template Mapping

Templates convert artifact items into graph edge artifact rows.

Example:

```json
{
  "source": {
    "artifact": "relations_v1",
    "nodes": {
      "model": "document",
      "source": "{{ _doc.key }}",
      "target": "{{ _item.target.document_id }}"
    },
    "edge": {
      "type": "{{ _item.type }}",
      "weight": "{{ default _item.confidence 1.0 }}",
      "metadata": {
        "source_text": "{{ _item.source.text }}",
        "target_text": "{{ _item.target.text }}",
        "evidence": "{{ _item.evidence.text }}"
      }
    },
    "context": {
      "doc_fields": ["tenant_id", "visibility"]
    }
  }
}
```

Templates receive:

```text
_doc.key
_doc.value.<field>
_artifact.name
_artifact.content_type
_artifact.value
_item
_item_index
```

`_doc.value.<field>` is only available for fields declared in
`context.doc_fields`. This makes dependency tracking explicit.

For extraction relation payloads, endpoint references may point into the same
artifact's `_entities` array. The materializer resolves those references before
template rendering.

## V1 Node Semantics

V1 should default to document nodes because Antfly's current graph query,
hydration, identity-generation, split, and merge paths are document-key based.

Supported V1 modes:

`document`

Both `source` and `target` render document keys. This is the default and should
be the first implemented path.

`external`

Templates may render non-document ids such as `entity:person:ada_lovelace`, but
those nodes are not hydrated as Antfly documents and must inherit visibility from
the producer document. Query responses may return them as graph node ids only.

Deferred modes:

- `entity` with global/hash routing.
- `mention` nodes with span-aware traversal.
- `mixed` node models that require query-time hydration decisions.

## Entity Documents And Resolution

The long-term product shape for cross-document entity graphs is to make
canonical entities normal Antfly documents, usually in a dedicated entity table:

```text
entities/person/ada_lovelace
entities/org/antfly
```

Then extracted relationships point at real document refs:

```text
doc:article-123 --mentions--> entities/person/ada_lovelace
doc:article-456 --mentions--> entities/person/ada_lovelace
entities/person/ada_lovelace --works_at--> entities/org/antfly
```

This keeps hydration, visibility, indexing, backup/restore, document identity,
split, merge, and distributed transactions inside the normal Antfly model. The
graph index should not directly invent canonical entities. Instead, it should
declare the extraction and resolution dependencies it needs, then consume
resolved entity document refs.

The durable pipeline is:

```text
source document
  -> extraction artifact: mentions, local entities, relations, evidence
  -> resolution artifact: local entity ids mapped to canonical entity docs
  -> graph edge artifacts: document/entity relationship edges
  -> graph index replay
```

This does not have to land all at once. V1 can keep the existing graph plan:

```text
source document
  -> extraction artifact
  -> graph materializer
  -> graph edge artifacts
  -> graph index replay
```

The V1 artifact contract should preserve the structure needed for later entity
resolution:

- local entity ids
- relation endpoints by local entity id
- mention spans and evidence
- optional canonical identity hints

Entity resolution and promotion can then be added as separate layers:

```text
resolver:
  extraction artifact -> resolution artifact

promoter:
  resolution artifact -> entity document upserts

graph materializer:
  extraction artifacts + resolution artifacts -> graph edge artifacts
```

The resolver decides identity, such as mapping local `e0` to
`entities/person/ada_lovelace`. The promoter performs the durable entity
document writes. The graph materializer remains responsible for rendering
extracted relation endpoints and resolution-backed provenance endpoints into
graph edge artifact rows.

Extraction artifacts remain source-document local:

```json
{
  "entities": [
    {
      "id": "e0",
      "label": "person",
      "text": "Ada Lovelace",
      "spans": [{ "start": 10, "end": 22 }]
    },
    {
      "id": "e1",
      "label": "org",
      "text": "Antfly"
    }
  ],
  "relations": [
    {
      "type": "works_at",
      "source": { "entity_id": "e0" },
      "target": { "entity_id": "e1" },
      "evidence": { "text": "Ada Lovelace works at Antfly" }
    }
  ]
}
```

Resolution artifacts map local extraction ids to canonical entity documents:

```json
{
  "entities": [
    {
      "local_id": "e0",
      "doc_ref": {
        "table": "entities",
        "key": "person/ada_lovelace"
      },
      "confidence": 0.98
    }
  ]
}
```

Entity records are ordinary Antfly documents:

```json
{
  "entity_type": "person",
  "canonical_name": "Ada Lovelace",
  "aliases": ["Ada", "A. Lovelace"],
  "provenance": [
    {
      "table": "articles",
      "key": "article-123",
      "artifact": "relations_v1",
      "local_id": "e0"
    }
  ]
}
```

Graph index shorthand may declare this dependency chain:

```json
{
  "name": "knowledge_graph",
  "type": "graph",
  "source": {
    "artifact": "relations_v1",
    "format": "extraction_graph"
  },
  "artifact": {
    "name": "relations_v1",
    "kind": "asset",
    "source": {
      "type": "field",
      "value": "body"
    },
    "content_type": "application/json",
    "producer_json": {
      "type": "extractor",
      "config": {
        "provider": "antfly",
        "model": "relations"
      }
    }
  },
  "entities": {
    "table": "entities",
    "key_template": "{{ lower _entity.label }}/{{ slug _entity.canonical_text }}",
    "resolver": {
      "type": "deterministic"
    }
  },
  "edges": {
    "mentions": true,
    "relations": true
  }
}
```

The first resolver should be deterministic: render a canonical entity key from
the extracted entity label/text and upsert that entity document. Later resolver
configs can be model-backed:

```json
{
  "resolver": {
    "type": "model",
    "provider": "antfly",
    "model": "entity-resolver",
    "candidate_search": {
      "table": "entities",
      "index": "entity_name_embedding"
    }
  }
}
```

Entity resolution should run outside Raft in enrichment/materializer workers.
Only durable writes go through Antfly's normal write paths:

1. Extractor produces a relation artifact on the source document shard.
2. Resolver reads the extraction artifact and computes canonical entity refs.
3. Resolver uses normal writes or distributed transactions to upsert entity docs
   and store a resolution artifact.
4. Graph materializer reads extraction plus resolution artifacts and writes graph
   edge artifacts.
5. Existing graph replay indexes those graph edge artifacts.

Internally, resolved entity endpoints should use a document reference shape even
if the first implementation only supports same-table graph hydration:

```json
{ "table": "entities", "key": "person/ada_lovelace" }
```

Using `DocRef` rather than raw string ids keeps cross-table entity graphs
possible without redesigning extraction, resolution, or graph materialization.

Recommended phases:

1. Deterministic entity documents: extract entities/relations, render canonical
   keys, upsert entity docs, and write document-to-entity mention edges plus
   entity-to-entity relation edges.
2. Resolver-backed entity documents: candidate search over existing entities,
   resolver chooses an entity or creates one, and the resolution artifact records
   the choice.
3. Entity merge/split: entity docs can carry `merged_into`, resolution artifacts
   can be replayed, and graph materialization rewrites stale entity edges.

## Replacement Semantics

Replacement happens at the graph edge artifact layer.

For each `(producer_doc_key, graph_index_name, source_artifact_name,
config_generation)` scope:

1. Read the current source artifact value and render its desired edge keys and
   payloads.
2. Persist that complete source manifest with the graph index generation.
3. Read the manifests for the index's configured sources in array order and
   choose the first payload for every logical edge key.
4. Diff the selected visible set against graph edge artifact rows, deleting
   stale rows and upserting changed winners in the primary store.
5. Let the existing graph replay path apply changed graph edge artifact keys to
   private graph stores.

The existing graph edge artifact key is:

```text
(doc_key, "graph", index_name, edge_type, target_doc_key)
```

Because this key has no logical edge id, V1 replacement collapses duplicate
relations with the same source document, target document, and edge type. If true
multigraph support is required, extend the graph edge artifact key and
`GraphEdgeWrite`/`GraphEdgeDelete` with `edge_id` before depending on multigraph
semantics.

Clearing every document/index edge before rewriting one source is unsafe and is
not used. Source manifests are the ownership boundary. Because they retain
payloads, deletion or mutation of a winning source can promote the next source
without rereading every source artifact. Reconciliation coalesces repeated
mutations and scans each affected state prefix once.

Manifests and graph edge payloads are generation-bound so replay from a retired
index cannot mutate a same-name replacement. Released v0.2.0 key-only manifests
and generation-less edge payloads are accepted as migration input and rewritten
on the next materialization. Index retirement durably pages through both edge
payloads and manifests before same-name recreation is admitted.

Each source manifest is bounded by entry and byte limits. Reconciliation also
has aggregate entry and byte budgets so many overlapping sources cannot create
unbounded work even when the visible edge count is small. Exceeding a guardrail
records terminal repair debt instead of retrying an impossible payload forever.

## Visibility And Identity

Graph artifact-derived edges inherit visibility from the producer document unless
the graph index explicitly declares itself public.

Document-node edges continue to use the existing document identity and visibility
guards. External-node edges must store enough metadata to trace the producer
document and visibility partition, because the target node may not correspond to
a document row.

V1 should fail closed:

- If a graph query needs document hydration for an external node, return the node
  id without hydration or reject that query shape.
- If a producer document is deleted or hidden, suppress its graph-derived edges.
- If document identity generation changes, replay should clean old graph edge
  artifact rows for that producer document/index.

## Query Execution

For V1, graph queries should continue to read `GraphIndex` private stores.

Allowed:

- Outbound/inbound/both traversal for document-key nodes using existing graph
  stores.
- Traversal over external node ids when no document hydration is required.
- Existing distributed graph expansion for stamped document result refs.

Rejected or deferred:

- Global entity lookup without a projection.
- Hash-routed entity traversal.
- Query shapes that require hydrating an external node as a document.
- Required cross-shard reverse/global projections.

Result semantics should preserve existing graph behavior:

- Edge rows are keyed by source/target/type in V1.
- Frontier nodes may be deduplicated for expansion.
- Path state should preserve the edge sequence used to reach a result.

## Recovery And Rebuild

V1 recovery uses existing Antfly mechanisms:

- Primary store durability for document rows, asset artifacts, and graph edge
  artifacts.
- Replay journal target hints and changed artifact keys.
- Enrichment state hashes for model-backed asset skip behavior.
- Managed index applied sequence for graph replay catch-up.
- Graph reverse rebuild from owned outgoing edges.
- Split/merge cutover code that copies graph edge ranges and replays managed
  indexes.

If the process crashes:

- Before the asset producer writes: the enrichment request remains replayable.
- After the asset producer writes but before graph materialization: changed
  artifact replay schedules graph materialization again.
- After graph edge artifact writes but before private graph apply: managed graph
  replay catches up from changed graph artifact keys.
- During private graph apply: graph replay is idempotent over graph edge artifact
  rows, and reverse rows can be rebuilt from forward rows.

No graph-specific crash recovery protocol is needed for V1.

## Future Extensions

True multigraph support:

- Add `edge_id` to graph edge artifact keys.
- Add `edge_id` to `GraphEdgeWrite`, `GraphEdgeDelete`, and graph query result
  edges.
- Use deterministic edge id templates based on extracted relation id, spans, or
  evidence identity.

Entity graph support:

- Add explicit entity-node semantics and hydration behavior.
- Decide whether entity nodes are producer-owned, tenant-owned, or hash-routed.
- Add optional entity resolution as a separate artifact or projection layer.

Cross-shard projections:

- Add reverse/global/hash projections only when query requirements justify them.
- Use existing distributed transaction machinery for required projections.
- Treat accelerator projections as rebuildable from graph edge artifacts.

Direct Raft graph state:

- Only consider this if private graph replay is not sufficient for a concrete
  correctness or latency requirement.

## Validation

Open/index validation rejects:

- Unknown graph source fields, including public `source.kind` discriminators.
- Artifact source without `artifact`.
- Artifact source with an unsupported `path` or `format`.
- Empty source arrays, more than 64 sources, or duplicate artifact names.
- Combining the single-source `source` convenience form with `sources`.
- Graph shorthand enrichment config whose name conflicts with an incompatible
  existing enrichment.
- Graph shorthand enrichment config that does not map cleanly to an asset
  `EnrichmentConfig`.
- Missing enrichment for an artifact source when no shorthand config is present.
- Deleting or changing an enrichment while graph indexes still reference it.
- Template references to undeclared `_doc.value.<field>`.
- Non-document node modes that require hydration without an explicit external
  node policy.
- Multigraph settings unless graph edge keys include `edge_id`.
- Query-required global/entity/reverse projections in V1.

## Implemented Boundaries

- `source` is a single-source construction convenience; normalized responses
  expose `sources`.
- `sources` owns ordered artifact identity, selection path, format, mappings,
  and context for each source.
- `IndexManager` provisions compatible shorthand enrichments and rejects
  missing or conflicting dependencies.
- Managed replay materializes source manifests, reconciles precedence into edge
  artifacts, and applies those artifacts to `GraphIndex`.
- Per-source status reports canonical `artifact`, `path`, and `format`; catch-up
  and repair state remain index-wide.
- True multigraph identity, global entity routing, and required cross-shard
  projections remain future work.

## Regression Coverage

Managed enrichment dependency tests:

- Graph index install provisions a missing shorthand asset enrichment.
- Graph index reuses a compatible user-defined asset enrichment.
- Incompatible enrichment config is rejected.
- Multiple graph indexes can share one identical source enrichment.
- Deleting a referenced enrichment is rejected.
- Deleting the original shorthand-owning index does not delete the enrichment
  while another index references it.
- Empty, duplicate, and oversized source sets are rejected.
- A multi-source graph preserves declaration-order precedence.

Materializer tests:

- Relation artifact renders graph edge artifact rows.
- Re-render deletes stale graph edge artifact rows.
- Mutating or deleting a winning source restores the next source's retained
  payload without rescanning all artifacts.
- Overlapping source manifests respect per-source and aggregate reconciliation
  budgets.
- Missing source artifact leaves prior graph state unchanged unless the artifact
  was deleted.
- Deleted source artifact clears graph edge artifact rows for that document/index.
- Template access to `_doc.value` requires `context.doc_fields`.

Replay tests:

- Graph edge artifact writes flow through existing graph replay.
- Crash/reopen after source asset write but before graph apply catches up.
- Crash/reopen after graph edge artifact write but before graph apply catches up.
- Retired-generation replay cannot mutate a recreated same-name graph index.
- Released key-only manifests migrate on the next materialization.
- Reverse graph store rebuilds from owned outgoing rows.
- Split/merge preserves graph edge artifact replay behavior.

Query tests:

- Document-node artifact edges can be traversed with existing graph queries.
- External node ids can be returned without document hydration.
- Hydration-required query over external nodes fails closed.
- Entity/global projection query shapes are rejected in V1.
