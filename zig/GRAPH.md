# Graph Indexing Design

## Summary

Graph indexes should be able to declare the enrichment inputs they need, then
consume the resulting artifacts through Antfly's existing managed-index replay
path.

V1 should not introduce a separate graph recovery protocol or move graph index
rows directly into the Raft state machine. Antfly already has the right boundary:

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
- Keep graph queries model-free. Queries should never call extractors,
  recognizers, readers, generators, or transcribers synchronously.
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

Existing pieces to reuse:

- `_edges` and explicit graph writes are converted into graph edge artifact rows.
- Graph edge artifact keys are source-owned:

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

Missing pieces:

- Graph config cannot yet declare a managed enrichment dependency.
- Graph config cannot yet declare an artifact source such as
  `_artifacts.relations_v1.relations[*]`.
- There is no graph materializer that renders relation artifact items into graph
  edge artifact rows.
- Existing graph edge keys do not include `logical_edge_id`, so duplicate
  relations with the same source/target/type collapse.

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
    "kind": "artifact",
    "artifact": "relations_v1",
    "path": "$.relations[*]",
    "format": "extraction_relation"
  },
  "artifact": {
    "name": "relations_v1",
    "kind": "asset",
    "field": "body",
    "template": "",
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

V1 graph indexes support two source families.

Document field edges:

```json
{
  "name": "doc_graph",
  "type": "graph",
  "source": {
    "kind": "document_field",
    "field": "_edges"
  }
}
```

The existing `edge_types[].field` config should normalize to this source family
internally.

Artifact relation edges:

```json
{
  "name": "relations_graph",
  "type": "graph",
  "source": {
    "kind": "artifact",
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

V1 should keep one materialized source per graph index. A source may be a
document field source or an artifact/enrichment source, but not both in the same
graph index. Combining multiple sources safely requires either source ownership
in graph edge keys or a materializer state row that tracks the exact edge keys
owned by each source.

## Template Mapping

Templates convert artifact items into graph edge artifact rows.

Example:

```json
{
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

## Replacement Semantics

For V1, replacement should happen at the graph edge artifact layer.

For each `(producer_doc_key, graph_index_name, source_artifact_name,
config_generation)` scope:

1. Read the current source artifact value.
2. Render desired graph edge artifact rows.
3. Find existing graph edge artifact rows owned by this graph materializer scope.
4. Delete stale rows and upsert desired rows in the primary store.
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

V1 stale cleanup can use the simple strategy because each graph index has one
materialized source:

- Store materializer-owned graph edge rows under the existing graph artifact
  prefix and delete all rows for the document/index before writing the newly
  rendered set.

If a later graph index supports multiple sources, add a small graph materializer
state row that records the last rendered source hash and owned edge keys, then
diff against that state. Without that state or source ownership in the graph edge
key, clearing document/index rows would delete edges owned by another source.

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

Open/index validation should reject:

- Unknown `source.kind`.
- Artifact source without `artifact`.
- Artifact source with an unsupported `path` or `format`.
- Graph shorthand enrichment config whose name conflicts with an incompatible
  existing enrichment.
- Graph shorthand enrichment config that does not map cleanly to an asset
  `EnrichmentConfig`.
- Missing enrichment for an artifact source when no shorthand config is present.
- Deleting or changing an enrichment while graph indexes still reference it.
- More than one materialized source in one V1 graph index.
- Template references to undeclared `_doc.value.<field>`.
- Non-document node modes that require hydration without an explicit external
  node policy.
- Multigraph settings unless graph edge keys include `edge_id`.
- Query-required global/entity/reverse projections in V1.

## Implementation Plan

1. Extend graph index config parsing with `source`, `artifact`, `nodes`, `edge`,
   and `context`.
2. Normalize existing `edge_types[].field` into `source.kind =
   "document_field"`.
3. Add graph managed-enrichment dependency/shorthand handling in `IndexManager`.
4. Add graph materializer replay for changed source asset artifacts.
5. Render selected relation items into graph edge artifact writes/deletes.
6. Reuse existing graph replay to apply graph edge artifacts to `GraphIndex`.
7. Add status output showing graph indexes waiting on source artifacts.

## Test Plan

Managed enrichment dependency tests:

- Graph index install provisions a missing shorthand asset enrichment.
- Graph index reuses a compatible user-defined asset enrichment.
- Incompatible enrichment config is rejected.
- Multiple graph indexes can share one identical source enrichment.
- Deleting a referenced enrichment is rejected.
- Deleting the original shorthand-owning index does not delete the enrichment
  while another index references it.
- A V1 graph index with multiple materialized sources is rejected.

Materializer tests:

- Relation artifact renders graph edge artifact rows.
- Re-render deletes stale graph edge artifact rows.
- Missing source artifact leaves prior graph state unchanged unless the artifact
  was deleted.
- Deleted source artifact clears graph edge artifact rows for that document/index.
- Template access to `_doc.value` requires `context.doc_fields`.

Replay tests:

- Graph edge artifact writes flow through existing graph replay.
- Crash/reopen after source asset write but before graph apply catches up.
- Crash/reopen after graph edge artifact write but before graph apply catches up.
- Reverse graph store rebuilds from owned outgoing rows.
- Split/merge preserves graph edge artifact replay behavior.

Query tests:

- Document-node artifact edges can be traversed with existing graph queries.
- External node ids can be returned without document hydration.
- Hydration-required query over external nodes fails closed.
- Entity/global projection query shapes are rejected in V1.
