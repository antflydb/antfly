# Graph Indexing Design

## Summary

Graph indexes should consume ordinary physical adjacency writes regardless of
where the logical edges came from. User-authored `_edges`, imported edge
artifacts, and model-produced extraction assets should all flow through the same
graph materialization boundary.

Extraction assets remain value-only artifacts. A graph index declares how to
turn artifact values or document fields into stable graph edges. Artifact-backed
edges are derived state with durable provenance, not fire-and-forget writes.

## Goals

- Let users build graph indexes from explicit document fields and from
  `_artifacts`.
- Use templates for edge id, source node id, target node id, edge type, weight,
  and metadata.
- Keep extraction output generic. NER, relation extraction, classifiers, and
  generators should not need graph-specific payloads.
- Preserve the existing distributed graph model: graph storage receives ordinary
  physical adjacency writes, source-owned for outbound adjacency and optionally
  target-owned for reverse adjacency.
- Make artifact-derived graph replacement deterministic across shard moves,
  split/merge, retries, and extractor output changes.
- Keep query execution model-free. Graph queries use the latest published graph
  index state and never call extractors or generators synchronously.

## Current State

Already wired:

- Explicit document `_edges` can produce graph edge writes; this design
  normalizes those logical writes into physical adjacency writes.
- Graph edge artifacts are encoded in the artifact stream.
- Graph queries and distributed graph expansion work across shards.
- Extractor-backed asset enrichments can produce JSON values with entities and
  relations under `_artifacts`.

Not wired yet:

- A graph index does not yet consume relation arrays from extraction artifacts.
- There is no graph materializer that evaluates templates over dependency
  replay, stores materialized-edge provenance, and emits physical adjacency
  writes.

## Index Sources

Graph indexes should support two canonical source families.

Explicit document field edges:

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

Artifact-backed graph edges:

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

The existing graph config shape using `edge_types[].field` should normalize
internally into `source.kind = "document_field"`. We should avoid creating two
independent field-backed graph configuration paths.

For artifact sources, the materializer registers a primary artifact dependency
for the configured artifact family, plus any context dependencies declared by the
index. Each matched item becomes one edge candidate during dependency replay.

## Template Mapping

Graph index configuration maps source values into edge writes with templates.

For extraction relations:

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
  "nodes": {
    "source": { "kind": "entity" },
    "target": { "kind": "entity" }
  },
  "context": {
    "doc_fields": ["title", "url", "tenant_id"]
  },
  "edge": {
    "source": "entity:{{_relation.source.label}}:{{_relation.source.text | canonicalize_v1}}",
    "target": "entity:{{_relation.target.label}}:{{_relation.target.text | canonicalize_v1}}",
    "type": "{{_relation.type}}",
    "weight": "{{_relation.score | default 1.0}}",
    "metadata": {
      "producer_doc": "{{_doc.key}}",
      "artifact": "{{_artifact.name}}",
      "source_span": "{{_relation.source.start}}:{{_relation.source.end}}",
      "target_span": "{{_relation.target.start}}:{{_relation.target.end}}"
    }
  }
}
```

The template output plus graph-index metadata is first normalized into a logical
resolved edge before placement routing:

```zig
ResolvedEdge{
    .index_name = "relations_graph",
    .config_generation = "sha256:...",
    .edge_id = "...",
    .source = "...",
    .target = "...",
    .edge_type = "...",
    .weight = ...,
    .metadata_json = "...",
}
```

`ResolvedEdge` does not contain `owner_shard`, `adjacency_node`, or
`neighbor_node`; those are assigned later when the materializer resolves physical
placements and builds commands. Bidirectional graph indexes produce one logical
`ResolvedEdge` and two physical placements.

`edge.id` is the logical edge id used by materialized-edge provenance,
replacement, idempotency, and multigraph physical adjacency keys. It is not a
separate storage-generated row id. When omitted, it should default to a stable
hash of the rendered config generation, producer document key, source node,
target node, edge type, and the best deterministic evidence available:

```text
hash_v1(
  config_generation,
  producer_doc_key,
  rendered_source,
  rendered_target,
  rendered_edge_type,
  evidence_identity
)
```

`evidence_identity` should be derived in this order:

1. Explicit `edge.identity` template output.
2. Explicit relation or item id from extractor output.
3. Source and target evidence spans for span-backed extraction.
4. Canonical evidence text when no spans exist.

If none of those are available and the same source, target, and edge type can
legitimately appear more than once, the config must provide `edge.id`.

The default identity path must not hash the entire `_item` JSON. Model metadata,
scores, timestamps, and other non-identity fields may change without changing
the logical edge. If a source format has no stable relation id or spans, the
index should require an explicit identity template:

```json
{
  "edge": {
    "identity": "{{ hash_v1(_relation.source.text, _relation.target.text, _relation.type, _relation.evidence.text) }}"
  }
}
```

`_item_index` may be included only as a collision suffix for true duplicate
edges, not as the primary identity, because model output ordering can change.

## Template Context

The materializer builds a normalized context before rendering templates.

Common fields:

- `_doc.key`: the document key whose field or artifact produced the edge
  candidate.
- `_doc.value`: projected stored document fields only. The index config should
  explicitly list which document fields are exposed to templates through
  `context.doc_fields`.
- `_artifact.name`: the artifact family name for artifact sources.
- `_artifact.content_type`: the artifact content type.
- `_artifact.value`: the full decoded artifact value.
- `_item`: the current value selected by `source.path`.
- `_item_index`: the zero-based index of the selected item.

Template rendering must be canonical before values participate in identity,
diffing, or command hashing:

- JSON objects are serialized with sorted keys and stable numeric formatting.
- Missing values render as explicit `null` only where the template allows nulls.
- Metadata JSON is canonicalized after template evaluation.
- Arrays used for desired-set hashes are sorted by `edge_id` and then by the
  canonical resolved edge bytes.

Extraction relation fields, available when
`source.format = "extraction_relation"`:

- `_entities`: shorthand for `_artifact.value.entities`.
- `_relation`: the current relation with `source` and `target` resolved.

Extraction relation endpoints may be embedded entities or references:

```json
{
  "type": "works_at",
  "source": { "entity_index": 0 },
  "target": { "entity_index": 1 },
  "score": 0.91
}
```

Before template evaluation, the materializer resolves endpoint references
against `_entities`, so templates consistently use
`_relation.source.text`, `_relation.source.label`,
`_relation.target.text`, and `_relation.target.label`.

Generic artifact formats should use `_item.*` instead of relation-specific
aliases.

`context.doc_fields` limits which stored document fields are read and exposed:

```json
{
  "context": {
    "doc_fields": ["title", "url", "tenant_id"]
  }
}
```

Validation should reject templates that reference `_doc.value.*` paths not
listed in `context.doc_fields`, unless we explicitly choose best-effort runtime
nulls for missing paths.

## Node Model

Templates render node ids, but the graph index should also declare the node
model. Initial node kinds:

- `document`: node ids are document keys or document-key-derived ids and can be
  hydrated from DocStore.
- `entity`: node ids are logical entities and should be returned as graph nodes,
  not hydrated as documents.
- `mention`: node ids identify spans within a producer document.
- `mixed`: source and target templates may produce different node kinds.

The `nodes` section declares the expected kind for each endpoint:

```json
{
  "nodes": {
    "source": { "kind": "document", "routing": "document" },
    "target": { "kind": "entity", "routing": "hash" }
  }
}
```

For non-document nodes, graph query results should return node records with
their rendered id, kind, label, and optional properties from metadata. They
should not pretend entity nodes are documents.

Routing policies:

- `document`: route by document key through document identity routing.
- `hash`: route by rendered node id through a metadata-managed graph-node
  identity namespace.
- `producer_doc`: route by the producer document's owning shard.

Document nodes should default to `document`. Entity nodes should default to
`hash`, so global entity ids distribute across shards and remain independent of
the document that produced the mention. Mention nodes should default to
`producer_doc` unless the template intentionally creates global mention ids.

`hash` routing must not be an ad hoc local hash inside whichever shard happened
to process the artifact. It should resolve through a logical graph-node
ownership namespace:

```text
graph_node_owner(index_name, node_id) -> group_id
```

The namespace is logical-index scoped, not generation scoped, so entity node
placement remains stable across template rebuilds unless the node id itself
changes. Graph storage still includes `config_generation`; ownership does not.

That graph-node ownership namespace is metadata-managed so split/merge and
reassignment can reason about entity-node placement. Owner changes are handled
the same way as document shard moves: materialization reads old applied
provenance, emits deletes to old physical placements, emits upserts to newly
resolved placements, and records the new placements after command success.

If V1 implementation needs to be narrower, it may initially support only
`document` and `producer_doc` routing, but the API shape should reserve `hash`
for global entity graphs and reject `routing = "hash"` until the ownership
namespace exists.

## Node Identity

Templates make node identity explicit. Useful policies include:

- Document graph: `{{_doc.key}}`
- Entity graph: `entity:{{_relation.source.label}}:{{_relation.source.text | canonicalize_v1}}`
- Mention graph:
  `mention:{{_doc.key}}:{{_relation.source.start}}:{{_relation.source.end}}`
- Provenance graph:
  `{{_doc.key}} -> entity:...` and `entity:... -> entity:...`

The index should not infer a universal entity-resolution policy. If users want
global entity nodes, they should opt into a canonicalization template or a
separate entity-resolution enrichment.

Canonicalization functions must be versioned. Changing node-id normalization is
a graph identity migration, not a harmless helper update.

## Materialized Edge Provenance

Artifact-derived edges need durable provenance separate from graph storage. The
replacement scope includes a typed source id:

```text
(index_name, producer_doc_key, source_kind, source_name, config_generation)
```

Resolved edge records live under that scope by `edge_id`:

```text
(index_name, producer_doc_key, source_kind, source_name, config_generation, edge_id)
```

For artifact sources, `source_kind = "artifact"` and `source_name` is the
artifact family name. For document field sources, `source_kind = "field"` and
`source_name` is the configured field name. This avoids provenance collisions
when a field and artifact share the same name.

`source_version` and `dependency_version` are stored as values, not as part of
the replacement scope. For artifact sources, `source_version` is the artifact
version; for field sources, it is the document or field version used for replay.
`dependency_version` is the replay position or dependency vector covering the
primary source plus projected context fields. A new dependency version replaces
the previous desired edge set for the same scope. If source or dependency
version were part of the scope, old versions would create independent namespaces
and stale edges would be easy to leak.

`config_generation` is a stable hash or monotonic generation for the graph
index source/template/node-routing configuration. Template changes, routing
changes, source path changes, or helper-version changes create a new generation.
Rebuild can then clear old generations and materialize the new one.

Generation lifecycle:

- active: the generation served by graph queries.
- rebuilding: a newly created generation being materialized from source fields
  or artifacts.
- retired: a previous generation kept only until the new generation is active
  and old provenance/graph state can be dropped.

On config change:

1. Create a new `config_generation`.
2. Materialize graph storage and provenance for that generation.
3. Switch the active generation after rebuild scan and catchup reach the replay
   watermark safe point.
4. Retire and later drop old generation graph state and provenance.

Queries should target one active generation at a time. Old and new template
outputs should not be mixed in one logical graph index.

Graph storage itself must be generation-scoped and keyed by physical adjacency,
not just the logical edge tuple. Two implementation models are acceptable:

- Include `config_generation` in graph storage keys:
  `(index_name, config_generation, adjacency_direction, adjacency_node, edge_type, neighbor_node, edge_id)`.
- Use physical shadow indexes per generation:
  `relations_graph@generation_a`, `relations_graph@generation_b`.

The first model is preferred if it fits the storage layer cleanly. The shadow
index model is acceptable for V1 if it keeps generation isolation simpler. In
both cases, switching active generation is a metadata operation after the new
generation has been fully materialized. Queries resolve logical index
`relations_graph` to exactly one active generation.

Query routing uses only the active generation recorded in index metadata. It
does not inspect reconciliation heads. Reconciliation heads are write-side,
generation-scoped materialization state; rebuilding generation reconciliations
are not query-visible until the active-generation cutover.

Each record stores the resolved edge and its applied physical placements:

```json
{
  "index_name": "relations_graph",
  "producer_doc_key": "doc:1",
  "source_kind": "artifact",
  "source_name": "relations_v1",
  "config_generation": "sha256:...",
  "source_version": "af1:asset:...",
  "dependency_version": "replay:12345",
  "edge_id": "doc:1:0:works_at",
  "placements": [
    {
      "adjacency_direction": "outbound",
      "owner_shard": "group:7",
      "adjacency_node": "entity:person:tim-cook",
      "neighbor_node": "entity:org:apple"
    },
    {
      "adjacency_direction": "reverse",
      "owner_shard": "group:2",
      "adjacency_node": "entity:org:apple",
      "neighbor_node": "entity:person:tim-cook"
    }
  ],
  "source": "entity:person:tim-cook",
  "target": "entity:org:apple",
  "edge_type": "works_at",
  "weight": 0.91,
  "metadata_json": "{\"producer_doc\":\"doc:1\"}"
}
```

This provenance is what makes replace-by-value artifacts safe. The materializer
can compare old and new resolved edge sets and delete stale edges even when the
old physical adjacency is owned by another shard.

Document-field graph sources use the same materialization/provenance model when
they are replace-by-value fields. The scope uses `source_kind = "field"`:

```text
(index_name, producer_doc_key, "field", field_name, config_generation)
```

Existing append-style graph writes may continue to bypass this path, but any
field-backed graph index that promises replacement semantics must go through the
same desired/applied reconciliation model as artifact-backed indexes.

The provenance model should distinguish desired and applied state:

- desired edge set: what the current source value plus current config renders.
- applied edge set: what graph storage is known to contain.
- pending commands: idempotent remote/local graph writes needed to reconcile
  applied to desired.
- reconciliation record: the durable latest reconciliation accepted for a
  provenance scope, including the expected command set.

V1 can implement this with a compact state machine, but the states must exist
conceptually so crash/retry behavior is well defined.

## Reconciliation Records

Reconciliation state is split into a scope head and immutable per-attempt
details:

```text
graph_reconciliation_head(scope) -> latest_reconciliation_id
graph_reconciliation(scope, reconciliation_id) -> state, source/dependency version, desired hash, command batch metadata
graph_reconciliation_command(scope, reconciliation_id, command_id)
```

The scope head is the authoritative fence that tells workers which
reconciliation currently owns the scope. The per-reconciliation record describes
one attempt and must not be overloaded as the scope head.

Every reconciliation detail record stores:

```json
{
  "scope": {
    "index_name": "relations_graph",
    "producer_doc_key": "doc:1",
    "source_kind": "artifact",
    "source_name": "relations_v1",
    "config_generation": "sha256:..."
  },
  "reconciliation_id": "sha256:...",
  "source_version": "af1:asset:...",
  "dependency_version": "replay:12345",
  "desired_set_hash": "sha256:...",
  "placement_set_hash": "sha256:...",
  "routing_epoch": "routing:42",
  "expected_command_count": 12,
  "commands_hash": "sha256:...",
  "state": "preparing"
}
```

Reconciliation states:

- `preparing`: desired set has been rendered but commands are not all durable.
- `active`: commands for this reconciliation may be applied.
- `applied`: commands have applied and provenance was advanced.
- `superseded`: a newer reconciliation owns the scope.
- `failed`: reconciliation cannot complete without retry, rebuild, or operator
  intervention.

`commands_hash` is the hash of sorted `command_id`s. V1 may also store the
sorted command id list for debugging and direct recovery, but membership must be
authoritative, not inferred from the hash. The durable state is:

```text
graph_reconciliation_head(scope)
graph_reconciliation(scope, reconciliation_id)
graph_reconciliation_command(scope, reconciliation_id, command_id)
```

Workers check `graph_reconciliation_command` for command membership. The hash is
only a batch integrity check. Recovery must be able to verify that the durable
command batch is complete before marking the reconciliation `active`.

Materialization installs reconciliation state in two phases:

1. Render the desired edge set.
2. Resolve physical placements and compute the full command set, desired set
   hash, placement set hash, expected command count, and command ids hash.
3. Write the reconciliation detail record as `preparing`.
4. Append every command and every command-membership row for that reconciliation.
5. Mark the reconciliation `active` and CAS the scope head to this
   `reconciliation_id` only after the command rows and membership rows are
   durable.
6. Command workers check the scope head, reconciliation record, and membership
   row transactionally before mutating graph
   storage.
7. After every expected command reaches `applied` or `noop`, update applied
   provenance and mark the reconciliation `applied`, but only if the
   reconciliation is still latest for the scope.

If the scope-head CAS fails because a newer reconciliation already owns the
scope, the losing reconciliation is marked `superseded`. Its queued commands are
never claimable because workers only claim commands whose reconciliation is
`active` and still referenced by the scope head. The materializer may retry by
rendering against the new head state, or exit if its dependency version is stale.

Owner shards may only apply a command when:

```text
command.reconciliation_id == graph_reconciliation_head(scope).latest_reconciliation_id
and reconciliation.state == active
and command.command_id exists in graph_reconciliation_command(scope, reconciliation_id)
```

Workers may only claim commands whose reconciliation is `active`. Commands for
`preparing` reconciliations are ignored until activation, so a partially written
batch cannot be applied.

If a newer reconciliation superseded the command, the worker marks the command
`superseded` without touching graph storage. Superseded commands are terminal
for that command, but they never allow their older reconciliation to advance
applied provenance. This reconciliation record must live in durable metadata
visible to every possible owner shard, or the command application path must
synchronously read the authoritative record before mutation. Local queues alone
are not sufficient because stale commands can arrive after a newer replay.

## Materialization Commands

Dependency replay should not directly mutate graph storage. It should produce a
deterministic desired edge set. The graph materialization layer reconciles that
desired set to the applied set with idempotent commands:

```json
{
  "command_id": "sha256:...",
  "reconciliation_id": "sha256:...",
  "op": "upsert",
  "adjacency_direction": "outbound",
  "owner_shard": "group:7",
  "adjacency_node": "entity:person:tim-cook",
  "neighbor_node": "entity:org:apple",
  "scope": {
    "index_name": "relations_graph",
    "producer_doc_key": "doc:1",
    "source_kind": "artifact",
    "source_name": "relations_v1",
    "config_generation": "sha256:..."
  },
  "edge_id": "doc:1:0:works_at",
  "edge": {
    "source": "entity:person:tim-cook",
    "target": "entity:org:apple",
    "edge_type": "works_at",
    "weight": 0.91,
    "metadata_json": "{\"producer_doc\":\"doc:1\"}"
  }
}
```

`command_id` should be a stable hash of
`(scope, edge_id, op, adjacency_direction, owner_shard, adjacency_node,
neighbor_node, canonical_resolved_edge)`. The scope is part of the graph write
target; command workers must write/delete in the generation identified by
`scope.config_generation`, even when the `edge` object itself does not repeat
that field. Owner shards can dedupe commands by id. Replaying the same
dependency version or retrying after a crash submits the same commands.

The resolved edge bytes used in the command id depend on the operation:

```text
upsert:
  hash_v1(scope, edge_id, "upsert", adjacency_direction, owner, adjacency_node, neighbor_node, canonical_new_resolved_edge)

delete:
  hash_v1(scope, edge_id, "delete", adjacency_direction, owner, adjacency_node, neighbor_node, canonical_old_applied_edge)
```

Delete commands are generated from applied provenance, not from the current
render output. That keeps delete retries stable across source, template, or
routing changes.

Each materialization attempt also has a reconciliation id:

```text
reconciliation_id = hash_v1(
  scope,
  dependency_version,
  desired_set_hash,
  placement_set_hash,
  routing_epoch
)
```

`placement_set_hash` is computed from the sorted physical placements for the
desired edge set. `routing_epoch` is the document routing, graph-node ownership,
or split/merge epoch that affects placement ownership. If deployments cannot
provide a single epoch, `dependency_version` must include the specific ownership
versions used while resolving placements. Placement or ownership changes must
produce a new reconciliation even when the logical desired edge set is
unchanged.

`desired_set_hash` is computed from the sorted list of
`(edge_id, canonical_resolved_edge)` pairs. It must not depend on extractor array
order except where duplicate edges require `_item_index` as an explicit collision
suffix.

Commands include this id so the materializer can distinguish commands for the
latest desired state from commands generated by an older dependency version or an
older render attempt.

Command records need durable lifecycle state:

```json
{
  "command_id": "sha256:...",
  "reconciliation_id": "sha256:...",
  "routing_epoch": "routing:42",
  "state": "queued",
  "attempts": 0,
  "last_error": null,
  "owner_shard": "group:7",
  "op": "upsert",
  "adjacency_direction": "outbound",
  "adjacency_node": "entity:person:tim-cook",
  "neighbor_node": "entity:org:apple",
  "scope": {
    "index_name": "relations_graph",
    "producer_doc_key": "doc:1",
    "source_kind": "artifact",
    "source_name": "relations_v1",
    "config_generation": "sha256:..."
  },
  "edge_id": "doc:1:0:works_at",
  "edge": {
    "source": "entity:person:tim-cook",
    "target": "entity:org:apple",
    "edge_type": "works_at",
    "weight": 0.91,
    "metadata_json": "{\"producer_doc\":\"doc:1\"}"
  }
}
```

Command rows may duplicate `routing_epoch`, `placement_set_hash`, or other
reconciliation metadata for debugging, but the authoritative values live on the
reconciliation detail record. Command execution relies on the command's physical
fields plus the scope head, reconciliation detail, and command membership row.

Command states:

- `queued`: command is durable but not yet known to be applied.
- `applied`: owner shard accepted the idempotent graph mutation.
- `noop`: command was intentionally unnecessary for the current reconciliation,
  such as an already-satisfied idempotent mutation.
- `superseded`: a newer reconciliation owns the scope, so this command must not
  mutate graph storage.
- `failed`: command failed permanently or exceeded retry policy and needs
  operator/rebuild intervention.

The materializer advances applied provenance only after every expected command in
the latest reconciliation batch reaches `applied` or `noop`, and only if:

- the reconciliation is still latest for the scope.
- the reconciliation state is `active`.
- command membership exists in `graph_reconciliation_command`.
- command batch membership, count, and hash all verify.

Superseded reconciliations never advance applied provenance.

If a rebuilding generation has failed commands, it must not become the active
generation. The previous active generation continues serving queries until the
new generation reaches a complete safe point or the index is explicitly marked
unavailable by policy.

V1 should allow only one active reconciliation per provenance scope. New
dependency replay for the same generation-scoped provenance scope supersedes
queued-but-not-applied commands, recomputes the desired edge set, and emits a
new reconciliation. If older commands already applied, the next reconciliation
treats them as applied state and diffs again.

Owner shards must fence command application by reconciliation. A command worker
may only apply a command if its `(scope, reconciliation_id)` matches the durable
scope head and the command belongs to that reconciliation's expected command
set. If a newer reconciliation has superseded it, the worker marks the command
superseded without mutating graph storage. This prevents an old in-flight upsert
from reappearing after a newer reconciliation deleted or changed the edge.

## Distributed Ownership

Graph storage is physical-placement-owned:

1. Evaluate templates on the shard that owns the source artifact/document.
2. Compute the edge source node id and `edge_id`.
3. Resolve physical placements using the configured direction model and node
   routing policy. Outbound placements route by logical source; reverse
   placements route by logical target.
4. Diff desired edges against applied materialized-edge provenance for
   `(index_name, producer_doc_key, source_kind, source_name, config_generation)`.
5. Append idempotent delete commands for old physical placements missing from
   the new desired placement set, using each old placement's stored
   `owner_shard`, `adjacency_direction`, and old resolved edge.
6. Append idempotent upsert commands for new or changed physical placements,
   using the newly resolved owner shard and adjacency direction.
7. Wait for command records to reach `applied` or leave the replay item
   pending/retryable.
8. After commands are accepted, replace applied provenance with the desired edge
   set and clear completed pending commands.

Graph storage is a multigraph. Distinct logical edges with the same source,
target, edge type, and physical adjacency placement remain distinct by
`edge_id`. If an implementation intentionally coalesces parallel edges, that
must be an explicit index option and the materializer must define how weights and
metadata merge. The default is no coalescing.

If source ownership changed since the previous materialization, deletes use the
old placement's `owner_shard`, while upserts use the newly resolved owner. This
handles split/merge and node-routing changes.

Delete commands are generated from each old applied placement:

- `owner_shard = old_placement.owner_shard`
- `adjacency_direction = old_placement.adjacency_direction`
- `adjacency_node = old_placement.adjacency_node`
- `neighbor_node = old_placement.neighbor_node`
- `source = old.source`
- `target = old.target`
- `edge_type = old.edge_type`
- `metadata_json` and `weight` from the old record when required by the delete
  API.

Upsert commands are generated from newly rendered placements. If an edge keeps
the same `edge_id` but changes placement owner, adjacency direction, source,
target, edge type, or other storage identity fields, materialization treats it
as delete old placement plus upsert new placement.

Physical placements are the graph storage diff unit. A logical edge can keep the
same `edge_id` while only one placement changes:

```text
old placements:
  edge_id=A adjacency_direction=outbound owner=group:1 adjacency_node=S neighbor_node=T
  edge_id=A adjacency_direction=reverse  owner=group:2 adjacency_node=T neighbor_node=S

new placements:
  edge_id=A adjacency_direction=outbound owner=group:3 adjacency_node=S neighbor_node=T
  edge_id=A adjacency_direction=reverse  owner=group:2 adjacency_node=T neighbor_node=S

diff:
  delete A/outbound/group:1/S/T
  upsert A/outbound/group:3/S/T
  keep   A/reverse/group:2/T/S
```

Cross-shard traversal stays unchanged at the routing layer. A query expands a
frontier, groups next nodes by owning shard, and dispatches internal graph
reads.

Graph reads return edges, not just neighbor node ids. Because graph storage is a
multigraph, multiple edge ids can connect the same adjacency node, edge type,
and neighbor node. Traversal semantics should be explicit:

- `edge` result mode preserves every physical/logical edge.
- reachability and neighbor expansion may dedupe frontier nodes per step to
  avoid repeated shard dispatch and infinite fanout amplification.
- path enumeration must preserve path state. It may dedupe by
  `(node, path_state)` only when that preserves all requested path results; it
  must not collapse distinct edge histories before paths are materialized.
- coalesced indexes use their configured merge semantics before query results
  are returned.

The default query behavior should dedupe frontier nodes for expansion but return
edge-distinct results. Path queries default to edge-distinct path semantics.

Graph storage should explicitly declare the supported direction model:

- `outbound`: store only source-owned outbound adjacency.
- `bidirectional`: store source-owned outbound adjacency and target-owned reverse
  adjacency for incoming traversal.

If a graph index supports `in` or bidirectional traversal, materialization must
emit the corresponding reverse-adjacency commands with the same provenance and
reconciliation fencing. If V1 only stores outbound adjacency, query validation
must reject inbound traversal instead of silently returning partial results.

For bidirectional indexes, one logical edge produces separate physical
placements:

```text
outbound:
  owner = route(source)
  adjacency_direction = outbound
  adjacency_node = source
  neighbor_node = target

reverse:
  owner = route(target)
  adjacency_direction = reverse
  adjacency_node = target
  neighbor_node = source
```

The physical placement direction is part of command identity. This avoids
collisions when the source and target route to the same owner shard, and it lets
delete reconciliation remove each physical adjacency independently.

Logical edge fields remain `source` and `target`. Physical graph storage reads
and writes use `adjacency_node` and `neighbor_node`; reverse adjacency stores the
target as the local adjacency node while preserving the original logical source
and target in the payload/provenance.

Query results preserve logical edge orientation. An inbound traversal from
`target` over a reverse placement returns the original logical edge
`source -> target` plus traversal metadata such as
`traversal_direction = "reverse"` and the local `adjacency_node`. Results are not
rewritten as `target -> source`.

The physical graph key includes `edge_id` so parallel logical edges do not
overwrite each other:

```text
(index_name, config_generation, adjacency_direction, adjacency_node, edge_type, neighbor_node, edge_id)
```

## Replay And Rebuild

Graph indexes register primary and context dependencies:

```text
primary dependency:
  artifact: relations_v1
  or field: _edges

context dependencies:
  fields: title, url, tenant_id
```

Any change to the primary source or a projected context field schedules
reconciliation for the affected provenance scope. Artifact-backed and
field-backed graph indexes use the same dependency model; artifacts are not a
special replay path.

Replay should follow the same shape as other derived indexes:

1. A primary source or context dependency changes.
2. Dependency replay notifies graph materializers subscribed to that dependency.
3. The materializer reads prior materialized-edge provenance for
   `(index_name, doc_key, source_kind, source_name, config_generation)`.
4. It evaluates templates over the current source value and projected context.
5. It computes `old - new` deletes, `new - old` upserts, and changed-edge
   replacements.
6. It writes the reconciliation detail record as `preparing`.
7. It appends idempotent materialization commands and command-membership rows.
8. It marks the reconciliation `active` and CASes the scope head after the
   command batch is complete. If the CAS fails, it follows the reconciliation
   loser path: mark this reconciliation `superseded`, leave its commands
   unclaimable, and retry only if the dependency version is still current.
9. Command workers apply graph deletes/upserts through normal graph write
   plumbing after checking the scope head, reconciliation record, and
   command-membership row.
10. The materializer records the new applied provenance only after the
    reconciliation head still points at this reconciliation, every expected
    command is `applied` or `noop`, and membership, count, and hash all verify.

The diff step is required because extraction output is replace-by-value. If an
extractor stops producing a relation, the old edge must disappear.

`context.doc_fields` creates an index dependency. If any projected document field
changes, the materializer must rerender affected edge sets because templates may
use those fields in node ids, edge types, weights, or metadata. Implementations
can optimize by recording which graph indexes, artifact families, and source
fields depend on each context field, but correctness requires replay on those
field changes.

For rebuild, the index uses a snapshot plus catchup watermark:

1. Create the new `config_generation` in `rebuilding` state.
2. Record the durable replay position used for the scan:

   ```text
   rebuild_watermark = current durable replay position
   ```

3. Scan source values at exactly `rebuild_watermark`. If the storage layer cannot
   scan exactly at that position, record the actual snapshot position and use it
   as the catchup boundary:

   ```text
   snapshot_position = position used by scan
   ```

4. For each source scope in the snapshot, render the desired edge set for the new
   generation and reconcile it against an empty applied set for that generation.
5. Replay all source and context changes with position greater than the scan
   boundary into both the old active generation and the rebuilding generation.
   The old generation must continue receiving replay so queries remain fresh
   until cutover.
6. Capture an activation watermark:

   ```text
   activation_watermark = current durable replay position
   ```

7. Verify the rebuilding generation has applied all replay positions less than
   or equal to `activation_watermark`.
8. Atomically switch the logical graph index active generation with a compare and
   swap from the previous active generation to the new generation, storing
   `activation_watermark` in index metadata.
9. Route replay positions greater than `activation_watermark` only to the new
   active generation. Replay positions less than or equal to the activation
   watermark were already applied to both generations before cutover.
10. Replay positions greater than `activation_watermark` that committed between
    watermark capture and the active-generation CAS are replayed to the new
    active generation after CAS. Replay routing uses the stored
    `activation_watermark` to resume at the first unapplied position. Replay is
    idempotent by replay position, so positions already dual-applied before CAS
    are skipped and positions not yet applied are caught up from
    `activation_watermark + 1`.
11. Retire and later drop old generation graph state and provenance.

This makes the "safe point" concrete. Updates that happen while the rebuild scan
is running either appear in the snapshot or are applied during catchup before the
generation becomes active. The activation watermark prevents the target from
moving under the cutover: the new generation only has to prove it is complete
through the captured watermark before the active-generation CAS.

Rebuild uses the same reconciliation path as normal replay:

1. Create reconciliation records.
2. Write command membership.
3. Append commands.
4. Apply graph mutations.
5. Advance applied provenance only after the reconciliation head still points at
   this reconciliation, every expected command is `applied` or `noop`, and
   membership, count, and hash all verify.

A bulk loader may optimize the physical writes, but it must produce equivalent
logical records: reconciliation record, command membership, applied command
states, and applied provenance. Bulk loading is not a separate correctness path.

Artifact deletes, producer document deletes, field-source deletes or nulls, and
missing source paths use the same reconciliation model with an empty desired
edge set:

```text
desired_edges = {}
desired_placements = {}
diff(applied_placements, desired_placements) => delete all applied placements for the scope
```

This keeps cleanup behavior identical to replacement behavior and prevents
deleted artifacts or documents from leaving stale graph edges.

## Failure Semantics

Materialization should be idempotent:

- Replaying the same dependency version with the same config and routing epoch
  renders the same `edge_id`s, resolved edges, and physical placements.
- Upserting the same resolved edge is harmless.
- Deleting an already-deleted old edge is harmless.
- Command append, command execution, and provenance application are independently
  retryable.
- If a materializer crashes after remote command success but before provenance
  update, replay submits the same commands again and the owner shard dedupes or
  applies them idempotently.
- Provenance updates happen only after graph commands are accepted, or the
  replay item remains pending/retryable.
- Commands from superseded reconciliations are ignored by the materializer when
  deciding whether the latest desired state is applied. If they already applied,
  the next reconciliation observes that through applied provenance or graph
  storage and corrects it.
- Hashes and reconciliation ids are computed from canonical bytes only. Runtime
  fields such as attempts, timestamps, worker ids, and last errors are never part
  of identity hashes.

Implementations may execute local commands inline as an optimization, but they
must still record the same command/provenance state. Synchronous execution is
not a separate correctness path.

## Mental Model

The graph index is:

```text
logical graph index
  -> active config_generation
  -> generation-scoped graph storage
  -> materialized-edge provenance per producer/source scope
  -> idempotent commands to reconcile desired/applied edges and placements
```

Dependency replay produces deterministic desired edge sets. Materialization
reconciles desired edge and placement state to generation-scoped graph storage.

## Validation

Graph index creation should validate:

- `source.kind` is known.
- `source.kind = "document_field"` requires `source.field`.
- `source.kind = "artifact"` requires `source.artifact`.
- `source.path` is required for structured artifact arrays unless the whole
  artifact is one edge object.
- `source.format`, when present, is known.
- `_doc.value.*` references are covered by `context.doc_fields`, unless
  best-effort runtime nulls are explicitly enabled.
- `edge.source`, `edge.target`, and `edge.type` templates are present.
- `edge.id`, when present, is deterministic and does not use unsupported
  helpers.
- `edge.identity`, when present, is deterministic and does not use unsupported
  helpers.
- `edge.weight`, when present, renders to a number.
- `edge.metadata`, when present, renders to JSON.
- graph `adjacency_direction` or direction-model option, when present, is known:
  `outbound` or `bidirectional`.
- inbound or bidirectional query support requires the direction model to be
  `bidirectional`.
- replace-by-value graph sources require reconciliation storage.
- bidirectional graph sources require placement-aware command and provenance
  storage.
- graph storage supports multigraph physical keys containing `edge_id`, or the
  index explicitly opts into coalescing parallel edges with defined merge
  semantics.
- multigraph query behavior is configured or defaults to edge-distinct results
  with node-deduped frontier expansion.
- path queries, when enabled, require edge-distinct path state handling and must
  not use node-only frontier dedupe for path enumeration.
- source and context dependency fields are registered and replayable.
- rebuild requires a snapshot/replay watermark provider.
- reconciliation identity has routing coverage: either a routing epoch provider
  exists, or dependency versions include the document and graph-node ownership
  versions used for placement resolution.
- `nodes.source.kind` and `nodes.target.kind`, when present, are known.
- Template functions used by the config are known, deterministic, and versioned
  when they affect node ids or edge ids.
- `routing = "hash"` is supported by the deployment's graph-node ownership
  namespace, or the index creation fails with an unsupported routing error.

## Template Functions

Initial deterministic helpers:

- `canonicalize_v1`: lowercase, trim, collapse whitespace, and escape node-id
  separators.
- `json`: render a value as JSON.
- `default`: fallback for missing values.
- `hash_v1`: stable hash for long or ambiguous ids.

Non-deterministic helpers should not be allowed in graph index templates.

## Open Questions

- Whether graph query APIs should expose non-document node properties from
  materialized-edge metadata, a separate node artifact family, or both.
- Whether entity-resolution should be a first-class producer role or remain a
  normal extractor/generator enrichment that feeds graph templates.
