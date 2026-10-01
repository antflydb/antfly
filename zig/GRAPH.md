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

## Relationship deletion durability

Primary graph relationship artifacts are the durable authority for graph replay,
reconstruction, and portable snapshots. Deleting a document retires its owned
artifacts and source-owned inline relationships that target it. This includes
legacy relationships, parallel explicit IDs, and self loops. Fact relationships
owned by an independent document retain that document's lifecycle and may refer
to absent endpoints; deleting their owner retires them.

A local target directory maps inline relationships to their complete primary
artifact keys. Every primary transaction and batch maintains it atomically with
artifact writes and deletes. New stores mark the directory ready on their first
user-record write. Older stores backfill at most 256 keys or 256 KiB of temporary
key data per transaction (one oversized key may form its own page). A durable
cursor resumes migration after interruption; readers use the directory only
when the final ready marker commits. Endpoint deletion records a durable cleanup
job instead of enumerating all incoming relationships in the foreground. Each
cleanup command advances at most one migration page, then processes at most 256
relationship retirements and completed endpoint jobs in total, or 256 KiB of
directory and job-key data (one oversized record may form its own page). Empty
endpoint jobs share a page instead of requiring separate replicated commands.
Each committed retirement removes its directory entry, so
restarting at the target prefix resumes without retaining the full adjacency.

In data-Raft deployments the owner leader proposes cleanup through the ordinary
replicated writer, one page per control round. Commands carry the leader-selected
relationship identities; followers never replan from local directory progress.
Planned commands are private and isolated: their only effects are inline
relationship deletions and cleanup-job removals. They cannot delete documents or
change constraint metadata, so completion also works on tables with unique or
foreign-key constraints.

Standalone DB writes and recovery drain the same commands locally. Resident
standalone owners also register with the shared maintenance scheduler, processing
one bounded page per turn independently of foreground writes, including after
TTL expiry. Idle maintenance also backfills the incoming directory in bounded
pages before future deletions. `DB.openOwned` installs a stable owner
automatically; callers using
the movable `DB.open` form must call `startResidentBackgroundWorkersIfNeeded`
after installing the DB at its final address to enable resident maintenance.
Explicit worker suppression leaves cleanup to writes, recovery, and maintenance
calls. Raft ownership is checked before local planning, including before the
first applied-entry marker exists. Pending jobs survive restart and leader
changes.

A transactionally maintained admission count fences graph reads only for jobs
with incident inline edges. Empty jobs do not interrupt unrelated traversals.
Older queues or invalidated directories without a complete admission summary
remain conservatively fenced until their jobs drain. Graph reads return
`StorageBusy` while an incident-edge job remains pending. New inline
relationships targeting a pending endpoint receive the deterministic rejection
`IntegrityTopologyBusy`, allowing Raft to advance to subsequent cleanup entries.
Independent fact documents keep their own lifecycle. Portable export returns
`StorageBusy` and range-source admission returns `IntegrityTopologyBusy` rather
than capture a cut that would omit pending cleanup. Native whole-store recovery
retains the durable jobs.

Deleting an endpoint also records retirement of each affected relationship whose
owner survives. Retirement records belong to that owner and retain the complete
relationship identity. Primary projection writes honor them even when retained
source artifacts are replayed, so rebuilding source precedence during restore
cannot recreate a retired relationship. Explicit relationship writes clear that
identity's retirement. A meaningful owner-document update clears its retirements
and replays its durable graph inputs; semantic no-ops preserve retirement.
Explicit relationship deletion records the same durable retirement, including
independently owned identities. Index deletion removes its retirement records
through the existing bounded cleanup fence before same-name recreation.

Retirements transfer with their owning document range and are included in
portable relationship blocks, including imports that omit derived indexes.
Ordinary batch deletion and TTL expiry share the same retirement planner. Bundles containing them require AFB reader version
7; ordinary relationship bundles require version 6. The target
directory and its migration checkpoint are local derived metadata and are
rebuilt through primary writes on import. Directory keys are fixed-size hashes,
while values retain complete artifact keys. A local reference directory maintains
an exact retirement count, including repeated writes, deletion, and rollback.
The bounded, resumable v2 migration indexes existing retirements before publishing
the count as authoritative. It runs before the first graph bulk batch or an owner
update with legacy retirement state, as well as endpoint deletion.
Stores with no retirements cache that state per transaction. Bulk ingestion checks
all candidate identities with sorted reads before adding append entries, then
ingests surviving edges directly even while other retirements remain. Batches
that write retirement records themselves use ordinary transactional writes.

Merge artifact pages include primary retirement records. Receiver replay applies
exact relationship deletions to existing projections as well as suppressing
future materialization. Commands that can generate retirements during apply
require data-Raft protocol version 18, including ordinary document deletion,
legacy relationship deletion, cleanup, transforms, committed transaction
decisions, and merge pages. Classification occurs before proposal even when retirement records
are absent from the input. Ordinary artifact-only batches retain their existing
protocol requirements.
Physical splits rebuild the incoming directory and retirement accounting on both
the child and retained parent before graph work resumes. Clearing and rebuilding
use bounded, durable pages; incomplete directories conservatively check primary
retirement records rather than inferring their absence from missing metadata.

Explicit mutation range validation uses the producing document (or the logical
source for an implicitly owned edge). Endpoints may belong to other ranges.
Graph document cleanup scans adjacency and fact-owner keys in pages of at most
256 records or 256 KiB of identity keys, admitting one oversized identity when
necessary. It does not hydrate relationship metadata. Independent incident facts
survive endpoint cleanup; their owner directory drives fact-document deletion.
Each page uses the normal graph mutation path, and interrupted replay resumes by
scanning remaining records before advancing its durable coverage checkpoint.

This preserves the local-store ownership scope of primary artifacts; it does not
introduce a global cross-shard endpoint-deletion protocol.

## Projection values and page budgets

JSON number literals remain intact in source artifacts, document context, and
metadata templates through live materialization, repair, and restore. Only
numeric fields used by the graph engine, such as edge weights and entity array
indices, are converted to their declared numeric types.

Artifact materialization and restore pages retain at most 2048 relation items
or 4 MiB of materialized writes. Byte accounting includes the mutation struct,
index name, both endpoints, type, relationship ID, owner, and metadata. One
oversized relationship may occupy a page by itself to guarantee cursor progress;
subsequent relationships resume from the durable item ordinal.

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

## Shared TTL maintenance and edge TTL

Document and relational rows use the same table TTL policy, timestamp sidecar,
bounded worker, lease, conditional delete path, and cleanup grace. Relational
rows also carry the authoritative write timestamp in their packed row header
so point reads and column scans can filter them without a second lookup.
The plain `get` path applies the same table TTL visibility rule as `lookup`;
document rows read their timestamp sidecar and relational rows use the packed
header. Coordinated relational constraints retain the existing explicit
expiration admission rule.
Expiration deletes a relational row through the normal batch path, updating
relational indexes, row counts, and document identity. The worker resumes its
bounded timestamp scan after restart. Graph sources join that worker and lease,
but use source-specific due entries and conditional mutation because an edge
contribution is not a table row. A relational worker test covers expiration
after reopen, and a relational index test covers index and count cleanup.

This section records the implementation contract. The current work adds a
server-authored creation timestamp to graph artifacts and derived replay,
preserves it through portable backup and graph reindexing, and projects source
contenders into durable private contribution rows. Stateful adjacency scans,
exact probes, and metric input scans choose the highest-priority live
contributor. Each source state keeps its own creation timestamp across
materialization, even when a different source owns the visible artifact. A
document-owned lifetime row survives the paged contender clear and rebuild
used by asset replay; source retirement removes it.
Native graph scans pin the TTL clock across pages, local graph query readers
share one clock across adjacency operations, and coordinator graph expansion
and exact-edge requests carry that clock to their workers. The prepared
single-group proxy envelope carries the same internal clock. Query entry
points keep that clock across a topology retry.
Incoming reverse-existence probes use that same clock, including source-shard
routing probes and standalone root probes. Their internal hydration envelope
carries `incoming_ttl_now_ns` and `incoming_max_scanned_rows`; workers echo the
clock and return `incoming_scanned_rows`. The scan allowance counts every
reverse adjacency row and every contributor visited, including negative probes
and expired rows, across the whole key batch. Existence uses the normal live
contribution selector in presence mode, stopping at the first live contributor
without copying metadata or calculating winner order. Empty prefixes consume
zero rows and may complete with zero allowance.
The coordinator shares the request's physical-work budget across routing
probes, request windows, shard reads, and subsequent expansion. Standalone root
probes pin one clock and use one bounded account across shards. Incoming probe
shards dispatch sequentially with the remaining allowance; an exhausted worker
fails admission rather than returning a partial negative mask. Workers without
clock and scan-count support cannot certify a probe result. The coordinator
rejects missing or mismatched clock echoes and over-ceiling statistics, and
timed incoming requests cannot retry the legacy hydration format. Ordinary
hydration and metric wire compatibility retain their existing behavior.
Workers also return `has_physical_incoming`, tracking any reverse adjacency
row independently of TTL visibility. Only that physical mask can populate the
time-independent incoming route directory after every shard probe succeeds:
a negative live mask at a later read time cannot prune an older pinned query.
Physical route certificates remain usable as candidate shard supersets and
require ordinary TTL filtering during adjacency reads. Standalone existence
probes also validate positive physical routes at the pinned clock, probing only
the listed shards; only a complete physical negative can bypass that read.
The route fence version invalidates durable certificates written by the older
TTL-filtered fallback while preserving the bounded durable slot layout.
The private graph store now keeps a deadline-ordered index for projected rows
and source contributions. Metric metadata records the TTL read time; metric
status and direct score reads become stale at an indexed deadline, and a fresh
build can publish after that boundary. Publication also rejects a build whose
TTL read time precedes an indexed deadline that passed while it ran. This
index is a replay projection; it does not replace the authoritative expiration
index and conditional delete described below. Native graph streams now charge
the request budget for every physical adjacency and private contribution row,
including expired and losing contributors. Exact probes admit their lookups before
issuing a batch read and use the same request budget for contribution selection.
Speculative exact-probe plans retain physical work charges when they fall back
to expansion, and propagate admission diagnostics when they fail.
Cursor pages report and bound physical rows independently of visible edges.
An edge with many document owners must complete winner selection within the
remaining physical scan ceiling; partial selection cannot publish a winner.
Bounded drains let a single edge spend the remaining request scan allowance even
when its contributor count exceeds the output page's edge limit. Winner selection
retains only the best key, then reads its value from the same snapshot and checks
its byte allowance before copying it. Losing payloads are never copied. A
row that does not fit a page's byte limit remains at its physical continuation.
The rejected visit and the next page's repeated visit both count as physical
scan work, including repeated contributor selection. Retained native cursors charge
the adjacency entry once and charge any repeated contributor visits. If no edge
can fit, a bounded read returns the byte-budget error rather than repeating an
empty page. Bounded local reads and graph neighbor enrichment enforce a total scan ceiling. Distributed pattern edge
RPCs carry a shard scan ceiling and report physical rows scanned, including
expired rows. The coordinator charges those rows to the request-owned graph
budget across shard reads and named pattern operations. It reads shards
sequentially while that physical budget is active because a failed fair-share
retry does not report its scan count. The distributed expansion RPC also
receives a shard scan ceiling and reports physical rows scanned, including
expired rows that produce no visible neighbors. The coordinator charges those
rows before merging results and carries the remaining ceiling to the next
shard request. TTL-enabled expansion dispatches shards sequentially so the
same request ceiling governs every physical scan. Non-TTL concurrent requests
each receive the remaining ceiling at dispatch; their combined reports are
charged before the result is returned, so a concurrent overrun fails the
query. A response without a scan count is charged at the full legacy
per-shard ceiling, so an older worker cannot silently bypass the budget.
Serverless segment visibility remains to be
implemented as a separate lake graph feature: lake sidecars are built from
row-source JSON and receive neither this index TTL policy nor the authoritative
edge creation timestamp. Stateful TTL-enabled graph indexes do not publish
through that sidecar path.
The DB materializer and enrichment runtime both consult source tombstones
during replay, clear them on source retirement, and admit a changed source
revision with a new lifetime. A guarded cleanup transaction now
rechecks the expected contender and winner inputs under the primary writer
lock, then commits source removal, the tombstone, lifetime retirement, and
derived replay atomically. The source contender mutation also maintains a
deadline-ordered due index with a versioned candidate value. The shared TTL
worker scans it under the same lease, key and byte page limits, scheduling,
and backpressure policy as document TTL, and invokes the guarded graph source
mutation. Graph-only TTL works without a document TTL schema. Candidate and
source revision digests use the repository's `antfly_hash.Sha256` so every
writer and cleanup guard computes identical bytes. Enrichment graph source
materialization takes the primary apply lock across its tombstone read,
winner reconciliation, and contender commit, serializing it with expiration.
When source contribution snapshots take authority for an edge, the private
graph projection retires any older reverse-row deadline for that edge. Its
contribution deadlines alone then drive metric staleness, including after the
last owner withdraws.
Shard split rebuilds the deadline index from document-owned source contenders
and direct artifacts on both sides. The source sets a durable rebuild marker
before its primary rewrite; open resumes an interrupted rebuild before starting
workers. Replicated merge transfers producer assets, source contenders,
lifetime rows, and tombstones, rebinding generation-scoped keys and payloads on
the receiver. Direct merge import copies the same source state, materializes
receiver-local asset manifests, then reapplies the donor's authoritative graph
artifacts, contenders, lifetimes, and tombstones before replay and deadline
rebuild under a durable recovery marker. A repeated import replaces the
receiver's artifact snapshot in the transferred range and retires its old
private graph rows before copying the donor's rows. This prevents an expired
direct contribution or removed source asset from surviving a later import.
Materialization may otherwise assign
a new source lifetime to an imported contribution. A source tombstone's `GET2`
digest omits the index generation, which is already fenced by its key, so an unchanged
asset remains suppressed after a merge. Receiver apply removes any locally
created due entry superseded by the donor's source lifetime. The worker removes
obsolete due rows after an index incarnation change or source retirement by
checking the exact candidate under the primary writer lock.

All primary graph contributor publishers use the graph primary publication
lease: ordinary batches (including document and relational deletes), merge
pages, graph TTL expiration and stale-candidate pruning, enrichment graph
materialization/withdrawal, and restore ownership pages. The exclusive lease
covers beforeimage reads, edge-limit validation, and primary/replay commit;
source replay takes shared publication before catalog and per-index apply and
holds it across materialization and projection. Different indexes can replay
in parallel. This prevents a prepared source page from overwriting a newly
committed count, lifetime, winner, or tombstone after another publisher commits.
Direct writes on indexes with asset sources enforce the same configured edge
budget against the reconciled durable count. Admission checks the final batch
state, so a deletion and replacement at the limit can commit together, and
rejected additions publish neither contender state nor replay work.

Document and relational TTL deletion take the same publication lease and retire
all selected direct/source deadline rows in the primary deletion transaction.
The due-key collector snapshots the input slice table before appending deletes,
so output growth cannot invalidate keys still being scanned.

TTL cleanup publishes versioned authoritative HA effects plus the ordinary
projection replay record. `HPE1` carries exact binary row/direct artifact mutations.
`HPE2` carries a source retirement identity (index incarnation, edge, source state,
priority, and retired content digest) and the primary-certified deadline. Source
expiration never copies a count or fallback winner derived from the primary's
worker progress. Before certifying source retirement, the primary captures the
committed replay tip under the apply lock and exclusive graph publication lease.
This boundary comes from durable replay metadata in a primary read transaction,
not the in-memory sequence allocator: abandoned reservations from failed mutations
do not create reconciliation debt, including when index workers are disabled.
Read or metadata corruption errors propagate instead of relaxing the barrier.
The graph incarnation must have durably reconciled that tip. Otherwise GC notifies
its executor and defers the candidate, releasing both locks without waiting for
workers. This per-index barrier includes unrelated pending replay and favors a
consistent lifecycle over retiring through a lagging projection. Logical reads
still hide expired contributions while reconciliation catches up.

A source update admitted before retirement inherits the existing creation time
and deadline; it does not renew TTL. Reconciliation refreshes the due candidate's
content digest, so stale candidates are retried before the new revision is retired.
A changed source admitted after retirement starts a fresh lifetime. This order is
independent of primary and standby worker timing and requires no new HA envelope.
Under exclusive publication, a standby removes only the matching source revision
and uses the shared contender reconciler to compute its own count
delta and surviving winner. A different revision remains intact; the primary
replay barrier prevents retirement from certifying an old revision while an
earlier source update still waits in its queue. An absent or different local
contribution still receives the retirement tombstone,
preventing delayed source replay from reviving the certified revision. In particular,
a standby behind the preceding update must remember its retirement before replay
catches up; its older materialized contribution is removed by that reconciliation.
The shared reconciler removes the replica's own due key even if its materialization
clock assigned a different deadline. No replica TTL clock or admission decision
is rerun. These mutations, the HA applied receipt, and local projection replay
commit atomically; duplicate delivery cannot overwrite newer contributor state.

Owner expiration also carries row, identity, catalog, and relational index effects.
Standbys collect their complete local owner artifacts and source sidecars and retire
all their deadlines in the same transaction. This includes contributions already
materialized on the standby while still queued on the primary. Collection uses the
same owner cleanup helpers as ordinary deletion and native TTL cleanup.

Document and graph scanning keep separate retry cursors and bounded page budgets
inside the shared GC worker. Coordinated document/relational admission backpressure
preserves its document page for retry and still runs graph expiration. An unavailable
coordinator mailbox cannot indefinitely retain unrelated expired graph state.

Every configured HA effect mirror, including asynchronous policy, gets a
mutation-scoped primary-effect outbox in that same conditional TTL commit. WAL
append and the configured acknowledgement run before clearing the outbox;
interrupted delivery uses existing matching-record recovery to avoid duplicate
WAL appends. An append failure fences later primary mutations until recovery so
an old afterimage cannot enter the HA tail behind a newer write. Stable callback
state notifies the resident recovery probe without retaining a movable DB handle.
Existing key-only derived records and HPE1 envelopes remain readable. HPE1 readers
reject HPE2, so standbys must support source retirement before an upgraded primary
publishes it; no reader can silently drop the semantic retirement operation.

The order is DB apply → exclusive publication for live primary mutations, and
shared publication → catalog → index apply for replay. Restore pages instead
run under exclusive lifecycle admission; direct range replacement and startup
migrations already quiesce replay through exclusive catalog/lifecycle leases.
Shared publication holders never acquire DB apply. Primary publishers hold no
catalog lease while waiting. The reusable lease releases once, including on
error, and is explicitly released before projection, visibility waits, or shadow
apply. Ordinary and replicated batches share this protocol even when requesting
only write visibility. A regression pauses a source worker before commit and
checks concurrent ordinary writes and TTL expiration, durable edge counts after
reopen, resource-limit enforcement, and subsequent contributor withdrawal.

TTL-state prefix scans validate one contributor at a time in a temporary arena.
Only matching mutation data is copied into the page arena. Rejected unit/chunk
contributors consume no retained page memory, so probe memory is bounded by one
candidate plus the input page and matching mutations, independently of fan-in.

Paged merge apply reconciles imported global contributors and TTL state against
the committed receiver view before publishing each page. Contributor membership,
distinct edge counts, canonical artifacts, source lifetimes, deadline entries,
and graph replay keys join the page's primary transaction. The import uses the
same contender reconciliation as source updates and TTL cleanup, while retaining
the donor's incoming creation time rather than a receiver's provisional lifetime.
Retrying a contributor page does not increment its edge count; contributors
spread across pages still count a logical edge once. Direct contributors remain
members after a producer asset withdraws, so the source edge limit stays valid.

Lifetime and tombstone keys carry the same edge digest as global contributor
keys. Import derives that edge's contributor prefix from the state key, probes
only those candidates, and authenticates the full state identity before applying
it. A matching late tombstone removes a provisional contributor, its lifetime
and deadline, and reconciles the surviving winner in the same transaction. It
also records graph replay work even when the page contains no asset or edge
artifact. A late lifetime row corrects the contributor timestamp and deadline.
These rules apply after restart and to retried or reordered pages; they do not
require buffering an entire document or deferring recovery until the last page.
An unchanged source remains suppressed, while a changed source digest retires
the old tombstone and can establish a new lifetime.

The receiver-owned merge page cleanup now enumerates physical document-owned
keys and validates each proposed owner delete under the primary apply fence.
It covers a direct edge without a primary document, and rejects a cleanup page
that claims exhaustion while skipping that owner. Its advisory scan observes
fixed physical key and byte limits across calls. The older replicated copy
path first rolls back primary rows from the Raft projection, then pages the
receiver's physical store under its transition lease and proposes ordinary
replicated owner deletes for remaining document-owned rows. The physical
continuation is exclusive and each page limits key count and bytes. This
retirement precedes donor artifact pages, so a previous attempt's graph-only
owner cannot survive because it was absent from the row projection. Owner
deletion also removes local and global source contenders and their due entries
in the same primary batch. Derived graph deletion follows the existing replay
sequence before donor artifact replay. The storage owner exposes the bounded
key page through a dedicated ABI entry point; the owner ABI version advances
with that addition. The coordinator releases each receiver read lease before
proposing its Raft delete batch so the apply writer can acquire the owner.

Graph TTL is an index-level policy, declared as `"ttl": {"duration": "7d"}`.
The documented `ttl_duration` string is a compatibility alias; specifying both
forms is invalid. Duration parsing follows document TTL. A zero or absent policy
disables expiration. The policy is immutable within an index incarnation; a
change requires a new incarnation and an explicit decision about existing edges.

Direct `_edges` and explicit graph writes on graph indexes without configured
asset sources create deadline entries alongside their artifacts. The shared
worker checks the artifact digest and current index incarnation under the
primary writer before deleting both the artifact and due entry with a graph
replay record. Split recovery rebuilds their due rows from retained artifacts.
The zero realtime instant maps to 1 ns for a newly created TTL edge because
zero is the persisted "no TTL creation time" sentinel, including for direct
database writes under a deterministic clock.
Ordinary graph replacement and deletion retire the beforeimage's due entry in
the same primary batch. Document deletion uses the same path for collected
graph artifacts, including an edge whose owner has no primary document row.
Derived document clears enumerate stored graph adjacency without the query TTL
filter. The reverse-store delete also retires that edge's owner membership,
contribution snapshots, marker, and private deadline entries in the same
reverse batch as its adjacency and physical count. Before deleting outgoing
rows, document clear durably records source-ordered edge identities in the
reverse store. The final reverse batch removes each intent with its adjacency
and accounting, after a forced outgoing-store sync. A replay retry can
therefore enumerate an edge after its outgoing row was deleted, even if it has
no owner or TTL contribution rows.
The contribution marker scan also recovers TTL clears interrupted before
durable intents were introduced. Incoming adjacency remains enumerable from
the reverse row. Ownership-range pruning uses its durable page intent for the
same ordering and retires private membership, contribution, marker, and
deadline rows with each pruned reverse edge.
Split retirement of an entity-sourced edge withdraws the moved owner's
private contribution snapshot explicitly. The primary artifact still exists
when split finalization prepares the graph fence, so ordinary mutation
reconciliation would read it again and restore the withdrawn TTL deadline.
The destination copies source-keyed physical rows only when they have no
document-owner membership; owner-backed rows are reconstructed from the
destination's moved primary artifacts. The parent retires moved direct and
document-owner snapshots before publishing the narrower range, then replays
retained owner artifacts to replace any shared physical winner that departed.
Retirement streams primary artifact keys and submits bounded graph mutation
pages, capped by both count and owned key bytes. The split state and primary
artifacts remain durable until the primary range commit. Each page forces its
graph index durable before the primary store records its last artifact key;
reopen resumes after that cursor, and replay of an uncheckpointed page is
idempotent. Before the first graph mutation, the parent durably writes an empty
cursor to mark active retirement with no completed page. The cursor remains until primary range commit so a crash anywhere
in the precommit window triggers restoration on reopen; it is then removed so
a later split cannot inherit it. No split-sized array of primary row values or graph deletes
is materialized.
If the process restarts during precommit retirement, writable open first
reprojects the still-owned artifacts and clears the cursor before serving
queries; split finalization can then start a fresh bounded pass. Read-only open
fails closed while this recovery is needed. A failed finalization restores
still-owned artifacts under the apply lock before returning. If restoration
also fails, the durable cursor blocks reads on that live DB handle until
writable reopen completes recovery. Explicit split-state cancellation
performs the same restoration. If the range already committed, writable open
clears the cursor and finishes the ownership fence without restoring moved
owners.
Source-range pruning advances over edges with a retained owner and keeps their
private contribution deadlines and adjacency. Graph mutation admission during
that cleanup uses the producer owner for entity-sourced edges, matching the
artifact key and the index manager's range routing.
The DB batch range check makes the same owner choice and does not require the
edge target to be local. Finalization drains the durable graph prune pages
before publishing the completed split, so its temporary source fence cannot
hide an edge that still belongs to a retained document owner. Writable reopen
finishes any committed prune pages left by a crash before serving reads.
Query-readonly open refuses unfinished graph ownership cleanup with
`GraphMaintenanceInProgress`; a writable owner drains pending pages before a
read-only snapshot can safely expose retained-owner adjacency.
Distributed outgoing and both-direction reads inspect every pinned source-table
group because an entity edge's physical rows follow its producing document
owner, which can be on a different shard from the source node. Results are
deduplicated by edge identity in both outgoing and incoming directions and
charged to a request-wide physical scan budget. Each shard sends the
priority rank and encoded owner/state tie key of its selected live contributor.
Source-backed indexes without TTL retain and send the same contributor order;
source precedence is independent of expiration. Untimed snapshots accept a
zero creation timestamp and create no expiration rows. Direct contributions
on artifact-source indexes use the same durable contender machinery and
outrank asset sources, including after source replay.
Writable open upgrades an existing source-backed projection by streaming its
owned, current-incarnation edge artifacts into contributor snapshots before
admitting reads. Scratch memory is released after each artifact. A versioned
private completion marker is synced only after all snapshots are durable.
Legacy untimed direct writes have no contributor provenance. During this
upgrade, an artifact whose full decoded payload matches none of its recorded
asset contributors is recovered as a direct contributor. Both its primary
global payload and document-local membership are committed atomically with
the visible-edge count, then synced before private migration completion.
This includes artifacts with no asset contenders. Repeated upgrades preserve
the recovered identity and count; subsequent source updates and direct
deletes use ordinary reconciliation. An artifact matching an asset contributor
retains that source identity: a historical direct write with an identical
payload cannot be distinguished from that source with the legacy format.
An interrupted upgrade repeats the idempotent scan; a read-only open without
the marker rejects that index until a writable open completes the upgrade.
Artifact replay, index repair, restore, and topology handoff all reconstruct
the same snapshots. Document clears and shard retirement remove untimed
contributor state through the existing private-state cleanup path.

Merge range replacement reconstructs document-local contributor membership
and visible-edge counts from authenticated donor global contributors, rebinding
them to the receiver generation. Multiple contributors to one edge count once.
These records replace stale receiver membership atomically with imported graph
payloads. Source manifest roots and segments are validated, copied, and rebound
to the receiver generation in that same transaction, preserving source identity
without regenerating outputs or lifetimes during import. The authoritative
membership and counts are retained with donor payloads. Direct contributors
participate in subsequent source withdrawal and edge-limit
checks in the same way as contributors written locally. Empty replacement
ranges retire their old membership and counts.
The replacement transaction also publishes a versioned graph import recovery
record containing the receiver index names and generations. It is synced before
private graph projection updates. Pending recovery fences live reads and writes;
query-readonly open rejects the generation with `GraphMaintenanceInProgress`.
Already-open read-only handles may retain a coherent pre-import snapshot;
reads that observe the persistent recovery fence are rejected too.
Successful live imports and writable recovery use the same publication routine.
A catalog rebuild lease drains pinned metric workers and excludes new scheduler
snapshots through reset, replay, sync and publication. On failure, scheduler
work remains suspended until recovery publishes the complete graph. Reset clears
both private stores in bounded transactions without closing their handles, so
allocation or storage failure leaves a safely destructible catalog entry. An
interrupted clear is retried under the original durable recovery record.
The live caller holds the receiver apply lock; startup completes publication
before admitting readers or maintenance workers. Both rebuild the entire owned
graph from authoritative artifacts, rather than pruning by edge source. This
retires withdrawn producing documents even when their entity source lies
outside the imported range, while preserving contributions from retained owners
that share the same edge. Repeated imports preserve artifact payloads and TTL
creation times. The rebuild scans the full owned graph in bounded batches and
invalidates metric caches; merge publication therefore costs a full graph scan.
Writable reopen validates those index identities and rebuilds private graph
stores from the committed owned primary artifacts in bounded mutation batches,
then reconstructs TTL deadlines. No donor is needed. Recovery may restart after
another interruption; the primary snapshot and recovery record remain intact.
The record is removed only after graph stores and primary state are synced.
Recovery rebuilds the opened graph indexes, including retained ranges, and
invalidates their metric caches rather than retaining scores from an older graph.
Before copying, import leases stabilize both catalogs and drain their graph
workers. Direct import checks runtime admission on both databases under their
ordered apply locks before acquiring either catalog rebuild lease. Paged donor
export uses the shared runtime admission lease and revalidates admission after
locking, including on continuation pages. A donor with an interrupted import
cannot export until its own writable recovery reconciles the durable source
worklist and publishes the rebuilt graph. Rejecting an export does not clear the
donor's recovery record or resume its paused graph workers; pending source work
may already have been removed from the replay journal. Pending moved donor inputs
are materialized and synced before copying,
so the replacement includes complete donor outputs and their original lifetimes.
The recovery record carries a deduplicated worklist of pending asset and
resolution inputs from retained receiver owners. Publication reconciles that
worklist against current primary inputs before
rebuilding edge projections or certifying replay coverage. Missing inputs retire
their prior source contributions. Existing lifetime and tombstone rules apply,
so retrying does not renew surviving edges. The worklist is committed with the
replacement snapshot; recovery no longer needs its original replay entries or
a donor handle. Journal capture streams bounded pages; worklist memory scales
with distinct pending source inputs.
The rebuilt graph also stores its authoritative snapshot replay floor in private
metadata, independently of executor progress. Graph replay checks that floor
after acquiring the catalog and index apply guard, so callbacks queued before
publication consume already covered records without mutating the newer graph.
The floor is synced before removing the recovery record and survives reopening
or delayed worker progress updates. Replay is rejected while a failed rebuild
remains pending; records committed after the snapshot remain eligible.
The record captures the primary replay floor; recovery publishes graph applied
watermarks at that floor after syncing the rebuilt stores, so older journal
entries cannot overwrite the recovered snapshot during startup replay.

The coordinator applies the same total order as the local graph index when
several shards return one edge identity. Shard response order does not choose
the payload. A contributor with the legacy maximum rank still outranks an
untracked projected-only copy. If two groups report the same contributor key
but different payloads, the read fails closed rather than returning a stale
copy. TTL-enabled and source-backed distributed neighbors, traversal, shortest
path, and K shortest path expand each frontier through the coordinator's canonical edge
reader. The reader selects one live physical contributor per edge identity
across the pinned source groups. Only then does the coordinator apply edge
weight filters, document admission, visited-node checks, result limits, and
path cost accumulation. This retains the winning contributor's weight and
metadata in returned paths. Cross-table frontier nodes read their tagged
table and the original source table, as with the existing expansion routing.
The request-wide budget charges scanned physical rows and selected edge
bytes/nodes across all steps and spur searches. Temporary candidate arrays
are capped by the remaining node/edge budget and reserve their bytes before
allocation. Untimed direct-only graph queries use batched shard-local
expansion. Untimed private indexes with no contributor rows retain their direct
projection read path. Snapshot publication enables contributor selection, and
open detects persisted contributor state even without a completion marker.
Canonical traversal reads metric columns in a separate, generation-pinned
hydrate call to the selected contributor's shard after canonical edge
selection. Scores and publication status are read from one metric session per
shard batch. Empty score batches collect status from the other pinned graph
shards, including when no edge survives filtering. The coordinator merges
those statuses before filtering, ordering, and projecting the canonical node
set. Metric filtering and ordering require the complete candidate set up to
the local graph engine's 100,000-node ceiling; reaching the sentinel candidate
returns `QueryCandidateBudgetExceeded` before postprocessing. Query-seeded
personalized metrics require a computation over the complete graph. Distributed
queries and their internal metric RPCs reject those reads explicitly until a
distributed seeded kernel exists; shard-local published scores are never
substituted for personalized values.
Distributed shortest and K shortest path execution also rejects metric
projection, filtering, and ordering until a globally materialized metric
snapshot can score path endpoints. The coordinator checks this before path
search so those clauses cannot be silently ignored by the path executor.
For non-TTL graph expansion, a worker that rejects the extended request can
receive the previous JSON shape. Because its response lacks a physical scan
count, the coordinator charges the full per-shard scan ceiling. The fallback
is allowed only when that ceiling fits the remaining request budget; TTL
expansion always requires the new wire contract.
New workers omit the scan count when replying to a legacy request, preserving
compatibility with older coordinators. The conservative charge can exhaust
the request budget after one legacy shard; a retry never assumes fewer rows
were scanned than the old worker could have read.
The graph hydrate RPC follows the same two-way wire compatibility rule: a
legacy request omits metric fields, and a new worker omits metric fields in
its reply to an old coordinator. Metric reads require the new worker contract.
The graph expansion and edge RPCs use legacy request and response shapes only
for direct-only indexes on a single full-range shard, without TTL, artifact
sources, or document-derived edge fields. For an edge request, the entire legacy physical scan ceiling must
fit the remaining budget, and its old response is charged at that full
ceiling. A new worker rejects an old coordinator's expansion or edge request
for an index needing contributor ordering. Canonical reads never substitute
an unranked legacy contribution. Legacy hydration of an incoming graph index
uses the same admission rule.
These writes have no producer asset state to retain. Replicated merge artifact
apply binds portable direct artifacts to the receiver generation and writes
their due entries in the same batch. The embedded merge import binds donor
artifacts to the receiver generation and rebuilds deadline entries under a
durable recovery marker. Direct writes on an index that also has configured
asset sources join the authoritative contender set with the reserved direct
priority value `2^32-1`. Persisted asset priorities remain zero through
sixty-three in declaration order; winner selection gives the direct value precedence
without changing their keys. The direct contender and its deadline are
committed with the user batch and graph replay record. Direct expiration uses
the guarded contributor cleanup path; the next live asset contender becomes
the winner without losing its original lifetime. Explicit direct deletion
retires its lifetime and tombstone and restores the asset winner. A later
explicit write starts a new direct lifetime.

TTL belongs to an **edge contribution**, identified by graph index incarnation,
logical edge identity, and source owner. Two documents or artifact sources may
contribute the same physical source/type/target edge. Expiring one contribution
must leave other live contributions intact. Source precedence is resolved among
live contributions, so expiration of the winning source may reveal another
source's weight and metadata. A stored winner alone is insufficient to decide
visibility.

The private graph member row stores only `"1"`; it cannot recover a losing
contribution's payload. A separate private projection now retains contender
payloads and precedence by logical edge, owner, and source state. Replaying an
artifact replaces that owner's snapshot while retaining other owners' rows.
The primary graph contender records remain the authority for source payloads;
the private projection is rebuilt from them. A marker distinguishes a projected
edge with no live contributors from an older edge without contribution rows.

The authoritative graph artifact and contender state must record a server-assigned
creation time. The expiration time is derived from the index policy. Conditional
cleanup uses the SHA-256 digest of the durable contender row as its expected
revision. A replay or rebuild
of the same contribution preserves these values. Replacing an existing payload
without changing its contribution identity preserves its creation time and
deadline. Deletion followed by reinsertion starts a new lifetime. This is a
creation-based policy; sliding expiration, if needed, is a separate policy.
Client-provided `created_at` is descriptive metadata and is not trusted as the
TTL clock. Legacy contributions without an authoritative timestamp require an
explicit migration policy before TTL can be enabled.

Source assets may outlive a graph contribution. Expiration therefore retains a
small authoritative tombstone containing its source revision and original
deadline. Replaying an unchanged asset must consult the tombstone and must not
recreate the expired edge with a fresh lifetime. A changed source revision may
create a new contribution. Removing the tombstone is safe only when the source
identity is retired or the index incarnation is discarded.

Graph reads capture one query time and pass it through shard requests. Every
adjacency scan, exact probe, traversal, path, pattern, and topology read excludes
contributions whose expiration time is at or before that time. Read filtering
uses no cleanup grace period. Scanned expired rows still consume query work
budgets. Document and producer visibility checks continue to apply as well.

A durable time-ordered expiration index maps `(expiration time, incarnation,
contribution identity)` to the expected mutation revision. It is updated in the
same authoritative mutation as the contribution. A bounded leader-side worker
scans due entries, then submits conditional deletes through the normal replay
path. Apply rechecks incarnation, revision, deadline, and cleanup grace before
removing a contribution. Stale candidates are harmless. Removal updates source
membership, winner selection, both graph directions, counts, and derived state.
Replicas apply the recorded mutation rather than consulting their local clocks.
The current document version predicates only protect document rows; expiration
requires an artifact/contribution predicate checked inside the authoritative
mutation, not an unlocked read followed by an ordinary graph delete.
Documents, relational rows, and graph contributions share the lease, bounded
worker scheduling, backpressure, and conditional mutation infrastructure. They
retain different candidate types and apply predicates: a table-row candidate
identifies a row and its version, while a graph candidate identifies one source
contribution, its index incarnation, source revision, and deadline. Graph GC
must never turn a contribution expiration into a whole-document or whole-edge
delete.

Time can change graph visibility without a write. Cached graph results and
published graph metrics carry a validity deadline and become stale when a
relevant contribution expires. If lake graph sidecars gain an edge TTL policy,
their segment format must encode enough expiration and source state to apply
the same read predicate; publication-time filtering alone would not keep a
segment correct after its publication. The present row-source sidecar format
has no such policy or authoritative edge lifetime and is not a projection of
stateful graph indexes.

Stateful graph regression coverage includes query-time expiration before
cleanup, all read paths, multiple owners and winner fallback, cleanup racing
with a replacement, restart/replication/rebuild preserving deadlines, and
metrics crossing an expiration boundary. A future lake graph TTL policy will
need separate serverless coverage across an expiration boundary.

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

## Immutable graph-metric execution

Document and external-source serverless publications use the same request-wide
plan in `serverless/build/lake_graph_metric.zig`. Reusable metrics are resolved
first. Dirty requests are grouped by authenticated source identity, equivalent
edge filter, and exact metric computation parameters. Names and refresh policies
are not computation identity. Each source is fetched/prepared once. Compatible
filters share a union topology when the whole group fits its work and memory
budgets; otherwise the planner processes cheaper exact topology requirements
first. An unaffordable spectral sibling must not force an affordable degree
metric to build its adjacency lanes or inherit its rejection. Each unique metric
is computed, encoded, and uploaded once. Compatible HITS authority/hub metrics
share their kernel.

Graph artifact wire v3 stores sorted node/type dictionaries and fixed-width
ordinal edge records (including weights and qualified-table ordinals). Metric
preparation reads validated borrowed views directly into compact outbound
topology, without per-edge string allocation, node hashing, or allocating then
discarding inbound edges. The graph-query reader reuses the same validated view
for memory admission and owned adjacency decoding. Encoders build dictionaries
once, check output limits before allocation, and observe cancellation. The
current score artifact remains v9: its prefix-compressed point/ranked blocks
remain independently readable without fetching another node dictionary.

The plan retains only one source and one filtered projection at a time; alias
fanout retains lightweight references, not score vectors or encoded payloads.
References preserve request order and independently carry index names and
publication, topology, and computation provenance. Equivalent PageRank aliases
use the first available prior artifact in request order as their optional seed;
authentication or compatibility failure still cold-starts the shared computation.
Aggregate budgets count actual unique work, source reads, and output uploads.

Both publication paths resolve the complete requested plan against a shared
inventory of prior computations. Equivalent new or renamed aliases reuse a ready
payload without source reads, kernel work, encoding, or uploads. Each immutable
prior payload is authenticated and its header read at most once per publication,
including failed verification. New aliases retain the original computation time
while carrying their own current publication and topology provenance.
Lake and sidecar manifest validation permits shared IDs for distinct graph/metric
names only when every immutable metadata field agrees; conflicting duplicate
declarations remain invalid.

The preceding manifest's ordered metric references are the admission-plan
witness. Rejected computations remain reusable only while the complete plan,
source identities, and materializer policy are unchanged. Removing or changing a
budget-consuming sibling therefore retries previously rejected work; an unchanged
plan does not cause a retry loop. Missing or invalid prior payloads are rebuilt
with fresh publication/computation provenance.

The storage-independent PageRank, eigenvector, and HITS kernels partition the
CSR vertex/edge work stream into fixed logical tiles, including boundaries inside
high-degree vertices. Complete rows remain target-owned. Only tile-boundary rows
need partial sums (at most 32 stack records), reduced in a fixed order independent
of the `std.Io` worker count. Large graphs have at most
`ceil((nodes + edges) / 16)` work units per logical tile; no extra edge-sized
scratch allocation or atomic floating-point updates are required.

Graph-metric queries authenticate control, routing, primary-score, and ranked
blocks before publishing them to the bounded shared memory cache. Disk retention
is optional and asynchronous: one cache-owned `std.Io` worker drains at most
32 outstanding jobs / 16 MiB, independent of request allocator, executor, and
cancellation lifetimes. Queue pressure or disk failure does not fail a verified
read or make shared waiters download it again. Shutdown cancels pending retention
and joins the worker before destroying the cache. Pending bytes/jobs, failures,
and bypasses are exposed in `QueryCacheStats`; maintenance can explicitly drain
retention, but queries never wait for it. Local-cache read errors fall back to
authenticated origin reads; origin integrity failures remain fatal.

Column queries resolve immutable physical computations before admission and
range planning. Equivalent aliases share routing, transport, and decode work,
while every logical output is admitted up front and owns its result array and
publication provenance. Conflicting immutable metadata cannot reuse another
column's validation.

Non-serverless single- and multi-column reads use the same snapshot-local
physical-key reader in `graph/score_read.zig`. Status policies are checked before
score allocation. Only identical encoded metric/generation prefixes are aliases;
equal configurations with different durable publications remain independent.
Rows and physical columns are sorted independently, duplicate keys are read
once, and all logical results preserve input order and independent ownership.
One reusable key slab and result-vector pair serve batches of at most 4096
storage keys, avoiding per-score prefix formatting and per-batch arena churn.
The existing durable ordinal/vector-chunk jobs and shared numerical kernels
remain the non-serverless computation path.

Materializer epoch 13 invalidates earlier admission and preparation policies,
including rejections retained before adaptive topology grouping. Serverless is
unreleased and supports only the current artifact contract: old graph wire
versions are rejected, not migrated or silently decoded.

See [preparation and score-reader benchmarks](bench/graph/METRIC_PREPARATION.md)
for reproducible phase-specific measurements and their limitations.

## Retrieval-agent navigation

Agentic graph walks and tree exploration are configured on the retrieval step.
See [Retrieval-step navigation](../docs/design/retrieval-navigation.md) for the
request contract, ranked/agentic behavior, budgets, and query API boundary.
