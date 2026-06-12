# Automatic Entity and Relationship Extraction

## Context

Antfly already has most of the substrate needed for a managed knowledge graph:

- graph indexes with typed, weighted, directional edges and reverse lookup state
- graph enrichment that can derive edges from document fields
- managed inference through Termite and generator providers
- embeddings indexes that can support candidate retrieval for entity resolution
- docsaf entity enrichment that can extract section-level entity and relation records during ingestion

The missing product layer is a canonical entity lifecycle. Extracted mentions should not become one-off edge targets forever. They should become evidence that feeds an entity target, which produces stable canonical entity records and graph edges that can be searched, traversed, merged, split, and corrected over time.

## Design Goal

Treat entity extraction as a graph-index enrichment mode that writes into a configured entity target.

The source table owns the extraction policy and evidence emission. The entity target owns canonical entities and identity resolution. Graph indexes connect documents, mentions, entities, and entity-to-entity relationships without making the graph index's private backing store the system of record for entities.

## Core Model

### Source Document Table

The source table contains user documents, chunks, or docsaf sections. It configures a graph index with an `entity_extraction` block:

```yaml
indexes:
  knowledge_graph:
    type: graph
    edge_types:
      - name: mentions
      - name: supports_mention
      - name: works_at
      - name: located_in
    entity_extraction:
      entity_target:
        mode: table
        table: entities
      evidence_target:
        mode: table
        table: entity_evidence
      namespace: company_knowledge
      fields: ["title", "content"]
      provider: termite
      model: fastino/gliner2-base-v1
      entity_types: ["person", "organization", "location", "product"]
      relationship_types:
        - name: works_at
          source_type: person
          target_type: organization
        - name: located_in
          source_type: organization
          target_type: location
      resolution:
        mode: candidate_then_merge
        threshold: 0.85
        allow_cross_table: true
```

`entity_target` should live at the graph-index extraction config level, not on every edge type. One graph index should target one entity namespace. That keeps entity resolution, hydration, and permissions tractable while still allowing many relationship edge types. `evidence_target` is optional: if omitted, mention and relationship evidence can be stored beside canonical entities in the entity target; high-volume cross-source deployments should configure a dedicated evidence table.

### External Entity Table References

The source table graph index should not assume every edge target is a local document key. Entity extraction needs a typed external node reference at the API boundary:

```json
{
  "source": {
    "table": "docs",
    "key": "doc-17"
  },
  "target": {
    "table": "entities",
    "key": "entity:company_knowledge:organization:01hz..."
  },
  "edge_type": "mentions:organization"
}
```

Use structured refs in API requests and responses. For storage, this can be encoded as a qualified node ID:

```text
antfly://tables/entities/keys/entity:company_knowledge:organization:01hz...
```

or as a compact string:

```text
entities/entity:company_knowledge:organization:01hz...
```

The important rule is that graph APIs should preserve the table component. The existing graph key format treats the target as opaque bytes, which is flexible enough for a compact qualified target string internally, but query hydration and traversal need to understand that the target belongs to the configured entity table.

Recommended behavior:

- `entity_extraction.entity_target` defines where canonical entity records are stored.
- `entity_extraction.evidence_target`, when present, defines where mention and relationship evidence are stored.
- `entity_extraction.namespace` defines the resolution scope within that target. One entity table may hold multiple namespaces, and candidate retrieval should filter by namespace unless cross-namespace matching is explicitly configured.
- same-table mode still uses the namespace as a scope; it is not just an ID prefix.
- edge metadata should still include `target_table` and `target_key` for debugging and forward compatibility.
- graph query results should return a structured `target_ref` when an edge target is external.
- hydration should use the target table's normal lookup/query path and permissions.
- entity-to-entity relationships should usually be written in the entity table's own graph index, not only in the source document table's graph index.

That yields two graph layers:

```text
docs.knowledge_graph:
  docs/doc-17 -> entities/entity-acme    mentions:organization
  docs/doc-17 -> rel_evidence/abc        supports_relation

entities.entity_graph:
  entities/entity-alice -> entities/entity-acme works_at
  entities/entity-old   -> entities/entity-new  merged_into
```

The source graph answers "which entities are mentioned by this document?" The entity graph answers "how are canonical entities related to each other?" Cross-table graph queries can compose both layers, but each table remains the system of record for its own nodes.

Hydration is separate from edge lookup:

- graph expansion should batch external target refs by table before fetching records
- target records should be fetched through the target table's normal read path, including auth and row filters
- permission-denied or missing targets should return an edge with an unhydrated `target_ref`, not fail the entire graph query by default
- SDKs may expose client-side hydration helpers, but the server graph API should support batched hydration because one RPC per edge is not viable
- database-level graph views can later centralize mixed-table hydration and traversal planning

### Local and Multi-Table Graphs

Entity extraction introduces a behavior that is different from most existing indexes: it naturally wants edges between nodes owned by different tables. Antfly should support this, but in layers.

Start with graph indexes remaining table-local:

- the graph index belongs to one table
- its outgoing edges are owned by that table's shard/write path
- targets may be qualified refs to another table
- hydration of external targets goes through the referenced table's normal lookup, auth, and row-filter path
- canonical entity relationships are written into the entity table's own graph index

This preserves the current index model and avoids making a graph index own documents from multiple tables. It also keeps writes clear: a source document table owns `doc -> entity` mention edges; the entity table owns `entity -> entity` canonical edges.

Add a multi-table graph layer later as a logical view, not as the first storage primitive:

```yaml
graphs:
  knowledge_graph:
    sources:
      - table: docs
        index: knowledge_graph
      - table: emails
        index: knowledge_graph
      - table: entities
        index: entity_graph
    default_entity_table: entities
```

A database-level graph view would provide:

- query planning across multiple table-local graph indexes
- hydration of mixed-table node refs
- cross-table traversal policies
- permission-aware expansion
- a single named surface for user queries

It should not be the only way graph indexes work. Antfly should support both:

- **same-table graph indexes** for local document graphs, trees, citations, dependencies, and explicit `_edges`
- **qualified cross-table edges** for source-to-entity links
- **database-level graph views** for composing multiple table-local graphs into one queryable knowledge graph

In short: keep storage and writes table-local; make cross-table graph behavior a query/composition layer. Entity extraction can use qualified refs immediately, and a multi-table graph view can come later once traversal and hydration semantics are proven.

### Same-Table Entity Mode

Antfly should also support storing extracted entities in the same table as the source documents. This matches the existing docsaf example shape, where section records, entity records, and relation records can be materialized into one record set with `_type` distinguishing them.

Example config:

```yaml
indexes:
  knowledge_graph:
    type: graph
    edge_types:
      - name: mentions
      - name: works_at
      - name: merged_into
    entity_extraction:
      entity_target:
        mode: same_table
        type_field: _type
        entity_type_value: entity
        mention_type_value: entity_mention
        relationship_evidence_type_value: relationship_evidence
      namespace: docs_local
      fields: ["title", "content"]
      provider: termite
      entity_types: ["person", "organization", "location"]
```

Same-table mode stores canonical entity records as normal documents in the source table:

```json
{
  "id": "entity:docs_local:organization:acme",
  "_type": "entity",
  "entity_type": "organization",
  "canonical_name": "Acme Corp",
  "aliases": ["Acme"]
}
```

and local edges do not need external table refs:

```json
{
  "kind": "graph",
  "index": "knowledge_graph",
  "op": "relate",
  "source": "doc-17",
  "target": "entity:docs_local:organization:acme",
  "edge_type": "mentions"
}
```

This should be an explicit target mode, not an accidental fallback from omitting `entity_table`. Suggested target config union:

```yaml
EntityTarget:
  oneOf:
    - type: object
      required: [mode, table]
      properties:
        mode:
          enum: [table]
        table:
          type: string
    - type: object
      required: [mode]
      properties:
        mode:
          enum: [same_table]
        type_field:
          type: string
          default: _type
        entity_type_value:
          type: string
          default: entity
```

Suggested evidence target config:

```yaml
EvidenceTarget:
  oneOf:
    - type: object
      required: [mode, table]
      properties:
        mode:
          enum: [table]
        table:
          type: string
    - type: object
      required: [mode]
      properties:
        mode:
          enum: [entity_target]
```

If `evidence_target` is omitted, treat it as `{ mode: entity_target }`. Same-table entity mode can still choose a separate evidence table later; the schema should not assume evidence and canonical entities share storage.

Tradeoffs:

- same-table mode is simpler for demos, single-source corpora, local knowledge graphs, and datasets where docs/entities share the same lifecycle
- same-table mode avoids cross-table writes and cross-table graph hydration
- separate entity table mode is better for cross-source identity resolution, shared golden records, entity-specific permissions, different retention policies, and high-volume mention evidence
- same-table mode can pollute search results unless queries filter `_type`
- same-table mode makes it harder to share canonical entities across multiple source tables unless those sources already write into one shared table

Recommended default:

- use same-table mode for local or single-source graphs
- use a separate entity table for Alchemia-style cross-source identity resolution and reusable golden records

API transforms should accept a shorthand `NodeRef` form:

```yaml
NodeRef:
  oneOf:
    - type: string
      description: Key in the transform's default table.
    - type: object
      required: [table, key]
      properties:
        table:
          type: string
        key:
          type: string
```

Rules:

- string node refs resolve against the transform item's `table`, or against the table in the table-scoped endpoint
- structured node refs use their explicit `table`
- responses should prefer structured refs whenever an edge crosses a table boundary
- hard `op: "merge"` should reject cross-table refs by default

### Entity Table

The entity table is a normal Antfly table. It should be queryable, embeddable, graph-indexable, and writable through the normal table write path.

Suggested canonical fields:

```json
{
  "id": "entity:company_knowledge:organization:01hz...",
  "_type": "entity",
  "namespace": "company_knowledge",
  "entity_type": "organization",
  "canonical_name": "Acme Corp",
  "aliases": ["Acme", "Acme Corporation"],
  "attributes": {
    "industry": {
      "value": "software",
      "source": "extracted",
      "confidence": 0.88,
      "last_evidence_id": "mention:...",
      "last_seen_at": "2026-04-09T00:00:00Z",
      "verified": false
    }
  },
  "status": "active",
  "resolution": {
    "strategy": "candidate_then_merge",
    "confidence": 0.93,
    "version": 7
  },
  "stats": {
    "mention_count": 42,
    "source_count": 18
  },
  "provenance": {
    "created_by": "entity_resolver",
    "created_at": "2026-04-09T00:00:00Z",
    "updated_at": "2026-04-09T00:00:00Z"
  }
}
```

Recommended indexes:

- full text over `canonical_name`, `aliases`, and selected attributes
- embeddings over `canonical_name + aliases + attributes` for resolution candidates
- graph over entity-to-entity relationships such as `alias_of`, `merged_into`, `works_at`, `located_in`, and `same_as_candidate`

Entity IDs should be resolver-assigned stable IDs, not deterministic hashes of canonical names. A generated ULID-style suffix keeps IDs stable across renames, alias changes, and later split/merge decisions. On merge, the survivor keeps its original ID and the losing entity becomes a redirect/tombstone, so old refs still hydrate and explain their lineage.

Entity embeddings should use the normal table enrichment pipeline. When the resolver appends an alias, changes an inferred attribute, or updates the canonical name, the entity document changes and the existing embedding index can re-embed it through the standard enrichment hash path. This is one of the main reasons canonical entities should be normal table records instead of hidden graph-index state.

`stats` fields such as `mention_count` and `source_count` are denormalized aggregates owned by the resolver/reconciler. Inline resolver updates can keep them approximately current, but rebuilds should be able to recompute them from active evidence.

### Mention Evidence

Mentions should be durable evidence, not just counters. A mention is the smallest unit that says "this document span refers to this candidate entity."

Suggested mention key:

```text
mention:<namespace>:<source_table>:<source_doc_key>:<field>:<span_start>:<span_end>:<extraction_fingerprint>
```

Suggested mention document:

```json
{
  "id": "mention:company_knowledge:docs:doc-17:content:118:127:ab91...",
  "_type": "entity_mention",
  "namespace": "company_knowledge",
  "source_table": "docs",
  "source_doc_key": "doc-17",
  "source_field": "content",
  "span": {
    "start": 118,
    "end": 127,
    "text": "Acme Corp"
  },
  "entity_type": "organization",
  "candidate_name": "Acme Corp",
  "candidate_attributes": {},
  "extractor": {
    "provider": "termite",
    "model": "fastino/gliner2-base-v1",
    "config_hash": "ab91..."
  },
  "confidence": 0.94,
  "resolved_entity_id": "entity:company_knowledge:organization:01hz...",
  "resolution_status": "resolved"
}
```

Mentions can live in the entity table if the table allows `_type = entity | entity_mention | relationship_evidence`, or in a dedicated evidence table. Same-table and simple deployments can start with typed evidence records in the entity target to reduce moving parts. Cross-source production deployments should strongly consider a separate `entity_evidence` table from the start because mention and relationship evidence is typically 10-100x larger than canonical entities, has different retention needs, and should not pollute entity search or embedding indexes.

Either way, the evidence schema should be portable: evidence records should not assume they share a physical table with canonical entities.

Spans are document-relative, not chunk-relative. If extraction runs over chunks, the extractor runtime must translate chunk-local offsets back to the original source field offsets before writing evidence. `span.text` should be bounded or optional for large spans; offsets and source references are the durable identity. Extraction chunking can reuse lower-level chunk helpers, but it should not be forced to use the same chunk boundaries as embedding indexes because extraction and embedding usually need different context windows.

### Relationship Evidence

Relations should also begin as evidence. A relation extractor may produce `person -> works_at -> organization` from a sentence, but the final entity-to-entity graph edge should only be written after both ends resolve to canonical entity IDs.

Suggested relationship evidence document:

```json
{
  "id": "rel_evidence:company_knowledge:docs:doc-17:content:118:180:4f2a...",
  "_type": "relationship_evidence",
  "namespace": "company_knowledge",
  "source_table": "docs",
  "source_doc_key": "doc-17",
  "source_field": "content",
  "relationship_type": "works_at",
  "head_mention_id": "mention:...",
  "tail_mention_id": "mention:...",
  "head_entity_id": "entity:company_knowledge:person:01hy...",
  "tail_entity_id": "entity:company_knowledge:organization:01hz...",
  "confidence": 0.88,
  "extractor": {
    "provider": "termite",
    "model": "Babelscape/rebel-large",
    "config_hash": "4f2a..."
  },
  "status": "active"
}
```

The canonical entity-to-entity edge can then use compact metadata:

```json
{
  "evidence_count": 13,
  "max_confidence": 0.91,
  "last_evidence_id": "rel_evidence:...",
  "provenance": "extracted"
}
```

## Component Architecture

### Resolver Ownership

Source table enrichers should not be the canonical identity writers. They run near source documents, extract candidate mentions and relationships, and write durable evidence plus resolver work items. The configured entity target owns canonical entity records, merge decisions, and canonical entity-to-entity edges.

For cross-table identity resolution, the clean default is:

```text
docs graph enricher
emails graph enricher  -> entity evidence/outbox -> entity resolver -> canonical entities
crm graph enricher
```

The entity resolver runs as a leader-only job for the entity table or entity namespace, using the existing leader-owned background worker pattern. That gives one serialized canonical writer per namespace while still allowing many source tables to extract in parallel. If one entity table holds multiple namespaces, the resolver may shard work by namespace or by a stable partition key such as `(namespace, entity_type, normalized_candidate_name_hash)`, but the same candidate space must route to the same resolver owner.

Optimistic duplicate creation can still happen during outages or reconfiguration, so the reconciler must be able to merge accidental duplicates later. But the normal path should be source enrichers writing evidence and the entity target resolver writing canonical state.

Table ownership still applies to graph edges. The entity resolver can decide that `docs/doc-17` mentions `entities/entity-acme`, but the source table should own the outgoing `doc -> entity` edge in `docs.knowledge_graph`. The resolver should therefore emit a source-edge work item or resolved-evidence update, and the source table graph reconciler should apply the edge through the source table write path. The resolver can directly own canonical `entity -> entity` edges only when those edges live in the entity target's graph index.

### 1. Extraction Planner

Runs inside the source table's graph-index enrichment path.

Inputs:

- table name
- graph index name
- extraction config
- source document key and document body
- last extraction fingerprint, if present

Responsibilities:

- render configured fields or templates into extraction inputs
- chunk large fields when needed, translating chunk-local offsets back to source document offsets before evidence is written
- compute an extraction fingerprint from source text, extractor config, model, entity labels, and relation labels
- skip unchanged documents by comparing the fingerprint to stored enrichment state
- emit idempotent extraction tasks or derived-log records
- enforce extraction budgets, provider rate limits, and per-table or per-namespace backpressure

The extraction planner should not directly merge canonical entities. It should produce evidence upserts and resolver work items.

### 2. Extractor Runtime

Provider-neutral interface:

```go
type EntityExtractor interface {
    Extract(ctx context.Context, req ExtractionRequest) (ExtractionResult, error)
}
```

Provider options:

- Termite recognizer: GLiNER-style labels for fast configurable NER
- Termite relation extractor: GLiNER relation extraction or REBEL-style triples
- generator/tool-calling provider: slower but flexible schema-driven extraction

The existing docsaf `pkg/docsaf/entity` package can be the ingestion adapter. Its current normalized IDs are fine for local examples, but the managed Antfly path should treat those as mention/candidate IDs, not as globally canonical IDs.

### 3. Evidence Writer

Writes extraction output as idempotent records:

- mention evidence upserts
- relationship evidence upserts
- optional source-document graph edges to mention IDs
- extraction fingerprint/checkpoint state
- pending resolver work items

This can be implemented as a derived-log/outbox operation so it survives crashes and retries.

Important rule: evidence writes are deterministic and replaceable. Re-running the same model/config on the same source text should produce the same keys. Re-running under a new extraction fingerprint should mark old evidence `superseded` with a pointer to the new fingerprint, not silently delete lineage. Retention policy can garbage-collect superseded evidence later.

The outbox is the boundary between source-table ownership and entity-target ownership. Source table enrichers can append evidence and pending resolution work through the target table's normal write path, but canonical entity mutation belongs to the entity resolver.

### 4. Entity Resolver

Runs against the entity target and owns canonical identity.

Resolution pipeline:

1. Normalize mention text and entity type.
2. Retrieve candidates:
   - exact normalized alias match
   - prefix/fuzzy name match
   - embedding nearest neighbors in the entity target's embedding index
   - optional domain-specific identifiers from attributes, such as email, URL, ticker, DUNS, GitHub login, or product SKU
3. Score candidates:
   - type compatibility
   - normalized name similarity
   - alias overlap
   - attribute agreement/conflict
   - embedding similarity
   - co-occurrence context
   - source trust
   - resolver overrides
4. Decide:
   - above auto-merge threshold: attach mention to existing entity
   - between review thresholds: create `same_as_candidate` or `needs_review` record
   - below threshold: create a new entity
5. Upsert canonical entity:
   - add alias
   - update mention/source counts
   - update selected inferred attributes when policy allows
   - update centroid/embedding materialized field if used
6. Emit canonical edges:
   - source-edge work item for `source doc -> canonical entity` with `mentions:<entity_type>`
   - canonical entity -> canonical entity for resolved relationship evidence when the edge belongs to the entity target's graph index

Attribute conflicts should be represented, not flattened away. For inferred attributes, keep versioned values or at least per-attribute provenance with last evidence, confidence, first/last seen timestamps, and a `verified` flag. Verified attributes should not be overwritten by extraction. Conflicting inferred values can either reduce merge score or attach as disputed/versioned observations depending on policy.

### 5. Source Edge Reconciler

Runs in the source table's graph-index worker path.

Responsibilities:

- consume resolved-evidence or source-edge work items from the entity resolver
- write outgoing `doc -> canonical entity` edges into the source table's graph index
- remove or supersede old doc/entity edges when evidence is superseded, suppressed, or re-resolved
- keep edge metadata tied to mention IDs, extraction fingerprint, resolver decision ID, and source span
- retry idempotently without requiring the entity resolver to mutate source-table storage directly

### 6. Entity Reconciler

Runs periodically or on-demand over the entity namespace.

Responsibilities:

- merge entities that later become clearly identical
- split entities when user feedback or contradictory identifiers show a bad merge
- rebuild canonical edge aggregates from evidence
- supersede or tombstone stale evidence when source documents or extraction configs are removed
- re-run extraction for explicit model/config backfills

The reconciler should be able to rebuild canonical state from evidence. That is the safety property that keeps bad LLM extraction or bad resolver thresholds recoverable.

Backfill should be explicit for large corpora. A model, label, prompt, or extraction-policy change should mark affected extraction state as `backfill_required` or `stale_for_config`, but it should not automatically re-extract an entire corpus unless the table config opts into that behavior. Operators should be able to trigger a budgeted backfill over a table/index/namespace.

### 7. Feedback API

Human feedback is part of the architecture, not an admin afterthought.

Operations:

- confirm same entity
- reject same entity
- merge entities
- split entity
- set canonical name
- add/remove alias
- mark attribute as verified
- suppress mention or relationship evidence
- lock entity from automatic merge

Feedback should create durable override records instead of mutating only the canonical entity. The resolver then treats overrides as hard constraints.

Override records should survive reconciler rebuilds because they are user/system feedback, not derived state. Useful override types:

- `must_merge` for entity/entity pairs
- `must_not_merge` for entity/entity pairs
- `mention_belongs_to` for mention/entity pairs
- `mention_does_not_belong_to` for mention/entity pairs
- `attribute_verified` for canonical attributes that extraction cannot overwrite
- `suppress_evidence` for bad mention or relationship evidence

The resolver consults overrides before threshold scoring and records which override affected the decision.

### 8. Budgets and Observability

Extraction and resolution should reuse the existing enrichment rate-limit and provider-budget machinery. At minimum, track usage by provider, model, table, graph index, namespace, and extraction fingerprint. A namespace or table should be able to pause extraction when budget or rate limits are exceeded without blocking normal document writes.

## Entity Lifecycle

### 0. No Evidence

The entity target contains no record for an entity. The source document is just a normal Antfly document.

### 1. Candidate Mention Extracted

The source graph-index enricher sees a changed document field and calls the configured extractor.

Output:

- raw entity mention candidates
- optional raw relation candidates
- extraction fingerprint

State:

```text
source doc -> extraction task complete
mention evidence -> pending_resolution
relationship evidence -> pending_resolution
```

No canonical entity has to exist yet.

### 2. Mention Evidence Written

The evidence writer upserts mention records keyed by source span and extraction fingerprint. Old mention evidence for a previous fingerprint is marked `superseded` and linked to the new fingerprint. Deletion or retention policy can tombstone it later.

Graph effects:

- optionally create `doc -> mention` edges immediately
- do not yet create canonical `doc -> entity` edges unless the resolver is inline

This stage is idempotent. It can be retried without creating duplicate mentions.

### 3. Candidate Resolution

The resolver consumes unresolved mentions.

Outcomes:

- `resolved`: points to an existing entity
- `new_entity`: creates a canonical entity and points to it
- `needs_review`: candidate is ambiguous
- `rejected`: mention is low quality, contradicted by an override, or suppressed by policy

The resolver should record the decision, score, candidate set, override effects, and feature summary. That makes later debugging possible.

### 4. Canonical Entity Created

If no candidate clears the threshold, the resolver creates a new canonical entity.

Initial canonical entity:

- stable generated ID, not a deterministic name hash
- namespace
- entity type
- canonical name chosen from the best mention
- alias set containing the observed mention text
- confidence/source stats
- status `active`

The entity may be searchable immediately. Its own embedding enrichment can run through the existing embedding index path and will be re-triggered when resolver updates change the entity document.

### 5. Mention Attached

The mention record gets `resolved_entity_id`.

Graph effects:

- source-edge work item is emitted for `source doc -> canonical entity`, e.g. `mentions:organization`
- source table graph reconciler applies the outgoing edge through the source table write path
- canonical entity -> source doc reverse edge is available through graph reverse lookup
- optional mention -> entity edge if mention records are graph nodes

Edge metadata should include:

- mention ID
- source field
- span start/end
- extractor provider/model/config hash
- confidence
- resolver confidence

### 6. Relationship Resolved

A raw relationship becomes canonical only after its head and tail mentions resolve.

Graph effects:

- head entity -> tail entity edge of the relation type in the entity target's graph index
- edge metadata aggregates evidence count and confidence
- relationship evidence remains queryable for provenance

If either side is ambiguous, keep the relationship evidence in `pending_resolution` or `needs_review`.

### 7. Entity Updated

New mentions and relations arrive over time.

Allowed automatic updates:

- append aliases
- update counts
- update non-verified inferred attributes
- update embeddings/centroids
- add relationship evidence

Guarded updates:

- canonical name changes
- verified attributes
- merges involving high-value entity types
- entities with manual locks

Guarded updates are enforced by resolver policy plus durable override/lock records. Extraction can propose them, but the resolver should either reject them, create `needs_review`, or require explicit transform feedback.

### 8. Entity Merged

When `entity:A` and `entity:B` are judged identical:

- choose a survivor canonical entity
- write `entity:B --merged_into--> entity:A`
- move aliases, mention links, and relationship evidence to the survivor
- rewrite or rebuild canonical edges
- keep redirects so old entity IDs still hydrate

Do not hard-delete the losing entity immediately. Tombstone or redirect it so graph traversals and old evidence can still be explained.

### 9. Entity Split

If a merge was wrong:

- create or reactivate the split-out entity
- move selected mentions and relationship evidence to it
- rebuild affected canonical edges
- add `must_not_merge` and mention-level override constraints so the resolver does not merge them again

This is why evidence must remain durable. Without mention-level provenance, a split becomes guesswork.

### 10. Source Document Updated or Deleted

When a document changes:

- compute a new extraction fingerprint
- mark old evidence for the old fingerprint as `superseded`
- write new evidence
- re-resolve affected mentions and relationships
- decrement or rebuild entity mention/source counts
- rebuild relationship aggregates for affected entities

When a document is deleted:

- tombstone its mention and relationship evidence
- remove doc -> entity edges
- decrement/rebuild counts
- optionally garbage-collect orphan entities under a retention policy

### 11. Model or Config Upgraded

When labels, prompts, model version, or extraction policy changes:

- config hash changes
- old evidence remains but is no longer current
- affected source records are marked `backfill_required` or `stale_for_config`
- a budgeted explicit backfill can run under the new fingerprint
- resolver can compare old and new evidence during migration

This lets users roll out better extractors without losing lineage.

## Consistency and Failure Model

Extraction is asynchronous and eventually consistent. Source document writes should not block on remote model calls or cross-table resolution.

Recommended flow:

```text
source write
  -> graph index sees changed extraction inputs
  -> derived enrichment request appended
  -> source enricher extracts mention/relationship candidates
  -> source enricher writes evidence and pending resolver work through the entity target write path
  -> entity target leader resolver consumes pending evidence
  -> resolver upserts canonical entity records through the entity target write path
  -> resolver emits source-edge work for resolved doc/entity links
  -> source table graph reconciler writes local doc -> entity edges
  -> entity target reconciler writes canonical entity -> entity edges
```

Cross-table writes must go through the target entity table's normal write path. The source table enricher should not directly mutate another table's storage engine, and it should not directly decide canonical merges for a shared namespace. This avoids races where two source tables concurrently extract the same real-world entity and both create separate canonical records.

All side effects should be idempotent:

- extraction evidence keys include source span and extraction fingerprint
- resolver decisions have stable keys
- canonical edge aggregation can be rebuilt from evidence
- supersession, deletes, and tombstones are explicit records, not only missing keys

## Query Model

Useful query patterns:

- find docs that mention an entity
- find all entities mentioned by a doc
- expand from search hits to nearby entities
- search entities directly by name/alias/attribute
- traverse entity relationships
- hydrate an edge with supporting evidence
- show why two records were merged

Suggested read/query API surfaces:

- `GET /tables/{entity_table}/entities/{id}`
- `POST /tables/{entity_table}/entities/search`
- `GET /tables/{entity_table}/entities/{id}/evidence`
- graph query hydration that can follow `mentions` and relationship evidence IDs

Hydration implementation notes:

- collect external refs during graph expansion and batch them by table
- apply each target table's read permissions independently
- return partial results when a target is hidden or missing, with the edge and structured ref intact
- let clients request `hydrate: false | shallow | full` so high-fanout graph traversals can avoid unnecessary fetches

## Transform-First Graph Mutation API

The mutation API should prioritize the existing batch/pipeline transform model, not new action endpoints. Entity resolution is one use case for node merge/link/split, but the primitives are also useful for non-entity graphs such as citations, duplicate documents, dependency graphs, ticket graphs, and user-curated ontologies.

These operations should be transform-style at the execution layer, but not only raw document transforms. Existing document transforms are field-level DML operations such as `$set`, `$inc`, and `$push`. Graph merge/split/relate operations are semantic graph transforms: they may update multiple records, edge keys, resolver override records, redirects, and evidence aggregates. They can still live in the normal `transforms` array as long as non-document items are clearly discriminated by `kind`.

The default transform kind is document. Existing document transform requests do not need to change. Only graph/entity transform items require `kind: "graph"`.

Primary table-scoped batch form:

```json
{
  "transforms": [
    {
      "kind": "graph",
      "index": "entity_graph",
      "op": "merge",
      "source": "entity:company_knowledge:organization:old",
      "target": "entity:company_knowledge:organization:survivor",
      "mode": "redirect",
      "reason": "human_feedback",
      "evidence_ids": ["mention:...", "same_as_candidate:..."]
    }
  ],
  "sync_level": "write"
}
```

For table-independent batch or pipeline APIs, include `table` on the transform item:

```json
{
  "transforms": [
    {
      "kind": "graph",
      "table": "entities",
      "index": "entity_graph",
      "op": "merge",
      "source": "entity:company_knowledge:organization:old",
      "target": "entity:company_knowledge:organization:survivor",
      "mode": "redirect",
      "reason": "human_feedback"
    }
  ],
  "sync_level": "write"
}
```

For relating nodes, use the same transform surface:

```json
{
  "transforms": [
    {
      "kind": "graph",
      "index": "entity_graph",
      "op": "relate",
      "source": "entity:company_knowledge:person:alice",
      "target": "entity:company_knowledge:organization:acme",
      "edge_type": "works_at",
      "weight": 0.92,
      "metadata": {
        "provenance": "human_feedback",
        "evidence_ids": ["rel_evidence:..."]
      }
    }
  ]
}
```

Use structured refs for cross-table relations:

```json
{
  "transforms": [
    {
      "kind": "graph",
      "table": "crm_accounts",
      "index": "account_identity_graph",
      "op": "relate",
      "source": "acct_123",
      "target": {
        "table": "entities",
        "key": "entity:company_knowledge:organization:acme"
      },
      "edge_type": "maps_to"
    }
  ]
}
```

Use directional merge semantics:

- `source` is the node being merged away.
- `target` is the surviving node.
- `mode: "redirect"` leaves a `merged_into` edge and redirect/tombstone record.
- `mode: "rewrite"` may rewrite affected edges immediately, usually with a rebuild/reconcile job.

Prefer hard merges only within the same canonical table. Cross-table identity should usually be represented as equivalence or mapping, not as destructive merge. Structured refs make cross-table operations expressible, but a cross-table merge should default to one of:

- `same_as`: assert that two nodes in different tables refer to the same real-world entity while preserving both source records
- `maps_to`: attach an operational/source record to a canonical entity
- `resolved_as`: record that an extracted mention or source-specific entity resolved to a canonical entity

Example:

```json
{
  "transforms": [
    {
      "kind": "graph",
      "index": "entity_graph",
      "op": "relate",
      "source": {
        "table": "crm_accounts",
        "key": "acct_123"
      },
      "target": {
        "table": "entities",
        "key": "entity:company_knowledge:organization:acme"
      },
      "edge_type": "maps_to",
      "metadata": {
        "reason": "identity_resolution"
      }
    }
  ]
}
```

True cross-table `op: "merge"` should be reserved for cases where both tables are explicitly configured as the same canonical namespace and the operation is implemented as a redirect/equivalence layer. It should not move or rewrite source documents across table ownership boundaries by default.

The response should return the survivor, the tombstoned or redirected node, and the graph mutations that were applied:

```json
{
  "target": "entity:company_knowledge:organization:survivor",
  "merged": ["entity:company_knowledge:organization:old"],
  "redirect_edges": [
    {
      "source": "entity:company_knowledge:organization:old",
      "target": "entity:company_knowledge:organization:survivor",
      "type": "merged_into"
    }
  ],
  "rebuild_required": true
}
```

The same operation can be used in a pipeline stage:

```json
{
  "operation_type": "transform",
  "table": "entities",
  "source": "$candidate_pairs",
  "transforms": [
    {
      "kind": "graph",
      "index": "entity_graph",
      "op": "merge",
      "source_field": "left_entity_id",
      "target_field": "right_entity_id",
      "policy": {
        "threshold": 0.95,
        "mode": "redirect"
      }
    }
  ]
}
```

Execution rules:

- graph/entity transforms live in `transforms` beside document DML transforms, but require `kind: "graph"`.
- document transforms remain the default kind for backward compatibility.
- graph/entity transform items are idempotent when `request_id` or deterministic evidence IDs are supplied.
- the operation should return applied low-level mutations for audit/debugging
- resolver-generated transforms should use the same API shape as human feedback
- raw document transforms remain available for direct field edits, but should not be the only way to express graph merge/split semantics
- if an operation touches multiple tables, route it through a transaction or outbox/reconciler rather than pretending it is a single-key document transform

Action endpoints such as `POST /tables/{table}/graphs/{graph_index}/nodes:merge` can be added later as thin convenience wrappers, but they should not be the first or authoritative mutation surface.

## Go and Zig Fit

### Go

The Go graph index config already has `edge_types`, `summarizer`, and `template`. Add `entity_extraction` alongside those fields. The existing graph enricher already handles leader-only backfill/queue processing and field hash checks; extend it with extraction fingerprint checks and evidence outbox emission.

Likely touch points:

- `src/store/db/indexes/openapi.yaml` or the OpenAPI source schema for `GraphIndexConfig`, `EntityExtractionConfig`, `EntityTarget`, and graph transform items
- `src/store/db/indexes/openapi.gen.go` generated config structs
- `src/store/db/indexes/graph_index_enricher.go` for source-table extraction planning and evidence outbox emission
- `src/store/storeutils/edge.go` for qualified target encoding and decoding helpers, while preserving opaque target bytes in storage
- graph query/hydration code for structured external refs and batched target fetches

The existing docsaf entity package can remain as an ingestion/example helper. It should not become the canonical resolver by itself because it currently normalizes entity keys from surface text. That is useful for deterministic local records but too weak for cross-source identity resolution.

### Zig

Zig graph options currently carry edge type configs and reverse-store options. Add an `entity_extraction` config shape at the API/index metadata layer, then plumb generated enrichment requests through the existing DB enrichment log.

Likely touch points:

- `pkg/antfly/src/graph/graph.zig` for graph index options and edge target encoding helpers
- the Zig enrichment worker/log modules for extraction fingerprint state and source-table evidence emission
- any graph index enricher implementation, including `graph_index_enricher_v0.zig` if still active
- Termite-Zig wrappers around GLiNER, relation extraction, REBEL, and resolver helpers

Termite Zig already has recognizer, GLiNER relation extraction, REBEL relation extraction, and resolver building blocks. The Antfly integration should wrap those through a provider-neutral extractor interface rather than coupling graph indexing directly to one model pipeline.

## Initial Implementation Order

1. Add config schema and validation for graph-index `entity_extraction`, including `entity_target`, namespace semantics, evidence target defaults, and resolver ownership rules.
2. Add entity/mention/relationship evidence record schemas, including document-relative spans, bounded span text, per-attribute provenance, and portable evidence records.
3. Add extraction fingerprint state and source-table enrichment task emission.
4. Add evidence outbox/pending resolver work from source tables to the entity target.
5. Implement Termite recognizer extraction into mention evidence, with chunk-local offset translation back to document-relative spans.
6. Implement the leader-owned exact/fuzzy resolver for a configured entity target and namespace.
7. Add source-edge work items for resolved doc/entity links and have the source table graph reconciler write local doc -> canonical entity mention edges.
8. Return structured refs in graph responses.
9. Add batched cross-table hydration for external node refs.
10. Add relation evidence extraction and canonical entity-to-entity edge aggregation in the entity target graph.
11. Add `kind: "graph"` transform items for merge, split, relate, alias, suppress feedback, and resolver overrides.
12. Add attribute conflict policy and verified-attribute handling.
13. Add reconciliation jobs that rebuild canonical edges, counts, and redirects from evidence and overrides.
14. Add explicit budgeted backfill operations for extraction config/model upgrades.
15. Consider action endpoints only after transform-based mutation semantics are stable.

## Design Risks

- Bad automatic merges are more damaging than duplicate entities. Bias thresholds toward duplicates and make review/merge easy.
- Relationship extraction should not bypass entity resolution. It should stay evidence until both endpoints are canonical.
- Entity updates need clear resolver ownership plus compare-and-swap or transactional semantics when multiple resolver partitions write into the same entity namespace.
- Provenance can become high-volume. Keep canonical records compact and push verbose span/model details into evidence records.
- Permissions and row filters need an explicit rule for cross-table entity hydration. A user who can read a source doc should not automatically gain global read access to every entity unless the entity table policy allows it.
- Evidence volume can dwarf canonical entity volume. Same-table evidence is acceptable for simple deployments, but production cross-source deployments likely need a separate evidence table.
- Automatic full-corpus backfill on config changes can surprise operators. Prefer explicit, budgeted backfill unless the table opts into automatic re-extraction.
