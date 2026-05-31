# Entity Resolution Design (Resolver, Promoter, Fusion)

## Summary

This document specifies how Antfly turns per-document extraction artifacts into
canonical entity documents and an entity graph. It extends `GRAPH.md`, which
already establishes the V1 graph pipeline:

```text
extraction artifact -> graph materializer -> graph edge artifacts -> graph index
```

`GRAPH.md` deferred three layers (its "Entity Documents And Resolution"
section). This document designs those layers:

```text
resolver:    extraction artifact -> resolution artifact
promoter:    resolution artifact -> entity document upserts (+ provenance)
materializer: extraction + optional resolution artifact -> graph edge artifacts
```

The guiding principle: model the resolver and promoter as **two more managed
replay stages**, each idempotent over its input artifact, so they inherit
Antfly's existing durability, catch-up, and crash-recovery model. No bespoke
recovery protocol is introduced.

## Goals

- Canonicalize extracted mentions into stable entity document references.
- Keep canonical entities as **ordinary, human-curatable Antfly documents** in a
  dedicated table (usually `entities`), not a pure projection.
- Make identity scoring a **declarative, pluggable function** that supports both
  hand-written deterministic rules and (later) learned weights, without changing
  structure.
- Support **multiple extractors feeding one entity table** with confidence
  fusion from day one in the data model (Knowledge-Vault style), even if the
  first fusion implementation is naive.
- Preserve replay determinism: a recorded resolution decision is re-applied on
  replay, never silently recomputed against moved-on global state.

## Non-Goals For Phase 1

- No human-in-the-loop review workflow. The REVIEW decision band exists in the
  scoring model, but the review queue / curation UI / label capture is **phase
  2**.
- No learned (model-backed) resolver. The deterministic scorer ships first and
  doubles as the label factory for the learned one.
- No two-phase-commit coupling of entity writes and edge writes. Phase 1 is
  decoupled and fails closed on hydration (see "Cross-Shard Placement").
- No entity merge/split rewrite engine. That is phase 3 in `GRAPH.md`.

## Pipeline As Managed Replay Stages

Each stage has its own change-journal `target_hint`, tracks an
`applied_sequence`, and is idempotent over its input:

```text
document write
  -> extractor producer (hint=enrichment): writes _artifacts.relations_v1
        { entities:[{id,label,text,spans}], relations:[{type,source,target,evidence}] }

  -> resolver (hint=resolution): reads extraction artifact, scores each local
        entity against candidates, writes _artifacts.resolution_v1
        { entities:[{local_id, doc_ref:{table,key}, confidence, decision}] }

  -> promoter (hint=promotion): reads resolution artifact, upserts entity docs
        (canonical fields via idempotent DocumentTransform); provenance is
        recorded as inbound mention edges, not an array on the entity doc.

  -> graph materializer (hint=graph): reads extraction + resolution, renders
        graph edge artifacts (doc->entity mention edges, entity->entity relation
        edges) with resolved DocRef endpoints.

  -> graph replay (hint=graph): applies graph edge artifacts to GraphIndex.
```

This maps onto existing machinery:

- `change_journal.zig` already carries `changed_artifact_keys` and a
  `target_hints` enum; add `resolution` and `promotion` hints.
- `io_threaded_runtime.zig` already runs managed workers with
  `applied_sequence -> target_sequence` catch-up and checkpointing.
- `enrichment` already produces asset artifacts via `producer_json`.

## The Replay-Stability Invariant

This is the load-bearing rule for everything below.

The moment resolution depends on **current global state** -- any candidate
search over the entity table, any graph-derived prior, any cross-extractor
fusion -- it is no longer a pure function of its input artifact. Replaying the
same extraction next week could resolve differently because the entity table
moved underneath it. Antfly replay assumes idempotence.

Therefore:

- The resolver **records its decision in the durable resolution artifact**.
- Replay **re-applies the recorded decision**; it does not recompute.
- Recomputation is an **explicit, config-generation-scoped re-resolution pass**
  (this is also the phase-3 merge/split path: bump the resolver config
  generation, re-resolve, and let edge replacement rewrite stale edges).

Get this right and the scorer can be arbitrarily fancy without corrupting
recovery. Get it wrong and a learned/fusion resolver quietly makes the graph
non-deterministic.

## Resolver = Blocking + Scoring + Decision

Entity resolution decomposes into three steps. Antfly already has a part for
each.

### 1. Blocking / candidate generation

Cheaply fetch ~k plausible candidates from the entity table. This is a
**separate, restricted sublanguage** because it must compile to index probes;
it cannot be the open scoring grammar (you cannot run Jaro-Winkler against every
entity).

```json
"candidate_search": {
  "any_of": [
    { "ann":    { "field": "name_embedding", "k": 25 } },
    { "exact":  { "field": "external_ids.isbn" } },
    { "prefix": { "field": "canonical_name", "len": 4 } }
  ]
}
```

`ann` maps to the existing vector index; `exact`/`prefix` map to key lookups.
A vector comparator in scoring (`cosine` on `name_embedding`) implicitly
declares an **enrichment dependency** on a name-embedding artifact, plugging
into the same managed-enrichment-dependency mechanism `GRAPH.md` defines for
graph indexes.

### 2. Scoring (comparison levels -- "Fellegi-Sunter")

The scorer is the one pluggable interface. It is **implemented** today in
`zig/lib/matcher/` and is pure, deterministic, and allocation-light so it is
safe inside replay workers.

A `Comparison` pairs a field on the mention (`left`) with a field on the
candidate (`right`) and lists weighted `Level`s. Each level tests a comparator
against a threshold; the first matching level contributes its weight. Weights
sum (plus a bias) into a log-odds score; a logistic link gives a calibrated
probability.

```json
{
  "comparisons": [
    {
      "name": "canonical_name",
      "left": "m.canonical_text", "right": "c.canonical_name",
      "levels": [
        { "when": "exact",               "weight":  8.0 },
        { "when": "jaro_winkler > 0.92", "weight":  5.0 },
        { "when": "jaro_winkler > 0.85", "weight":  2.0 },
        { "else": true,                  "weight": -6.0 }
      ]
    },
    {
      "name": "name_vector",
      "left": "m.name_embedding", "right": "c.name_embedding",
      "levels": [
        { "when": "cosine > 0.9", "weight": 4.0 },
        { "when": "cosine > 0.8", "weight": 1.0 },
        { "else": true,          "weight": -2.0 }
      ]
    }
  ],
  "combine":  { "bias": -3.0 },
  "decision": { "match": 0.9, "review": 0.6 }
}
```

Supported comparators (see `lib/matcher/src/mod.zig`):

| Comparator     | Operates on | Notes |
|----------------|-------------|-------|
| `exact`        | text/number | 1.0 on equality, else 0.0 |
| `jaro_winkler` | text        | reused logic; prefix-boosted edit similarity |
| `levenshtein`  | text        | `1 - dist/max_len` |
| `jaccard`      | text        | token-set overlap |
| `prefix`       | text        | shared-prefix ratio (also a blocking primitive) |
| `cosine`       | vector      | clamped to `[0,1]` |

Field accessors may be written `m.<field>` / `c.<field>`; the `<ctx>.` prefix is
stripped so records are keyed by bare field name.

`exact` is sugar for `threshold{exact, >=, 1.0}`. A level with `"else": true`
is the catch-all. Comparators and allocation failures degrade to a 0 similarity
so scoring is **total** -- a missing field never crashes, it just falls through
to the `else` level.

### 3. Decision

`probability >= match` -> MATCH (link to best candidate).
`probability >= review` -> REVIEW (phase 2 workflow; treated as no durable link
in phase 1).
otherwise -> NO_MATCH (mint a new entity).

`Scorer.explain()` returns the matched level per comparison, which is both the
review-UI breakdown and the signal for bootstrapping learned-resolver labels.

### Deterministic vs learned: same structure

The levels are the features. In deterministic mode the weights are hand-written.
In learned mode (`weights.mode: "learned"`, phase 2) the **same** levels get
their weights fit by EM (Fellegi-Sunter) or logistic regression over labelled
pairs. Blocking, comparators, and the decision interface are unchanged -- only
the numbers move. Inference scales because blocking already cut candidates to
~k; the model only scores mention x k. The hard part is labels, which the
deterministic resolver bootstraps (high-confidence matches/non-matches) plus
phase-2 human review.

### Why not a general scripting language

The scorer is intentionally **declarative config + a tiny predicate grammar**,
not embedded Lua/Starlark, because it must be:

- index-pushdown-analyzable (for blocking),
- pure/deterministic (for replay),
- introspectable (to learn weights and explain decisions).

Generality lives in the comparator + level space, not in arbitrary code.

## Resolution Stage Runtime (backend_runtime)

The resolution stage worker body is implemented in `lib/resolver` as
`ResolutionStage`, behind two vtable seams so it stays pure and unit-testable:

- `ArtifactStore` -- `get`/`put`/`delete` of artifact bytes by primary-store key.
- `CandidateProvider` -- append the ~k blocking candidates for a mention (a null
  provider means deterministic minting only).

`ResolutionStage.run` performs the full worker step: read the changed extraction
artifact, parse its entities, resolve, serialize, and persist **idempotently** --
if the recomputed resolution bytes equal the stored artifact it writes nothing
(`RunResult.unchanged`), and if the source extraction artifact is gone it clears
the stale resolution artifact (`RunResult.cleared`). This is the same
read/recompute/skip shape the embedding and graph stages already use, which is
what keeps replay cheap and crash-safe.

Driving it in the DB (the integration adapter, not yet landed):

1. **TargetHint.** Add `resolution` to `change_journal.zig`'s `TargetHint`
   (bit 6; the `u3`/`u8` hint mask already has room). In
   `recordFromDerivedBatch`, emit the `resolution` hint when a changed asset
   artifact key is an extraction source (today asset-artifact changes emit
   `.graph`; the resolution stage subscribes to the same keys). Update the hint
   codec sites: `decodeHintMask`, `decodeHintMaskBorrowed`, the static singleton
   slices, and `recordMatchesHintMaskFast`'s hint list.
2. **Worker.** Register a managed worker for the `resolution` hint alongside the
   existing derived workers (the `applied_sequence -> target_sequence` catch-up
   in the io-threaded runtime). On each changed extraction artifact key it builds
   a `ResolutionStage` and calls `run`.
3. **backend_runtime.** The actual resolve+persist is submitted as a
   `background_runtime.Job` (class `.maintenance`, owner = the shard's owner id
   from `BackendRuntime.allocOwnerId`) on the shard's
   `BackendRuntime.durable_jobs` lane, so it runs off the apply path and is
   drained on shard handoff via `drainOwner`. The worker advances and checkpoints
   `applied_sequence` only after the job's durable write completes.
4. **ArtifactStore adapter.** Implement the `ArtifactStore` vtable over the
   shard primary store, encoding extraction/resolution artifact keys with
   `internal_keys` (mirroring the asset-artifact key helpers).
5. **CandidateProvider adapter.** Implement blocking over the entity table's
   indexes (`ann`/`exact`/`prefix`), returning `matcher.Record`s whose field
   names match the scorer's `right` accessors.

No new recovery protocol: a crash before the durable write leaves the extraction
artifact key replayable; the worker re-runs `ResolutionStage.run`, which is
idempotent.

## Promoter

The promoter turns resolution decisions into durable entity state.

- **Entities are ordinary documents** in a dedicated table, editable by humans.
  Keys are rendered by a deterministic template, e.g.
  `{{ lower _entity.label }}/{{ slug _entity.canonical_text }}` ->
  `person/ada_lovelace`.
- **Canonical fields** (name, aliases) are merged via an idempotent
  `DocumentTransform` (which `BatchRequest` already supports), so replaying the
  same promotion is a no-op and concurrent promotions union aliases instead of
  clobbering.
- **Provenance is inbound mention edges**, not a `provenance: [...]` array on the
  entity doc. "Which documents mention this entity" = the doc->entity mention
  edges the materializer already writes, with replace-on-rerender and
  delete-on-source-delete semantics. This avoids hot-key read-modify-write
  contention and unbounded array growth on popular entities, and reuses the
  graph machinery we are already building.

### Deterministic resolver makes the promoter optional in phase 1

A deterministic `key_template` computes the canonical key **purely from
extracted text** -- no global state. Consequence: the materializer can write
edges pointing at `entities/person/ada_lovelace` even before that entity
document exists. So in phase 1 the **graph works end-to-end without the
promoter**; the promoter's job is to make the canonical doc exist for hydration,
search, and display. This de-risks the first ship.

### Cross-shard placement

Entities are sharded by entity key and generally live on a different shard than
the source document. Two options:

1. **Transactional**: wrap the entity upsert (entity shard) and the source-shard
   edge artifact in one 2PC `BatchRequest` (predicates + participants). Gives
   read-your-write consistency between entities and edges.
2. **Decoupled (phase 1)**: promoter upserts the entity in its own write;
   materializer writes edges referencing the entity key independently; hydration
   **fails closed** if the entity doc is not yet present (already mandated for
   external nodes in `GRAPH.md`).

Phase 1 uses decoupled + fail-closed. 2PC is a later hardening step when
read-your-write entity guarantees are actually required.

### DocRef endpoints

Resolution endpoints and resolved edge endpoints use a document-reference shape
from the start, even if phase 1 only hydrates same-table:

```json
{ "table": "entities", "key": "person/ada_lovelace" }
```

Using `DocRef {table, key}` rather than raw string ids keeps cross-table entity
graphs possible without redesigning extraction, resolution, or materialization.
Antfly's write path is single-shard-key today, so introducing `DocRef` into the
resolution artifact and graph edge endpoints is a foundational prerequisite;
doing it now is cheap insurance against a later migration.

## Fusion (Multiple Extractors / Knowledge Vault)

Multiple extractors may feed one entity table. The data model supports this from
day one; the first fusion implementation may be naive.

```json
"fusion": {
  "sources": {
    "relations_v1":  { "trust": 0.9 },
    "tables_v1":     { "trust": 0.6 },
    "human_curated": { "trust": 1.0, "override": true }
  },
  "combine": "noisy_or",
  "prior":   { "from": "graph", "snapshot": "config_generation", "weight": 0.3 }
}
```

Knowledge-Vault mapping:

| Knowledge Vault             | Antfly equivalent |
|-----------------------------|-------------------|
| Many extractors fused       | multiple enrichment producers -> multiple extraction artifacts |
| Calibrated probability/triple | graph edge `weight: f64` already exists; holds fused confidence |
| Graph-derived prior         | `prior.from = graph`, read from a config-generation-pinned snapshot |
| Fusion layer                | a stage that sets the edge weight; itself a future model plug point |

`trust` and `prior.weight` follow the same philosophy as the scorer: hand-set in
deterministic mode, learnable later.

**Streaming caveat.** Knowledge Vault was batch over a static-ish corpus. Antfly
is incremental. If the prior is derived from the same edges currently being
written, the graph reinforces itself. Compute priors from a **stable snapshot**
(pinned to the config generation) with decay, and keep confidence updates
monotone-ish, so an entity cannot bootstrap itself to certainty.

## Artifact Schemas

Extraction artifact (source-document local, unchanged from `GRAPH.md`):

```json
{
  "entities": [ { "id": "e0", "label": "person", "text": "Ada Lovelace",
                  "spans": [{ "start": 10, "end": 22 }] } ],
  "relations": [ { "type": "works_at", "source": { "entity_id": "e0" },
                   "target": { "entity_id": "e1" },
                   "evidence": { "text": "Ada Lovelace works at Antfly" } } ]
}
```

Resolution artifact (maps local ids to canonical DocRefs; the durable record of
the decision):

```json
{
  "config_generation": 7,
  "entities": [
    { "local_id": "e0",
      "doc_ref": { "table": "entities", "key": "person/ada_lovelace" },
      "confidence": 0.98,
      "decision": "match" }
  ]
}
```

Entity document (ordinary, curatable; provenance lives as edges, not here):

```json
{ "entity_type": "person", "canonical_name": "Ada Lovelace",
  "aliases": ["Ada", "A. Lovelace"] }
```

## Validation

Open/index/enrichment validation should reject:

- Unknown comparator or operator in a `when` clause.
- A level with neither `when` nor `else`.
- A blocking predicate that does not map to an available index.
- A `cosine` comparator whose embedding dependency is undeclared/unprovisioned.
- A resolver config that references a missing entity table.
- A learned-weights config without a trained model artifact (phase 2).
- A fusion `prior.from = graph` without a pinned snapshot policy.

## Phasing

1. **Deterministic + decoupled (phase 1, in progress).**
   - [x] Comparison-levels scorer + comparators (`lib/matcher`).
   - [x] Resolution artifact schema + serialization (`lib/resolver`).
   - [x] `DocRef {table, key}` type introduced (`lib/resolver`).
   - [x] Deterministic `key_template` resolver core: mint canonical keys, or
         link to a supplied candidate via the scorer (`lib/resolver`).
   - [x] Resolution stage worker body: read extraction -> resolve -> idempotent
         persist (write/unchanged/cleared), behind `ArtifactStore` /
         `CandidateProvider` seams (`lib/resolver`).
   - [x] Reserve + plumb the `resolution` `TargetHint` through the change-journal
         codec and replay-payload filtering (`change_journal.zig`,
         `docstore.zig`).
   - [x] Resolution artifact key scheme: `internal_keys.resolutionArtifactKeyAlloc`
         / `isResolutionArtifactKey` / `parseResolutionArtifactKeyAlloc` (an
         asset-style artifact under the distinct `"resolution"` type).
   - [x] Resolution stage core (`storage/db/resolution_runtime.zig`
         `resolveExtraction`): given the shard's resolvers + a changed
         extraction artifact, pick the consuming resolver, build its engine via
         `Resolver.initFromParts`, and produce the resolution artifact bytes.
         `antfly_matcher`/`antfly_resolver` threaded into the antfly module graph
         (build.zig). Verified by `db-test` + `root-test`.
   - [x] Per-key processing (`resolution_runtime.processChangedExtraction`):
         parse a changed asset key (`parseAssetArtifactKeyAlloc`), find the
         consuming resolver, and run the tested `ResolutionStage` over an
         `ArtifactStore` to idempotently persist the resolution artifact
         (written/unchanged/cleared). End-to-end tested over an in-memory store.
   - [x] Emit the `resolution` hint on extraction-artifact changes
         (`recordFromDerivedBatch` + the rafted thin-record path in `db.zig`).
   - [x] db-backed `ArtifactStore` over the shard primary store
         (`resolution_runtime.DbArtifactStore`): generic over the store type,
         binds the production erased store and is fake-store tested.
   - [x] `ResolutionRuntime` worker (`resolution_runtime.ResolutionRuntime`):
         wraps the shard store, loads/persists `applied_sequence` (scope
         "resolution"), runs a `backend_runtime` io loop draining applied ->
         target via `catchUp` (snapshots resolvers, `catchUpWindow` over a
         `DbArtifactStore`, persists applied only after durable writes), with
         `notifySequence`/`start`/`stop`. `catchUpWindow` unit-tested with a fake
         replay `Source`; the runtime compile-verified end-to-end via
         `refAllDecls`.
   - [x] `db.zig` lifecycle attachment: `DB` / `BatchExecutionContext` /
         `EnrichmentAppendContext` carry a `resolution_runtime`;
         `initResolutionRuntime` (created before enrichment) constructs the
         worker reusing an append context + `appendDerivedBatchFromEnrichment`;
         started in `startOptionalRuntimes`, torn down in `deinitWrapperState`,
         and `notifySequence`d wherever enrichment is (incl. the enrichment
         derived-batch append where extraction artifacts land). Verified by
         `db-test` + `root-test`.
   - [x] End-to-end integration test: doc write -> extraction asset artifact ->
         resolution worker -> resolution artifact, driven via `runUntilIdle`
         ("db resolves extracted entities into a resolution artifact
         end-to-end"). Exposed and fixed the replay prune-watermark interaction
         and the exclusive `from_sequence` convention.
   - [x] Candidate blocking (exact_key): `ResolverConfig.candidate_search` +
         `ExactKeyCandidateProvider` look up the rendered canonical key as an
         existing entity through the store seam, so the scorer links to it
         (decision=match) instead of re-minting. Tested.
   - [x] Prefix candidate blocking: `candidate_search = "prefix"` +
         `PrefixCandidateProvider` scan the entity table's `label/` key range
         (via an optional `scanPrefix` store seam) and let the scorer rank the
         results, so a typo'd mention links to an existing entity under a
         different key. Tested.
   - [x] Embedding (cosine) scoring: extraction entities and entity candidates
         carry vector fields (`name_embedding`); the matcher cosine comparator
         links by similarity even when text differs. This is the *scoring* half
         of ANN. Tested.
   - [x] Cross-shard candidate seam: the resolution worker threads an optional
         `CandidateSource` (get / scan_prefix / nearest) through
         `processChangedExtraction` -> `processRecordKeys` -> `catchUpWindow` ->
         `ResolutionRuntime`; `SourceCandidateProvider` dispatches exact_key /
         prefix / ann against it. Local-only (null) by default; unit-tested with
         a fake source across all three modes (`db-test`).
   - [x] Cross-shard candidate adapter (`api/distributed_candidate_source.zig`):
         `DistributedCandidateSource` implements the seam over the routing-aware
         `TableReadSource` -- `get` via `lookup`, `scan_prefix` via a ranged
         `scan`, `ann` via a dense-vector `query` -- so blocking fans out to the
         entity shard and resolves local-or-remote, reusing existing group
         routing. Unit-tested with a fake `TableReadSource`
         (`lib-resolution-source-test`).
   - [x] Serving-layer injection: `DataServer.initApiServer` wraps
         `read_source.source()` in a `DistributedCandidateSource` and hands it to
         the write source(s); the managed write cache applies it to every DB it
         opens (`adoptPreparedOpenLocked` -> `DB.setResolutionCandidateSource` ->
         `ResolutionRuntime.setCandidateSource`, taken under `catch_up_mutex`).
         Because the worker only queries the source when a resolver declares
         `candidate_search`, injection is unconditional and needs no open-time
         config discovery. Compiles + passes `public-api-parity-test`; the live
         link across a real shard boundary still needs the multi-node harness to
         exercise (no behavior change until a resolver + entity table exist).
   - [x] Resolver catalog config (`resolver_catalog.zig` `ResolverConfig`) +
         per-shard persistence in `IndexManager` + `addResolver` / `removeResolver`
         / `listResolvers` through DB -> DBCore -> IndexManager (verified by a
         reopen persistence test).
   - [x] `table_provisioner` parsing: ingest a `resolvers` section from table
         config so resolvers are declarable, not just API-driven.
   - [x] Live candidate blocking adapter: `DistributedCandidateSource` fetches
         candidates from the entity table (`ann`/`exact`/`prefix`) over the
         routing-aware read source. Pending only the serving-layer injection
         (above) to be live end-to-end.
   - [x] Promoter: a managed stage (`promotion_runtime.zig`, `promotion` change-
         journal hint) consumes resolution artifacts and upserts canonical entity
         documents through an injected `EntitySink`. `DistributedEntitySink`
         implements the sink over the routing-aware `TableWriteSource` with an
         idempotent merge `DocumentTransform` (set entity_type/canonical_name,
         add_to_set aliases, upsert) -- the decoupled cross-shard write. Wired
         through `DataServer.initApiServer` + the managed write cache
         (`adoptPreparedOpenLocked` and `seedCreatedDbLocked` ->
         `DB.setEntitySink`). Verified end-to-end on a live multi-Raft swarm by
         `e2e/antfly/test_resolution.py` (document -> extraction -> resolution ->
         cross-shard entity upsert) plus db-test/lib-resolution-source-test.
   - [x] Resolvers declarable via table config: a `resolvers` section in the
         index config (top-level or nested in an index) is registered by the
         provisioner on both the reconcile and create-local paths.
   - [x] Provenance as inbound mention edges: a graph index whose artifact
         source sets `mention_edge_type` emits `doc -> entity` edges to the
         canonical key the resolver renders (deterministic, so the edge exists
         pre-promotion). "Which documents mention this entity" == the entity's
         inbound edges. Implemented in both materializer paths (sync db.zig +
         async enrichment_runtime); inherits replace-on-rerender and
         delete-on-source-delete from the graph-edge-artifact machinery.
   - [x] Fail-closed hydration: a graph node whose document is not present
         (entity not yet promoted, or a cross-table entity key) hydrates to
         nothing rather than being fabricated or erroring -- the storage path
         returns the node id with `stored_data = null`, the distributed hydrate
         path skips the missing key. Verified by a db-test.
   - [~] `DocRef` endpoints threaded through graph edge artifacts: mention edges
         now record the resolved target table (`{"target_table":...}` in edge
         metadata, both materializer paths). Remaining: surface the per-node
         endpoint table through traversal -> SearchHit and route a cross-table
         lookup at the api layer (the storage path keeps failing closed). 2PC
         entity+edge coupling is still future.
   - [x] Name-embedding backfill for ann/cosine blocking: a resolver with a
         `name_embedding` model (+ `name_embedding_dims`) backfills a mention's
         name embedding from its text via an injected `DenseEmbedder`
         (`OpenOptions.resolution_embedder`) when the extraction artifact carries
         none, so `ann`/`cosine` blocking has a query vector. Verified by
         lib-resolver-test (the MentionEmbedder seam) and a db-test (full storage
         path: backfill -> cosine -> link). The entity side is config -- an
         embeddings enrichment on the entity table over `canonical_name`
         produces the `name_embedding` the dense index serves to
         `DistributedCandidateSource.nearest`. Remaining: serving auto-injects
         the table's embedder as `resolution_embedder` (its lifetime must outlive
         the resolution runtime's final catch-up).
2. **Learned + reviewed (phase 2).**
   - [x] Learned weights (logistic regression) over the same levels:
         `matcher.fitLogisticRegression`/`predictLogistic` plus the training
         harness `matcher.fitScorerWeights` (encodes labelled pairs by which
         level matched per comparison) and `applyLearnedWeights` (writes the
         fitted weights back into the scorer's levels + bias, in place). A
         learned scorer classifies match vs no-match end to end. Unit-tested.
   - [x] Calibrated fusion across extractors: `matcher.fuse` combines per-source
         `trust * confidence` (noisy_or / max / mean) with a config-generation-
         pinned graph prior into one edge confidence. Unit-tested. Remaining:
         the fusion stage that reads multiple extraction artifacts and sets the
         edge weight from a pinned prior snapshot.
   - [x] REVIEW band workflow: a review-band decision is recorded durably in the
         resolution artifact (`decision: "review"`); the review queue
         (`DB.listPendingReviews` / `resolution_runtime.listPendingReviews`)
         enumerates the mentions awaiting curation. Human curation:
         `recordReviewDecision` writes a durable per-document override
         (confirm / relink / reject), the resolver honors it through an
         `OverrideProvider` seam, and it survives re-resolution (replay-stable
         curation; combined with the config-bump backfill it takes effect over
         the corpus). The override record doubles as a training label for
         `fitScorerWeights`. Verified by lib-resolver-test (the seam) and a
         db-test (record -> re-resolve honors the curated link).
   - [x] 2PC atomic promotion: the promoter commits all of a document's
         resolved entities in one multi-participant transaction
         (`EntitySink.upsertBatch` -> `DistributedEntitySink` ->
         `commitTransaction` across the entity table's shards), so a document
         never lands a partial set of its entities. Enabled in serving
         (`transactional = true`); verified live by e2e/test_resolution.py and a
         db-test (atomic batch). NOTE: atomically coupling the entity upsert with
         the *graph-edge* artifact is still not possible -- `TableCommitRequest`
         carries document writes/transforms, not graph edges -- so the
         entity+edge coupling from option 1 needs that machinery extension; the
         decoupled + fail-closed path remains correct meanwhile.
3. **Merge/split (phase 3).**
   - [x] Entity `merged_into`: resolution follows a matched candidate's
         `merged_into` redirect to the surviving canonical entity (lazy merge).
   - [x] config-generation re-resolution: `reresolveAll` scans the user-key
         namespace for a resolver's extraction artifacts and re-runs resolution
         (idempotent), and `DB.upsertResolver` auto-triggers it when a
         resolver's `config_generation` bumps (via
         `ResolutionRuntime.reresolveBacklog`), re-resolving the existing corpus
         and driving the downstream promotion/graph stages. Verified by a
         db-test (bump gen 1 -> 2 re-resolves an already-ingested document).
   - [ ] Eager edge rewrite on merge (lazy redirect via `merged_into` is done).

## Test Plan

Scorer (done, `lib/matcher`):

- [x] Exact match -> MATCH with high probability.
- [x] Dissimilar -> NO_MATCH via the `else` level.
- [x] Near-miss typo -> REVIEW band.
- [x] Cosine comparison over vector fields.
- [x] Missing fields fall through without crashing.
- [x] Invalid configs are rejected.
- [x] `explain` reports the matched level per comparison.

Resolver core (done, `lib/resolver`):

- [x] Deterministic resolver renders stable canonical keys per mention.
- [x] Mention links to a supplied candidate on a MATCH; falls back to minting on
      review/no_match.
- [x] `type_must_match` blocks cross-type links.
- [x] Resolution artifact serializes to / round-trips the documented schema.
- [x] Unknown template variables/helpers and invalid configs fail closed.

Resolution stage (done, `lib/resolver`):

- [x] Reads an extraction artifact, resolves, and persists the resolution
      artifact.
- [x] Idempotent replay: re-running unchanged input writes nothing.
- [x] Deleted source extraction artifact clears the resolution artifact.
- [x] Links to a provider-supplied candidate on a MATCH.

Resolver / promoter integration:

- [x] The `resolution`/`promotion` workers advance `applied_sequence` only after
  the durable write; idempotent replay re-applies (db-test).
- [x] Live candidate blocking links across shards (e2e `test_resolution.py`).
- [x] Promoter upsert is idempotent under replay; concurrent promotions union
  aliases (db-test + `DistributedEntitySink` merge transform).
- [x] Provenance mention edges appear with source documents and disappear on
  source delete (db-test).
- [x] Hydration of a not-yet-promoted entity fails closed (db-test).
- [ ] Fusion combines per-source confidences into the edge weight from a pinned
  prior snapshot (phase 2).

## Cross-shard candidate blocking (built; serving-layer injection remaining)

Exact/prefix/embedding blocking are all implemented and tested, but on their own
they only see entities the resolution worker's own shard store can read.
Canonical entities normally live in a dedicated `entities` table on a *different*
shard, so meaningful cross-document blocking needs the worker to query that table
across shards. Sublinear ANN candidate generation has the same requirement (the
entity table's vector index is on the entity shard). Both are the same blocker,
and both are now served by the same seam.

**Implemented:**

1. **Seam.** `db_mod.CandidateSource` (storage) exposes `get` / `scan_prefix` /
   `nearest`. The resolution worker takes an optional `CandidateSource`;
   `SourceCandidateProvider` renders the mention's canonical key and queries the
   source by exact key, label prefix, or the mention's `name_embedding`, building
   candidates the existing matcher scorer ranks unchanged. Null = local-only
   blocking (the in-store exact/prefix providers stay the co-located fast path).
2. **Adapter.** `api/distributed_candidate_source.zig`'s
   `DistributedCandidateSource` implements the seam over the api layer's
   routing-aware `TableReadSource`: `get` -> `lookup`, `scan_prefix` -> a ranged
   `scan` over `[prefix, prefixUpperBound)`, `nearest` -> a dense-vector `query`.
   The read source already resolves each group to local or remote and fans out,
   so blocking reuses all existing topology/transport instead of re-deriving it.
   Unit-tested with a fake `TableReadSource` (`lib-resolution-source-test`) and a
   fake `CandidateSource` (`db-test`).
3. **Serving-layer injection.** `DataServer.initApiServer` wraps
   `read_source.source()` in a `DistributedCandidateSource` (a long-lived
   `DataServer` field) and hands its `CandidateSource` to the API and raft-apply
   write sources via `withResolutionCandidateSource`. The managed write cache
   applies it to each DB at its single open chokepoint
   (`adoptPreparedOpenLocked` -> `DB.setResolutionCandidateSource` ->
   `ResolutionRuntime.setCandidateSource`, serialized under `catch_up_mutex`).
   Injection is unconditional -- the worker only queries the source when a
   resolver declares `candidate_search`, so there is no open-time config
   discovery and no behavior change until a resolver + entity table exist.
   Passes `public-api-parity-test`; the live cross-shard link still needs the
   multi-node harness to exercise end to end.

**Remaining:**

4. **Promoter dependency.** Cross-document linking only pays off once the
   promoter writes canonical entity docs (phase 3); until then blocking finds
   nothing to link to. The promoter's cross-shard entity upsert uses the same
   topology + transport (and the existing 2PC `BatchRequest` participants).
5. **Embedding generation.** ANN also needs the mention `name_embedding` to be
   produced -- a name-embedding enrichment over the extraction entities (an
   `embedding` artifact the resolver reads), reusing the dense-embedding
   producer already in the enrichment runtime.

Recommended order: (a) serving-layer injection (unlocks a real separate entity
table), (b) the name-embedding enrichment to feed the cross-shard `ann` source,
(c) the promoter's cross-shard entity upsert.
