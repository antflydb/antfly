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
"candidate_search": "ann",
"candidate_ann_index": "name_embedding",
"candidate_limit": 25
```

The implemented v1 config uses a single `candidate_search` mode:
`"exact_key"`, `"prefix"`, or `"ann"`. `exact_key` renders the resolver's key
template and fetches that entity doc, `prefix` scans the rendered entity-key
namespace, and `ann` probes the named dense vector index in
`candidate_ann_index`, capped by `candidate_limit`.

A vector comparator in scoring (`cosine` on `name_embedding`) implicitly
declares an **enrichment dependency** on a name-embedding artifact, plugging into
the same managed-enrichment-dependency mechanism `GRAPH.md` defines for graph
indexes. A richer boolean candidate-search DSL can be layered on top later, but
the v1 runtime intentionally exposes one blocking strategy per resolver.

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

Driving it in the DB:

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

Resolver catalog/runtime invariants:

- Source artifacts are a fanout boundary. Multiple resolvers may consume the
  same `source_artifact`, and the resolution worker runs every matching resolver
  for a changed source artifact.
- `resolution_artifact` names are unique across the resolver catalog. This gives
  each resolver an unambiguous durable output stream and lets review/promotion
  scopes use `(source_artifact, resolution_artifact)`.
- For an existing resolver name, `source_artifact` and `resolution_artifact` are
  immutable stream bindings. A new source/output stream is a new resolver, not an
  in-place upsert that would orphan old resolution artifacts.
- Asset source-index markers are maintained by the storage paths that write or
  delete asset artifacts, including async enrichment runtime writes. Resolver
  backfills use that marker index as the fast path. Upgraded stores that already
  contain asset artifacts without markers are repaired by a separate bounded
  legacy asset scan, which persists missing markers and then converges back to
  the marker index.
- Adding a resolver or materially changing its resolver/scorer config persists
  the catalog change and marks the re-resolution cursors dirty in one store
  transaction. Existing source artifacts are replayed in bounded windows through
  the normal resolution, promotion, and graph materialization stages. Retrying
  the same resolver upsert is idempotent: if the material config no longer
  changes but a dirty cursor remains from a prior partial attempt, the retry
  drains that pending backlog before returning.
- Declarative table provisioning is authoritative for resolver definitions.
  Reconcile does not skip an existing resolver name; it calls the same durable
  resolver upsert path as the API and reports whether the resolver was inserted,
  materially updated, or unchanged. This keeps table metadata changes to
  `config_generation`, scorer weights, candidate search, entity table, and
  templates on the production backfill path instead of becoming add-only
  bootstrap state.
- Replay retention is based on resolver-backed stage state, not generic journal
  hints. While a configured resolver pipeline is behind, or a resolution /
  promotion stage has reached an actual blocked state, journal truncation is
  clamped to that stage's applied sequence. A never-configured resolver pipeline
  does not pin unrelated replay records. Removing a resolver first drains
  bounded automatic work and then refuses catalog deletion with
  `ResolverReplayPending` if resolution or promotion still has pending or
  blocked replay. That prevents retiring the catalog entry a pending resolution,
  promotion, or human-review continuation still needs to interpret durable
  artifacts.

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
- **Single promotion owner per source shard.** In raft deployments every replica
  applies the same source-table write, but only the source shard's current local
  leader may turn the resulting resolution replay into public entity-table writes.
  Followers keep the `promotion` checkpoint unapplied and report
  `blocked_reason="not_source_group_leader"` until leadership moves to them. This
  keeps idempotent entity merges from multiplying raft proposals by replica count.
- **Provenance is inbound mention edges**, not a `provenance: [...]` array on the
  entity doc. "Which documents mention this entity" = the doc->entity mention
  edges the materializer already writes, with replace-on-rerender and
  delete-on-source-delete semantics. This avoids hot-key read-modify-write
  contention and unbounded array growth on popular entities, and reuses the
  graph machinery we are already building.

### Deterministic resolver makes the promoter optional in phase 1

A deterministic `key_template` computes a fallback canonical key **purely from
extracted text** -- no global state. The resolver still records that decision in
a durable resolution artifact, and the graph materializer emits provenance only
by replaying that artifact. Consequence: canonical `new` or `match` decisions can
produce `doc -> entity` edges before the entity document exists, but their target
is always the resolved DocRef, not a speculative extraction-time render. `review`
decisions are deliberately not canonical: they remain durable in the resolution
artifact and review queue, but they do not create entity documents or ordinary
doc->entity provenance edges until a curator override re-resolves them. So in
phase 1 the **graph works end-to-end without the promoter** for canonical
decisions; the promoter's job is to make those canonical docs exist for
hydration, search, and display. This de-risks the first ship without leaking
unresolved review state into the canonical graph.

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

### Sync and visibility contract

Semantic resolution is not part of the public `full_index` barrier. `full_index`
continues to mean that the primary write and ordinary managed index work
(including extraction artifacts and graph/full-text/vector materialization driven
directly by that write) are query-visible. Resolution and promotion are separate,
durable replay stages that may lag behind `full_index`.

That separation is intentional: a resolver can produce `review` decisions that
require a human or external curation workflow, so no normal write sync level can
promise "all semantic resolution is complete" without risking an unbounded wait.
When a curator does act, the DB curation entry point commits the resolver-scoped
override and the replay enqueue atomically. The replay record targets the exact
source extraction artifact for that resolver; resolution, graph materialization,
and promotion then progress through the same bounded worker/checkpoint path as
automatic changes.
Callers that need semantic readiness must inspect the stage status:

```json
{
  "resolution": { "applied_sequence": 120, "target_sequence": 123,
                  "catch_up_required": true },
  "promotion":  { "applied_sequence": 119, "target_sequence": 122,
                  "catch_up_required": true, "blocked": false }
}
```

Query paths that depend on resolved entity state must be explicit about this:
fail closed or omit unresolved/hydration-missing entity nodes by default, expose
pending/review state where appropriate, and only add a future opt-in semantic
wait mode for automatic-only stages. That future mode must return structured
blocked status rather than waiting for human review.

Resolver removal follows the same rule. It is not a synchronous semantic
completion barrier and it must not discard catalog context that pending replay
still needs. The production remove path drains what can run immediately, checks
resolution and promotion status, and returns structured pending state when a
stage is blocked or behind. Operators can then inspect the stage status, resolve
the external blocker (for example leadership or a human-review decision), and
retry the idempotent removal.

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

## Status

The phasing plan and test plan (phases 1-3: deterministic resolver,
learned/reviewed fusion, and merge/split) are implemented. The scorer and
comparators live in `lib/matcher`; the resolver core, resolution stage, and
`DocRef` type live in `lib/resolver`; the resolution and promotion managed
replay stages are `resolution_runtime.zig` and `promotion_runtime.zig`;
cross-shard candidate blocking and the promoter's cross-shard entity sink are
`api/distributed_candidate_source.zig`; and the resolver catalog persists
through `resolver_catalog.zig` and `table_provisioner`. Test coverage spans
`lib/matcher`, `lib/resolver`, and the `antfly-storage-db-test` /
`e2e/antfly/test_resolution.py` suites. Two follow-ups remain open and are
tracked in "Cross-shard candidate blocking" below: a name-embedding
enrichment to feed cross-shard ANN blocking, and atomic entity+edge coupling
via a future graph-edge participant in `TableCommitRequest`.

## Cross-shard candidate blocking

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
   Unit-tested with a fake `TableReadSource` (`antfly-api-resolution-source-test`) and a
   fake `CandidateSource` (`antfly-storage-db-test`).
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
   Passes `public-api-parity-test`; the live multi-node e2e exercises the
   cross-shard read path end to end.
4. **Promoter dependency.** Cross-document linking pays off once the promoter
   writes canonical entity docs. The promoter's cross-shard entity upsert uses
   the same topology + transport through `DistributedEntitySink` and the
   transactional `TableCommitRequest` path, so promoted entities are visible to
   later resolver candidate reads. Verified by db-tests and the live multi-node
   e2e.
5. **Mention hydration.** Mention edges carry the resolved entity table and
   graph hydration buckets result nodes by effective table, so a document graph
   query can hydrate promoted entity docs across shards. Verified by db-tests and
   the live multi-node e2e.

**Open follow-ups:**

6. **Embedding generation.** ANN also needs the mention `name_embedding` to be
   produced -- a name-embedding enrichment over the extraction entities (an
   `embedding` artifact the resolver reads), reusing the dense-embedding
   producer already in the enrichment runtime.
7. **Entity + edge atomicity.** Entity promotion is transactional across entity
   shards, but atomically coupling the entity upsert with graph-edge artifacts
   still needs a graph-edge participant in `TableCommitRequest`.

Recommended order: (a) the name-embedding enrichment to feed the cross-shard
`ann` source, (b) graph-edge participation in `TableCommitRequest` for optional
entity+edge atomicity.
