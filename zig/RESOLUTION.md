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
   - [ ] db-backed `ArtifactStore` over the shard primary store
         (`beginRead/get`, `beginWrite/put`, `beginBatch/delete`) to back
         `processChangedExtraction` in production.
   - [ ] `ResolutionRuntime` worker: catch up on the `resolution` hint
         (`applied_sequence`), call `processChangedExtraction` per changed asset
         key, journal the resolution key via a `DerivedBatch`; submit on the
         shard `backend_runtime` durable lane; wire init/start/shutdown in
         `db.zig`.
   - [x] Resolver catalog config (`resolver_catalog.zig` `ResolverConfig`) +
         per-shard persistence in `IndexManager` + `addResolver` / `removeResolver`
         / `listResolvers` through DB -> DBCore -> IndexManager (verified by a
         reopen persistence test).
   - [x] `table_provisioner` parsing: ingest a `resolvers` section from table
         config so resolvers are declarable, not just API-driven.
   - [ ] Live candidate blocking adapter: fetch candidates from the entity table
         (`ann`/`exact`/`prefix`) so the resolver runs against real entities.
   - [ ] Promoter: entity upsert via `DocumentTransform`, provenance as mention
         edges, decoupled cross-shard write, fail-closed hydration.
   - [ ] `DocRef` endpoints threaded through graph edge artifacts.
2. **Learned + reviewed (phase 2).**
   - Learned weights (EM / logistic regression) over the same levels.
   - REVIEW band workflow: review queue, human curation, label capture; resolver
     decision provenance and `pending_review` state.
   - Naive -> calibrated fusion across multiple extractors.
3. **Merge/split (phase 3).**
   - Entity `merged_into`, config-generation re-resolution, edge rewrite.
   - Optional 2PC coupling of entity + edge writes.

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

Resolver / promoter integration (to come):

- The `resolution` worker advances `applied_sequence` only after the durable
  job's write; a crash before the write replays the same extraction key.
- Live candidate blocking returns the right ~k entities from the entity table.
- Promoter upsert is idempotent under replay; concurrent promotions union
  aliases.
- Provenance mention edges appear/disappear with source documents.
- Materializer writes edges to resolved `DocRef` endpoints; hydration of a
  not-yet-promoted entity fails closed.
- Fusion combines per-source confidences into the edge weight from a pinned
  prior snapshot.
