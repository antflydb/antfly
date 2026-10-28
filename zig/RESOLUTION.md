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

## Non-Goals

- No curation UI. Bounded review resources expose REVIEW mentions and accept
  durable curator decisions; applications supply their own workflow.
- No learned scorer weights. Optional model adjudication of REVIEW uses a frozen
  typed-decision policy; the deterministic matcher and its weights remain explicit.
- No two-phase-commit coupling of entity writes and edge writes. Entity and
  edge writes are decoupled and fail closed on hydration (see "Cross-Shard
  Placement").
- No entity merge/split rewrite engine. That belongs to `GRAPH.md`'s
  merge/split design.

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
  (this is also the merge/split path from `GRAPH.md`: bump the resolver
  config generation, re-resolve, and let edge replacement rewrite stale
  edges).

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
`zig/pkg/antfly-embedded/matcher/` and is pure, deterministic, and allocation-light so it is
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

Supported comparators (see `pkg/antfly-embedded/matcher/src/mod.zig`):

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
`probability >= review` -> REVIEW (eligible for configured model adjudication or
curator override; produces no durable canonical link while it remains REVIEW).
otherwise -> NO_MATCH (mint a new entity).

`Scorer.explain()` returns the matched level per comparison, which is both the
review-UI breakdown and the signal for bootstrapping learned-resolver labels.

### Deterministic vs learned: same structure

The levels are the features. In deterministic mode the weights are hand-written.
A learned mode (`weights.mode: "learned"`) is not yet implemented; it would fit
the **same** levels' weights by EM (Fellegi-Sunter) or logistic regression over
labelled pairs, leaving blocking, comparators, and the decision interface
unchanged -- only the numbers would move. Inference would scale because
blocking already cuts candidates to ~k, so the model would only score
mention x k. The hard part is labels: the deterministic resolver already
bootstraps high-confidence matches/non-matches, which is the intended label
source once human review is added on top.

### Model adjudication of REVIEW (#1176)

A resolver can attach a frozen `adjudicate` policy to its deterministic scorer.
The matcher stays pure and nominates its best candidate. The managed resolution
stage calls the ordinary decision asset producer through an injected executor.
The same implementation serves embedded Lite, Directory, distributed resolution,
and ephemeral query resolution. There is no second model/provider client.

```json
{
  "name": "concepts",
  "table": "concepts",
  "source_artifact": "mentions",
  "resolution_artifact": "resolved",
  "key_template": "concept/{{ uslug _entity.text }}",
  "candidate_search": "prefix",
  "scorer": {
    "comparisons": [{
      "name": "name",
      "left": "m.text",
      "right": "c.canonical_name",
      "levels": [
        {"when": "exact", "weight": 8},
        {"when": "jaro_winkler > 0.85", "weight": 1},
        {"else": true, "weight": -8}
      ]
    }],
    "combine": {"bias": 0},
    "decision": {"match": 0.95, "review": 0.6}
  },
  "adjudicate": {
    "decider": {"provider": "antfly", "model": "convaiinnovations/laya"},
    "template": "A: {{m.text}} — {{m.description}}\nB: {{c.canonical_name}} — {{c.description}}",
    "question": {
      "type": "choice",
      "name": "same",
      "instructions": "Do these names and descriptions identify the same entity? Different products and different versions are different entities.",
      "choices": [{"value": "same"}, {"value": "different"}]
    },
    "accept": {"choice": "same", "min_probability": 0.9},
    "reject": {"choice": "different", "min_probability": 0.9},
    "on_failure": "review",
    "max_pairs": 32,
    "max_input_bytes": 16384,
    "timeout_ms": 10000,
    "budget_ms": 10000
  }
}
```

This is a resolver declaration nested in a graph config, not a complete graph
index request. ANN blocking still requires the normal candidate index and mention
embedding configuration. The model name is an example, not an identity-precision
qualification. Measure false merges and abstention on your own held-out pairs.
Neither cosine similarity nor a typed model's confidence is a measured precision
guarantee. `embedding_similarity` cannot satisfy this probability-based policy.

**Selection and outcomes.** The resolver retains its best deterministic candidate
policy. Equal scores use table/key order, independent of provider enumeration.
MATCH and NO_MATCH do not call the model. Only the best scored REVIEW pair is
judged, at most once per mention in a source execution. A REVIEW caused by an
unavailable redirect survivor cannot be repaired by model judgment. Type/label
admission, explicit aliases, durable merges and curator overrides retain priority;
curated mentions do not incur adjudication calls.

An accepted choice links through the same validated survivor and canonical fields
as MATCH. A confident configured rejection produces `new`. A response below both
thresholds abstains and stays REVIEW. Without `reject`, every non-accepted answer
stays REVIEW. Failure and exhausted budgets default to REVIEW; `on_failure: mint`
explicitly opts into a new identity on those outcomes. Cancellation and ownership
errors propagate without publishing a fallback. REVIEW has a provisional key but
is withheld from promotion; `listPendingReviews` provides the existing internal
listing, not a public paginated review service.

**Authoritative evidence.** Each judged entity carries `adjudication` alongside its
final decision. Evidence records the policy hash, complete execution/source hash,
input and candidate fingerprints, candidate endpoint, original deterministic
probability, outcome/failure, and the normalized decision response (resolved model,
full distribution, usage and producer provenance when supplied). The original
resolution confidence remains the deterministic score; model probabilities are
separate evidence. Frozen provider credentials are never included in evidence or
public identity inspection. A provider's mutable model name is not an exact asset
revision; use a pinned identity where the provider supports one.

The receipt is outside `_resolution_work` and survives disabled retention, cache
loss and portable backup. Recovery of the same source/execution reuses committed
identity and evidence without candidate search, embedding, or model inference.
The existing source transaction publishes the decision and downstream replay
handoff atomically. A crash before publication may repeat inference; execution is
not exactly-once. A committed failure is replay-stable too. Retry it by explicitly
re-resolving, rather than relying on a later drain to flip identity.

Policy changes participate in material config comparison, advance the effective
resolver generation, and use ordinary bounded corpus backfill and inline graph
replacement. A generation bump explicitly revisits committed evidence. Changed
source extraction also creates a new execution. Destination edits affect future
decisions; they do not silently revise committed model verdicts. Candidate/survivor
identity fields and selected description assets are rechecked after model execution;
concurrent changes return Busy for retry. Existing source/config/override read-set
and ordered-publication checks fence stale publication. Databases using this policy
require reader epoch 5; older readers must reject the database before parsing.

**Description inputs.** Extraction mentions may supply `description` and a numeric
`description_embedding`; candidate document fields supply the corresponding text
and vector. The existing cosine comparator handles explicitly supplied description vectors.
ANN hydration remains scoped to name vectors; it never substitutes a name vector
for a missing description vector. Supply both description vectors in a compatible
model space; description-vector generation is an upstream/application concern.
Missing description comparisons should use a neutral `else` weight when the desired
fallback is name-only. Templates interpolate `m.text`, `m.canonical_text`, `m.label`,
`m.description`, `c.canonical_name`, `c.label`, and `c.description`; absent text
renders empty. This interpolation language does not execute helpers or scripts.

Optional `description_inputs` binds asset artifacts explicitly:

```json
{"description_inputs": {
  "mention_artifact": "mention_descriptions",
  "candidate_artifact": "concept_description"
}}
```

Place this object inside `adjudicate`. The mention asset belongs to the source
document and contains a JSON object such as `{"e0":"A search database"}`, keyed
by extraction-local ids. It is supported for asset-scoped resolvers and cannot be
the extraction or resolution stream itself. Its publication/deletion re-drives the
owning extraction through the existing resolution journal, with its contents or
absence included in the execution fingerprint. The candidate asset is a text
asset on the canonical candidate document, such as a generator enrichment output.
It is projected explicitly and rechecked after inference. Missing assets fall back
to directly supplied description fields, then names. Candidate asset updates affect
future decisions; bump the resolver generation to revisit earlier decisions.

Resolution never waits for a description producer or its own future entity's
promotion. Mention context must already be available; candidate descriptions may
come from previously promoted entities. This avoids a promotion/description
readiness cycle. No automatic retrospective candidate-dependency scan is introduced.

**Bounded execution and queries.** `max_pairs` bounds model invocations per source,
`max_input_bytes` bounds rendered text, `timeout_ms` bounds an individual call, and
`budget_ms` bounds the source's total model-time allowance. Calls use the shared
decision executor's token, memory, rate, and provider capability limits. Query
resolution applies the same policy against an ephemeral store and returns evidence
without writing artifacts, promoting entities or recording curator overrides. Its
model costs are bounded by the resolver policy too.

This extension resolves mentions; it does not infer a global merge of established
keys, reconsider every old mention after a new description, or merge a transitive
component of positive model pairs. Use the existing explicit key-merge API or an
explicit corpus re-resolution for those operations. A rejected homonym still uses
the configured key template: identical rendered keys are not disambiguated by a
pairwise model rejection. Multi-candidate arbitration remains a separate extension; the curation
resources below expose unresolved REVIEW mentions.

### Why not a general scripting language

The scorer is intentionally **declarative config + a tiny predicate grammar**,
not embedded Lua/Starlark, because it must be:

- index-pushdown-analyzable (for blocking),
- pure/deterministic (for replay),
- introspectable (to learn weights and explain decisions).

Generality lives in the comparator + level space, not in arbitrary code.

## Resolution Stage Runtime (backend_runtime)

The resolution stage worker body is implemented in `pkg/antfly-embedded/resolver` as
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

### Deterministic resolver makes the promoter optional

A deterministic `key_template` computes a fallback canonical key **purely from
extracted text** -- no global state. The resolver still records that decision in
a durable resolution artifact, and the graph materializer emits provenance only
by replaying that artifact. Consequence: canonical `new` or `match` decisions can
produce `doc -> entity` edges before the entity document exists, but their target
is always the resolved DocRef, not a speculative extraction-time render. `review`
decisions are deliberately not canonical: they remain durable in the resolution
artifact, but they do not create entity documents or ordinary doc->entity
provenance edges until a curator override re-resolves them. So the **graph
works end-to-end without the promoter** for canonical decisions; the
promoter's job is to make those canonical docs exist for hydration, search,
and display. This keeps unresolved review state out of the canonical graph.

### Cross-shard placement

Entities are sharded by entity key and generally live on a different shard than
the source document. Two options:

1. **Transactional**: wrap the entity upsert (entity shard) and the source-shard
   edge artifact in one 2PC `BatchRequest` (predicates + participants). Gives
   read-your-write consistency between entities and edges.
2. **Decoupled (current)**: promoter upserts the entity in its own write;
   materializer writes edges referencing the entity key independently; hydration
   **fails closed** if the entity doc is not yet present (already mandated for
   external nodes in `GRAPH.md`).

Antfly uses decoupled + fail-closed today. 2PC is a later hardening step for
when read-your-write entity guarantees are actually required (see "Open work").

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
from the start, even though today it only hydrates same-table:

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
- A learned-weights config without a trained model artifact (not yet
  supported; see "Open work").
- A fusion `prior.from = graph` without a pinned snapshot policy.

## Status

The design in this document -- deterministic resolver, promoter, fusion data
model, and cross-shard candidate blocking -- is implemented and tested. The
scorer and comparators live in `pkg/antfly-embedded/matcher`; the resolver core, resolution
stage, and `DocRef` type live in `pkg/antfly-embedded/resolver`; the resolution and promotion
managed replay stages are `resolution_runtime.zig` and `promotion_runtime.zig`;
cross-shard candidate blocking and the promoter's cross-shard entity sink are
`api/distributed_candidate_source.zig`; and the resolver catalog persists
through `resolver_catalog.zig` and `table_provisioner`. Test coverage spans
`pkg/antfly-embedded/matcher`, `pkg/antfly-embedded/resolver`, and the `antfly-storage-db-test` /
`e2e/antfly/test_resolution.py` suites.

Learned-weights resolution, a review queue/curation UI, entity merge/split, and
transactional entity+edge coupling are not part of this implementation; see
"Open work" at the end of this document.

## Cross-shard candidate blocking

Exact/prefix/embedding blocking are all implemented and tested, but on their own
they only see entities the resolution worker's own shard store can read.
Canonical entities normally live in a dedicated `entities` table on a *different*
shard, so meaningful cross-document blocking needs the worker to query that table
across shards. Sublinear ANN candidate generation has the same requirement (the
entity table's vector index is on the entity shard). Both are the same blocker,
and both are served by the same seam:

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
   Unit-tested with a fake `TableReadSource` (`antfly-api-test` filtered by `DistributedCandidateSource`) and a
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

Lite supports candidate reads and atomic promotion across named tables. A
configured `name_embedding` is generated from each mention using the destination
table's dense embedding index configuration, before ANN candidate blocking.
Configure `candidate_search: "ann"`, `candidate_ann_index`, `name_embedding`,
and `name_embedding_dims` to match that index. The entity table needs a dense
index over its canonical name; its embeddings and the query mention must use
the same model and dimensions. `run_until_idle_json` drains destination tables
as well as source tables, including recursive generator/resolver pipelines.

For known synonyms, write an entity document at the exact minted alias key
with `merged_into` set to the surviving canonical key (and `merged_into_table`
for a different table). The survivor must exist. Exact aliases are followed
before statistical scoring, even when no scorer is configured; dangling
redirects never authorize a match. This allows deterministic corrections such
as `concept/qwen3` → `concept/qwen3_embedding` without embeddings.

The `slug` template helper is ASCII-only for compatibility with existing keys.
Use the opt-in `{{ uslug _entity.text }}` helper for Unicode identities. It
normalizes with NFKC, folds case, retains Unicode letters and numbers, and
separates runs with `_`. `Qwen3`, `qwen3` and `Ｑｗｅｎ３` become `qwen3`;
`Straße` becomes `strasse`; `東京 大学` becomes `東京_大学`. The Unicode 16.0.0
normalization tables are checked in, independent of the host locale. Existing
non-empty `slug` keys retain their spelling. Both helpers reject an empty
result with `EmptyEntityKey`, including all-CJK inputs passed to ASCII `slug`,
so a resolver cannot silently mint `entity/`. Keep the original mention in
`canonical_name` and `aliases` for display and search.

Resolution merges mentions that extraction found; it cannot recover a missing
subject. For short claims, use specific labels and descriptions naming the
expected domain (for example, model or software component), and measure recall
on representative claims while lowering the extraction threshold. Lower
thresholds also increase false positives. Track missing main subjects separately
from synonym resolution and keep an application-supplied subject when available.

## Open work

- **Entity + edge atomicity.** Entity promotion is transactional across entity
  shards, but atomically coupling the entity upsert with graph-edge artifacts
  needs a graph-edge participant in `TableCommitRequest`. Until then, entity
  writes and edge writes are decoupled and hydration fails closed (see
  "Cross-Shard Placement").
- **Learned resolver.** `weights.mode: "learned"` is not implemented. The
  comparator/level structure is designed to support it (see "Deterministic vs
  learned: same structure") once labelled pairs are available.
- **Review queue / curation UI.** Curators can record an override for a
  `review` decision through the resolution API (see "Sync and visibility
  contract"), but there is no queue, UI, or label-capture surface for
  triaging pending `review` decisions.
- **Entity merge/split.** Rewriting entities after a merge or split is
  `GRAPH.md`'s merge/split design, not this document's.
- **Semantic-wait read mode.** Callers that need semantic readiness inspect
  resolution/promotion stage status directly today (see "Sync and visibility
  contract"). A future opt-in wait mode for automatic-only stages would need
  to return structured blocked status rather than waiting on human review.

Recommended order: (a) the name-embedding enrichment to feed the cross-shard
`ann` source, (b) graph-edge participation in `TableCommitRequest` for optional
entity+edge atomicity.


### Per-mention admission and replay isolation

A mention whose `slug` or `uslug` is empty is skipped before embedding or candidate
blocking. Other mentions in the document still resolve. The resolution artifact
records `skipped_mentions`, and the native log identifies the skipped local ID.
Invalid templates and resource failures still fail the operation.

Graph replay ignores corrupt persisted resolution entries individually, preserving
valid siblings and logging each omission. An unparseable resolution artifact contributes
no endpoints; it does not invent owning-document or raw-text fallback endpoints and
does not prevent unrelated artifacts from materializing. Allocation/storage failures
still propagate.

ANN vector hydration follows the scorer's candidate field names for comparisons
against the mention `name_embedding`. Text-only scorers consume the original stored
JSON. A hit whose indexed vector disappeared remains a candidate with a missing
value; it does not abort processing of the remaining hits.


### Original two-hop request (#1089)

The regression uses the original request verbatim (with the configured index name
substituted for `<graph>`), including default node deduplication. It asserts the
second claim is reached through a shared concept, with paths and documents enabled.
The same request also passes on the issue's reported revision `1258e8cfcb`, returning
the second claim at depth 2 with its document and a three-node path. This rules out
a general unsupported-option/parser restriction at that revision. The original
failure is not reproduced; identifying it requires the original database state or
linked native binary rather than guessing a routing fix.

The receipt and test-only patch are retained in
`pkg/antfly-embedded/testdata/issue1089_two_hop_baseline.{json,patch}`. To reproduce,
create a detached worktree at `1258e8cfcb`, apply the patch, then run from `zig/`:

```sh
python3 tools/run_bounded_zig_build.py build capi-test -Dembedded-only=true \
  '-Dtest-filter=--test-filter=capi issue1089 original revision exact two hop request' -j1
```

## Query keys, aliases, and migration (Lite native)

Use `antfly_db_render_resolver_key_json(db, resolver_name, mention_json, &out)`
with `{"text":"Qwen3","label":"concept"}`. It reads the durable resolver,
executes its exact key template (including Unicode 16 NFKC/case folding and
attached combining marks), and returns `minted_key`, alias-canonical `key`,
`table`, and `config_generation`. Free the result with `antfly_buffer_free`.
Python exposes `db.render_resolver_key("concepts", mention)`; Go exposes
`db.RenderResolverKeyJSON("concepts", mentionJSON)`. This is a read-only render
operation: it does not extract text, run ANN, or create documents. Extract
mentions using `Inference.extract` first if needed, or use `resolve_query` below
to run the configured extractor and ANN resolution together. Query callers must
use the configured resolver name, not copy its template into application code.

Inline resolvers accept aliases between **fully rendered keys**, for example:

```json
"aliases": [{"key":"concept/qwen3","target":"concept/qwen3_embedding"}]
```

These explicit aliases take precedence over fuzzy matches and require no
canonical entity documents. Set `promote: false` and `table` to `default` to keep keys in the source
table without creating an entity table. With `promote: false`, both minted and aliased mentions produce graph keys
without materializing entity documents. Alias chains are followed;
empty keys, duplicate sources, and cycles are rejected. Aliases use exact key
bytes; use the render API when deriving either endpoint from text. Known alias
redirects also canonicalize scorer-selected keys in the resolver's table.

To merge an existing key after ingestion, call
`antfly_db_merge_resolver_keys_json(db, resolver, {"key":"concept/old","target":"concept/new"})`,
or Python `db.merge_resolver_keys(resolver, old, new)`, or Go
`db.MergeResolverKeysJSON(resolver, requestJSON)`. This atomically persists the
redirect and its corpus-backfill cursor, bumps the resolver generation, and
returns before replay. Repeat requests are idempotent; re-pointing an existing
redirect requires `replace:true`. Optional `pin_distinct:true` pins the former
target apart from the corrected identity. `run_until_idle`
then re-resolves stored extraction artifacts and replaces their mention and
relation edge contributions. It retains unrelated graph contributors. Reopen
resumes pending backfill from the durable catalog/cursor. Concurrent rendering
may observe either complete resolver generation, not an uncommitted alias.

This operation changes **resolver-owned identity**, not arbitrary manually
supplied edges. With promotion enabled, its existing merge path updates entity
documents and leaves redirects for prior promoted keys. With `promote: false`,
entity documents are untouched;
application references outside resolution artifacts need their own migration.
Human-curated per-document overrides keep their existing precedence. Source
artifacts must still exist for replay. Treat a template/helper change as an
identity migration; aliases preserve old query seeds while replay converges.
Explicit unmerges mask an inline alias across configuration replay; update
declarative aliases when deliberately migrating the namespace.

## Embedding merge recipe and ordering

The supported setup uses public table/index calls; it does not silently create
or take ownership of an application's entity table. The real Qwen regression
`TestLiteLocalResolverEmbedsMentionsAndMatchesStoredCanonicalEntity` executes
this recipe through `AddIndexJSON` and tests a non-empty merged key. Set
`ANTFLY_LITE_EMBEDDING_MODELS_DIR` to a model root containing the Qwen bundle to
run it.

1. Open Lite with profile `native`, local inference enabled, and an explicit
   `models_dir` containing both extraction and embedding bundles. For a JSON-field
   artifact without a model producer, also enable `generated_enrichment_replay`.
2. Create an `entities` table and open its handle. Add this index to it:

```json
{"name":"names","type":"embeddings","dims":1024,"field":"canonical_name","metric":"cosine","embedder":{"provider":"antfly","model":"Qwen/Qwen3-Embedding-0.6B-GGUF"}}
```

3. Add an artifact-backed graph index to the source table with this inline
   resolver (the `kg` artifact can be produced by an extractor or a JSON field):

```json
{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","candidate_search":"ann","candidate_ann_index":"names","candidate_limit":25,"name_embedding":"names","name_embedding_dims":1024,"type_must_match":true,"scorer_json":"{\"comparisons\":[{\"name\":\"embedding\",\"left\":\"name_embedding\",\"right\":\"name_embedding\",\"levels\":[{\"when\":\"cosine > 0.9\",\"weight\":8.0},{\"else\":true,\"weight\":-6.0}]}],\"combine\":{\"bias\":-3.0},\"decision\":{\"match\":0.9}}","config_generation":1}
```

`name_embedding` names the destination table's configured embedding index, not
an arbitrary model ID. The mention and candidate must use the same model and
dimensions. The cosine floor (`0.9` above) controls semantic similarity; the
matcher decision (`0.9`) is a separate logistic-score threshold. Calibrate both
on actual synonyms and hard negatives. Keep `type_must_match` unless your label
policy explicitly allows merging across types. Known aliases need no seed
documents; optional curated ANN seeds use `canonical_name` and `entity_type`.

A successful **database-root** `run_until_idle` drains all tables repeatedly,
including embeddings queued by cross-table promotion. Entities promoted during
that drain are ANN-visible to requests submitted **after it returns**. Candidate
search inside a drain can precede another document's promotion/embedding;
within-batch document order is not an ANN visibility guarantee. For deterministic
incremental merging, submit one ingestion unit, drain the root, then submit the
next. Batch ingestion may mint separate keys; use the explicit merge API and
another drain to reconcile them. A failed/non-convergent drain does not provide
the barrier. This contract avoids order-dependent claims that a replay batch
or candidate snapshot cannot satisfy.

## Comparing extraction configs

`scripts/eval_extract.py` runs two or more extraction-v2 configs against the same
labelled JSONL corpus through the public embedded inference API. Example:

```sh
PYTHONPATH=py/packages/embedded/src python scripts/eval_extract.py \
  --models-dir "$ANTFLY_INFERENCE_MODELS_DIR" \
  --corpus scripts/testdata/eval_extract/corpus.jsonl \
  --config scripts/testdata/eval_extract/plain.json \
  --config scripts/testdata/eval_extract/descriptions.json \
  --output comparison.json
```

Set `ANTFLY_LIBRARY` if the shared library is not on its normal search path.
The report records installed models, config/corpus SHA256s, effective configs,
raw outputs, timings, per-label mention counts, and exact-span precision,
recall and F1. Annotation offsets are Unicode code points, end-exclusive.
Duplicate label/spans are counted once; repeated text at different offsets is
counted separately. Every row needs an explicit `entities` list, including
negative examples. Undefined denominators are `null`, not perfect scores.
Malformed annotations, incompatible response offsets, and inference errors
fail the run. The bundled two-document corpus is a smoke fixture, not an
accuracy benchmark. Use held-out domain annotations before choosing a config.

Descriptions are prompt/schema input, not output filters; changing them can
change logits, token budget and which candidates cross `threshold`. Compare
both descriptions at fixed threshold/window geometry first, then sweep
thresholds on a development split and evaluate the selected setting on a
held-out split. Longer windows increase context and attention cost while
reducing window count; overlaps can recover boundary mentions and affect
cross-window duplicate merging. Keep identical geometry when isolating the
effect of descriptions, and inspect empty outputs as well as precision.

Windowed descriptions and single-label classification are qualified for the
FP32 base and multi-Decide bundles described in [the qualification guide](pkg/inference/models/gliner2/GLINER25.md). The fp16-encoder
base remains qualified only for its existing feature set; windowed descriptions
are unsupported and **not planned for this implementation**. Its measured Metal
throughput is worse than FP32, and FP32 remains the recommended bundle. A future
fp16 description qualification requires pinned upstream non-empty fixtures and
reviewed parity tolerances; this change does not widen its advertised capabilities.

### Resolve ad-hoc query text and mentions

`antfly_db_resolve_query_json(db, resolver_name, request, out)` (Go
`ResolveQueryJSON`, Python `Database.resolve_query`, Rust `resolve_query_json`)
accepts exactly one of:

```json
{"text":"What did Apple announce in Cupertino?"}
```

```json
{"extraction":{"entities":[{"id":"q0","label":"company","text":"Apple"}]}}
```

The text form invokes the extractor producer on the resolver's source asset,
with its durable model, schema, descriptions and thresholds. The extraction form
lets callers keep their own GLiNER extraction and still use Antfly's full
resolution pipeline. Both return `{extraction, resolution}`; `resolution.entities`
contains canonical `doc_ref` (table/key), decision, confidence, and mention
identity. Empty or unkeyable mentions are reflected in `skipped_mentions`.

Resolution uses the same label routing, sibling identities, explicit aliases,
exact/prefix/ANN candidates, configured mention embeddings, and scorer as
write-time resolution. Configure `candidate_search:"ann"`, `candidate_ann_index`,
`name_embedding`, dimensions and the scorer as in the embedding-merge recipe
above. Supplied mention vectors are accepted too. Existing embeddings must be
visible before querying; drain the root after writing candidate documents.
Explicit aliases do not require canonical documents or embedding IO.

Query extraction and resolution never publish a synthetic document, resolution
artifact, promotion, graph edge, or replay record. They also work on read-only
handles. Read-only text queries lazily cache a foreground extractor producer on
the handle, reusing its inference provider and loaded models across calls.
Only cache construction holds the producer mutex; each extraction gets an
invocation context and uses the synchronized capability cache. Native Lite
captures an owned extractor plan and pins its root inference snapshot, then
releases the connection, path gate and file lease during text extraction.
Concurrent text queries and writer commits can proceed on the same database
path. The short canonical lookup/resolution phase reacquires a current snapshot;
a resolver or extractor config change during extraction returns a retryable busy
error. Model execution/admission gates still apply. Named tables share their root
snapshot's cache. Retired snapshots retain providers until their last in-flight
query finishes; the cache is released with the snapshot.
They use resolver-level aliases and ignore document-specific curation
since the query has no stored document identity. Queries currently require an
asset-scoped resolver (including `source_artifact_kind:"any"`); a chunk-scoped
resolver is rejected rather than inventing chunk context. The text form requires
an extractor producer; copy-only or missing source assets fail with a diagnostic.
For callers that only need template/alias rendering, `render_resolver_key` stays
available and avoids extraction, embeddings and candidate search.

### Durable merges, config replay and compatibility

API-created key merges live in `resolver_merges_v1` metadata, separately from
inline `aliases`. The resolver catalog records their version and generation;
merge state, generation, and backfill cursors commit in one transaction. Normal
inline config upserts preserve merge state even when callers omit all aliases.
Callers can keep reapplying declarative index configs on open without reading
curated merges first. Replaying an identical native index config on an existing
index is idempotent, including after close/reopen; a different physical config
is rejected. Durable merges take precedence over an inline redirect of
the same source. Retiring a resolver retains its associated identity state; explicit identity
deletion is allowed only after every declaring index has retired it. Splits and portable backups copy both catalogs. Memory/file restore and live
import validate and preserve durable merge state. Once merges exist, changes to
`table` or `key_template` are rejected with a namespace diagnostic; callers must
explicitly reset or migrate the identities before changing their namespace
using `set_resolver_namespace` (C `antfly_db_set_resolver_namespace_json`, Go
`SetResolverNamespaceJSON`, Rust `set_resolver_namespace_json`).
The effective resolution revision advances on either a config change or merge,
even when the incoming config generation equals the preceding merge revision.
For example, config generation 2 → merge → config generation 3 yields effective
revisions 2 → 3 → 4. Identical config replay leaves the revision unchanged.
Namespace operations preserve the caller-owned `config_generation` and advance
the internal clock, so reapplying the correctly updated inline config does not
schedule a second replay.
Missing, corrupt, cyclic, or unsupported required merge metadata fails opening
rather than silently resolving keys differently. Both mention and relation
edges are reconciled on the root drain, including removal of old endpoints.

The namespace request contains `table`, `key_template`, and exactly one of:

- `reset_merges:true`: explicitly discard durable redirects and inline aliases.
  Optional `aliases:[...]` supplies their complete replacement under the new
  namespace; omitting it leaves no inline aliases. This is also the explicit
  way to change aliases after a namespace fence has been installed.
- `key_map:[{"key":"concept/old","target":"topic/old"}, ...]`: map every
  stored inline/durable alias source and target, suppression key, unmerge mask
  and distinct-pair endpoint into the new namespace. Incomplete
  maps, empty keys, duplicate sources, resulting duplicate aliases and cycles
  fail before mutation. Existing merges are transformed, preserving identity
  relationships rather than deleting the resolver.

The operation validates the destination table and commits the config, merge store,
and replay cursors atomically. Call `run_until_idle` to reconcile resolver-owned
mention/relation edges. Application-owned documents and references are not
renamed. Update your declarative inline config to the new table/template and
mapped inline aliases before reapplying it. A durable namespace fence rejects
old table/template or alias configurations even after reset leaves zero merges.
Further identity changes require another explicit namespace operation. Changing an existing physical index config still requires
its normal delete/recreate lifecycle, which retains resolver identity and merges.
Resolver identity is scoped by source table and resolver name, independently of physical
index names: deleting index v1 and adding index v2 with the same resolver name
and source/output artifact bindings retains merges and namespace fences. Renaming
the resolver itself creates a new identity; removing it deliberately deletes its
curation. Inline declarations own resolver execution: deleting the last declaring
index retires its resolver, so rendering/query resolution returns not found and
new source writes do not run that resolver. A shared declaring index keeps it
active. Retired definitions retain merges and namespace fences in the portable
catalog, including across backup/restore; re-add/rename reactivates them through
the same namespace admission checks. Reactivation advances the internal revision
so source changes made while retired cannot reuse stale decisions. Independently
registered resolvers remain
active when an index is removed. Reapplying an older inline declaration records
its ownership without changing its resolution revision. Retirement is additive
catalog metadata and does not advance the required merge-state format version.
A previous reader that tolerates optional fields can open the database, but may
execute retired definitions; use a lifecycle-aware library when relying on
retirement to suspend execution. Native Lite and Directory C API handles both
reapply inline resolvers when adding an identical existing index.
Existing artifact/index repair APIs rebuild derived state under the declared
identity rules; they do not infer a key migration. This namespace operation
reuses the existing resolver replay/repair pipeline for derived reconciliation.

Only material resolution changes (including an explicit `config_generation`
bump) schedule replay; changing revision bookkeeping alone does not. A changed
scorer, routing rule or identity rule can affect every subscribed document and
requires scanning its source corpus. Backfill skips source artifacts whose
subscribers already have the current internal resolution revision, avoiding
unrelated journal writes, ANN searches and embedding generation. The internal
clock also advances on material changes before the first merge. Stale empty
resolutions with no currently routed mentions are skipped; nonempty previous
decisions are always revisited to retire removed labels/edges. A global scorer
change necessarily revisits every source with eligible mentions; it cannot
safely infer unchanged decisions without evaluating the new scorer. Older
catalogs acquire the internal clock on their first material change. Identical
config reapplication incurs no backfill. Portable import cross-checks sidecar
records against the resolver catalog before publishing destination state,
including missing required records and orphan/duplicate records.

Default resolver catalogs omit new default fields (`aliases:[]`, `promote:true`,
and zero merge metadata), retaining the previous wire shape. The current reader
tolerates unknown optional fields and rejects unsupported `merge_state_version`
values explicitly. **Downgrading a database using aliases, disabled promotion,
or durable merges to a library predating those features is unsupported.** Such
old strict readers reject the feature metadata; this is intentional protection
against silently losing identity rules. Keep a pre-upgrade backup for rollback,
or export primary application documents and rebuild a separate older database.
Do not delete merge metadata or strip feature fields to force an old open.
Files written by interim PR builds are not a supported downgrade format.

The normal native regression now ingests an extraction, promotes its first
entity, computes its embedding through the enrichment runtime, drains the root,
and matches a different mention through ANN. It uses a deterministic embedding
provider for both canonical entities and mentions so the ordering contract runs
in ordinary CI. The required public Python Qwen test downloads the SHA-pinned
Q8 bundle from the runtime registry, exercises the same promotion/drain/second-
mention path with real embeddings and checks ad-hoc ANN query resolution.
The opt-in Go Qwen test also exercises this contract. Neither test writes a candidate entity
or its vector by hand.

Both base and full-validation jobs run the required real extraction and public
Qwen embedding gates. The pinned CPU extraction CI test sets
`ANTFLY_LITE_REQUIRE_EXTRACTOR_TEST=1`: missing runtime/model support is an error,
not a skip. CI then runs `scripts/eval_extract.py` through its real-model gate on
`regression.jsonl`, compares plain/described schemas, checks minimum precision
and recall and non-empty correct spans for each label, and uploads the complete
report even on failure. This 23-document curated regression set includes six
negative documents, repeated names, accented/Arabic/Indic text, and a document
long enough to require windowing. It remains a regression set, not a claim of
production-domain accuracy; domain-specific labelled evaluation is still needed.

## Incremental resolution work

Resolution artifacts contain an optional, versioned `_resolution_work` cache.
Each extraction-local mention records its embedding dependencies, candidate
universe revision, candidate records and raw comparator features. This is
reusable work, not authoritative identity state: old artifacts without it work
normally, and malformed or unknown cache versions are discarded and recomputed.
Public `resolve_query` responses omit the private cache and do not serialize
or calculate persistence revisions for ephemeral work. Canonical records and
vectors shared by several mentions are stored once per artifact. A validated
candidate/mention revision also avoids rehashing unchanged vectors while
reusing their comparison features.

| Change | Repeated work |
| --- | --- |
| Match/review threshold, scorer weights or bias | Recombine cached features and classify candidates |
| Comparator, comparison fields or similarity cutoff | Reuse raw similarities with matching input digests; compute missing comparisons |
| Mention text or vector | Recompute that mention's dependent work |
| Mention label/confidence or sibling routing | Readmit mentions and recompute affected candidate/decision dependencies |
| Key template | Render keys again, refresh exact/blocked candidates, reuse embeddings and unchanged comparisons |
| Blocking strategy, ANN index, candidate limit or hydrated vector fields | Fetch the new candidate set; reuse comparisons whose inputs still match |
| Embedding producer or dimensions | Regenerate mention embeddings and dependent candidates/comparisons |
| Alias/merge or human review | Apply current redirects/overrides to freshly classified decisions |

Native Lite candidate revisions include the physical table binding, committed
source/enrichment sequences, selected index configuration and its applied replay
sequence. This covers canonical inserts/deletes, table recreation and vectors
becoming visible after an ANN drain. Mention embedding keys combine the model
identity, dimensions, effective provider settings (including input type,
instruction and truncation), and a hash of the exact input string, independently of the
source or destination table. Local capabilities expose identity before weights
load: managed downloads hash sorted relative artifact paths and pinned receipt
SHA-256 digests. Copying or restoring identical files preserves that identity.
Receipt digests are verified against file content. Verified digests are memoized
in memory and in disposable file-generation sidecars; inode, size, mtime and ctime
changes invalidate verification. Legacy managed receipts use the same persistent
content-hash memo, avoiding full rehash on every process reopen. Only
unmanaged local models fall back to resolved weight, shard, ONNX external tensor,
tokenizer and processor file generations (inode/size/mtime/ctime).
Model construction cross-checks file generations to reject mid-load changes, and
an already-loaded model retains its immutable identity. Unattested providers
conservatively re-embed. Runtime adapters without complete candidate revisions
refetch candidates while still allowing an attested embedding identity to reuse
mention vectors.
Embedding cache identities include the provider endpoint, region/project,
credential source and resolved API-key digest, secret-store generation, and all
embedding settings in addition to model revision. Keys never contain plaintext
credentials. Equivalent destinations can still share vectors; distinct tenants,
endpoints, rotated secrets and embedding settings cannot.

The distributed adapter currently takes this conservative path. Candidate cache
dependencies use the effective blocking strategy: a resolver without a scorer
uses exact-key lookup even if its declarative strategy is prefix or ANN.
Cross-table redirect candidates are refetched because the source-table revision
does not cover survivor changes in another table. Comparison input hashes still
allow safe feature reuse when the refreshed candidate fields are unchanged.

The work cache is part of the resolution artifact, so the existing atomic
artifact/replay commit, portable backup/import, source deletion and bounded
crash-resumable replay own its lifetime. Resolver catalog changes mark the
existing durable source-index cursors dirty in the same transaction. Replay
loads the current configuration, so a superseding change does not install old
cached decisions. Cache/revision-only refreshes with identical identity payloads
publish a graph-only handoff and completion marker; they do not repeat canonical
entity promotion. Changes to the promotion setting use the full handoff.

A global scorer/template change still visits eligible source artifacts and
mentions. It no longer requires rerunning extraction, embeddings, candidate
search or comparisons whose dependencies are unchanged. Reuse is deliberately
conservative: a destination mutation invalidates the table's candidate revision,
including empty candidate sets, even if it ultimately changes no matches.


### Embedded upgrade and identity management

Prefer `add_index({..., "replaces":"knowledge_v1"})` for graph index versioning.
Registration validates the request, reserves a hidden shadow and returns. The
existing maintenance owner builds bounded source pages and catches up replay,
saving its cursor and applied sequence after projection durability. The old
index serves until the final bounded tail and atomic catalog swap. Field-backed
changes re-process their changed sources rather than abandoning the build.
Catch-up coalesces bounded journal windows by source key; larger activation
windows resume per entry. Repeated updates to a hot document can share one
reconstruction per window. Replay retention advances after durable checkpoints;
it does not pin the entire journal.
Restart resumes the durable marker/checkpoints, without rebuilding completed
pages. Background construction retains its private manager across rounds, shares
a source snapshot across each page, and checkpoints its resumable progress.
Source pages collect at most 32 sources per turn, sharing their write plan,
artifact transaction and graph apply. Collection yields after 100 ms, followed
by the current bounded source batch. Resident managers are invalidated when the
catalog revision changes, so reused builders cannot retain stale resolver rules.
The private builder uses the LSM backend's existing bounded block cache to avoid
repeated native immutable-source lease publication across checkpoint runs.
It also drives the existing bounded foreground compaction policy, since private
builders have no independent compactor to limit checkpoint-run accumulation. A resident progress cursor lets these
short turns share a durability checkpoint: checkpoint the first turn, then at
most once per second during construction, and always before activation. The
journal remains pinned at the last durable cut; crash recovery repeats only
unacknowledged work. Checkpoint fsync remains required before the durable cursor
advances. The scan excludes unrelated metadata/journal keys, and replay begins
strictly after the last consumed sequence.

Explicit transient resource, disk-full and remote/network failures retry at most
four consecutive times with persisted 50 ms, 200 ms, 800 ms and 3200 ms backoff.
A durable source/replay checkpoint resets the consecutive count and backoff;
volatile work does not. Each accepted job also has a lifetime ceiling of 64
retries, which progress never resets. `retry_count`, `total_retry_count`,
`max_total_retries`, `retry_after_ns` (Unix nanoseconds) and `last_error` are visible
in replacement status. Generic `Unexpected`, `InputOutput`, `ReadFailed`,
`WriteFailed` and `UnexpectedReadFailure` errors fail immediately, rather than
presuming ambiguous storage failures are transient. Failure recording reloads the
latest durable checkpoint so it cannot undo progress from the failed turn.
Backoff survives reopen. When the scheduler reads a deadline more than 3.2 seconds
in the future, it persistently clamps it to that horizon; a backward wall-clock
step cannot impose an hours-long stall. Persisting the clamp once prevents it
sliding forward on every poll. Waiting jobs yield to later replacements and
ordinary maintenance.
A permanent failure or exhausted retry budget enters terminal `phase: "failed"`,
exposes `last_error`, releases its
journal retention pin and retires any inline execution owned only by that shadow.
It is excluded from scheduling: later replacements, artifact repair, drains and
backups continue. Backups export serving state and exclude failed shadow outputs.
Call `cancel_replacement(new_name)` to remove a running or failed reservation and
schedule bounded artifact cleanup; the serving old index stays intact. Repeated
cancellation is a no-op. Deleting the pending new name cancels its reservation.
Deleting the serving old name removes it and cancels its shadow in one catalog
transaction; a rejected deletion preserves the reservation and old index.
Cancel and drain cleanup before registering a fresh attempt after failure.
Plain `add_index` with the identical pending config joins its shadow even without
`replaces`; conflicting configs are rejected before changing catalogs or storage.
The final write fence consumes and durably checkpoints a bounded tail even when
the gap exceeds 1024. An intra-entry source cursor also permits yielding within a
large journal batch. Each pause advances completed sources rather than restarting
the tail. Ordinary activation uses a 100 ms cooperative work budget, followed by
the required durability checkpoint. After eight yielding attempts, the budget
increases to at most one second, still capped by an explicit wait deadline.
Escalation always has a finite deadline: it yields and persists its cursor when
that budget expires. A current batch (up to 32 sources), storage operation or
fsync may overrun the cooperative budget, so this is not a hard wall-clock bound
on writer latency. In-process writers block on the apply lock for up to about
one second during escalation, plus any in-flight batch and durability checkpoint.
Sustained producers can require further bounded rounds; the
old index keeps serving until the atomic swap. `activation_attempts` persists
across reopen and is reported with replacement progress.
Cutover coalesces windows containing at most 32 unique sources. Larger windows
use resumable per-entry batches, including during escalation. Repeated writes to
a hot document can coalesce within a window; the entire tail is not promised to
finish in one pass. Partial large entries retain a durable source ordinal.
Tail rebuilds share a source snapshot and write plan across batches of 32
sources. Graph owner cleanup also pins one coherent outgoing/reverse snapshot
pair for each batch, instead of reopening both stores for every document/prefix. Full document rebuilds cover their owned artifacts, so those artifacts
are excluded from the source scan and journal artifact pass. Resolution batches
also deduplicate owning extraction artifacts, rendering canonical relation
endpoints once after all mention resolutions are applied.

Identical in-progress requests join the existing reservation generation. A wait
captures that generation under registration admission and retains it across
quanta. Cancellation or target-name reuse returns `ANTFLY_CANCELLED`; a new
serving index or reservation with the same name (even the same configuration)
cannot complete the original wait. `replaces` and optional
`"wait":{"timeout_ms":10000}` are command fields, excluded from stored config.
Waiting cooperatively advances the same work, releasing handle/path locks between
quanta so another caller can cancel or write. A missing job record triggers a
second committed-catalog check, so a racing successful swap returns success.
`ANTFLY_CANCELLED` describes cancellation of the call: it includes an explicitly
cancelled replacement, callback cancellation, or interrupted IO/mutex admission.
Interrupting a worker or a call does not cancel or fail accepted durable work;
volatile work is discarded and the checkpoint remains resumable. If interruption
races completion, inspect the index/job state. Storage/admission errors propagate, and a manual runtime sleeps through
its control IO rather than spinning. Replacement precondition errors have distinct
codes in C, Python, Go and Rust: `ANTFLY_REPLACEMENT_FAILED`,
`ANTFLY_REPLACEMENT_PENDING` and `ANTFLY_REPLACEMENT_CONFIGURATION_MISMATCH`.
Clock-backoff correction leaves queued jobs unchanged on replicas, write-gated
handles and databases requiring ordered maintenance; it never chooses a local
write merely to shorten a retry deadline.
A pending plain replacement temporarily raises the minimum reader version to 2
and publishes a versioned resolver envelope. The envelope fences pre-epoch
writers whose replay cleanup ignores shadow retention, including already-open
Lite writers when they refresh before mutation or maintenance. Failure releases replay retention but retains the name reservation and its
compatibility fence. Once the last reservation swaps or is cancelled, its
catalog transaction removes the temporary fence. Cancellation validates both
in-memory and durable generation ownership before retiring anything; a foreign
publication returns `ANTFLY_REPLACEMENT_CONFIGURATION_MISMATCH` without mutation. A separate baseline preserves permanent resolver-feature
requirements: a plain completed replacement returns to version 1, while actual
authoritative resolver features retain version 2, and inline resolver handoffs
or graph producer ownership metadata require version 3. Graph TTL expiry
namespace migration requires permanent version 4; cancelling a shadow cannot
remove that format fence.
Deadline Busy means registration
was accepted and the shadow continues in the background. Without wait, Busy is
limited to bounded registration admission. Drain with `run_until_idle` to await
all replacements. When both names are absent, the command behaves like ordinary
add. Atomic replacement currently supports graph-to-graph upgrades with distinct
names; other index kinds are rejected before inline configuration is changed.

The separate-call recipe is add v2 first, then delete v1. When changing identity
rules, migrate/reset the resolver namespace first and submit v2 with the new
inline rules; existing v1 keeps execution active during this sequence. Delete-first
upgrades are supported too: `list_resolver_identities` includes archived configs
and durable merges, and `set_resolver_namespace` works on retired identities
without reactivating them. Adding v2 then reactivates the migrated identity.
`drop_resolver_identity` explicitly discards an archived identity and its merges;
it rejects active identities. It does not delete existing resolution artifacts,
canonical rows, application references or their redirects. Delete the source
documents to remove their artifacts, and drain index cleanup to remove retired
graph contributions. Explicitly delete canonical rows only after checking other
resolvers and application references; shared destination rows have no single
resolver owner. Index deletion only retires last-owner identities.

The capabilities/status JSON reports the effective `minimum_reader_version`
compatibility guard independently of package release: 1 for the baseline catalog,
2 for extended identity/ownership/cache-policy fields such as `inline_managed`
or `retired`, 3 for inline resolver handoffs or graph producer ownership, and
4 for the separate graph-expiry namespace.
Producer-replay reservations also temporarily require epoch 3, even with explicit
producers and no staged resolver; plain field-backed reservations require epoch 2. The authoritative feature epoch is monotonic;
a pending replacement can temporarily raise its effective guard as described
above. Opening checks the effective epoch before parsing resolver definitions.
Extended catalogs also use a versioned object envelope, which pre-epoch array readers
reject even if they ignore unknown resolver fields. Older unstamped
catalogs infer their epoch on load; the next mutation stamps it. Status polling
uses cached epochs under a read guard. Plain table creation/drop preserves the
original table catalog format and reader epoch 1; actual resolver format changes
stamp the aggregate requirement in separate metadata. Authoritative feature
epochs remain monotonic; removing a temporary replacement guard restores
its authoritative baseline. Open loads the lightweight table catalog,
then opens each table on demand. Named-table mutations aggregate only permanent
format requirements into the root's authoritative baseline, including when the
root itself has a temporary fence. Temporary guards remain scoped to their table
reservations; status combines the effective cached epochs of loaded tables.
Cancelling or completing a plain named-table replacement does not permanently
upgrade the root. Background discovery isolates an unavailable table and reports its error
in `unavailable_tables`; unrelated root/table reads remain available. Portable
exports retain the stamp; imports validate it against supported versions and
catalog requirements before publication. CI checks a real pinned pre-PR reader
against both a plain database and one containing inline ownership fields.

### Cache storage and mutation invariants

Inline resolver configs accept `retain_work: "none" | "embeddings" | "full"`
(default `full`) and `max_work_bytes` (default 1 MiB, maximum 16 MiB). The latter
bounds persisted private work per resolution artifact; oversize work is omitted
entirely without dropping authoritative entity decisions. `embeddings` retains
only per-mention embedding references and dependencies, never candidate records
or comparison features. Embedded mention vectors live once per model/text key
in a shared content-addressed side store. Generated ANN candidate vectors retain
index name, vector id and index revision; application-provided vectors reference
the primary document field instead, preserving their values. Reuse reloads and
validates each reference. Missing ids, moved revisions or missing/corrupt cache
entries trigger candidate fetching or inference, never an identity change based
on an absent vector. Scorer inputs remain lossless f32.

Public artifact lookup and raw resolution lookup strip `_resolution_work`.
Artifact deletion/replay own the reference lifetime. The mention side store is
strictly expendable and excluded from portable backups. Inserts queue in a bounded
16 MiB buffer. Root-source resolutions include the shared-cache writes in their
resolution transaction; named sources flush one optional shared-cache transaction
per resolution window after source commit, since Directory tables have distinct
physical stores. No inference result commits independently per mention. A durable
byte counter and eviction bound the shared store to 256 MiB across all tables.
Periodic expiration visits at most 256 cache entries per page, never document
keys, and expires entries after 30 days. Explicit vacuum can additionally mark
live artifact references and discard unreferenced entries. Losing cache entries
only costs inference.
Portable artifacts may retain cache references; restore safely recomputes misses.

Candidate epochs must advance on every visible destination insert, update,
delete, TTL expiration and index publication. TTL uses ordinary guarded delete
batches and advances the same replay clock. TTL-enabled destinations disable
candidate reuse because logical expiration can precede physical cleanup. Physical
compaction preserves that clock and cannot resurrect a deleted candidate. Directory adapters return no
candidate revision until they can provide a pinned snapshot across tables;
they refetch instead of reusing an unpinned candidate set. Aliases and merges are
applied from current identity state on each resolution and override cached matches.
Retiring a labeled resolver invalidates its source-sharing catch-all siblings and
publishes durable backfill cursors in the retirement transaction. Routing dependencies
use the same artifact-scope overlap rule as admission and query routing: `any`
overlaps both `asset` and `chunk`, while `asset` and `chunk` remain disjoint.
Activation and label changes use this rule too.

### Source-table identity and retries

Resolver names, aliases, merges and namespace fences are scoped by source table.
Two source tables may use the same resolver name and key template. Separate
destinations give independent entities. With a shared destination, identical keys
intentionally identify the same canonical row: promotion preserves its first
canonical name and unions aliases. Native promotions serialize under the database
writer fence; Directory promotions use the transaction coordinator's version
checks. Conflicting writes retry rather than silently overwrite one another.
A merge on source A does not modify source B's resolver mappings, even when both
promote to the same destination; shared destination rows remain shared data.

`ANTFLY_BUSY` means concurrent execution/configuration prevented establishing a
consistent operation. The public schema calls `add_index` (including `replaces`),
`delete_index`, `set_resolver_namespace`, `drop_resolver_identity` and key merging
may also return Busy before mutation when replay/promotion exceeds their bounded
admission wait. Retry the identical schema request. Retry `resolve_query` with
bounded exponential backoff (for example 5 ms doubling to 100 ms, with jitter).
Queries perform no writes and are safe to retry. Identical index config replay,
namespace reset/mapping to the same desired state, and repeated key merges are
idempotent in outcome; revisions may advance on explicit namespace operations.
Inspect identity/catalog state after an uncertain transport failure before retrying
an irreversible drop. Catalog activity admission waits at most 100 one-millisecond
intervals, then returns Busy before mutation; index deletion cannot spin forever
behind extraction or promotion. No internal text-query retries repeat model work
without the caller's latency budget. Native same-path calls retain the documented
path serialization while inference releases the fence with pinned lifetimes.


### Unavailable embedded namespaces

Database-wide drain discovers available tables and reports failed opens in
`pending_work_stats_json.unavailable_tables`, in both Lite and Directory.
Healthy-table work and SQL reads continue; an operation that actually references
an unavailable table still fails. Coordinated SQL constraints require a complete
catalog and fail closed when dependency metadata is unavailable.

Full portable backup retains its completeness guarantee and fails if any table
cannot be opened. Explicit `backup_json({"tables":["healthy"]})` (Python
`backup(tables=["healthy"])`) includes default plus exactly those named tables,
without opening unrelated namespaces. Restore reconstructs only that explicit
scope. Include dependency tables when exporting schemas with cross-table
constraints or resolver references. Go and Rust expose `BackupJSON`/`backup_json`;
all bindings expose replacement cancellation.

## Inline resolvers during graph replacement

Embedded `add_index` with `replaces` can transfer an inline resolver to a new
graph version without changing its public resolver name. A changed resolver must
use fresh extraction **and** resolution artifact names. An overlapping extraction
subscription cannot already belong to a serving resolver or another pending
replacement. Siblings within one replacement may share their private source;
asset and chunk subscriptions remain disjoint unless either uses `any`. Ordinary
resolver admission and reactivation cannot join a reserved private source until
that replacement finishes or is cancelled. Publication revalidates this rule for
reservations from older writers. Admission stages a
separate execution contract alongside the serving resolver, records it with the
durable shadow reservation, and resets the existing bounded resolution replay
cursors. Inline extraction producers are admitted in that transaction too;
existing documents are replayed in resumable pages before graph construction,
without a corpus scan at admission or activation. This replay covers every
artifact source required by the new graph and its upstream producers, including
graphs with absent or unchanged resolvers and staged resolver sources that are
not direct graph sources. A missing enrichment runtime is
created on demand, including after reopen; manual owners retain synchronous
execution without starting background workers. The old graph and public resolver
remain authoritative during the build. Label exclusions, query routing, and
sibling canonical-key dependencies use separate serving and per-replacement
views, including for legacy reservations with shared input streams. Graph replay,
relation endpoint maps, foreground projection and mention-state protection use
only public resolvers plus the target graph's own private contracts. Serving
graphs cannot consume staged decisions even when they share its extraction
producer without declaring a resolver, or are admitted after staging. Every
successor advances its revision and schedules bounded replay at cutover,
including resolve-only contracts, to release decisions to those serving graphs.
`run_until_idle` drains replay released by its final swap before returning,
including on manual owners without background workers.
Cancellation of fresh reservations
never changes serving sibling revisions. Cancelling or failing a legacy shared-source
reservation invalidates any potentially contaminated serving decisions durably. Cutover durably invalidates source-sharing
siblings affected by retiring the predecessor, including canonical-key dependencies and resets their replay cursors in
the catalog transaction. Explicit waits drive manual enrichment
with the remaining caller deadline, without enabling background workers or
resuming deliberately paused background owners. A named wait advances its own
reservation and rechecks its generation under scheduler ownership, so an earlier
corpus build or a cancelled-and-reused name cannot redirect its work.
Resolution yields between source artifacts, after at most 32 keys per quantum,
and checks the same monotonic deadline before each unit. Each source keeps its
sibling resolver routing intact, including ordered receipt/read-set validation. An in-flight source/provider operation can overrun the deadline; a large
journal batch cannot extend that into unbounded additional source operations.
A durable intra-record cursor advances only after source output publication, and
the resolution watermark stays pinned until the entire record completes. Restart
and ordinary workers resume this cursor. The resident runtime retains one decoded
record across quanta to avoid repeatedly parsing a large batch.
The same deadline bounds indexed backfill discovery and legacy source-index
repair. These scans count physical rows, including stale markers, and retain
cursors behind unfinished sources. Replay is durable before discovery markers
are repaired; a failed append cannot hide source work from a retry.
Write-fenced and ordered owners leave prerequisites queued; waiting can still
observe completion or reach its deadline without choosing local mutations.
Producer replay yields after at most 4096 physical records, a 4 MiB byte budget,
or a 100 ms quantum (and honors an earlier caller deadline). Byte and time
budgets are checked between records, so a single oversized record still makes
progress. Its durable cursor includes artifact-only pages, rather than counting
only primary documents toward the work budget. The scan ends at the document
namespace boundary; replay journals and identity state do not extend it.
The successor takes the public name in the same transaction that activates the
shadow graph; cancellation or terminal build failure retires only the staged
execution contract. Staged resolver decisions cannot promote entity documents.
Activation advances the successor's resolution revision and saves a bounded
backfill cursor in the same transaction. That backfill releases promotions after
cutover, including after a restart and when retained resolver work is disabled;
serving resolver promotions continue during construction.

Graph shorthand records producer ownership in the enrichment catalog. Removal,
cancellation and cutover atomically remove owned producers with no surviving
index, resolver, pending shadow or downstream producer references. Quarantined
indexes retain their ownership and dependencies even without loaded runtimes;
malformed quarantined definitions retain producers conservatively. Reachability
walks each upstream dependency once. Explicit
producer upserts adopt ownership, keeping those producers available independently.
Older producer catalogs without ownership provenance are conservatively retained.
Pending replacement and resolution work resume after reopen.

The successor inherits durable merges and, when omitted in its declaration,
inline aliases. Resolver mutations return `replacement_pending` during the build;
finish or cancel the replacement before making further identity changes. A
resolver independently created through the resolver API, or declared by another
configured graph, including a quarantined one, cannot be taken over. An unchanged resolver contract can be reused
without creating a new execution stream. Pending graph reservations participate
in ownership checks too. A graph cannot acquire a resolver while its handoff is
pending, and publication revalidates exclusive ownership before retiring the
serving execution contract.

Changing a key template is supported when no aliases, merges, or explicit
namespace fence exist. Curated namespaces still require an explicit
`set_resolver_namespace` migration with an endpoint mapping (or an explicit reset)
before requesting a replacement with a different table or template. Replacement
cannot infer an arbitrary old-key to new-key mapping and does not discard curated
redirects to make that change succeed.

Internal predecessor contracts remain retired so replay can recognize old
resolution artifacts without executing the old resolver. They do not appear in
public resolver or identity listings. Databases containing staged contracts
require resolver reader version 3; older readers reject them before parsing.


## Resolver curation resources

Resolver curation is table-owned durable policy, independent of the graph indexes
that declare execution. Unmerge removes only the selected *direct* redirect:
`A → B → C`, followed by unmerge of `B`, leaves `A → B`. Unmerge of a missing
redirect is a no-op. Re-point uses `replace:true`; it preserves the former
survivor rather than turning that survivor into another redirect. Optional
`pin_distinct:true` on unmerge or re-point forbids rejoining the separated
components through another alias, candidate, or model adjudication. Redirect
admission validates the entire graph before publication.

An explicit unmerge also records restoration of the original rendered identity.
Replay bypasses fuzzy matching and an old promotion tombstone for that identity,
restores its canonical document, and replaces the source's mention/relation
contributions. Other graph producers and other source tables keep their
contributions. A subsequent explicit merge can redirect the restored identity.

Declarative resolver configuration accepts:

```json
"suppress": {
  "keys": ["concept/note", "concept/content"],
  "texts": ["Note", "Content"],
  "min_text_chars": 3
}
```

Runtime additions/removals live separately from the declarative configuration.
Text comparison uses the resolver's Unicode slug normalization; minimum length
counts Unicode scalars after trimming ASCII whitespace. Text/length checks run
before key rendering, embedding, candidate lookup, and adjudication. Rendered-key
suppression runs before candidate lookup or minting. The resolution retains an
explainable `decision:"suppressed"` and `suppression_reason`, but creates no
canonical edge or entity. Suppressed existing mentions remove their former
resolver-owned edge contributions on replay. Existing promoted originals receive
a scoped `resolver_suppressed_by` marker; suppression never creates a document
solely to hold that marker. Unsuppress removes runtime rules and replays under
the remaining declarative policy. Suppressed keys cannot be redirect targets.

Promotion records alias contributions by source resolver, rendered identity and
spelling. Re-point/unmerge/suppression detach that contribution using conditional
writes in the same atomic entity batch. Concurrent contributions survive; older
curation intents cannot overwrite a newer correction. Aliases that predate
ownership tracking remain conservative: un-attributable legacy aliases are
preserved, while aliases introduced by a tracked correction can be removed.
Destination reads are deduplicated and run with at most 16 concurrent lookups.
They fence both content and absence; conflicts retry ordinary replay rather than
advancing the source receipt with a partial promotion.

The HTTP surface uses the following table resources. Every route also exists at
`/databases/{database}/namespaces/{namespace}/tables/{table}/...`.

| Resource under `/tables/{table}` | Operations |
| --- | --- |
| `/resolvers` | GET active and retired identities |
| `/resolvers/{resolver}` | GET configuration, curation revision and replay progress; DELETE a retired identity |
| `/resolvers/{resolver}:render` | POST a mention to preview its minted/canonical key and suppression |
| `/resolvers/{resolver}:resolve` | POST text or extraction for ephemeral resolution through the serving policy |
| `/resolvers/{resolver}:namespace` | POST an explicit namespace mapping/reset |
| `/resolvers/{resolver}/redirects` | GET direct redirects and distinct pins; POST `{from,to,replace?,pin_distinct?}` |
| `/resolvers/{resolver}/redirects/{key}` | DELETE a direct redirect; optional `?pin_distinct=true` |
| `/resolvers/{resolver}/suppressions` | GET configured/runtime rules; POST `{keys,texts}` or atomic `{add,remove}` selections |
| `/resolvers/{resolver}/suppressions/{key}` | DELETE runtime key suppression |
| `/resolvers/{resolver}/reviews` | GET a bounded page; `cursor` and `limit` (1–256) |
| `/resolvers/{resolver}/reviews/{id}` | POST `{decision:"match"|"new"|"suppressed",key?,table?}` |
| `/entities/{key}/redirects` | GET resolver state targeting an entity table/key |
| `/entities/{key}:merge` | POST `{into,replace?,pin_distinct?}` across matching resolver namespaces |
| `/entities/{key}:unmerge`, `:suppress`, `:unsuppress` | POST corrections across those namespaces |

Keys and review IDs are single URL-encoded path components. Review IDs bind the
source key, mention ID and exact resolution artifact hash; a stale decision
returns Conflict. Review operations use the owner's raw-artifact policy gate:
tables with active row security fail closed until review paging supports bound
row-policy evaluation. Review pages use keyset cursors, visit at most 256 storage keys
per owner request, and never load the entire corpus. Empty pages can carry a
continuation cursor. Decisions atomically append the override and ordinary
source replay record. A curator's suppressed/rejected decision persists through
replay and has `suppression_reason:"curator"`. A per-mention rejection removes
that mention's graph contribution and preserves existing canonical aliases,
which can still be used by other mentions; global key/text suppression owns
destination alias cleanup and scoped retirement.

Per-resolver policy mutations publish through the source table's authoritative
metadata CAS until namespace adoption. Entity corrections publish through one
destination-owned native transaction; source projections enqueue ordinary resolver backfill.
Entity writes require destination write permission, and entity-state GET requires
destination read permission. Background propagation uses internal metadata
authority and does not delegate source-table authorization to the caller.
Native Lite uses the same namespace atomicity.
Entity inspection returns redirects, key suppressions, distinct pins and
unmerged keys for the requested redirect component; text suppressions and
unrelated keys remain available only through the source resolver resources.
HTTP and embedded entity inspection share this projection.
Serving `:resolve` preflights read permission on its destination and executed
sibling destinations using the admitted credential, including API-key scopes.
Redirect survivor tables bind through the same authorization boundary. Candidate
lookups, prefix scans, ANN searches and description assets apply the destination
row predicate before projecting results; projected assets never authorize a read
using a separate primary-document snapshot. Background replay continues to use
its own stored destination grants.

Review receipts retain the curator's selected identity. Replay follows its
current redirect chain when emitting graph and promotion endpoints, so a later
merge moves reviewed links and unmerge restores them. Resolver redirects never
rewrite an override in a different entity table. Go revision-fence request fields
preserve presence explicitly: `nil` omits the guard; a pointer to zero fences the
first edit just as `expected_revision:0` does on the wire.
Retired identities accept and retain policy without running resolution or
promotion. Remote replay progress remains pending until an index declares
execution again; waiting never reactivates a retired identity.
Durable deletion markers prevent offline owners from resurrecting dropped
identities; native backup carries both policy and consumed deletion fences.

A mutation returns the accepted revision and replay sequence immediately (HTTP
202 while pending). `replay_sequence_kind` distinguishes a resolver revision
from a source-journal review decision. `wait:{timeout_ms:...}` bounds observation
of detached replay; a timeout leaves the accepted mutation intact. The remaining
observation budget bounds metadata reads and remote owner RPC timeouts. Before
admission, cancellation and deadlines reject the request normally. After acceptance,
an unavailable progress read (including deletion, cancellation or deadline expiry)
returns HTTP 202 with the accepted commit receipt, `applied:false`, and
`progress.observation_error`. Observation failures never retry the mutation.
Resolver GET
reports per-group revision, freshness, durable backfill and resolution/promotion
watermarks. An idle watermark alone does not prove the new revision is applied.
If cached/heartbeat status omits resolver diagnostics, lags an accepted revision,
or has not reached an accepted review journal sequence,
status obtains compact observations from the current mutation owners, with at
most 16 concurrent shard requests. Probe selection and completion share the
same revision and resolution/promotion sequence requirements, so an idle
heartbeat below the accepted sequence cannot suppress a current-owner probe.
Polling never drains replay on a request
worker. A shard with no runtime completes only with a durable proof of no
primary records and no derived replay sequence; absent observations alone never
prove completion. Native `pending_work_stats` includes these per-resolver revisions under
`resolver_curation`.
Generation-aware input reads dispatch by physical artifact family. Extraction
heads govern asset roots and units; chunk heads govern chunks. Resolution
outputs retain physical reads and exact absence/value preconditions, including
when ordered replay recomputes an existing result after a curator decision.
Ordered publication notifies detached resolution and promotion workers after
its journal and receipt commit, so the final publication progresses without a
later primary write. Catalog effect capabilities come from the captured
producer/resolver definitions (15 for receipts, 16 for grouped ownership),
retaining any already-installed extension. They are independent of the durable
reader floor: reviews require curation reader 6 and preserve higher floors,
including when an accepted decision is retried at a new Raft position.
Busy is HTTP 429 with `Retry-After`; capacity and metadata unavailability are
explicit retry states. Curation needs table-write authorization; namespace
migration additionally requires administrative route admission and authority to
write the destination before grants are resealed.

Namespace migration remaps all key-valued negative policy too. Incomplete maps
fail before publication. `reset_merges:true` requires `reset_curation:true` when
key-valued curation exists; text suppression and minimum length remain meaningful
and are preserved. Runtime curation, including namespace migration of negative policy, requires
minimum reader epoch 6. Ordinary configurations without these features retain their
existing reader requirement.

C exposes `antfly_db_unmerge_resolver_key_json`,
`antfly_db_suppress_resolver_keys_json` (atomic add/remove selections),
`antfly_db_unsuppress_resolver_key_json`, `antfly_db_resolver_curation_json`,
`antfly_db_resolver_reviews_json`, `antfly_db_curate_entity_json` and the named
`antfly_db_entity_{merge,unmerge,suppress,unsuppress,redirects}_json` calls alongside the
existing render/resolve/list/drop/namespace family. Native entity requests carry
`action:"redirects"|"merge"|"unmerge"|"suppress"|"unsuppress"` and optional
`into`; review requests carry `operation:"reviews"|"review"` and `resolver`.
Go, Rust, Python and Node embedded bindings expose these JSON operations. Remote
Go, TypeScript, Python and Rust clients share the generated public schemas and
namespaced operations; successful accepted replay is returned without automatic
mutation retries.


The server stores authoritative entity corrections on the destination table,
keyed by the exact key template. A correction commits once across the matching
namespaces (`atomicity: "namespace"`); the response lists asynchronous source replay
progress. Adding a source resolver reads the current namespace in its metadata
admission transaction, so its first document inherits existing corrections.

A durable namespace-to-consumer index is maintained by table admission,
replacement and removal. Metadata lifecycle rounds advance at most 16 dependent
records per Raft transaction, atomically publishing source projections and the
resume cursor. A bounded bootstrap rebuilds the index for legacy tables. Both
the dependency index and pending work survive metadata snapshots and restarts.
Destination deletion retires obsolete dependency rows in the same bounded
metadata lifecycle. Adoption checks its catalog cut so concurrent source edits
cannot disappear when legacy corrections move to the namespace.
Resolution artifacts record `namespace_curation_generation`; stale cached work
cannot cross that generation. Identical legacy runtime corrections are folded
when the namespace is adopted, and conflicting redirect targets reject adoption
with `ResolverNamespaceMigrationConflict`. Source projections are derived views;
per-resolver runtime edits return `ResolverNamespaceCurationRequired` after
adoption. Inline aliases remain source configuration, applied before shared
namespace corrections. Lite uses the same destination-owned native record contract.

Namespace projection is read-only; reverse dependency edges change only after
table admission succeeds, in the same metadata transaction. Updates apply the edge
set difference, so policy-only replay pages do not rewrite unchanged edges.
Resolver resources read only their source definition and ranges. Source policy
mutations additionally resolve and inspect their destination against the source
metadata cut; logical catalog revisions and physical metadata cuts remain separate
clocks. Entity requests read one destination and seek its declared-target consumer
index; progress polling reads only the source table and its ranges. Consumer pages visit
at most 16 edges and stop after a 1 MiB source-record budget (one record may be up
to 8 MiB). Cursors bind the destination identity, entity key, and catalog revision.
Declared-target edges retain qualified logical names independently of physical
owner IDs, so destination recreation does not hide existing consumers. Legacy
catalog discovery uses bounded table pages until the v2 bootstrap is durable.
Each page owns a scratch arena that is released before the next read. Discovery
retains only compact consumer identities and the folded namespace policy; GET
does not retain observation identities. Legacy folds replace and free the prior
aggregate. Mutations validate full consumer policies in a second pass over the
same revision-fenced cut before CAS, so memory does not accumulate source schemas
or unrelated index declarations across pages.
Initial ownership is published even when a requested correction matches folded
legacy policy. An entity merge matching an inline alias materializes a shared
redirect for consumers that lack that local alias. Subsequent identical requests
are idempotent once the authority and correction are durable.
Every changed namespace fences its validated consumer cut, including local alias
edits. Source policy edits racing ownership publication are rejected at apply
with an exact no-commit receipt, instead of being overwritten by projection;
ordinary definition edits and unchanged legacy policy still inherit authority. Incompatible alias cycles, distinct pins, and suppressed redirect targets
are rejected before proposal and rechecked before any apply writes. A rejected
namespace CAS retains one digest and exact Raft term/index per destination, allowing a certified
retry without treating a committed outcome as retryable; acceptance or deletion
clears it. Semantic rejection cannot stall metadata apply. Standalone and Raft preserve
the same typed admission conflict through transport and return HTTP 409. A
forwarded conflict is definitive only with an explicit non-admission outcome.

Destination namespace records are private metadata and are excluded from index
reconciliation and public table/index resources. Owner route discovery shares
the caller's absolute deadline and cancellation token, including peer-cache
refresh and periodic checkpoints through metadata routing scans.

Curator REVIEW decisions use a private ordered data-Raft command. The artifact,
configuration, policy bundle and prior override are checked at apply; override,
outcome receipt, source replay and ordered marker commit together. Retrying an
older decision cannot replace a newer override. Public batch JSON cannot submit
this control. Native/Lite decisions use the same atomic batch and full HA
mutation stream; HA payload versions 22/23 reject older decoders before apply.
Metadata protocol 35, data batch protocol 24 and native reader 9
fence the new state during upgrades.

Native resolver owners retain the full candidate interface through a borrowed,
layout-checked same-toolchain view (kernel owner ABI 81). Batch catalog bindings,
bulk reads, generated adjudication, model revisions, descriptions, and caches
survive the boundary. Batch leases and callback error identities retain their
originating owner. Resolver review entry points dispatch through the storage
engine rather than reading the control proxy's store.

### Curation attribution and entity proposals

Entity suppression accepts `absorbed: reject | release | suppress` (default
`reject`). Release removes every transitive redirect into the selected target;
`pin_distinct: true` also pins every released source against that target.
Cascade suppression removes the same redirects and suppresses their source keys.
The planner computes the component before changing any edge, then publishes one
policy edit. Removed configured aliases stay masked. Unsuppression never restores
a redirect or unsuppresses other keys implicitly.

Curation writes accept UTF-8 `actor` (256 bytes) and `reason` (4096 bytes). The
admission owner stamps origin, creation time and the committed policy revision; clients
cannot supply a server origin through entity/redirect writes. Read projections attach
provenance to redirects and distinct pairs. Runtime suppressions retain the compatible
`keys`/`texts` sets and add attributed `entries`/`text_entries`. Cascades name their
original target in `cascade_from`; review acceptance names its proposal in
`proposal_id`. Attribution remains sparse when no origin was recorded; reading a policy
never synthesizes per-key origins. Records may have zero creation time/revision;
table-level proposals can also have revision zero until an individual policy edit is
recorded. Mention-review provenance requires reader version 10; scoped legacy
curation state and queue receipts require reader version 11. Original native
resolver records remain readable at Lite reader version 12. Physical projection
bindings, shared catalogs and fingerprinted heads require version 13; native
metadata commands and shared execution attachments require coordinated decoder
version 42. Version 41 introduced shared attachments; version 42 adds selective
execution cuts and indexed namespace routes.
Coalesced native Lite projection jobs require reader version 14.
Legacy blob policy publication retains its version 37 decoder fence.

`/tables/{entity_table}/curation/proposals` is the entity-level review surface;
it does not replace mention-level resolver reviews. Create a typed entity action
(`merge`, `unmerge`, `suppress`, `unsuppress`, or `distinct`), inspect/list it, then
accept or reject it. Both default and database/namespace-qualified routes are
available. Accept uses the entity planner and returns its curation result. A
merge rejection can atomically pin the proposed pair distinct. Accepted effects
carry `origin: review`. Direct entity corrections supersede affected open
proposals. Source-only edits are rejected with
`ResolverNamespaceCurationRequired` once the destination owns a namespace or
its proposals. Clients use entity resources so policy and queue changes commit
in one native owner transaction. The same
ownership check runs inside the primary metadata transaction, so proposal
creation racing a source write cannot bypass it. Ownership checks index only
namespace templates and proposal keys, with one index per destination table.
Terminal proposals retain their identity: repeated screening does not
reopen a rejection, and repeated acceptance does not override a later correction.

The stable ID hashes the action and length-framed JSON keys, excluding confidence,
evidence and options. Distinct-pair IDs are symmetric. Listing uses ordered IDs for
keyset pagination, with a default open-state filter and pages of 1–256 records. Evidence
is bounded at 16 KiB, 4096 JSON values and depth 32; confidence must be finite and
between zero and one. Each table queue has a 4096-record, 4 MiB budget. Admission
reserves the maximum escaped decision attribution for every open proposal, so rejecting
or superseding admitted work never runs out of queue space. Native queue heads track
reserved bytes with per-record deltas. Legacy HTTP publication additionally reserves
this growth in both definitions of its bounded metadata CAS. Size checks use a counting
writer with fixed scratch space. Legacy interrupted Lite decisions retain their separately
bounded frozen-attribution envelope until migration. Budget exhaustion is explicit; no
existing decision is silently evicted. Queue records are metadata, never resolver
declarations, and inference admission parses only execution policy, skipping proposal
evidence and attribution history. Index configuration walkers share an opaque boundary
for `_curation_proposals`, `_resolver_curation`, and `_resolver_namespaces`: enrichment
and resolver discovery skip these subtrees without allocating producer state, and
authorization sealing and namespace edits preserve their payloads verbatim as JSON
values. Configuration-shaped evidence cannot install producers, require destination
credentials, or be rewritten as a live resolver declaration.

HTTP and Lite store policy and proposals in destination-owned native records.
Namespace heads fence component deltas; a separate compact queue head fences
proposal deltas. Redirect/incoming, suppression/cascade, distinct-endpoint and
proposal state/action/entity indexes are updated atomically with these heads.
Queue-only decisions leave policy revisions unchanged. Terminal decisions stay
idempotent across reopen and later corrections. Lite queues retain their random
receipt identity across reopen and receive a fresh identity on destination
recreation. Native publication and its minimum reader version commit together.

Existing legacy corrections are folded once at namespace adoption. A legacy Lite
pending fanout must finish through its frozen original decision before adoption;
new decisions never create that envelope. Worker resolver configurations are derived
projections. Remote replay coalesces updates in its bounded durable namespace job;
Lite pulls one current source projection per detached maintenance turn. Publication
rechecks the destination identity and head after building a projection outside the
writer lease. Both adapters close components over alternating static aliases and
native forward/reverse edges before planning or validating; the merge destination
and every reached alias endpoint participate in the same traversal. Static aliases
remain local configuration, rather than becoming shared corrections.

Metadata source records carry compact physical destination/template/revision
bindings. Raft replay reads heads and writes bindings, never full policy payloads.
Read-only execution/provisioning cuts expand bindings from one immutable namespace
snapshot per destination/template in that cut. A bounded immutable payload cache
is keyed by the transaction's physical namespace revision and state fingerprint;
warm reads do not scan policy records again. Adoption and every delta derive a
new fingerprint, including divergent equal-revision histories after restore.
Revision-zero native adoptions remain valid and cannot be mistaken for an
unprojected source. Raw source records remain unchanged
for DDL CAS; portable export materializes the policy and removes physical bindings.
Lite pulls a namespace once per turn and publishes all of its serving/retired
consumers in one catalog transaction. Reader-13 catalogs store shared projection
payloads once in a content-addressed dictionary, with per-resolver references.
The serving catalog and reopened catalogs retain immutable projection owners:
policy allocation and digest computation happen once per shared projection,
while consumer slots hold lightweight references. Detached config clones retain
ordinary allocator ownership, including arena readers. Runtime ownership never
crosses JSON boundaries. Legacy merge sidecars retain only source-owned policy;
private compact consumer wires explicitly retain those unadopted merges until
the namespace's first native adoption.

The source execution clock (`resolution_generation`) advances independently of
the destination curation revision. Projection/readiness stamps include the physical
destination ID and explicit missing-authority state, so moving to a namespace
with a lower revision or recreating a destination cannot regress execution or
falsely report an old projection applied. A dropped or unadopted replacement
retracts derived corrections even while the retired authority's rows/cache remain;
publication fences both authority presence and absence.
Replay retains its cursor when writes arrive and marks the pass dirty. It finishes
the tail before another pass and rotates among namespace jobs; continuous writes
cannot repeatedly restart the first page.

Native Lite maintains a durable reverse consumer index in the same physical
transaction as resolver catalog edits. Warm mutations seek the destination and
open only matching sources. Legacy index bootstrap reads catalog bytes without
opening unrelated indexes/providers. Directory changes enroll/retire their own bindings atomically;
source IDs fence dropped/recreated tables. Queue-only rejections and their receipt
replays use point admission and no consumer discovery. Their effect classification
is a durable native receipt witness, independent of caller-supplied retry flags or
actor revision; rejected distinct pins still observe their policy consumers.
Unclassified legacy receipts conservatively observe the scoped consumers.
Entity inspection of an unadopted native namespace uses the same reverse lookup
without opening unrelated tables. C ABI waits capture immutable source and
namespace requirements before releasing admission, then batch lightweight
identity and replay observations per source. Physical source/destination IDs,
namespace template/generation, missing authority and accepted revision fences
all participate; a moved or recreated namespace cannot satisfy an old wait.
Manual maintenance uses the same pull operation. `complete` describes
atomic authority admission; `applied` observes asynchronous consumer replay.

HTTP initial decisions and terminal retries return the same decision envelope,
including the proposal, `changed`, `complete`, `applied`, and per-consumer results.
HTTP terminal retries observe current authoritative namespace revisions using
paged consumer discovery without replanning or republishing policy. A pending
replay returns HTTP 202 and honors the retry's `wait` budget; a plain rejection
has no replay and returns HTTP 200 with empty results. Lite uses the ordinary
post-commit replay observer after releasing writer admission. Ordered/HA
maintenance is rejected by the raw embedded queue path rather than bypassing
ordered mutation admission.

The C ABI and Rust embedded crate expose `propose_curation_json`,
`list_curation_proposals_json`, `get_curation_proposal_json`, and
`decide_curation_proposal_json`. Lite decisions include `accept: true | false`;
HTTP uses the `:accept` and `:reject` resources. Public HTTP SDKs are generated from
`specs/openapi/antfly/resolvers.yaml` and `metadata.yaml`.

The HTTP convenience clients expose Go `ProposeCuration` / scoped variants,
TypeScript `curationProposals` / `namespaceCurationProposals`, and Python
`curation_proposals` with `in_namespace`. Entity suppression uses the typed
`EntitySuppressRequest` in each SDK, including its absorbed-key disposition.


### Bounded resolver candidate work and policy reuse

Within one resolution work unit, missing mention texts are deduplicated and sent
through the configured Antfly embedding provider in batches of at most 64.
Embedding identity, role, dimensions and existing source/publication fences stay
unchanged. Providers without a batch callback retain their scalar adapter.

ANN discovery deduplicates identical vectors and uses the shared native
`nearest_many` callback in batches of at most 64 queries. Routed adapters execute
at most eight neighbor queries concurrently; embedded adapters use their native
owner lease. This is bounded fanout over Antfly's existing indexes. It does not
introduce another vector store or pretend that one RPC replaces all neighbor
queries. Distinct redirect targets are bulk-read once per table and work unit,
including missing targets. Repeated mentions share immutable decoded candidates.
ANN requests hydrate scorer inputs and canonical/type/redirect metadata instead
of full documents. Routed hit decoding borrows the `_source` JSON slice rather
than building and serializing a source DOM.

Resolver execution caches immutable compiled suppression, distinct-pair and
redirect indexes by the exact alias and policy content. The process cache holds
at most 32 MiB with byte-budgeted LRU retention and coalesced cold builds.
There is no fixed profile-count limit. Each executing resolver holds a reference-counted
lease, so eviction cannot free an active reader's policy. Content changes,
including removal of a correction, select a different compiled policy. Grants,
authorized documents, and decision results are not cached.

Native query execution projects the configured forward paths together with native
redirects, tombstones, negatives and incident distinct endpoints at one fenced
owner read cut. Ready persistent nodes provide point/seek access even when the
profile exceeds the process cache budget. Attachment compiles only the returned
paths; it never builds a whole configured alias index. Configured key/text
negatives use a separate immutable cache identity so a previous full-policy
entry cannot collide with the projection. Live negatives, pins and removals
remain request-local. The 16,384-node bound applies per traversal, not to the
union of pages: 9,000 independent aliases remain a valid multi-page query.
The two resolution phases replace their leases explicitly and restore caller
state on completion. Legacy source-owned policies keep their full cached
semantics until native enrollment; augmenting callbacks keep their baseline.

Topology protocol 55 adds the private opt-in `complete_aliases` policy projection.
Its namespace alias slice contains effective configured and native query paths,
not new durable corrections. Inspection reads retain their previous wire shape.
Page union rejects a mixture of complete and incomplete projections, as well as
mixed heads. HTTP and Lite consume the same complete projection; older replies
retain the configured-policy fallback. Existing static-node readiness and
backfill fences remain required before using persistent lookups.

Lite initial query and key-render admission lease immutable declarations from a
catalog-owned name and source/routing directory. Bodies are copied once per
admitted declaration and generation. Declaration fingerprints and profile IDs
are computed once outside the catalog lock with the shared SHA-256. Warm
admissions copy only small descriptors and retain their owners; catalog
retirement releases its references without invalidating in-flight readers.
Catalog epochs, native heads and final declaration proofs still revalidate
independently. Private replacements retain their own routing domains.
A one-resolver warm admission uses 2,296 bytes of request arena capacity at
1,000, 10,000 and 100,000 selected aliases, rather than copying up to 12 MB of
selected aliases and recomputing their identities on every request.

HTTP query admission uses topology protocol 57's opt-in compact execution rows.
The owner writes each stripped execution descriptor and static profile identity
atomically with its source declaration. Initial reads return those rows only
after v12 bootstrap readiness and same-cut native-head, profile-enrollment,
persistent-node and configured-negative checks. Alias bodies, configured
suppression lists and source-projected curation stay out of
the warm response; scorer, embedding, routing and other execution options remain.
The API consumes the supplied identities without rehashing the source aliases.
A null profile list denotes a legacy response; an empty qualified list explicitly
selects inline fallback and must not synthesize a potentially inactive reference.
Missing heads or node readiness hydrate only the selected legacy descriptor.
Older source rows remain readable until the leader's bounded walk publishes
version-three sources and version-two execution rows. Old in-flight bootstrap cursors rewind for the new
generation in both work previews and committed advancement. The regression with
10,000 aliases and 100,000 suppression rules returns a 1,902-byte response with
53,072 bytes of request arena capacity and zero hydrated aliases or suppression
lists; compact and full reads have identical query
fingerprints, and policy replacement invalidates the captured proof.

Lite schema capture retains one content digest on its physical DB owner, keyed
by the process-local instance, core, document namespace, catalog epoch, schema
generation and schema version. Cold captures coalesce and publish only after
rechecking the epoch. Warm requests neither copy nor serialize unrelated index
or enrichment bodies. Source and destination captures share the same proof when
they address the same DB. A changed catalog or incarnation requalifies content;
unchanged content may update its stamp, while changed content fences old queries.
Native heads, live bindings, authorization and declaration checks are independent
of this content-only cache. The large-catalog regression captures 1,000 unrelated
20 KB templates once and reuses the owner proof for 1,000 subsequent captures
with no scratch allocation. Nothing is retained across a new physical owner.

Lite key rendering uses the same policy preparation and final fences as query
resolution, rather than rebuilding the entire configured redirect index for
one key. It observes sibling label declarations without executing sibling
resolvers or opening their destinations. Declaration-only rendering without a
destination and legacy source-owned policy retain their prior semantics.
The oversized-profile regression performs ten phases with zero full-profile
loads and twenty point expansions. Only configured negatives enter the compiled
policy cache; native projection attachment retains zero configured aliases.

Lite final declaration verification reads a scalar scoped fingerprint from the
owner's namespace lookup. The scope includes the selected resolver and overlapping
routing siblings. Catalog generation changes invalidate these proofs; cold
construction hashes only matching declarations and retains no copied alias or
producer bodies. Structural publication discards the lookup. Retired/stale scopes
cannot grow a history of retained proofs. Warm verification takes a shared lock
only for proof lookup; legacy policy loads only the explicitly tracked legacy
names. Source/schema/incarnation and native-head checks remain independent.
A fixture with 1,000 declarations and 999,000 unrelated aliases performs 1,000
warm proof checks with one proof build and zero descriptor copies, replacing
59,848,688 bytes of transient full-descriptor copies per final fence. Selected
and routing-sibling edits change the proof; unrelated edits preserve its value.

Entity and terminal proposal progress observations group consumers by source
table. Every polling round shares one source catalog cut, one local status sample,
and at most one owner probe per group across its resolvers. Each resolver still
checks its own namespace revision, configuration generation, journal floor and
promotion requirements. An overall wait budget covers all source tables.

Native curation no longer serializes a destination's entire `indexes_json` for
ordinary corrections. Protocol 39 publishes typed component record deltas and
compact version heads. Table metadata retains immutable discovery references;
current revisions live in native rows. Schema and consumer catalog cuts remain
final publication fences. Generic table mutations cannot replace these private
references or bypass source ownership. Native metadata snapshots include all
records, indexes, receipts and pending replay; destination retirement removes them
in bounded pages.

Point entity reads seek the redirect component and incident negative policy.
Proposal lookup uses its stable ID; listing seeks state/action indexes with keyset
pagination. Consumer admission reads compact static descriptors rather than every
source's projected policy. Full namespace exports occur only for derived worker
configuration. The shared record-store regression imports 10,000 unrelated aliases
and bounds point-component reads and single-component writes independently of that
namespace size. Namespace graph size and aggregate policy/queue quotas remain
explicit bounds; no record or decision is silently discarded.

Portable system-catalog exports materialize current native policy and proposals
into self-contained table authority. They omit instance-local adoption cuts and
native discovery markers. Live catalog projections keep compact native references.
Lite portable backups retain typed native authority rows and require reader version
12 plus the native marker; native file backups retain their original indexes and
heads. Durable authority is never reconstructed from a possibly lagging worker.

### Scoped admission and projection work

HTTP and embedded entity mutations build the same effective graph from all
consumer static profiles, native overrides and tombstones. Planning never
selects a consumer as the namespace authority. Static-profile compatibility is
validated once per distinct profile from the admitted catalog cut; HTTP retains
those profiles rather than fetching consumers again. Explicitly asserting an
identical source-local alias publishes a shared redirect with its own origin.

Private execution/provisioning catalogs carry one content-addressed namespace
attachment and compact references from resolver identities. Hydration and
validation are batched; repeated references borrow the same parsed policy and
encoded state. Raw source records remain compact for CAS. Portable exports
expand attachments and detach physical bindings in one pass. Persistent owners
copy each policy out of the metadata arena once, then retain shared immutable
ownership across consumers. Runtime validation evidence binds the policy digest
and static profile, is never serialized, and is invalidated by caller edits;
unchanged shared policies compare by digest.

Native Lite maintains coalesced per-owner namespace jobs in the same physical
transaction as authority-head changes, binding changes and destination directory
changes. A durable cursor rotates work, so a hot namespace cannot pin the tail.
Projection publication acknowledges the selected job and advances its cursor
atomically, under the same namespace dependency fence. Reader 14 introduced
pending jobs and cursor records; reader 15 fences the durable namespace routes
and projection delta records, including portable restore. Work discovery
seeks the pending-job prefix instead of opening or polling every destination.

The native transaction overlay keeps an ordered staged index. Prefix reads merge
bounded base and staged ranges, including replacements and tombstones; unrelated
staged writes do not participate in adjacency scans during adoption or curation.


### Incremental projection publication and scoped reads

Native projection replay writes one namespace snapshot and its execution clocks
under a private projection prefix. Static resolver catalogs are unchanged by
replay. Empty work acknowledgements write only their job/cursor records. A source
owner loads the static catalog and namespace snapshots at one read cut; structural
edits fold serving snapshots into the catalog and retire their delta records in
the same transaction. Publication epochs fence prepared work across reopen and
snapshot replacement. Portable imports validate the snapshot bindings against the
static catalog before publication.

Namespace routing uses an atomically populated prefix index; legacy read-only
spaces retain the scan fallback until an authorized writer completes adoption.
Native Lite consumer lookup and invalidation use namespace-specific reverse
bindings. Projection clocks are read from the source owner's namespace index,
not a second copy in reverse bindings. Structural routing comparisons group
labels once per routing domain rather than rescanning sibling catalogs for each
resolver.

HTTP descriptor/status cuts export no execution policies. Execution cuts hydrate
only named resolvers and, for query resolution, the siblings that can consume the
same extraction. Consumer pages own strings independently of storage cursor
movement. Progress observation runs in bounded groups of 16 source tables, with a
shared deadline and per-worker allocation ownership.


Resolver catalog lookup shares generation-owned indexes for namespace consumers,
identity names and replacement reservations. Warm projection publication checks
reservations by identity rather than rescanning the catalog per consumer; point
entity discovery clones only the selected descriptor. First-use Lite adoption
prepares selected namespace policies before entering the writer lease and fences
source configuration generations both at adoption and final publication.

Metadata consumer discovery uses the same durable prefix trie as Lite, with
separate namespace/source postings. Matching namespaces are ordered and paged
under one catalog revision; a source may appear in multiple namespace pages, so
HTTP admission deduplicates resolver identities rather than source tables. Source
edits and deletions update postings and reference-counted routes atomically.
Decoder 43 rebuilds the namespace routing index through the bounded bootstrap
worker before publishing v3 readiness. Its records remain in the resolver
namespace snapshot projection; legacy reads retain their scan path until ready.

### Scoped diagnostics and batched metadata

Owner progress reads scalar catalog descriptors without cloning aliases,
provider configuration or namespace policy. The private progress request accepts
`resolver_names`: omission enumerates public active identities, an empty list
selects none, and a named set uses the catalog's identity index. Point waits
filter heartbeat diagnostics to those identities and probe only their progress;
an unrelated resolver's backlog does not cause another owner request.

HTTP reads and metadata apply share a name-indexed declaration/identity join,
including dropped fences, retired identities and shared policy validation.
Projection propagation resolves each destination and namespace binding once per
transaction, replaces the identity set in one batch, and serializes metadata
once. Existing projection attachments and retirement records survive the batch.
Namespace/source postings are rewritten only when their encoded content changes.

A shared streaming namespace builder owns unique policy entries independently of
consumer transport pages. Routed adoption releases each page after consumption;
embedded adoption uses the same union semantics. Both validate the final graph
once before publication, preserving the revision cut, origin precedence and
static-policy checks. Scale regressions cover 5,000 independent policy pages,
1,000 shared source bindings, reversed identity joins and progress reports under
a fixed allocation budget with large configured policies.

Destination-wide reverse bindings contain only source IDs. Scoped postings are
the sole descriptor directory; old aggregate descriptor rows are retired during
ordinary reconciliation or bounded bootstrap. Dependency maintenance caches
logical destination resolution per source cut, so repeated declarations do not
reparse the same destination catalog. Legacy read-only inspection also uses the
shared streaming union before a namespace has been adopted.


### Curation admission and retirement observations

Entity consumer discovery uses the shared native `ResolverAdmission` contract.
It retains identity, namespace, static aliases and suppression, publication
clocks, promotion/retirement flags, and legacy policy only until adoption. It
excludes extraction artifacts, scorer/adjudicator prompts, provider configuration,
and embedding selectors. Embedded consumers clone that contract; private routed
consumer rows serialize the same fields. Empty artifact fields preserve existing
private `ResolverConfig` wire decoders, but admission descriptors must never be
used as execution definitions. Reconciliation and query execution continue to
load their complete selected execution contracts.

Scoped consumer rows are already selected and are consumed directly. Legacy
whole-source rows use a name-membership set, so a dense source with R resolvers
costs O(R) selection work. Named review/curation reads select one catalog identity
before cloning policy; review admission still excludes retired and private
replacement streams.

A named owner progress probe may acknowledge a retained retired resolver, while
an unselected public progress listing continues to enumerate active resolvers.
Retirement is a private owner proof, not an inference from missing heartbeat
rows or a client flag. An acknowledgement is terminal only after the current
owner reports the required config generation, curation generation, physical
authority incarnation and missing-authority state. Retired identities have no
replay workers, so unrelated live-worker backlog cannot hold entity curation
pending. Retirement never substitutes for an explicit source-journal watermark;
those waits still require their sequence proof.


### Indexed static profiles and compact owner observations

Static declarations have a shared content-addressed adjacency index. A profile
identity includes its key template, aliases and suppression policy. Source
bindings reference that immutable identity; profile refcounts and forward/reverse
edges change atomically with source enrollment, edits, retirement and removal.
Identical profiles share their graph. Source execution-only edits leave it intact.
The per-owner descriptor cache retains profile identities until catalog changes;
worker projection clocks do not invalidate static content identity.

Native point planning traverses curated and indexed static edges together, then
loads only the incident aliases for profile validation. A namespace with 17,000
unrelated static aliases no longer transports, copies or reindexes that universe
for a tiny correction. Traversal limits cover the visited component and its
edges, consistently for routed and embedded execution. Suppression policies and
source publication fences still apply. Legacy adoption folds source-owned state
once; warm native operations use the index.

Metadata publishes v8 consumer/profile readiness only after its bounded backfill
finishes, under topology protocol 51. Lite bootstraps the same layout atomically
and installs reader floor 20. Embedded planning builds the static component
outside writer ownership, checks source/catalog generations and validates the
observed curated head again inside the final transaction. Routed publication
retains the catalog revision, physical authority and head fences.

Resolver progress reads the compact, named current-owner contract directly,
without constructing full runtime-status/index snapshots. A non-waiting request
samples once; a waiting request repeats bounded group batches within its budget.
Both can obtain retirement and empty-owner proofs. Ordinary runtime snapshots
also preserve every namespace publication clock when cloning diagnostics.


Lite additionally captures the static-directory epoch with consumer discovery
and checks it after acquiring the destination writer. Late enrollment, source
removal and directory changes fence publication even when no initially selected
source changes. Read-only redirect inspection retains curated-only traversal.
Progress routing uses identity and range directories directly; it neither loads
DDL bodies nor builds runtime snapshots.

### Scoped native publication and admission proofs

The v7 directory additionally retains each immutable static definition separately
from its adjacency and lightweight head. Before a namespace has a native head,
authorized adoption reads the complete selected declarations once. Private
consumer pages carry a deduplicated cold-profile dictionary, so shared static
arrays are not repeated for every resolver; Lite uses the same profile identities. This applies
both to the first native correction and to new namespaces on an already native
destination. Warm consumer pages retain only profile references and incident
edges; an indexed-readiness flag never substitutes for a missing namespace head.

A private admission proof is maintained atomically with table DDL, containing
physical identity, native-authority status and the exact schema fingerprint.
Native entity and proposal APIs receive this proof with a minimal table descriptor.
Authority component reads and warm publication do not decode the table definition.
The final transaction checks the physical proof, catalog revision and individual
head/queue revisions. New namespace membership and legacy adoption retain their
cold DDL path; ordinary corrections never enumerate all heads to rebuild headers.
Table removal removes the derived proof in the same transaction. A bounded v7
backfill installs proofs before indexed readiness is advertised.

Native corrections enqueue the changed namespaces. A scoped job owns one active
namespace cursor; separate durable rows index its pending namespaces. Repeated changes mark its active
pass dirty without rewinding it; after its tail it rotates through pending namespaces before repeating a dirty
namespace. Table lifecycle and bootstrap invalidations subsume scoped
work and retain the destination-wide path. Scoped ticks use namespace postings,
so unrelated source namespaces do not receive replay/projection work.

Validation compiles curated canonical paths, suppression and distinct adjacency
once per affected namespace. Each source profile adds a compressed overlay at
curated terminal nodes and checks only its changed paths and incident distinct
pairs. Source-local suppression remains independent. This avoids copying the
curated graph and parsing the namespace state for every profile.

Owner observation requests are partitioned by both the 4,096-name receiver bound
and a 512 KiB encoded request budget, with the same bounded fan-out across groups
and chunks. Partial or failed chunks cannot certify absent owner facts. This
contract is the same for non-waiting observations and bounded waits.

Grouped-source retirement caches only the semantic digest of an exact immutable
catalog content generation. Every lookup hashes the current transaction's catalog
bytes; publication, rollback and restored histories therefore miss immediately
when content changes. A cold calculation retains the maintenance allocation
budget. The cache owns only two digests and never retains transaction buffers,
provider objects, readiness decisions or authorization.


### Effective component and queue publication costs

Static adjacency is filtered against native redirects and removal tombstones
before traversing an edge or charging component limits. Masked declarations
cannot bridge a small correction into an unrelated large graph. Each request
prepares effective aliases once per immutable admission profile, before consumer
fan-out; embedded planning reuses that prepared slice through validation.

The v7 static directory stores configured suppressed-key membership as postings
by entity and profile. Warm planning reads compact profile heads plus membership
for visited component keys. It neither transports the complete configured key
universe nor parses unrelated text suppression rules. Cold adoption retains the
complete immutable definition. Source enrollment and removal update negative
postings with aliases and refcounts in the same transaction.

Scoped metadata replay writes namespace binding deltas and refreshed compact
consumer postings, without decoding the source table, rebuilding its dependency
index, or rewriting unrelated resolver declarations. Execution and provisioning
reads join these durable deltas in their catalog cut. Source declaration stamps
fence postings; a declaration edit or removal retires its deltas. Lifecycle and
legacy bootstrap retain the complete source projection path.

Proposal bodies retain evidence separately from compact lifecycle headers.
Entity correction reads incident headers, computes queue deltas, and publishes
only changed headers and indexes. Public proposal reads join the original
evidence body; quotas continue accounting for evidence and reserved decision
provenance. Header-only publication validates immutable proposal fields and
retains evidence identity. Existing rows acquire a header when touched, without
a queue scan. Lite reader floor 20 and memory identity format 3 reject older
readers that would ignore these lifecycle headers.


### Incident profiles and bounded replay control

The v7 directory retains one scalar count of active immutable profiles per
namespace. Enrollment/refcount transitions update it with alias adjacency and
negative postings. Point discovery reads only profiles reached by an incident
edge or matching negative-key posting. If the count exceeds that incident set,
one empty overlay represents every remaining source constraint; this preserves
base suppression validation without enumerating unrelated heads. Private native
views explicitly mark sparse profiles, and their catalog-fenced enrolled consumer
bindings certify omitted profiles as having no incident declarations. Cold
adoption still reads complete immutable definitions once per content identity.
Topology protocol 51 and Lite reader floor 20 fence this representation, and
indexed readiness follows the v8 backfill.

Lite groups consumer indices by namespace once outside the destination writer
lease. Each namespace retains its prepared profile slice in a shared lookup map,
used directly for component discovery, planning and validation. Source enrollment
and head fingerprints remain publication fences. This avoids rescanning all
consumers for every matching namespace. Queue incidence lookups also deduplicate
entity keys before scanning postings, so a canonical target repeated by thousands
of aliases is visited once.

Native replay uses a fixed-size command containing physical destination ID,
catalog revision, deployment mode and the expected durable job digest. A stale
revision, cursor, dirty state, transition lock or retired target prevents cursor
advancement. Scoped work descriptors contain no source DDL and require no table
manager replacement: execution reads already join the committed binding deltas.
Global/bootstrap descriptors carry at most sixteen source IDs, and refresh their
actual table definitions after publication. The standalone coordinator certifies
its epoch only if that command was the sole catalog revision change; failed or
uncertain publication leaves its projection uncertified for the next reader.
The older same-table CAS encoding remains replayable for existing log entries.

### Query policy reads and native mutation outcomes

HTTP render/resolve reads obtain compact namespace bindings without expanding
execution attachments. HTTP and embedded queries hydrate policy in two phases
per resolver: rendered mention keys and normalized texts before blocking, then
mention/candidate keys before scoring. Each phase uses bounded requests of at
most 1,024 keys and 128 texts, then unions duplicate facts while fencing the
exact head revision and fingerprint. Transport pages are released between
requests; the union retains only unique facts. Embedded queries read current
native authority immediately, independently of projection replay.

Query publication checks the source definition and schema, logical and physical
destination bindings, destination schemas, and the heads of executed namespaces.
Routing includes only applicable artifact/kind siblings. Unrelated catalog edits
and projection replay do not invalidate completed model work. HTTP admission and
final authorization still recheck the principal and row predicates. Source-owned
legacy policies retain their existing execution path. Full namespace payloads
remain available for provisioning and explicit exports.

Key rendering uses the native forward policy reader. It follows static/native
edges with native overrides and unmerge tombstones, without incoming-alias or
cascade traversal. Text rules use normalized point lookups. Resolution uses a
sparse pin index: each native node records its pinned-descendant count, and only
child branches containing pinned endpoints have reverse entries. An unrelated
pin therefore never expands an unpinned reverse component. Static aliases select
the relevant canonical roots; incident pins retain endpoint tombstones and
negative policy. Zero-pin namespaces take the forward-only path.

Pin-index storage is linear in native nodes and pin endpoints. Authorized writes
upgrade older namespaces with one linear backfill and publish readiness last.
Mutations detach replaced edges, update endpoint counts, then attach final edges
in the same transaction as the head. Read-only legacy namespaces retain the
conservative component reader until upgraded. Reader floor 20, memory identity
format 3 and topology protocol 51 fence writers that cannot maintain this index.

### Query policy and destination proof costs

Configured aliases and live native redirects compose at a pinned namespace head.
A prepared immutable context indexes distinct pairs by effective canonical
component and is keyed by profile identity plus qualified native graph lineage. Warm
reads follow only requested/incident paths; unrelated pinned aliases do not
trigger forward qualification. An immutable static profile base is loaded once; graph overlays retain its maps
and node postings and compile only native records plus affected static
predecessors. Cold overlays read native graph pages outside writer ownership.
The shared cache assigns 48 MiB to bases/overlays and 16 MiB to routing/pin membership
with access-based byte-budgeted eviction and reference-counted leases across
pages. Identical concurrent misses share a build; conservative build reservations
prevent profile and graph compilation under cache pressure. Base admission
precedes profile loading; denied modern profiles use persisted node postings
without materializing a static index. Idle LRU entries are reclaimed
before denying admission; active payloads remain charged through their last
lease even after retention eviction. Dependencies keep bases discoverable and
charge shared payload once; reclaiming idle overlays releases their bases.
A separate derived graph stamp
preserves reuse through text/provenance edits while the complete namespace head
still fences publication. Legacy or mismatched stamps use full lineage.

Oversized or unadmitted graphs use pin-directed queries over immutable profile
node/component postings and sparse native pin counts. Native edit intersections
identify components redirected into the requested roots. Roots absent from both
the static domain and native incoming directory skip this intersection while
retaining their own pins. A bounded root-to-component projection reuses the
intersection across pages and pin edits. Its SHA-256 key includes profile identity
and a redirect/removal signature; the optional head-qualified `query-routing`
stamp falls back to full lineage after legacy writes or restore. Compilation
reserves 12 MiB, bounds scratch at 8 MiB and raw route data at 1 MiB; budget or
scratch exhaustion retains the exact indexed join. A per-component pin-membership
projection groups pinned nodes by effective root and uses the qualified graph
fingerprint, including pin edits. It shares the bounded routing lane and keeps
only owned memo/membership data across reusable 2 MiB cursor/path frames. Warm
split-component pages avoid requalifying other roots' pinned nodes. Only matched pin-bearing
nodes need effective forward qualification and native pin-child traversal;
unpinned reverse alias branches are never visited. Returned facts and forward
path limits match the compiled query path. Embedded and routed adapters share
this join, preserving native precedence and forward-only rendering.

New enrollments publish node postings and readiness atomically. Older profiles
use an immutable in-memory node index until enrollment supplies the optional
projection. Legacy namespaces without propagated pin counts stream distinct
records until an authorized writer adopts the index. Reads do not bootstrap or
mutate these projections.

Routed pages reference the enrolled immutable profile instead of retransmitting
its alias universe. Each page revalidates enrollment, logical target and native
head; full declarations load only on cache misses. The source query fence still
revalidates schema, binding, incarnation and authorization before returning.
Protocol 54 qualifies references against the existing v10 directory; missing
readiness retains inline compatibility. Lite phases retain a physical snapshot of the active immutable node projection
and reuse one prepared lease; each native page and the final source/schema fence
still revalidate independently. Legacy profiles keep inline compatibility until
enrollment publishes the node index. Text-only pages skip identity preparation, aliases and distinct discovery.

Metadata stores ordinary destination query fingerprints atomically with table
DDL, independently of native curation admission. Initial query admission and
final verification read the small fingerprint together with live logical
bindings, namespace heads and metadata incarnation. Protocol 53 and the v10
bootstrap generation qualify the complete destination proof backfill. A missing
proof retains the definition-hashing fallback until the leader repairs it;
schema/index edits replace the proof and fence captured queries immediately.

Source query declarations share one serialized producer projection per source
artifact. Repeated resolver references never multiply producer serialization or
retain duplicate producer bodies in the metadata writer arena.

HTTP entity discovery builds immutable namespace adoption and profile maps once.
Namespace policy replacements parse and serialize the source document once per
batch; publication joins predecessors by namespace key. These operations remain
linear across multi-namespace entities.

Standalone native mutation receipts distinguish record-only commits from table
definition changes. Only definition changes refresh destination DDL under the
catalog mutex; legacy receipts conservatively refresh it. Native records,
receipts and the outcome publish in the same transaction. Metadata protocol v51
qualifies dependency-scoped query-policy reads and sparse pin maintenance.


### Batched identity publication and bounded consumer progress

Pin-count maintenance unions affected ancestor paths in each of three phases:
detach all changed edges using captured old weights, update endpoint flags in
the detached forest, then attach the final forest. Each phase propagates summed
deltas in topological order and writes each affected count/branch once. Edits
cannot multiply work by walking the same tail repeatedly. The head and sparse
index still publish in one transaction. Rotating a path, changing pin endpoints,
and removing all redirects preserve the same pin semantics.

Entity admission prepares one open-proposal incidence index and one lazy queue
overlay for the whole request. Namespace operations consume matching key or
symmetric-pair postings; a terminal proposal preserves its first cause, while
an explicit review overrides its own lifecycle header. Terminal history stays
borrowed until the first actual decision. Lite entity edits share this batch
contract and publish queue changes once after their namespace plans. Explicit review IDs
resolve once, and decision updates use the shared ID index rather than rescanning
terminal history for each namespace.

Native HTTP mutations return `namespaces` once per affected namespace, including
its revision and absorbed-key outcomes. They return an empty `results` array
and a `progress_url`, relative to the mutation request URI. Consumer identities
refer to namespace outcomes by `key_template`; the embedded C ABI also returns
shared namespace outcomes instead of repeating absorbed keys per consumer.
Legacy sources without native authority retain scalar consumer summaries.

`GET /tables/{table}/entities/{key}/progress` (also available in the explicit
database/namespace scope) returns at most 64 consumer proofs by default, with
`limit` from 1 through 256 and an opaque `next_cursor`. The private keyset cursor
seeks individual resolver descriptor rows, so thousands of resolvers in one
source do not escape the page's decode or output bound. Each page loads only its selected native heads, checks
destination read authorization again, and verifies those heads after observing
replay. Physical table ID, entity key and catalog revision fence continuation.
Catalog changes return Conflict and require restarting enumeration. Namespace
revisions are current per page; `complete` means enumeration ended, while each
consumer's `applied` reports its own replay proof. An empty final page is valid.
No progress page loads redirect components, static profiles or proposal evidence.
Until the leader certifies the consumer-index backfill, progress returns
Unavailable instead of scanning the table catalog.

The v7 routing directory certifies whether consumers exist. Warm admission
without `wait` skips all adopted consumer rows; only cold source-owned namespaces
are enumerated for authorized folding. Namespace head membership and the catalog
cut fence this omission. The presence proof distinguishes an empty consumer set
from pending replay without enumerating warm sources.

Admission without `wait` schedules no per-consumer observation and conservatively
returns pending when consumers exist. Explicit waiting still observes all required
consumers under its deadline, without expanding the response payload. Replaying
a terminal native proposal follows the same progress contract. A successful
mutation is durable independently of subsequent progress-resource failures.

Embedded query sessions retain one schema digest per destination table. Catalog
epochs, active schema versions and schema namespace generations permit
constant-space reuse within a qualified DB incarnation;
a new process-local instance or core, physical namespace change, or epoch change
requires content requalification. Equal content after replay updates the stamp
without rejecting the query. Digest capture checks these stamps before and after
reading definitions so concurrent publication cannot qualify mixed content.
Verification checks each unique table once and
keeps independent native-head fences for each executed namespace.

Topology protocol 51 qualifies bounded consumer-reference cursors and selected
head reads. Native reader floor 20 and memory identity format 3 remain unchanged
because pin-index storage and proposal lifecycle representation do not change.


### Selective proposal admission and replay observation

Proposal creation reads namespace membership and the selected proposal ID, without
loading redirect components, other proposals or their evidence. Native heads and
indexed membership certify warm ownership; consumer enumeration is restricted to
cold namespaces that still need authorized source folding. Duplicate creation is
a point-only receipt. Queue-only rejection reads and updates the selected item.
Accept/pin admission loads component policy and incident lifecycle headers once;
terminal receipts without waiting do not hydrate policy or enumerate consumers.
Explicit receipt waits read namespace heads, then observe the required consumers.

Embedded proposal review prepares namespace plans with only the reviewed proposal,
unions affected queue keys (including absorbed keys), reads incident headers once,
and applies one supersession overlay. Policy, lifecycle headers and retained
evidence commit atomically. First-cause attribution and the explicit reviewed
verdict retain their precedence across overlapping scopes.

Metadata v8 readiness additionally certifies one progress descriptor row per
source/resolver/namespace. These descriptors contain only progress identity and
configuration fences. They publish with DDL, removal and enrollment; bounded
leader backfill repairs unchanged v7 source rows and advertises readiness last.
Missing readiness yields a deterministic virtual bootstrap job even for an idle
catalog; discovery is read-only, and only a catalog/digest-fenced leader advance
publishes its cursor. Bootstrap reconciliation uses durable profile identities,
so retries do not enroll static-profile references twice.
The existing grouped v7 rows remain the cold-admission and replay projection.
Progress pagination uses keyset continuation over individual descriptors, never
an offset into a fully decoded source array. Old progress cursors must restart.
Topology protocol 51 qualifies membership reads and this readiness generation.

Replay observations read only resolution/promotion stages, durable backfill and
empty-owner proofs. They exclude enrichment, graph maintenance and text health.
Embedded mutation responses capture namespace heads before commit and render
consumer status after the writer transaction closes. Each source is sampled once
for all consumers, including mixed promotion requirements. Wait polling uses the
same selective observation and reuses it for replay floors and response progress.
An unavailable observation reports pending without invalidating a committed edit.

Configured suppression is an immutable indexed part of each static profile.
Enrollment/backfill validates it with the execution parser, stores exact keys,
normalized text and the scalar minimum text length, and publishes readiness in
the same transaction. Query pages point-read only their seed/forward-path and
incident-pin endpoints and requested mention texts. The same helper serves
HTTP and Lite; it never scans the negative directory. A separate optional
`configured_suppression` projection is query-only, keeps native corrections
separate, and is unioned across bounded pages. Mixed projected/legacy pages
are rejected. Attachment caches bounded negatives by their own content, never
by the full profile ID (which would conflate different query frontiers).
Text-only pages retain the profile reference and avoid alias-context builds.
An isolated 100,000-rule projection with two keys and two mention texts issues
five point reads, zero scans and uses 7,086 bytes of arena capacity.
Until readiness, admission hydrates the inline descriptor. V12 metadata and
V8 Lite bootstrap repair existing enrolled profiles without changing refcounts.

Lite detached publication captures a small proof over persisted schema, index,
enrichment, resolver, merge and projection-epoch records. The proof retains a
native read transaction for the detached turn and compares immutable document
record references at final admission, under the existing writer fences. This
uses existing transactional native identities rather than adding a second
catalog digest format. The pinned checkpoint prevents record-page reuse/ABA;
file inode and the existing namespace/root-incarnation checks fence replacement.
Unrelated primary commits may replace B-tree leaves but do not change these
record references. No full catalog is copied, serialized or hashed on that path.
A rewritten record receives an exact byte comparison; identical bytes remain
valid, changed or newly present/missing records refuse publication. The pin
ends with the detached turn, retaining no permanent reclamation frontier.
Live execution grants, source guards, leases and namespace heads still undergo
their own publication checks. No authorization or decision is cached.

Topology protocol 58 distinguishes routing declaration admission from execution
dependencies in query proofs. Both key render and resolve retain sibling headers
for label ownership and fence the source declaration, authorization and binding.
Key render qualifies only its selected destination's live binding, schema and
native head. Resolve qualifies every sibling destination it may execute. The
controller carries the operation's execution scope into final verification;
the scope is hashed into the proof, so a render proof cannot qualify a wider
resolve. Changing an unused sibling destination or its native curation no longer
invalidates key rendering; changing selected state or sibling routing labels
still does. Existing indexed-projection readiness and legacy hydration remain
independent, and this proof change needs no new durable backfill generation.
With 1,000 sibling destinations, key render performs one destination qualification
at admission and one at final verification; resolve retains all 1,000 checks.

Published native query nodes take precedence over full-profile graph compilation
in the shared preparation contract. A complete lookup supplies both direct node
and ordered node-seek operations. Its prepared query retains native head, graph
and routing stamps but leases no compiled profile graph, whether the graph cache
is cold, warm, evicting or occupied by active callers. HTTP and Lite both admit
that lookup without a full-body loader or a synthetic oversized build estimate.
Ready routed reads no longer fetch an optional profile-size hint either.
Only legacy/unready profiles, or explicit bulk callers without a complete
lookup, supply a full-profile loader. Missing readiness therefore retains exact
legacy results while backfill runs; restored readiness immediately returns to
indexed execution even if a legacy compiled graph is still resident.

Forward paths and incident pins still use the existing bounded indexed reader,
request-local canonical memo and bounded native-routing cache. Native heads,
profile enrollment, source declarations and final authorization remain independent
live fences. This changes execution planning, not durable keys or protocol shape.
The working-set regression publishes five profiles of 5,000 aliases, alternates
three passes with different size estimates and absent loaders, and requires zero
full-profile loads or compiled-graph leases. Each two-key query visits at most
two rows and retains less than 128 KiB of arena capacity. Native head changes
invalidate an already prepared query, and missing readiness still exercises a
single exact legacy load before returning to the indexed path.

The document-inference integration preserves main's published job-state wire
command (tag 79); resolver namespace advancement uses tag 80, gated by topology
version 59. The shared snapshot registry retains document job bodies, owner quota
counts and due-order scheduling entries together. A replay/snapshot-install test
checks that a queued job remains discoverable and counts against its owner after
restoration, without leaking into another metadata group.

Distinct discovery no longer builds a namespace-wide map of native override
routes. The durable native `pin-count` postings already identify every point
where a pin-bearing native path can enter a configured profile path. An ordered
intersection with that profile's native query-node index produces precisely the
boundaries worth canonicalizing. The same join handles alternating configured
and native edges, removed configured aliases and native-only pin branches.
Ordinary native overrides with zero pin weight do not participate.

Static and curated graphs use destination-owned durable
`query-pins-v1/<namespace>/<profile>/member/<root>/<boundary>` postings. A
`state` row binds complete membership to the immutable profile identity and
native graph fingerprint. Ready reads seek only the requested roots' posting
prefixes before following their pin-bearing native branches. Static component
seeks remain an exact fallback during backfill, rather than a permanent repeated
intersection with unrelated namespace pins. No cache lease or entry-size budget
participates in the ready path.

Only owner maintenance builds membership. One shared budget limits a turn to
128 posting skips, forward hops, path-compression steps and member publications,
with at most 129 derived/state writes. Each hop resets reusable scratch; a durable
walk cursor stores only its boundary, current node, hop count and optional resolved
root. Complete configured paths never accumulate in a turn's arena. Static-only
graphs jump directly through immutable scalar canonical nodes. Curated walks
publish graph-fenced `path/<node>` canonical shortcuts during a bounded second
pass, so later converging boundaries reuse completed paths across owner turns.
The cursor, shortcuts, members and readiness share one native transaction.
Native graph edits append to a destination-owned, 256-slot bounded edit journal
in the same transaction as pin propagation and the new head. Enrollment is owner-only:
native memory identities without static consumers create no unused log. Slots are
limited to 64 KiB. Each event captures changed pin-count boundaries, routing keys,
and old/new redirect targets. The same transaction maintains a deduplicated
`invalidated/<latest-sequence>/<key-hash>` range and a point version for each key.
Overwriting a ring slot removes an expired invalidation only if that event still
owns its point version. Repeated edits to a hot key therefore retain one posting;
storage is bounded by retained events, with no synchronous profile fanout.

Projection format 5 builds every configured node, including unpinned and
masked configured parents, plus the native forward paths reachable from them.
It never enumerates the whole native namespace for each static profile. Each
included node has one canonical path row, one root-dependency posting, and at
most one direct reverse-parent posting. Converging paths share these rows.
Static-only nodes use scalar canonical lookups and one direct edge; curated
paths share durable, resumable path compression. Pin membership follows only
after dependencies and configured-edge guards are complete. Each configured
edge retains a direct parent guard. If its source and parent have different
effective roots, a latent root dependency covers future unmerge. Parent-path
changes refresh guards through immutable reverse configured edges, including
edges currently masked by native routing.

Scoping the projection also requires detecting new incoming native paths.
The shared edit journal records native redirect targets that themselves have a
native redirect, using bounded point reads in the mutation transaction, without
walking chains or publishing per-profile rows. A latest-version `tails/` index
and `tail-key/` ownership postings deduplicate these entry points across slots;
slot expiration removes only descriptors it still owns. Negative dependency
proofs first intersect edited routing sources with the profile's published
paths, dirty nodes and active traces. All routing sources, including currently
flat edges, enter shared `routed-key/` and `routed/` indices; this preserves a
configured source that first enters an unknown node and later reaches a native
chain. Small bursts use point coverage checks. Larger sets freeze sorted recent
routing digests from the bounded compact journal, then gallop between that input
and the profile's coverage rows. A recently changed source is the only source-side
input: the retained historical key index is never traversed to discover recent
versions. Sparse inputs can skip gaps on either side. Cache validation uses a
separate workspace, so extending an older checkpoint cannot mutate an active
source iterator. Both changed-key workspaces fit in the existing 8-MiB proof
scratch. The target
index remains a cheap trigger, so flat unrelated edits need one empty range scan.
Both index families expire only versions still owned by the overwritten slot.

A fixed full-SHA memo shares converging suffixes within one proof. Exact terminals
also share the existing 16-MiB routing LRU, miss coalescing and live-lease accounting.
Antfly SHA-256 cache keys include the template, immutable profile, journal origin
and source key. Each immutable result owns its terminal and sorted full-SHA path
dependencies, including the terminal's absent outgoing edge. Unchanged routing
can reuse it directly. Otherwise one bounded pass over retained `routed-compact/`
records builds a sorted changed-key set; only edits newer than the result and
intersecting its dependencies invalidate reuse. Journal ancestry, barriers and the
256-event window are checked before accepting that proof. Unrelated edits retain
warm terminals; edits to a path or terminal immediately require a fresh walk.
Successful validation advances a small, synchronized validated-through checkpoint
without changing retained paths or dependency arrays. Readers copy sequence,
graph and routing identity together. Publication compares the observed checkpoint
and refuses backward sequences, so older or competing snapshots cannot replace a
new certificate. The next unrelated head validates only the new suffix rather
than replaying the result's entire lifetime. No checkpoint is advanced on failed
ancestry, dependency, history-window or barrier validation.
Removal compares the observed cache generation, preserving active leases and
concurrent replacements. Older snapshots cannot evict newer entries in that lineage.
Root sets, pin memberships, source proofs and grants are never cached in a terminal
entry. Retained strings and dependencies own their bytes; bounded caller scratch
and a 1-MiB reservation cover the largest frozen terminal result.

After a cached terminal is excluded from the requested root set, its full-SHA
dependencies can establish request-local suffix absence. The memo retains at most
32 immutable leases and materializes their digests only before a cold walk (or
when that pending set fills). Warm-only proofs avoid copying entire long paths;
a new entry into an already validated suffix spends no native forward work.
Pending leases are released on materialization and every proof exit, and their
memory remains charged to the existing routing budget. Root-set verdicts remain
local and no partially resolved canonical result is published.

A proof spends at most 16,384 cold forward steps. Cache hits spend no forward
steps. A cycle, affected canonical root or exhausted budget selects exact fallback.
Completed entries survive a later entry exhausting the work budget: the next read
can finish that entry instead of repeating the completed walks. Cache admission
and retention are optional; denied entries use the same bounded exact walker.
Routing-source enrollment has its own readiness flag, origin and barrier; legacy
history cannot authorize a scoped projection accidentally.

Owner repair expands reverse dependencies from changed routing nodes, rather than
all members of their old canonical root. It clears and rebuilds only those path
nodes and their incident pin boundaries. Only configured, projected, queued or
active-walk nodes enter routing repair; unrelated native edits are ingested
without copying their paths. New targets acquire routing rows while the changed
source path is compressed;
pin-only edits do not enroll unnecessary graph work.
Dirty old/new roots remain unreadable during partial publication. Journal ingestion
has its own durable sequence and graph anchor, separate from the published graph.
New edits enter deduplicated next-work queues without rewinding active expansion,
path reconstruction or membership cursors. Ingestion consumes the shared turn
budget until it catches up, then active repair uses the remaining capacity.
A fixed quarter-turn quota must not idle spare capacity and starve active work.
Each event is decoded once per turn in a separate 1-MiB scratch frame. The
remaining 7-MiB item frame resets after every boundary, routing or target lookup;
valid 16-KiB canonical keys cannot accumulate decoded strings across a batch.
Only queued writes survive in the transaction arena. Active work pauses only after exhausting its
available ingestion capacity while still behind. A persisted set of visited walk
nodes invalidates only walks whose prefix intersects a routing edit. Unrelated heads do
not restart a long walk, even across more than 256 updates, provided ingestion
keeps pace. Trace rows and retired work sets are reclaimed in bounded turns.
A bounded prune queue also removes unconfigured nodes with no incoming graph
dependencies, cascading through their old forward edges. A native declaration
alone does not keep a profile path live. Old targets and removed sources cannot
accumulate path rows under sustained unique-target churn. Configured nodes and
referenced targets remain live. Cleanup and the final
graph watermark share the native writer transaction.

Read-side validity uses one retained lineage anchor. Tiny edit bursts use one
ordered invalidation page and exact dependency reads. Strategy selection also
bounds the roots-by-edits cross product: beyond 128 combinations (except one/two
edits, which stay linear in roots), a complete small page supplies digests to the
same summary proof without reopening journal slots. Empty root summaries return
before allocating a leaf-page buffer. Larger bursts read compact full SHA-256
digests from the same 256-event journal into bounded scratch. A full-SHA hash
set compacts duplicate digests once, then sorts only the unique digests. The
maximum retained ring uses 5 MiB for digests and a 1-MiB index table. No per-root
sort or repeated slot decoding is required.

Root summaries contain sorted 64-bit SHA fingerprints with reference counts.
Small roots fit in one compact binary row; larger roots split into sorted
interval leaves of at most 128 entries. Inserts, splits, local merges, collapse
and deletion require at most four native writes per dependency. Readers stream
up to 128 leaves per page with reusable scalar caches. Intersection searches the
smaller side: small-root fingerprints binary-search the sorted changed hashes;
large roots probe the changed hashes through cached leaves. Every full-SHA hash
sharing a positive 64-bit fingerprint is emitted for exact verification. Empty leaves and exhausted
roots are reclaimed. Fingerprints are negative proofs only: every hit is checked
against the full-SHA root dependency posting. Reference counts preserve collisions
on deletion; latent configured-edge hits also require the native mask to be
absent. Compact journal records are at most 20 KiB plus an eight-byte sequence
per event, with no per-profile fanout on native edits.

This keeps scans independent of distinct edited-key cardinality for unrelated
roots. All transient journal, native mask and dependency reads use reusable
scratch rather than the request arena. Old/new redirect targets and the native-tail fence cover
new incoming paths. Dirty roots retain exact fallback during partial publication.
A journal gap, oversized event or older projection format also uses exact fallback.
Fallback canonicalization decodes one hop at a time in resettable 256-KiB
scratch. Cycle detection retains only full-SHA hashes (64 on the stack, then a
bounded 640-KiB workspace for at most 16,384 keys), rather than decoded path strings.
The optional memo has a separate bounded lifetime. Both cached membership and
streaming fallback accept long keys without accumulating JSON per hop.
No request advances owner work. Final head, source and authorization fences remain
independent. Full rebuilding is reserved for initial enrollment, format upgrade,
oversized invalidations, or ingestion falling behind retained history.

This generation uses Lite reader 27, topology protocol 66, metadata bootstrap v19,
and Lite query-pin enrollment v7. Legacy v12-v18 declaration/profile proofs remain
valid during dependency backfill; older pin projections use exact fallback
until their owner publishes format 5. Older native readers refuse the new floor.
A profile enrollment epoch also drives bounded retirement cleanup, so replacing
immutable profiles does not accumulate their obsolete membership generations.
Namespace retirement uses the existing bounded native-prefix cleanup.

Metadata namespace replay advances these pages before acknowledging a source
binding. Partial work yields to pending sibling namespaces and retains each
namespace's source cursor and dirty state. Bootstrap v15 discovers historical
static and curated profiles even in an idle catalog. Already-certified v12/v13/v14 declaration,
consumer and profile proofs remain usable during this backfill; pin readiness
is a separate graph-specific watermark. Metadata topology version 62 gates the
new durable owner work. Lite maintenance uses the same generation builder under
its destination writer grant, retains the source work item until complete, and
preserves ready postings on reopen. Historical Lite sources enroll their scopes
in the existing fair work queue; partial pin pages advance its observation cursor
without deleting the pending work. Scoped admission identities reuse cached
profile hashes and avoid cloning unrelated alias declarations. The physical
profile-enrollment cut is revalidated after writer admission before member or
retirement publication. Lite reader version 23 fences the additional
native records and enrollment epochs. An existing ready directory needs only
a reader-floor update, not a wholesale rebuild; read-only files do not attempt
that update. Source documents, native heads and final authorization keep their
independent live fences.

Legacy or not-yet-materialized profiles retain the exact bounded-scratch
sparse-join fallback until owner work completes. Foreground reads never publish
projections or advance backfill. Ready root postings make read work proportional
to requested roots and their returned boundaries, even when the complete profile
would exceed the former 1 MiB route-cache entry limit.

The 6,000-component regression uses 128-byte root suffixes, real native profile
indexes and a storage-style ordered cursor. It checks repeated uncurated reads,
curated owner publication, head invalidation, changed roots, unmerge and retired
profile cleanup, keeping reads below 24 storage scans and request arenas below
128 KiB. Owner turns remain bounded independently of the total projection size.
The component probe reports 6 scans without overrides and 4 scans with ready
curated membership at both 1,000 and 6,000 components. The 10,000-override
regression remains bounded. The independent full-composition reference checks
1,024 seed queries over randomized acyclic graphs, exercising both durable
membership and exact legacy fallback across alternating paths and removed aliases.
Metadata replay and Lite reopen tests verify the service adapters use this shared
contract.

The dense-component regression has 1,000 and 6,000 unrelated pins and equally
many unpinned aliases in the queried component. Three repeated indexed reads
return no pairs in four scans at both sizes (previously 1,366 and 7,992 scans
per request). A 16,000-edge configured-chain regression checks bounded durable
resume, invalidation during a paused walk, converging-boundary shortcut reuse,
portable state validation and final readiness without scratch exhaustion. Lite
owner tests exercise static materialization, curation, reopen and unmerge.

The incremental single-pin regression adds one pair with 1,000 and 6,000 unrelated
pins. Both cases complete in one owner turn with 13 writes and 24 scans;
the previous whole-profile repair needed 2,025/12,142 writes and 24/141 turns.
A separate full-composition reference checks 1,536 seed queries during 48
interleaved routing edits and partial owner turns. It also exercises a missed
256-event journal window and verifies bounded recovery rather than stale readiness.

Dependency projection regression evidence: with 1,000 and 6,000 pins sharing a
root, redirecting one configured leaf completes in one 128-work owner turn with
45 derived/state writes and 39 scans. A zero-result query on an unrelated root
uses six scans after 17 unique edits and after 145 edits with repeated hot keys;
the prior 6,000-pin implementation used 48,481 owner writes/470 turns for the leaf
and 25,995 scans for the 17-edit read. Fixtures assert the same bounds at both
sizes and validate portable rows, legacy projection upgrade, masked unmerge at
every 32-work repair cut, and ring eviction without orphan invalidations.
Continuous redirects to unique temporary targets retain only the live graph;
a still-masked configured fallback keeps its unaffected root indexed.
Run the focused dependency contracts with `zig build memory-dependency-test`
from `zig/`, or include them through the complete `memory-test` target.

The distinct-invalidation regression edits 256 and 4,096 different configured
keys. Both unrelated zero-result queries use seven scans and less than 34 KiB
of request arena; both succeed with a 64-KiB fixed request allocator. An
8,192-root proof checks scratch reuse when every summary is empty. The former
4,096-key path used 133 scans, 3.75 MiB of arena and exhausted that allocator.
A 128-pin root repair completes in 51 owner turns with an unrelated routing edit
before every turn (49 without churn), moving all 128 members. Previously it moved
none in 64 busy turns. The long-walk regression ingests 500 consecutive unrelated
heads during a 16,000-hop backfill. Summary regressions cover fingerprint collisions, repeated references, leaf
splits, merges, collapse, deletion and empty-root reclamation. Owner turns allow at most
129 derived/state writes with a 128-work budget; the minimum internal repair
budget is 32 to accommodate atomic summary and guard updates.

Further owner/read regressions use 16 unrelated routing updates before each
128-work owner turn. The older 128-member component is fully moved within
128 busy turns; the current background component then reaches global readiness
in one quiet turn. Previously no members moved in 64 busy turns and recovery
needed another 123 quiet turns. Global readiness under sustained input still
depends on whether the complete repair workload fits available owner capacity.
For 128-key journal batches, a turn performs 64 derived/state writes with under
93 KiB of owner arena (formerly 17 writes with 1.03 MiB); a 512-KiB fixed allocator
succeeds, and event reads are bounded to one lineage anchor plus one ingestion
read. A 1,024-root, small-burst proof uses 3,074 point reads rather than 41,986;
the complete zero-result query uses 12,294 point reads rather than 51,206.
The summary path adds bounded root-prefix scans; this measures storage-operation
counts rather than claiming equivalent end-to-end latency gains.

Scoped-projection and summary regressions extend these measurements. With 16
configured aliases, adding 6,000 unrelated native aliases leaves 18 projected
paths and 264 bytes of summary values: enrollment takes six bounded turns and
204 writes, rather than 863 turns and 67,121 writes. Updating 64 unrelated native
routes then takes three state writes. For 6,000 independent two-node roots,
summary values occupy 288,000 logical bytes instead of 47,841,280 bytes of counter
pages. These are encoded value sizes, not physical database or resident sizes.
A 100,000-member root uses 1,204,420 summary bytes; 10,000 absent fingerprints
produce zero positives in nine leaf scans with the deterministic fixture.
Repeated-edit proof coverage holds 130 unique invalidations across 64 events and
64 queried roots to 8,320 summary probes, versus 532,416 without deduplication.
Native-tail tests cover chains present before and constructed after a redirect,
correct results at every partial owner cut, orphan pruning while native
declarations survive, shared thousand-hop suffixes and ring ownership expiry.

The membership backfill now merge-seeks configured node and native pin-count
indexes. Either index skips unrelated gaps, so neither all native pins nor all
configured aliases are enumerated merely to find a sparse intersection. The
exclusive digest cursor remains compatible with an in-progress format-5 posting
pass. A paused boundary walk finishes its original key even if that pin disappears;
queued repair retracts the old member before readiness. No storage-format or
reader-floor change is needed for these improvements.

With 16 configured aliases and 0, 1,000 or 6,000 unrelated native distinct pairs,
enrollment takes four owner turns, 122 writes and 192 point reads at every size.
The former 6,000-pair case took 107 turns and 25,032 point reads. A separate
membership-phase fixture with two relevant pins uses 10 reads and eight scans
(including result verification) with either 1,000 or 10,000 configured aliases.
A historical native-posting cursor resumes and restores four missing members in
one bounded turn. Removing a pin during a paused curated boundary walk preserves
exact results at every repair cut.

An unrelated 8,192-edge native tail still takes 8,194 native reads on its cold
proof, and zero native reads on subsequent proofs (one bounded journal-range
scan remains). Two disjoint cold tails can exhaust the shared work budget once;
the next read resolves the remaining tail in 8,194 reads, then both are warm.
Regressions cover immediate routing invalidation, distinct configured profiles,
caller-scratch poisoning and exact behavior when routing-cache admission is denied.
The 128-key, maximum escaped canonical-key fixture that formerly exhausted 8 MiB
after 72 entries now completes with 128-work turns; mixed routing/target events
also complete with the supported 4,096-work quantum.

Fresh-review regression measurements for the routing-source generation:

- With 64 queried roots, three dependencies each and 16,385 unique invalidations,
  adaptive intersection makes 192 fingerprint probes, versus 1,048,640 previously.
  Exact full-SHA checks still fence every positive, including fingerprint collisions.
- Two unrelated 8,192-edge chains and repeated unrelated routing edits require
  38–48 reads per public query, versus roughly 16,670 before scoped discovery.
  Relevant cached chains additionally retain zero forward steps after unrelated
  edits; changing a downstream dependency forces a fresh walk immediately.
- A valid 3,647,828-byte namespace (4,194,304-byte default limit), 1,800 native
  hops and 1,000-byte keys succeeds through cached and streaming membership
  fallback. Previously the unrelated-root query exhausted scratch.

The recent-source and checkpoint regressions add these operation-count bounds:

- With 4,096 historical covered sources, eight new unrelated sources use 11
  scans and nine use 13. The former history-wide join jumped to 8,197 scans at
  nine sources. Recent intersection also covers dirty nodes and active traces,
  and remains stable while another proof extends older routing history.
- After warming an 8,192-hop canonical path, a new entry into its suffix uses
  three reads, versus 8,196 before cached dependencies fed the local memo.
  Warm-only cache hits materialize no path digests.
- Accepted checkpoints use two journal reads per unrelated routing head: 512
  reads across 256 heads, versus 33,152. Continuous validation also remains valid
  at head 257; actual dependency edits, wrong ancestry, older snapshots and
  journal barriers still refuse reuse. Concurrent checkpoint tests preserve
  coherent fields, compare/publication fencing, immutable dependencies and
  constant cache charges.

These are read-side changes. Projection format 5 and the reader-27 / topology-66 /
bootstrap-v19 / Lite-enrollment-v7 durable contracts remain unchanged.
