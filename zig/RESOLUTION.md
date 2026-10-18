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

Remote policy mutations publish through the source table's authoritative
metadata CAS, in reserved `_resolver_curation` records. Owner reconciliation
installs the policy through the ordinary resolver configuration/backfill path;
public requests never patch individual shard catalogs. Entity fanout preflights
all matching policies and source-table write permissions, then commits one CAS
per source table. It is not a transaction across unrelated source tables. HTTP
207 reports partial acceptance with each resolver's accepted revision; retrying
the same corrections is safe. Native fanout reports atomicity per resolver.
Retired identities accept and retain policy without running resolution or
promotion. Remote replay progress remains pending until an index declares
execution again; waiting never reactivates a retired identity.
Durable deletion markers prevent offline owners from resurrecting dropped
identities; native backup carries both policy and consumed deletion fences.

A mutation returns the accepted revision and replay sequence immediately (HTTP
202 while pending). `replay_sequence_kind` distinguishes a resolver revision
from a source-journal review decision. `wait:{timeout_ms:...}` bounds observation
of detached replay; a timeout leaves the accepted mutation intact. Resolver GET
reports per-group revision, freshness, durable backfill and resolution/promotion
watermarks. An idle watermark alone does not prove the new revision is applied.
If cached/heartbeat status omits resolver diagnostics or lags an accepted revision,
status obtains compact observations from the current mutation owners, with at
most 16 concurrent shard requests. Polling never drains replay on a request
worker. A shard with no runtime completes only with a durable proof of no
primary records and no derived replay sequence; absent observations alone never
prove completion. Native `pending_work_stats` includes these per-resolver revisions under
`resolver_curation`.
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


The entity routes deliberately identify the destination entity table and key,
rather than exposing source-table enumeration to clients. A future namespace-owned
store (entity table plus key template) can replace the current fanout behind
these routes. That change must register durable namespace dependencies during
resolver admission/replacement/retirement, fence resolution artifacts with the
namespace revision, and enqueue dependent source replay from the authoritative
namespace commit. New source resolvers must inherit the current revision before
processing their first document. Migration must coalesce identical legacy
redirects and report conflicting targets before switching ownership. Resolver-local
inline aliases remain configuration; namespace redirects apply after aliases.
This release implements the first step: source-owned durable corrections and the
stable entity API, with explicit per-source acceptance and replay progress.
