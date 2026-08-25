# Progressive embeddings-index publication

## Status

Accepted for implementation. This document defines the public and storage
contract for making a managed embeddings index queryable before its initial
backfill is complete.

## Motivation

Managed embeddings are eventually derived from durable source documents.
Requiring complete source coverage before the first semantic query makes a new
index unavailable for the entire embedding and HBC construction interval. That
is stricter than Antfly's ordinary write contract, where `sync_level=write`
acknowledges durable source data and `sync_level=full_index` additionally waits
for derived indexes.

Initial index construction should follow the same model: a safe published
generation can answer useful queries while the remaining source corpus is
still being materialized. Clients that require globally complete coverage can
continue to wait for it explicitly.

## Contract

Embeddings indexes have a `publication_policy`:

- `progressive` is the default. The active generation becomes queryable after
  it contains at least one vector and its published replay checkpoint is valid.
  It remains `queryable_partial` until generation-scoped source coverage and
  replay are complete.
- `atomic` preserves fail-closed initial construction. A shadow generation is
  built and activated only after the complete-generation invariants pass.

The persisted representation is intentionally migration-free: an embeddings
index whose stored configuration predates `publication_policy` is interpreted
as `progressive`. No catalog rewrite, rebuild, warning, or operator action is
required. An explicit `atomic` value is the only way to request the old
initial-admission behavior.

Publication policy controls availability, not source coverage policy.
`coverage_policy=partial`, for example, describes which source outcomes count
as settled; it does not authorize querying an incomplete generation.

The lifecycle states are:

| State | Queryable | Complete | Meaning |
| --- | --- | --- | --- |
| `pending` | no | no | No safely published generation exists. |
| `queryable_partial` | yes | no | One safe generation is published, but source coverage or replay is incomplete. |
| `ready` | yes | yes | The desired incarnation is fully covered, current, and published. |
| `failed` | no | no | The desired incarnation cannot make automatic progress without corrective action. |

`IndexReadinessStatus` is authoritative and includes `queryable`, `complete`,
an opaque `incarnation`, and target/published revisions. Embeddings status also
continues to expose exact generation-scoped coverage counts. A client must not
infer completeness from the physical vector count.

`antfly index wait` remains a complete-readiness wait for compatibility.
`antfly index wait --queryable` exits for either `queryable_partial` or `ready`.
Long waits belong in the waiter rather than a held search HTTP request.

Search uses the best safely published generation by default. It may therefore
observe eventual index consistency after `sync_level=write`. A write accepted
with `sync_level=full_index` retains its existing stronger guarantee that the
write's fence has passed through configured indexes; it is distinct from
waiting for the initial corpus-wide backfill to complete.

Partial visibility advances only at durable producer and index checkpoints;
an embedding-provider batch is not itself a publication boundary. This matches
the quickstart's index-before-load flow, where separate durable load revisions
become queryable as they catch up. The current bootstrap scan of a corpus that
predates index creation emits one final producer revision, so it cannot expose
its in-memory provider batches progressively yet. Splitting that scan into
bounded durable producer revisions is a follow-up optimization, not a reason to
weaken the generation-safety checks here.

## Generation safety

Progressive publication must obey these invariants:

1. A query reads exactly one generation. Vectors from different models,
   dimensions, chunkers, or configuration incarnations are never mixed.
2. A mutable shadow repair candidate is never queried. Progressive initial
   admission uses the canonical active generation and publishes only durable
   worker checkpoints.
3. A partial generation is admitted only for the managed-admission repair that
   created its current incarnation, only under `publication_policy=progressive`,
   and only after at least one physical entry is visible.
4. The catalog configuration hash, coverage incarnation, projection
   checkpoint, and applied replay cursor must agree. A corrupt or untrusted
   generation remains fail-closed.
5. Replacement rebuilds keep serving the previous trustworthy generation
   until activation. A repair with no trustworthy active generation remains
   unavailable until the progressive-admission proof succeeds.

The durable repair intent is not retired at partial publication. It remains the
owner of completion, restart recovery, and fallback shadow reconstruction.
Only the existing complete-generation proof may retire it.

Dense and sparse indexes use the same generation, coverage, and replay fences,
but their physical publication proofs reflect their storage engines:

- Dense publication additionally requires the active HBC cardinality to match
  its durable artifact target (or produced-source count for non-chunked data)
  and requires no deferred posting repair.
- Sparse derived batches are applied transactionally before their replay cursor
  advances. A partial sparse generation therefore requires a clean matching
  checkpoint and at least one live sparse document. Completion additionally
  requires complete generation-scoped source outcomes and enough live sparse
  documents to cover produced sources; chunking may make the physical count
  larger.

Externally supplied vector fields have no managed producer/coverage authority,
so they do not use the managed partial-admission exception. Their ordinary
write and replay contract remains authoritative.

## Performance and verification

Correctness and latency have separate gates:

- The existing 500-document test continues to require complete coverage,
  idempotent replay, a complete second index, and a stable first index.
- A deterministic partial-publication test pauses production after an initial
  batch and proves `queryable_partial`, successful semantic retrieval,
  generation/coverage metadata, restart safety, and the final transition to
  `ready` with exact cardinality.
- A release-binary benchmark records time to first queryable result separately
  from time to complete. The benchmark, not an unoptimized debug build, owns
  the performance target.

## Compatibility

This contract lands before the v0.2 API and SDK surface is released, so the
OpenAPI enum and required readiness fields are updated in place rather than
adding version negotiation. All generated in-repository clients are regenerated
together. Existing persisted indexes remain compatible through the silent
`progressive` default described above. Existing `index wait` behavior is
unchanged, and explicit `publication_policy=atomic` provides the previous
initial-admission behavior.
