# DB Package

`src/storage/db` is the high-level database orchestration layer that sits on top of `DocStore` and the individual index engines.

## Layout

- `db.zig`
  Public DB implementation and lifecycle. This is the main entry point for batch, search, lookup, schema, transaction, and maintenance wiring.
  `DB.open(..., .{})` now defaults to the durable LSM primary backend profile.
- `types.zig`
  Public request/response/config types for the DB surface.
- `document_mapper.zig`
  Parse-once document preprocessing for writes, including special-field extraction like `_edges` and `_embeddings`.
- `mutation_preparation.zig`, `mutation_commit.zig`
  Coalescing, pinned transform snapshots and private prepared physical publications.
- `document_validation_preparation.zig`: request-owned exact-byte validation
  effects, changed-root validation and complete fallback; no public proof flags.
- `document_validation_authority.zig`: lazy root-local key derivation and caching
  for disposable validation attestations, independent of publication rights.
- `document_change.zig`: bounded metadata-only transform plans over authenticated
  manifests; preserves immutable references before one compatibility rendering.
  Complete logical JSON remains the validation/index/replay contract; see
  [BATCH.md](../../../../../BATCH.md) for qualification and publication fences.
- `../document_input.zig`: exact provider observation checks that own their
  digest before verification can retire borrowed input. A private verifier avoids
  allocating another primary/group JSON copy, with typed-row decode fallback.
  Whole-source guards are preserved; the slower native candidate is test-only.
- `../index_source_claim.zig`, `../index_publication.zig`, `../index_source_map.zig`,
  `../index_source_recovery.zig`:
  qualification-only exact afterimage ownership, atomic index publication receipts,
  bounded authenticated source-map pages, snapshot/query leases across reopen,
  and independent history/index retention accounting. `PersistentIndex` exposes
  source-aware publication and capture. A durable primary journal coordinates
  preparation, publication, bounded ACK/cancellation, reader drainage and page
  reclamation across stores. Native indexed query consumers preserve the
  original afterimage through predicates, fusion and final delivery. The existing metadata-maintenance owner now drives bounded recovery and
  wakes on the final publisher/query release; legacy indexes allocate no owner
  state. Ordinary replay production, field-family roots and format rollout remain. See [the consumer-format design](../../../../../../docs/design/antfly-grep.md)
  for activation, replay/transfer and field-generation work still required.
- `../document_commitment.zig`, `../document_payload.zig`: bounded immutable
  `DLC2` field trees retain byte navigation and canonical identities without a
  second serialized root directory. Metadata patches graft unchanged branches;
  scalar-cache adapters avoid disposable metadata reads. Captured indexed sources
  can project their exact historical fields with bounded output, opaque-prefix
  coverage and shared ranges for overlapping selections. Native query frames
  lazily pin primary reads; selected row pages retain real source leases until
  owned delivery, including conditional identities without scalar value reads.
  Ordinary production writer activation remains gated.
- `producer_input_revision.zig`, `scoped_producer_inputs.zig`: closed literal-field
  dependencies from the pinned completion plan, content-derived input revisions,
  live incarnation/reprocess validation and bounded inherited conjunctions.
  Protocol17 adds APF4/APB2/APR3 while preserving legacy wire identities. Native
  acceptance/replay/completion can validate scoped results without source-body
  reads; receiver adoption recomputes revisions under receiver authority. Catalog
  admission persists the ASCH27 reader fence; released activation remains gated.
  Open dependencies retain complete-source checks.
- `document_query.zig`
  Parsed lookup and projection helpers shared by lookup and hydrated search results.
- `lease.zig`
  Shared metadata-backed lease primitive used by enrichment and maintenance workers.
- `ownership.zig`
  Shared worker-ownership state layered on top of leases, including acquisition/loss accounting used by async worker stats.
- `transaction_resolution.zig`
  Small resolver callback interface used by coordinator-side transaction recovery to notify unresolved participants.
- `../transactions.zig`
  Local MVCC/OCC transaction manager used by `DB` for transaction lifecycle, commit-visible versioning, participant metadata, and local recovery of stale/orphaned intents.
- `catalog/`
  Persisted definitions and orchestration metadata.
  - `index_manager.zig`: index registration, reopen, backfill, routing, and shared enrichment planning.
  - `enrichment_catalog.zig`: durable enrichment definitions such as shared chunk and embedding artifacts.
- `derived/`
  Thin change journal, replay workers, and per-index apply state.
  - `change_journal.zig`: ordered thin replay journal keyed by committed
    sequence, including the compact record codec and target-hint filtering
    used by streaming replay.
  - `replay_stream.zig`: storage-level committed-change helpers for append,
    iterate, truncate, and sequence queries.
  - `derived_log.zig`: legacy payload replay log kept only for compatibility/migration.
  - `derived_types.zig`: encoded log payloads and apply-time data structures.
  - `replay_source.zig`: replay-source abstraction over journal or storage-level
    replay enumeration.
  - `derived_worker.zig`: replay/catch-up logic per managed index.

Replay/runtime code now consumes replay rows through `DocStore` methods instead
of reaching into backend-native commit streams. Replay rows live in the primary
store keyspace and are the authoritative DB-level replay surface.

The remaining future step is not more DB-layer replay work; it is collapsing
any remaining backend-native recovery-only differences into the replay-row
surface where that is meaningfully different.
  - `derived_executor.zig`, `runtime_types.zig`, `io_threaded_runtime.zig`: worker backends and runtime abstraction.
  - `apply_state.zig`: per-index applied watermark persistence.
- `enrichment/`
  Generated enrichment pipeline and reusable artifact logic.
  - `embedder.zig`: embedding provider interface and deterministic test embedder.
  - `chunker.zig`: text chunking utilities.
  - `enrichment_runtime.zig`: leased enrichment worker runtime.
  - `enrichment_worker.zig`, `enrichment_state.zig`, `enrichment_lease.zig`, `enrichment_types.zig`: request collection, watermarking, lease ownership, and generated-work types.
  - `ENRICHMENTS.md`: storage-side enrichment format and skip-by-source-hash plan.
- `maintenance/`
  Background maintenance workers that are not index-specific.
  - `ttl_runtime.zig`: TTL cleanup that reclaims expired documents through normal DB delete semantics, optionally under shared lease ownership.
  - `transaction_runtime.zig`: coordinator-side transaction recovery that retries unresolved participants through a notifier callback and only allows finalized metadata cleanup once participants are resolved.
- [BATCH.md](../../../../../BATCH.md)
  Batch coalescing semantics, bulk ingest scope, and dense HBC replay-window
  policy.
- [FULL_TEXT.md](../../../../../FULL_TEXT.md)
  Full-text visibility and merge-maintenance policy. Scheduled merges are
  background maintenance; force-compaction is a separate explicit path.

## Design Rules

- Base documents remain canonical in `DocStore`.
- Derived artifacts live under binary internal artifact keys and can be shared across indexes.
- Public APIs should expose `ArtifactRef` plus an opaque `artifact_id` token, never raw internal artifact keys.
- Expensive generated work is lease-owned and async.
- Lease-owned workers should share the same ownership semantics and observability surface.
- Deterministic index application is sequence-based and replayable from the
  active replay source, which now reads from the primary-store replay stream.
  Older roots are migrated from the compatibility journal into that stream on
  open.
- The final collapse to direct primary replay still requires a durable enumerable
  primary replay stream; today that exists as backend-native committed-change
  streams, not raw primary WAL enumeration everywhere.
- Background maintenance should route through normal DB semantics instead of raw side-channel deletion.
- Transaction recovery should repair local coordinator intents first and only
  delete finalized transaction metadata once all tracked participants have been
  marked resolved.
- Distributed transaction recovery should stay transport-agnostic at the DB
  layer: background workers call a resolver callback and only mutate local
  participant-resolution metadata on successful acknowledgement.

## Exact indexed-source ownership (unreleased)

`../index_source_recovery.zig` coordinates primary claims and physical full-text
publication receipts. Its journals become durable before a separate index store
may publish references. Source-owned managed and forced compaction preserve
indexed afterimages using native merge provenance; split-range pruning forks only
surviving sources. These paths never substitute mutable primary rows.

Catalog removal persists the original physical owner in the existing deletion
outbox. A metadata-only handle closes the owner, retires at most 64 roots per
turn, drains primary claims and source-map pages, and waits for actual native
reader/preparation leases. Owner-scoped registries and retained wake contexts
resume work after lease release without polling a waiting owner. Namespace
finalization revalidates the durable terminal phase after acquiring its lease and
flushes directory deletion before forgetting the primary tombstone. Same-name
admission remains fenced until that cleanup finishes.

The static storage host also supplies a canonical reader-registry broker. Fresh
owner lookups across separately compiled archives use that broker; existing
query leases retain their originating shard. The receive boundary checks method
and private reader layouts before opening a registry, copies the native interface
and retains no caller descriptor storage. Five targeted runtime-owner checks
cover invalid contracts, allocation failures, original-request prefix
compatibility and borrowed filesystem lifetime. Cross-process transport and
independent clone/rebase semantics remain separate work.

Active-owner maintenance can reclaim terminal publication receipts in bounded
windows behind a durable birth/admission epoch fence. It waits for old
preparations, query readers, primary claim/journal cleanup and source-map cleanup,
then rechecks ownership after forcing primary durability. Epoch transitions are
coalesced after 256 terminal receipts; idle owners stop polling. Source-map
preparation retains only its hash directory and uses one bounded page buffer,
revalidating borrowed coordinates during publication. Held native source sessions
can import their exact recipes and missing authenticated immutable values into
another primary, atomically with destination claims/accounting and a recovery
journal. They preserve destination current rows and reuse existing value sets;
distributed transfer orchestration remains separate work. Fresh preparations
use strict transactional claim creation instead of a second document-digest hash
table. Duplicate inputs poison the compound transaction; arbitrary-binding
low-level claim retries retain idempotency. Native query-page caches share their
root binding and retain only compact coordinates, saving 8 KiB of inline cache
storage per source frame. Handoff resolves bounded provenance windows before
reading source pages; fully discarded windows do not load source references.
Single-row capture follows its held lease even after
the current segment binding disappears.
Multi-output handoff shares one primary transaction/admission and forces one
preparation sync, preserving a separate receipt per output. The receipt codec's
64-segment bound does not cap atomic output count. Caller admission and primary
quotas still bound working state. Sorted publication identities replace quadratic
duplicate checks; the 65-output fixture covers retirement and exact-source reads
after primary changes and reopen.

Startup infers the native payload path for an already-admitted document schema
before publishing the core. It reuses the loaded schema and leaves legacy tables
and admitted relational rows on their own paths; restarting an admitted document
table needs no process-local flag to restore cursor/identity/retirement behavior.
This is existing-format recovery, not permission to promote a legacy table.

Source ownership has its own atomic initialization witness: the shared reader
fence alone does not imply that a budget exists. First admission rejects orphan
ownership records; a missing initialized budget fails closed even after the
last claim is reclaimed. A transaction-local preparation shares schema admission
across claims while retaining per-document checks and failure poisoning. Logical
cursors hide private ownership records even without a configured payload reader
and reject unresolved native descriptors instead of exporting them as JSON.
Startup presence checks, key pages and median-key selection avoid value hydration;
key-only logical cursors also handle adapters without native key-only support.

This remains an internal format qualification path. Ordinary mutation/outbox and
field generations, distributed cross-primary transfer/import,
and reader-cohort rollout are not activated. Native query-driver integration is
implemented; the current publication/recovery and claim/map suites pass 47 and
28 checks on macOS and the constrained ARM64 Linux VM, without leaks. Public
update/reopen checks pass 4 native checks (27 including the Linux artifact's
module-root checks); payload checks pass 36 with one optional benchmark skipped.
Large maps use a fixed 104-byte manifest and streamed authenticated directories;
small maps retain their existing flat-directory lookup. Large read-view open
allocates no directory and reclamation advances at most 128 leaf/node deletes
per transaction. The 65,537-document fixture covers boundaries, borrowed-input
rejection, aborted collection and complete directory cleanup. Query/transport contracts pass
157 native checks; logical-field checks also qualify the bounded selector cache. The
released writer remains ASCH25. Details and current limits are in
[the grep design](../../../../../../docs/design/antfly-grep.md).

The family metadata ownership primitive is `storage/immutable_graph.zig`.
Content-addressed nodes retain direct children; root CAS archives old edges until
native retirement and reader drain. Reserved retirement/queue space and durable
bounded collection preserve accounting across quota failures and restart. Family
roots cannot be children. `ISP3` distinguishes family receipts from segment/map
receipts, and receipt GC retains decisions still needed by archived graph edges.
Path replacement copies only affected ancestors; no-op replacement leaves
accounting unchanged, and shared descendants receive no duplicate edge charges.
Held metadata navigation borrows the actual family lease; descriptors and JSON
cannot substitute for it. The native source-family publisher and cohort reader
are implemented in
`storage/index_source_family.zig` and `storage/persistent.zig`, with bounded
source-only replacement, query-page routing and cross-store reclamation. Field
families, source-only mutation reuse, distributed activation and the production
rollout gate remain. Qualified ASCH28 JSON replay now captures a native read cut,
prepares exact-source families before the apply guard, releases heap encoder
output and publishes the segment and source root atomically. Prepared families
retain their cleanup owner through catalog invalidation; their claims do not
borrow the collector after preparation. Typed/artifact sources and the released
ASCH25 writer are not activated by this qualification path. Benchmark runners are
`scripts/bench/source_family.py` for bounded native cohort updates and
`scripts/bench/source_replay.py` for collector/mapper/publication/query phases.


The native source-family primitive also accepts an atomic batch of up to 64
sorted, distinct dirty cohorts. It forces claims once, copies shared directory
ancestors once and installs one reader generation/root CAS. This is not ordinary
mutation eligibility: the consumer still must prove its indexed inputs unchanged.
`source-cohort-batch-bench-build` and `scripts/bench/source_cohort_batch.py` compare
two dirty cohorts against sequential publication on fresh disk stores, verifying
new and held old source bytes. Keep generated evidence under ignored `output/`.
