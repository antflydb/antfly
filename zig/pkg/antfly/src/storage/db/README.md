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
  - `derived_executor.zig`, `async_runtime.zig`, `io_threaded_runtime.zig`: worker backends and runtime abstraction.
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
