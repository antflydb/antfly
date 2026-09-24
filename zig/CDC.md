# CDC

This document describes the Postgres replication-source / CDC subsystem in
`antfly-zig`.

It is intentionally narrower than query-time `foreign_sources`.
`foreign_sources` lets a query read or join against external Postgres tables at
request time. CDC is a control-plane workflow that ingests external changes into
canonical Antfly table storage.

## Metadata substrate

- Public create-table parsing accepts `replication_sources`.
- The canonical `TableRecord` metadata persists `replication_sources_json`,
  and metadata raft/apply storage round-trips that table field.
- Table metadata/status routes expose the raw `replication_sources` payload.
- Metadata status counts `projected_tables_with_replication_sources` and
  `projected_replication_sources`.
- Metadata persists explicit replication-source status/progress records with:
  source ordinal, source kind, external table name, slot/publication
  identity, phase, display checkpoint, structured `snapshot_offset`,
  structured `stream_checkpoint`, lag/error fields, `cutover_mode`,
  `failure_class`, `consecutive_failures`, `last_source_commit_at_ms`,
  `last_success_at_ms`, `last_change_applied_at_ms`, and last update time.
- Metadata status/admin surfaces expose `projected_replication_source_statuses`
  and projected `replication_source_statuses` records in admin snapshot
  output.
- Shared Postgres query/aggregate substrate lives in
  `pkg/antfly/src/foreign`.

## Snapshot and backfill

A snapshot/backfill runner handles simple Postgres sources. It resolves the
configured DSN, then prefers a repeatable-read PostgreSQL snapshot session
through the shared foreign-source runtime seam when the source supports it,
falling back to a stable `ORDER BY` derived from simple key templates when a
source does not support a consistent snapshot, so snapshot paging stays
deterministic for common `id`-style sources. It writes rows through the
normal Antfly batch-write path; in the metadata HTTP runtime, it resolves
target shards from the metadata catalog and forwards CDC mutations to
data-store API URLs instead of opening metadata-local replica paths. It
honors source-level `on_update` transforms during snapshot import and
supports routed snapshot fan-out into target tables. It persists `snapshot` /
`cutover_prepared` status plus offset checkpoints.

A metadata-owned snapshot backfill coordinator discovers configured
replication sources from projected table metadata and resumes from stored
`snapshot_offset` checkpoints, preserving an already-established
`prepared_checkpoint` / `cutover_mode` on snapshot resume instead of
drifting the cutover marker forward. It skips sources already marked
`cutover_prepared` or later, and establishes the logical publication/slot
before snapshot import starts, so the later streaming phase does not miss
writes that land after snapshot rows become visible.

For brand-new Postgres sources, the coordinator prefers an exported-snapshot
exact cutover path: create the logical slot with `EXPORT_SNAPSHOT`, import
that snapshot into the repeatable-read backfill transaction, and persist the
slot's consistent point as `prepared_checkpoint`. When an exact exported
snapshot is unavailable, it falls back to the older slot-first handoff with
repeatable-read snapshot import plus idempotent apply semantics. When the
source attaches to an already-existing logical slot, that path is exposed
explicitly as `cutover_mode = "slot_resumed"` instead of being conflated with
fresh fallback slot creation. The coordinator is wired into metadata
service/server `runRound()` on the leader path with a throttled cadence.

## Streaming

A metadata-owned streaming CDC coordinator runs only after snapshot reaches
`cutover_prepared`. It polls an optional foreign-source replication seam with
stored checkpoints, deriving Go-shaped `slot_name` / `publication_name`
defaults when omitted. It applies `insert` / `update` through normal Antfly
document transforms with upsert semantics, and applies `delete` through field
unsets by default, honoring `$delete_document` when explicitly configured. It
supports source-level `on_update` / `on_delete` transform evaluation and
routed fan-out replication with route-local filters, `key_template`,
transforms, and `$delete_document`. It persists `streaming` /
`streaming_failed` status plus structured stream checkpoints, and is wired
into metadata service/server `runRound()` on the leader path with the same
throttled cadence.

## Distributed apply

CDC follows the same ownership split as the Go implementation:

- The metadata leader owns source discovery, snapshot/stream orchestration,
  durable checkpoints, retry classification, and operator-visible status.
- Source reads happen in the CDC runtime under metadata control.
- Target writes are data-plane writes, not metadata-local storage writes.
- In the HTTP/runtime path, CDC uses the metadata admin snapshot to resolve
  placement, merged leader status, healthy stores, and store `api_url`.
- CDC forwards batches through the same group batch API used by normal
  routed table writes, so schema validation, indexing, enrichment,
  transactions, and shard movement semantics stay on the canonical path.
- The CDC router treats the metadata process as non-local for apply
  purposes, even in `standalone`, so the same route is exercised in combined
  and split deployments.
- If placement, leader/store health, or store API URL is missing, CDC
  surfaces a retryable route failure and resumes from the persisted
  checkpoint after metadata/data topology converges.

The important constraint is that metadata owns CDC state, but it does not
become the owner of table shard files. Distributed and standalone mode both
use the metadata snapshot plus data API route for CDC apply; direct local
shard access is only appropriate for explicitly local, non-HTTP test/service
paths.

Per table, Zig supports one or more replication sources, with source config
and phase/status persisted in metadata, backfill and streaming progress
checkpoints, operator-visible status and errors, and deterministic apply into
the normal Antfly write/index/enrichment path.

## Status and observability

The CDC status model tracks:

- Slot/publication identity, persisted per source.
- Snapshot progress in `snapshot_offset`.
- Prepare/cutover state in `prepared_checkpoint`.
- Stream progress in `stream_checkpoint`.
- Failure state classified as `retryable` vs `terminal`.
- Both record-count lag and time-based `lag_millis` derived from source
  commit timestamps when available.
- An `observed` lag rollup derived from `last_source_commit_at_ms` in
  metadata status, so stalled sources remain visible even when no new poll
  result updates `lag_millis`.
- Aggregate per-phase counts plus lag/error rollups for projected
  replication-source statuses, including failure class, failure streak,
  source commit timestamp, and lag-millis maxima.
- The older `checkpoint` field, retained as a display/backcompat summary.

Metadata status/admin also surface reseed guidance: an aggregate
`projected_replication_source_statuses_reseed_recommended` count and
per-source `replication_source_action_hints` in admin snapshot output. An
explicit metadata-admin reseed path handles exact cutover: it rotates one
Postgres replication source onto a fresh derived slot/publication, forces
`require_exact_cutover = true`, and clears the old source status so the next
leader-owned CDC round starts a fresh exact-cutover snapshot on the new slot.

## Testing

Real Postgres CDC E2E coverage exists in `e2e/antfly/test_cdc.py` for:

- Snapshot import.
- Logical-stream insert/update/delete.
- Restart/resume on the unified `standalone` entrypoint with persisted
  `prepared_checkpoint` and `stream_checkpoint`.
- Publication recreation during streaming.
- Terminal visibility for mid-stream logical slot loss.
- Metadata status aggregate for slot-loss failures.
- Metadata status aggregates for `exact_cutover` vs `non_exact_cutover`.
- Opt-in `require_exact_cutover` for Postgres replication sources, so an
  existing-slot fallback can fail terminally instead of silently degrading
  to `slot_resumed`.

## Open work

- Exported-snapshot / exact cutover only covers fresh-slot sync. Existing-slot
  / resume flows preserve the original prepared checkpoint and surface
  themselves as `slot_resumed`, but still rely on non-exact slot-based
  handoff plus normal upsert/delete idempotence. The operator escape hatch
  for that case is explicit reseeding onto a fresh slot/publication pair, not
  automatic mutation of an existing slot.
- Other foreign runtimes (beyond Postgres) still fall back to the non-exact
  cutover path.
