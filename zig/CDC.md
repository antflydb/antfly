# CDC Plan

This document scopes the remaining Postgres replication-source / CDC work for
`antfly-zig`.

It is intentionally narrower than query-time `foreign_sources`.

`foreign_sources` lets a query read or join against external Postgres tables at
request time. CDC is a control-plane workflow that ingests external changes into
canonical Antfly table storage.

## Current State

What already exists:

- public create-table parsing accepts `replication_sources`
- canonical `TableRecord` metadata persists `replication_sources_json`
- metadata raft/apply storage round-trips that table field
- table metadata/status routes already expose the raw
  `replication_sources` payload
- metadata status can now count:
  - `projected_tables_with_replication_sources`
  - `projected_replication_sources`
- metadata now also persists explicit replication-source status/progress records
  with:
  - source ordinal
  - source kind
  - external table name
  - slot/publication identity
  - phase
  - display checkpoint
  - structured `snapshot_offset`
  - structured `stream_checkpoint`
  - lag/error fields
  - `cutover_mode`
  - `failure_class`
  - `consecutive_failures`
  - `last_source_commit_at_ms`
  - `last_success_at_ms`
  - `last_change_applied_at_ms`
  - last update time
- metadata status/admin surfaces now expose:
  - `projected_replication_source_statuses`
  - projected `replication_source_statuses` records in admin snapshot output
- a first snapshot/backfill runner now exists for simple Postgres sources:
  - resolves the configured DSN
  - now prefers a repeatable-read PostgreSQL snapshot session through the
    shared foreign-source runtime seam when the source supports it
  - falls back to a stable `ORDER BY` derived from simple key templates when a
    source does not support a consistent snapshot, so snapshot paging remains
    deterministic for common `id`-style sources
  - writes rows through the normal Antfly batch-write path
  - in the metadata HTTP runtime, resolves target shards from the metadata
    catalog and forwards CDC mutations to data-store API URLs instead of
    opening metadata-local replica paths
  - now also honors source-level `on_update` transforms during snapshot import
  - now also supports routed snapshot fan-out into target tables
  - persists `snapshot` / `cutover_prepared` status plus offset checkpoints
- a first metadata-owned snapshot backfill coordinator now exists:
  - discovers configured replication sources from projected table metadata
  - resumes from stored `snapshot_offset` checkpoints
  - now preserves an already-established `prepared_checkpoint` /
    `cutover_mode` on snapshot resume instead of silently drifting the cutover
    marker forward
  - skips sources already marked `cutover_prepared` or later
  - establishes the logical publication/slot before snapshot import starts, so
    the later streaming phase does not miss writes that land after snapshot
    rows become visible
  - for brand-new Postgres sources, it now prefers an exported-snapshot exact
    cutover path:
    - create the logical slot with `EXPORT_SNAPSHOT`
    - import that snapshot into the repeatable-read backfill transaction
    - persist the slot's consistent point as `prepared_checkpoint`
  - when an exact exported snapshot is unavailable, it falls back to the older
    slot-first handoff with repeatable-read snapshot import plus idempotent
    apply semantics
  - when the source attaches to an already-existing logical slot, that path is
    now exposed explicitly as `cutover_mode = "slot_resumed"` instead of being
    conflated with fresh fallback slot creation
  - is now wired into metadata service/server `runRound()` on the leader path
    with a throttled cadence
- a first metadata-owned streaming CDC coordinator now exists too:
  - only runs after snapshot reaches `cutover_prepared`
  - polls an optional foreign-source replication seam with stored checkpoints
  - derives Go-shaped `slot_name` / `publication_name` defaults when omitted
  - applies `insert` / `update` through normal Antfly document transforms with
    upsert semantics
  - applies `delete` through field unsets by default and honors
    `$delete_document` when explicitly configured
  - now supports source-level `on_update` / `on_delete` transform evaluation
  - now supports routed fan-out replication with route-local filters,
    `key_template`, transforms, and `$delete_document`
  - persists `streaming` / `streaming_failed` status plus structured stream
    checkpoints
  - is also wired into metadata service/server `runRound()` on the leader path
    with the same throttled cadence
- shared Postgres query/aggregate substrate exists in `go/pkg/antfly/src/foreign`
- the CDC status model is now richer:
  - slot/publication identity is persisted per source
  - snapshot progress is kept in `snapshot_offset`
  - prepare/cutover state is kept in `prepared_checkpoint`
  - stream progress is kept in `stream_checkpoint`
  - source statuses now classify failure state as `retryable` vs `terminal`
  - source statuses now carry both record-count lag and time-based
    `lag_millis` derived from source commit timestamps when available
  - metadata status now also derives an `observed` lag rollup from
    `last_source_commit_at_ms`, so stalled sources remain visible even when no
    new poll result updates `lag_millis`
  - metadata status now also reports aggregate per-phase counts plus lag/error
    rollups for projected replication-source statuses, including failure
    class, failure streak, source commit timestamp, and lag-millis maxima
  - the older `checkpoint` field remains as a display/backcompat summary
- real Postgres CDC E2E coverage now exists in `e2e/test_cdc.py` for:
  - snapshot import
  - logical-stream insert/update/delete
  - restart/resume on the unified `swarm` entrypoint with persisted
    `prepared_checkpoint` and `stream_checkpoint`
  - publication recreation during streaming
  - terminal visibility for mid-stream logical slot loss
  - metadata status aggregate for slot-loss failures
  - metadata status aggregates for `exact_cutover` vs `non_exact_cutover`
  - opt-in `require_exact_cutover` for Postgres replication sources, so an
    existing-slot fallback can fail terminally instead of silently degrading to
    `slot_resumed`
  - metadata status/admin now also surface reseed guidance:
    - aggregate `projected_replication_source_statuses_reseed_recommended`
    - per-source `replication_source_action_hints` in admin snapshot output
  - an explicit metadata-admin reseed path for exact cutover:
    - rotates one Postgres replication source onto a fresh derived slot /
      publication
    - forces `require_exact_cutover = true`
    - clears the old source status so the next leader-owned CDC round starts a
      fresh exact-cutover snapshot on the new slot

What does not exist yet:

- exported-snapshot / exact cutover is now only partial:
  - initial Postgres sync on a fresh slot uses exported snapshots
  - existing-slot/resume flows now preserve the original prepared checkpoint
    and surface themselves as `slot_resumed`, but they still rely on non-exact
    slot-based handoff plus normal upsert/delete idempotence
  - the operator escape hatch for that case is now explicit reseeding onto a
    fresh slot/publication pair, not automatic mutation of an existing slot
  - other foreign runtimes still fall back to the non-exact path

## Target Shape

The control plane should own CDC.

Per table, Zig should eventually support:

- one or more replication sources
- source config persisted in metadata
- source phase/status persisted in metadata
- backfill + streaming progress checkpoints
- operator-visible status and errors
- deterministic apply into the normal Antfly write/index/enrichment path

## Distributed Apply Shape

CDC follows the same ownership split as the Go implementation:

- the metadata leader owns source discovery, snapshot/stream orchestration,
  durable checkpoints, retry classification, and operator-visible status
- source reads happen in the CDC runtime under metadata control
- target writes are data-plane writes, not metadata-local storage writes
- in the HTTP/runtime path, CDC uses the metadata admin snapshot to resolve
  placement, merged leader status, healthy stores, and store `api_url`
- CDC forwards batches through the same group batch API used by normal routed
  table writes, so schema validation, indexing, enrichment, transactions, and
  shard movement semantics stay on the canonical path
- the CDC router treats the metadata process as non-local for apply purposes,
  even in `swarm`, so the same route is exercised in combined and split
  deployments
- if placement, leader/store health, or store API URL is missing, CDC should
  surface a retryable route failure and resume from the persisted checkpoint
  after metadata/data topology converges

The important constraint is that metadata owns CDC state, but it does not become
the owner of table shard files. Distributed and swarm mode should both use the
metadata snapshot plus data API route for CDC apply; direct local shard access is
only appropriate for explicitly local, non-HTTP test/service paths.

## Execution Order

1. Metadata substrate
- keep `replication_sources` persisted in canonical table metadata
- add operator-visible configured-source counts and source status records
- expose source state in admin/status surfaces

2. Snapshot/backfill
- keep the Postgres snapshot reader on the repeatable-read path where
  available, with deterministic ordered fallback for unsupported sources
- map source rows into Antfly mutations
- persist snapshot progress and cutover state

3. Streaming
- add a generic streaming/apply runner first
- then implement real Postgres logical replication / WAL tail transport
- persist last applied source position and make replay restart-safe

4. Apply/runtime
- route CDC mutations through the normal table write path
- keep schema/index/enrichment behavior honest
- expose lag, health, and last-error status

5. Coverage
- unit tests for config, checkpoints, mapping, and status
- integration tests for snapshot + streaming + restart/resume
- parity/e2e tests against a real Postgres instance

## Near-Term Tasks

- [x] Persist `replication_sources` in canonical table metadata
- [x] Round-trip that metadata through raft/apply storage
- [x] Expose raw `replication_sources` in table metadata/status routes
- [x] Expose configured-source counts in metadata status
- [x] Add explicit replication-source status/progress records to metadata
- [x] Add admin snapshot visibility for source status/progress
- [x] Add a first passthrough Postgres snapshot/backfill runner
- [x] Add a first metadata-owned snapshot backfill coordinator
- [x] Wire snapshot/backfill orchestration into metadata runtime/server execution
- [x] Add a first metadata-owned streaming/apply coordinator
- [x] Add transform-aware snapshot import and routed CDC fan-out on the
  stateful metadata-owned path
- [x] Build the first real Postgres logical replication tail transport
- [x] Add richer durable CDC checkpointing
- [x] Add end-to-end CDC tests
- [x] Add restart/resume CDC E2E coverage on the unified stateful `swarm` path
