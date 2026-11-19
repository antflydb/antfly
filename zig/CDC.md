# CDC

This document describes inbound PostgreSQL CDC and outbound logical replication in
`antfly-zig`.

It is intentionally narrower than query-time `foreign_sources`.
`foreign_sources` lets a query read or join against external Postgres tables at
request time. CDC is a control-plane workflow that ingests external changes into
canonical Antfly table storage.

The proposed generalization to other database connectors, lake notifications,
authenticated commit hooks, and managed archive publication is described in
[Lake ingestion, change capture, and publication](../docs/plans/lake-ingestion-and-publication.md).
It builds on this subsystem's canonical apply, metadata ownership, and durable
checkpoint contracts; it does not imply those additional source APIs exist today.

## Outbound PostgreSQL logical replication

A native data node can serve a configured Antfly publication over pgwire. The
slot binds to that publication at creation. It retains whole committed
transactions in `pgoutput` format and advances retention after durable client
feedback. A slot can cover multiple native relational tables and up to 128
history roots. Coordinated transactions become visible only after all their
published participants can be emitted under one Begin and Commit. Clients can
start with `NOEXPORT_SNAPSHOT` or create a slot with `EXPORT_SNAPSHOT` and adopt
its initial LSN after importing the coherent cut in a repeatable-read SQL
transaction.

Stock `psql` and `pg_recvlogical` 18.6 use `pgoutput` protocol version 1 in the
end-to-end suite. Single-table and two-table cases cover feedback, restart,
resume, and slot drop. The exported-snapshot cases read pre-cut rows using
`psql` and verify that `pg_recvlogical` receives the post-cut changes. The
two-table cases also exercise transaction rollback and rollback to savepoint.
Run these cases from the `zig` directory.

```sh
ANTFLY_BIN="$PWD/zig-out/bin/antfly" uv run --project e2e/antfly pytest e2e/antfly/test_pgwire_replication.py
```

The stock Debezium 3.3.0.Final connector with pgjdbc 42.7.7 passed initial
snapshot, streaming, graceful reconnect, and crash recovery on a native Antfly
table. The qualified deployment used one metadata node, one data node, and
a dedicated local spool. The qualification driver and exact deployment details
are recorded in the [Debezium README](e2e/antfly/clients/debezium/README.md).

Provision a dedicated mounted directory for the slot spool. Every data node
serving the publication or reclaiming its spool must see the same files and
fencing inventory. The filesystem must provide cross-node advisory locking,
atomic rename, and durable file and directory synchronization. Keep the volume
mounted at the configured path while these nodes are active.

```sh
antfly storage provision-pgwire-spool /srv/antfly/pgwire-spool
```

Use the returned namespace in each participating data node's config.

```json
{
  "pgwire": {
    "enabled": true,
    "replication_publication": "changes",
    "replication_spool": {
      "directory": "/srv/antfly/pgwire-spool",
      "namespace": "<64 hexadecimal digits returned by provisioning>"
    }
  }
}
```

Keep the namespace and the fencing inventory with the spool volume. At startup,
a missing or different spool incarnation prevents the node from serving that
configuration. A replacement spool requires resynchronizing consumers that were
bound to the original incarnation.

## Retained HTTP feed benchmark

From the `zig` directory, run the isolated fixture against a local binary.

```sh
uv run --project e2e/antfly python e2e/antfly/bench_changefeed.py zig-out/bin/antfly --rows 32 --batch-size 8 --fanout 0 2 4
```

A macOS arm64 debug run with one metadata node, one data node, and one table
root gave the following results. At each nonzero fan-out, one consumer stayed
offline until both servers restarted. Active consumers pulled and acknowledged
each batch serially. Lag includes the pull and durable acknowledgement. The
replica allocation column measures whole replica directory growth during the
workload. The no-feed baseline provides a comparison for the retention cost.

| Consumers | Write rows/s | Write and deliver rows/s | Median batch ACK lag | Replica growth | Restart | Offline catch-up |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 38.3 | 38.3 | — | 104 KiB | — | — |
| 2 | 38.7 | 2.11 | 3.58 s | 312 KiB | 1.40 s | 14.39 s |
| 4 | 38.9 | 0.73 | 7.20 s | 352 KiB | 1.64 s | 14.34 s |

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

## Provider-neutral lake transaction ingress

The complete row-transaction envelope lives in
`pkg/antfly-embedded/src/storage/source_transaction.zig`, independently of
Iceberg. The Iceberg row writer aliases that type and continues to own typed
schema validation. Its retry identity retains the existing source/epoch/batch
hash, so extracting the contract does not change durable queue identities.

PostgreSQL poll results expose complete committed transactions. All rows in
one transaction share an opaque source checkpoint. Native destinations admit
the transaction through their canonical write path. Standalone writable
Iceberg destinations admit it through the lake ingress and resolve the
catalog commit before advancing the durable source checkpoint. A retry uses the
same source epoch, transaction identity, and checkpoint. Unsupported mutation
messages, including inbound TRUNCATE, stop progress before the checkpoint.

A native destination can separately expose retained committed history through
the public changefeed API and eligible relational tables through pgoutput.
Writable Iceberg reports its ingress and catalog commit capability. External
read-only Iceberg attachments report committed snapshots as their change
authority. Index publication is a separate milestone across these contracts.
See [destination integration](https://github.com/antflydb/antfly/issues/1698)
and [committed history and changefeeds](https://github.com/antflydb/antfly/issues/1639).

Writable native Iceberg tables also accept complete provider-neutral CDC
transactions at `POST /db/v1/tables/{tableName}/lake/changes`. This push boundary
uses stable source epochs, batch IDs, key definitions, and predecessor
checkpoints. It durably owns a transaction in a segmented object-store WAL
before acknowledging it, then drains native Parquet and Iceberg delete/manifests
through either configured catalog authority. Catalog commitment and searchable
index publication are distinct visibility stages. This does not replace the
metadata-owned PostgreSQL snapshot/slot coordinator, provide automatic vendor
subscription setup, or make recent uncommitted rows queryable.

The exact envelope, recovery contracts, configuration and limitations are in
[Native transaction ingestion and searchable publication](../docs/plans/lake-ingestion-and-publication.md#native-transaction-ingestion-and-searchable-publication).
