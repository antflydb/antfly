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
The stream-only case verifies a stored generated value and replays rows from
before and after adding a nullable column, with relation metadata for the
schema that encoded each retained row.
The cross-node case kills a data node mid-stream, verifies the confirmed LSN
on a surviving node, and streams a later commit with stock `pg_recvlogical`.
The topology case uses the same slot through a live range split and merge. It
verifies writes to both split participants, confirmed LSN advancement, and
continuation after the merge and serving-node failover. These fixtures use
three metadata nodes and three data nodes on a single host.
Changing replica identity invalidates the old slot binding and returns a
resynchronization error before streaming. Recreate the publication and use a
fresh slot name when the publication binding changes. Dropped names remain
reserved by the durable slot record.
Publications advertise `pubtruncate=false` and reject requests to publish
TRUNCATE. Where Antfly SQL TRUNCATE is supported, it replaces the table
generation and invalidates the previous binding.
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

For cross-node connections, configure the same trusted principal issuer and
signing authority on each data node. Connect with username
`__antfly_trusted_principal__` and supply a current signed user token as the
password. The issuer, subject, tenant, and signing authority bind publication
and slot ownership. Rotating the signing authority changes that ownership
binding. Local credentials bind ownership to the authenticated user instance.

## Public HTTP changefeeds

Create an authorized feed with `POST /db/v1/changefeeds` and an
`Idempotency-Key` header. The definition selects tables, `start`, `include`, and an optional lease.
Row changefeeds support native document and relational tables and Antfly-written
Iceberg tables with retained WAL lineage. A subscription can select tables
from both providers. Its cursor tracks each provider's commit frontier.
External Iceberg snapshots and remote Parquet inventories expose publication
activation. Requests for row changefeeds on these read-only bindings fail
with a capability error. `start` accepts `now` and
`snapshot_then_changes`. Use optional per-table selectors to limit row payloads
and match exact scalar values.

```json
{
  "tables": ["orders"],
  "start": "snapshot_then_changes",
  "include": "rows",
  "selectors": [{
    "table": "orders",
    "fields": ["status", "total"],
    "predicates": [{"field": "region", "value": "eu"}]
  }]
}
```

A selector table name matches an entry in `tables`. Each selector supports up to
64 top-level fields and 16 exact predicates. All predicates must match. Field
selection applies to the payload after authorization and predicate matching.
The feed evaluates both historical row images. Committed row events include
inserts, updates, and deletes. Predicated feeds also report `enter` and `leave`
when an update changes whether the row matches. Delivery is at least once. Use `event_id` for
deduplication and the transaction completion marker before applying a whole
transaction.

Use `events: ["artifact_published"]` for native artifact publications and
`events: ["publication_activated"]` for coordinated activation on external
lake tables. Each starts at `now` and requires table-wide replication
permission. These event feeds omit row images and selectors. Artifact events
carry the producer generation, configuration digest, and source coverage digest.
Activation events carry the committed publication digest and generation.
The native artifact journal keeps the most recent 4,096 publications per
physical root. The lake activation journal keeps up to 256 activations per
table. A cursor behind retained history requires resynchronization.

Read `GET /db/v1/changefeeds/{id}/events?after={cursor}` and advance using
`Antfly-Changefeed-Cursor`. Acknowledge successfully applied pages with
`POST /db/v1/changefeeds/{id}/ack` and a JSON `cursor`. Get the acknowledged
cursor, current phase, outstanding delivery count and expiry with
`GET /db/v1/changefeeds/{id}`. The outstanding count measures deliveries
already issued to the client. Delete the feed to release its retention grants.

Extend a streaming feed with `POST /db/v1/changefeeds/{id}/renew` and a
JSON `expires_at` Unix timestamp in seconds. A successful reply confirms the
expiry at every root. The metadata worker retries an interrupted renewal.
Repeating the requested timestamp is safe and preserves the acknowledged cursor. Bootstrap deadlines stay
fixed. Active pgoutput sessions renew their one-hour history lease before
it expires. Disconnected slots remain subject to their recorded expiry.

Expired or invalidated cuts require resynchronization. A native partition
split or merge can invalidate an existing cut. Each read rechecks current
authorization.

## Retained HTTP feed benchmark

From the `zig` directory, run the isolated fixture against a local binary.

```sh
uv run --project e2e/antfly python e2e/antfly/bench_changefeed.py zig-out/bin/antfly --rows 32 --batch-size 8 --fanout 0 2 4
```

A macOS arm64 debug run with one metadata node, one data node, and one table
root gave the following results. At each nonzero fan-out, one consumer stayed
offline until both servers restarted. The other consumers pulled and
acknowledged each batch concurrently using independent connections. Lag
includes the pull and durable acknowledgement. Replica growth measures the
whole replica directory. The no-feed baseline provides a comparison for the
retention cost.

| Consumers | Write rows/s | Write and deliver rows/s | Active consumer events/s | Median batch ACK lag | Replica growth | Restart | Offline catch-up |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 38.6 | 38.6 | 0 | — | 100 KiB | — | — |
| 2 | 38.5 | 1.91 | 1.91 | 3.97 s | 304 KiB | 1.68 s | 16.16 s |
| 4 | 38.9 | 1.73 | 5.18 | 4.00 s | 292 KiB | 1.48 s | 16.55 s |

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
explicit metadata-admin reseed path rotates one PostgreSQL source onto a
fresh derived slot and publication and requires exact cutover. On hosted
native storage, the request waits for the old scoped actor to retire and
fence its participating roots. The catalog records the replacement and
retains the old provider authority in the same update. The replacement
worker removes the superseded Antfly-managed resources under the provider
lock.

Hosted native sources that set `require_exact_cutover = true` capture the
exported snapshot in durable pages before applying it. Scoped snapshot
transactions advance the durable snapshot offset one committed page at a
time. The stream begins at the provider checkpoint after the final page.
Public progress retains the original snapshot cut in `prepared_checkpoint`.
The managed-source status reports durable acceptance in
`accepted_stream_checkpoint` and destination commit in `stream_checkpoint`.
Writable Iceberg publishes acceptance after its WAL admits the transaction.
The committed checkpoint advances after the catalog commit. Treat checkpoints
as opaque provider values. A new routed
source can use this path after a destination closes legacy CDC intake.

Use `GET /db/v1/tables/{table}/sources/managed` to read the destination
contract, source configuration hash, and per-source progress. For a managed
configuration change, `POST` to the same route with the table ID,
`expected_sources_hash`, and the replacement `replication_sources` array.
On a table already using scoped CDC, append new definitions after the
existing definitions. The catalog preserves the existing ordinals and
checkpoints. Use the reseed operation to rotate an existing source's
PostgreSQL resources.

Capture is bounded to 4,096 pages and 256 MiB of retained page data per
export. An oversized capture reports backpressure and leaves the streaming
checkpoint unadvanced. Before a capture is sealed, recovery may need to
restart the provider export. Once sealed, recovery replays the retained
pages without reopening the original export.

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
the transaction through their canonical write path. Writable Iceberg
destinations admit it through the lake ingress and resolve the catalog commit
before advancing the durable source checkpoint. On a distributed cluster, the
metadata leader uses the configured shared S3 or GCS artifact lane for that
ingress. Configure the same artifact store on metadata and serving nodes.
Each writable Iceberg table owns one PostgreSQL source cut and checkpoint. A retry uses the
same source epoch, transaction identity, and checkpoint. Unsupported mutation
messages, including inbound TRUNCATE, stop progress before the checkpoint.

An Iceberg bootstrap records its staged checkpoint under the source ordinal
and cutover authority before uploading pages. When a replacement cutover
retires that authority, the destination marks it retired before sweeping its
staging objects. The table maintenance worker revisits retired authorities
with a durable cursor to collect uploads that arrived after the initial sweep.

Each Antfly Iceberg row commit records the exact WAL interval in its catalog
snapshot. WAL garbage collection preserves groups inside the maintenance
retention window and honors the configured minimum count of committed groups.
Active changefeed pins and index readers can hold the collection floor back
further. An Iceberg changefeed advances its pin after acknowledgment and
can renew it while streaming. If a server stops after persisting an
acknowledgment, maintenance advances the pin from the durable cursor.
Managed vacuum preserves the snapshot lineage
and historical row images needed by retained groups.

A native destination can separately expose retained committed history through
the public changefeed API and eligible relational tables through pgoutput.
For a native source with a fenced scope, the hosted metadata worker verifies
the PostgreSQL provider identity and stages each complete source transaction as
a durable reservation and immutable plan. The worker recovers the original
plan after interruptions and acknowledges PostgreSQL through the committed
frontier. Managed native streams use the compatibility writer until their
source is handed over to scoped admission. An interruption between a
compatibility write and its source checkpoint can replay that write.
Writable Iceberg reports its ingress and catalog commit capability. External
read-only Iceberg attachments report committed snapshots as their change
authority. Index publication is a separate milestone across these contracts.
The managed-source status reports the durable published generation and its
base snapshot alongside pending or failed index generations. Its
`destination_contract` reports transaction scope, commit authority, and the
outbound event and bootstrap capabilities available on that runtime.
`cdc_supported` reports whether the runtime can run an inbound connector
for that destination.
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
