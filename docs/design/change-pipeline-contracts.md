# Shared contracts for ingestion, commits, and changefeeds

Status: proposed architecture. This document defines the shared boundary between
[inbound sources and destination apply (#1698)](https://github.com/antflydb/antfly/issues/1698)
and [outbound changefeeds (#1639)](https://github.com/antflydb/antfly/issues/1639).
It does not claim these integrations are implemented. Existing runtime contracts
remain authoritative until corresponding implementation and qualification land.

## Organization and ownership

Keep two implementation issues and this shared design; no additional umbrella
issue is required. Record cross-cutting decisions here, connector details in
#1698, and feed/protocol details in #1639.

| Owner | Deliverables | Supplies / depends on |
| --- | --- | --- |
| #1698 | Source lifecycle, capture, transaction admission, destination adapters, snapshot reconciliation, index maintenance and coverage | Supplies final committed effects and source/index coverage; depends on #1639's committed-history capture contract |
| #1639 | Committed-history requirements and implementation, native feeds, bootstrap/resume, consumer retention and authorization, pgoutput | Supplies retained-history and outbound cursor contracts; depends on destination commit hooks and coverage/lifecycle events from #1698 |
| Shared design | Authority, identities, frontiers, recovery, capability matrix, intersection qualification | Both issues review before incompatible interfaces are implemented |

Neither issue waits for the other's entire implementation. In particular, inbound
admission does not depend on completing pgoutput. PostgreSQL logical replication
and qualification with an existing client remain requirements for completing
#1639; splitting the foundation does not reduce that scope.

## Independent choices and public interface

Keep change source, authoritative data owner, storage format, and deployment
independent. PostgreSQL capture can target supported native or writable Iceberg
destinations. An externally owned Iceberg table is a separate ownership mode.
Native storage remains the simplest default; Iceberg is an interoperability choice.

Tables hold storage/base bindings, schemas, and indexes. Durable source definitions
hold a connection, source table, destination, key/column mappings, capture settings,
attached-versus-managed resource ownership, and progress/status. They may initially
remain nested under tables or projected from `replication_sources`. Credentials
belong in connections, catalog authority in Iceberg bindings, and slot/publication
configuration in PostgreSQL sources. Teardown must preserve attached resources.

Application writes retain the batch API. Extract an engine-independent internal
transaction admission contract while preserving `/lake/changes`; a generalized
public push endpoint is a later API decision, not a prerequisite.

## Two change contracts

```text
Application / inbound PostgreSQL CDC
                  |
          Row transaction admission
                  |
         Destination durable apply
          (native or Iceberg)
                  |
          Authoritative commit
             /           \
 Retained committed     Internal index maintenance
 effects/history          and coverage
        |                       |
 Outbound feeds           Index publication
 HTTP / SQL / pgoutput          |
                         Readiness events

External Iceberg catalog
        |
Committed snapshot reconciliation ----> Internal index maintenance
```

These are logical boundaries, not a requirement for one physical WAL or journal.
Internal index/dependency maintenance consumes native contracts directly. Public
feed delivery, consumer lag, and consumer retention do not control its correctness.

### Row transactions

Capture owns source backfill, snapshot/stream cutover, decoding, source transaction
boundaries, and resumable offsets. Admission receives source identity/epoch, stable
batch identity, predecessor/new opaque checkpoints, key definition, and ordered
keyed mutations. Adapters normalize partial images and key changes as required by
the destination. A destination must advertise transaction scope and atomicity;
sharing an envelope does not establish cross-shard or cross-table atomicity.

Destination adapters reuse canonical durable writes, validation, transforms, and
recovery. Do not add a second WAL where replicated native storage already provides
the required guarantees. Writable Iceberg reuses its object WAL, writer, catalog
commit, and publication recovery. Source acknowledgment advances only after Antfly
durably owns enough data and progress to recover the accepted transaction.

### Committed snapshots

An external Iceberg input identifies the table and authoritative committed snapshot.
Reconciliation resolves schema, inventory, data files, deletes, and source cut;
rows remain in the lake. Notifications are wake-up hints, with periodic catalog
reconciliation repairing missed events. Plain Parquet needs an explicit immutable
manifest for atomic file-set semantics; a prefix listing alone is insufficient.

Do not convert each file event into a row transaction. Stable logical identity and
per-index input fingerprints allow physical rewrites to reuse unaffected indexes
and embeddings. Compaction alone must not manufacture logical row-feed events.

Snapshot following does not promise a complete row changefeed: successive observed
snapshots may omit intermediate history, original transaction boundaries, or old
values. Row-feed support requires a separately qualified reconstruction/history
capability. Never present an inferred diff as original source transactions.

## Commit-to-history boundary

Capture final committed effects below API handlers for every advertised write path:
HTTP, SQL/pgwire, embedded writes, inbound CDC, cascades, and TTL expiry. Aborted
transactions and rolled-back savepoint work produce no committed events. Bind
effects to immutable table incarnation, schema version, stable transaction/event
identity, final coerced/generated values, and required old-key/old-row identity.

#1639 owns the retained representation and history lifecycle; #1698's destination
adapters must populate it at commit. Current row locations or later row lookups
cannot reconstruct historical updates. Preserve required values before updates,
deletes, compaction, or GC discard them. Before/after data must also support the
advertised filtered enter/update/leave/delete semantics.

Define a recoverable atomic relationship between destination commitment and
durable change capture: a committed write cannot be silently missing from retained
history, and an uncommitted intent cannot appear as committed. An implementation
can use an atomic journal or a durable recoverable linkage; best-effort dual writes
are insufficient. Replay must retain identities after lost acknowledgments and
must not turn recovery into a second logical commit.

## Frontiers, visibility, and retention

| Frontier | Meaning | Must not be confused with |
| --- | --- | --- |
| Source checkpoint | Position acknowledged to a particular source/epoch | Destination commit or globally ordered offset |
| Acceptance receipt | Minimum durable admission frontier for a table incarnation | Commit, exact snapshot, or universal transaction ID |
| Committed frontier | Authoritative native commit cut or Iceberg catalog snapshot | Search readiness |
| Index coverage | Source cut and configuration covered by an index generation | Coverage of every other index |
| Native feed cursor | Resumable position in an authorized subscription | Source offset or global scalar order |
| PostgreSQL LSN | Stable position in a defined outbound slot ordering domain | Cast of unrelated shard counters |

Correlate these positions through durable mappings and causal identities; do not
collapse them into one universal offset. Accepted, committed, and published are
separate milestones even where an engine makes some coincide. Read receipts and
bounded waits expose only supported visibility modes. Recent/archive query merges
retain a consistent cut, and per-index status reports the coverage actually served.

Feed history, admission recovery, snapshot/bootstrap pins, and index/cursor pins
have distinct retention obligations. Consumer acknowledgment releases only history
proven safe for that consumer and other owners. Slow consumers obey bounded quotas;
expired cursors require explicit resynchronization, never silent skipping. Feed
retention cannot determine the correctness of internal index maintenance.

Artifact publication events carry the source/configuration generation they cover.
Keep base commit, artifact publication, and coordinated channel activation distinct;
failed publication is not readiness. Lifecycle events use the native envelope unless
an explicitly compatible SQL representation exists, never invented pgoutput types.

## Capability matrix to qualify

This is a target qualification matrix, not a current support announcement. Publish
the actual supported combinations per runtime, schema, and destination.

| Binding | Input and authority | Row feed obligation | Key qualification |
| --- | --- | --- | --- |
| Native local/replicated storage | Row writes; native commit authority | Final committed effects for supported paths | Transaction scope, journal/history linkage, topology changes, bootstrap |
| Native object storage | Row writes; native durable admission/commit rules | Requires explicit commit definition and retained effects | Acceptance versus commit, recovery, published/recent visibility |
| Antfly-written Iceberg | Row writes; managed or external catalog authority | Link each supported committed mutation to retained effects | Catalog conflicts/unknown outcomes, commit before publication, historical values |
| Externally managed Iceberg | Committed snapshots; external catalog authority | Snapshot refresh alone does not imply row CDC | Missed snapshots, deletes, physical rewrites, history availability, concurrent external writers |
| Remote Parquet | Immutable inventory/manifest; producer authority | No automatic row-feed promise | File-set atomicity, object versions, logical identity |

Reject unsupported transaction, bootstrap, replica-identity, visibility, or protocol
requests explicitly. In particular, enabling a writable Iceberg adapter does not
by itself prove complete feed coverage for mutations made by other catalog writers.

## Failure and recovery invariants

- Stable admission and event identities resolve retries and lost responses without
  duplicate logical effects; at-least-once delivery still requires consumer dedup.
- Acknowledged source progress implies recoverable ownership, not index readiness.
- Destination commit and retained effects recover together; unresolved cross-shard
  work never appears as a completed transaction.
- Catalog commit followed by index publication failure resumes publication without
  replaying the transaction as a new write.
- Ownership fences and table incarnations prevent stale workers or recreated tables
  from inheriting queues, publications, subscriptions, or history accidentally.
- Bootstrap snapshot and continuation frontier share one consistent cut, with
  bounded pins and explicit expiry. A live unfenced scan is insufficient.
- Outbound authorization, projection, and revocation remain #1639 requirements;
  internal history does not grant consumers permission to read its contents.

## Milestones and intersection qualification

1. Both issues agree on this contract and fill in the capability matrix, transaction
   scope, commit-history linkage, and identity/frontier mappings before incompatible
   interfaces land. Definition of per-index coverage belongs here too.
2. Implement and qualify the commit-to-history boundary: retries, rollback/savepoints,
   crash windows, stale ownership, and table recreation.
3. Proceed independently: #1698 connects source capture, destination apply, snapshot
   reconciliation, and index coverage; #1639 implements retained history consumers,
   bootstrap/resume, authorization/retention, and actual pgoutput compatibility.
4. Qualify the intersection: inbound PostgreSQL transaction -> destination commit ->
   outbound feed, with independently advancing index publication. Exercise lost
   acknowledgments, restart, catalog commit/publication failure, deletes/key changes,
   schema binding, and slow consumers. Add external snapshot tests proving physical
   rewrites do not become logical updates and unsupported row feeds fail explicitly.

Existing-client pgoutput qualification stays in #1639. Connector cutover and source
acknowledgment qualification stay in #1698. Neither issue is complete merely because
the shared interface exists.

## Related contracts

- [PostgreSQL CDC](../../zig/CDC.md)
- [Lake ingestion and publication](../plans/lake-ingestion-and-publication.md)
- [Lake row identity](../plans/lake-row-identity.md)
- [Composed sources and visibility](../plans/composed-query-sources.md)
- [Read requirements](../read-requirements.md)
