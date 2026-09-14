# Shared document and relational restore architecture

One restore engine serves document, relational, and mixed-table backups. Restore
targets new, isolated generations for a dependency-complete set of tables. It
must not modify live generations in place or publish independently restored
children and parents.

Jobs, manifests, artifact storage, admission, staging, placement, checkpoints,
cancellation, publication, progress, and recovery are shared machinery. Storage
mode selects row decoding and index reconstruction; relational constraints add
dependency closure and UNIQUE/FK validation to that pipeline. Document-only
restores must not pay for redundant relational scans or claim generation.

The existing document backup attempt/lease, restore job/attempt, and artifact
publication protocols remain the foundation. Private staging reservations are
owned by that restore job/attempt, not by another scheduler or public job API.
Staging is a general lifecycle improvement, not a parallel relational product.
Native and portable cohort document, relational, and mixed restores use this same driver.
Legacy independent portable document artifacts retain their existing admission
path; they do not acquire a cross-table consistency guarantee retroactively.

During a rolling upgrade, shared cohort/staging commands and constraint
retirement table records require metadata decoder protocol v7 on every voter,
outgoing voter, learner, and configured metadata peer. Admission probes before
catalog serialization; final single/batched Raft proposals consume the cached
proof and recheck its exact term, incarnation, and membership under the append
lock. Unknown lifecycle commands are never proposed to older members. Ordinary
document metadata and legacy backup/job entries keep their existing protocol
requirements. Once new-format entries are committed, rolling back a member to
a binary that cannot decode those entries is not supported.

Keep the shared `fail_if_exists`, `skip_if_exists`, and `overwrite` policies.
Explicit overwrite means staging a fresh generation and replacing the exact
observed old generation at publication, never dropping it before validation.
For relational or mixed sets, skip/overwrite must also preserve dependency
closure; an existing same-named parent is not automatically a valid substitute
for the parent generation in the backup. Overwrite reserves fresh hidden IDs and
binds the exact old table/range descriptors. After target validation, it freezes
and drains the old generations before an atomic metadata generation-CAS swap.

The reference guarantees come from:

- [Spanner restore](https://docs.cloud.google.com/spanner/docs/backup/restore-backup-overview): a new database is unavailable during creation; availability and later physical optimization are separate milestones.
- [CockroachDB RESTORE](https://docs.cockroachlabs.com/docs/stable/restore): dependencies must be included in the restored set by default.
- [PostgreSQL pg_restore](https://www.postgresql.org/docs/current/app-pgrestore.html): dependency-aware restoration and optional all-or-nothing execution with `--single-transaction`.

These systems do not share one restore implementation. Antfly adopts the
isolation, consistency, dependency, and publication guarantees, not an assertion
that its current backup protocol already implements their MVCC mechanisms.

## Required lifecycle

1. Authenticate the manifest and capabilities, validate the complete source
   cohort, and construct an explicit source-to-target name/identity mapping.
2. Reserve fresh target names, table IDs, group IDs, and document-identity
   namespaces in a private metadata namespace. Normal catalog/routing reads
   must not see these descriptors.
3. Provision those targets through a job-bound internal view. Stream the
   authenticated source artifacts into them using bounded, resumable import
   pages. Never transplant source UNIQUE/FK claim generations into fresh IDs.
4. Rebuild and validate target UNIQUE/FK ownership across the entire staged
   dependency set. Persist native owner completion receipts bound to the job,
   immutable plan digest, group, and range identity.
5. After all owners report validated completion, publish every target table
   and range in one authoritative metadata transaction. Existing live tables
   remain untouched. An acknowledged import is not publication.
6. Expose any optional post-publication physical optimization separately from
   correctness-critical readiness. Cancellation before publication must clean
   or tombstone every owner before releasing reserved names; delayed work may
   not attach to a later table incarnation.

## Consistent backup cut and write availability

Independent table snapshots do not establish a common transaction cut. The
cohort protocol pins exact source definitions and owners, fences new mutations,
and lets existing transaction decisions drain before capture. Lifecycle locks
also reject schema/index-definition changes and newly introduced dependencies.

The desired production boundary is **durably pin, then export**: retain
restart-stable immutable roots, journal their identities, release the cohort
fences, and upload from those roots. An in-process read snapshot or open WAL
descriptor is not a restart-stable backup handle. Holding all writes fenced
through corpus copying/upload is not the intended online backup contract.

### Restart-stable native pins

Native cohort capture uses a separate disk-backed seal, not process-local
snapshot references. Capability is checked before any source fence is installed.
The filesystem-managed LSM backend seals these operations separately:

1. Seal a generation under the owner fence: persist a checksummed manifest,
   hardlink or retain immutable runs in a job-owned namespace, and rotate/seal
   committed WAL prefixes so no live descriptor is needed to recover them.
   Sync the inventory and directory before acknowledging the handle.
2. Journal every handle, bound to the cohort, owner incarnation, exact fence,
   selected revision, and exact capture replica. A pin acknowledgement is durable ownership, not an
   expiring in-process lease. Only after all handles are journaled may source
   mutation fences be released.
3. Reopen and export the same handles after restart or lost replies, using the
   existing artifact generations, writer fencing, manifests, and cancellation
   protocol. Reopening must never silently capture a newer live generation.
4. Reclaim handles through durable job completion/cancellation ownership. GC
   must honor pending pins after restart and reject delayed work for a canceled
   incarnation. Backends without a restart-stable seal must reject this mode
   before installing any source fences. A lost capture replica cannot be
   replaced with a newer snapshot from the current leader.

Committed WAL-prefix copying has a bounded seal budget; pressure is retryable,
not permission to upload while retaining the write fence. Existing enrichment
and resolver producers must be caught up before freeze admission. Normal owner
progress advances immediately; only transaction-drain waits introduce a delay.
Owner control acknowledgements are currently serialized by the durable cursor,
so pause time still grows with owner count and control-plane round-trip latency.
This is not an owner-count-independent pause-time guarantee.

This machinery is shared by document and typed-row tables. Codec selection and
integrity validation are plug-ins; admission, common-cut proof, artifact upload,
progress, cancellation, and publication are one backup/restore lifecycle.

## Durable metadata and efficiency

The staging implementation stores its immutable plan once. Per-owner receipts
and a small progress record are separate, so an acknowledgement does not rewrite
all target schemas. Replayed owner receipts do not inflate completion counts.
Publication checks every name, table-ID, and group-ID reservation before writing
any public descriptor. Cancellation retains identity tombstones and job receipts.
All these private records are included in metadata Raft snapshots.

The shared `metadata/backup_cohort.zig` journal likewise separates its immutable
source plan from a fixed-size progress record. Normal owner checkpoints do not
rewrite or reparse every schema and range. Source table locks are installed with
the initial plan and released with terminal progress; stale or malformed private
CAS requests cannot revive a finished job or stop Raft apply.

Recovery discovery scans an active-job index rather than all historical backups.
The existing repository writer/cleanup protocol proves that a failed attempt
cannot publish before cancellation reclaims its pins. An absent manifest alone
is not that proof.

Restore row pages, validation passes, and owner prefixes checkpoint within the
existing durable restore job. Retries retain the same target incarnation; a
bounded slice resumes after its verified owner prefix rather than repeatedly
walking completed owners. Definitive integrity failures retain their diagnostic
while cancellation drains/tombstones every planned owner. Cleanup does not need
the source repository to remain available. After metadata publication,
cancellation completes native publication instead of rolling back live names.

Destination grants are sealed into the immutable target plan and checked again
against live authorization on resume. The same principal may restore revoked
permissions and retry. Changing the authorization principal after reservation
requires canceling the old restore and starting a new restore; reauthorization
does not silently replace the identity in an already reserved plan.

Current bounded admission allows at most 128 target tables, 4096 ranges, 4 MiB
of aggregate schema/index JSON, and a 32 MiB encoded private command. These are
explicit admission limits, not an unbounded allocation fallback.

## Streaming source materialization and replica readiness

Each owner binds the source artifact, native manifest, layout, destination
namespace, and immutable plan before accepting bytes. Materialization reads
bounded ranges into an unpublished tree, checkpoints the verified byte prefix
and running checksum durably, and copies only primary/catalog files. Disposable
source projections and source constraint claims are not transplanted. The
normal prepared-row pipeline reconstructs destination rows and indexes.

Finalizing a fully verified source adopts its already durable directory through
the existing generation publisher; it does not hardlink or resync the entire
corpus again. Recovery recognizes both sides of the publication boundary and
can finish from local verified state even if the repository becomes unavailable.

Index readiness is replica-local. A leader's replicated validation receipt
does not prove that a recovering follower has rebuilt its CHECK coverage or
physical indexes. Followers and HA standbys advance bounded reconstruction
slices before accepting that receipt. Catch-up defers apply without advancing
the applied index, rejecting a valid command, or poisoning the replica.

## HA ownership and canceled generations

An authenticated HA begin record carries the immutable hidden-owner descriptor.
The receiver durably records that descriptor before acknowledging replay, so
owners created after its seed remain discoverable in a subsequent seed.
Descriptor/schema inventory is bounded by active owners, not historical jobs.

Cancellation writes a compact terminal proof before the global HA replay
receipt advances. Subsequent seeds stream those proofs in an authenticated
artifact with fixed-size buffers, independently of the active topology. The
proof permits an exact repeated cancellation without reopening a deleted DB,
while rejecting late imports or an unrelated incarnation. Full descriptors are
then retired, and physical roots use the existing replica-retirement journal
and generation cleanup machinery after resident reader/writer leases drain.

Compact terminal proofs are reclaimed behind a durable contiguous HA replay
floor, not an advisory WAL-retention observation. Each bounded maintenance step
first proves that the exact canceled root and registry descriptor were retired,
then atomically deletes eligible proofs and advances its scan cursor. The floor
rejects delayed records before hidden-owner discovery. Seeds carry that floor,
and activation persists an external anti-rollback anchor before publishing
`ACTIVE`, so an older seed cannot resurrect a reclaimed owner on the same PVC.
Interrupted cleanup and reader-held generations remain retryable; they retain
their proofs until physical retirement is established.

## Current implementation boundary

Filesystem standalone uses the same indexed metadata transactions, cohort and
staging records, and replicated restore-job persistence as distributed metadata;
it does not require data Raft or maintain a second restore scheduler. Existing
local restore jobs migrate atomically into that authority. Local hidden-owner
imports pass through the normal durable batch path and the same validation and
publication barriers.

For HA standalone, incremental metadata effects and a durable outbox accompany
catalog, lifecycle, and user-job commits. Seed checkpoints include the complete
private metadata state, not only public table descriptions. Mutation leases are
acquired before the local catalog lock; promotion rehydrates jobs under the new
HA epoch before dispatch. Coordinated native and portable backup publication
pins that primary epoch through capture, writer heartbeats, and repository
publication. Freeze/release/cancel controls share the normal durable HA effects
path. Cancellation releases every replicated write fence before attempting
replica-local pin reclamation, so an unavailable capture node cannot leave live
writes frozen. Pins remain bound to the original capture store: missing pins
never authorize recapturing a newer live cut under the old backup identity.
Pin reclamation is exact-fence/path scoped rather than a live table lookup;
durable filesystem locking and tombstones serialize it with seal/export even
after the original catalog generation is dropped or replaced.

The table backup route is a one-table adapter over this same cohort driver and
publishes both the ordinary table manifest and its certified aggregate commit.
The table restore route submits the same durable staging job with one explicit
selection and fresh-generation overwrite. A table with outgoing foreign-key
dependencies requires cluster restore with the complete dependency set from the
same cohort; a live same-named parent is never substituted. Uncertified
historical independent snapshots still cannot enter the HA restore path.

Large metadata effects use authenticated frames whose complete HA envelope is
at most 1 MiB. The receiver checkpoints contiguous verified chunks in its
indexed metadata store; these uncommitted chunks are included in subsequent
seeds. Final decoding streams one frame/key/value at a time into one atomic
transaction and verifies both the canonical effect checksum and full transfer
digest before publication. A prefix never exposes part of a schema, reservation,
or job update. Duplicate frames are idempotent, and a newer HA epoch may replace
only an uncommitted transfer. This preserves the ordinary bounded HTTP fetch
budget without narrowing legitimate multi-table plan admission.

The existing cluster backup and asynchronous restore handlers now drive the
cohort journal, disk seals, hidden placement, scoped Raft-replicated imports,
global constraint validation, native index readiness, and atomic publication.
Administrative constraint repair, validation retry, and retirement have separate
public APIs. They do not grant public access to staging or private receipts.

The shared-worker regression drives real metadata Raft/apply persistence,
sealed LSM artifacts, hidden document/parent/child targets, distributed UNIQUE/FK
activation, and every publication acknowledgement. Its negative case verifies
that a missing parent publishes no names, cancels all native targets, and reports
failure only after cleanup. A 129-owner regression verifies bounded scheduler
progress beyond one slice and cancellation after metadata publication.
Native seal restart/export and metadata overwrite tests cover their additional
boundaries. Standalone regressions exercise mixed document/relational restore
without data Raft, canceled-owner recovery after restart, and HA-policy-enabled
restore with native owner effects and replayed metadata/user-job publication.
A full multi-node backup-to-restore fault matrix
remains a release-verification requirement; component tests are not a claim that
all distributed failure combinations have been exercised. The shared worker
also exercises one-table native and portable selections without importing
unselected cohort tables. Portable constrained export carries the exact sealed
cohort proof and rebuilds claims in the fresh target; unrelated independent
historical snapshots do not acquire a common-cut guarantee retroactively.

The public standalone HA regression additionally performs an actual HTTP table
backup, changes the live row, then restores through the same replicated user-job
and staged-generation machinery. It verifies a fresh table identity and the
snapshot's original row. Native and portable worker cases preserve active/read
schema mappings and query both indexes after closing and reopening the target.

A debug-build LSM work-count baseline uses 768 total rows across document,
UNIQUE-parent, and FK-child owners. One run measured native 135,818 artifact
bytes, 152 import calls, 10 integrity transactions and 42.6 seconds; portable
183,377 bytes, 206 import calls, 11 integrity transactions and 57.5 seconds.
These are restart-safe driver validation baselines, not optimized throughput
results or a comparative speedup claim. Import pages also have a 5 ms preparation
budget, so calls can contain fewer than the 128-row ceiling, particularly in
debug builds. Benchmark timing includes source materialization and publication.
