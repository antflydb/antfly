# Shared document and relational restore architecture

One restore engine serves document, relational, and mixed-table backups. Restore
targets new, isolated generations for a dependency-complete set of tables. It
must not modify live generations in place or publish independently restored
children and parents.

### Stored-generated rewrites: shared lifecycle, distinct row contract

An explicit stored-generated rewrite extends this lifecycle without weakening
ordinary ALTER or creating another scheduler. It is not an ordinary overwrite
restore: overwrite deliberately replaces a live table with a historical cut,
whereas a rewrite must retain every acknowledged mutation through cutover.
The explicit schema route (`?rewrite=true`) uses these shared contracts:

1. Bind the versioned rewrite intent to the existing durable job admission.
   The shared staging plan has an explicit target rewrite intent, preallocated
   source scopes, and durable `preparing_sources` reservation. Drafts lock the
   source definitions but do not provision target owners. `freeze_rewrite`
   accepts certified source artifacts only under the unchanged draft identity;
   cancellation requires source tombstones, not nonexistent target receipts.
   A single metadata transaction admits the compact restore job and complete
   draft before the worker sends any source admission. Source artifact receipts
   are compact point records, not repeated whole-plan writes.
   Authenticate the original cohort/source definitions unchanged, and separately
   bind the target public schema, immutable physical layout, scalar-program
   semantics version, and complete dependent table set into the plan digest.
   Ordinary `cohortSource`/`buildPlan` intentionally derive the target definition
   directly from the authenticated source manifest; substituting a target
   schema there would discard a required proof. Rewrite admission uses a separate
   immutable source/target plan while sharing the same staging lifecycle.
2. Extend the shared owner page operation with an explicit row operation:
   ordinary restore preserves the source row's historical logical values;
   rewrite reads those values under their source epoch, evaluates the pinned
   target expression/default plan, then prepares checks, indexes, typed bytes,
   and semantic hash once. Target row effects, source cursor, progress digest,
   and hot-standby outbox must commit atomically. Retries replay prepared effects
   under that exact target digest, never reevaluate a newly active schema.
   A schema-version number alone is insufficient when source and target layouts
   differ: both registries and their identities must remain independently bound.
3. Capture an immutable source cut through the existing cohort pin protocol,
   but also durably retain committed logical effects after that cut for each
   selected source owner. Register retention before releasing the capture
   fence. Rewrite tail pages must transform the retained mutations using the
   same target plan and checkpoint a contiguous source sequence. The existing
   backup pin retains a cut, not the subsequent mutation stream; the overwrite
   fence/drain step alone cannot recover intervening acknowledged writes.
4. Reuse hidden target placement, replica-local index/CHECK readiness, and
   distributed UNIQUE/FK activation against the transformed dependency set.
   After tail convergence, briefly fence and drain the old owners, prove exact
   final source watermarks, and use the existing all-table generation-CAS
   publication transaction. Cancellation and lost replies use the existing
   staging incarnation, owner tombstones, and post-publication completion rule.
5. Gate the new intent/page semantics through the existing metadata and owner
   capability admission before reservation. Resume requires the same source
   cut, target program, dependent generations, and authorization; it must never
   silently recapture current rows or downgrade to historical overwrite.

The executable row-transform substrate now lives in
`storage/db/relational_row_transform.zig`. A program owns independent immutable
source and target schema views and binds their exact-number canonical public
definitions, physical layouts, expression semantics version, and explicit
defaults/drop policy into its identity. It first verifies the historical row's
physical checksum, logical hash, generated values, and constraints. Then it maps
same-name/same-type cells, computes the target generated DAG, validates the target
row, and returns owned canonical bytes and both source/target integrity values.
Defaults are either deliberately applied to absent fields or left absent; an
explicit NULL is never defaulted. Dropping fields requires explicit permission;
implicit type conversion is rejected. Unrelated wide scalar/vector/blob cells
remain typed borrowed inputs, while root-sensitive schemas retain full-root
validation. Limits are 4 MiB per schema, 4,096 columns, 16 MiB per row (with a
conservative pre-allocation output bound), and the shared scalar work budget.
The source timestamp is preserved. This contract performs no writes and grants
no publication authority: the shared worker supplies the snapshot/tail and atomic
receiver/cutover integration above. A program must be pinned for each historical
source schema identity, not selected solely by a numeric version.

`storage/db/relational_rewrite_staging.zig` connects that transformer to the
shared hidden-owner import transaction. A program set binds every historical
source schema and shares one compiled target epoch across them.
Admission unions the bounded native schema histories from every selected owner,
including epochs no longer named by the active/read pair. Before publishing a
certified source decoder, it checks every artifact epoch's public definition and
physical layout against that admitted program set once; numeric-version equality
alone cannot substitute a different source definition. Snapshot pages
and retained REF3 after-images use the same program; source claim/ref effects
advance the source cursor but are not copied into target constraint generations.
The target commits prepared rows/deletes, original timestamps, partial-frame
continuation, progress digest, Raft marker and standby obligation through the
existing staging batch. A partial frame cannot be acknowledged as complete,
and snapshot completion alone cannot produce a validation receipt: the exact
drained final source cut is required. Ordinary restore rejects rewrite scopes.

Unchanged document dependents use an explicit preservation program: snapshot
and tail JSON bytes and timestamps survive without applying defaults or generated
expressions. They use the same hidden-owner progress and final-cut requirement.

The `rewrite_source` role reuses source admission, immutable pin, publication
certificate, retained journal and cleanup while binding a different hidden
target table identity. Shared private donor reads have an explicit role
allowlist; merge receiver/checkpoint operations remain forbidden. REF3 frames
transfer in bounded 64 KiB chunks from a cached checksum-verified frame and are
assembled in a disposable durable spool under the existing restore source
generation. The shared owner importer validates the immutable program and uses
the same replicated staging CAS for snapshot and transformed tail pages.
The receiving DB retains one immutable, authenticated frame and an ordinal
boundary table across page RPCs. Each durable `(offset, remaining)` cursor is
checked against that table in O(1); consumed prefixes are neither reread nor
rehashed. The cache is limited to the 16 MiB frame ceiling plus boundary metadata,
charged to the shared relational preparation budget, and reclaimable while idle.
Restart, eviction, and corruption always require fresh spool verification; only
replicated progress acknowledges effects. Frame completion, terminal cleanup,
and owner close release the cache. Cache leases use `std.Io.Mutex`.
The work-count regression consumes 4,096 effects in 32 pages from an 8,519,728-byte
frame: one payload read/verification instead of 272,631,296 bytes of per-page
reloads. Warm page access allocates nothing for the frame and boundary lookup.
Separate checks cover both slice-local and aggregate pressure eviction, leased
frame protection, allocation-failure retries, and corrupt persisted spool bytes.

The shared restore worker now drives source publication, bounded authenticated
peer-artifact push, snapshot transformation, retained catchup, all-source
fence/drain, exact final tails, shared validation and atomic cohort publication.
ACK and bounded retained-journal reclamation are separate replicated steps.
Old source retention is released while its final topology write fence remains
in place, before the old metadata route disappears. Cancellation tombstones
all planned sources even if their admission acknowledgement was lost.

Unflagged generated ALTER remains guarded. Explicit rewrite returns the existing
asynchronous restore-job resource; it does not modify the live schema in place.
The complete selected cohort requires administrator authority, checked again
before each mutable scheduling slice. The initial public policy preserves
absent fields and rejects implicit column drops or type conversion. Sources with
independently authored graph/vector/enrichment artifacts remain unsupported;
they are rejected before source admission, not silently omitted.
The source transport uses explicit owner authority: data-Raft owners use their
committed log, while non-Raft standalone rewrite owners use a durable native
source clock. The native clock advances atomically with source controls and
retained row mutations and is carried through hot-standby replay. Owner opens
persist the authority mode and reject incompatible reopen or request modes;
standalone admission also pins the hot-standby generation. Native authority
never invents a Raft term or applied index and does not enable ordinary online
split/merge. Full worker regressions exercise both authority modes, including
historical schemas, namespace aliases, post-pin writes, cancellation and reopen.

Real native worker regressions exercise mixed typed/document owners, immutable
source publication, peer transfer, acknowledged post-pin update/insert/delete,
original timestamps, generated recomputation, lost target replies and reopen,
and atomic metadata publication. A target expression overflow during tail
replay cancels the hidden cohort while retaining the old definitions and
acknowledged rows and restoring ordinary source writes. Shared restore
regressions separately exercise generated
parent/child and document cohorts through the production HTTP client and actual
HTTP listener/owner route, native and portable artifacts, lost acknowledgements
after durable owner effects, owner reopen, partial covering-index readiness, and
invalid-FK cleanup. The transport fault adapter drops an already successful
response; it does not simulate multi-node elections or arbitrary network
partitions. Neither fixture substitutes for a full distributed election and
network-partition matrix of the new rewrite action.

Private restore import admission selects preserved-value preparation in both
the parallel worker and serialized fallback. A newly added default cannot fill
an absent historical column, and a forged stored-generated value is rejected
rather than repaired. Canonical values still undergo target type, CHECK, and
generated-value validation; an incompatible required target column fails.
Ordinary mutations continue to evaluate defaults and generated expressions.

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
physical indexes. Followers and hot standbys advance bounded reconstruction
slices before accepting that receipt. Catch-up defers apply without advancing
the applied index, rejecting a valid command, or poisoning the replica.
The same gate includes relational index build jobs, not only ordinary derived
indexes. Each restore preparation advances at most one relational build page;
validated/published receipts are rechecked after owner reopen and lost replies.
Even an already-applied exact Raft entry or hot-standby LSN must restore local
coverage when its completion scope and phase are still current. This repair
does not replay logical effects or advance receipts; older entries, replaced
scopes, and canceled attempts remain no-ops. Hot-standby duplicate inspection
projects only the fixed-size finish proof, without materializing row payloads.
Cancellation never waits for projection readiness. An explicitly failed target
projection terminates the immutable restore attempt and enters shared cleanup;
resource pressure and generic decoder corruption remain distinct retryable
failures rather than being mislabeled as a successful readiness receipt.

## Hot standby ownership and canceled generations

An authenticated hot standby begin record carries the immutable hidden-owner descriptor.
The receiver durably records that descriptor before acknowledging replay, so
owners created after its seed remain discoverable in a subsequent seed.
Descriptor/schema inventory is bounded by active owners, not historical jobs.

Cancellation writes a compact terminal proof before the global hot standby replay
receipt advances. Subsequent seeds stream those proofs in an authenticated
artifact with fixed-size buffers, independently of the active topology. The
proof permits an exact repeated cancellation without reopening a deleted DB,
while rejecting late imports or an unrelated incarnation. Full descriptors are
then retired, and physical roots use the existing replica-retirement journal
and generation cleanup machinery after resident reader/writer leases drain.

Compact terminal proofs are reclaimed behind a durable contiguous hot standby replay
floor, not an advisory WAL-retention observation. Each bounded maintenance step
first proves that the exact canceled root and registry descriptor were retired,
then atomically deletes eligible proofs and advances its scan cursor. The floor
rejects delayed records before hidden-owner discovery. Seeds carry that floor,
and activation persists an external anti-rollback anchor before publishing
`ACTIVE`, so an older seed cannot resurrect a reclaimed owner on the same PVC.
Interrupted cleanup and reader-held generations remain retryable; they retain
their proofs until physical retirement is established.

## Current implementation boundary

The compiled storage owner owns hidden native databases, source decoding, and
physical validation. Control code exchanges pure versioned contracts and keeps
Raft proposals outside owner leases: prepare a bounded mutation, commit through
the existing replicated path, then observe the durable result. Lookup and scan
contracts preserve typed-row options and exact snapshot digests. Coordinated
TTL transfers bounded owned observations to the existing background job lane;
its native callback never reenters the writer cache.

Source modules use `storage/hot_standby` and the standalone coordination port is
`standalone_hot_standby`. Released compatibility aliases and durable record
identifiers remain unchanged; this is not a storage-format migration.

Filesystem standalone uses the same indexed metadata transactions, cohort and
staging records, and replicated restore-job persistence as distributed metadata;
it does not require data Raft or maintain a second restore scheduler. Existing
local restore jobs migrate atomically into that authority. Local hidden-owner
imports pass through the normal durable batch path and the same validation and
publication barriers.

For standalone hot standby, incremental metadata effects and a durable outbox accompany
catalog, lifecycle, and user-job commits. Seed checkpoints include the complete
private metadata state, not only public table descriptions. Mutation leases are
acquired before the local catalog lock; promotion rehydrates jobs under the new
hot standby epoch before dispatch. Coordinated native and portable backup publication
pins that primary epoch through capture, writer heartbeats, and repository
publication. Freeze/release/cancel controls share the normal durable hot standby effects
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
historical independent snapshots still cannot enter the hot standby restore path.

Large metadata effects use authenticated frames whose complete hot standby envelope is
at most 1 MiB. The receiver checkpoints contiguous verified chunks in its
indexed metadata store; these uncommitted chunks are included in subsequent
seeds. Final decoding streams one frame/key/value at a time into one atomic
transaction and verifies both the canonical effect checksum and full transfer
digest before publication. A prefix never exposes part of a schema, reservation,
or job update. Duplicate frames are idempotent, and a newer hot standby epoch may replace
only an uncommitted transfer. This preserves the ordinary bounded HTTP fetch
budget without narrowing legitimate multi-table plan admission.

The existing cluster backup and asynchronous restore handlers now drive the
cohort journal, disk seals, hidden placement, scoped Raft-replicated imports,
global constraint validation, native index readiness, and atomic publication.
Administrative constraint repair, validation retry, and retirement have separate
public APIs. They do not grant public access to staging or private receipts.

Distributed owners read staging authority through the existing authenticated
internal metadata transport. Metadata workers dispatch to remote owners through
the same data-bearing router and owned HTTP executor as ordinary table reads
and writes; no separate restore transport is created. Compact progress and receipt requests use point
lookups, not repeated whole-plan decoding, and have a 4 KiB response ceiling;
the immutable plan is fetched separately when needed. A durable plan/node
placement witness and permanent group reservations keep cleanup reads authorized
after live placement removal. These are scoped cluster-service claims, not
cryptographic per-node identities. Old replacement owners use the ordinary
topology fence path; they do not gain access to staged-owner authority.

Each distributed data node keeps its private restore-owner ledger beneath its
own configured replica root. Standalone explicitly selects the metadata root
included in its seed and activation layout. Replica retirement drains compiled
owner leases before changing physical paths; nameless recovery journals still
target one exact positive group ID. Cold opening an already frozen owner can
rehydrate an exactly matching schema without a metadata write, but changed
catalog, participant, or outbox effects must still pass the normal write fence.

Ordinary compiled owners initialize a missing durable key range from their
authenticated catalog descriptor before admitting writes. Existing durable
split/merge ranges take precedence over historical descriptor hints. Older
missing-range roots receive a one-time, cancellation-aware containment check
over primary rows before binding; namespace mismatches and out-of-range rows
fail without changing the range. Portable archives continue to exclude local
routing metadata. Only their unpublished decoder reconstructs the range from
the restore plan, before publication, so native and portable import share the
same strict source/target range checks.

The shared-worker regression drives real metadata Raft/apply persistence,
sealed LSM artifacts, hidden document/parent/child targets, distributed UNIQUE/FK
activation, and every publication acknowledgement. Its negative case verifies
that a missing parent publishes no names, cancels all native targets, and reports
failure only after cleanup. A 129-owner regression verifies bounded scheduler
progress beyond one slice and cancellation after metadata publication.
Native seal restart/export and metadata overwrite tests cover their additional
boundaries. Standalone regressions exercise mixed document/relational restore
without data Raft, canceled-owner recovery after restart, and hot standby-policy-enabled
restore with native owner effects and replayed metadata/user-job publication.

The real six-process regression (three metadata nodes and three data nodes)
passes for both native and portable backups. It restores three-shard document
and relational tables, exercises schema-epoch-zero query/mutation admission and
a composite expression/partial/covering index, kills the metadata coordinator
after durable job admission, and verifies indexed values and versions after a
data-owner restart following publication. This is a specific failover scenario,
not an exhaustive network-fault or mid-import owner-crash matrix.

A full multi-node backup-to-restore fault matrix
remains a release-verification requirement; component tests are not a claim that
all distributed failure combinations have been exercised. The shared worker
also exercises one-table native and portable selections without importing
unselected cohort tables. Portable constrained export carries the exact sealed
cohort proof and rebuilds claims in the fresh target; unrelated independent
historical snapshots do not acquire a common-cut guarantee retroactively.

The public standalone hot-standby regression additionally performs an actual HTTP table
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

### Recovery and activation boundaries

Replica persistence distinguishes a received Raft snapshot from completed native
application. Only the state-machine completion sink advances the durable
completed-applied cursor. Startup places any persisted but incomplete snapshot
in the ordinary bounded apply queue ahead of log replay and read barriers;
snapshot installation retries therefore cannot expose an older native root as
current. Legacy completion records are replayed conservatively, not inferred
from the Raft log cursor. Provider persistence, queue ordering/admission cleanup,
and actual native-root recovery have dedicated regression coverage.

UNIQUE/FK activation consumes cold-scan projections through a distinct typed
projection boundary, not the full-row mutation validator. Selected required
columns, types and NULL rules remain checked; omitted unrelated required columns
are not fabricated or treated as missing user input. Activation retains the
full primary-row digest/version predicate and never writes a projected primary
row. Ordinary writes retain full required-column validation.

Schema-rewrite job status, listing and cancellation use the same immutable
dependency cohort as admission: the caller must currently have administrative
permission on every member. Losing any member's permission removes job access;
unrelated cluster-wide administration is not required. Ordinary cluster restore
jobs retain their cluster-admin requirement. Resumed staged cluster jobs use the
same bounded terminal summary as first-attempt jobs, including committed-table
counts, while single-table jobs preserve their existing response shape.

The dependency-cohort schema-rewrite network regression passes both metadata
coordinator failure and lost accepted publication-reply cases. It writes after
the retained cut, verifies recomputed generated values through all data frontends,
checks the terminal committed-table count, rejects post-publication orphan and
duplicate writes, and executes a new cascading delete against rebuilt references.
