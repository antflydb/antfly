# Native indexed-WAL physical collection

This is the native reclamation protocol implemented for issue #1278. The
collector uses the existing Antfly transactional page engine and immutable log.
It is opt-in for new prefixes. Local fault/allocation, native scale and real
standard-GCS qualification pass after integrating main `d35a1ef33e`. The final
source and evidence boundaries are recorded below.

## Safety boundary and activation

The hosted collector already has durable marks/frontier, an epoch barrier and
bounded observations. Reuse its native authenticated tracing and the existing
Antfly transactional page store for a native collector. Do not add SQLite or
use an in-memory mark set as durable progress.

An ordinary native WAL read currently has no fixed lifetime. A sweep cannot
assume that an old root stops being used after a Cloudflare request timeout.
Manifest leases protect published artifacts; they do not automatically protect
the WAL page tree captured by an unrelated native reader.

The explicitly selected managed layout is `antfly-wal:indexed-managed:1`,
distinct from the immutable `antfly-wal:indexed:1` marker. Initially activate it only on a
new, empty prefix. Reject legacy and existing unmanaged indexed prefixes; an
inventory without a marker rejects both current objects and historical versions
of preexisting WAL data, even when no current names remain. Coordinated
upgrade/export-import is a separate protocol. Never reinterpret the existing
marker as permission to collect its pages.

The managed HEAD persists a fixed maximum root-use lifetime, initially 30
seconds, independently of whether the local collector is enabled. Older readers
must reject that HEAD rather than ignore the policy. Reopening with collection
disabled still enforces the persisted reader contract. No flag downgrades it.
All readers/writers of the prefix must support this contract before use.

Start each native operation's local deadline before its first HEAD request.
After decoding the persisted policy, reject already-expired observations before
fetching any page. Check the original deadline and caller cancellation before
and after every page transport operation and before returning decoded results.
Use a local clock that includes host suspension; ordinary wall-clock rollback
must not extend an operation. Combine this deadline with transport cancellation.
An expired root returns an explicit snapshot-expired result, never partial data.

No page reference may escape the bounded operation into a lazy iterator or
cache for later dereference. Owned decoded records may outlive it. Existing
read-state cache identities are hashes used to select authenticated new reads,
not permission to dereference an expired old tree. Audit every WAL API, pruning,
receipt replay and write-planning path against this distinction.

Writers check the root-use deadline before publishing a HEAD CAS. A stale CAS
after a collection barrier fails its captured revision. Preserve a successful
durable acknowledgement if its HEAD write succeeded: a post-write timeout is
not permission to report that the write did not commit. Existing keyed replay
continues resolving ambiguous acknowledgements.

## One durable active job

Use the existing WAL HEAD `gc_attempt` and captured record, receipt and document
roots as the active-job pointer. Do not introduce an independently published
`GC_CURRENT`: two object publications would create a takeover/stray-job race.

Managed `beginCollection` must refuse a different active job. Retrying the same
ID returns the same captured roots and retired epoch. Status, tracing and finish bind the job ID and retired epoch together; a reused
ID cannot grant an older collector permission to trace or finish a newer job. Clear active roots only after durable completion;
a different job cannot finish or mutate the current job. Reconcile a lost finish
acknowledgement through a fresh HEAD read. Reopening first reads status and
resumes its job; it must never recreate a cached old job after another finished.

Beginning a job increments the write epoch and captures all three roots in one
HEAD CAS. Subsequent acknowledged writes either derive from those roots or
create pages in the new epoch. Sweep only strictly older epochs. Preserve the
receipt tree even when all corresponding records have been logically pruned.

Create a private Antfly transactional metadata namespace below that job ID.
Store phase, frontier position, partial-node continuation, mark membership,
sweep cursor and counters there. Use the existing immutable log's fencing,
recovery and checkpoint protocol; do not invent another mutable database format.
Each job owns its metadata forever until its HEAD pointer has been released.

Deletion requires **60 seconds of local suspend-inclusive elapsed time after
observing the committed barrier**. Restart or takeover waits the entire grace
again. No cross-host wall timestamp authorizes deletion, so this protocol does
not depend on a clock-skew estimate. Clock unavailability or rollback fails
closed. The reader lifetime stays 30 seconds; tests do not shorten either limit.

## Bounded tracing and sweep

Seed the frontier with record, receipt and document roots. Read and authenticate
each page with the same native tree decoder as serving. Persist up to eight
nodes' marks, pending children, advancement and the canonical job row together
in one transactional commit. A bounded pending-mutation overlay resolves reads
against earlier changes in the same step and coalesces repeated keys, including
a frontier entry inserted and consumed within that step. A node
can contain hundreds of child references, so retain an explicit child offset
and commit chunks of at most 60 children within the existing
512-command ceiling. Never construct the entire reachable set in memory.

Load phase and mark membership from the same captured transactional root. Never
refresh the metadata root, retain a cached `sweep` phase, and interpret an empty
replacement root as a complete mark set. A missing/corrupt mark page is an error.
Validate the authenticated metadata-tree row count against the job, seen/mark
counts, remaining frontier and unfinished parent before enabling deletion. A
copied completed phase over a reset mark map fails this check. Durably close
tracing before enabling deletion. Interrupted deletes are safe to
retry, and durable progress must never skip an unprocessed listing suffix.

List only the exact namespace-owned WAL page prefix, in bounded pages. Validate
the full key shape and encoded attempt epoch before considering deletion.
Retain current/recent epochs, reachable keys and unrecognized names. Delete
using a captured typed revision, or an explicitly supported old-version delete.
Do not turn a failed precondition into an unconditional delete.

Versioned buckets need their own contract. Retaining a key conservatively retains
its reachable versions. Deleting mutable HEAD history is not the same operation
as deleting immutable WAL pages. In particular an S3 suspended-versioning
`null` version can be reused; do not delete it based on a stale listing. Provider
retention/soft-delete policies can delay reclaimed storage bytes even after a
successful API delete. Report logical deletion separately from physical savings.

Mark the job complete in its private transactional namespace before releasing
the HEAD pointer. Delete its metadata only when a fresh HEAD no longer pins it.
A superseded collector may encounter a missing closed mark map and must abort;
it may not bootstrap an empty map and continue sweeping. Private uploads from
a stale writer can arrive after a sweep; periodic later passes reclaim those
orphans without claiming that one listing catches every future upload.

## Scheduling and qualification

Attach at most one offered collector step to a native maintenance round after
publication, under the existing namespace work lease and pass cancellation.
Logical pruning wakes the relevant namespace for a subsequent round. Preserve publication/foreground progress and the
existing work budget. Use 64 fixed local namespace hints, durable active-job status, a global 250 ms
step interval and five-minute idle backoff. Advance the namespace cursor only
when offering a slot, avoiding tick/interval aliasing that starves tenants.
Lost local hints cannot lose durable jobs. Writes may notify a scheduler without
adding remote I/O to their acknowledgement path.

Required tests include all of the following:

- An old captured reader, a stalled HEAD response, host suspension, cancellation
  and a late page response cannot use a root after its original deadline.
- A collector cannot activate on an unmanaged populated prefix or be bypassed
  by reopening with a flag disabled.
- Record, receipt and document roots survive writer/collector races, including
  lost barrier/mark/progress/finish acknowledgements and restart/takeover.
- A branch with 193 children resumes inside that node without skipping marks;
  allocation failure never commits a partially advanced frontier.
- Missing mark pages, an old collector after job completion and a reused job ID
  fail closed. No metadata reset permits deletion with an empty mark set.
- Bounded listings/deletes resume after cancellation, preserve typed revision
  conflicts and leave out-of-prefix or unrecognized objects untouched.
- Retained published-reader pins, in-progress publication/reindex roots and
  original operation receipts remain valid after logical pruning and collection.
- A late orphan is retained safely and reclaimed by a later cycle. Qualified
  unversioned/versioned provider populations remain distinct.

Run fault/allocation tests with an explicit deterministic test clock, then a
real-provider reopen/collection/receipt test with actual grace. Rebuild and hash
the resulting native and WASM artifacts before their full scale/ANN/live gate.
Earlier sealed modules predate this collector and are not evidence for its
final artifact. Their reports remain immutable; new evidence lives under
`dist/qualification/native-gc-lab/`.

## Implementation bounds and operating contract

Enable native collection with `--indexed-wal true --wal-gc true` or
`ANTFLY_SERVERLESS_INDEXED_WAL=true` and `ANTFLY_SERVERLESS_WAL_GC=true`. The
managed format can only be selected on a pristine prefix. Disabling the collector
leaves the persisted reader policy enforced. Coordinated legacy migration is
not implemented.

Each step has a 30-second transport/cancellation deadline and handles at most
eight completed trace nodes, one partial branch continuation, or 16 inventory
entries. Native metadata reads batch candidate marks against one captured root,
limit each response to 64 KiB encoded JSON and each value to 16 KiB, and fail the
whole read rather than truncate. The shared transactional API exposes optional
encoded/value limits for other hosts too. At most eight groups of 60 children,
two marks and one frontier deletion, plus the job row, require 505 commands;
the existing 512-command ceiling remains unchanged. Immutable-log checkpoints
still occur every eight commits. The job namespace includes namespace hash,
retired epoch and ID.

Version inventories support opaque GCS cursors and S3 key/version marker tuples.
Reachable immutable keys retain all their versions. Historical HEAD deletion
requires an explicit non-null version whose decoded managed epoch is retired;
mutable `null` HEAD versions and HEAD delete markers are retained. Private job
metadata is reclaimed only after HEAD releases its tuple. Unknown key shapes,
foreign domains and active jobs are retained. Provider retention policies can
prevent immediate physical-byte reclamation.

Local coverage includes 193-child restart continuations, fresh restart grace,
lost delete/finish/barrier acknowledgements, reused IDs, all allocation failures
in initial marking and sweeping, missing authenticated mark pages, reset mark
maps, newer-epoch writes, document/receipt roots, late orphan uploads, historical
activation guards, opaque/tuple/null versions and four-tenant scheduler fairness.
The real-provider opt-in test publishes 512 approximately 1 KiB documents, uses
actual grace, writes after the barrier, collects and cleans job metadata, then
reopens with collection disabled to verify every body and the original receipt.


Transport cancellation polling is distinct from a fallible maintenance
checkpoint. A transport watchdog polls shared request/shutdown flags and cached
terminal failure; it never renews a lease or checks cancellation of its own
separately canceled task. Explicit operation checkpoints still serialize lease
renewals and source-pin checks and preserve their specific error. This shared
runtime contract avoids turning a stopped HTTP watchdog into request-wide
cancellation. Qualification preserves the initial provider failures separately.


## Qualified checkpoint before main integration

Implementation `d10a545001` and scale fixture `71f79b92ff` add the managed
collector and its shared cancellation/read-budget contracts. The native suite
passes 630 tests with eight environment-dependent skips and no failures/leaks;
the later focused scale run passes 27 with two live-provider skips. The native
50,000-record fixture retains and verifies every one of 25,000 records, their
LSNs/timestamps and original batch receipt, while a newer-epoch write remains
visible. It traces 1,965 pages in 884 steps, deletes 6,224 obsolete objects and
peaks at 194 backend operations per step. Its clock is deterministic: these
numbers describe CPU/I/O scale, not real grace or provider latency. Job-end
object counts include the completed job's private metadata; repeated-cycle
cleanup qualification must include the later maintenance slots too.

Real GCS tests use two published generations, assert that ordinary pruning
establishes a retired WAL cut, then wait the actual 60-second collection grace.
They verify all 512 approximately 1 KiB document bodies and the original receipt
after reopening with collection disabled. The final unversioned bucket
`antfly-1278-multi-mvsvdcnq-1f2dda0d` deletes 54 objects; versioned bucket
`antfly-1278-multi-mvsvdc29-ba368fb1` deletes 59 including eligible HEAD history.
Both pass transport/runtime contracts and cleanup with bucket absence verified.
All six disposable GCS buckets created during this phase were removed. Earlier
failed cancellation and missing-retired-cut reports remain preserved.

Evidence is in `examples/cloudflare-serverless/dist/qualification/native-gc-lab/`.
AWS authentication remains unavailable. Mock opaque/tuple/null version tests
are not real S3 qualification, and native GCS success does not add a GCS
JavaScript transport. The live Cloudflare gate remains separate and unpassed.


## Integrated qualification and batched progress

Main `d35a1ef33e` is integrated through `22c815aabb`. The batched collector in
`be56c13991` passes the repeated 50,000-record collection fixture: 1,965 marked
pages, 6,224 obsolete WAL objects deleted, 884 first-cycle and 398 second-cycle
steps. Every retained record, original LSN/timestamp and receipt is checked,
along with writes after the epoch barrier. Private metadata cleanup and a
second collection leave 1,973 and then 1,966 objects, respectively.

Against the integrated per-node-commit baseline, private metadata objects
requiring cleanup fall from 25,109 to 7,397 (70.5% fewer), and peak storage
operations per step fall from 194 to 99 (49.0% fewer). Final object counts are
unchanged. The deterministic clock in this fixture measures work and correctness,
not cloud latency. A lost acknowledgment of the grouped mark transaction is
recovered on takeover; both committed marks remain visible and takeover waits
the complete real grace again.

The final clean-source provider runs at `10eadf1f24` pass for standard GCS with
and without versioning. They exercise the actual 60-second grace, preserve all
512 document bodies and the original receipt, and reopen with collection off.
Unversioned bucket `antfly-1278-multi-mvt0y08h-0c4e699e` collects 54 objects;
versioned bucket `antfly-1278-multi-mvt0xmzx-04773d2d` collects 59. Both reports
preserve the executed native binary, verify fresh managed data in the owned
bucket, and confirm cleanup and bucket absence.

The guarded complete native command passes with 631 primary tests, nine
environment-dependent skips, and no failures or leaks. The separate focused
50,000-record command passes 28 tests with two live-provider skips. Failed
commands caused by a deleted compiler cache remain preserved as failed runs;
they are not substituted for the successful guarded command.

Evidence is under `examples/cloudflare-serverless/dist/qualification/native-gc-batch-lab/`.
Real S3 still requires local AWS credentials. These GCS and local results do
not qualify the live Cloudflare latency or physical cold-start gates.


## Empty logical prefixes and explicit layout version two

The native release HTTP smoke exposed `InvalidObjectKey` before the first
ingest: the version-one indexed implementation concatenates an empty prefix
with `/wal-v1/...`. S3/GCS permit those leading-slash object names, while the
filesystem adapter correctly rejects absolute-looking keys. Trimming the slash
would change authenticated tree domains and hide existing cloud data.

New empty logical prefixes therefore select `antfly-wal:indexed:2` or
`antfly-wal:indexed-managed:2`. Their physical indexed prefix is
`.antfly/indexed-v2`, beneath which existing WAL page and GC-job naming applies.
The immutable selection marker remains `.antfly/WAL_FORMAT`. Both managed and
unmanaged version-two activation require a pristine logical prefix, including
historical versions, before writing the marker. Relative layout two never
adopts an unmarked tree or unrelated data. Nonempty new prefixes continue to
select version one; existing version-one cloud roots retain their original
keys, domains and receipts. An older reader rejects the new marker.

A previously selected version-one filesystem root fails startup with
`LegacyIndexedRootMigrationRequired`; its marker is never rewritten. Use a new
owned prefix or a coordinated migration. Disabling collection on a managed
version-two prefix retains its 30-second reader contract and the same 60-second
grace. No resource limit or grace threshold changes.

The focused regression command passes 31 tests with three environment skips
and no failures/leaks. It covers fresh filesystem HTTP namespace admission,
selection acknowledgment loss, receipt replay, collection/private cleanup under
the relative root, version-one cloud compatibility, filesystem refusal to
reinterpret old markers, and historical activation guards. The initial release
smoke failure remains preserved; the corrected release and full qualification
are being rebuilt separately before claiming that smoke passes.
