# Native indexed-WAL physical collection

This is the remaining native reclamation design for issue #1278. It is proposed,
not an implemented or qualified collector. Logical WAL truncation already runs
on native object-store hosts. Physical immutable WAL pages are retained until
the reader and durable-frontier protocol below is implemented and tested.

## Safety boundary and activation

The hosted collector already has durable marks/frontier, an epoch barrier and
bounded observations. Reuse its native authenticated tracing and the existing
Antfly transactional page store for a native collector. Do not add SQLite or
use an in-memory mark set as durable progress.

An ordinary native WAL read currently has no fixed lifetime. A sweep cannot
assume that an old root stops being used after a Cloudflare request timeout.
Manifest leases protect published artifacts; they do not automatically protect
the WAL page tree captured by an unrelated native reader.

Introduce an explicitly selected managed indexed layout, distinct from the
current immutable `antfly-wal:indexed:1` marker. Initially activate it only on a
new, empty prefix. Reject legacy and existing unmanaged indexed prefixes; an
inventory without a marker must reject any preexisting WAL data. Coordinated
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
ID returns the same captured roots and retired epoch. Expose a status operation
and a matching-ID finish CAS. Clear active roots only after durable completion;
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

The earliest deletion time is recorded **after observing the committed barrier**:
local observation time plus the full 30-second reader lifetime and a separately
bounded clock-skew allowance (initially another 30 seconds). A timestamp sampled
before a delayed barrier CAS does not prove a grace period. If clocks cannot meet
the declared bound, stop collection; do not reduce grace to make tests faster.

## Bounded tracing and sweep

Seed the frontier with record, receipt and document roots. Read and authenticate
each page with the same native tree decoder as serving. Persist a node's mark,
pending children and advancement together in one transactional commit. A node
can contain hundreds of child references, so retain an explicit child offset
and commit bounded chunks, for example 60 children, within the existing
512-command ceiling. Never construct the entire reachable set in memory.

Load phase and mark membership from the same captured transactional root. Never
refresh the metadata root, retain a cached `sweep` phase, and interpret an empty
replacement root as a complete mark set. A missing/corrupt mark page is an error.
Durably close tracing before enabling deletion. Interrupted deletes are safe to
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

Attach at most one bounded collector step per namespace to the native maintenance
round after logical pruning. Preserve publication/foreground progress and the
existing work budget. Use bounded local due hints, durable active-job status and
idle backoff so a 25 ms runtime tick does not become 40 remote GC polls/second.
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
The current qualified modules do not contain this proposed collector.
