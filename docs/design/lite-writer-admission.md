# Lite writer admission

The data-path sidecar flock remains the authority that excludes incompatible
database operations. A crash-safe coordination queue orders cooperating writers
before they seek that flock. This separates turn ordering from storage safety and
preserves exclusion when a binary using the older flock-only protocol is present.
Fairness applies to callers participating in the queue.

## Queue protocol

The queue uses main's crash-safe writer ticket protocol. A bounded-length
coordination directory beside the canonical safety-lock path contains a checked
monotonic tail counter and one flock-protected file per live ticket. Enrollment
locks the ticket before publishing it and advances the tail under its own lock.
An uncontended writer can skip ticket creation while holding the tail fence.

A writer waits on its predecessor. Granted/done predecessors establish a turn;
abandoned or dead waiters are skipped only while holding their unlocked ticket.
A missing file is safe because a later grant proved all earlier tickets finished.
Ticket numbers are never reused. Granted holders reclaim prior ticket files, and
counter recovery cannot assign a number below a live ticket after an interrupted
write. Cancellation and process death release kernel ownership.

The safety flock closes before the queue turn is released. Legacy callers and
primitive native opens still use that safety flock, preserving exclusion while
only participating writers receive FIFO ordering. Readers retain their shared
safety lock and inode-fenced fallback. Namespace construction and read-snapshot
alias/replacement protections remain independent of queue ordering.

This branch uses the queue landed in #1479; it does not add another queue around
native opens or retain the earlier byte-range-lock implementation.

## Admission budget and lifetime

A configured nonzero busy timeout supplies one remaining budget across the connection
lane, shared process/path lane, path lock, retries, and suspended-call resumes.
Zero retains the established process-local serialization policy and refuses
foreign path waits without sleeping. Active preparation and inference preserve
the unspent admission balance. This is
an admission waiting limit; execution and mandatory cancellation teardown have
their own lifetimes.

The local queue uses caller-owned linked nodes under its condition mutex. A timed
out or cancelled waiter unlinks before returning. This avoids abandoned numeric
tickets, retained cancellation heaps, and borrowed nodes outliving their stacks.
The condition epoch is read before releasing its mutex, preserving wakeups across
the transition into a timed wait.

A successful detached allocator proof may outlast its waiting budget. Admission
must retain that finished proof, allow an uncontended stable-tip acquisition, and
refuse new contention waits after expiry. The completed verification witness is retained as a checked proof receipt before
lease reacquisition and accepted only against its matching checkpoint or checked
append-only lineage. A Busy
refusal therefore cannot force another full walk on a stable tip. No mutable
verifier ledger is shared with borrowed runtimes. Both local handoffs after
verification and reader teardown use the original admission budget; mandatory
teardown still drains under the gate, and a refused handoff cannot start a new
mutation afterward. Runtime and reader references remain owned until that drain
finishes.

## Work under a turn

Fairness bounds the number of earlier turns, not their duration. Replay and
immutable merge preparation run outside path writer admission. Publications and
existing mutation groups retain bounded work. A queued local writer may receive
its predecessor's lease directly. Handoff chains stop after 50 ms, or after
20 ms when another process is queued. An idle gate closes any retained lease;
a timed-out local waiter unlinks before returning, so it cannot strand a lease.
Reader registration batches end at each handoff.

Per-process path statistics separate acquisition duration from closed-lease hold
duration, including maxima and a logarithmic hold histogram. Acquisition includes
queue coordination and kernel waiting. Hold duration ends when the safety flock
closes and includes reader-registration cleanup. These observational counters
survive serving-runtime replacement and expose long turns during qualification.

## Commit outcomes

SQL JSON responses expose `commit_outcome`: `applied`, `not_applied`, or
`unknown`. The existing `mutation_outcome` separately describes derived visibility.
Applied mutations may still have pending derived visibility. A refusal before the
commit decision can report that the request was not applied. Once a decision may
be durable, failures must preserve an unknown outcome and a stable reconciliation
token until recovery establishes its state. Response allocation or visibility
failure cannot turn an applied mutation into an ordinary retryable error.

Every well-formed COMMIT reserves a refusal envelope before admission. Execution
reserves a separate fallback before the decision; an uncertain response includes
its native transaction ID. A committed response that cannot allocate its full
result retains its applied disposition, affected-row count, command tag and empty
row/column arrays. Python (including DB-API), Go, Rust and TypeScript error wrappers
retain the disposition and reconciliation token. Absent fields from legacy or
unstructured failures confer no permission to replay a mutation. Receipt
allocation failure before admission is reported in the allocation-free native
last-error detail; no SQL is executed.

Existing uncertain handles remain fenced even at an unchanged checkpoint. A
fresh writable open skips cached owners with uncertain serving or detached publication state
and reconstructs the durable decision from disk. Old query/runtime references
retain their original ownership until close.

Decode the hexadecimal token into its 16-byte transaction ID and use
`antfly_db_get_transaction_status`, or Python's
`db.transaction_status(bytes.fromhex(token))`, on that fresh writable handle.
A committed status forbids replay; an aborted status permits a new attempt under
the operation's retry policy. Pending status or a failed lookup leaves the
outcome unresolved. A read-only open cannot finish recovery of a pending decision.

Raw batch JSON responses contain request counts, so the complete response is
encoded before mutation. An encoding allocation failure therefore applies no
transform. Success transfers the existing buffer to the caller without any
post-commit response allocation or wire-format change.

Diagnostics preserve the actual failure class alongside this disposition.
Changing an internal error's spelling does not establish whether a mutation was
applied. A `not_applied` disposition does not make every error retryable: failed
SQL sessions still require rollback, and resource/constraint errors retain their
original retry policy.

Generation-control publication uses the same distinction below the API boundary.
The native transaction group captures its checkpoint uncertainty under the store
mutex and delivers that original result to every queued member. Atomic sinks can
then certify a failed publication as unapplied, including admission refusal and
staging cancellation. The control wrapper returns such a failure directly and
posting stores retain their previous generation and prepared output. A genuinely
uncertain publication still retries the identical control record once and then
requires reopen. Sinks without phase information remain conservative; error
names alone never certify that a publication did not occur.

## Replay preparation and runtime ownership

Text replay projects one source-byte-bounded window, including document
ordinals, before producer admission. The plan owns schema-derived field names
and analyzer pipelines; collected document bodies outlive the plan. Segment
preparation consumes that projection without extracting the fields again.
Observations are attributed to each admitted chunk, and index instance or
projection revision changes discard only unpublished work. Projection residency
is charged to the full-text build budget; neither a large journal record nor a
producer-capacity wait creates an unbounded projection cache.

Eligible text-only detached runtimes keep immutable index state across turns.
Each turn independently acquires and releases its primary read epoch. Reuse
requires the same inode, nonzero index identity, namespace bindings, recovered
SQL decisions, and exact schema/index/provider configuration fingerprints.
Before using the new primary cut, replay invalidates source certificates without
reusing their generations, resets replay-head and retention observations, and
reloads shard and identity visibility state. The cache retains at most root plus
one named namespace and expires when source and artifact work settle. Other
index/provider runtimes reopen until they define an equivalent rebinding
contract. Selecting a named namespace uses the detached runtime directly,
avoiding a duplicate serving-table open solely for maintenance discovery.

Relational text tables use the same contract, including table storage settings,
relational and policy catalogs, and initial-child publication controls in the
configuration witness. Rebinding reloads the table catalog and durable worker
positions. Query leases, active policies, restore and migration state prevent
reuse. The cache, active turns, and immutable merge jobs own independent strong
references. Starting a merge therefore does not evict a usable runtime; changing
its inputs retires the cache reference while the merge retains its old owner
until cancellation or completion is joined. Namespace switching never closes a
table still borrowed by a merge.

Both producer and consumer controllers size preparation windows from elapsed
time minus measured admission/publication overhead. That fixed cost still
counts toward call deadlines and yield decisions. Contention therefore does
not by itself shrink preparation windows and multiply admission and sync work.

Automatic full-generation format conversion follows the same replay/quiet
admission and primary-publication preemption as automatic shrinking. A legacy
file may continue serving supported operations while conversion is deferred.
Manual conversion retains its explicit execution semantics. This avoids
restarting a whole-database upgrade on every maintenance turn under active writes.

## Qualification

Component coverage includes the ordering model, slot reuse, bounded capacity,
partial initialization, partial enrollment writes, delayed descriptor close,
cancellation, and deadline expiry. Real process fixtures
exercise immediate holder reentry, death while queued and while holding a turn,
middle-waiter timeout, read cohorts, and interoperability with a legacy flock
holder. Database/API tests must also cover ownership transfer, failed opens,
allocator proof completion, local-lane timeouts, and suspended-call budgets.
`antfly-generation-publication-test` independently exercises the control wrapper
and posting-store publication/recovery contracts. Native adapter and queued-group
regressions remain in `lite-native-test`.

Final qualification uses the unchanged importer and its original timeout, cached
optimized reference observations, contended writers with status/logical readers, acknowledged
write and commit-outcome checks, equivalent index settling and byte accounting,
active verified page reuse, and sustained throughput/storage tests. Cross-process
ordering does not explain a single-process replay bottleneck; both dimensions
need their own evidence.

A dedicated writer process is a possible later architecture if measured IPC and
batching savings justify its election, recovery, authentication, and shutdown
contracts. The single-process replay failure does not establish that benefit.
