# Lite writer admission

The data-path sidecar flock remains the authority that excludes incompatible
database operations. A bounded coordination queue orders cooperating processes
before they seek that flock. This separates turn ordering from storage safety and
preserves exclusion when a binary using the older flock-only protocol is present.
Fairness applies to callers participating in the queue.

## Queue protocol

The versioned coordination file has a bounded hashed name beside the canonical
safety-lock path. Its size is 8,256 bytes: a 64-byte header and 1,024 ticket slots.
An OFD byte-range lock protects enrollment; a distinct range belongs to each live
ticket. These kernel locks belong to open file descriptions. Independent owners
inside one process exclude each other, and closing an unrelated descriptor does
not release their ownership.

Enrollment holds the short queue guard, removes entries whose descriptions have
closed, and locks a free slot before publishing its sequence and access kind.
The sequence follows the greatest live sequence. An empty queue can restart at
one. A full table waits within the caller's remaining admission budget. The file
does not grow with the number of completed requests or the retained database.

A writer waits behind every earlier live ticket. A reader waits behind earlier
writers while sharing a cohort with earlier readers. A later reader therefore
cannot continually overtake a queued writer. The nearest conflicting predecessor
is a wake hint: every disappearance, cancellation, or slot reuse causes another
guarded ordering check. Removing a middle waiter cannot grant a successor a turn
while an earlier conflicting owner remains alive.

Liveness queries do not acquire a predecessor's range. This prevents several
successors from accidentally keeping a dead predecessor locked. Generation checks
detect slot reuse; authority to proceed always comes from the guarded live-table
check, followed by acquisition of the safety flock.

The safety descriptor closes before the ticket descriptor. Cancellation closes
both in that order, and process death releases their kernel ownership. The next
visit reaps stale metadata. Interrupted initialization can be repaired only while
the queue guard and the entire ticket-lock range prove that no live ticket exists.
Failed enrollment retains the guard through descriptor close. A partial new-slot
write therefore cannot appear live to a peer while its failed description is
still draining.

Queue metadata needs no data-durability barrier: after a machine restart all its
former descriptions are gone, so their records cannot authorize a new operation.

The range-lock shim issues nonblocking operations; cancellable `std.Io` waits
separate failed attempts. It cannot leave a raw blocking syscall that acquires a
lock after its caller has returned. The existing joined kernel-wait helper still
handles a safety flock held by an older binary.

Read-only callers retain the existing inode-fenced fallback when they cannot
access a coordination sidecar. Namespace construction follows the established
canonical safety-lock identity. Native read-snapshot alias and replacement
protections remain required independently of turn ordering.

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
refuse new contention waits after expiry. The completed verification witness is retained against its matching cached
checkpoint or checked append-only lineage before lease reacquisition. A Busy
refusal therefore cannot force another full walk on a stable tip. No mutable
verifier ledger is shared with borrowed runtimes. Both local handoffs after
verification and reader teardown use the original admission budget; mandatory
teardown still drains under the gate, and a refused handoff cannot start a new
mutation afterward. Runtime and reader references remain owned until that drain
finishes.

## Work under a turn

Fairness bounds the number of earlier turns, not their duration. Replay and
immutable merge preparation run outside path writer admission. Publications and
existing mutation groups retain bounded work and release their ticket at their
boundary. A public call releases its lease before another public call starts.

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

## Qualification

Component coverage includes the ordering model, slot reuse, bounded capacity,
partial initialization, partial enrollment writes, delayed descriptor close,
cancellation, and deadline expiry. Real process fixtures
exercise immediate holder reentry, death while queued and while holding a turn,
middle-waiter timeout, read cohorts, and interoperability with a legacy flock
holder. Database/API tests must also cover ownership transfer, failed opens,
allocator proof completion, local-lane timeouts, and suspended-call budgets.

Final qualification uses the unchanged importer and its original timeout, matched
optimized controls, contended writers with status/logical readers, acknowledged
write and commit-outcome checks, equivalent index settling and byte accounting,
active verified page reuse, and sustained throughput/storage tests. Cross-process
ordering does not explain a single-process replay bottleneck; both dimensions
need their own evidence.

A dedicated writer process is a possible later architecture if measured IPC and
batching savings justify its election, recovery, authentication, and shutdown
contracts. The single-process replay failure does not establish that benefit.
