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

Public handle admission uses one remaining waiting budget across the connection
lane, shared process/path lane, path lock, retries, and suspended-call resumes.
Active preparation and inference preserve the unspent admission balance. This is
an admission waiting limit; execution and mandatory cancellation teardown have
their own lifetimes.

The local queue uses caller-owned linked nodes under its condition mutex. A timed
out or cancelled waiter unlinks before returning. This avoids abandoned numeric
tickets, retained cancellation heaps, and borrowed nodes outliving their stacks.
The condition epoch is read before releasing its mutex, preserving wakeups across
the transition into a timed wait.

A successful detached allocator proof may outlast its waiting budget. Admission
must retain that finished proof, allow an uncontended stable-tip acquisition, and
refuse new contention waits after expiry. Runtime and reader references remain
owned until cancellation/verification work has drained.

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

The required caller contract distinguishes applied, not applied, and unknown.
Applied mutations may still have pending derived visibility. A refusal before the
commit decision can report that the request was not applied. Once a decision may
be durable, failures must preserve an unknown outcome and a stable reconciliation
token until recovery establishes its state. Response allocation or visibility
failure cannot turn an applied mutation into an ordinary retryable error.

Diagnostics should preserve the actual failure class alongside this disposition.
Changing an internal error's spelling does not establish whether a mutation was
applied. SDK error wrappers must retain the disposition and reconciliation token.

## Qualification

Component coverage includes the ordering model, slot reuse, bounded capacity,
partial initialization, cancellation, and deadline expiry. Real process fixtures
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
