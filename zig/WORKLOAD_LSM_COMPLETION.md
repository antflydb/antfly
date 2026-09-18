# Internal LSM completion allocation ownership

## Allocator provenance prerequisite

This stage preserves allocator provenance when an in-memory publication uses
an allocator different from the backend allocator. Persistent AVL nodes free
through their originating allocator, including recursive and incremental
retirement after the last reader releases a root. The spare-node vector records
its allocator separately. Switching its allocation domain first retires the
old unused pool; a transferred spare vector therefore cannot contain nodes
from another allocator domain.

The prerequisite kept the existing shared memory `Account` unchanged and alive through
the same generation and allocation references. Mixed publication allocators do
not create new accounts or exclude any allocations from ordinary accounting.
The allocator contexts themselves must outlive every allocation made through
them. This stage does not supply a retained allocator-context owner.

On the tested 64-bit aarch64 target, the memtable `OrderedIndex.Node` grows from
120 to 136 bytes: 16 bytes, or 13.3% of the node structure, excluding its document
payload. `OrderedIndex` grows from 40 to 64 bytes. Existing `@sizeOf`-based
accounting and preparation estimates include those increases. This affects
default nodes too; a future compact domain owner could reduce the overhead,
but no throughput or memory performance qualification is claimed here.

Validation uses the actual in-memory LSM backend for alternate candidate
allocation followed by ordinary writes, retained old/new snapshots, final
retirement, and an allocation-failure sweep. The state tests additionally
exercise incremental reclamation, shared-account conservation, and allocator
domain changes in a spare pool. Final focused results: 25 state/backend-type
tests and two actual LSM backend tests in each of Debug and ReleaseSafe.

## Sealed memory-only point batches

`Backend.prepareCompletionPointBatch(namespace, operations, limits)` is an
internal, callable backend path. It copies bounded point puts/tombstones and
prepares the complete COW successor before returning a heap-stable ticket.
The caller supplies operation, encoded-input and physical-allocation ceilings.
The physical ceiling includes the owner, ticket, copied input, nodes, spare
vectors, shared entry headers and preparation overlap. An insufficient ceiling
or backing allocation failure aborts preparation without publishing anything;
it is not an estimate promising that arbitrary input will fit. Range/bulk
operations are absent from the API. Persistent roots and storage-backed modes
are rejected before reservation. There is no public configuration or transaction
policy activation.

The allocator stages exact requested buffer bytes from `CompletionCredit`
before calling its backing allocator. Resizing uses allocate-copy-free, so the
overlap is reserved as well. The owner metadata is itself reserved before
allocation. Each physical buffer retains the owner, including buffers retained
by readers after the submitting ticket is destroyed. The manager and backing
allocator must outlive those buffers. Allocator-private overhead, resource
manager identity-ledger storage and whole-process RSS are outside this byte
accounting contract.

The same shared `Account` follows old and new roots. Its single atomic
`ordinary_bytes` counter contains only buffers charged by the ordinary backend
observer. The trusted prepaid allocator's buffers retain the account but are
charged solely by their separate credit observer. This preserves old ordinary
nodes without counting the prepaid subset twice, and avoids subtracting two
independently changing counters. This change adds no `Account` size overhead.
The backend has one additional pending-ticket list pointer.

Outstanding tickets participate in the backend's ordinary accounting walk,
including their pinned original root. A competing write or mutable rotation
makes publication fail as stale; the original root remains charged until
ticket destruction. Successful publication checks the pinned root identity,
marks credit published, and swaps the prepared root without allocation. It
can use already-owned capacity after aggregate or slice limits are reduced.
Ticket destruction releases unused credit; subsequent physical frees release
their exact charges. Destruction retains immutable account handles on the
backend list while destructive tree cursors run outside the writer lock in
64-node slices. This matters when a tiny stale batch is the final pin of a
large prior generation. Close waits for outstanding tickets just as for readers;
callers must destroy tickets before synchronously closing their backend.

Actual backend tests cover saturation and reduced limits, failed backing
allocation at publication, retained old/new readers across ordinary writes,
mutable rotation, stale and persistent-path rejection, insufficient physical
capacity, 64 preparation-allocation failure positions, off-lock retirement of
a 2,001-key stale generation, and close waiting for both ticket and reader.
They validate
resource-manager byte ownership, not process memory or throughput.

The final focused suites passed 27 tests each in Debug and ReleaseSafe, with
zero leaks: seven actual backend tests and twenty existing state regressions.
Run from `zig/` with `zig build lsm-backend-test -j1` (add
`-Doptimize=ReleaseSafe` for that mode), followed by
`-- --test-filter 'workload admission lsm' --test-filter 'storage.lsm_backend.state.test'`.

This is not durable transaction completion. No WAL or manifest admission is
bypassed. Durable pre-prepare physical-plan certificates, restart reservations,
replay/backlog capacity, and protected WAL/manifest/flush resources remain
requirements before public transaction policy can claim mandatory completion.

## One-shot native WAL point commits

`Backend.applyCompletionPointBatchWithWal(namespace, operations, limits)` now
provides an internal native persistent path. It accepts bounded point puts and
tombstones on a writable, backend-owned native store with WAL enabled and a
ResourceManager. Custom providers, memory-only stores, read-only handles and
active bulk sessions fail closed. No public configuration invokes it.

Before sealing, it applies ordinary aggregate-memory, manifest/backlog and WAL
retention admission, including any pressure-driven flush. It then owns the
complete copied input, COW successor, encoded WAL record, prepared native paths,
two native FD permits, WAL serialization, and a backend lifecycle pin. COW is
built after admission that can release the backend mutex. Observer metadata is
pinned before the irreversible boundary; these pins grant no byte credit.
WAL growth is separately admitted into a temporary observer and atomically
transferred into the backend retention observer after the attempt. The same
bytes are never released and reacquired or counted twice. This follows the
existing ResourceManager treatment of WAL retention; it does not reserve
filesystem free space or guarantee successful device I/O.

The seal forbids further allocation in the physical owner. Native append and
publication keep the backend and WAL locks. Publication is a prepared root
swap with no subsequent stale-state validation. A failed or uncertain storage
attempt fences the backend, preserves manifest debt and retains conservative
WAL-growth accounting until recovery or an authoritative retention snapshot.
The original error is returned, with no implicit resend. Fully written but
unpublished records remain recoverable through ordinary backend reopen.

The operation marks maintenance deadlines and debt without starting new
allocating maintenance. Its caller must continue the normal maintenance loop;
this API does not finish SST construction or manifest publication. Native
working buffers and unused credit retire on return, while shared tree nodes
remain charged to their heap-stable owner through the final reader. Ticket
reclamation runs in bounded slices outside the writer lock, with a backend pin
held through cleanup. Close waits for that ownership.

Native tests cover allocation/FD/admission exhaustion after sealing, lower
limits, pinned readers, WAL rotation, pressure checkpoints before COW, manifest
backlog and FD rejection, preparation allocation failures, partial and fully
synced unknown outcomes, failure between append and publication, writable
recovery, and close overlapping the sealed operation. These are local resource
ownership and storage correctness tests, not throughput qualification.

This remains a single-call internal mechanism. It supplies no durable
pre-prepare transaction certificate, retained cross-request ticket, restart
reservation reconstruction, or protected SST/manifest completion guarantee.
Those boundaries must be implemented before enabling a public transaction
policy that promises mandatory completion.

The native stage's focused suite passed 21 tests in each of Debug and
ReleaseSafe, with zero failures, skips or leaks. From `zig/`, run
`zig build lsm-backend-test -j1` (or add `-Doptimize=ReleaseSafe`), followed by
`-- --test-filter 'workload admission lsm' --test-filter 'observer metadata pin' --test-filter 'lsm WAL uncertainty' --test-filter 'lsm backend write stats separate'`.

## Next stage: one durable transaction completion slot

This is proposed work, not an implemented guarantee or an activation instruction.
The accepted scheduling design requires completion resources and durable recovery
information before an irrevocable vote or decision. Calling the one-shot helper
at resolution does not meet that requirement: its memory, manifest, WAL and FD
admission can still fail after a participant has voted prepared. Its COW candidate
also describes the current mutable root; retaining that candidate through other
writes does not make it a valid successor later or after restart.

The first target is a transaction with one participant and one overwrite of an
existing live document, with an inline value, complete identity mappings and a
pinned schema. It excludes inserts, resurrection, deletes, relational storage,
payload externalization, secondary or derived indexes, generated/graph/artifact
work, child dispatch and HA outboxes. Unsupported profiles must be rejected
before `prepared=true`. The existing identity classifier can prove an ordinary
overwrite needs no identity mutations, but schema, identity and topology fencing
must preserve that proof through resolution. This profile is not yet certified.

The integration boundary is `TxnManager.writeIntentsExtraBatch`: its atomic
batch publishes `prepared=true` alongside intents, membership, locks, admission
and the schema lease. `DB.prepareTransactionRows` currently prepares relational
row encoding, not a complete physical plan. Resolution enters the general DB
batch expander again. Even the proposed document overwrite needs document and
TTL writes, transaction status, intent/member/lock/admission/schema cleanup,
completion accounting and derived replay. Transaction resolution explicitly
cannot elide replay; `docstore.writeReplayEntries` adds sequence metadata and
replay entries. Commit version, replay sequence, Raft markers and shared ledger
values cannot be frozen to their earlier values. The persisted plan must enumerate
operations plus bounded late-binding fields and encoding workspace. It must never
replay a stale shared-counter value over intervening transactions.

Implement this as one internal vertical slice: a single durable completion slot
holding a private bounded delta generation, rather than rebasing against an
arbitrarily changed foreground tree. Before publishing the prepare batch, reserve
the slot's generation/publication metadata, input and late-binding buffers, WAL
encoding and retained growth, native FD bundle, and one bounded SST/manifest drain
workspace. Persist a versioned descriptor containing the operation shape, bounds,
transaction identity, intent revision and fencing information in that same batch.
Ordinary writers must not consume the reserved slot or drain capacity. Resolution
publishes the private generation without new ordinary allocation or FD admission;
reader-held state keeps its physical charge until its last reference disappears.
The delta must participate in existing read precedence and atomic publication,
including writes that occurred while the transaction was prepared.

This requires actual SST and manifest support beyond `NativeWalCompletionIo`,
which currently serves only WAL operations. Existing manifest wire/backlog credit
is not preallocated table-builder memory, output buffers, run-directory nodes or
file capacity. WAL reservation must cover prepare, decision, application and
cleanup records, segment/control overhead and retention until a durable checkpoint;
it cannot be released merely because one append succeeded. Existing I/O errors
still produce durable uncertainty. Resource accounting does not reserve filesystem
free space or promise a device operation will succeed.

Startup restoration belongs in this same slice. Before foreground admission,
read authoritative pending-slot records and restore their bounded memory,
publication/drain slots, WAL debt and FD capacity, including undecided prepared
votes. Smaller configured limits must not silently discard old obligations or
send them through ordinary admission. `reconcileCompletionAdmission` currently
reconstructs logical count/byte debt only; its records are not physical slot
certificates. Preserve the original transaction identity and existing terminal,
participant-acknowledgement and outbox retirement conditions. Replicated activation
must establish compatible capacity before accepting obligations; a committed Raft
apply cannot acquire a new node-local admission veto.

Acceptance for this stage requires actual prepare and resolution paths:

- Prepare, perform unrelated writes, reduce ordinary memory and FD limits, then
  resolve using only the reserved slot and drain resources.
- Crash while prepared and after a durable decision but before publication;
  restore ownership before traffic and complete under the original identity.
- Hold an old reader through resolution and retirement, proving its physical
  memory stays charged and ordinary writes cannot consume completion resources.
- Exercise allocation, storage and publication failures without losing debt,
  repeating a mutation under a new identity, or reporting uncertain work aborted.
- Reject every unsupported profile before the prepare vote, and verify exact
  read/reopen results plus atomic metadata cleanup for both commit and abort.

Keep this path internal until those proofs pass. A physical-plan encoder alone,
or a successful one-shot WAL test, does not establish mandatory transaction
completion or full performance qualification.
