# SQL write team on a retained execution

The production SQL capability factory uses the existing scheduler execution;
it performs no new operation/byte admission. It permits write/control roots,
rejects absent/read roots and refuses widened control roots. Ordinary teams
use the configured ordinary partition; protected COMMIT has exactly one slot.
Its resize callback separately rejects attempts to widen a control borrower,
so modifying a local Team maximum cannot borrow the health reserve. Wait and
checkpoint callbacks fail until accepted-write completion authority exists.
The caller supplies a stable concurrent bounded allocator through every join.

Four regressions execute on native macOS and pinned Nix/glibc GNU AArch64.
They reserve three ordinary slots on one retained operation, exhaust a real
Shared byte limit, free child allocations, preserve operation/byte/CPU charges
through joined completion and scheduler close, and allow health progress while
ordinary CPU is full. They exercise protected COMMIT plus another health root
at full ordinary capacity, including rejected direct control borrower widening.
Cancellation and deadline retain the original execution fences and do not
activate a failed team or retire its root. Invalid root classes are rejected.
The exact capability used by the proposed SQL adapter is compiled and tested;
the focused gate does not compile or run that adapter, the API constructors,
physical storage, SQL HTTP/pgwire or durable transaction coordinator.

The supplemental SQL proposal now constructs a team only for data mutations
and COMMIT. BEGIN, ROLLBACK, settings and DDL do not need a participant team.
The legacy ApiHttpServer.init and initWithProcessRequestAllocator constructors
now delegate to bounded initialization when scheduling is enabled. Their former
path configured CPU scheduling without request_memory; that would reject the
write capability and left child allocator ownership unavailable. A regression
for both actual constructors checks request limits, allocation header accounting
and retained write team lifecycle. It is registered in the existing runnable
execution filter but has not compiled/run yet. The explicit testing allocator
constructor keeps its supplied allocator contract; unsupported write execution
cannot silently fall back to unbounded fanout.

These call sites plus inherited production changes remain supplemental pending
full archive/API/native and pinned GNU qualification. The supplemental patch
is relative to 9c3d34d8a8 and includes new capability/test sources and both process
fixture files; reverse-apply checking confirms the exact candidate. A hash in
the Linux result identifies a source proposal even when the focused graph does
not compile it. Pooling remains disabled. Accepted completion I/O authority,
whole-node accounting, numerical teams and default proof remain outstanding.
