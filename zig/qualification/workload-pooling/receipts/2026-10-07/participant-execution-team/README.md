# Invocation team retains accepted execution through child joins

Runnable Team borrows one existing checked execution, reserves CPU for its
coordinator plus bounded parallel children, and supplies a required thread-safe
bounded allocator whose owner must outlive every child. Its maximum demand is
provided by trusted ingress and validated by the actual scheduler. Failed
admission never activates the team or silently submits unscheduled children.
Single-slot teams expose zero parallel child capacity and can execute serially
on the owning caller. Joining children retains the admitted grant to owning
execution finish; it does not seek new foreground admission after cancellation
or shutdown. A subsequent proven-aborted attempt can reuse joined team state
under the same execution fences; active reuse is rejected.

Native macOS and pinned Nix/glibc GNU AArch64 pass all 37 ABI contracts and 33
executed owner/listener/worker/descriptor contracts. Unchanged 26 RPC contracts
are cached from the prior successful gate. Real-owner regressions reserve a
four-slot team on one capsule, verify unchanged operation/byte/borrower ownership,
health progress, retention after join and shutdown, failed activation, serial
capacity and joined reuse. These are owner contracts, not physical Raft/WAL
acceptance or transaction coordinator runtime proof.

The supplemental coordinator proposal adds ExecuteOptions.team, reserves before
first participant contact, caps fanout to admitted child slots, and directs begin,
prepare, resolve, abort and acknowledgement arena backing to the supplied owner
allocator. Existing durable ordering and original outcome paths are preserved
in the proposal. A real-owner coordinator fixture exercises bounded concurrent
prepare and a zero-capacity child allocator which must report OutOfMemory rather
than fall back to page allocation. That source passes Zig ast-check and diff
checks; its type checking and transaction runtime gates are still pending.
The focused owner graph does not compile distributed_txn.zig. Its presence in
source hashes identifies the exact proposed candidate, not runtime qualification.

The supplemental patch is relative to 5089385a72 and includes inherited pending
production changes plus both untracked process fixture files. Reverse-apply
checking verifies that it matches the current candidate. External zig-pkg
dependencies retain the existing workspace mount. Full coordinator and
production archive gates must run against this frozen candidate. Request
ingress/table-write forwarding of the team is still unimplemented, so production
transaction calls continue using their previous fanout path. Accepted completion
I/O parking, whole-node/default proof and actual numerical teams remain required.
Pooling stays disabled. Compile reservations are not runtime memory limits.
