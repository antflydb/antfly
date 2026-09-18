# LSM completion allocation ownership prerequisite

This stage preserves allocator provenance when an in-memory publication uses
an allocator different from the backend allocator. Persistent AVL nodes free
through their originating allocator, including recursive and incremental
retirement after the last reader releases a root. The spare-node vector records
its allocator separately. Switching its allocation domain first retires the
old unused pool; a transferred spare vector therefore cannot contain nodes
from another allocator domain.

The existing shared memory `Account` remains unchanged and stays alive through
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

This is not prepaid backend completion integration. `CompletionCredit` remains
disconnected from LSM publication. There is no new public transaction policy,
WAL or manifest admission bypass, or mandatory-completion guarantee. The next
step needs a stable retained allocator owner and an exact accounted subset
inside the existing shared account before credit can move into backend
ownership without being counted twice. Durable pre-prepare physical-plan
certificates, restart reservations, replay/backlog capacity, and protected
WAL/manifest/flush resources remain separate requirements.
