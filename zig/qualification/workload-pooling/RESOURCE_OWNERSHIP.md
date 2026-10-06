# Pooling resource ownership and acquisition order

This ledger describes the native standalone/data-server integration. Embedded,
serverless and caller-injected owners retain their own contracts. A managed byte
ceiling is a reservation limit, not a resident-set ceiling.

## Memory owners

| Owner | Parent / accounting | Ceiling and release boundary |
| --- | --- | --- |
| Storage caches, search/apply working sets | Storage-owner ResourceManager | Detected/clamped memory envelope plus existing per-slice limits; actual reservation/observer retirement |
| Model/tokenizer/KV/scratch reservations | Local ResourceManager borrowing the linked storage context's node memory owner | Existing per-slice limits plus shared parent ceiling; allocation-free callbacks charge growth once and return actual releases |
| API foreground scratch, plans, retained readers and transaction registry | Storage-owner ResourceManager external bytes | `admission.max_request_memory_bytes` less control reserve; provenance headers/alignment included; resize/free returns storage-owner charge |
| API control scratch and SQL retirement | Pinned storage-owner ResourceManager external reservation | Reserve `min(16 MiB, request ceiling / 8)` before publishing API; foreground cannot borrow it; release only after API workers, cursors and stores join |
| API sessions, jobs, caches and owned durable session heap | Storage-owner ResourceManager external bytes | `admission.max_retained_memory_bytes`; stable allocator until actual destruction |
| HTTP decoded context and buffered output | API foreground/control allocator; nested listener application quota | Held through response writer and cleanup; count once in storage-owner totals, despite appearing in both listener and API counters |
| Native pgwire protocol/frame/session and transport-runtime heap | Pinned public transport allocator; nested connection/listener/retained quotas | Shares HTTP transport's reservation; native session/listener joins precede allocator destruction; adapter execution state retains its API allocation owner |
| SQL plan cache | Nested API foreground allocation | Existing 32 MiB cache cap; do not add again to storage-owner totals |
| HTTP raw/transformed body storage, headers/parser/router/runtime heap | Pinned node reservation and bounded shared transport allocator | `min(64 MiB, API request ceiling)` reserved before HTTP/pgwire publication; body/task/connection limits remain nested; released after all listener/connection/runtime owners join |
| Outbound HTTP request/output storage and sockets | Independent service transport owners | Operation/wait/byte/socket caps; streamed readers retain leases until close; API-owned portions also appear in API accounting |
| Caller-supplied stores and backend allocations ignoring supplied allocators | Caller/backend owner | Cannot infer limits from API configuration; caller must report its independent budget |
| Native C/C++ model scratch, allocator metadata, OS threads/stacks/TLS, mappings and page cache | Native/kernel/backend owner | Not comprehensively intercepted; measure RSS/threads/FDs and audit backend-specific reservations |

In linked native composition the callback crosses the storage context ABI and
charges the real storage-kernel manager; in direct composition it charges the
local manager. The local storage/inference manager borrows the linked context's
memory and pressure owner before publication. Child strict growth must reserve
parent bytes before mutation; observation-only growth still reports already
existing allocations, including over-budget usage. Child slice and external
releases wake the shared pressure event, as do releases from other owners. Child
shutdown returns remaining observer charges after checking strict leases. The
parent context outlives API/transport allocations and child-manager destruction.
A reclaim-capable child may ask the parent for bounded sibling-cache eviction
outside the child accounting locks. Non-reclaiming commit fences never make this
call, and parent eviction cannot bypass child ceilings.
Root aggregate memory already includes child usage, so health reporting counts
that aggregate once. Per-slice reporting combines distinct owner contributions.
This coordinates managed storage, inference, API and public HTTP heaps; native
scratch, OS memory and independently supplied service heaps still require
measurement and their own ownership contracts.

`ResourceManager.memory.external_bytes` includes pinned control/transport
reservations, child-manager usage and foreground/persistent API charges. It is a subset of
`memory.used_bytes`, not an additional quantity. API control used bytes are a
subset of its pinned reservation. Transport used bytes are likewise a subset of
its pinned reservation, with no second charge at allocation. Pgwire protocol
retained bytes are a subset of pgwire allocation bytes, themselves a subset of
shared transport used bytes. Adapter-owned retained execution objects keep their
API persistent quota. Reservations can
be returned on backing-allocation failure without changing any live object.
The external callbacks are allocation-free, do not invoke reclaimers, and must
outlive the allocator owners. The manager asserts that external charges are zero
at final destruction.

## Acquisition and progress

1. Bound transport connections, descriptors, raw bodies and application scopes
   before decoding or publishing a handler. Authentication precedes policy changes.
2. Preparation and fresh plan-cache fills finish before foreground admission
   waiting. A waiter retains only its caller-owned entry, input, original deadline
   and already reserved task; it cannot reserve a backend execution lease twice.
3. Execution admission precedes new downstream work. Each allocation first
   reserves its local API quota and then the storage-owner quota; denial rolls back the
   local reservation before returning OOM. Neither lock calls into an allocator.
4. Storage/model admission may reject a fresh operation after query admission.
   The operation then retires its lease; it does not hold it while retrying new
   mutations. Accepted durable work retains its own completion contract.
5. Verified transaction controls bypass fresh-query/write/preparation/cache-fill
   admission, use independent bounded workers and the pinned control allocator.
   Cleanup frees through allocation provenance. It cannot erase an already
   started durable decision.
6. Closing transports joins writers/sessions before API allocators disappear;
   API joins accepted jobs and releases persistent state before returning pinned
   control capacity. The storage-owner manager is destroyed last.

Pinned transport memory protects against exhaustion of the shared node budget;
it does not grant a separate parser allocation reserve after the transport heap
itself is exhausted. Existing connection/body ceilings still matter, and that
distinct exhaustion schedule needs a native protected-progress receipt before
defaults can change.

The native socket fixture now exhausts the real DB ResourceManager alongside
API foreground allocations and foreground SQL workers, then verifies held-cursor
COMMIT and failed-transaction ROLLBACK. This proves those local storage schedules.
It does not prove physical WAL/Raft/LSM completion through arbitrary distributed
locks or remote-owner failures. New storage work required during COMMIT can still
fail under its own downstream ceiling; do not turn that failure into a claim of
replay-safe mutation rejection.

## Default release gates

Automatic query waiting/sizing remains unqualified until all of these have
receipts: Linux GNU signal-stack ownership, fixed small and large container
profiles, slow-reader and mixed open-loop pressure, main-versus-candidate latency
and throughput, and protected distributed completion. A larger independently
owned allocation or native-thread cost must not silently enlarge a process budget.
The manifests record enforced cgroup CPU/memory/PID limits, not just YAML intent.
Static-musl retains the original Zig signal-stack implementation and needs its
own profile.
