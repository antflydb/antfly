# Bounded workload pooling implementation

Baseline: `e4d707de5634a8d758c606b4432037e1a1682052`. Design:
[WORKLOAD_SCHEDULING.md](WORKLOAD_SCHEDULING.md). Branch:
`design/default-workload-pooling`.

## Implemented scope

- [Shared admission controller](pkg/antfly-embedded/src/common/bounded_admission.zig):
  allocation-free FIFO queue, count/input-byte/wait limits, original deadlines,
  cancellation/grant ownership, close/drain notification, and single-release
  operation leases. Legacy callers retain fail-fast acquisition and zero-capacity
  semantics. Caller-owned entries also expose nonblocking enqueue/retire APIs.
- [HTTP admission](pkg/antfly/src/api/httpx_handler.zig): contextual query-class
  routes can opt into waiting. Saturation identifies the admission stage and
  limiting reason, with `execution_started=false`. Queue wait consumes an existing
  transport deadline or a declared JSON `timeout_ms`; public table query execution
  preserves that original deadline through catalog binding and readiness retries.
  NDJSON multi-query requests retain fail-fast admission, but each line now
  clamps its own timeout to the original outer deadline.
- [Native SQL owner](pkg/antfly/src/api/http_server.zig) and
  [pgwire adapter](pkg/antfly/src/api/sql_pgwire.zig): HTTP SQL and pgwire use the
  same query controller, preserving their original deadline/cancellation source
  and typed parameters. Preparation leases are released after compilation and
  before admission waiting. Fresh writes remain fail-fast. Transaction completion
  uses separate bounded preparation, execution, and worker reservations while
  retaining existing durable completion rules.
- [Aggregate pgwire allocator](pkg/antfly/src/pgwire/budget.zig): each connection
  keeps its existing local cap and also charges a listener-wide atomic cap.
  Failed backing allocation/growth returns its reservation; session teardown
  reclaims it. The listener joins sessions before destroying this owner.
- Canonical configuration and regenerated Zig bindings. Queue depth, charged
  input bytes, capacities, wait ceiling, wait histogram, expiration/cancellation,
  and draining metrics extend the existing admission namespace.
- Default protected HTTP tasks, parser/body partitions and bounded pgwire frame
  and acknowledgement memory. Verified SQL controls use the API control arena;
  fail-fast reserves control tasks within its existing ceiling. Ordinary bodies
  and frames cannot consume these partitions.
- Separate node-accounted outbound read and forwarding client heaps, plus
  synchronized caller-charged join/graph/model/probe child arenas. Clients and
  tasks join before their allocation owners retire; caller results keep their
  original charge.
- A combined retained-client pressure manifest and CI using pinned Nix glibc Zig
  on Linux. Qualification retains failed attempts and explicit source patches;
  see [the latest receipt ledger](qualification/workload-pooling/receipts/2026-10-06/nix-glibc-release-safe-b793/README.md).

## Opt-in configuration

This example is a correctness/exploration profile, not a qualified production
default or a throughput recommendation:

```json
{
  "admission": {
    "query": {
      "max_concurrent_requests": 4,
      "waiting": {
        "max_queued_requests": 4,
        "max_queued_bytes": 1048576,
        "max_wait_ms": 100
      }
    }
  },
  "pgwire": {
    "max_connections": 32,
    "max_session_bytes": 67108864
  }
}
```

All three waiting bounds must be positive to enable waiting, or zero to disable
it. The maximum accepted wait is 60 seconds. Explicit capacity zero retains its
legacy unlimited-operation meaning; it does not select automatic sizing.
Pgwire still requires its existing enable/authentication/transport configuration.
`max_session_bytes=0` preserves the sum of connection memory ceilings; it does
not disable the aggregate allocator. A positive ceiling must allow at least one
protocol frame. Backend-global caches and allocations outside connection budgets
retain their own resource owners.

### Protected dispatch and native executor ownership

The public HTTP transport reserves four request slots under both fail-fast and
waiting policies for
fixed GET status/health/readiness paths, existing transaction commit/abort/savepoint
rollback paths, SQL resource retirement, and supported SQL transaction completion. Ordinary requests cannot consume that
reserve. SQL body classification uses the execution grammar, accepts only COMMIT,
ROLLBACK, ROLLBACK TO SAVEPOINT, and RELEASE SAVEPOINT, and rejects trailing fresh
statements. Bodies are limited to 8 KiB and statements to 4 KiB for protection;
larger requests retain ordinary admission. Authentication and session ownership
checks still run in the handler. Headers and workload hints cannot claim slots.

The same permit policy runs before H1, H2, and h2c application dispatch. With
ordinary dispatch full, H2 parks only candidate SQL bodies, without publishing a
handler or claiming a protected permit. Existing connection/stream/body-byte
owners bound these candidates; an 8 KiB body ceiling and a maximum five-second
absolute timer bound classification. One joined watchdog per H2 connection exists only while candidates are pending
and expires incomplete bodies; idle sockets do not retain scheduling workers. Complete bodies must prove they are controls
before dispatch; fresh queries receive REFUSED_STREAM. The frame pump continues.

With waiting enabled, outstanding handler capacity is the smaller of the existing connection ceiling
and `existing request-task capacity + queue count + 4`. Fail-fast reserves the
protected slots within its existing total request-task ceiling. The shared HttpRuntime
reserves the resulting capacity before bind. Existing connection, descriptor,
body-byte, slow-upload and per-H2-connection stream ceilings still apply. The
existing separate health listener remains necessary when public connections are
full; this request reserve does not defeat process-wide FD or heap exhaustion.

The pinned Zig 0.17 Threaded runtime retains a worker during an Event wait. SQL
with a supported queue policy now borrows a dedicated foreground WorkerLease,
independent of the general/durable, control and request-forward executors. Its
capacity is:

`min(query capacity + queue count + write capacity + max(1, preparation capacity) + 4, (dedicated worker budget / 2) - 5)`

The runtime's aggregate dedicated-worker reservation is authoritative. Five
control workers are reserved before foreground workers. Their combined
reservation occupies at most half of the budget, leaving the remainder for
existing service owners. Dispatch remains bounded even when query/write admission ceilings exceed
the foreground executor capacity. The lane must fit the configured queue plus
four non-waiter slots. Otherwise SQL retains fail-fast behavior on its original
executor. If a supported lane cannot be reserved because other service owners
hold the budget, SQL reports capacity unavailable; it does not wait on an unsafe
shared executor. The lane is reserved lazily before first SQL work, reused across
both adapters, and released after listener/cursor owners join at server shutdown.

Both query and write capacities must be finite. Borrowed backend runtimes retain
fail-fast SQL behavior until their suspension contract is qualified. Direct
embedded callers without a BackendRuntime must supply independent completion
capacity themselves. `ApiHttpServer.sqlQueryWaitingEnabled()` and query admission
statistics expose the effective policy; the metric
`antfly_admission_query_sql_waiting_enabled` becomes true after the dedicated lane
is available. These mechanisms preserve general/control executor capacity; they
do not establish progress through every downstream lock, allocation or remote
resource wait.

SQL controls have four preparation slots and four execution slots, with five
independent workers so local disconnect cleanup has additional dispatch capacity.
Control plans compile with small fixed limits outside the fresh plan-cache fill
owner. HTTP prepared-resource retirement uses the control preparation/execution
owner too. Pgwire's read-stream probe skips controls, and COMMIT's existing WITH HOLD
cursor materialization uses the control executor for already-owned streams.
Fresh statements and newly opened streams cannot use that drain path. HTTP
transaction-session COMMIT and final ACK retries use the control execution owner;
one-shot fresh transaction submission retains write admission. Disconnect joins
local cleanup, with a local release fallback if scheduling fails; it never erases
a started distributed decision. Listener and cursor owners join before control
workers and session storage are released. Explicitly disabled or small dedicated
worker envelopes and borrowed runtimes retain their existing control executor;
legacy fail-fast SQL still works without reserving dedicated foreground workers.

This does not provide cheap coroutine suspension or justify raising idle-client
limits. The default 32-worker dedicated budget leaves at most eleven foreground SQL
workers after the control reserve. A queue must leave four non-waiter workers;
the four-waiter example above fits that envelope. Eight waiters require a larger
worker envelope for SQL waiting and otherwise remain REST-only.

Queued-byte accounting covers charged input and adapter metadata. It does not
track every allocation in authentication, cached plans, transport arenas, or the
shared heap. Native SQL parameter charges include decoded cells and containers;
HTTP charges include body allocation capacity, headers, path, and fixed context
layout. Existing resource-manager and transport memory limits remain necessary.
Snapshots and suspended portals retain their existing coarse execution ownership
until a verified detach boundary; their subsequent operations cannot replay a
query to obtain a new snapshot.

## Validation

Run with the repository's Zig 0.17 toolchain:

```sh
cd zig
zig build antfly-workload-pooling-test -j1
zig build lib-httpx-protected-dispatch-test -j1
zig build pgwire-test -j1
zig build check-openapi -j1
```

The focused pooling target includes deterministic queue ownership tests, native
worker-capacity/burst tests, config validation, HTTP/SQL policy contracts, native
row/session qualification, and a real-loopback HTTP burst. The socket test holds the execution slot, queues two
requests, checks status remains responsive, verifies a third receives a queue
count rejection, then releases the slot and verifies both queued requests return
the fixture's ordinary response and all live counters return to zero. Its fixture
has no table storage: the ordinary response is 404. It proves transport/admission
lifetime behavior, not database throughput or successful-query performance.
A second profile fills every ordinary request slot, verifies ordinary transport
rejection, and still receives status successfully through the reserve.

The protected-dispatch target tests hard reservation/release bounds and a real H2
peer on one socket: two blocked query handlers fill the ordinary lane, a third
query stream is refused, status and a proven control body complete, an untrusted
fresh body is refused, an incomplete candidate expires, then both queries drain.

Native SQL qualification uses real relational storage, a one-query shared owner,
and both HTTP SQL and the authenticated pgwire adapter. Both queue together,
release preparation admission before waiting, then preserve the exact integer
parameter/result and transaction status. A third queued pgwire operation is
cancelled before grant and maps to SQLSTATE 57014. A live native stream remains
charged after cancellation until close. Prepared HTTP execution is also forced
through waiting, with its identity and parameters preserved. Existing session,
setting, prepared-resource retirement, catalog replacement, and credential
revocation regressions run with these contracts; the HTTP session/setting
fixtures now enable waiting.

Native socket qualification additionally starts a real pgwire TCP listener over
that same storage fixture. It tests Parse/Bind/Execute portal suspension and
continuation, exact integer results, Sync recovery, authenticated CancelRequest
at connection capacity, COMMIT with WITH HOLD materialization and ROLLBACK while
ordinary execution/preparation/write capacity is full, disconnect reclamation,
and shutdown while a query is queued. A whole-test watchdog bounds joins. Queued
user cancellation returns 57014 and ReadyForQuery instead of the transport cancellation signal; subsequent SQL can
use the same connection.

A native H2 peer queues two SQL requests behind a held query slot, then verifies
fresh SQL rejection, native H1 COMMIT, bounded-body H2 ROLLBACK and status
dispatch, RST_STREAM cancellation of one waiter, and correct native rows after the other
waiter is granted. These are deterministic correctness schedules, not an
exhaustive protocol fault or pressure/performance qualification matrix.

Validation on 2026-10-05, macOS native, Zig 0.17.0:

- `antfly-workload-pooling-test`: 34 passed, zero skipped, failed, or leaked,
  including native socket/H2 waiting, protected controls, durable recovery/ACK
  under fresh-write saturation, and disabled-worker compatibility.
- `lib-httpx-protected-dispatch-test`: 7 passed, including H2 saturation,
  incomplete body expiration, and idle H2 sockets with one shared scheduling worker.
- `pgwire-test`: 83 passed, including retained-state quotas, idle transaction
  expiry, and stable allocator ownership across portal map growth and cursor fetches.
- `lib-httpx-client-lifecycle-test`: 22 passed, including connection reservations,
  shared socket limits, outbound waiting, TLS, and H2 lifecycle coverage.
- `common-http-test`: 128 passed, one skipped, zero failed or leaked. The skipped
  external wrong-route soak requires an external endpoint. Socket coverage verifies
  connection reuse across buffered and streamed requests through both executors.
- Focused `antfly-data-runtime-test`: 2 passed, checking metadata owner reuse and
  forwarding progress while the control executor is saturated.
- `check-openapi`: passed against the regenerated bindings.
- `zig fmt --check` for changed Zig sources and `git diff --check`: passed.
- `zig build antfly`: production CLI and native provider archives built
  successfully, including the opaque API bridge and deployment runtime wiring.
  The installed CLI also passed an `antfly --help` smoke check.

The final combined build exited successfully. Socket tests ran outside the
filesystem/network sandbox because it prevents loopback listeners. These are
correctness receipts for this checkout, not production performance qualification.

## Outbound pooling and retained state

Service HTTP executors now default to reusable, cookie-free clients. The resolved
buffered and streaming paths share one concurrent owner rather than serializing
all requests or constructing a client for every stream. Direct `std.http` remains
available through `resolve_before_connect=false`. Automatic mutation replay stays
disabled. Delivery tracking switches to “may have been sent” at the actual send
boundary, so queue cancellation/expiry preserves “not sent”.

The outbound operation envelope covers TCP, TLS, H2, buffered output and streaming
output. Service defaults are 20 active operations, 5 per endpoint, at most 20
waiting operations, 64 MiB of queued request storage (including body capacity,
headers, URI/query bytes, and capsule storage), and 5 seconds
of pool waiting. The original request deadline can shorten that wait. Queue
cancellation, client shutdown, and failed connection creation reclaim ownership.
This is bounded operation waiting, not runnable CPU scheduling or a FIFO fairness
policy. Callers can configure the executor limits, disable waiting with zero
waiters, or supply a stable `httpx.OutboundAdmission` shared by several clients.
Shared owners must use the same Io/clock and outlive joined clients.

A client also has one physical socket ceiling across TCP, TLS, H2, and connections
currently opening. H1 creators reserve before DNS/connect; failed opens return
reservations. H2 uses one handshake owner per endpoint and distinguishes HTTP
from HTTPS in its cache key. Unused sockets can be retired to make room for another
endpoint/transport, and active TLS entries are never reclaimed by pool cleanup.
Incremental H2 readers retain their operation and client lifetime leases until
closed. An owner must close those readers before destroying its client.

Data-Raft forwarding now retains a service-owned transport, independent of the
public-read pool, while every caller still holds its existing forwarding-lane
reservation. Reuse validates the same Io authority. The metadata source now uses
one concurrent owned executor instead of multiplying endpoint capacity across
four separately serialized executors. Asset/chunk provider-owned clients have
finite outbound operation/waiting bounds. CLI one-shot clients remain short-lived.
HA replication explicitly disables keep-alive and cached resolution because its
headless-Service authority must be resolved on every replication request.

Pgwire retained arenas, prepared/portal/cursor map storage, snapshots opened with
those allocators, cursor spools, savepoints, and setting overlays now charge an
independent per-connection budget and aggregate listener counter. These bytes
also remain charged to the existing connection/listener allocation budgets;
transient frames and result pages do not consume the retained allowance. Global
backend plan caches, durable transaction records, and backend allocations made
outside the supplied allocator retain their separate limits. HTTP prepared
resources already have a bounded durable directory/record size and expiration;
the native plan cache has its own 32 MiB limit.

Prepared, portal, and cursor arena owners have stable addresses so backend readers
can retain their allocator contexts across map growth and later fetches. Closing
or replacing these resources returns their retained charges.

The new pgwire policy is configured independently:

```json
{
  "pgwire": {
    "max_session_bytes": 67108864,
    "max_retained_bytes": 16777216,
    "retained_bytes_per_connection": 1048576,
    "idle_transaction_timeout_ms": 5000
  }
}
```

The default retained allowance is 8 MiB per connection. Zero aggregate retained
bytes uses the sum of connection allowances, capped by aggregate session bytes.
The default transaction idle timeout is 30 seconds, capped by the ordinary
60-second connection idle timeout. Both active and failed transactions use it;
expiry closes the connection and joins ordinary disconnect cleanup. It does not
turn a durable decision into an abort. Native retained readers keep their original
statement deadline; subsequent portal execution cannot extend that snapshot.

## Resource and entry-point integration

Production API initialization now installs a shared, provenance-aware allocation
owner. `admission.max_request_memory_bytes` defaults to 256 MiB and accepts values
at least 1 MiB. Up to 16 MiB (at most one eighth of the envelope) is reserved for
verified control operations. Foreground allocations cannot consume that reserve.
Objects retain their originating partition through resize, remap and free, even
when control cleanup destroys state created by a foreground request.

```json
{"admission":{"max_request_memory_bytes":67108864}}
```

| Allocation owner | Coverage and lifetime |
| --- | --- |
| API foreground/control | API scratch and backend allocations using `server.alloc`, SQL plan cache, retained read runtime, in-memory transaction/session registry; released on actual destruction |
| API persistent state | 256 MiB default independent envelope; API-owned session/job stores and queues, provider/routing caches, foreign registries, and opened durable session backend heap; released on actual destruction |
| HTTP application scope | Context/decoded values and response buffers, shared 256 MiB listener envelope with a verified-control reserve; held until writer and request cleanup join |
| HTTP raw body and parser | Bulk and parser/control partitions within the original pinned node transport ceiling; raw/transformed-body quotas remain nested and separate from output/context storage |
| Pgwire | Existing aggregate connection allocation cap plus independent retained-state cap; 32 KiB control-frame partition within each original connection ceiling; prepared/portal/cursor owners keep stable allocator addresses |
| Outbound DataServer clients | Read-client heap charges node growth; forwarding-client control heap is pinned before publication; active/waiting/socket and queued-byte limits remain nested per stable transport owner |
| Helper and distributed fan-out arenas | Synchronized child allocator charges the caller's owner, with a 64 MiB per-group ceiling; every child joins before owner destruction and result merging |
| Storage/models/durable backend | Managed storage/models share the linked node owner; independently supplied backend allocations outside supplied allocators retain their own budgets |

The root allocator includes provenance-header and alignment overhead. The HTTP
scope counts requested buffer bytes. Neither is a total-process RSS guarantee:
allocator metadata, runtime threads/stacks, storage mappings, model memory and
other backend-global caches have distinct costs. Legacy infallible API constructors
and test-injected allocators deliberately retain their existing ownership;
production fallible constructors install the envelope.

Pgwire now uses one deadline owner per listener rather than a deadline task per
connection. Each accepted connection has one session task. The native API listener reserves
a bounded transport executor (configured connections plus six accept/deadline/
cancel slots), so idle clients do not occupy the outbound API network lane.
This transport envelope is independent of the existing backend worker envelope
and must be included in deployment thread/stack sizing.
The shared owner
signals cancellation and shuts down an expired socket without closing its file
descriptor; the connection owner joins backend cleanup and performs destruction.
A real-socket fixture expires an idle client under saturation and joins all owners.

| Entry point | Execution policy and original deadline |
| --- | --- |
| HTTP JSON/SQL and pgwire queries | Configured shared bounded waiting; existing outer deadline/cancellation preserved |
| NDJSON multi-query | Fail-fast class admission; per-line timeout clamped to the outer request |
| MCP | Context-aware dispatch; one class lease, catalogue/query/table helpers receive outer deadline/cancellation |
| A2A | Context-aware dispatch; retrieval/query/scan/graph probes, generation and web search clamp their local limit to the outer request |
| Extension database host calls | Context-aware query/write variants; original context passed to table/query helpers |
| Writes and inference | Existing fail-fast class policies; no additional waiting queue |
| Internal query RPC/retrieval | Original deadline before semantic planning; query/scan/graph/generation/provider helpers preserve it |
| Embedded reads and batch handoff | Context overloads compose existing cancellation and normalize borrowed lookup clocks; writes preserve durable completion after handoff |
| Serverless and jobs | Original deadline flows through synchronous work and maintenance checkpoints; accepted jobs own independent attempt policy and budgeted storage |
| Legacy administrative create/drop | Context variants cover handoff and projection waits; committed outcomes preserve visibility/repair debt |

Compatibility wrappers accept an empty context for direct callers; callers with
an original deadline must use the context-aware variants. Legacy backend
callbacks without a context capability check before handoff and preserve the
backend's completion result afterward. They cannot preempt uncooperative host
code. The wrappers do not grant new waiting policy.

## Qualification and remaining deliveries

The native pooling suite includes COMMIT/ROLLBACK while foreground query/write,
SQL preparation/execution, and API allocation capacity are saturated. Shared
allocation tests verify provenance, reservation rollback, output lifetime and
protected memory availability. These are correctness checks, not performance
qualification for automatic sizing.

[The isolated idle-client harness](qualification/workload-pooling/run_local.py)
uses a [fixed small macOS manifest](qualification/workload-pooling/macos-small.json)
for 32 HTTP keep-alive clients plus 32 pgwire startup clients. It samples actual
RSS, virtual address size, thread count and file descriptors, checks status latency,
then closes clients and verifies recovery and joined server shutdown. It creates
isolated temporary storage and an ephemeral authenticated administrator; receipts
exclude credentials. The startup profile retains pre-authentication peers. The
[authenticated cursor manifest](qualification/workload-pooling/macos-authenticated-cursors.json)
adds 32 authenticated sessions, 16 held cursors, exact integer and continuation
checks. Neither profile qualifies distributed recovery.

Still required for the full design:

- Complete Linux release qualification: all 30 final candidate absolute profiles
  pass, but main fails idle/combined pgwire setup and its combined load comparison
  is unavailable. Raw matrices remain failed. Arbitrary
  distributed storage/lock/remote-owner exhaustion remains beyond the named
  protected progress schedules.
- A complete ledger and coordinated sizing for independent storage/model/durable
  allocations; total-process accounting is not established by API interception.
- Context capabilities for remaining legacy/host callbacks that cannot receive
  cancellation. Native hot query policy changes and operator suspension exist.
- Fairness, cooperative quanta and complete distributed uncertainty ledgers.
  Caller-charged helper/fan-out isolation exists; native/backend costs outside
  those allocators still need independent ownership qualification.
- Main-versus-candidate performance receipts and measured automatic sizing/default
  query waiting. Waiting remains opt-in until those release gates pass.

### Previous verification receipt (2026-10-05)

The final integrated build completed all 139 steps. The pooling suite passed
38/38 tests, pgwire passed 84/84, protected HTTP dispatch/allocation scope passed
10/10, client lifecycle passed its 22 tests, and common HTTP passed 128 tests
with one existing skip. Canonical OpenAPI checks and the native executable build
passed. `git diff --check` passed. Socket checks ran outside the sandbox because
it prevents local listeners.

The [fixed-manifest native receipt](qualification/workload-pooling/macos-small-receipt.json)
**failed** its unchanged 128 MiB RSS growth/recovery thresholds. All 32 HTTP and
32 pgwire clients remained connected. Idle growth was 146.4 MiB, 64 threads and
64 FDs; maximum status latency was 190 ms. FDs returned to baseline and shutdown
exited zero, but RSS retained approximately 146.5 MiB of growth. The
[first failed receipt](qualification/workload-pooling/macos-small-receipt-before-memory-audit.json)
reproduces the same memory failure before category diagnostics were collected.
These results are evidence that the idle-memory release gate is still open,
not a passing qualification or permission to enable automatic defaults.

The diagnostic process map reports approximately 185 MiB of resident large
malloc regions and 7.7 MiB of ordinary resident stacks. A symbol audit of this
native binary found eight distinct 256 KiB alternate signal-stack TLS copies,
consistent with the existing large static TLS finding in
[FLAKES.md](FLAKES.md).
This evidence motivated the shared ownership change below. The request envelopes
do not count native TLS allocations; the historical failed receipts remain
available for comparison.

## Persistent ownership and native thread TLS (2026-10-06)

Production API construction now installs a second allocator envelope:
`admission.max_retained_memory_bytes` (256 MiB by default, minimum 1 MiB).
This counts API-owned job stores/queues, protocol sessions, routing/provider
caches, foreign registries and opened durable session-store/backend heap.
The allocator has a stable address until background owners join and all stores
are destroyed. Exhaustion rolls back a failed allocation; existing objects keep
their original allocator and remain charged until actual destruction.
The request envelope continues to reserve control memory for SQL retirement.
Caller-injected stores and independently owned storage/model caches retain their
own limits. Deployment sizing must budget those limits in addition to both API
and HTTP envelopes; these counters do not promise a total-process RSS ceiling.

The seven native runtime partitions no longer allocate private copies of Zig's
256 KiB alternate signal-stack TLS. The executable and public C API link a
single POSIX thread-entry owner that supplies a full 256 KiB stack before native
worker execution, preserves an already installed stack, and leaves TLS lifetime
with libc. The main Zig root retains its startup stack and crash handlers.
`zig build shared-signal-stack-test` induces a real guard-page overflow and
checks that the signal handler runs within the shared worker stack. The change
reduces duplicate TLS ownership without shrinking crash-handler storage.

Internal query planning clamps the declared body timeout and the original
request deadline before semantic embedding. Internal retrieval carries that
same deadline and cancellation through query, scan, graph, generation and web
search. Serverless requests and maintenance checkpoints accept native absolute
deadlines; nested synchronous waits observe the borrowed original token.
Embedded `OperationContext` overloads cover JSON and direct DB reads and batch
handoff, compose existing cancellation checkpoints, and convert a borrowed
lookup clock before comparing deadlines. Administrative create/drop handoff and
projection waits also carry context. Once a write commits, deadline expiration
reports pending visibility/repair rather than a replay-safe failure. Accepted
background work retains its durable owner's execution policy and must not retain
request-borrowed callbacks after ingress returns.

The authenticated qualification manifest retains 32 HTTP clients plus 32
pgwire sessions, including 16 cursors held across COMMIT. It validates exact
integers above JavaScript's safe-integer range and the remaining cursor rows,
then closes clients and checks descriptor/memory recovery and joined shutdown.
Its limits are fixed before measurement and match the original small manifest.

The shared TLS implementation uses dynamic POSIX libc interposition on macOS and
GNU Linux. Static-musl targets retain Zig's original crash stacks and thread entry;
no dynamic-symbol lookup is introduced there. Linux qualification remains a
release requirement. Index reconciliation payloads/snapshots and restore fan-out
arenas use the persistent envelope, including memory held while their joined
operations finish. Each accepted index reconciliation attempt has its own finite
five-second authority-read deadline.

Native qualification also exposed a pgwire clock-domain bug: `Io awake` deadlines
were passed to native metadata owners using platform monotonic time. Pgwire now
converts the original remaining duration at ingress and preserves the native
absolute deadline through downstream work. Namespace validation also uses the
runtime error bridge rather than calling a cross-partition callback directly.

### Current verification receipt (2026-10-06)

The final integrated build passed all 149 steps. Pooling passed 42/42, pgwire
84/84, embedded DB 16/16, embedded API 8/8 and package surface 1/1. Protected
HTTP dispatch/allocation passed 10/10, client lifecycle 22/22, common HTTP
128 with one existing skip, and OpenAPI checks passed. The signal-stack fixture
handled an actual worker guard-page overflow on the full shared stack. The
serverless suite passed 547 tests with six existing skips; the final three
deadline/checkpoint tests also passed after the checkpoint refinement. Formatting
and `git diff --check` passed.

Both final native receipts use the same binary SHA-256 and unchanged 128 MiB
RSS growth/recovery limits:

- [Startup receipt](qualification/workload-pooling/macos-small-shared-tls-receipt.json):
  all 32 HTTP and 32 pgwire clients retained; 36.2 MiB idle RSS growth, 64
  additional threads and FDs, 199 ms maximum status latency, zero FD growth
  after close, and shutdown exit zero.
- [Authenticated cursor receipt](qualification/workload-pooling/macos-authenticated-cursors-receipt.json):
  32 authenticated pgwire sessions, all 16 held cursors verified across COMMIT,
  all 32 HTTP peers retained; 3.0 MiB idle RSS growth, 65 additional threads
  and 64 FDs, 197 ms maximum status latency, zero FD growth after close, and
  shutdown exit zero.

RSS depends on warm-up and concurrent reclamation: earlier checks of this change
measured approximately 50 MiB for startup and 60 MiB for authenticated cursors,
and also passed the fixed limits. These receipts qualify the specified small
macOS profiles; they are not main-versus-candidate throughput measurements or
permission to enable automatic query waiting. Linux, larger/slow-consumer and
distributed release profiles remain required.


### Resource accounting, controls and qualification harness (2026-10-06)

API foreground and persistent allocation budgets now report successful reservation
peaks and quota rejections, and charge an allocation-free borrowed parent port.
The native data server connects that port to the actual storage-kernel context in
linked composition, or the local ResourceManager in direct composition. The
control allocation reserve is pinned before API publication and returned only
after API owners join. Nested HTTP application/output scopes and native pgwire
protocol/runtime allocations use these API allocators; parent reservations are
counted once. Backing OOM, growth failure and free return charges. The compiled
storage-owner ABI regression verifies reserve/release changes the actual owner's
memory usage and restores the original value.

The [ownership ledger](qualification/workload-pooling/RESOURCE_OWNERSHIP.md)
records independent inference, raw transport, native and OS owners rather than
calling the storage-owner ceiling a complete process/RSS cap. The native DB
socket fixture exhausts real storage reservations alongside SQL foreground
workers and allocations, then verifies held-cursor COMMIT and failed-transaction
ROLLBACK. Distributed durable completion remains a separate qualification gate.

Authenticated GET/PUT `/admin/v1/workload/query` expose effective/startup policy,
version, queued bytes, wait outcomes/histograms, heap accounting and quiescence.
PUT can shrink/restore limits within startup reservations, suspend/resume, or
permanently drain using an expected version. Granted work remains owned;
shrinking retires excess newest waiters; quiescence includes notified waiters
until they retire. Verified operator requests use protected transport/application
capacity. Policies are node-local and reset on restart. Tests cover authorization,
stale versions, growth rejection, suspension, restoration, drain and ownership.

The [qualification harness](qualification/workload-pooling/README.md) adds an
open-loop finite generator, cheap/expensive queries, writes, delayed readers,
exact row validation, scheduled-offer latency, successful throughput including
drain, offered/dropped/error accounting, status responsiveness and recovery.
Baseline, candidate fail-fast and fixed-waiting variants run in isolated stores.
Receipts preserve binary/manifest identities, configuration and enforced cgroup
limits; comparisons reject missing/different identities, retries, failed phases,
latency regression and throughput loss. Linux small/large manifests and a manual
CI workflow use the same pinned toolchain/optimization for main and candidate.
Seven Python tests verify these gates fail closed.

Final macOS pooling tests passed 46/46; the linked storage-context test passed.
The preceding integrated run also passed resource budgets 92/92 plus its
filesystem fixture, pgwire 84/84 and protected HTTP 10/10. GNU Linux ran the real
worker guard-page overflow fixture successfully and passed all 56 compiled
pooling-root tests from a writable temporary directory. The first Linux build
attempt exceeded its declared compiler memory estimate and the read-only source
mount prevented test temporary-directory creation; those attempts are build/test
environment failures, not passing qualification receipts.

Historical macOS cold/warm comparisons retain both outcomes in
[receipt provenance](qualification/workload-pooling/receipts/2026-10-06/README.md).
The cold profile failed relative low-load p95. The distinct recorded warm profile
passed, with low-load p95 about 1% above main and sustained successful throughput
about 99% of main. These measurements precede the final linked-storage hook
correction and do not certify the final binary. Automatic defaults remain opt-in
pending coordinated whole-process ownership, distributed progress and all
required performance profiles; a passing warm run cannot erase a cold failure.


The corrected GNU Linux build/test targets passed 17/17 steps, 46/46 pooling
contracts and the native overflow fixture. Candidate and unmodified origin/main
CLIs built successfully using identical pinned AArch64 GNU Debug settings.
Fixed waiting passed small and larger authenticated idle profiles under enforced
2 CPU/2 GiB/512 PID/512 FD and 4 CPU/4 GiB/768 PID/1024 FD containers.
The equal-heap small mixed comparison retains a **failed** throughput gate:
fixed waiting achieved 82.5% of main and 80.3% of candidate fail-fast sustained
successful throughput. All absolute profiles, offered-work/no-retry gates and
low-load/recovery latency gates passed; main failed authenticated idle setup.
Raw receipts and server logs are preserved, including the earlier unequal-heap
comparison and its limitation. Default waiting remains disabled; do not tune
or relax an existing manifest to erase this result.


### Post-main integration and ReleaseSafe qualification

The branch includes origin/main e6d4ce9bbc. Managed local storage and inference
now borrow the linked storage context's allocation-free memory parent. Strict
reservations charge the parent before mutation; observers report actual existing
usage, even over budget; releases from sibling owners share the pressure event.
A separate bounded parent-reclamation callback runs outside child accounting
locks and preserves child ceilings, allowing inference to reuse sibling storage
cache capacity. The parent outlives child teardown, and consolidated memory reporting counts its
aggregate once. The public HTTP and pgwire transport allocator reserves a bounded
node pin before publication and releases it after listener/session joins. This
protects those heaps against exhaustion of other managed owners; exhaustion of
the transport pin itself still needs separate protected parser capacity.

The matrix now runs three repetitions with rotated variant order. Phase receipts
include operation-class throughput, client queue delay, CPU use, and independent
admission wait/lease-hold counter deltas. Runtime limits and relative release gates
are unchanged. These diagnostics are not substitutes for passing receipts.
Linux ReleaseSafe qualification uses the pinned Nix glibc-linked Zig package for
both variants and retains compiler version, store path, linkage and checksum.
The interrupted official-archive ReleaseSafe attempts do not qualify this
toolchain or justify increasing its compiler scheduling reservations.

The new LSM transaction regression checks safe rejection with a captured intent
batch under node exhaustion, preservation of pending intents, successful durable
retry, shutdown while competing node ownership remains held, and reopen with
committed data and aborted data absent. It does **not** prove immediate mutating
COMMIT under exhaustion: both intent enumeration and later write-path cursors need
storage resources outside the API control pin. A completion reserve must cover
the whole downstream operation before that guarantee can be claimed. Distributed
accepted-decision recovery remains a separate required receipt.

A second LSM regression persists terminal commit/abort records with unresolved
intents, rejects publication under node exhaustion while preserving the learned
decisions, and closes all owners while pressure remains held. After reopening,
recovery publishes the committed value and removes aborted intents without
presuming abort or repeating resolution. A second reopen verifies durable results.
This models a local participant's learned decision, rather than generating a
distributed coordinator decision. The two real six-node ReleaseSafe fault cases
passed with fresh queries suspended on every data node: owner restart and lost
accepted-resolution reply both completed the durable cascade and permitted
claim/reference reuse. Those schedules do not fill every distributed memory
owner or prove arbitrary transport-heap exhaustion.

Targeted post-main verification has passed 48 pooling cases, 98 resource and
transaction cases plus the filesystem fixture, and 12 harness tests. Both macOS
production ReleaseSafe binaries have built successfully. The original mixed-load matrix
passed all three comparisons against main but failed candidate fail-fast
low-load/recovery latency comparisons in two repetitions. Its fixed variant
recorded no completed admission waits. Separate one-slot, 750 requests/s profiles
require completed waiting and retain the original performance gates. All nine
macOS pressure profiles passed; fixed waiting recorded 1,304–2,045 completed
waits and passed every sustained throughput comparison. The matrix still failed
relative low-load/recovery latency in two repetitions. Both successful and failed
receipts are retained under `receipts/2026-10-06/release-safe-macos`. The linked
storage context ownership/ABI fixture also passes in ReleaseSafe,
and the GNU Linux signal-stack fixture passes with the pinned Nix compiler.
Linux qualification uses identical stripped ReleaseSafe settings after preserving
the unqualified unstripped attempt that exceeded the storage compiler claim.
Both Linux GNU ARM64 production builds against origin/main b79388a7d6 completed
all 45 steps using pinned Nix glibc Zig and stripped ReleaseSafe. The GNU
signal-stack fixture and all 99 resource/transaction/filesystem cases pass.
The first small/large matrices failed before measurement in both variants:
the default ChatGPT credential store attempted `fchmod` on an `O_PATH` directory
handle. The focused readable-handle fix retains `0700` and is applied identically
to main and candidate, with explicit patched-baseline provenance. The credential
fixture passes on macOS and with unstripped ReleaseSafe Linux tests; stripped
Linux test compilation crashes in Zig. Production binaries remain stripped.
Startup failures and compiler diagnostics are retained under
`receipts/2026-10-06/nix-glibc-release-safe-b793`.
The startup-fixed final candidate passes all 30 small/large/mixed/waiting/combined
absolute profiles. All nine fixed/fail-fast comparisons and six comparable
fixed/main load comparisons pass unchanged performance gates. Main fails
idle/combined pgwire setup, so combined-versus-main timings remain unavailable
and the raw matrices fail. Waiting remains opt-in; these are not automatic
default qualification receipts.

The parser/control and outbound/fan-out accounting delivery has passed 54 native
ReleaseSafe pooling cases, 12 protected HTTP cases and 12 connection-helper cases.
The corrected pgwire target explicitly imports its protocol module; earlier
focused two-test summaries ran anonymous imports, although the broad pooling
suite did run the two original protocol cases. Three named startup/frame/control
cases and all 87 pgwire cases now pass on macOS and GNU Linux.
The combined-pressure diagnostic smoke also exposed and corrected a startup
ParameterStatus allocation/free owner mismatch before rebuilding the candidate.
The pressure harness passes
14 tests; the test-selection auditor passes 10. The linked data-runtime target
passes all 91 build steps and its forwarding fixture with the read-client heap
exhausted and the control executor saturated. The generated control catalog now
exports bounded admission after the main merge, and partition inventories are
inserted before runtime arguments so linked selections retain their strict audit.
The new Linux combined-pressure manifest retains authenticated connections,
cursors and unfinished uploads throughout mixed writes and slow-reader load.
Its latency, throughput, resource and recovery thresholds are unchanged.
The first 16-slot combined profile retained every candidate client but correctly
failed its completed-wait gate. A stricter one-slot rerun preserves all load,
client/resource settings and thresholds; all six candidate runs and all three
fail-fast comparisons pass, with 1,548–1,587 completed sustained waits. Original
failures and the exact workload change remain in the receipt ledger.
These checks establish the named parser/control and allocation-owner contracts;
they do not establish allocation-free physical mutating completion.

After merging main at `ac2d3779ee`, the pgwire setup failure was traced to
main's 16-slot shared API executor: each peer owns three tasks, so five idle
peers plus accept fill it. The candidate's listener-owned transport is now
initialized through the same helper exercised by a saturation regression.
All 88 pgwire cases pass on macOS and pinned Nix glibc GNU Linux ReleaseSafe.
The merged native API pooling target passes 55 selected contracts and all 50
build steps.
CI retains a focused baseline repair with capacity for main's existing three
tasks per peer, without importing candidate SQL admission or allocation policy.
The [merge verification receipt](qualification/workload-pooling/receipts/2026-10-06/main-ac2d-pgwire/README.md)
records the regression and baseline patch. Historical failed matrices remain
failed; this merge has no new full production performance receipts.


### Runnable scheduler implementation

The [runnable scheduler contract](WORKLOAD_RUNNABLE_SCHEDULING.md) records separate
operation, execution, retained-memory and durable-completion ownership. The core
now implements weighted slot service, bounded backfill with a starvation barrier,
reserved control slots, and explicit yield/resource/output-block transitions.
Outstanding reservations persist while blocked, so a resumption never needs a
new queue reservation. Deadline and cancellation options survive every transition.
It is exported through request admission and selected by the pooling target.
Production protocol integration, cooperative execution quanta, distributed
attempt accounting, qualification and automatic defaults remain incomplete.

Ten named scheduler fixtures plus the exported module's import fixture pass on
macOS and pinned Nix glibc GNU Linux ReleaseSafe. The shutdown/block race was
corrected before final verification. The [core receipt](qualification/workload-pooling/receipts/2026-10-06/runnable-core/README.md)
preserves source hashes and complete outputs. The expanded whole API pooling
target has not yet been executed for this scheduler change.

The next integration adds a stable single-worker execution capsule, cooperative
time/checkpoint quanta, and reserved control metadata inside the total scheduling
envelope. Thirteen named scheduler fixtures plus the import fixture pass on
macOS and pinned Nix glibc GNU Linux ReleaseSafe; the [execution-core receipt](qualification/workload-pooling/receipts/2026-10-06/runnable-execution-core/README.md)
records those results separately from the earlier core receipt. The source
boundary audit still passes for 734 sources.

Canonical opt-in configuration and node-owned HTTP/pgwire SQL read integration
are implemented in the current worktree and under whole-pooling-suite
verification. Page streams park between pulls while retaining their operation,
snapshot and memory owners. Verified protected controls use reserved runnable
capacity. Scheduled SQL temporarily uses serial backend execution until parallel
children have independent runnable ownership. Genuine backend wait suspension,
other query adapters, ordinary writes, inference/background scheduling, remote
attempt accounting, release qualification and automatic defaults remain required.
The core test receipt does not establish production protocol behavior.

The exported scheduler plus telemetry now passes sixteen tests on both macOS
and pinned Nix/glibc GNU Linux, using the real runtime ABI root. The local pooling
shard separately passes all 31 selected tests, including canonical runnable-policy
parsing. The [telemetry receipt](qualification/workload-pooling/receipts/2026-10-06/runnable-metrics/README.md)
records tested source hashes, complete outputs, compiler provenance and the
735-source authored boundary audit. Telemetry is wired through API ABI version 22
to data-node and standalone metrics. Full protocol/endpoint verification is still
pending; the new protected rollback fixture's retained-state allocator mismatch
was corrected before its current rebuild. These results do not qualify defaults.


Scheduled reads now retain serial disk spilling independently of parallel CPU
execution. The blocking continuation fixture passes sorting, grouped aggregation
and window queries in both modes on macOS and Nix/glibc GNU Linux; bounded pages,
retained-memory limits, cancellation and spool cleanup remain checked. All spill
CPU submissions use the manager's parallel-work gate. The first full native SQL
run passed 263 of 264 tests without leaks, exposing nondeterministic in-place
allocator growth in the exhaustive pull-stream fault test; its focused rerun
reproduces that failure. The API protocol rebuild and corrected exhaustive SQL
verification remain pending. This evidence does not qualify automatic defaults.


The corrected pull-stream allocation sweep passes in the full 264-test local SQL
shard on macOS without leaks. Linux passes both that shard and all 118 server SQL
tests; its clean full-target rerun passes all nine build steps. Compiler memory
estimates now reflect measured peaks of 1.93 GB on macOS and 1.95 GB on Linux,
without changing executor runtime budgets. The macOS server shard exposes a
plan-cache fixture whose allocator growth varies during eviction; deterministic
growth passes a standalone reproduction at the original cache/compile bounds.
The full native API pooling target passes 33 local and 38 API contracts but reports
an eight-byte leak in the new rollback fixture. Its caller arena ownership is
corrected and the rebuild is pending. The [serial-spill receipt](qualification/workload-pooling/receipts/2026-10-07/serial-spill/README.md)
preserves initial failures, corrections and complete verification outputs.


The corrected native API artifact passes all 38 selected contracts with loopback
sockets enabled and no skips, failures or leaks; the local pooling shard passes
all 33 contracts. The full native SQL target passes all 264 local and 118 server
SQL tests without leaks and succeeds in all ten build steps. The cache eviction
fixture keeps its original hard bounds with deterministic allocate/copy growth.
These checks qualify opt-in native SQL read integration; full Linux API and
runtime metrics endpoints remain required, alongside genuine backend waits,
parallel child ownership, wider workload coverage and automatic default sizing.


Private spill reads and writes now relinquish runnable ownership around positional
file I/O and reacquire before decoding or propagating I/O errors. The full SQL
corpus passes 267 local and 118 server contracts on both macOS and pinned
Nix/glibc GNU Linux. One fixture runs ten stalled read/write schedules; explicit
module inclusion and a strict selection check corrected an initially empty
focused run. The [spill-wait receipt](qualification/workload-pooling/receipts/2026-10-07/spill-waits/README.md)
preserves both the non-qualifying selection and the complete passing schedules.
The authored boundary now covers 736 sources. New API hook qualification remains
pending; storage/RPC and directory waits, fresh policy/catalog fences, independent
parallel children, broader workload classes and automatic defaults remain required.


The isolated a19cf06f23 Linux API artifact passes all 38 protocol contracts in a
separate 2 GiB runtime container; its local pooling shard passes all 33 tests. The
initial build failed its obsolete compiler estimate and read-only test scratch
setup, both preserved in the serial-spill receipt. The measured 24.11 GB compiler
peak updates the Linux compilation estimate to 26 GiB while runtime qualification
limits remain unchanged. This snapshot precedes the private spill wait hooks;
their full native API verification is now running separately.


The private spill wait hooks now pass the complete native API pooling target:
33 local and 38 API contracts without skips, failures or leaks, and all 50 build
steps succeed. Full Linux API qualification of the new hooks is the next gate.
The documented hard original cursor snapshot deadline is unchanged; buffered
rows still need fresh policy/catalog validation after cooperative resumption.


The private spill wait hooks also pass the complete Nix/glibc GNU AArch64 API
target: 33 local and 38 API tests, no skips or leaks, and all 47 build steps.
Both exact compiled artifacts pass again under a separate two-CPU, 2 GiB runtime
limit without extra swap. The isolated `d5baee40e5` snapshot required only a
build estimate adjustment from 26 to 23 GiB: 26 GiB exceeded available VM memory
before compilation. The [spill-wait receipt](qualification/workload-pooling/receipts/2026-10-07/spill-waits/README.md)
preserves that scheduling rejection, the successful build, and bounded runtime
results. These checks do not enable scheduling by default.

Current credential policy snapshots now capture permissions, layered row
filters, transitive roles, metadata, and persistent user identity under one
mutation lock. They carry no credential verifier and do not authenticate the
caller. The standalone UserManager target passes on macOS and Nix/glibc GNU
AArch64, including revocation, deleted/recreated account identity, key expiry,
and exhaustive allocation-failure cleanup. The [policy-snapshot receipt](qualification/workload-pooling/receipts/2026-10-07/policy-snapshot/README.md)
records the source and test evidence. Full HTTP/PG resume integration, fresh
catalog fencing, broader backend waits, independent parallel-child ownership,
all-workload scheduling, and automatic defaults remain required.


Shared read authority now covers scheduled HTTP SELECT/EXPLAIN and retained PG
SELECT initial reads, cooperative resumes, private spill resumes and result
handoff. Immutable bounded pins retain table/schema/catalog and native RLS serving
generations; current managed credentials are checked against admitted scope.
API-key ingress preserves persistent account identity, and physical projection
preserves explicit null user-filter overrides. Three embedded guard fixtures and
22 coordinator schedules pass on macOS and pinned Nix/glibc GNU AArch64 alongside
UserManager/adapter/archive tests. The full expanded native API target passes all
75 selected tests and 50 build steps without skips or leaks. Full expanded Linux
API qualification is running separately. The [read-resume receipt](qualification/workload-pooling/receipts/2026-10-07/read-resume-fences/README.md)
records exact source hashes and platform evidence. General storage/RPC/directory
waits, external credential refresh, parallel children, preparation/serialization,
all-workload scheduling, remote attempt completion and automatic defaults remain
required for the full design.


Private directory acquisition and file creation/unlinking now suspend runnable
ownership around filesystem operations, alongside existing positional spill I/O.
Owned wait results are discarded if execution reacquisition or the post-resume
authority checkpoint fails. Mandatory cancellation-protected cleanup closes
handles and removes private artifacts without depending on a new scheduler grant.
All 269 embedded and 118 coordinator SQL tests pass without skips or leaks on
macOS and pinned Nix/glibc GNU AArch64, including seven real creation/retirement
schedules and the existing ten stalled positional I/O schedules. The
[creation receipt](qualification/workload-pooling/receipts/2026-10-07/spill-creation-waits/README.md)
records source hashes, complete logs and the 737-source production boundary.
Whole API qualification of the new creation hooks remains required. General
storage/RPC waits and cleanup execution ownership, independent parallel children,
preparation/serialization, wider workloads, uncertain remote completion and
automatic default qualification remain incomplete.


The expanded read-resume fence API target now passes all 36 embedded and 39
coordinator contracts on pinned Nix/glibc GNU AArch64, with all 47 build steps.
Both exact compiled artifacts subsequently pass again in a separate two-CPU,
2 GiB container without extra swap, including all 22 coordinator authority
schedules. The [read-resume receipt](qualification/workload-pooling/receipts/2026-10-07/read-resume-fences/README.md)
preserves source/artifact hashes, complete logs and explicit zero runtime exit
statuses. This qualification snapshot precedes private spill creation waits;
whole API verification of those newer hooks is running separately on macOS.


Whole native API qualification of private spill creation waits now passes all
36 embedded and 39 coordinator pooling tests without skips or leaks, and all
50 build steps. The exact-source [creation receipt](qualification/workload-pooling/receipts/2026-10-07/spill-creation-waits/README.md)
preserves the complete output. Full Linux API verification also passes all
47 build steps and 75 tests. Both exact GNU AArch64 artifacts pass again in a
separate two-CPU, 2 GiB container without extra swap, with explicit zero exit
statuses and no skips or leaks. The receipt records artifact hashes and exact
arguments; this snapshot precedes request-local read-index wait integration.


The checked native execution-wait interface is now qualified through a separately
compiled callback owner on macOS and pinned Nix/glibc GNU AArch64. All three wait
fixtures and 25 error ABI fixtures pass, including foreign dispatcher selection,
version/layout rejection, exact scheduling/authority error identity and original
caller heap cleanup after a foreign resume failure. Scheduling details append to
the existing error ABI without renumbering previous outcomes. The
[native wait receipt](qualification/workload-pooling/receipts/2026-10-07/storage-wait-boundary/README.md)
preserves matching post-format source hashes, owner artifact hashes and complete
commands/logs. The explicit embedded boundary covers 738 sources. Actual storage
wait propagation, parallel children and wider workload integration remain required;
this interface does not by itself release capacity in those backends.


Request-local SQL scan waits now reach the DataServer read-index barrier through
the checked callback interface. Only actual owner-lock and quorum/apply polling
sleeps relinquish runnable ownership. Registration and retries retain one stable
request context; failed resumption cancels its tracker registration through the
original cleanup owner. Scheduled group admission remains serial, and long-lived
retained remote cursors clear the borrowed callback before opening physical
views. Dynamic snapshot bootstrap and other storage/RPC waits remain pending.

All 269 embedded SQL, 118 coordinator SQL and 13 read-gate contracts pass on both
macOS and pinned Nix/glibc GNU AArch64. The six controlled event-wait schedules
prove cancellation, expiry, shutdown, I/O failure and authority failure without
stranding slots or metadata. They do not prove real DataServer quorum/apply delay.
Actual DataServer owner-lock and retained-owner poison-callback integration also
pass on Linux, including exact artifacts under two CPUs and 2 GiB, as recorded
below. Native DataServer owner-lock execution remains pending. Three checked callback and all 25
error ABI tests also pass on both platforms with exact updated catalog hashes.
The [read-index receipt](qualification/workload-pooling/receipts/2026-10-07/read-index-waits/README.md)
preserves initial and corrected snapshot manifests and complete logs.

Whole native CLI qualification found a missing generated read-fence export in
the control catalog and a scheduler field passed to an unsupported serverless
bootstrap. The export is regenerated; serverless explicitly rejects that opt-in
policy while its execution owner awaits integration. The initial distributed
runtime archive compiled successfully with the new wait hooks, but the complete
CLI initially failed on those two configuration issues. The corrected complete
native production build now passes all 56 steps against `7e7232ce73`; the
[production receipt](qualification/workload-pooling/receipts/2026-10-07/sql-preparation/native-production.json)
pins its source and installed CLI artifact hashes. Linux production qualification
and serverless scheduler integration remain pending. The Linux SQL compiler reservation reflects its
observed 2.31 GB peak; executor runtime limits remain unchanged.


Actual Linux DataServer qualification now passes all six read-safety contracts
and all 86 build steps. The four scheduled owner-lock failures preserve the
original deadline, allow ordinary/protected progress, and cancel the real tracker
registration with zero remaining slots/operations/metadata. The exact artifact
passes again under two CPUs and 2 GiB without extra swap or a compiler; the
verifier requires every failure marker and six tests, with no skips/leaks and a
zero exit status. The [read-index receipt](qualification/workload-pooling/receipts/2026-10-07/read-index-waits/README.md)
preserves the exact artifact hash, commands and complete logs. Live stalled
quorum/apply transport, native owner-lock execution and Linux production/API
qualification remain separate requirements. Complete native production and the
preparation/API snapshot are qualified separately below.


Linux routed-read/retained-owner verification passes all 119 selected tests,
including both registry lifetime contracts and the borrowed-wait poison checks.
Three exact artifacts also pass under two CPUs and 2 GiB without extra swap,
with required test counts/names, matching artifact hashes, zero exit statuses
and no skips/leaks. The [read-index receipt](qualification/workload-pooling/receipts/2026-10-07/read-index-waits/README.md)
preserves the full logs. Compiler estimate overruns remain visible; corrected
26 GiB storage and 21 GiB routed-implementation reservations were attempted in
the isolated build snapshot, but Zig correctly declined the 26 GiB step against
the Docker VM's 23.43 GiB available budget. That attempted graph is not a passing
production qualification. Native preparation/API work is verified separately.


SQL preparation now acquires runnable capacity before parsing, plan-cache access
and Describe in both HTTP and pgwire. It releases that grant before coarse
execution admission; protected controls use reserved capacity. Pgwire read setup
uses a stable request owner before setup allocations, and ordinary Describe
also acquires execution capacity. The mixed protocol fixture verifies queued
preparation makes no new catalog calls while ROLLBACK progresses. All 75 selected
native API contracts pass, with no skips/leaks and all 50 build steps successful.
The [preparation receipt](qualification/workload-pooling/receipts/2026-10-07/sql-preparation/README.md)
records source hashes and complete output, including a complete 56-step native
production build. Linux API/production qualification and production qualification
of subsequent write changes remain pending. Parser quanta, exact preparation wait suspension,
serialization ownership and broader execution classes remain separate work;
this phase does not enable scheduler defaults.


Ordinary HTTP/pgwire SQL execution now acquires write-class runnable capacity,
and HTTP settings commands acquire query/write capacity by classification.
Writes retain their grant through worker completion and accepted durable work.
Their native backend runs serial CPU work with spill I/O, withholding read-only
suspension callbacks. This prevents unscheduled SQL CPU children while preserving
durable completion. Write wait suspension, mutation quanta and independently
owned durable completion/parallel children remain required before defaults.

All 75 selected native API contracts pass with zero skips/leaks and all 50 build
steps successful. The mixed protocol fixture verifies read-committed BEGIN and
protected ROLLBACK grant ownership; unsupported default serializable BEGIN
preserves 501/0A000 and retires its grant. Native HTTP INSERT/RETURNING and
credentialed pgwire INSERT/RETURNING and DELETE/RETURNING verify write-class
grants and zero remaining slots, operations and metadata. Existing joined UPDATE
and normalization cases run with scheduling enabled. The
[write receipt](qualification/workload-pooling/receipts/2026-10-07/write-runnable/README.md)
records the initial fixture error and corrected source-pinned run. Linux
API/runtime, production qualification of this write snapshot, and stalled
accepted mutation schedules remain pending. Broader execution classes and
automatic defaults remain required for the overall design.


The combined preparation/read-index/write API snapshot also passes pinned
Nix/glibc GNU AArch64 qualification: all 47 build steps and all 75 selected
contracts, without skips/leaks or a compile memory overrun. This whole API
harness does not link the production storage archive, so its unchanged 23 GiB
compiler allowance fits the VM's default budget. Separate storage-linked graphs
retain the 26 GiB admission constraint. Both exact API artifacts pass again under
two CPUs and 2 GiB without extra swap or a compiler, with matching ELF/SHA-256,
required fixture names/counts, zero exits and no skips/leaks. The
[write receipt](qualification/workload-pooling/receipts/2026-10-07/write-runnable/README.md)
preserves commands, compiler/image hashes and complete build/runtime logs.
Complete production qualification of the write snapshot and broader scheduler
ownership/default rollout remain pending.

### Complete GNU write-integration production build (2026-10-07)

Frozen source `2cf1393dfc` completes all 53 GNU AArch64 production build steps,
including all seven native archives and the installed CLI. The pinned Nix/glibc
compiler used an explicit 26 GiB compile scheduling allowance on the unchanged
23.43 GiB Docker VM; this is not default-budget qualification. The exact ELF
also passes `--help` under two CPUs and 2 GiB with no extra swap. The
[production receipt](qualification/workload-pooling/receipts/2026-10-07/write-runnable/README.md)
records complete output and source/compiler/artifact hashes. Full service
pressure, the newer isolated provider/shared-owner candidate, and native
post-write whole production qualification remain pending.

### Shared node runnable owner and provider checkpoints (2026-10-07)

Standalone opt-in scheduling now creates an opaque API-kernel-owned scheduler
at node lifetime and passes a borrow to its API. Checked factory/lifecycle
exports preserve allocator ownership, refuse destruction with live borrowers
or executions, and close before node consumer joins. API teardown captures its
owner pointer before poisoning the server and releases the borrow after joins.
Scheduled SQL provider frontend work remains on its parent worker and reaches
read execution checkpoints before/after invocation and before normalization.

All 50 native pooling API build steps and 78 tests pass (37 embedded, 41
coordinator), with no skips or leaks. The core scheduler also passes all 14
contracts on native macOS and pinned Nix/glibc GNU AArch64; two layout/prefix
checks pass on both platforms. The
[owner receipt](qualification/workload-pooling/receipts/2026-10-07/shared-node-owner-abi/README.md)
retains passing evidence and the initial assertion/teardown failures. GNU API,
separate archive execution and whole-production startup/shutdown of the new
wiring remain pending. Inference/background execution admission, inherited
provider grants across the inference boundary, completion ownership, pressure
qualification and automatic defaults remain required.

### Weighted execution capsules (2026-10-07)

The stable execution capsule can now admit explicit slot demand while keeping
its original count, memory and deadline owners across yields, waits and class
transitions. Existing callers remain one-slot. Atomic demand rejection has an
append-only stable error ABI detail. All 15 scheduler and 26 error-ABI contracts
pass natively and with pinned Nix/glibc GNU AArch64; the
[weighted receipt](qualification/workload-pooling/receipts/2026-10-07/weighted-execution/README.md)
pins source bytes and complete output. Cross-archive execution operations,
inference/background callers and CPU-team enforcement remain pending, along
with the full API/production gate for these bytes and automatic defaults.

### Checked execution port (2026-10-07, native API gate passed)

A process-local C execution port now keeps weighted capsules inside the API
kernel owner. Foreign consumers receive checked start/checkpoint/park/enter/finish
callbacks and stable statuses; consumer leases and every live capsule prevent
owner destruction. Executor receivers, cancellation and clocks stay borrowed
through finish. Each capsule charges its own metadata, preserves original
deadlines and demand, and releases the owner only after retiring all state.
The append-only optional API table capability checks the new tail before access.

Five real scheduler capsule tests, 33 protocol/imported ABI tests, three
API layout/prefix tests and two inference ABI checks pass on native macOS and pinned Nix/glibc GNU AArch64
(43 per platform). The [execution port receipt](qualification/workload-pooling/receipts/2026-10-07/execution-port/README.md)
pins source and full contract logs. The complete native API gate passes all
50 steps and 84 tests (38 embedded, 46 coordinator), with no skips/leaks,
including factory/getter/wrapper behavior. The initial restricted fixture
failures are preserved alongside the unchanged authorized passing rerun. Production linkage,
standalone startup/shutdown of these bytes, inference/background consumers and
CPU team enforcement remain pending; automatic defaults are still disabled.

Inference configure now optionally borrows the API execution port and retains
a consumer lease through worker and node teardown. Failure during configuration
returns that lease. Configure ABI version 28 rejects old layouts before touching
the new field; standalone supplies the port only when its node owner exists.
The lease primitive is qualified against the real scheduler, including refused
destruction and shutdown retention rejection. Full inference configure/destroy
integration and actual execution admission still require their complete gates.
SQL parent grant inheritance/transfer must precede any nested inference admission.

### Shared owner GNU production and small clients (2026-10-07)

Frozen f9e1e0c37d passes all 53 GNU AArch64 production build steps with pinned
Nix/glibc Zig and seven independently compiled archives. Its explicit compiler
scheduling budget is 26 GiB on the unchanged 23.43 GiB Docker VM. The exact
artifact passes authenticated small-client qualification with a two-slot shared
node owner: 32 HTTP/32 pgwire connections, held cursors/extended portals, recovery
and clean SIGINT shutdown, under two CPUs and 2 GiB without extra swap. Control
latency is 0.0521 s and all thresholds pass. The
[production receipt](qualification/workload-pooling/receipts/2026-10-07/shared-node-owner-production/README.md)
pins commands, source/artifact hashes, logs and limits. This proves opt-in owner
startup/shutdown across production archives, not later execution-port/inference
lease bytes, CPU teams, sustained overload, completion stalls or defaults.

### Borrowed synchronous phase transfer (2026-10-07)

A checked C borrow transfers the parent's single-worker operation between
classes without a nested admission or a new deadline. Explicit return uses the
parent's resumption callback; the caller retains final capsule/lease ownership
even when failed reacquisition retires the scheduler entry. Seven real scheduler
capsule tests and 34 protocol/imported ABI tests pass on native macOS and pinned
Nix/glibc GNU AArch64, including full ordinary demand alongside control progress,
stale deadlines, shutdown and malformed inputs. The
[transfer receipt](qualification/workload-pooling/receipts/2026-10-07/borrowed-transfer/README.md)
pins source and output. Production SQL/inference propagation and backend CPU
team enforcement are still pending; this primitive alone does not claim
inference execution is bounded. Full API/production gates and defaults remain
required. Parallel children must own independently admitted capsules.

### Scoped native CPU teams (2026-10-07)

Native linalg dispatch now supports an admitted per-worker CPU scope. Explicit
Io jobs run in bounded waves; children get one worker and cannot expand nested
kernels. Sync dispatch follows the same cap, while process capacity and BLAS
initialization remain independent of scoped requests. System BLAS is bypassed
under a scope; OpenBLAS is used only when its measured team fits. Scopes are
synchronous and stay on one OS thread.

Native/GNU scope, matrix-reference, real native backend and existing matrix
regressions pass, including the macOS system-BLAS configuration. All nine GNU
pool/CPU-budget tests also pass after moving temporary fixtures to writable
cache storage. The [CPU worker receipt](qualification/workload-pooling/receipts/2026-10-07/admitted-cpu-workers/README.md)
records sources, flags, full logs, limits and the initial read-only fixture
failure. Production invocations do not yet install scopes. Parent propagation,
actual broker/worker grants, other backend teams, whole-production integration
and automatic default qualification remain pending.
### Remote provider resource waits (2026-10-07, native API gate passed)

Decision provider remote HTTP calls now park a scheduled read parent's CPU grant
while retaining operation/memory/deadline ownership. The SQL adapter reuses its
read-fence resumption path; failed reacquisition or authority validation discards
the response. Real HTTP tests verify another query and control work progress
during the wait and successful, rejected, HTTP 429, shutdown and disconnected
transport outcomes all retire charges. All 47 native steps and 25 focused tests
pass; GNU also passes all 44 steps and 25 tests under two CPUs and 4 GiB
without extra swap. The [resource wait receipt](qualification/workload-pooling/receipts/2026-10-07/provider-resource-waits/README.md)
retains source hashes and full logs. The combined native API gate also passes
all 50 steps and 85 tests (39 embedded, 46 coordinator), with no skips/leaks,
including actual adapter callback wiring and the real transport fixture. Local inference grant transfer,
backend CPU teams, background scheduling and default qualification are still
required. Ordinary accepted writes do not attach the cancellable read callbacks.

### Physical inference broker admission boundary (2026-10-07)

Singleton calls and native groups share an optional immutable execution
admission boundary outside shard locks. Collective control preserves live
peers; callers canceled during admission are removed before backend work.
Rejected admission, all-canceled cohorts and staging allocation failure release
acquired grants. All 21 broker tests pass on native macOS and pinned Nix/glibc
Linux; the [broker receipt](qualification/workload-pooling/receipts/2026-10-07/broker-admission-boundary/README.md)
pins source and full logs. Production owner/CPU wiring, parent wait handoff,
complete retained-memory accounting and whole-inference qualification remain
pending. This boundary alone does not bound production inference execution.

### Inference owner adapter and real handoff gate (2026-10-07)

A broker provider now acquires checked inference capsules and installs a
one-CPU native scope on the executing worker. Collective cancellation retains
per-member deadlines; completion restores scope and retires the capsule.
Both platforms pass 22 adapter/broker and 8 real owner/integration tests;
[adapter receipts](qualification/workload-pooling/receipts/2026-10-07/inference-owner-adapter/README.md)
pin source, arguments and full output. The real scheduler proves parent parking,
inference admission, concurrent control progress and parent reacquisition.
Production Node/host installation requires propagating that handoff first:
current embedded calls retain the SQL grant and nested admission could deadlock.
Worker-process transfer, other backend teams, complete memory accounting and
full production/default qualification are still required. The adapter is not
installed by production yet.

### Local inference parent phase (2026-10-07, final native API gate passed)

SQL decision calls now propagate a checked borrowed execution descriptor through
RequestContext and provider bridge ABI v29. The local host transfers the same
operation to inference, installs its native CPU scope, and restores the parent
through original deadline and read-fence checks. Failed return discards owned
JSON/numeric responses. Borrowed invocations use a non-fan-out executor so task
pipelines cannot migrate CPU work off the admitted worker. Native and Nix/glibc
Linux focused host tests pass; the earlier full native API gate passed 87 tests
before the executor addition. The [parent phase receipt](qualification/workload-pooling/receipts/2026-10-07/local-inference-parent-phase/README.md)
records the evidence. The final combined native API gate also passes all 50
steps and 87 tests, with no skips/failures/leaks and exit 0, after a verified
interrupted run and unchanged-source rerun. Source hashes match the completed
gate. This ABI v29 gate does not qualify the separate worker-protocol candidate. At this historical ABI v29 source, subprocess
borrowed calls required protocol handoff and failed before transfer; the ABI v30
worker delivery below provides that handoff.
Independent broker admission, fused waiting, broader backend teams and full
production/default qualification remain required; defaults are disabled.

### Worker CPU budget handoff (2026-10-07, focused gates passed)

Provider ABI v30/private worker protocol v6 now carry CPU demand while the
parent keeps its checked operation. Callback pointers never enter IPC. The
worker validates demand before payload preparation and installs its own native
scope and non-fan-out executor. Conflicting/out-of-range demand and worker
relays fail closed. All seven focused host/wire tests pass on native macOS and
Nix/glibc Linux; [worker budget receipts](qualification/workload-pooling/receipts/2026-10-07/worker-cpu-budget-handoff/README.md)
pin source and output. Production archive and real subprocess probes now pass
on both platforms, as recorded below. Whole API/default overload gates remain required.
The separate local-parent ABI v29 gate does not qualify these worker bytes.

### Production worker handoff (2026-10-07, native and GNU probes passed)

The complete production inference archive and exported C probe pass all 23
native build steps. The probe covers provider-tail rejection, demand validation,
delegated scope, borrowed restoration and authority failure. A second 23-step
gate re-execs an actual private worker through the production process ABI:
CPU demand travels over real RPC, invalid demand is rejected by the child,
parent ownership is restored and reaping clears resource-owner references.
[Native production receipts](qualification/workload-pooling/receipts/2026-10-07/production-worker-native/README.md)
pin source, commands and logs. These malformed-provider probes verify plumbing,
not model team peaks or overload fairness. Corresponding Nix/glibc production
gates also pass all 23 steps; ABI v30 full API and combined startup/default gates
remain required. Defaults are still disabled.

The ABI30 production archive and subprocess probes now pass on both native macOS and pinned Nix/glibc aarch64-linux-gnu. The GNU run validates all seven recorded source hashes and retains its exit-0 result and full log beside the native production-worker receipts. Numerical model execution and combined overload/default qualification remain pending.

### Owning-thread inference checkpoints (2026-10-07, focused gates passed)

Borrowed local provider phases now lend their scheduler checkpoint to the owning OS worker through a synchronous TLS scope. Existing inference execution checks can yield/check original parent authority; watchdogs and parallel children do not receive the borrowed callback. Nested scopes restore in stack order, and callbacks are removed before returning the grant. All eight focused host/wire contracts pass on macOS and pinned Nix/glibc Linux; [receipts](qualification/workload-pooling/receipts/2026-10-07/inference-owning-checkpoints/README.md) retain hashes, commands, full logs and the initial mountpoint failure. These focused contracts do not qualify model numerical work, subprocess physical scheduling, other resource waits or automatic defaults. Full API and production qualification must use these newer source bytes.

Inference HTTP response ownership follow-up: the consumer now installs the host response destructor before its post-dispatch original deadline/cancellation check. A rejected publication therefore retires the host-owned response rather than leaking it. Missing-response errors retain the cancellation/deadline precedence. Formatting and diff checks pass; combined integration/production qualification of these newer consumer bytes is pending.

The native `antfly-inference-response-cleanup-test` gate now passes the post-dispatch cancellation regression with exit 0 and no leaks. The real host handler allocates a response and then cancels the original invocation; the production consumer rejects publication and retires that response and its lifetime guard. [Receipts](qualification/workload-pooling/receipts/2026-10-07/inference-http-response-cleanup/README.md) pin five source hashes and complete output. Linux and consolidation with newer inference wait scopes remain pending.

### Inference model-lock waits (2026-10-07, focused contracts passed)

Contended model gates now park the synchronous borrowed provider grant and reacquire its inference class through the original authority callback before work continues. Cancellation checks remain active while parked; runnable-only checkpoints are suspended. Failed reacquisition stops the child phase, leaves retirement to its owner, unlocks any acquired model gate, and discards responses even if backend code swallows the error. All nine host/wire contracts pass on native macOS and pinned Nix/glibc Linux; [receipts](qualification/workload-pooling/receipts/2026-10-07/inference-model-lock-waits/README.md) record scope and source. Real scheduler/control-progress metrics, numerical model contention, other wait boundaries and full API/production/default qualification remain pending.

### Real inference model-lock owner gate (2026-10-07, both platforms passed)

`antfly-inference-owner-handoff-test` now provides a reusable minimal contract gate. All nine tests pass on native macOS and pinned Nix/glibc Linux. A real two-slot API capsule parks while retaining its bytes; a query and protected control acquire capacity during that wait; normal resume restores the original demand, while cancellation and expiry of the original deadline reject reacquisition. Final cleanup drains all operation, CPU and byte counters. [Receipts](qualification/workload-pooling/receipts/2026-10-07/inference-model-lock-owner/README.md) pin source and commands. This closes the earlier callback-only accounting gap at the model-lock boundary, while numerical model, worker/cohort, other wait and consolidated production/default qualification remain outstanding.

### Nested admitted inference wait scopes (2026-10-07, focused gates passed)

An independently admitted child scope can run checkpoints and resource waits while its caller remains parked, then restore that caller's suspended state. Resource waits use guarded checkpoints to avoid recursive yields through collective/member cancellation checks. Combined host/wire and real-owner gates pass all 18 tests on native macOS and pinned Nix/glibc Linux; [receipts](qualification/workload-pooling/receipts/2026-10-07/inference-nested-waits/README.md) record both runner summaries and five source hashes. Production broker admission, numerical/workload fairness, worker physical waits and consolidated API/production/default qualification remain pending.

### Broker result publication ownership (native and GNU focused gates passed)

Owned result slots now dispose allocated values exactly once when final member or admission fences reject publication. Original request cancellation also fences completed physical waves, while late I/O executor cancellation retains joined caller cleanup. All 23 broker/adapter tests pass on native macOS and pinned Nix/glibc GNU Linux; [receipt](qualification/workload-pooling/receipts/2026-10-07/broker-result-ownership/README.md) preserves the initial expectation failure and final passing output. Production owning producers were converted but are not compiled by this focused gate; production/numerical qualification remains required.

Provider context adapter follow-up (native integration passed): dense text/parts and raster embedding, text/document reranking, and both text and binary chunking now pass the complete RequestContext into the inference ABI. Controls-only public helpers retain their signatures and delegate to shared context implementations. A model-free integration regression covers seven affected calls, requiring a denied parent checkpoint to stop before any transfer or model work and checking that provider lifetime borrowers are released. This does not yet qualify numerical model execution, other provider contexts, or default enablement.

The [provider context receipt](qualification/workload-pooling/receipts/2026-10-07/provider-context-propagation/README.md) records all 69 passing native steps and the seven-adapter regression, with no skips, failures or allocator leaks. Linux and combined-source qualification remain pending.

### Owning broker checkpoint and wait handoff (both focused platforms passed)

The inference owner provider now installs the shared owning-thread checkpoint/wait scope alongside its native CPU scope, parks its capsule for resource waits, and applies a final owner publication fence. Failed reacquisition remains fatal even if backend code swallows its error; collective deadline causes survive the boolean owner cancellation ABI. Native and pinned Nix/glibc Linux pass 23 broker/adapter and 10 real-owner tests each. The new fixture verifies parked CPU release, retained bytes, query/control progress, cancellation/expiry, late internal cancellation and final counter drain; [receipts](qualification/workload-pooling/receipts/2026-10-07/broker-owning-checkpoints/README.md) retain arguments, hashes and full logs. Production Node installation and parent/cohort parking are still pending, as are worker physical waits, numerical/full-memory coverage and default qualification.

### Caller parking before independent broker admission (both focused platforms passed)

Singleton calls and each native array wave now park the synchronous caller before enrollment/admission and reacquire only after every borrowed ticket joins, outside shard locks. Failed parent reacquisition disposes the current owned wave and stops later work. The real scheduler fixture fills ordinary capacity with a query parent, then verifies singleton/native child execution and cleanup on rejected resumption. Native and pinned Nix/glibc GNU pass 23 broker/adapter plus 11 real-owner tests each; [receipts](qualification/workload-pooling/receipts/2026-10-07/broker-caller-parking/README.md) pin sources and full logs. Production Node/host installation, multi-caller/numerical fairness and default qualification remain pending.

The [native production ownership receipt](qualification/workload-pooling/receipts/2026-10-07/broker-owned-production-native/README.md) records all 26 passing combined archive/C/subprocess steps at immutable source base `697ca0826c`. It qualifies the earlier production output conversions, not the later broker owning/caller scopes or numerical/default behavior.

### Concurrent broker cohort owner gate (both focused platforms passed)

Two independently scheduled callers now have a real-owner regression proving both parents park before their tickets share one physical broker invocation and one inference CPU grant. Protected control work runs during that callback; both callers receive original outputs, reacquire their own query capsules and drain operation/CPU/byte counters. All 12 owner tests pass on native macOS and pinned Nix/glibc GNU Linux; [receipts](qualification/workload-pooling/receipts/2026-10-07/broker-cohort-owner/README.md) preserve source hashes, exact commands and the resolved initial cache-mount failure. Production Node wiring, numerical/open-loop fairness and broad/default qualification remain separate obligations.

### Shared cohort parent cancellation/deadline isolation (both focused platforms passed)

The owner provider is now a generic factory that uses the production backends' existing checkpoint module, preserving one TLS scope. The two-parent real-owner cohort fixture covers successful completion plus cancellation and original-deadline expiry of one parent during shared physical execution. That caller receives its original error at reacquisition while the healthy peer retains its output; protected control and final counter drain hold in every case. All 12 owner test functions pass on native macOS and pinned Nix/glibc GNU Linux; [receipts](qualification/workload-pooling/receipts/2026-10-07/broker-cohort-isolation/README.md) pin the factory and full output. Production host installation and broad/numerical/default qualification remain separate requirements.

### Io mutex capacity handoff and worker-client contention (focused mechanism passed)

InferenceExecutionControl.lockIo parks only contended waits, preserves original controls and bytes, reacquires before returning a held lock, and unlocks rejected resumes. All 13 real-owner tests pass on native macOS and pinned Nix/glibc GNU Linux, including weighted resume, cancellation/expiry and query/protected-control progress. Worker Client.invokePayload now uses that helper for startup/replacement mutex contention and retains its grant through active child RPC. [Receipts](qualification/workload-pooling/receipts/2026-10-07/worker-client-mutex-waits/README.md) explicitly distinguish the qualified helper from the uncompiled worker wiring. RPC allows 128 outstanding requests; physical child and payload-offer waits still need an explicit admission handshake. Production/subprocess qualification, broad memory/backend coverage and defaults remain pending.

### Optimized ARM64 VOPR fiber switching (2026-10-08, simulator gates passed)

The broad native transaction run exposed a VOPR crash, reproduced by the existing
standalone task-stack regression. Both switch directions now use a C call boundary
and corrected ARM64 assembly register constraints. Stack guards remain active.
All 167 VOPR tests pass on native macOS and pinned Nix/glibc Linux;
[receipts](qualification/workload-pooling/receipts/2026-10-08/vopr-fiber-switch/README.md)
retain original failures, the unsuccessful first wrapper, final assembly and
source hashes. The new production transaction binary and consolidated API gates
still need to run; these simulator results do not enable defaults.

### Pre-decision clock ceilings (2026-10-08, callback contracts passed)

PreDecisionContext can clamp a shared deadline into an attempt's clock domain
without extending the original budget, while retaining cancellation, route
fences and restore identity. All five callback-contract tests pass on native
macOS and pinned Nix/glibc Linux;
[receipts](qualification/workload-pooling/receipts/2026-10-08/predecision-clock-budget/README.md)
retain commands, source and initial failed expectations. The working-tree hosted
worker now captures a deadline before routing and shares it across participants,
BEGIN, PREPARE and retries; its actual worker regression and full production
gates still need to run. SQL ingress and physical durability deadline coverage
remain pending, so these focused results do not enable defaults.

### Original commit-control callback contract (2026-10-08, focused gates passed)

CommitControl carries the request-owned execution team and original pre-decision
deadline, clock and cancellation independently of post-commit visibility
cancellation. New batch/stable-ID callbacks fail closed when a supplied control
would otherwise be discarded. All six compiled callback tests pass on native
macOS and pinned Nix/glibc Linux, including foreign dispatch and exact fallible
fence errors; [receipts](qualification/workload-pooling/receipts/2026-10-08/sql-commit-control/README.md)
pin the tested RequestContext execution-team field and boundary source. Hosted
worker/retry wiring and its actual no-skip regression are proposed separately
and still require production qualification. Provisioned/bound sources, SQL
ingress adoption and physical durability waits remain pending. Defaults remain
disabled.

Native text scoring now borrows synchronous lifetime authority from the original
storage request. Ordinary and filtered WAND, brute-force scoring, pivot repair,
block interval pruning and competitive block scans check that authority at most
every 64 loop steps. The callback uses the retained runnable owner's clock and
shutdown/cancellation state without acquiring CPU or yielding under index read
locks. A borrowed grant without lifetime support fails closed. Unpooled requests
use their existing monotonic deadline and cancellation token.

`zig build antfly-text-scoring-authority-test -Doptimize=ReleaseSafe -j1`
checks cancellation, timeout and shutdown during real scoring, partial output
retirement, successful snapshot reuse, unchanged controlled/legacy scores, and
expiration during unscored block pruning. This is focused correctness evidence.
It does not qualify CPU fairness under locks, arbitrary postings decoder work,
phrase/count/sort/aggregation/vector paths, background execution, runtime resource
limits, performance or automatic defaults. Those remain completion requirements.

Both production count-only entry points now retain the same non-suspending
original lifetime authority: exact snapshot counts and materialized candidate
counts. Term postings (including synonym expansion), recursive Boolean clauses,
minimum-should-match accumulation, match-all construction and candidate copying
poll during their loops. Multi-segment offsets and deletion masks retain their
existing semantics. Focused tests compare legacy/controlled membership and
counts, exercise nested Boolean filters across deleted documents, and inject
lifetime failures at every observed checkpoint to check partial retirement.
Specialized leaf filters, bulk bitmap operations, positional verification,
provider/decoder work and lock-safe CPU yielding still need bounded-work coverage;
checks surrounding these operations do not prove bounded execution within them.

Prefix (including companion exact lookup), wildcard, fuzzy, regexp and term-range
filters now pass original lifetime authority into postings scans and request-local
dictionary iterators. Contiguous and range-backed iterators poll while decoding
rejected terms and skipping blocks, including within a single `next()` call that
exposes no accepted entry. Ordinary filter queries, match-all and sorted-query
filter selection retain the authority through the public search dispatcher.
Focused tests compare membership with the legacy paths, reject every observed
checkpoint, and expire no-output iterator scans in both storage representations.
Automaton construction/transitions, bulk bitmap work, specialized leaf filters,
projection/sort/aggregation loops, physical I/O and lock-safe CPU yielding remain
separate bounded-execution requirements. In particular, production text sources
currently retain an analysis read lock for the query's lifetime.

Controlled service-lock acquisition now preserves reader priority and writer
intent while releasing runnable CPU only around the actual timed futex wait.
It polls retained, non-suspending lifetime authority without translating the
owner's deadline into the lock's Io clock. Resume is mandatory before consuming
a wake or reporting an Io error. Failure unwinds registered waiters, writer gate
and intent, or a newly acquired shared lease. The legacy acquisition paths keep
their existing behavior.

Local text-source acquisition, structured filter resolution and text-statistics
analysis acquisition use this path with the original request's borrowed phase.
Tests cover a real scheduler with one ordinary CPU slot (the waiting writer
releases CPU so its lock holder can resume), unchanged operation/byte ownership,
simulated timed waits and rejection, failed CPU resumption, the production text
adapter, and all prior lock cancellation/fairness/wakeup regressions.
`zig build antfly-controlled-service-lock-test antfly-text-scoring-authority-test`
provides focused correctness evidence. Controlled exclusive acquisition still
needs adoption across production writers and other service fences. CPU yielding
inside lock-holding query work remains unqualified until the complete lock/wait
dependency graph permits resumption and control progress; these tests do not
prove the broader runtime resource/performance/default gates.

Native batch admission now has a synchronous original-owner contract through
`DB.batchWithAdmissionControls`. It checks the caller at entry and carries the
same lifetime and optional CPU wait boundary into snapshot mutation admission
and exclusive apply acquisition. Portable-runtime gates still run before and
after apply acquisition. An existing mutation lease is explicitly retained,
including capture-owned helper borrows, rather than reentering reader admission
behind a queued capture. Admission controls stop at those pre-decision fences;
primary durability, mandatory retirement and post-commit visibility retain their
existing distinct contracts.

Focused snapshot tests use a real scheduler with one ordinary CPU slot, exercise
both mutation/capture wait directions, preserve operation/byte ownership and
exactly-once lease release, and reject expired/canceled/closed nested retains.
Native focused snapshot/lock and text regressions passed 57 tests within the
3 GiB compiler bound. A separate `antfly-mutation-service-admission-test`
exercises the actual DB batch entry, every observed admission checkpoint,
absence of rejected documents, release of both service fences and successful
reuse. That production test and full storage integration are queued and have
not yet qualified this wiring. Serving/ABI write callers still need to transport
this original-owner contract; other writer/maintenance/catalog waits and the
full lock dependency graph remain unfinished. These changes do not enable CPU
yielding inside pinned query work or automatic pooling defaults.
