# Bounded workload pooling implementation

Baseline: `e4d707de5634a8d758c606b4432037e1a1682052`. Design:
[WORKLOAD_SCHEDULING.md](WORKLOAD_SCHEDULING.md). Branch:
`design/default-workload-pooling`.

## Implemented scope

- [Shared admission controller](pkg/antfly/src/common/bounded_admission.zig):
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

With waiting enabled, the public HTTP transport reserves four request slots for
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

Outstanding handler capacity is the smaller of the existing connection ceiling
and `existing request-task capacity + queue count + 4`. The shared HttpRuntime
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
| HTTP raw body | Existing shared raw/transformed-body reservation; independent of output and context storage |
| Pgwire | Existing aggregate connection allocation cap plus independent retained-state cap; prepared/portal/cursor owners keep stable allocator addresses |
| Outbound service clients | Active/waiting/socket and queued-byte limits remain independent per stable transport owner |
| Storage/models/durable backend | Existing independent owners; allocations outside supplied allocators are not intercepted by these envelopes |

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

- Linux and larger deployment envelopes, controlled open-loop overload, slow
  consumers/retained cursors with combined descriptor/body/heap pressure, and
  distributed control progress through storage, locks and remote owners.
- A complete ledger and coordinated sizing for independent storage/model/durable
  allocations; total-process accounting is not established by API interception.
- Context capabilities for any remaining legacy/host callbacks that cannot
  receive cancellation; hot policy changes and operator suspension.
- Helper/fan-out isolation, fairness, cooperative quanta and distributed
  uncertainty ledgers.
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
