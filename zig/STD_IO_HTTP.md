# Structured `std.Io` HTTP and API Runtime Design

## Status

This document describes the long-term design for HTTP transport, listener
concurrency, runtime supervision, executor ownership, cancellation, and
shutdown across the data, metadata, standalone, and inference runtimes.

Replacing an individual `std.Thread.spawn` with `std.Io.concurrent` is only the
first step. A production design also needs explicit task ownership, failure
propagation, executor isolation, bounded admission, and deterministic shutdown.
The intended end state also removes the duplicated legacy public HTTP
dispatcher: `httpx.Server` is the only public HTTP transport and a
transport-independent API kernel is the single implementation of application
behavior.

### Implementation checkpoint

The current branch establishes the runtime and migration boundaries needed for
that end state:

- `httpx` handlers and generated routers carry explicit instance context.
- Long-lived `httpx` listeners are owned Futures with synchronous bind,
  explicit stop, fatal-error publication, and mandatory join. Futures capture
  separately allocated run state rather than the address of a movable task
  handle.
- Data, metadata, standalone, serverless, inference, and health listeners use
  the common ownership pattern; storage-backed roles borrow leased API or
  control lanes from `BackendRuntime`.
- `HttpRuntime` is the role-owned transport service shared by `httpx`
  listeners. It owns one bounded HTTP/1 cancellation multiplexer, listener
  leases, independent bounded listener, connection, and protocol-neutral
  request-execution lanes, reserved capacity, health state, and HTTP runtime
  metrics. The executor injected into each server remains the `std.Io` used by
  handlers for nested application/backend operations; handler invocation itself
  is admitted and scheduled by `HttpRuntime`. Long-lived keep-alive connections
  and HTTP/2 frame pumps therefore cannot exhaust request execution or a
  `BackendRuntime` API lane.
- Every `httpx.Context` has transport-provided cancellation. HTTP/2 uses the
  stream reset signal, while HTTP/1 registrations share the role's
  `HttpRuntime`. The linked API and inference ABIs carry the same semantic
  cancellation callback without route-specific OpenAPI policy.
- `error.Canceled` is the canonical response-free transport terminal outcome.
  `httpx` also normalizes `error.Cancelled`, which remains in client and
  application error sets, at its ingress boundary. It closes or resets the
  affected request/connection and increments a dedicated cancellation counter;
  neither spelling can become a synthetic 500. Deadlines remain distinguishable
  and map to 504 when a response is still legal.
- The serverless HTTP boundary now borrows that same semantic callback in both
  its native `httpx` and compatibility-executor adapters. Admission rechecks
  it before and after dispatch; semantic embedding, artifact fetches, indexed
  result materialization, graph traversal, join scans, foreign queries, and
  synchronous write-publication waits have bounded cancellation checkpoints.
  Provider pacing sleeps poll the same token, and text/sparse postings plus
  RaBitQ distance scans check it inside their potentially large inner loops.
  Public-table callbacks have one required contextual signature rather than
  legacy plus optional cancellation variants, keeping the request lifetime
  outside serializable query and write command types without a fallback path
  that can silently drop it. The native adapter also preserves typed retry
  metadata, and both adapters route on the parsed path rather than the
  query-bearing raw target.
- Serverless bootstrap also lends its process-owned application `std.Io` to
  managed embedders and remote template helpers. Listener/request scheduling
  remains isolated in `HttpRuntime`; nested outbound work therefore reuses a
  stable application executor instead of creating short-lived threaded
  executors that can exhaust process thread resources under sustained traffic.
- Cancellable outbound requests own the complete resolve/connect/request
  attempt in a `std.Io` task. The semantic watchdog interrupts established
  sockets and cancels the owning task, so DNS and initial connection work no
  longer sit outside the cancellation and request-deadline boundary.
- Once an HTTP/1 streaming response is committed, handler failure closes the
  connection instead of serializing a second status line. Linked stream
  callbacks preserve cancellation, timeout, capacity, and end-of-stream
  status classes rather than collapsing them into a generic failure.
- Linked request bodies use a transport-owned lazy body source. The API kernel
  can identify a still-streaming upload, acquire application body admission,
  and only then ask the listener to buffer it; direct and independently linked
  handlers therefore enforce the same limit and publish the same metrics.
- Header and body ingress have separate absolute phase deadlines. HTTP/2 body
  readers retain one deadline across every DATA wait, so trickle traffic cannot
  renew a timeout or hold an upload permit indefinitely.
- Query and write admission are application-operation gates owned by
  `ApiHttpServer`, not handler-local or listener-local limits. Generated HTTP,
  MCP, A2A, extension-host, and other in-process entry points therefore share
  the same capacity and rejection metrics.
- Continuous-HA mutation safety is enforced by one inventory-backed ingress
  policy across generated, contextual, and internal routes. Direct handlers
  install it as `httpx` middleware; linked handlers enter the same API-kernel
  policy before invoking a route from the manifest. Removing the compatibility
  dispatcher therefore cannot remove the fail-closed mutation gate or let a
  new non-GET route bypass classification.
- Process roles share signal cancellation and one absolute shutdown deadline.
- Listener address reuse and listener sharing are independent policies:
  `SO_REUSEADDR` supports deterministic restart, while `SO_REUSEPORT` is an
  explicit opt-in. Runtime listeners default to exclusive kernel ownership and
  do not use process-local lock files.
- Data, metadata, standalone, serverless, and the dedicated inference command
  compose their listener and control tasks under a common supervisor state
  machine. Readiness is gated on the supervisor reaching `ready`; the first
  fatal component/task error cancels the role and is preserved while teardown
  shares the original deadline.
- Listener lifecycle transitions are monotonic and serialized with listener
  attachment. Once stop wins, a late listener is stopped and rejected, ready
  cannot be republished, and a shutdown-induced listener exit cannot replace
  the terminal state with a failure. The first genuine listener failure is
  retained across later stop notifications.
- Supervisor phase and cancellation state are exported with data, metadata,
  standalone, and serverless health metrics. The dedicated inference command
  uses the same lifecycle while retaining model-specific readiness. The
  internal compatibility listener now owns connection tasks in a `std.Io.Group`
  and cancels/joins that group instead of detaching OS threads. Accepted
  sockets are registered in stable task ownership before executor handoff, so
  shutdown can interrupt them even if a worker has not started the task yet.
- The API-kernel and inference archives expose versioned function tables and
  immutable route manifests; the runtime owns router mutation and wire
  adaptation on both boundaries.
- Linked archives do not construct private `std.Io.Threaded` pools. API
  dispatch receives a request-scoped, layout-validated executor borrow;
  standalone retains a dedicated bounded `BackendRuntime` inference-lane lease
  until the linked inference handle is destroyed.
- Generated route inventories include operation ID, request-body mode, and
  streaming-response metadata, with uniqueness/contract tests.
- The transport-neutral operation layer now defines request identity,
  principal, cancellation, absolute deadline, admission-reservation, typed
  result, and backpressured streaming contracts. Root Kubernetes probes and
  storage-maintenance jobs are the first completed vertical slices: their
  concrete `httpx` handlers call typed operations directly and no longer enter
  `ApiHttpServer.handle()`.
- Metadata health, head, status, snapshot, active-transition, table-range,
  group-placement, and node-shutdown status reads now use transport-neutral
  operations with owned aggregate results. Their concrete, method-specific
  `httpx` handlers bypass the metadata method/path dispatcher. Catalog
  publication validation, reallocation, and schema-progress mutations use the
  same direct typed path.
- Metadata extension install, update, drop, enable, disable, configure, and
  restore are transport-neutral operations registered as concrete method/path
  pairs; extension lifecycle no longer enters the metadata dispatcher.
- Metadata node registration, status reporting, drain request/cancel, and
  shutdown finalization are transport-neutral operations with explicit source
  capabilities and ownership transfer. Node lifecycle no longer enters the
  metadata dispatcher.
- Metadata table create, definition replacement, drop, schema update, index
  create/drop, and artifact-enrichment put/delete are transport-neutral
  operations registered as concrete method/path pairs. Their direct-operation
  tests cover cancellation, while the real `httpx` client/server round trip
  covers the canonical wire contract without restoring a compatibility
  dispatcher.
- Metadata table restore, split, merge, and replication-source exact-cutover
  reseed now use the same operation layer and concrete `httpx` routes. The
  metadata router no longer registers any contextual catch-all. Its manual
  dispatcher, public request executor, synthetic `handle` entry point, and
  legacy request/response conversion have been deleted. Metadata integration
  and simulation fixtures use a real `httpx` test runtime with owned listener
  tasks and stop/join teardown.
- Internal repair cancellation-state lookup is the first internal control
  endpoint extracted into a transport-neutral operation. Its concrete `httpx`
  handler owns path decoding and status mapping, while the operation owns job
  lookup and cancellation semantics; it no longer enters the synthetic
  internal `HttpRequest` dispatcher.
- Internal group median-key and document lookup reads now follow the same
  split. Query-string/path parsing and version-header adaptation remain at the
  `httpx` edge, while group-local consistency, storage lookup, and error
  classification live in typed operations callable without HTTP.
- Internal distributed-join job-state lookup now accepts a typed job ID and
  returns an owned typed state. JSON request decoding and response encoding are
  confined to its concrete `httpx` handler; the old join route dispatcher no
  longer recognizes this path.
- Join finalize, rows, unmatched, and partition workers now expose typed
  request execution beneath their retained wire helpers. The concrete `httpx`
  routes decode into those owned requests and call typed operations directly;
  the separate internal join HTTP dispatcher has been deleted.
- Internal corrupt-embedding-artifact control now calls the table-write source
  through the typed internal-group operation surface. Its body/path decoding
  and empty JSON response are handled only by the concrete `httpx` adapter.
- Internal split observation, merge observation, and transition execution are
  concrete `httpx` routes over typed shard operations. Route-group invariants,
  local-leader projection, and operational error classification now live below
  the transport; their former method/path dispatcher branches are gone.
- Every internal group/table worker route is now registered as a concrete
  `httpx` handler. Dead artifact wildcard registrations have been removed, so
  an unknown artifact operation is rejected by the router instead of entering
  the public dispatcher; router registration still rejects duplicate route
  shapes.
- The ordinary internal group batch route now decodes directly into an owned
  batch request and invokes a typed operation for schema validation, local
  group write, cancellation, and outcome classification. The explicitly
  versioned routed-forwarding endpoint now does the same, passes the request's
  semantic cancellation token to the data runtime, and preserves its
  outcome headers without manufacturing an `HttpRequest`. The residual
  internal dispatcher no longer receives any write route.
- Internal transaction begin, prepare, resolve, status, and acknowledge are
  registered as concrete `httpx` handlers over typed group operations. The
  operation layer owns schema validation, participant writes, status lookup,
  cancellation, and conflict classification; JSON ownership remains at the
  transport edge. Their dead synthetic-dispatch branches and the legacy
  transaction-validator hook have been deleted.
- Document-artifact placement updates, child-range batches, and single-document
  reprocessing now have concrete `httpx` adapters over typed group operations.
  Artifact key-scope validation lives below the transport, and the three
  synthetic-dispatch branches have been removed.
- Table-range artifact reprocessing is also a typed operation with an owned
  result and a concrete `httpx` adapter; its response projection remains at
  the transport edge and its manual dispatcher branch has been deleted.
- Artifact-repair issue listing now returns an owned typed result through a
  concrete `httpx` handler; the compatibility dispatcher no longer owns that
  route.
- Artifact-repair execution now enters through a concrete `httpx` handler and
  typed operation. Cancellation probing is expressed as an injected lookup
  capability, keeping local job-state and remote HTTP details out of the
  operation. The old repair branch, probes, and compatibility-context fields
  have been deleted.
- The obsolete internal write-route dispatcher has been deleted. Split/merge
  JSON ownership is isolated in `internal_transition_wire.zig`; it contains no
  route matching, HTTP request/response conversion, or runtime capability
  context.
- Single and list document-artifact reads now use concrete `httpx` handlers
  over typed operations returning owned storage-domain manifests. Their legacy
  GET branches and duplicate response-projection structs have been removed.
- Internal group scans now parse their wire request at the `httpx` edge and
  call a typed scan operation returning owned NDJSON. The operation owns
  read-consistency selection and storage error classification; the manual
  scan branch has been removed.
- Internal graph expand, hydrate, and edge workers are concrete `httpx`
  adapters over typed group operations. Request ownership stays at ingress;
  the operations own read consistency and storage error classification, and
  all three manual-dispatch branches are gone.
- Internal text-statistics and algebraic-partials workers now call typed
  operations from concrete handlers. Their raw JSON is an intentional worker
  protocol payload, while consistency and storage errors are transport-free;
  both manual branches have been removed.
- Internal query, query-preflight, and vector-worker routes are now concrete
  `httpx` adapters over a transport-neutral query-planning service and typed
  group-local read operations. Schema routing, read consistency, cancellation,
  and storage error classification no longer depend on a synthetic HTTP
  request. The old internal read dispatcher and its dispatcher-only tests have
  been deleted; semantic planning retains a direct service test and the real
  router test covers all three wire adapters.
- The retrieval-agent worker is a concrete `httpx` route over a typed executor
  that accepts a body and returns an owned encoded result. HTTP status,
  retry-after, JSON, and event-stream adaptation remain at ingress. The last
  `http_internal_routes` request/response dispatcher and its executor function
  table have been deleted.
- ARD discovery, catalog, search, explore, skill, resource, and OpenAPI routes
  now share an explicit `httpx` adapter over a transport-neutral owned response
  contract. The service accepts path/query/body values rather than a synthetic
  HTTP request; authentication, status/error mapping, content type, and public
  CORS headers remain at ingress. ARD no longer enters
  `ApiHttpServer.handle()` or allocates a legacy `HttpResponse`.
- Extension-agent run, status, event, and cancellation routes reuse the same
  owned contextual result contract and have a dedicated registrar. Route
  parsing and visibility checks receive typed method/path/query values; JSON
  and event-stream adaptation is performed by `httpx`. Their legacy dispatcher
  branch and response-conversion helper have been removed.
- Extension catalog and lifecycle management routes now have their own direct
  `httpx` registrar over typed method/path/body inputs and the shared owned
  contextual result. Metadata-leader retry policy and headers remain at the
  transport edge; the application operation no longer receives a synthetic
  request, returns a legacy response, or enters `ApiHttpServer.handle()`.
- HA administration and internal replication paths now use a dedicated
  registrar and typed method/target/body ingress instead of the global
  contextual fallback. This removes the last listener route that could enter
  `ApiHttpServer.handle()`. Runtime-provided HA servers now expose a typed
  operation executor with explicitly owned content type and body results; the
  public `httpx` route and API-kernel boundary no longer manufacture or return
  legacy HTTP request/response values. The HA client-facing legacy executor is
  retained only as an adapter over the same typed operation for internal HTTP
  clients that have not moved to `httpx`.
- Public transaction-session handlers now pass typed method/target/body values
  into the session-forwarding operation. Only the remote HTTP executor boundary
  constructs its wire request; `httpx.Context` is no longer converted into a
  legacy request, and the shared context-conversion helper has been deleted.
- Query-builder execution is now one transport-neutral owned operation shared
  by the generated public handler and A2A. Request parsing, table-policy checks,
  contextual schema loading, generation, and operational error mapping are no
  longer duplicated, and A2A no longer manufactures a REST request for this
  skill.
- Extension WASM host imports now call explicit table-query and table-batch
  operations after capability and scope resolution. They preserve the caller's
  authenticated row-policy context without constructing a `/tables/...`
  request or routing back through the public HTTP dispatcher.
- Canonical and legacy-location A2A agent-card reads now use a direct `httpx`
  handler over an owned JSON builder. Card generation no longer constructs a
  request or response compatibility object, and both locations share the same
  implementation.
- Buffered A2A JSON-RPC and event-stream responses now use a direct `httpx`
  adapter over typed authorization/body inputs and the shared owned contextual
  result. The old buffered `HttpRequest`/`HttpResponse` adapter has been
  deleted. The live streaming executor is retained only until the public
  listener streaming cutover is complete.
- MCP GET, POST, DELETE, extension-scoped, and profile routes now use a direct
  `httpx` registrar with typed method/body/session inputs. The owned contextual
  result carries cloned MCP session/protocol headers, so ingress no longer
  constructs a legacy request or response and the protocol dispatcher has
  been deleted. Built-in MCP tools now submit an explicit
  `McpApplicationOperation` union with the authenticated identity instead of
  manufacturing REST requests and calling the public dispatcher. Several
  operation arms now call typed table index, backup, restore, batch, and query
  operations directly. Query success and operational-error results use the
  owned contextual contract, so MCP no longer contains a legacy application
  response conversion.
- Public table-repair and document-artifact reprocess job handlers now return
  owned typed responses directly to their concrete `httpx` routes. Their
  synthetic public-dispatch adapters and legacy `HttpResponse` projections
  have been deleted.
- Cluster/table restore submission and restore-job list/get/cancel operations
  now return the shared owned contextual response directly to generated
  `httpx` handlers. Location, retry, and metadata-authority headers are owned by
  that result contract and projected once at the transport edge.
- Public single-query and NDJSON multi-query execution now return owned
  contextual results for success, cancellation, validation, retryable, and
  storage-error outcomes. Generated `httpx` handlers and MCP consume those
  results directly; the listener-side `respondWithAllocator`, the last
  legacy-to-contextual response converter, and the typed-to-legacy query
  adapters have been deleted.
- The in-repository API client now resolves generated public operations below
  the canonical `/db/v1` namespace while keeping internal group RPCs and the
  contextual retrieval worker rooted. Stateful public multi-node fixtures use
  an owned real-`httpx` test runtime, including deterministic listener restart
  and stopped-node teardown; they no longer exercise `ApiHttpServer.executor()`.
  Durable terminal transaction sessions remain visible until TTL cleanup so
  commit retries can replay the stable result.
- Stateful single-node public API fixtures now use that same owned `httpx`
  runtime. Their raw wire requests use canonical generated paths, while
  executor-backed listeners remain only for fake outbound provider peers.
  Restore submission validates the complete request and backup location before
  reporting worker availability, keeping deterministic client errors ahead of
  transient runtime admission failures.
- API-client wire round trips now run against the real generated and contextual
  router as well, including public tables, transaction sessions, and internal
  group control. The transaction assertions cover read-set preflight conflicts,
  stable commit replay, and terminal-record TTL cleanup instead of depending on
  the synthetic dispatcher's obsolete response lifecycle.
- `ApiHttpServer` no longer exposes public buffered or streaming request
  executors. The last executor-based lookup test now verifies the canonical
  generated route through the owned `httpx` runtime, including its version
  header. The static executor adapters, synthetic `handle` dispatcher, public
  response compatibility wrappers, and legacy A2A streaming executor entry
  point have been deleted.
- The former shared non-generated compatibility manifest and listener
  catch-alls are gone. Metadata has no manual dispatcher, and data and
  standalone register generated and contextual families explicitly. Unknown
  paths therefore retain native `httpx` 404 behavior. Route-manifest
  registration uses the router's normalized duplicate-shape validation, wire
  tests keep `/healthz` and `/readyz` root-only while proving removed data
  aliases return 404, and API-kernel and linked-inference ABI tests validate
  their supported function-table prefixes.

The legacy public transport migration is complete on this branch: public test
fixtures exercise direct operations or owned real-`httpx` listeners, and no
public in-process caller manufactures an HTTP request merely to invoke
application logic. The remaining architectural work is to continue splitting
the large `AntflyApiHandler` application surface into cohesive typed services
so generated handlers are thin adapters. That is an operation-layer
maintainability improvement, not retention of a second public HTTP stack.

## Goals

- Give every long-lived task an explicit owner and join point.
- Use the same lifecycle contract across all server runtimes.
- Keep executor ownership separate from component task ownership.
- Preserve isolation between API, storage, Raft, control, and inference work.
- Propagate cancellation and deadlines from ingress to backend operations.
- Make startup failure and shutdown ordering deterministic and testable.
- Remove detached production tasks and mutable process-global runtime state.
- Use `httpx.Server` as the sole public HTTP transport for every runtime role.
- Keep routing and wire adaptation thin, generated, and separate from API
  application behavior.
- Implement every API operation once in a transport-independent API kernel.
- Eliminate duplicated legacy route dispatch, authentication, parsing, error
  mapping, and response construction.
- Keep the API-kernel and linked-inference boundaries independently
  code-generated without weakening lifetime or ABI guarantees.

## Non-goals

- Mechanically replace every `std.Thread.spawn` in the repository.
- Move all work onto an evented executor immediately.
- Make storage's `BackendRuntime` a dependency of the inference-only runtime.
- Preserve `StdHttpListener` or the legacy public `ApiHttpServer.handle()` path
  as an alternative production public API stack.
- Change the linked-runtime code-generation boundary merely to alter runtime
  concurrency. The LLVM graph and runtime task model are separate concerns.

Some operations may continue to require dedicated OS threads because of
thread-affine libraries, blocking foreign code, or CPU scheduling requirements.
Those threads must still be owned, stopped, and joined.

## Core ownership model

`BackendRuntime`, or a future process-level executor owner, owns `std.Io`
implementations. Components own the tasks submitted to those implementations.

```text
Process Runtime Supervisor
├── Executor Set
│   ├── control lane
│   ├── API lane
│   ├── storage lane
│   ├── Raft inbound lane
│   ├── Raft outbound lane
│   └── inference CPU lane
├── HTTP Runtime
│   ├── listener leases
│   ├── bounded listener executor
│   ├── bounded connection executor
│   ├── bounded request executor
│   ├── bounded HTTP/1 cancellation registry
│   └── one multiplexed observer thread
├── Data/Metadata/Standalone Server
│   ├── httpx listener Future
│   ├── connection task Group
│   └── contextual HTTP adapter
├── API Kernel
│   └── transport-independent operations
├── Raft Runtime
│   └── progress and transport tasks
└── Maintenance/Inference Runtimes
    └── owned task scopes
```

The central lifetime rule is:

```text
signal component stop
→ wake blocked operations
→ await component Futures and Groups
→ destroy component/provider state
→ release executor leases
→ deinitialize executor implementations
```

`BackendRuntime.deinit()` must not attempt to discover how to stop arbitrary
listeners. It does not know how to wake their accept loops or perform
protocol-specific graceful shutdown. Joining an executor while an unknown task
is still blocked can hang indefinitely.

## Common runtime supervisor

All process roles should eventually run beneath one supervisor abstraction:

```zig
const RuntimeSupervisor = struct {
    executors: ExecutorSet,
    cancellation: CancellationSource,
    tasks: TaskRegistry,
    shutdown_deadline: ?std.Io.Clock.Timestamp,
    failure: ?RuntimeFailure,
};
```

Runtime components should follow a consistent lifecycle:

```text
init      allocate state without launching background work
start     bind resources and launch owned tasks
ready     publish readiness only after startup succeeds
quiesce   stop admitting new external and background work
drain     allow admitted work to finish within the deadline
stop      cancel or wake remaining work
join      await every owned task
deinit    release resources after no task can retain them
```

A fatal listener, Raft, storage, maintenance, or inference task reports its
failure to the supervisor. The supervisor drops readiness, initiates shutdown,
and returns an appropriate nonzero process status. Background task failures must
not be reduced to logs while the process continues in an unknown state.

Signal handling belongs at this process boundary. Data, metadata, standalone,
and inference should share the same SIGINT/SIGTERM-to-cancellation mechanism
rather than maintaining role-specific globals or infinite loops that bypass
deferred cleanup.

## Structured task ownership

Every long-lived task must be represented by an owned `std.Io.Future`,
`std.Io.Group`, or an explicitly owned and joinable OS thread. This includes:

- HTTP accept loops and per-connection work
- Peer-disconnect observers
- Health and metrics refresh work
- Raft progress and transport workers
- Storage maintenance and durable-job loops
- Cache warmup and recovery work
- Inference eviction and model-management loops

Detached production tasks are not permitted. A useful invariant is:

> If an object can be deinitialized, no task may still retain its address.

For the runtime-owned `httpx` listener, the basic shape is:

```zig
const Listener = struct {
    const RunState = struct {
        server: *Server,
        state: std.atomic.Value(RuntimeState),
        failure: ?anyerror,
    };

    server: *Server,
    io: std.Io,
    run_state: ?*RunState = null,
    serve_future: ?std.Io.Future(anyerror!void) = null,

    pub fn start(self: *Listener) !void {
        // Bind synchronously so startup failures are returned directly.
        try self.server.bind();
        const run_state = try self.server.allocator.create(RunState);
        run_state.* = .{ .server = self.server, .state = .init(.running), .failure = null };
        self.run_state = run_state;
        self.serve_future = self.io.concurrent(serve, .{run_state}) catch |err| {
            self.server.allocator.destroy(run_state);
            self.run_state = null;
            return err;
        };
    }

    pub fn stopAndJoin(self: *Listener) !void {
        self.server.requestStop();
        if (self.serve_future) |*future| {
            const run_state = self.run_state.?;
            defer {
                self.server.allocator.destroy(run_state);
                self.serve_future = null;
                self.run_state = null;
            }
            try future.await(self.io);
        }
    }
};
```

The Future must never capture the address of a task handle that can be returned
by value, stored in a resizable collection, or otherwise moved after `start`.
The handle owns a stable run-state allocation until `join`, while the run state
borrows the server and publishes terminal state and failure. The handle is
movable but logically unique: copying it would duplicate Future ownership and
is not supported. The server, its allocator, and the executor lane must outlive
the joined task.

Use `std.Io.concurrent` for a listener that must progress concurrently with its
caller. `std.Io.async` is appropriate for operations that can use its weaker
scheduling guarantee. Under `Io.Threaded`, a long-lived concurrent listener
still consumes a worker thread. That worker comes from `HttpRuntime`'s bounded
listener lane; accepted connections and application requests use separate
bounded connection and request lanes. None of those long-lived tasks consumes
capacity promised to nested backend or storage operations. The benefit is
structured and isolated ownership rather than elimination of threads.

## Listener bind ownership

Fast restart and simultaneous listener sharing are different requirements and
must not be represented by one flag:

- `reuse_address = true` enables the platform's `SO_REUSEADDR` behavior so a
  replacement process can promptly reclaim a released address. It must not
  allow two live Antfly listeners to own the same bind tuple.
- `reuse_port = false` is the production default. Enabling it requests
  `SO_REUSEPORT` and is reserved for an explicitly designed multi-acceptor
  deployment where connection distribution, graceful removal, and observability
  have been validated.
- Port `0` always asks the kernel for an independent ephemeral port and must not
  be serialized across processes.

The kernel is the authority for bind ownership. A path derived from a port in
`/tmp` is neither namespace-aware nor equivalent to socket ownership: it can
serialize unrelated ephemeral listeners, become stale, and cannot protect
non-cooperating processes. Antfly therefore does not layer a file lease over
listener binding. Bind remains synchronous, and `AddressInUse` is the startup
failure surfaced to the supervisor.

Zero-downtime replacement should be owned by the deployment system—readiness,
load-balancer draining, or socket activation—not by accidentally permitting
two generations to bind the same port. If a future runtime intentionally uses
`SO_REUSEPORT`, that choice must be explicit in its configuration and covered
by platform-specific integration tests.

## Executor topology

`BackendRuntime` separates general, API, inference, control, Raft-inbound, and
Raft-outbound `Io.Threaded` implementations. This should evolve into an explicit
executor set usable by process roles without making all of them depend on the
storage runtime:

```zig
const ExecutorSet = struct {
    control: std.Io,
    api: std.Io,
    storage: std.Io,
    raft_inbound: std.Io,
    raft_outbound: std.Io,
    inference_cpu: std.Io,
};
```

Each lane needs:

- A bounded worker count and queue
- Reserved capacity for health, cancellation, and shutdown work
- Explicit behavior when admission is exhausted
- Queue-depth, active-worker, rejection, and saturation metrics
- A documented policy for blocking and CPU-bound operations

HTTP handler invocations run on `HttpRuntime`'s bounded request lane. Their
`Context.io` borrows the API lane rather than the general storage lane, so
nested futures, backend waits, and outbound operations retain role-specific
isolation. Long-lived accept loops and connection lifetimes use two other
bounded lanes owned by `HttpRuntime`. CPU-heavy model execution should not
occupy the workers required to admit requests, serve readiness probes, wake
shutdown, or complete storage commits.

The first implementation should keep these lanes on `Io.Threaded`. Moving a
lane to an evented backend requires auditing every task for synchronous file
operations, POSIX sleeps, blocking foreign calls, and CPU-heavy work. A Future
does not make blocking code event-loop-safe.

## Backend runtime API and leases

Consumers should borrow `std.Io`, not depend on `*std.Io.Threaded`:

```zig
pub fn apiIo(self: *BackendRuntime) ?std.Io {
    if (comptime builtin.os.tag == .freestanding) return null;
    return if (self.api_io_impl) |impl| impl.io() else self.io();
}
```

Returning the interface value keeps consumers independent of the executor
implementation. The backing implementation must remain at a stable address for
the entire borrow.

Long-lived borrowing is represented by an executor-lane lease:

```zig
var lease = try backend_runtime.acquireApiLane();
defer lease.release();

const io = lease.io();
```

Lane admission and the active lease count share one atomic state so shutdown
cannot race between a separate closed check and counter increment. Runtime
deinitialization closes every lane first, then waits for all committed leases
to drain before destroying any executor; this lifetime guarantee applies in
production builds as well as debug and test builds. The backend's process-level
`std.Io` lane remains alive through this phase and drives the gate's
`std.Io.Condition`, so teardown parks without consuming a dedicated observer
thread and without depending on a lane that it is waiting to destroy. Manual
runtimes have no successful executor-lane borrows in normal operation and keep
only an executor-independent fallback for a close racing an unavailable
acquisition. The component remains responsible for stopping and awaiting its
tasks before releasing the lease.

As with any owned object, callers must retain the `BackendRuntimeHandle` while
calling its methods. The lane gate synchronizes acquisitions already operating
within that lifetime; it does not make a raw runtime pointer valid after its
owning handle has been destroyed.

## HTTP transport runtime

`HttpRuntime` and `BackendRuntime` solve different ownership problems.
`BackendRuntime` owns application and storage execution capacity; `HttpRuntime`
owns shared transport state and execution capacity for one process role. Data,
metadata, standalone, serverless, and inference use it for every `httpx`
listener. Data, metadata, and standalone construct one `HttpRuntime` and inject
it into each `httpx.Server` they compose. A standalone library `Server` creates
a private fallback runtime for convenience.

`HttpRuntime` owns independent bounded `Io.Threaded` lanes for accept loops,
connection lifetimes, and application request execution. Listener-lease
acquisition atomically reserves each listener's complete declared connection
and request-task bounds before bind; an undersized shared runtime is therefore
a startup error. Once bound, `ListenerTask` runs on `listenerIo()`, accepted
sockets and their connection groups run on `connectionIo()`, and every HTTP/1,
HTTP/2, and h2c handler runs on `requestIo()`. These lanes stop only after every
listener, connection, and request task has been joined.

`ServerConfig.normalized()` is the single configuration boundary for listener
defaults, sentinels, and dependent bounds. A role that owns a shared
`HttpRuntime` first builds and normalizes each listener configuration, sizes the
runtime from those resolved `max_connections` and `max_request_tasks` values,
and passes that same resolved configuration to `Server`. Runtime owners must
never size a lane from raw zero-sentinel fields or independently reproduce the
server's default arithmetic; otherwise a valid configuration can fail at bind
or silently reserve a different request bound than operators configured.

Aggregate reservation alone is insufficient: a shared executor does not know
which listener a task belongs to. Each `httpx.Server` therefore owns a local
atomic request-permit pool equal to its leased request capacity. It must claim a
permit before publishing application work and retain it through the response
lifecycle. This prevents a busy public listener from consuming capacity
reserved for health, admin, or another application listener. Releasing the
permit and active-request accounting is one invariant on every success,
rejection, cancellation, and shutdown path.

Concurrent connection execution is a declared contract, not a best-effort
optimization. A server reserves `max_connections` from its `HttpRuntime`, and
bind fails if the process-wide transport capacity cannot honor that bound. A
post-accept scheduling rejection closes that socket, releases all
admission/accounting state, and increments
`connection_dispatch_rejections_total`; the accept loop never falls back to
serving the connection inline. Explicit serial execution remains available for
small test or embedding configurations and is selected deliberately.

Protocol-native overload behavior is decided before application code runs.
HTTP/1 returns 503 with `Connection: close` and closes the connection when its
listener has no request permit or cannot schedule the claimed task. HTTP/2 has
an additional invariant:
the connection task is the sole frame pump and must remain able to receive DATA
for streaming request bodies. A saturated HTTP/2 or h2c stream is therefore
reset with `RST_STREAM(REFUSED_STREAM)` and is never executed inline on the
frame pump. Every such outcome increments
`request_dispatch_rejections_total`; the HTTP/2 subset also increments
`h2_stream_dispatch_rejections_total`.

Each listening server acquires a lease before bind/accept and releases it only
after its connection group has drained. The first lease starts the HTTP/1
cancellation service and the last lease stops and joins it. `HttpRuntime`
cannot be destroyed while a listener lease remains. Its H1 registry is bounded;
each listener reserves its configured maximum before startup, so the sum of
listener reservations cannot exceed runtime capacity. A listener that cannot
reserve its complete bound fails startup instead of discovering an undersized
observer only under load. Per-request registration failure is still fail-closed
with a retryable 503 rather than silently running an uncancellable request.
The exception is an explicitly bounded health/control listener: it disables
peer-disconnect observation, reserves zero observer slots, and retains normal
server-shutdown cancellation. This is necessary so `/readyz` can still report
an unhealthy shared observer rather than being rejected before route dispatch.
Application listeners always require observation.

The current `std.Io.Threaded` backend grows a worker pool as concurrency demands
and retains those workers until executor deinitialization. Consequently, one
long-lived watcher submitted per connection or request would turn peak socket
concurrency into retained thread stacks. The current H1 implementation instead
uses one explicitly owned OS thread and multiplexes all registered sockets with
`poll`, `kqueue`, or `WSAPoll` on Windows. Its default stack reservation follows
Zig's platform thread contract because a fully linked runtime may add target-
and libc-specific requirements that a transport library cannot safely size.
Supported hosts reserve that address space virtually, so committed memory still
tracks actual use; embedders may configure a smaller stack only after validating
every deployment target. The observer only peeks; the HTTP parser remains the
sole consumer of socket bytes. Readable pipelined input suppresses further
readability notifications while retaining hard-error/reset observation; it
must never unregister the descriptor before the active request completes.
Unsupported freestanding targets fail listener startup when observation is
required; no supported platform can silently turn `.required` into a no-op. An
HTTP/1 peer may
half-close its request side with FIN while legitimately awaiting the response,
so orderly EOF is never treated as request cancellation. Only a hard socket
failure/reset cancels an active H1 request; explicit protocol cancellation and
deadlines remain necessary when an orderly half-close must not retain work.
Fatal observer failures mark `HttpRuntime` unhealthy, cancel all current
registrations, and make shared-role readiness fail until the runtime is
restarted.

Retained `Io.Threaded` workers are an executor capacity plateau, not leaked
connection ownership. The three finite HTTP runtime lane capacities put a hard
ceiling on that plateau even as connections churn. Cancellation-storm tests
therefore require descriptors, active tasks, and observer registrations to
return to baseline while asserting that the warmed worker count remains bounded
and cannot ratchet upward on later rounds. Requiring process thread count to
return to its cold baseline would misstate the executor's documented lifetime
model.

This observer is intentionally not a `BackendRuntime` lane. Giving it a lane
would mix transport lifetime with storage-executor policy and would still risk
pool growth on `Io.Threaded`. When Zig provides a production evented `std.Io`
backend suitable for socket readiness, `HttpRuntime` may replace its private
multiplexer internally. The `Server`, handler, and linked-runtime cancellation
contracts do not change.

Shutdown order is:

```text
drop readiness
→ request listener stop
→ interrupt active connection/request cancellation signals
→ join listener and connection tasks
→ release the listener's HttpRuntime lease
→ destroy servers and handlers
→ deinitialize HttpRuntime
→ destroy linked inference handles
→ release BackendRuntime API/inference/control-lane leases
→ deinitialize BackendRuntime
```

## Runtime-specific integration

### Data and metadata

Data and metadata public and admin APIs use the common `httpx.Server` lifecycle
and borrow the API executor from `BackendRuntime`. This migration does not
depend on a large structured-concurrency retrofit of `StdHttpListener`; that
listener remains only for explicitly internal consumers that have not yet
migrated.

Their public, admin, health, and Raft listeners must all participate in the
same process supervisor and shutdown deadline. Existing background threads
should be migrated separately based on their semantics rather than folded into
the public HTTP transport change. Raft or other internal users may retain an
internal-only listener temporarily behind an explicit compatibility boundary.

### Standalone

Standalone should continue using its unified `httpx.Server`, but run it on the
backend runtime's API lane rather than its general storage lane. It owns the
listener Future and must await it before destroying the API adapter, API
kernel, inference provider, data server, or backend runtime.

Standalone's protocol, internal, HA, maintenance, ARD, MCP, extension, and
other routes use the same contextual registrar as data. The remaining adapter
must be replaced with typed operations; it must not regress to a global active
API server. Standalone-specific termination and active-server globals should be
replaced with supervisor-owned cancellation and explicit route context.

### Inference

The normal inference command may continue to serve on its main task because
serving is the role's primary operation. It does not need storage's
`BackendRuntime`.

An embedded or spawned inference server should borrow a caller-owned `std.Io`
or own a heap-stable executor implementation. Its returned handle must retain
an owned Future and stop/await it during deinitialization. It must not detach a
thread and intentionally leak the node for the remainder of the process.

Inference forwarding admission uses explicit runtime identity, not URL
comparison. The reserved `local-inference` virtual connection is created by the
runtime and dispatches directly through the linked inference route boundary; it
does not open a loopback connection or infer locality from a public URL. The
destination route receives the original cancellation signal, the same absolute
invocation deadline used for a remote provider, and the original response
stream sink. SSE therefore retains listener backpressure and HTTP/1 or HTTP/2
close semantics instead of being buffered or failing for lack of a socket. The
destination is the sole owner of the shared embedded-inference permit. Every
configured connection is instead admitted by the forwarding operation for the
full upstream request,
even when its URL text matches or aliases the local listener. Operators that
intend in-process inference must use the reserved connection; configured URLs
remain ordinary network boundaries and cannot silently change resource
ownership because of DNS, proxy, case, path, or listener configuration. Tests
must preserve this distinction at capacity one.

The local target itself is a versioned C-layout interface. It carries only ABI
byte views, fixed status values, a callback allocator, cancellation, deadline,
and stream views. It never transports a Zig allocator, slice, error union,
tagged union, `httpx.Context`, or default-calling-convention function pointer
through `ApiHttpServerConfig`. Buffered response bytes are allocated through
the caller's ABI allocator and become caller-owned; streamed bytes remain
borrowed for each sink callback. The forwarding operation computes one
absolute process-monotonic deadline and remote HTTP derives its remaining
timeout from that value, so changing transport cannot reset or remove the
request ceiling.

### Health

Health listeners should use `httpx.Server` on a supervisor-provided control or
API lane, and metrics refresh tasks should borrow the same executor set. They
should not create process-global or per-health-server executor state
implicitly.

Health capacity must remain available under API, storage, and inference
saturation. Readiness should be dropped before ordinary ingress is stopped so
load balancers can begin draining the process.

The health listener shares the role's `HttpRuntime` for lifecycle and health
visibility but does not depend on the H1 disconnect observer to dispatch its
bounded handlers. Its connection/body limits and cached metrics path are the
resource bound; its request cancellation signal is still tripped during server
shutdown.

## Deadline-based shutdown

Shutdown uses one absolute process deadline. Independent per-component timeout
budgets can add together and greatly exceed the operator's termination grace
period.

A production shutdown sequence is:

1. Publish not-ready.
2. Stop accepting new external requests.
3. Reject new writes and background submissions.
4. Where applicable, transfer Raft leadership and deregister or fence the node.
5. Drain admitted HTTP requests.
6. Drain durable jobs and storage mutations.
7. Stop inference/provider work.
8. Stop Raft, recovery, and maintenance tasks.
9. Flush and close storage.
10. Await all remaining tasks.
11. Release executor leases and destroy executors last.

Every phase receives the remaining time until the shared deadline. If graceful
shutdown expires, the supervisor escalates to cancellation and then controlled
process termination. It must not deinitialize memory still referenced by a
stuck task.

## End-to-end cancellation

Cancellation should be carried in a request context from ingress through
distributed operations, storage, inference, and outbound calls:

```zig
const RequestContext = struct {
    cancellation: CancellationToken,
    deadline: ?std.Io.Clock.Timestamp,
    request_id: RequestId,
    admission: AdmissionReservation,
    principal: Principal,
};
```

All blocking loops require cancellation points. `error.Canceled` must be
propagated or deliberately translated at a documented boundary rather than
silently swallowed. Client disconnect and server shutdown should cancel actual
backend work, not only stop response delivery.

Every public-table operation receives a required `RequestContext`; there is no
nullable cancellation field or alternate non-contextual callback signature.
Adapters that have no external cancellation still pass `.none` explicitly via
an empty context. Multi-stage operations check the context at bounded intervals
and immediately before irreversible publication. They do not report
cancellation after a commit has begun, because the durable outcome may already
exist. Linear merge follows this rule before its single HA-mirrored batch
boundary, and its scan and comparison loops contain bounded checkpoints.

At HTTP ingress, application `error.Canceled` and `error.Cancelled` are not
application error responses. If no response has committed, `httpx` terminates
the stream or connection without emitting a status and records
`request_cancellations_total`. This preserves the peer-disconnect/server-stop
meaning and avoids misleading 500 logs. `error.DeadlineExceeded` remains a 504
before commitment. After a response is committed, either outcome closes/resets
the transport because a second status line is impossible.

The universal representation is a borrowed `(context, is_cancelled)` callback.
Atomic values are adapters used by concrete listener, lifecycle, or test
owners; they are not an operation, storage, client, or compiled-runtime ABI.
The callback token is preserved through distributed query/graph execution,
storage search, vector and sparse kernels, foreign sources, managed inference,
and outbound HTTP. This avoids the semantic hole where linked runtimes could
observe cancellation at ingress but deep work continued unless a same-process
atomic fast path happened to be available.

Outbound HTTP translates that semantic token at the transport boundary. Every
cancellable request owns its complete attempt—including address resolution,
initial connect, retries, and response I/O—in a cancellable `std.Io` task. A
single short-interval watchdog combines the semantic token and absolute request
deadline. When it wins, it first shuts down a published HTTP/1 socket or resets
the affected HTTP/2 stream and then cancels and drains the owning task. Task
cancellation is what reaches resolver/connect operations before a socket is
available; the socket/stream interrupt makes established-transport teardown
immediate. Individual `std.Io` backends remain responsible for the platform
details and latency of canceling an in-progress resolver syscall. Separate
losing timeout and cancellation sleepers can otherwise retain
`std.Io.Threaded` workers until a long deadline after a successful request.
Every request race also owns an explicit atomic watchdog-stop signal. The
request winner publishes it before draining the select because cancelling a
select does not guarantee that a sleeper using the parent `Io` observes group
cancellation. Cleanup is therefore bounded by one polling interval rather than
the request timeout on every executor backend. Provider code sees one request
context in both cases and does not choose an executor-specific mechanism.

Component stop signals and Future cancellation are complementary:

- First use the component's semantic stop operation so it can stop admission,
  wake `accept`, send HTTP/2 GOAWAY, or flush state.
- Await graceful completion until the deadline.
- Use Future or Group cancellation only as escalation.

## Public HTTP and application architecture

`httpx.Server` is the sole long-term public HTTP transport. Data, metadata,
standalone, inference, admin, and health endpoints use one hardened listener
and connection lifecycle. `StdHttpListener` and `http_common.RequestExecutor`
may remain temporarily for Raft or other internal compatibility users, but are
not alternative public API stacks.

The target request flow is:

```text
httpx.Server
→ generated contextual route adapter
→ transport middleware
→ transport-independent API kernel operation
→ typed result or stream
→ httpx response encoder
```

The shared HTTP transport must consistently handle:

- Synchronous bind and startup failure reporting
- Graceful HTTP/1 and HTTP/2 shutdown
- Bounded connections, requests, and aggregate request-body memory
- Header, body, request, idle, and shutdown deadlines
- Slow clients and peer disconnects
- Listener wakeup, restart, and port-reuse behavior
- TLS or an explicitly supported reverse-proxy deployment contract
- Readiness and metrics semantics

The loopback connection used to wake a blocked accept should be encapsulated
behind a cancelable-listener abstraction. When the standard library provides a
reliable cancelable accept path for every supported executor, the workaround
can be replaced without changing component lifecycles.

## Transport-independent API kernel

The current `ApiHttpServer` mixes long-lived API state, business operations,
manual HTTP routing, authentication, request parsing, and response encoding.
Its stateful and operational responsibilities should become a transport-neutral
`ApiKernel`:

```zig
const ApiKernel = struct {
    source: StatusSource,
    table_reads: ?TableReadSource,
    table_writes: ?TableWriteSource,
    sessions: TransactionSessionStore,
    restore_jobs: RestoreJobStore,
    inference: ?AntflyProvider,
};
```

Kernel operations accept typed input plus the common request context and return
a typed result or `ApiError`:

```zig
pub fn createTable(
    self: *ApiKernel,
    request: RequestContext,
    input: CreateTableInput,
) ApiError!CreateTableResult;
```

The kernel owns catalog access, retry and convergence behavior, transaction
coordination, job state, provider use, and storage calls. It does not own a
listener or router and does not accept `httpx.Context`,
`http_common.HttpRequest`, or other wire-specific request types.

The HTTP adapter is limited to extracting parameters, decoding bodies, creating
`RequestContext`, invoking a kernel operation, and encoding its result. During
migration, both legacy and `httpx` entry points may call the same extracted
operation, but duplicated business implementations must not remain afterward.

## Contextual routing without globals

`httpx.Handler` should carry an instance pointer rather than being only a bare
function pointer:

```zig
pub const Handler = struct {
    ptr: *anyopaque,
    call: *const fn (
        ptr: *anyopaque,
        ctx: *Context,
    ) anyerror!Response,
};
```

The router stores this value per route. Generated routers can bind a particular
adapter instance without a type-level `active_impl`, and handwritten route
registrars can bind explicit component context. This enables multiple server
instances in one process and makes handler lifetime part of the listener's
ownership graph.

The current generated active-implementation globals and standalone's
`active_api_server` must be removed. Route registration must not publish hidden
global pointers that outlive or alias the registered server instance.

## One route and policy source of truth

OpenAPI generation should emit:

- HTTP method and path
- Stable operation identifier
- Path, query, header, and body decoders
- Typed handler interface
- Response encoders
- Route policy metadata

For example:

```zig
pub const RouteMetadata = struct {
    operation: Operation,
    auth: AuthPolicy,
    admission: AdmissionClass,
    body_mode: BodyMode,
    streaming_response: bool,
};
```

Transport middleware uses this metadata for authentication orchestration,
admission, body policy, deadlines, cancellation, tracing, and request identity.
Authorization decisions that depend on application state remain kernel policy.
Individual handlers should not repeat the same authentication, overload, and
error-mapping sequences.

Routes outside the public OpenAPI contract—including internal groups, MCP, A2A,
ARD, extensions, HA, and maintenance—need explicit contextual registrars or
their own generated schemas. They must not fall through to a manual legacy
method/path dispatcher.

## Internal and in-process calls

In-process callers invoke typed kernel or service interfaces directly. They do
not construct synthetic HTTP requests merely to reuse the legacy dispatcher.

Real internal HTTP endpoints still use `httpx`, but follow the same layering:

```text
internal httpx route
→ internal authentication and admission
→ typed internal operation
→ kernel or storage service
```

Internal and public policies may differ, but they share application operations
where semantics are the same. The transport boundary, not a path-prefix check
deep in the kernel, establishes the caller domain.

## Streaming contract

Streaming must be first-class in the transport-independent operation contract:

```zig
const OperationResult = union(enum) {
    json: JsonResult,
    bytes: BytesResult,
    stream: StreamProducer,
    empty: StatusResult,
};
```

A `StreamProducer` writes through a transport-neutral sink that implements
backpressure, cancellation, deadlines, and close semantics. SSE, incremental
generation, and other streams must not require business logic to manipulate an
`httpx.Context` or socket directly.

Request bodies likewise need an explicit buffered or streaming mode. Large or
incremental inputs should use a bounded reader contract instead of forcing
every operation through a fully materialized byte slice.

## Legacy public HTTP removal

Historical root aliases are not part of the target contract. `/tables`,
`/secrets`, `/transactions`, `/backup`, `/restore`, `/status`, and similar data
routes are removed rather than registered as compatibility aliases. Generated
data routes are canonical only below `/db/v1`; generated authentication routes
remain below `/auth/v1`.

The two Kubernetes probes are deliberately not namespaced. Every runtime serves
exactly `/healthz` and `/readyz` at the root. A runtime may provide a stricter
readiness operation than the common data implementation—for example standalone
also checks API initialization and exclusive storage maintenance—but it must not
move the probe or add a prefixed alias. Linked standalone calls the typed
`check_ready` kernel operation instead of restoring a generic HTTP-dispatch ABI
just to implement its probe.

The first cutover removes both global fallback layers and the public legacy
dispatch ABI. Data, standalone, and metadata listeners install concrete
generated or contextual `httpx` routes. The API-kernel ABI no longer advertises
`legacy_http_dispatch` and no longer exports request-executor, streaming-
executor, generic-handle, or internal-handle function-table entries. Unknown
and removed alias paths are rejected by the router before application code.

That boundary cleanup is complete on this branch. MCP, A2A, ARD, extensions,
HA, metadata administration, internal group/table workers, storage
maintenance, and root probes all have explicit generated or contextual
registrars. The public synthetic dispatcher, manual metadata dispatcher,
public request executors, context/request conversion, response conversion,
catch-all bridges, and obsolete API-kernel dispatch ABI entries have been
deleted. Historical data aliases are not registered.

Further operation extraction should keep the same boundary: move cohesive
business behavior out of `AntflyApiHandler` into typed services without
reintroducing request/response compatibility types. `StdHttpListener` remains
an explicitly internal transport for Raft and selected test/provider peers; it
is not used as an alternative public, admin, or probe listener. It can be
migrated independently if those internal consumers need the `httpx` lifecycle
or backpressure model.

Migration enforcement belongs in ordinary Zig behavior and invariant tests,
not in a checked-in source-scanning shell script or an extra build dependency.
The `httpx` router rejects duplicate method-and-route-shape registration (even
when duplicate parameter names differ), wire tests assert that root probes
remain and removed aliases return 404, and ABI tests
validate the supported function-table prefix. These gates test actual behavior
and types while avoiding a fragile list of forbidden source spellings.

The removal is complete when every public wire operation has exactly one kernel
implementation, every runtime serves it through `httpx`, no generated or
handwritten router uses active-instance globals, and no public request is
converted into a legacy request/response pair.

## Remove mutable process globals

Route and runtime state should be passed explicitly rather than published
through global pointers or atomics:

```zig
const StandaloneRouteContext = struct {
    api_kernel: *ApiKernel,
    inference: InferenceProvider,
    lifecycle: *RuntimeLifecycle,
};
```

Explicit context permits multiple instances in tests, prevents accidental
cross-runtime access, and makes lifetime relationships visible in types.
Process-global shared `Io` implementations should likewise become explicit
owned or borrowed executors.

## Compiled API kernel boundary

The independently code-generated API kernel should not require the runtime
archive to pass an opaque `httpx.Server` pointer across its ABI indefinitely.
A hardened boundary exports:

- A versioned route manifest
- Stable operation identifiers and route policy metadata
- A versioned request view
- A response and streaming sink
- A dispatch function keyed by operation identifier

Conceptually:

```text
register_route(method, path, operation_id, metadata)
dispatch(kernel, operation_id, request_view, response_sink)
```

The runtime-side `httpx` adapter owns route registration and invokes dispatch.
The API-kernel archive owns operation implementation. This keeps
`httpx.Server`, Zig error sets, and unstable Zig layouts out of the ABI while
preserving the compiler-memory benefit of independent code generation.

The linked API and inference manifests carry buffered-body and streaming-
response policy. Cancellation is deliberately absent from route manifests and
OpenAPI extensions because it is a universal request-lifetime property, not an
operation opt-in. Each API dispatch receives request-scoped cancellation, a
request-scoped validated host-executor borrow, lazy body-source, and streaming
callback views. The API interface may be copied for nested work but cannot
escape the synchronous dispatch call; inference dispatch uses its separately
leased lifetime borrow described below. A deferred HTTP/2 body remains owned
by the listener until the archive requests it; the archive's reconstructed
`httpx.Context` exposes that source as streaming so application admission runs
before buffering. Callback outcomes preserve cancellation, timeout, size,
capacity, and end-of-stream errors across the ABI. If a streaming handler fails
after HTTP/1 headers are committed, the host closes that connection and never
attempts a second response. The same transport-neutral
delegate model lets an inference SSE handler start, write, and close the
original listener's HTTP/1 chunked stream or HTTP/2 DATA stream without sharing
socket or connection layouts across the ABI. The cancellation callback is used
by operation contexts, storage/search internals, inference generation, and
outbound requests. There is no ABI fast flag or dependence on Zig atomic
layout.

Application ingress policy happens inside linked dispatch, where the API
kernel owns request accounting, continuous-HA mutation classification, and
retryable metadata-authority response mapping. It must not rely on direct-
registration middleware, which is absent from opaque builds, or duplicate
application configuration in each host runtime. ABI versions were advanced
with these layout changes; old callers fail prefix validation rather than
interpreting a new structure with an old layout. Cancellation metrics are
owned and exported by `HttpRuntime`; kernel handler statistics retain only
application admission state, avoiding duplicated or partially observed
transport counters.

Function-table validation is capability-prefix-aware. Each capability defines
the byte extent through its last callable field; validation rejects unknown
capability bits and tables shorter than the largest requested extent, but does
not require unrelated fields appended later. This makes append-only tables
useful in both directions without allowing a caller to read beyond the
provider's published `struct_size`.

Passing an opaque server pointer may remain as a same-toolchain migration step,
but it is not the final ABI contract. Kernel-created objects must still be
destroyed by the archive and allocator that created them.

## Linked inference ABI

The linked inference bridge is an internal code-generation boundary, not a
public plugin API. It nevertheless crosses independently compiled archives and
therefore needs enforceable compatibility and ownership rules.

A hardened bridge should provide:

- An ABI version and structure-size fields
- One exported getter returning a versioned function table
- Fixed-width scalar status codes
- Explicit object, allocator, and slice ownership
- Creation and destruction of an object in the same archive
- Capability negotiation for optional operations
- Capability-specific minimum function-table extents rather than the size of
  the newest complete table
- No unstable Zig errors, slices, or layouts passed by value
- Tests that intentionally detect layout and version mismatches

The bridge borrows `std.Io`; it never owns or deinitializes the executor. The
borrow carries the native type contract because this is a same-toolchain static
archive boundary. Standalone acquires the bounded inference lane before create,
the archive copies the interface into its state, and the host retains the lease
until every listener/provider call has completed and archive destruction has
returned. Only then may standalone release the lease and destroy
`BackendRuntime`.

## Observability

Runtime metrics and structured logs should expose:

- Task count and state by component and executor lane
- Worker count, queue depth, saturation, and rejected work by lane
- Listener state, active connections, and active requests
- Cancellation counts by source and reason
- Startup phase and duration
- Shutdown phase, remaining deadline, and duration
- Tasks exceeding their shutdown deadline
- The first fatal background error
- Outstanding executor leases

Lifecycle log records should include the component, task name, old and new
state, deadline, and failure cause. Operators should be able to determine why a
process is not ready or why shutdown is stuck without attaching a debugger.

## Validation

Lifecycle tests should cover:

- Failure after every startup phase
- Bind failure and partial route registration
- Executor and queue exhaustion
- Listener, connection, and request-lane reservation exhaustion at bind
- Connection scheduling rejection without inline accept-loop fallback
- HTTP/1 request saturation with 503 and no handler execution
- h2c upgrade saturation with 503 before the protocol switch
- H2 request scheduling rejection with `REFUSED_STREAM` and no inline
  frame-pump execution
- Per-listener request-quota isolation on a shared `HttpRuntime`
- Shutdown during active HTTP/1 and HTTP/2 requests
- Shutdown during storage commits and model generation
- Client disconnect during distributed and inference work
- Cancellation observed during a multi-stage operation before its irreversible
  publication boundary, with proof that no write was issued
- A task that ignores cancellation
- Provider destruction while work is pending
- Backend runtime destruction with an outstanding lease
- Repeated start/stop and immediate port reuse
- Exclusive rejection of a second live listener with `reuse_address` enabled
- Independent simultaneous ephemeral listeners
- Explicit `reuse_port` sharing on platforms that support it
- Moving a started listener-task handle before stop and join
- SIGINT and SIGTERM during startup, steady state, and drain
- Thread, file-descriptor, task, and memory counts after repeated cycles

HTTP and kernel migration tests should also cover:

- A generated inventory proving every OpenAPI operation is registered exactly
  once
- Route precedence and absence of accidental catch-all shadowing
- Canonical `/db/v1` behavior and explicit 404 coverage for removed root aliases
- Status, headers, content type, and body parity for success and error cases
- Malformed parameters and bodies, authentication, authorization, and overload
- Buffered and streaming response parity
- Direct kernel tests without an HTTP server
- Multiple simultaneous server instances with independent handler context
- API-kernel route-manifest ABI compatibility
- Linked lazy-body admission before transport buffering
- Absolute H1 and H2 phase deadlines under byte-trickle traffic
- Required HTTP/1 disconnect cancellation on every supported OS
- Successful cancellable outbound HTTP/1 requests returning promptly even
  when their configured deadline is long
- Outbound HTTP/1 cancellation interrupting an active response read and HTTP/2
  cancellation resetting only the selected stream

The CI matrix should include sanitizer and stress coverage, supported operating
systems and architectures, and the real ARM64 `ReleaseFast` linked build. The
linked-runtime bridge needs both ABI-focused tests and an executable smoke test;
a host Debug build alone does not validate the original compiler-memory issue.

API HTTP and linked-boundary tests use the focused `api-http-runtime-test`
discovery root. They are also part of `root-test` and `unit-test`, but no longer
force transport-specific test code through the monolithic library test root.
This keeps the existing 7 GiB aggregate compiler reservation honest instead of
raising it whenever the HTTP boundary grows; further growth should be handled
by another cohesive test shard, not by increasing the repository-wide default.

## Implemented migration sequence

The branch completed the migration in this order:

1. Defined common lifecycle states, one shutdown deadline, supervisor failure
   propagation, `RequestContext`, `ApiError`, and typed operation results.
2. Kept contextual handlers on `httpx` and removed generated `active_impl`
   globals.
3. Made each `httpx.Server` listener an owned Future on the appropriate
   executor lane, with an owned connection Group and deterministic shutdown.
4. Extracted route families into transport-independent operations with direct
   operation and canonical wire-contract tests.
5. Moved internal, HA, protocol, extension, and maintenance routes to explicit
   contextual registrars and typed operations.
6. Kept data and metadata public/admin listeners on `httpx.Server` while
   deleting their residual transport conversions.
7. Kept `/healthz` and `/readyz` root-only on control or reserved API capacity.
8. Moved standalone's unified listener to `BackendRuntime`'s API lane and
   removed its duplicated bridge handlers.
9. Deleted the residual public dispatcher, public executors, request/response
   conversions, duplicated public response adapters, and public
   `StdHttpListener` use.
10. Replaced embedded inference listener threads with owned Futures.
11. Added API/control lane leases, bounded HTTP capacity, and lifecycle and
    listener metrics.
    Long-lived accept loops, connections, and protocol-neutral request handlers
    use independent bounded `HttpRuntime` lanes. Per-listener request permits
    enforce each lease locally, while the injected API executor supplies
    `Context.io` for nested application/backend work.
12. Audited long-lived background work and retained explicit OS threads only
    where they remain owned, stopped, and joined.
13. Hardened the API-kernel and linked-inference boundaries with versioned
    function tables, owned route manifests, route body/stream metadata,
    request-scoped cancellation, lazy body sources, streaming sinks, prefix
    validation, and duplicate route rejection.
14. Added lifecycle, routing, cancellation, shutdown, ABI, restart, and
    resource-leak tests plus linked native and ARM64 Linux build coverage.
15. Migrated public API simulation fixtures from compatibility executors to
    shared-I/O `httpx` runtimes and made Windows listener reuse retain the
    platform's exclusive bind default.

Each migration should preserve a strict stop-and-await-before-deinit invariant.
Executor backend changes should occur only after the blocking and cancellation
audit for that lane is complete. Avoid substantial new investment in the legacy
public listener beyond correctness and migration safety; the structured runtime
work should converge on the one `httpx` production stack.
