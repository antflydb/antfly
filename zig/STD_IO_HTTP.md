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
  explicit stop, fatal-error publication, and mandatory join.
- Data, metadata, standalone, serverless, inference, and health listeners use
  the common ownership pattern; storage-backed roles borrow leased API or
  control lanes from `BackendRuntime`.
- Process roles share signal cancellation and one absolute shutdown deadline.
- Data, metadata, standalone, serverless, and the dedicated inference command
  compose their listener and control tasks under a common supervisor state
  machine. Readiness is gated on the supervisor reaching `ready`; the first
  fatal component/task error cancels the role and is preserved while teardown
  shares the original deadline.
- Supervisor phase and cancellation state are exported with data, metadata,
  standalone, and serverless health metrics. The dedicated inference command
  uses the same lifecycle while retaining model-specific readiness. The
  internal compatibility listener now owns connection tasks in a `std.Io.Group`
  and cancels/joins that group instead of detaching OS threads.
- The API-kernel and inference archives expose versioned function tables and
  immutable route manifests; the runtime owns router mutation and wire
  adaptation on both boundaries.
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
- The temporary internal compatibility registrar now enumerates every
  remaining canonical internal read, write, artifact, routed-batch, and
  retrieval-agent route. This preserves reachability during
  extraction without a global fallback or any legacy public alias; router
  registration still rejects duplicate route shapes.
- The ordinary internal group batch route now decodes directly into an owned
  batch request and invokes a typed operation for schema validation, local
  group write, cancellation, and outcome classification. Only the explicitly
  versioned routed-forwarding endpoint retains the temporary legacy
  cancellation adapter; forwarding headers on the ordinary route are rejected
  directly from `httpx` headers without manufacturing an `HttpRequest`.
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
- Remaining non-generated route families share one explicitly temporary
  request/response compatibility module, preventing per-runtime wire glue from
  diverging while each family is extracted. Data and metadata register those
  families from one explicit compatibility manifest; neither public listener
  has a global catch-all, so generated namespaces and unknown paths retain
  native `httpx` 404 behavior.

The remaining application migration is intentionally explicit: non-OpenAPI
admin/internal route families still use compatibility adapters, and public API
handlers still delegate substantial business behavior to `ApiHttpServer`.
Those adapters are deletion scaffolding, not a second supported transport.
Completion requires extracting the typed kernel operations and contextual
registrars described below, then deleting the compatibility paths.

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
    io: std.Io,
    serve_future: ?std.Io.Future(void) = null,

    pub fn start(self: *Listener) !void {
        // Bind synchronously so startup failures are returned directly.
        try self.bind();
        self.serve_future = try self.io.concurrent(serve, .{self});
    }

    pub fn stop(self: *Listener) void {
        self.requestStop();
        self.wakeAccept();
        if (self.serve_future) |*future| {
            future.await(self.io);
            self.serve_future = null;
        }
    }
};
```

Use `std.Io.concurrent` for a listener that must progress concurrently with its
caller. `std.Io.async` is appropriate for operations that can use its weaker
scheduling guarantee. Under `Io.Threaded`, a long-lived concurrent listener
still consumes a worker thread; the benefit is structured ownership rather than
elimination of threads.

## Executor topology

`BackendRuntime` already separates general, API, Raft-inbound, and
Raft-outbound `Io.Threaded` implementations. That should evolve into an explicit
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

Long-lived HTTP listeners should use the API lane rather than the general
storage lane. CPU-heavy model execution should not occupy the workers required
to serve readiness probes, wake shutdown, or complete storage commits.

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

Long term, borrowing should be represented by an executor-lane lease:

```zig
var lease = try backend_runtime.acquireApiLane();
defer lease.release();

const io = lease.io();
```

Debug and test builds should assert that no leases or registered tasks remain
when `BackendRuntime` is deinitialized. The component remains responsible for
stopping and awaiting its tasks before releasing the lease.

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

### Health

Health listeners should use `httpx.Server` on a supervisor-provided control or
API lane, and metrics refresh tasks should borrow the same executor set. They
should not create process-global or per-health-server executor state
implicitly.

Health capacity must remain available under API, storage, and inference
saturation. Readiness should be dropped before ordinary ingress is stopped so
load balancers can begin draining the process.

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

That boundary cleanup must not be confused with completion of the operation
extraction. The remaining contextual adapters must be reduced in this order:

1. Give MCP, A2A, ARD, extensions, and HA shared contextual registrars, with
   runtime-specific dependencies supplied explicitly. Storage maintenance and
   the root health/readiness probes have already crossed this boundary and
   must remain direct typed-operation routes.
2. Split metadata administration into typed operations and bind each concrete
   route directly to its operation; remove the method/path dispatcher.
3. Convert internal group and table modules to typed inputs and results, then
   adapt their real internal HTTP routes at the edge.
4. Replace MCP, A2A, and extension-host calls that construct an `HttpRequest`
   with direct calls to the same table, query, batch, backup, restore, and agent
   operations used by public handlers.
5. Delete the residual contextual request/response conversion and manual data
   dispatcher once the last route and in-process caller uses typed operations.

At completion, remove:

- The residual `ApiHttpServer.handle()` adapter
- The manual method/path dispatcher behind that adapter
- Business logic in the current `AntflyApiHandler`, replacing it with a thin
  generated transport adapter
- Public `RequestExecutor` and `StreamingRequestExecutor` adapters
- `httpx.Context` to `http_common.HttpRequest` conversion
- `http_common.HttpResponse` to `httpx.Response` conversion
- Standalone protocol, internal, HA, and extension catch-all bridge handlers
- Duplicated standalone route arrays covered by generated or contextual routes
- Duplicated authentication, parsing, retry, error, and response logic
- `StdHttpListener` usage for public, admin, and health APIs
- API-kernel handler create/register ABI calls once the route manifest and
  operation dispatcher replace them

The underlying kernel state and extracted operations remain. Legacy transport
types may be deleted only after Raft and any other internal consumers either
migrate or adopt an explicitly supported internal-only compatibility module.

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
- No unstable Zig errors, slices, or layouts passed by value
- Tests that intentionally detect layout and version mismatches

The bridge borrows `std.Io`; it never owns or deinitializes the executor.
Standalone must await listener and provider work, destroy the inference handle
through the archive that created it, and only then destroy `BackendRuntime`.

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
- Shutdown during active HTTP/1 and HTTP/2 requests
- Shutdown during storage commits and model generation
- Client disconnect during distributed and inference work
- A task that ignores cancellation
- Provider destruction while work is pending
- Backend runtime destruction with an outstanding lease
- Repeated start/stop and immediate port reuse
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

The CI matrix should include sanitizer and stress coverage, supported operating
systems and architectures, and the real ARM64 `ReleaseFast` linked build. The
linked-runtime bridge needs both ABI-focused tests and an executable smoke test;
a host Debug build alone does not validate the original compiler-memory issue.

## Remaining migration order

1. Define common lifecycle states, shutdown deadlines, supervisor failure
   propagation, `RequestContext`, `ApiError`, and typed operation results.
2. Keep contextual handlers on `httpx`; regenerate routers without
   `active_impl` globals.
3. Make the `httpx.Server` listener an owned Future on the appropriate executor
   lane, with an owned connection Group and deterministic shutdown.
4. Extract one vertical route family at a time into transport-independent
   `ApiKernel` operations. Run canonical wire-contract and direct-operation
   parity tests during this phase.
5. Move internal, HA, protocol, extension, and maintenance routes to explicit
   contextual registrars and typed operations.
6. Preserve data and metadata public and admin listeners on `httpx.Server` while
   removing their residual transport conversions.
7. Keep `/healthz` and `/readyz` at the root on a control or reserved API lane.
8. Move standalone's unified listener to `BackendRuntime`'s API lane; its
   duplicated bridge handlers are already removed.
9. Remove the residual public dispatcher, public executors, request/response
   conversions, duplicated handler logic, and public `StdHttpListener` use.
10. Replace the embedded inference server's detached thread with an owned
    Future.
11. Add lane leases, bounded executor capacity, and lifecycle metrics.
12. Audit Raft, maintenance, observer, and inference background threads and
    migrate them where structured `std.Io` concurrency is appropriate.
13. Harden the API-kernel and linked-inference ABIs.
14. Add adversarial lifecycle, routing, cancellation, shutdown, ABI, and
    resource-leak tests.

Each migration should preserve a strict stop-and-await-before-deinit invariant.
Executor backend changes should occur only after the blocking and cancellation
audit for that lane is complete. Avoid substantial new investment in the legacy
public listener beyond correctness and migration safety; the structured runtime
work should converge on the one `httpx` production stack.
