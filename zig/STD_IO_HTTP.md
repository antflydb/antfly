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

> **Relocated:** The route-by-route implementation-checkpoint changelog that previously lived here (333 lines) is preserved verbatim in [work-log/completed/http-runtime/implementation-checkpoint.md](../work-log/completed/http-runtime/implementation-checkpoint.md). Durable decisions from it are folded into the deadline/cancellation, HTTP transport runtime, and continuous-HA sections below; see also [`Implemented migration sequence`](#implemented-migration-sequence).

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

Listener startup is also a supervised operation. The task that owns the
listener publishes its first terminal startup transition through a
`std.Io.Event`; the process owner waits on that event with both the startup
deadline and the process-signal cancellation token. Readiness, bind failure,
explicit stop, cancellation, and timeout are therefore observable without a
`std.Thread.yield` polling loop. The event is only the notification mechanism;
the lifecycle state remains the source of truth, so publication and stop races
have one monotonic outcome.

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
listener, connection, and request task has been joined. The executor injected
into each server remains the application `std.Io` handlers use for nested
application/backend operations; handler invocation itself is admitted and
scheduled separately by `HttpRuntime`'s request lane. Long-lived keep-alive
connections and HTTP/2 frame pumps therefore cannot exhaust request execution
or a `BackendRuntime` API lane.

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
shutdown expires, the supervisor escalates to cancellation and then process
termination. It must not deinitialize memory still referenced by a stuck task.

The first request for the shared shutdown deadline arms one hard watchdog for
that absolute timestamp. Clean completion disarms and joins it before the
supervisor becomes stopped. The watchdog deliberately uses one owned OS thread,
not a task on `std.Io`: it must remain schedulable when the executors it is
policing are saturated, deadlocked, or occupied by non-cooperative work. It is
created only during teardown, sleeps against the platform monotonic clock, and
is not a general runtime lane. If the deadline wins, it exits immediately
without running destructors, because a task that failed to drain may still hold
those objects. This makes the process deadline a hard lifetime bound rather
than an advisory timeout followed by an unbounded join.

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
application error responses. `error.Canceled` is the canonical response-free
transport terminal outcome; `error.Cancelled` is a second spelling that
persists in client and application error sets and is normalized to the same
outcome at the `httpx` ingress boundary, so neither spelling can surface as a
synthetic 500. If no response has committed, `httpx` terminates the stream or
connection without emitting a status and records `request_cancellations_total`
for either spelling. This preserves the peer-disconnect/server-stop meaning
and avoids misleading 500 logs. `error.DeadlineExceeded` remains distinguishable
and maps to 504 before commitment. After a response is committed, either
outcome closes/resets the transport because a second status line is
impossible.

Linked-runtime body and response callbacks check the same semantic token both
before blocking transport I/O and after a failed read, stream start, write, or
close. Cancellation can arrive while the callback is blocked and commonly
wakes it as `StreamReset` or `ConnectionClosed`; when both facts are present,
cancellation takes precedence. This preserves response-free cancellation
instead of translating a cancellation-induced transport error into a generic
handler failure or 500.

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

A future incremental public-streaming implementation (for example A2A
live-streaming responses) must use this typed-operation and `httpx` transport
contract rather than reintroduce a buffered compatibility executor.

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
retryable metadata-authority response mapping. This mutation-classification
gate is one fail-closed, inventory-backed policy enforced uniformly across
generated, contextual, and internal routes: direct `httpx` handlers install it
as middleware, linked handlers enter the same API-kernel policy before
invoking a route from the manifest, and a new non-GET route cannot bypass
classification. It must not rely on direct-
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

API HTTP and linked-boundary tests use the focused `antfly-api-test`
discovery root. They are also part of `root-test` and `antfly-unit-test`, but no longer
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
