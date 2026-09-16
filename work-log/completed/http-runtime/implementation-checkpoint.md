# HTTP Runtime: Implementation Checkpoint

> Relocated verbatim from `zig/HTTP_API_RUNTIME.md` (lines 17–349 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`HTTP_API_RUNTIME.md`](../../../zig/HTTP_API_RUNTIME.md). Durable decisions from this log were folded into that document before the move.

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
- The canonical A2A agent-card read now uses a direct `httpx` handler over an
  owned JSON builder. Card generation no longer constructs a request or
  response compatibility object. The former `/db/v1/.well-known/agent-card.json`
  compatibility location is intentionally not registered and receives the
  router's native 404 response.
- Buffered A2A JSON-RPC and event-stream responses now use a direct `httpx`
  adapter over typed authorization/body inputs and the shared owned contextual
  result. The old buffered `HttpRequest`/`HttpResponse` adapter has been
  deleted, along with the legacy live-streaming executor entry point. A future
  incremental public-streaming implementation must use the typed operation and
  `httpx` transport contracts rather than restore that compatibility executor.
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

