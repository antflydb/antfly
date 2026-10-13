# HTTP Runtime: Implementation Checkpoint

> Summary of a verbatim log. Full text: [docs/operations/http-runtime/history/implementation-checkpoint.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/operations/http-runtime/history/implementation-checkpoint.md). Originally relocated verbatim from `zig/HTTP_API_RUNTIME.md` (lines 17–349 at commit 271838a195) on 2026-09-16; the living design is [`HTTP_API_RUNTIME.md`](../../../../zig/HTTP_API_RUNTIME.md), into which durable decisions were folded before the move.

Route-by-route changelog of the branch that moved every listener onto `httpx` with
role-owned runtime services and transport-neutral typed operations. The end state:
no synthetic `HttpRequest`/`HttpResponse` dispatcher remains anywhere (metadata,
internal, public, A2A, MCP, ARD, HA), `ApiHttpServer.handle()` and its public
executors are deleted, and unknown paths get native `httpx` 404s.

## Decisions
- **Explicit instance context and owned listeners** — `httpx` handlers and generated routers carry explicit instance context; long-lived listeners are owned Futures with synchronous bind, explicit stop, fatal-error publication, and mandatory join, capturing separately allocated run state rather than the address of a movable task handle.
- **`HttpRuntime` as the role-owned transport service** — owns one bounded HTTP/1 cancellation multiplexer, listener leases, independent bounded listener/connection/request-execution lanes, reserved capacity, health, and metrics. Handler invocation is admitted by `HttpRuntime` while handlers keep the injected `std.Io` for nested work, so keep-alive connections and HTTP/2 frame pumps cannot exhaust request execution or a `BackendRuntime` API lane.
- **Transport-provided cancellation on every `httpx.Context`** — HTTP/2 uses stream reset; HTTP/1 registrations share the role's `HttpRuntime`. Linked API and inference ABIs carry the same semantic cancellation callback.
- **`error.Canceled` is the canonical response-free terminal outcome** — `error.Cancelled` is normalized at ingress; cancellation closes/resets the request and increments a dedicated counter, never a synthetic 500. Deadlines stay distinct and map to 504 when a response is still legal.
- **Serverless cancellation** — both serverless adapters borrow the semantic callback; admission rechecks it before and after dispatch, and long operations (embedding, artifact fetch, graph traversal, join scans, postings and RaBitQ inner loops, provider pacing sleeps, write-publication waits) have bounded checkpoints. Public-table callbacks have one required contextual signature, avoiding a fallback path that could silently drop the token.
- **Stable application executor for outbound work** — serverless lends its process-owned `std.Io` to embedders and remote template helpers instead of creating short-lived threaded executors, which can exhaust process thread resources under sustained traffic. Linked archives likewise do not construct private `std.Io.Threaded` pools.
- **Cancellable outbound requests own resolve/connect/request in one `std.Io` task** — so DNS and initial connect fall inside the cancellation and deadline boundary.
- **Committed HTTP/1 streams close on handler failure** rather than writing a second status line; linked stream callbacks preserve cancellation/timeout/capacity/end-of-stream status classes.
- **Lazy transport-owned request bodies** — the API kernel acquires body admission before asking the listener to buffer, so direct and linked handlers enforce the same limit and metrics.
- **Separate absolute header and body deadlines** — HTTP/2 body readers keep one deadline across all DATA waits so trickle traffic cannot renew a timeout or hold an upload permit.
- **Query/write admission are application-operation gates owned by `ApiHttpServer`** — shared by generated HTTP, MCP, A2A, extension-host, and other in-process entry points.
- **One inventory-backed continuous-HA mutation policy** across generated, contextual, and internal routes (middleware for direct handlers, API-kernel policy for linked ones), so removing the compatibility dispatcher cannot remove the fail-closed gate or let a new non-GET route bypass classification.
- **Listener socket policy** — `SO_REUSEADDR` for deterministic restart; `SO_REUSEPORT` only as explicit opt-in; exclusive kernel ownership by default; no process-local lock files.
- **Common supervisor state machine** for data, metadata, standalone, serverless, and inference roles — readiness gated on `ready`; first fatal error cancels the role and is preserved; one shared signal cancellation and absolute shutdown deadline. Listener lifecycle transitions are monotonic: once stop wins, late listeners are rejected and the first genuine failure is retained. Supervisor phase is exported in health metrics.
- **Versioned function tables and immutable route manifests** for API-kernel and inference archives; the runtime owns router mutation and wire adaptation.
- **Transport-neutral operation layer** defines request identity, principal, cancellation, absolute deadline, admission reservation, typed result, and backpressured streaming. JSON/status/header adaptation stays at the `httpx` edge; consistency, storage lookup, and error classification live in typed operations.
- **Generated route inventories** include operation ID, request-body mode, and streaming-response metadata, with uniqueness/contract tests; route-manifest registration uses the router's normalized duplicate-shape validation.
- **Test fixtures use a real owned `httpx` test runtime** (listener tasks, stop/join teardown, deterministic restart) instead of `ApiHttpServer.executor()`.

## Migration order (abridged)
1. Root Kubernetes probes and storage-maintenance jobs (first vertical slices).
2. Metadata: status reads, catalog publication, extension lifecycle, node lifecycle, table DDL/artifact enrichment, then restore/split/merge/reseed — after which the metadata manual dispatcher, catch-all, and `handle` entry point were deleted.
3. Internal: repair cancellation lookup, median-key/document reads, distributed-join workers, split/merge/transition, batch and routed-forwarding writes, transactions, artifact placement/reprocess/repair, scans, graph workers, text-statistics/partials, query/preflight/vector, retrieval agent — ending with deletion of the last `http_internal_routes` dispatcher.
4. Public/contextual: ARD, extension agents and management, HA admin/replication (removing the last route into `ApiHttpServer.handle()`), transaction sessions, query builder (shared with A2A), extension WASM host imports, A2A card and JSON-RPC, MCP, table repair, restore jobs, single/multi-query.
5. Removal of the shared non-generated compatibility manifest and listener catch-alls; data and standalone register generated and contextual families explicitly.

## Rejected / removed
- **Compatibility dispatcher and synthetic request/response conversion** — deleted everywhere; a future incremental public-streaming implementation must use the typed operation and `httpx` contracts rather than restore the legacy A2A live-streaming executor.
- **`/db/v1/.well-known/agent-card.json` compatibility location** — intentionally not registered; returns the router's native 404.
- **Detached OS threads in the internal compatibility listener** — replaced by a `std.Io.Group` that is cancelled and joined; accepted sockets are registered before executor handoff.
- **Dead artifact wildcard registrations** — removed so unknown artifact operations are rejected by the router.
- **Removed data aliases** — wire tests prove they return 404; `/healthz` and `/readyz` stay root-only.

## Behavior notes recorded in the log
- The HA client-facing legacy executor is retained only as an adapter over the typed operation for internal HTTP clients not yet on `httpx`.
- Durable terminal transaction sessions remain visible until TTL cleanup so commit retries replay the stable result.
- Restore submission validates the full request and backup location before reporting worker availability, keeping deterministic client errors ahead of transient admission failures.
- The in-repository API client resolves public operations under `/db/v1` while internal group RPCs and the contextual retrieval worker stay rooted.
