# Operating the fixed workload policy

This branch provides opt-in mechanisms while the full scheduling design is
implemented and qualified. The default query admission capacity remains 32;
waiting and dense execution scheduling remain opt-in. See the
[implementation record](WORKLOAD_SCHEDULING_IMPLEMENTATION.md) for exact path
coverage and the [qualification gates](WORKLOAD_SCHEDULING_QUALIFICATION.md)
before selecting production policies.

## Capacity and waiting

`admission.query.max_concurrent_requests` bounds admitted query operations.
`admission.write.max_concurrent_requests` has an independent mutation budget.
These coarse operations may wait on storage or other services while admitted;
they do not measure running CPU threads. Legacy zero disables that count gate.

Migration note: public document GETs (including database/namespace aliases) and
MCP `get_document` previously bypassed foreground admission. They now share the
existing query capacity, including its default of 32. Lookup-heavy applications
may therefore see 429 under pressure and compete with queries for the same
configured budget. Waiting remains opt-in. Review concurrency and retained-byte
headroom when upgrading; this fixes policy coverage rather than qualifying a
new default. Metadata lookups, readiness, metrics, and control routes retain
their existing bypass behavior. Serverless currently has no document-lookup
endpoint; its supported query routes already use this gate.

To test throughput above C30 without intentionally testing admission rejection,
set the baseline and candidate's query capacity explicitly above the offered
concurrency and record it. Alternatively, deliberately test a smaller execution
capacity with bounded waiting and report rejection and queue time separately.
Neither choice increases the independent HTTP connection/request-task limits.

With `admission.ingress.max_requests` enabled, the data/API listener provisions
at least that many request tasks and 32 additional connection slots for parsing
and rejection. Disabled ingress preserves its legacy 32-task/64-connection
limits. This does not multiply upload buffering: body slots stay bounded at 32.
Standalone also sizes request tasks to cover configured ingress, while retaining
its descriptor-based connection limit and existing upload-buffer ceiling.

Control and authenticated recovery request-task reservations are inside the
listener's total capacity, matching the configured ingress partitions. Empty
GET/HEAD health/readiness probes can use control slots; only bounded, signed
internal completion/control requests can use recovery slots. HTTP/1 rejection
before execution returns structured 429 with `execution_started=false` and
`Retry-After: 1`; HTTP/2 dispatch rejection uses `REFUSED_STREAM`. Authentication
and framing are checked before selecting recovery capacity and checked again
at application ingress. These reservations protect request tasks, not incoming
connection slots, header parsing, or the entire process. The separate health
listener retains its independent listener/runtime reservation.

`antfly_http_request_permit_rejections_total` distinguishes exhausted listener
request partitions from `antfly_http_request_executor_rejections_total`, which
records failures to dispatch a task after acquiring its permit. Their sum is
`antfly_http_request_dispatch_rejections_total`.

Waiting is configured under the class's `waiting` object:

| Field | Meaning |
| --- | --- |
| `max_queued_requests` | Maximum requests held in the admission FIFO |
| `max_queued_bytes` | Maximum payload/metadata reservations held by waiters |
| `max_retained_bytes` | Shared ceiling for request reservations and tracked query/output allocations, including output retained after execution |
| `max_wait_ms` | Maximum admission wait within the original query deadline; zero is fail-fast |

Positive waiting requires all four positive bounds, with queue bytes no greater
than retained bytes. The retained ceiling needs headroom for admitted execution
and output, not just waiting bodies. Actual allocation coverage is incomplete;
storage resource-manager and transport limits remain necessary. A reduced policy
preserves live ownership and blocks further growth until it fits.

## Dense execution

`admission.dense_execution` is disabled when `max_runnable_tasks` is zero.
Its limits apply to integrated dense drivers and helpers, not every operator in
the process:

| Field | Meaning |
| --- | --- |
| `max_runnable_tasks` | Shared dense driver/helper permits, including coarse paths that retain a permit while waiting |
| `max_outstanding_tasks` | Dense request owners, including queued and suspended owners; must cover runnable capacity |
| `max_queued_tasks` | Bounded waiting for fresh dense starts; helpers do not wait |
| `max_wait_ms` | Queue allowance within the original request lifetime |
| `max_working_bytes` | Actual bytes in integrated exact-scoring workspaces and opted-in native read arenas; zero retains legacy accounting |
| `max_suspended_io` | Native exact positional reads allowed to release runnable ownership while retaining an I/O credit; zero retains coarse execution |

Suspended I/O requires positive working-memory and waiting budgets, and its
ceiling cannot exceed outstanding capacity. It applies only to audited serial
immutable exact reads with preallocated destinations and no live helper wave.
When its I/O credits are full, another caller keeps its runnable permit during
the read. It is not a ceiling on all storage I/O.

This path requires a proven pinned-caller executor. The native runtime bridge
preserves the originating Threaded executor's capability across the compiled
storage boundary; unknown or migrating providers remain coarse. The synchronous
system read cannot be preempted. After it returns, the caller must reacquire
runnable capacity before decoding or continuing, using the original deadline
and cancellation. Pooled HBC scratch, result ownership, projection/residual
helpers, and other unaudited paths keep their existing policies.

Dense-only scheduling does not supply protected interactive capacity. Keep
fixed policies for qualification; automatic/adaptive sizing is unavailable.

## Reading the diagnostics

The existing Prometheus endpoint exports the same admission metrics across
runtimes. Replace `query` with `write` or `inference` for the corresponding class.
Check `antfly_admission_query_diagnostics_available` first: zero means that the
provider supplies only partial legacy statistics, not that pressure is absent.

The dedicated health endpoint refreshes its cached metrics asynchronously every
five seconds. A scrape immediately after a request may still show startup or
pre-request counters. Before using idle zeroes as cleanup evidence, observe a
refreshed snapshot that includes known admitted work. These cached metrics alone
cannot establish the finer queue-retirement or recovery timing gates.

| Observation | Interpretation |
| --- | --- |
| `rejections_by_reason_total{reason="execution_capacity"}` increases | Fail-fast capacity rejection, or a legacy caller refused because earlier work is queued |
| `reason="queue_count"` or `reason="queue_bytes"` increases | The bounded waiting room filled; increasing execution is not automatically the remedy |
| `reason="retained_bytes"` increases | Existing request/output ownership leaves insufficient bytes for a new request |
| `reason="request_bytes"` increases | A request exceeds the entire configured retained envelope; repeating it does not make it fit |
| `reason="wait_timeout"` increases | Waiting consumed the server admission allowance before execution |
| `reason="draining"` or `reason="policy_reduction"` increases | Lifecycle or policy changes rejected work rather than CPU saturation |
| `allocation_denials_total` increases | A tracked allocation could not grow; this counts allocation attempts separately from rejected requests |
| `in_flight_requests` is zero but `retained_bytes` is positive | Output or other tracked storage still owns memory after foreground execution |

The reason and allocation names above belong to the
`antfly_admission_query_` namespace. Inspect actual capacity gauges alongside
active/queued counts, retained bytes, and the admission wait histogram.
`policy_generation` is a process-local revision that advances on accepted
reconfiguration and resets on restart. It is not a cluster-wide configuration
version. Compare metric series with process identity and restart information.

`antfly_dense_execution_*` gauges expose runnable, outstanding, fresh-queue,
tracked working bytes, and configured ceilings. `suspended_io` and
`suspended_io_limit` show held I/O credits and their configured limit. The
compiled storage-owner metrics distinguish the effective executor capability,
but the public suspended-I/O limit gauge is configured capacity; a nonzero
limit alone does not prove that a query reached an eligible read boundary.

`antfly_data_runtime_status_refresh_submit_deferred_total` counts status work
that could not obtain a durable executor slot. These submissions retain their
wake and retry after a bounded delay, while existing cached/Raft-only status
can remain available. This is distinct from refresh execution failures. The
fix preserves control-loop availability under transient executor saturation;
it does not promise maintenance fairness under permanently saturated work.

Cloud can consume these bounded-cardinality diagnostics without querying the
engine synchronously on each user request. Dashboard integration, sustained
capacity recommendations, and coordinated account-wide limits remain separate
work. A process-local limit must not be presented as an account-wide contract.

## Deadlines and retries

Public query `timeout_ms` starts before admission. NDJSON uses the original
submission time and the shortest explicit timeout across its lines. Storage
queues and readiness retries consume that same budget.
Document lookups preserve the incoming request deadline and cancellation through
admission, storage options, and readiness retries. They do not gain a new public
`timeout_ms` parameter from this change. Late lookup results are freed before a
timeout/cancellation response is returned.

SDK read retries are optional and bounded. A known query route is eligible only
for an explicit 429 `instance_busy` response with `stage="admission"` and
`execution_started=false`. Pool ownership ends before backoff. Retries preserve
the shortest caller, body, and retry-policy deadline; they do not restart the
query timeout. Unknown bodies/outcomes and writes do not retry automatically.

An execution or output memory failure reports `execution_started=true`. A write
may already have committed when its output failed. Preserve its transaction or
idempotency identity and inspect the existing write outcome contract; do not
turn the HTTP status alone into a new write attempt.

## Durable remote worker prerequisite

`admission.remote_attempt_worker` is opt-in and currently covers authenticated
internal `join-rows` requests only. `max_attempts=0` disables it; enabling it
requires the durable API session backend, runtime node identity, and configured
internal service authentication. `max_attempts` bounds journal records and
coordinator fence entries, `max_bytes` bounds their reserved journal footprint,
and `max_run_ms` caps local execution time within the received remaining budget.
The supported maximums are 4096 attempts, 64 MiB journal reservation, and 60
seconds per run. These are implementation ceilings, not qualified policy values.

Active duplicates do not run again. A terminal duplicate returns completion
evidence, not a stored copy of the original result. Generation closure becomes
durable before cancellation is signaled; signed quiescence evidence requires
the handler to unwind and terminal state to persist. Deadline expiry or an
ordinary HTTP response does not establish that evidence.

Restart preserves unknown old-incarnation work and its capacity charges; new
work remains blocked until uncertainty is reconciled. Lowering limits cannot
erase existing obligations, and deleting/restoring the journal is not fencing.
Terminal tombstones remain charged until durable generation closure permits
retirement. The v2 journal updates point records and accounting atomically;
only startup and generation fences scan bounded records. Read-only diagnostics
and duplicate reconciliation do not write the journal. Performance qualification
remains outstanding. A legacy v1 journal causes `WorkerJournalMigrationRequired`:
its state must be preserved for an offline migration that fences earlier writers.
Do not delete the journal to bypass this refusal. Enabling this worker does not
enable coordinator dispatch, cluster-wide uncertainty reconciliation, or automatic
remote-work retirement.

## Qualification

Use Debug for iteration and lifecycle correctness and ReleaseSafe for optimized
safety checks. Final performance comparisons use separately built ReleaseFast
baseline/candidate artifacts with retained source/build/configuration receipts.
The [local harness](../scripts/WORKLOAD_QUALIFICATION.md) uses direct processes
or resource-limited Docker containers; it does not require Kubernetes.

Retain generator drops, admission rejects, unknown outcomes, and errors alongside
latency and completed throughput. A generator that falls behind invalidates
offered-load evidence. Small harness smoke tests and passing unit tests do not
qualify new defaults or satisfy the full release matrix.

Tracked query/lookup allocation ownership, dense overload responses across
runtime boundaries, and background task-capacity failures have focused
regressions. Consult the [validation record](WORKLOAD_SCHEDULING_VALIDATION.md)
for native results and retained failed experiments. No optimized performance
pass is claimed by this guide. Full operator coverage, fair bounded/general
lanes, protected control/recovery progress, coordinator reconciliation,
streaming retention, and Cloud policy integration remain release work.

## Frontend and retained session envelopes

The optional `admission.ingress` envelope limits accepted requests until their
response has drained. Its count and byte totals include a nonborrowable control
partition; they are not additional capacity on top of the totals. Only empty
GET/HEAD health and readiness probes use this partition. Other requests acquire
ordinary ingress capacity before authentication and application parsing.

```yaml
admission:
  ingress:
    max_requests: 128
    max_retained_bytes: 67108864
    control_requests: 2
    control_retained_bytes: 262144
  session_max_retained_bytes: 67108864
```

These are explicit example limits, not qualified defaults. Ingress defaults to
disabled (`max_requests: 0`, `max_retained_bytes: 0`). Enabling it requires both
ordinary and control capacity. Query/write limits still apply beneath ingress;
a request cannot bypass either ceiling by moving work to an offload executor.
Slow consumers keep their outstanding reservation and output charge until the
transport releases the response. Streaming also observes transport progress
limits and request cancellation.

`session_max_retained_bytes` defaults to 64 MiB and separately bounds retained
MCP, A2A task, and transaction-session allocations. Before a stable transaction begins
execution, the server reserves completion memory for its sealed request and
terminal record. Startup rebuilds reservations for existing recovery obligations
before ordinary sessions can consume that capacity. Reducing the limit below
existing obligations fails initialization; it does not discard those obligations.
An uncertain transaction must be observed or retried using its existing ID.
Lease loss or an unavailable coordinator does not establish that it aborted.

Ingress covers tracked frontend allocation, not all process memory. HTTP body
buffers, connection framing/HPACK, storage snapshots/caches, inference runtimes,
Wasm stores, and durable background jobs retain their own resource owners and
limits. In particular, these settings do not qualify a process RSS ceiling or a
protected CPU service guarantee. Execution scheduling and release qualification
have separate gates.


## Shared read execution

`admission.read_execution` applies a shared fixed storage read limit. It is
mutually exclusive with enabled `admission.dense_execution`. Both default to
disabled; this example is an explicit policy, not a qualified sizing default:

```yaml
admission:
  read_execution:
    max_runnable_tasks: 2
    max_outstanding_tasks: 8
    max_queued_tasks: 4
    max_wait_ms: 25
    max_working_bytes: 8388608
    max_scan_state_bytes: 1048576
    max_scan_snapshot_ms: 30000
    protected:
      max_runnable_tasks: 1
      max_outstanding_tasks: 1
      max_working_bytes: 65536
      max_transition_tasks: 1
      max_transition_bytes: 65536
```

Protected and transition partitions are carved from these totals. Each possible
protected probe needs a 64 KiB scratch reservation and a transition ticket.
The default LSM backend stays in general execution; omit the protected
partition unless serving eligible LMDB reads. Protected eligibility is verified
by the engine: local LMDB, bounded key,
empty projection, JSON storage, no TTL, and bounded validation. Wider records
restart in the general lane after closing their first snapshot. General work
cannot consume the protected execution floor, but upstream query/ingress limits
still apply. A transition waits at most 100 ms and never beyond the original
request deadline.

`max_scan_state_bytes: 0` keeps scans coarse. A nonzero value prepays each
participating scan's owned buffers, with 4096 bytes reserved for logical pin
bookkeeping. It must fit `max_working_bytes`. The absolute snapshot ceiling
also reaches socket and HTTP/2 flow-control waits; a trickling consumer cannot
renew it. Physical MVCC pages pinned by snapshots remain a storage concern.
Suspension requires LMDB, a proven pinned runtime, and a deadline-capable sink.
LSM scans stay coarse until their mutable snapshot allocations have an audited
retained-state certificate.

Serverless supports shared general reads and separately admitted graph helpers.
Omit `protected` and leave `max_scan_state_bytes` zero there; unsupported
provisioned-only settings fail startup instead of being ignored.

Native metrics use `antfly_read_execution_*` when shared reads are enabled;
legacy dense-only mode retains `antfly_dense_execution_*`. Metrics include
runnable/outstanding/queue state, participating working bytes, protected work,
and parked transitions. Serverless metrics include a `read_execution` snapshot.
These byte metrics cover participating workspaces, not the entire process.
