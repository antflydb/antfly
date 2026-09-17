# Workload scheduling implementation record

Design: [WORKLOAD_SCHEDULING.md](WORKLOAD_SCHEDULING.md).

The design was committed first as `74820f0ec2`. Implementation is in progress.
The current implementation provides fixed foreground admission with opt-in
bounded waiting. It does not yet implement the full execution scheduler or
qualify new defaults.

## Implemented admission contract

`common/workload_admission.zig` owns an allocation-free intrusive FIFO, active
operation count, queued count/bytes, and retained request reservations. A grant
removes queue accounting while preserving the request's byte reservation until
its lease releases. Cancellation rejoins the queue lock before retiring waiter
storage; a concurrent grant returns its execution and byte reservations exactly
once. Waiters use the caller's runtime and deadline clock. Queue reductions
retire newest excess waiters, preserve the oldest requests, and never revoke
running work. Closing admission rejects new work and retires waiters while active
leases remain owned by their callers.

All operations admitted here retain coarse operation leases. There is no claim
that these counts measure busy CPU threads. Payload reservations cover request
bodies and a conservative metadata allowance; they are not a measurement or
ceiling for all decoded allocations, output buffers, or execution working memory.
Existing transport and storage resource limits still govern those resources.

The REST/httpx, alternate-listener API-kernel paths, MCP, query builder, A2A,
extension-host query/write calls, and serverless query/write handlers share this
owner at their existing admission boundaries. Legacy nonwaiting callers cannot
jump ahead of queued work. Metadata/data teardown can close admission across the
compiled API boundary; API ABI version 20 includes that operation and expanded
statistics. No inference-provider admission or transaction durability contract
is replaced by this queue.

Query and write classes have independent fixed count/byte budgets. This preserves
their existing isolation and does not introduce borrowing between them. Configured
and effective queue bounds, queue residence histograms, active/queued counts,
retained bytes, expirations, cancellations, and draining are observable through
the shared admission metrics. Explicit zero execution capacity retains its legacy
unlimited meaning; explicit byte ceilings still apply to contextual leases.

The Go, TypeScript, Python, and Rust SDKs have optional shared client pools. They bound active operations and
waiting, observe context cancellation before dispatch, and hold a slot through
response EOF/Close. They preserve unknown write outcomes and do not add automatic
write retries. TypeScript shares pools across database/inference clients; Python
shares a FIFO across synchronous threads and generated asyncio calls. Python's
canceled stream cleanup retains a slot until its separately owned cleanup task
finishes. Rust's `PooledClient` constructs the request future after admission and
retains capacity in an `Admitted<T>` wrapper through stream ownership. Safe read
retries remain outstanding.

Public query timeouts are captured before admission and retained through catalog
binding, execution, and readiness retries. Native and runtime clock domains are
translated by remaining duration without extending the deadline. NDJSON batches
use one submission time and the shortest explicit timeout; later lines cannot
restart the budget. Deadline extraction skips unrelated JSON fields rather than
allocating a full vector/document tree before admission.
Serverless execution carries the captured deadline through a scoped cancellation
token. HTTP token adapters preserve fallible checkpoints and their timeout cause.

## Execution ownership core (not enabled for operators)

`common/workload_resources.zig` implements typed, generation-checked leases over
a fixed metadata pool. Atomic count/byte/working-set grants preserve protected
floors for each lane. Queue, runnable, retained state, resume queue, local I/O,
remote attempts, and recovery credits have distinct ownership. A request cannot
release live children; detached state transfers ownership without changing global
charges. Policy reductions preserve live reservations while rejecting growth.

`common/workload_scheduler.zig` supplies weighted service accounting corrected by
measured work, bounded resource-fit backfill, and deterministic large-request
barriers. Timer polling does not earn service credit. Existing continuations
reuse their reserved metadata and state when queueing for resumption, so full
start queues cannot prevent their progress. They precede new starts in their lane.
Grant/cancel races restore retained state to its caller, and resume cannot reduce
the charge below the state still owned. Transfers reject active execution or
queued ownership so scheduling identity cannot change underneath a waiter.

These modules are tested foundations. Their credit ledger is not a replacement
for resource-manager allocation reservations, worker fencing, or durable write
recovery. Production operators do not use this scheduler yet. Their integration
must supply audited completion bundles and separately charged actual allocations.

## Configuration

The canonical schema exposes `admission.query.waiting` and
`admission.write.waiting`. Both default to fail-fast behavior. An illustrative
fixed policy is:

```yaml
admission:
  query:
    max_concurrent_requests: 32
    waiting:
      max_queued_requests: 64
      max_queued_bytes: 16777216
      max_retained_bytes: 33554432
      max_wait_ms: 100
```

These values are examples, not qualified defaults. Positive wait ceilings require
positive queue count, queue bytes, and retained-byte bounds. Queue bytes cannot
exceed retained bytes. Waits are additionally limited by the original request
deadline; maximum configurable waiting is 60 seconds. A request larger than its
entire retained-byte envelope fails immediately without retry guidance. Queueing
does not raise a transport's independent connection or request-task limit.

## Operator and ownership audit

| Execution path | Current owner and boundaries | Requirement before fine-grained scheduling |
| --- | --- | --- |
| Public query and write operations | Admission lease held around the existing synchronous operation, including joins of its helpers | Split runnable, retained state, and request lifetime at verified quiescent boundaries |
| Query decoding/planning | Body admission precedes buffering; query decode follows foreground grant; some catalog/auth/session resolution precedes it | Account actual allocation expansion and introduce a separately bounded planning owner |
| Dense rerank and helpers | `storage/dense_work_admission.zig` has whole-caller FIFO admission and nonwaiting helper leases | Charge helpers against the same node execution envelope and avoid duplicate waiting |
| Vector, text, graph, aggregation | Existing cancellation/work budgets and storage resource reservations; no scheduler continuation contract established | Inventory maximum nonyielding intervals, resumable state, and minimum completion resources; remain in the general lane until verified |
| Scan/stream output | Handler retains its operation lease through the stream producer; snapshot/result lifetime remains storage/transport-owned | Transfer retained state before releasing runnable capacity; account slow consumers and resume only through admission |
| Remote coordinator/worker tasks | Existing request deadlines, route fencing, transport cancellation, and independent storage work limits | Add attempt identities, worker expiry, destination uncertainty accounting, generation fencing, and restart reconciliation |
| Transaction commits | Existing durable decisions, session identity, owner fencing, and `PendingSessionRecovery` | Reserve bounded mandatory-completion capacity before irrevocable decisions and transfer resource ownership to recovery |
| Background/control/recovery | Existing dedicated runtime owners and cancellation protocols | Prove process-wide protected count/byte/progress floors across foreground and background work |

No operator is admitted to a new protected bounded-plan execution lane by this
change. None releases execution on an unverified suspend boundary. Existing
committed-write recovery is preserved; client cancellation must not be used as
evidence that a write was rolled back or that remote work quiesced.

## Validation

```sh
cd zig
zig build antfly-common-config-test -j2
zig build antfly-workload-admission-test -j2
cd ../go/pkg/sdk
go test -race ./...
cd ../../../ts
pnpm --filter @antfly/sdk test
pnpm --filter @antfly/sdk typecheck
cd ../py/packages/sdk
uv run pytest
uv run pyright
cd ../../../rs
cargo test -p antfly-sdk
```

The dedicated Zig target covers FIFO pressure, count/byte bounds, deadline/grant
races with VOPR time, cancellation/grant races, policy reductions, draining,
compiled API ownership, legacy MCP responses, and serverless behavior. Controller
burst tests cover C1/5/10/20/30/40/60/80 with a fixed execution capacity of 32.
The real HTTP C80 fixture verifies 32 admitted plus 48 queued operations; its
missing-table responses test transport admission, not database query throughput.
Network tests need permission to bind local listening sockets.

## Remaining design phases

- Complete the prerequisite resource ownership model for continuations, remote
  attempts, mandatory recovery, and demotion; add their deterministic models.
- Complete Phase 1 ingress/planning/output byte ownership, process-wide protected
  floors, and all SDK/Cloud error contracts. The current byte counter is limited
  to foreground request reservations.
- Implement Phase 2 operator suspend/resume, execution working-set bundles,
  resource-fit backfill, bounded/general lane isolation, measured service debt,
  demotion transfers, helper/fan-out accounting, remote uncertainty fencing,
  and recovery handoff.
- Implement Phase 3 safe read retries, Cloud diagnostics
  and policy integration, adaptive control behind an explicit mode, and deployment
  qualification. Select numerical acceptance thresholds before measurements and
  retain both open-loop and closed-loop results before changing defaults.

Passing queue correctness tests does not satisfy the full design's release gates.
Automatic/adaptive modes and new default waiting policies remain unavailable.
The [qualification matrix](WORKLOAD_SCHEDULING_QUALIFICATION.md) records actual
Cloud package sizes and numerical release thresholds selected before measurement.
