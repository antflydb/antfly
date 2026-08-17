# Resource and model manager design

Antfly has three cooperating process services because they answer different
questions:

- `ResourceManager` owns physical, process-wide capacity shared by storage,
  indexing, caches, and embedded inference.
- `ModelManager` owns inference objects and decisions: model inspection,
  memory estimates, residency, request working sets, eviction, and backend
  selection.
- `BackendRuntime` owns execution infrastructure and its lifetime: bounded I/O
  executor lanes, durable background jobs, native storage I/O pools, and
  shutdown fencing. It decides where and for how long work may execute, not
  whether enough memory exists for that work.
- `RunBudget` is request-local accounting. It prevents one execution from
  exceeding the limits granted by `ModelManager`; it is not a node scheduler.
- `AdmissionController` is the adapter between model-aware estimates and the
  physical owner. It atomically mirrors inference leases into the external
  `ResourceManager` when one is present and also performs live pressure checks.

Connecting these services does not merge them. `ResourceManager` cannot decide
which model can be evicted, `ModelManager` must not independently assume it
owns all memory in a process that also serves data, and `BackendRuntime` lane
capacity must not be treated as permission to allocate memory.

## Ownership by deployment mode

| Deployment | Physical owner | Model owner | Execution owner | Required connection |
| --- | --- | --- | --- | --- |
| `antfly inference run` | local inference admission controller | process-local `ModelManager` | command-owned `std.Io` executor | local ownership is declared at `Node` creation |
| full standalone Antfly | the data node's `ResourceManager` | embedded `ModelManager` | node `BackendRuntime`; inference borrows its isolated inference lane | external resource owner and inference lane are required before preload or serve |
| distributed data process | that process's `ResourceManager` | none unless inference is embedded | process `BackendRuntime` with data, Raft, API, and control lanes | no cross-process memory or executor ledger |
| distributed inference process | local inference admission controller | process-local `ModelManager` | role-local executor | cluster scheduling routes work; process admission protects memory |
| offline conversion/inspection | command-scoped budgets | command-scoped loaders | command-owned executor | no serving residency contract |

Memory coordination is deliberately process-local. A network-wide reservation
protocol would be stale at allocation time and would couple failure domains.
Distributed placement and request routing use advertised capacity; every target
process still performs authoritative local admission.

The Apache-licensed inference package also cannot import Antfly's internal
storage `ResourceManager`. The standalone ABI bridge preserves that boundary:
inference exposes generic reserve/retain/release callbacks, while the full
product maps them to node-owned resource slices.

## BackendRuntime: execution and lifetime authority

The node `BackendRuntime` in `pkg/antfly/src/storage/background_runtime.zig`
owns process-long execution machinery. Its responsibilities are:

- bounded general, Raft inbound, Raft outbound, public API, inference, and
  control-plane `std.Io` executors;
- a durable job lane with owner identities, close/drain semantics, and exact
  job-payload ownership transfer;
- shared native storage I/O pools;
- lifetime leases that prevent an executor lane from being destroyed while a
  component still retains its `std.Io` interface;
- shutdown ordering: close lane admission, reject new borrowers, drain active
  leases and owned jobs, then destroy executors and pools;
- lane telemetry for active/peak leases, acquisitions, and shutdown
  rejections.

The separate lanes are isolation domains. Public API saturation cannot consume
the last control path, inference graph/model I/O and nested fan-out cannot
ratchet API workers, and Raft traffic does not share the public listener's
executor ceiling. A lane's bounded worker count also bounds retained thread
stacks, but this is an execution guardrail rather than memory admission.

Full standalone creates one node `BackendRuntime`, acquires its inference lane,
and passes the borrowed `std.Io` interface through the inference ABI. The lease
is retained until the embedded inference `Node` is destroyed. The same node
runtime supplies the API and control lanes and is shared by storage maintenance
and durable jobs. This makes executor ownership and shutdown order consistent
without making inference depend on Antfly storage internals.

The ordering contract for composed standalone startup is:

1. construct the node `BackendRuntime` and acquire the inference lane lease;
2. construct the inference `Node` with that borrowed executor;
3. attach the node `ResourceManager` admission bridge;
4. preload models, then publish request surfaces;
5. during shutdown, stop and await submitted work, destroy the inference node,
   release lane leases, and finally destroy the `BackendRuntime`.

Resource admission precedes scheduling: `ResourceManager` and `ModelManager`
must reserve capacity before code submits allocation-producing work to a
backend lane. Executor saturation may queue, reject during shutdown, or apply
concurrency backpressure; it must never bypass or manufacture a resource
reservation. Conversely, a memory reservation does not grant an executor lane
or extend its lifetime.

There is also an inference type named `backends.BackendRuntime` in
`pkg/inference/src/backends/backends.zig`. It is a small value describing the
selected backend and concrete execution provider—for example, whether ONNX is
CPU- or CUDA-hosted—so admission can select the correct physical domain. It
does not own threads, tasks, shutdown, or budgets. New code should keep this
distinction explicit; when ambiguity is possible, use “node BackendRuntime” for
the executor owner and “inference backend runtime descriptor” for the value.

## Invariants

1. Every serving `Node` declares `local` or `external_required` ownership before
   loading a model.
2. `external_required` fails closed until a resource-budget bridge is attached.
   This check runs before startup preload, serving, and inference acquisition.
3. Admission happens before allocation. A successful model or run reservation
   is retained for the lifetime of the corresponding resident or transient
   memory and is released on every teardown path.
4. A model transition reserves its construction peak before import, retains its
   post-load residency, and acquires any later growth before promotion.
5. `ModelManager` may satisfy temporary contention by evicting an idle model and
   retrying. It must not retry a request that is intrinsically larger than the
   configured hard limit.
6. The external manager sees model residency, KV working set, and scratch
   working set as separate logical slices. Those metrics may contain host plus
   accelerator bytes, while only the physical host component is charged to the
   `ResourceManager` host aggregate on discrete-GPU systems. Unified-memory
   systems charge both components. The inference controller retains the split
   and remains authoritative for device capacity.
7. A process envelope is charged against raw container usage, not only memory
   allocated through inference. Page cache, a test harness, storage, and sibling
   work therefore reduce the capacity available to a new inference allocation.
8. Every retained `std.Io` interface has a live owning `BackendRuntime` lane
   lease, and the borrower stops and awaits its tasks before releasing that
   lease.
9. Lane concurrency and resource admission are orthogonal. Work must satisfy
   both contracts before it can allocate and execute.
10. Release accounting is fail closed. Batch admission returns an exactly-once
    ownership token, retain operations can only shrink that token, and teardown
    releases the token rather than reconstructed byte totals. Malformed,
    overflowing, stale-observer, or over-release input retains capacity and
    increments an accounting error counter; it must never erase capacity owned
    by unrelated work.

## Budget derivation

Budget sources have this precedence, with an explicit value always clamped by
any smaller finite cgroup limit:

1. an explicit process/container envelope supplied by the operator;
2. a finite cgroup limit (`memory.max` or the v1 equivalent);
3. host memory as a development fallback.

An explicit envelope is necessary for Burstable Kubernetes pods whose request
is lower than node memory but whose cgroup hard limit is `max`. Kubernetes does
not expose the request as an allocation boundary inside that cgroup. Inference,
distributed data, and full standalone accept
`--process-memory-budget-mb` and
`ANTFLY_PROCESS_MEMORY_BUDGET_MB`. The older inference-prefixed flag and
environment variable remain compatibility aliases for inference-capable
processes. CLI values take precedence—including an explicit zero that requests
automatic detection—and malformed or overflowing selected values fail startup
instead of silently reverting to host detection.
Set the envelope below the orchestrator allocation so the kubelet, runtime, and
test harness retain headroom.

The process envelope is not another inference slice. One resolved value is
passed to storage and inference during standalone composition. ResourceManager
derives an aggregate managed-host-memory budget from it; every storage slice
reservation charges that aggregate as well as its local policy slice. Stable
model and request limits use the same envelope, and immediately before an
inference allocation the controller checks the requested increment against the
envelope minus raw leaf-cgroup usage and safety headroom. This live check covers
unmanaged allocations and page cache that cannot appear in the reservation
ledger.

Linux automatic resolution reads the process's actual cgroup path, walks every
ancestor to the visible controller mount, and falls back to streamed mountinfo
discovery for namespace and subtree mounts. Storage does not perform a second,
root-only probe after composition. This prevents storage from sizing against
host RAM while inference independently discovers a nested systemd or container
limit.

Full standalone keeps node-owned inference slices as logical metrics without a
host-derived hard limit. Applying one host limit to combined host and VRAM bytes
would reject valid discrete-GPU models. Cross-subsystem host contention is
instead enforced by the aggregate host ledger, while ModelManager enforces
backend-local and combined device policy.

## Reservation lifecycle

The common lifecycle is:

1. inspect the artifact and estimate construction peak plus retained residency;
2. atomically acquire model residency, KV, and scratch domains as applicable;
3. allocate/import;
4. shrink the lease from construction peak to retained residency;
5. acquire growth before lazy materialization, cache promotion, or a larger run;
6. on temporary denial, reclaim eligible inference state and retry once per
   useful eviction;
7. release transient leases at request completion and resident leases at model
   destruction.

External leases mirror the same transitions. They are not sampled telemetry:
successful reserve/retain/release operations are part of allocation
correctness. The standalone ABI carries an opaque owner-issued token through
the inference `AdmissionLease`; byte totals are never accepted as release
authority. Monotonic tokens are validated against an active owner registry, so
a delayed duplicate cannot target a newer reservation that reused the same
pool slot. Token records and registry capacity are reused, making steady-state
admission allocation-free after reaching its concurrency high-water mark.
The core ResourceManager applies the same rule to single and batch reservation
handles: the manager-issued identity and authoritative record, not copyable
byte fields, authorize retain, grow, shrink, and release. Stable observer
addresses are registered with their slice and last accepted total, so a stale
same-slice value cannot debit another observer. External ABI boundaries add
monotonic owner identities to avoid pointer-reuse ambiguity.

Prompt and tokenizer caches both report `(observer identity, previous total,
next total)`. Each tokenizer serializes only cold allocation, eviction, and
teardown transitions; cache hits remain callback-free. Standalone maintains a
separate registry entry per tokenizer, so a duplicate teardown can never
consume bytes retained by a different tokenizer.

Tokenizer callbacks are admission operations, not telemetry: growth must fit
the logical tokenizer slice and the process host aggregate before allocation,
while a validated decrease is always allowed so an over-limit owner can
converge to zero. Embedded inference applies that transition to the node
`ResourceManager`. Direct and distributed inference install a ModelManager-owned
adapter that keeps one exact total per tokenizer and charges deltas to the same
local `AdmissionController` used by model and request leases. The adapter never
evicts while called from a tokenizer lock; denial simply skips optional cache
growth, keeping the hot cache-hit path callback- and allocation-free.

Observer records are manager-owned accounting snapshots, not leases. An owner
that continues using the manager reconciles its snapshot to zero when its
allocation disappears. Destroying the manager is the terminal cancellation
boundary for any remaining snapshots because the observed allocations and
their ledger are being torn down together. Reservation handles remain strict:
they must be released before manager destruction because they are transferable
ownership tokens whose lifetime is independent of the observed allocation.

## Failure semantics

- A request larger than a stable hard limit is a permanent resource-limit
  failure. Changing concurrency cannot make it fit.
- Current contention or live pressure is retryable. Serving layers should
  return an unavailable/retry response while leaving the process alive.
- Missing external ownership is a startup/configuration error, not a reason to
  fall back to an independent inference budget.
- Estimation overflow fails admission. It never becomes an unlimited budget.

The desired failure mode is a rejected or deferred operation, never kubelet
eviction or a host OOM.

## Observability and tests

Resource metrics must expose used, peak, soft-limit, hard-limit, pressure, and
rejection and release-accounting-error counts for the aggregate host ledger,
plus pressure and byte metrics for every logical slice.
Inference metrics additionally retain backend class and the pressure domain
that selected an eviction victim. Data and full-standalone startup logs report
the operator source (CLI, canonical or compatibility environment variable, or
automatic), the effective source (explicit, cgroup v2, cgroup v1, host, or
unavailable), configured and effective bytes, and the derived managed hard
limit. Keeping operator intent separate from the effective source makes
container clamping visible without waiting for an admission failure. Direct
inference also reports operator source and configured bytes alongside its
resource-ownership policy. Linux live admission obtains host, cgroup limit,
and raw leaf usage from one coherent probe so the hot path does not repeat
filesystem work or combine different sampling instants.
Backend runtime metrics separately report active/peak lane leases, acquisitions,
and shutdown rejections; they must not be combined with byte-budget metrics.

Permanent tests cover:

- cgroup hierarchy and explicit-envelope derivation;
- raw leaf usage reducing explicit-envelope availability;
- fail-closed external ownership;
- atomic external reserve/release and denial classification;
- aggregate host admission across otherwise-independent slices;
- host-only external charging with logical host-plus-backend inference metrics;
- oversized minimum-progress operations remaining inside the host envelope;
- exactly-once batch release and invalid retain preserving unrelated capacity;
- single-release and stale-observer mismatch retaining all accounted capacity;
- preload and request paths using the same admission controller;
- inference, API, and control executor isolation plus lane shutdown/drain
  behavior.

CI should give memory-heavy suites an explicit envelope when the runner cannot
provide a finite cgroup limit. This is workload policy, not a runner resize.

## Non-goals and follow-ups

- `ModelManager` should not depend directly on storage internals.
- `ResourceManager` should not learn model formats or eviction ordering.
- `BackendRuntime` should not estimate model memory or become a second byte
  ledger.
- Resource reservations should not own tasks or replace executor lane leases.
- Per-process admission does not replace cluster placement or autoscaling.
- A future ABI version may expose node-owned device capacity domains. Until the
  node owner has reliable per-device capacity information, inference's
  backend-aware limits remain authoritative for accelerator memory.
