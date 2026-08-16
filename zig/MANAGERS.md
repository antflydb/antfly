# Resource and model manager design

Antfly has two cooperating managers because they answer different questions:

- `ResourceManager` owns physical, process-wide capacity shared by storage,
  indexing, caches, and embedded inference.
- `ModelManager` owns inference objects and decisions: model inspection,
  memory estimates, residency, request working sets, eviction, and backend
  selection.
- `RunBudget` is request-local accounting. It prevents one execution from
  exceeding the limits granted by `ModelManager`; it is not a node scheduler.
- `AdmissionController` is the adapter between model-aware estimates and the
  physical owner. It atomically mirrors inference leases into the external
  `ResourceManager` when one is present and also performs live pressure checks.

Connecting the managers does not merge them. `ResourceManager` cannot decide
which model can be evicted, and `ModelManager` must not independently assume it
owns all memory in a process that also serves data.

## Ownership by deployment mode

| Deployment | Physical owner | Model owner | Required connection |
| --- | --- | --- | --- |
| `antfly inference run` | local inference admission controller | process-local `ModelManager` | local ownership is declared at `Node` creation |
| full standalone Antfly | the data node's `ResourceManager` | embedded `ModelManager` | external owner is required before preload or serve |
| distributed data process | that process's `ResourceManager` | none unless inference is embedded | no cross-process memory ledger |
| distributed inference process | local inference admission controller | process-local `ModelManager` | cluster scheduling routes work; process admission protects memory |
| offline conversion/inspection | command-scoped budgets | command-scoped loaders | no serving residency contract |

Memory coordination is deliberately process-local. A network-wide reservation
protocol would be stale at allocation time and would couple failure domains.
Distributed placement and request routing use advertised capacity; every target
process still performs authoritative local admission.

The Apache-licensed inference package also cannot import Antfly's internal
storage `ResourceManager`. The standalone ABI bridge preserves that boundary:
inference exposes generic reserve/release callbacks, while the full product
maps them to node-owned resource slices.

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
   working set as separate logical slices. The inference controller retains the
   host/backend split needed for unified-memory and discrete-accelerator policy.
7. A process envelope is charged against raw container usage, not only memory
   allocated through inference. Page cache, a test harness, storage, and sibling
   work therefore reduce the capacity available to a new inference allocation.

## Budget derivation

Budget sources have this precedence, with an explicit value always clamped by
any smaller finite cgroup limit:

1. an explicit process/container envelope supplied by the operator;
2. a finite cgroup limit (`memory.max` or the v1 equivalent);
3. host memory as a development fallback.

An explicit envelope is necessary for Burstable Kubernetes pods whose request
is lower than node memory but whose cgroup hard limit is `max`. Kubernetes does
not expose the request as an allocation boundary inside that cgroup. Inference
accepts `--process-memory-budget-mb` or
`ANTFLY_INFERENCE_PROCESS_MEMORY_BUDGET_MB`; full standalone accepts the
corresponding `--inference-process-memory-budget-mb` flag and environment
variable. Set the envelope below the orchestrator allocation so the kubelet,
runtime, and test harness retain headroom.

The process envelope is not another inference slice. It bounds the whole live
memory view. Stable model limits are derived from it, and immediately before an
allocation the controller checks the requested increment against the envelope
minus raw leaf-cgroup usage and safety headroom.

Full standalone additionally derives finite node-owned inference slices from
detected node memory. These slices provide cross-subsystem policy and metrics;
the live envelope guards allocations that are invisible to slice ledgers.

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
successful reserve/release pairs are part of allocation correctness.

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

Resource metrics must expose used, peak, soft-limit, hard-limit, and rejection
counts for every logical slice. Inference metrics additionally retain backend
class and the pressure domain that selected an eviction victim. Startup logs
should report the detected or explicit process envelope and ownership mode.

Permanent tests cover:

- cgroup hierarchy and explicit-envelope derivation;
- raw leaf usage reducing explicit-envelope availability;
- fail-closed external ownership;
- atomic external reserve/release and denial classification;
- finite standalone inference slices;
- preload and request paths using the same admission controller.

CI should give memory-heavy suites an explicit envelope when the runner cannot
provide a finite cgroup limit. This is workload policy, not a runner resize.

## Non-goals and follow-ups

- `ModelManager` should not depend directly on storage internals.
- `ResourceManager` should not learn model formats or eviction ordering.
- Per-process admission does not replace cluster placement or autoscaling.
- Logical host-plus-backend slices are conservative on discrete GPUs. A future
  ABI version may expose separate physical host and device domains when the
  node owner has device-specific capacity information; until then inference's
  backend-aware limits remain authoritative and the node slices are an
  additional cross-subsystem ceiling.
