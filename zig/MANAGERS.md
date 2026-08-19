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
5. `ModelManager` may satisfy temporary contention by evicting an idle model or,
   for host pressure, asking an active backend session to shed one cold unpinned
   cache entry before retrying. It re-probes authoritative admission after every
   useful release and must not retry a request that is intrinsically larger than
   the configured hard limit.
6. The external manager sees model residency, KV working set, and scratch
   working set as separate logical slices. Those metrics may contain host plus
   accelerator bytes, while only the physical host component is charged to the
   `ResourceManager` host aggregate on discrete-GPU systems. Unified-memory
   systems charge both components. The inference controller retains the split
   and remains authoritative for device capacity.
7. A process envelope is charged against container working-set usage, not only
   memory allocated through inference. Active page cache, a test harness,
   storage, and sibling work therefore reduce the capacity available to a new
   inference allocation.
   Linux envelopes use leaf-cgroup working set, matching kubelet pod-eviction
   accounting while excluding inactive file pages the kernel can reclaim.
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
11. A resource capability owns its callback context. Admission and tokenizer
    capabilities are retained when installed and released only after their last
    lease, observer record, and physical allocation are gone. Configuration
    fails closed if an external owner cannot provide this lifetime contract.
    Public policy values such as `NodeConfig` never expose an unowned callback.
12. Artifact accounting follows physical lifetime. Decoder weights retained by
    a model session belong to the model lease. A multimodal projector opened by
    one generation belongs to that request lease, and its clean mmap pages are
    discarded tensor-by-tensor after the compute backend has copied or consumed
    them. Standalone and distributed serving use this same lifecycle; deployment
    mode cannot turn request-scoped page cache into untracked model residency.

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

Mapped files are not free memory. Their resident clean pages contribute to raw
cgroup and kubelet usage even though the allocator does not own them. ModelManager
therefore separates stable decoder artifacts from request-scoped projector
artifacts. A multimodal admission holds the projector's encoded bytes as host
weight capacity for the whole request, while the projector reader uses random
access and releases each consumed tensor range with `MADV_DONTNEED` and
`POSIX_FADV_DONTNEED`. The hints are opportunistic and never affect correctness:
the read-only mapping remains valid and a later access faults the file data back
in. This bounds cold image/audio page-cache growth without serializing unrelated
models or adding redundant copies.

Lazy native weight caches also cannot size themselves independently from the
serving owner. Session construction may derive an optimistic cache size from
the visible node, but ModelManager installs the effective hard host/backend
limits before publishing the session. Architecture-specific cache floors may
improve throughput inside that envelope; they never widen past it. ModelManager
also binds the session cache to its aggregate AdmissionController. The model's
resident lease is the baseline credit for encoded weight bytes, so faulting a
mapped source page is not double-counted. Before a lazy weight, prepared quant
layout, or dense promotion allocates beyond that baseline, the cache acquires
an incremental lease from the same controller used by model and request
admission. In standalone that lease is mirrored into the node ResourceManager;
in direct and distributed inference it remains process-local. A temporary live
pressure denial is returned as retryable `MODEL_RESOURCE_BUSY`, while a stable
policy ceiling remains a non-retryable memory-budget response.

Each weight handle additionally reserves its physical representation in the
request RunBudget for exactly the handle lifetime. The shared cache evicts cold
unpinned entries, releases incremental leases only after their physical storage
is destroyed, and drains any remaining credits before the resident model lease
is released at session teardown. Offline tools without a serving owner retain
counter-only cache policy. This keeps the Hypura-style mapped-artifact and
bounded-hot-set behavior without allowing lazy promotion to bypass
ResourceManager policy or charging bookkeeping on cache hits.

Cache geometry and live pressure are separate constraints. A model can remain
inside its configured hot-set ceiling while active mmap pages from the model,
test harness, or sibling subsystem consume the process envelope. Native and
PJRT cache growth therefore treats a live-host denial as a reclaim signal: under
the lazy-entry residency lock it destroys one cold unpinned entry, releases its
exact aggregate credit, drops the corresponding clean GGUF file-cache range,
and retries the pending growth. Because page faults are not allocator calls,
last-borrower release boundaries also re-probe the authoritative live signal at
a bounded cadence. Once the process reserve is reached, the session enters
pressure mode: every subsequent GGUF tensor drops its clean source range when
its last borrower releases the handle. Prepared cache entries remain resident,
and normal sessions that never encounter live pressure pay only the rate-limited
cgroup probe, with no extra model I/O.
ModelManager uses the same bounded session capability when request admission is
denied and no idle model can be removed. Backends own the mechanics and
pin-safety of reclamation; ModelManager owns victim ordering and retries;
ResourceManager remains the authoritative capacity decision. This path is
identical in direct inference, distributed inference, and full standalone—the
only difference is whether the admission lease is local or mirrored through the
external bridge.

The process envelope is not another inference slice. One resolved value is
passed to storage and inference during standalone composition and by the
dedicated `antfly inference run` entry point. ResourceManager derives an
aggregate managed-host-memory budget from it; every storage slice reservation
charges that aggregate as well as its local policy slice. Stable model and
request limits use the same envelope, and immediately before an inference
allocation the controller checks the requested increment against current leaf
cgroup working set plus safety headroom. On Linux, when the remaining explicit
envelope is the tighter live constraint,
admission keeps a fixed 512 MiB emergency reserve inside that bounded view
instead of reserving half of the remaining capacity a second time. Automatic
host/cgroup sizing retains the dynamic pressure reserve. Current node or finite
cgroup pressure also remains authoritative when it is tighter, so an explicit
value cannot weaken a physical pressure signal. The working set is
`memory.current - inactive_file`: it includes anonymous memory, the test
harness, sibling processes, and active mapped pages while excluding file pages
that the kernel can reclaim. The kubelet uses the same working-set signal to
rank memory-pressure evictions.

When an mmap-backed model is evicted, teardown issues `MADV_DONTNEED` before
unmapping and `POSIX_FADV_DONTNEED` after unmapping the whole weight file. These
best-effort Linux hints release only clean, unshared file-cache residency;
anonymous, dirty, writeback, and still-shared pages remain charged. This keeps
sequential model churn from pinning recently active weight pages inside the
process envelope without weakening admission accounting for live memory.

Resolved bytes and provenance travel together through direct `NodeConfig` and
the standalone inference ABI. Only an effective `explicit` source selects the
fixed-reserve policy; cgroup, host, unavailable, and explicit-input-clamped-by-
cgroup sources retain their exact automatic provenance and the dynamic-pressure
policy. Inference never reconstructs operator intent from numeric equality with
a detected limit, because an automatically resolved leaf-cgroup limit normally
equals that same detected total.

Budget overrides are normalized once in the inference memory tier and then
used by direct CLI runs, server request budgets, and ModelManager load/run
admission. Host and backend are the physical components of `combined`; when an
operator overrides either component without specifying `combined`, the
aggregate is recomputed from the effective component limits. This makes a lone
`--host-budget-mb` authoritative for CPU inference instead of leaving a smaller
auto-derived combined limit in its path. An explicit combined override still
wins, allowing an operator to impose a deliberately tighter cross-domain cap.

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

Local tokenizer credits remain ordinary `AdmissionLease` values held privately
inside a ref-counted resource-domain observer record. The domain owns the
`AdmissionController`, tokenizer ledgers, and upstream capabilities independently
of `ModelManager`; each lease retains its backend attribution and admitted
amount. Shrinking uses `retain`, while full credit release uses the lease itself.
There is no public detach or raw-byte release API, so a callback cannot
reconstruct a release, select another backend ledger, or debit accounting owned
by another tokenizer.

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
adapter that keeps one exact usage total per tokenizer and charges bounded 1 MiB
admission credits to the same local `AdmissionController` used by model and
request leases. Unused credit is bounded to less than one quantum per tokenizer;
near a policy boundary, a denied preferred quantum is retried with only the
required bytes so usable capacity is not stranded. Shrink releases whole unused
credits and teardown releases the exact remainder.

Tokenizer identities are distributed over independent accounting shards. A
shard lock marks one identity transition in flight, then is released before an
OS/cgroup live-memory probe; other tokenizer owners continue independently.
Growth inside existing credit performs no probe at all, so filesystem sampling
is amortized across thousands of small cache entries rather than paid per miss.
The adapter never evicts while called from a tokenizer lock; denial simply skips
optional cache growth, keeping the hot cache-hit path callback- and
allocation-free.

Ownership provenance is explicit and immutable once attached. Local mode
installs only the ModelManager adapter. Embedded mode attaches the admission
lease bridge and tokenizer observer bridge as one external pair; supplying only
one half, mixing an external tokenizer callback into local mode, or changing
ownership after attachment fails before model loading. Pairing replaces only
the observer capability: tokenizer cache geometry remains the policy selected
by `NodeConfig` or `configureTokenizerCaches`, so attaching process ownership
cannot silently widen, disable, or otherwise reset cache sizing.

Every tokenizer that adopts a resource capability retains the ref-counted
resource domain directly; managed tokenizer handles additionally keep their
model-residency lease in that domain. `ModelManager` shutdown closes new cache
growth and drops only its own domain reference. It does not settle residency or
pretend observer totals reached zero while the corresponding memory is still
live. Existing tokenizers can continue read-only use, and physical teardown
performs the exact cache decrease and residency-lease release before dropping
the final capability reference. The last reference asserts empty ledgers,
detaches the upstream admission bridge, releases both external capability
contexts, and destroys the controller.

The standalone boundary mirrors the same rule. ABI version 15 added
`ResourceBudget` retain/release callbacks for its host context; version 16 adds
effective process-envelope provenance to `CreateContext`. The inference archive
copies the resource table into an independently ref-counted context, and its
local admission and tokenizer capabilities retain that context. The host
`InferenceResourceBudgetOwner` in turn retains the node `ResourceManager` bridge
until inference Node destruction has released every lease and observer. This
keeps the Apache inference package decoupled from storage internals without
depending on stack/defer ordering or a raw `LinkedInferenceState` pointer.

Observer records remain accounting snapshots rather than raw byte-release
authority. Local records own ordinary `AdmissionLease` credits; external
records retain exact observer totals solely so shutdown can perform the final
validated transition. Other reservation handles remain strict and must be
released before their owning manager is destroyed unless their public wrapper
explicitly carries an equivalent lifetime pin.

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
- tokenizer handles surviving manager shutdown without callback use-after-free
  or admission leakage;
- external ownership pairing preserving configured tokenizer cache geometry;
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
