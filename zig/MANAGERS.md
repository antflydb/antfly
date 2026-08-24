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

Operational probes are deliberately outside expensive manager work. Dedicated
inference computes its first usable-model inventory before publishing the HTTP
listener, then a `Node`-owned `std.Io.Group` refreshes a last-known-good snapshot
on the attached runtime. `/readyz` only copies that snapshot; it never traverses
a shared model cache, parses manifests, loads a model, or waits for a refresh.
External pull commands can therefore converge into readiness without coupling
their process to the server, while transient cache failures fail closed before
the first successful scan and do not erase a later known-good snapshot. Node
teardown cancels and joins the refresher before releasing the borrowed executor
or model-directory state. Full standalone retains its aggregate node readiness
handler, so it does not create a redundant inference inventory task.

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
   macOS envelopes use the process physical footprint, including compressed
   and unified-memory residency, from the low-overhead process pressure probe.
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
completed weight operations and last-borrower release boundaries also re-probe
the authoritative live signal at a bounded cadence. Once the process reserve is
reached, the session enters pressure mode: each completed GGUF weight operation
drops its clean source range immediately, even if a graph scope retains the
handle, and release repeats the hint after the final borrower. `MADV_DONTNEED`
does not invalidate the mapping, so concurrent readers remain correct and may
refault a discarded page; pressure mode deliberately trades that I/O for process
survival. Prepared cache entries remain resident, and normal sessions that never
encounter live pressure pay only the rate-limited cgroup probe, with no extra
model I/O.
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
allocation the controller checks the requested increment against the current
leaf-cgroup working set on Linux or process physical footprint on macOS, plus
safety headroom. When the remaining explicit envelope is the tighter live
constraint,
admission keeps a fixed 512 MiB emergency reserve inside that bounded view
instead of reserving half of the remaining capacity a second time. Automatic
host/cgroup sizing retains the dynamic pressure reserve. Current node or finite
cgroup pressure also remains authoritative when it is tighter, so an explicit
value cannot weaken a physical pressure signal. The working set is
`memory.current - inactive_file`: it includes anonymous memory, the test
harness, sibling processes, and active mapped pages while excluding file pages
that the kernel can reclaim. The kubelet uses the same working-set signal to
rank memory-pressure evictions.

An explicit envelope also makes its live-usage probe mandatory. If the
leaf-cgroup working set or macOS process footprint is temporarily unavailable,
new allocation-producing work receives retryable resource pressure until a
later probe succeeds. The runtime never substitutes the configured total as
fresh free capacity; doing so would fail open by admitting the envelope a
second time. Automatic sizing may continue without a live probe because its
stable inference limits remain the only contract in that mode.

When an mmap-backed model is evicted, teardown issues `MADV_DONTNEED` before
unmapping and `POSIX_FADV_DONTNEED` after unmapping the whole weight file. These
best-effort Linux hints release only clean, unshared file-cache residency;
anonymous, dirty, writeback, and still-shared pages remain charged. This keeps
sequential model churn from pinning recently active weight pages inside the
process envelope without weakening admission accounting for live memory.

Physical teardown also has an allocator boundary. A long-lived glibc process
may retain freed pages in malloc arenas after a model, ephemeral audio sidecar,
or cold native cache has been destroyed. That retained high-water mark is still
anonymous cgroup working set even though ModelManager has released the matching
lease. After those coarse teardown events, the platform allocator performs a
best-effort `malloc_trim(0)` on GNU/Linux; unsupported allocators and platforms
make it a no-op. This purge never runs on ordinary request completion or
individual frees, preserving allocator reuse on hot paths. Admission is still
authoritative and re-probes the cgroup after reclamation rather than assuming
that a successful hint released a particular number of bytes. Denial logs
include process RSS, anonymous RSS, and private-dirty bytes so operators can
distinguish allocator residency from active file cache or sibling processes.

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

## Storage cache governance

### Diagnosis and ownership boundary

The strongest current explanation for the VectorDBBench scale discontinuity is
a cache-capacity and admission-churn problem before it is an index-sharding
problem. A retained 768-dimensional exact vector requires
3,072 payload bytes plus entry, hash, and CLOCK bookkeeping. The former
provisioned policy limited the entire shared HBC cache to
`min(process limit / 12, 2 GiB)` and began shrinking it at 75% of that value.
That geometry can retain only a mid-six-figure exact-vector working set after
routing nodes, quantized sets, and metadata take their share. It provides a
concrete mechanism by which a 50k corpus can be wholly warm while a 1M corpus
crosses a sharp latency and QPS elbow; the matched benchmark rollout below is
still required to establish how much of the observed result it explains.
Sharding may improve CPU parallelism and working-set
locality, but identical per-shard cache policy merely moves or multiplies this
boundary.

Primary LSM values remain the authoritative embedding representation. HBC's
exact-vector cache is a derivative read optimization, not a second database:

1. a write follows the existing WAL/LSM transaction and dense-index update;
2. mutation invalidates the affected derivative HBC entry;
3. a read miss loads the authoritative LSM value, validates and decodes it,
   computes the exact distance, and may admit a retained copy;
4. eviction drops only that retained copy and never changes durable state.

Production `IndexManager` instances use an adaptive retained decoded-vector
policy by default. `IndexBackendOptions.retained_vector_cache_enabled = null`
enables the path only when ResourceManager derives nonzero capacity; `false`
is a hard operator opt-out and `true` permits governed retention. In adaptive
and enabled modes ResourceManager still owns the byte target, pressure
sampling, admission, and eviction. A 1M/768-dimensional production-shaped
run with retention forced off fell from the governed-cache baseline of about
236 QPS to about 44 QPS, so unconditional disablement is not a viable default.

There is consequently no write-side check-and-set against an independent
vector store and no dual-write recovery protocol. A future mmap-friendly
vector segment or LSM value separation must remain a versioned, rebuildable
derivative with generation validation; it must not become a second authority.

### Node envelope and hierarchy

One `ResourceManager` is authoritative for each process/container memory
domain in both standalone and distributed data roles. It derives its envelope
from the operator limit and finite cgroup limit as described above. A cache,
DB, index, or shard must never infer that it owns the full machine limit.

The intended hierarchy is:

1. retain safety headroom outside the managed hard limit;
2. charge non-reclaimable residency and transient reservations to their
   existing logical slices and to the aggregate host ledger;
3. let reclaimable caches consume unused aggregate capacity within
   manager-owned elastic ceilings;
4. rotate reclamation across registered cache owners so one standalone index
   is not always drained first, and apportion pressure by explicit owner
   weights while the node ledger remains the final admission authority.

Future child leases are quotas, not independent physical ledgers. They may
borrow unused capacity and must return it when a higher-value class or
transient operation applies pressure. The current implementation provides one
shared provisioned cache across namespaces, weighted namespace shares, and
weighted rotating reclamation across standalone cache owners. These weights
choose the first source of bytes under pressure; they are not rigid partitions
and do not prevent borrowing unused capacity.
Distributed placement can advertise node capacity, but each target process
rechecks local admission. Standalone derives the same aggregate, HBC, and LSM
envelopes from the detected process/cgroup limit without a network control
plane.

The first storage-cache implementation retains the existing
`hbc_node_metadata_cache` physical slice for metric and accounting
compatibility. Provisioned HBC now receives an elastic hard ceiling of one
third of the effective process limit, bounded from 128 MiB through 16 GiB, and
a normal-pressure target of seven eighths of that ceiling. This is a maximum,
not a reservation: unused HBC capacity costs nothing, and the aggregate
managed-host budget still protects LSM, write working sets, full-text,
inference, and the process safety reserve. This removes that configured 2 GiB
ceiling; it does not by itself prove that the observed QPS elbow is gone or
promise that an undersized container can hold every exact vector.

The provisioned LSM block/table cache follows the same rule with a one-quarter
hard ceiling bounded from 64 MiB through 8 GiB and a seven-eighths target. Its
ceiling may overlap HBC's because neither is reserved: their observed physical
bytes compete in the aggregate ledger, and a foreground denial synchronously
reclaims HBC first and then LSM. This lets the authoritative vector miss path
scale with the node without recreating a fixed 512 MiB cliff.

### Cache classes and reclaim order

The HBC physical charge is subdivided by policy rather than by rigid ledgers:

| Class | Value | Current base protected target | Reclaim behavior |
| --- | --- | ---: | --- |
| routing nodes | required across many searches | 1/8 of HBC target | protected while a lower-value class can yield |
| quantized routing payloads | avoids LSM reads and routing decode/compute | 1/4 | protected while a lower-value class can yield |
| exact vectors | avoids LSM fetch/decode during final rerank | 0; elastic remainder plus bounded adaptive share | first reclaim source when above its adaptive target |
| result metadata | useful but cheap to reload | 1/32 | reclaimed after vectors and before routing state |

Protected targets are not reservations or hard partitions. Exact vectors can
borrow the whole pool when routing state is small. As routing state grows, it
reclaims borrowed bytes from exact vectors. If every class is within its
target, aggregate pressure can still evict metadata, quantized payloads, and
nodes in that order; process survival always outranks cache protection.

The shared LSM block/table cache and both shared and standalone HBC caches now
register lifetime-safe shrink callbacks with ResourceManager. A denied
foreground reservation computes its aggregate deficit, invokes cache owners
outside the accounting mutex, and retries admission. HBC is asked before LSM,
and owners within a cache class rotate across requests. ResourceManager samples
query profiles and maintains a slow EWMA of observed benefit per byte—hits
multiplied by measured miss-service latency, divided by resident bytes—inside
priority bands. Only one in 64 observations takes the manager lock. Static
node, quantized, and metadata minima remain in force; at most one quarter of
the target is adaptively distributed, with per-class caps, so noisy feedback
cannot starve routing state or swing the whole cache. No cache may maintain a
private memory-limit heuristic once it is attached to a node manager.

### Admission, concurrency, and CLOCK

Cache admission is optional work and must not become the QPS bottleneck. The
former blanket `active_searches > 1` guard prevented nodes, quantized payloads,
and exact vectors from warming under benchmark or production concurrency. It
also made warmup behavior depend on whether requests happened to serialize.

Shared exact-vector, node, and quantized entries use retained leases and may be
populated during concurrent searches. Published node and quantized entries are
immutable: mutation clones the retained value, modifies the clone, persists it,
and atomically replaces the cached generation. No cache API exposes an unpinned
node or quantized pointer across eviction.
ResourceManager returns a concurrent exact-vector admission stride:

- normal pressure below the cache target admits every shared-cache miss;
- a cache at its steady target samples one in eight decoded-owner requests even
  if synchronous eviction has already returned the pressure reading to normal;
- soft HBC or aggregate pressure samples one in eight optional decoded-owner
  requests;
- hard pressure stops optional concurrent vector admission until reclamation.

This rate limits lock and allocation churn under pressure or saturation; it
does not permanently select a hash subset of the corpus. Repeated misses can
therefore warm over time without turning a full cache into an insert/evict lock
convoy. Routing nodes and quantized payloads use retained handles and may warm
during concurrent search.

External-vector queries choose one residency owner before opening their
primary-store transaction. A decoded-owner lease uses transient LSM block
admission, then precharges physical cache capacity immediately before each
actual miss batch. Publication atomically transfers that charge to retained
entries, so unrelated admissions cannot steal it and no second charge or hard
limit overcommit is possible. At saturation, one sampled request receives a
bounded replacement window rather than freezing the resident set or rotating
the entire cache. If the next complete batch cannot be precharged because the
window is exhausted, entries are pinned, or pressure changed, the transaction
is recycled before the read and the remainder of the request uses retained LSM
ownership with decoded publication suppressed.
If a stale or missing quantized payload expands exact work beyond the admitted
bound, the session releases the remaining lease, closes the transient
transaction, and switches to retained LSM admission before the additional
read. Direct-primary-document compatibility fallback makes the same switch.
Thus policy changes and cache saturation cannot leave a request with neither
reusable representation, and concurrent queries cannot overbook decoded
headroom.

Shared CLOCK reference bits are atomic. Node, quantized, vector, and metadata
borrow/hit paths refresh recency while holding the shared map lock, without an
exclusive lock upgrade. Exact-vector publication uses 256 striped single-flight
locks: duplicate fills converge on one insertion, while vector, node,
quantized, and metadata cloning occurs outside the global map/admission
critical section. Every vector write path, including
external/`skip_vector_store` writes, unconditionally invalidates the key before
commit. Exact-vector miss fills use a fixed 4,096-stripe generation fence: the
reader captures an even stripe epoch before the authoritative LSM read and
rechecks it while holding the cache admission lock. A fill that overlaps a
commit or abort is returned to its caller but is not retained. Stripe
collisions only suppress optional fills during an active mutation; they never
hide or invalidate an unrelated resident entry. `skip_vector_store` values are
seeded only after successful publication, when their authoritative external
LSM transaction is already visible.

Cache insertion never returns an unpinned view into cache-owned memory. The
caller continues using its request-owned scratch/value, while later hits use a
retained lease and keep detached storage physically accounted until release.
This separates admission from lifetime: eviction can run immediately after a
miss without invalidating the current request's result.

LSM block/table retention follows the same pre-admission rule. It reserves the
entry's exact byte cost in the aggregate ledger before publishing the entry in
the shared map. If neither HBC nor an unpinned LSM victim can make room, the
load still returns a reference-counted transient handle; the value is usable
for the current request but is never linked into the cache or charged as
retained residency. This is intentional graceful degradation under pressure,
not an allocation failure or a post-allocation accounting observation.

Attaching a `ResourceManager` is idempotent. Reattaching the same manager keeps
observer ledgers and reclaimer identities intact. Moving an index or cache to a
different manager first debits every old observer contribution, then credits
the same live search, routing, apply, local-cache, and detached-pinned bytes to
the new owner. A manager change must never reset a local counter while leaving
its old physical charge behind.

### Pressure response

Owners react in increasing order of disruption:

1. stop or sample low-value cache admission;
2. evict exact vectors consuming elastic/borrowed capacity;
3. evict excess metadata, quantized payloads, then routing nodes;
4. reduce allocation-producing query concurrency;
5. defer compaction, repair, and other background work where its slice policy
   permits;
6. throttle writes before WAL/apply when write-side hard admission requires
   it;
7. reject retryably rather than exceed the process envelope.

Cache owners perform pin-safe victim selection. Detaching removes resident
cache bytes, but any reader-retained allocation moves to pinned accounting and
is not released from the aggregate ledger until the final lease drops.
ResourceManager owns targets, admission, and
pressure decisions; it must not reach into cache data structures or choose a
format-specific victim. A cache hit never creates a new memory charge.

### Telemetry and rollout

The HBC status surface reports per-class used/peak bytes, hits, misses,
insertions, replacements, sampled admissions, admission skips, and evictions,
plus total accounted and detached pinned bytes. Shared retained vector and
metadata borrows are included in hit/miss counters. ResourceManager reports
reclaim requests and reclaimed bytes in its snapshot and benchmark resource
logs; LSM cache stats and benchmark logs count pressure-denied transient serves
separately from retained inserts. Query profiles report cache and rerank LSM
hits/misses and artifact read/decode time. Operators should graph those values
with aggregate and HBC soft/hard limits, process working set, active searches,
and p50/p95/p99 query latency. A capacity cliff is confirmed when exact-vector
bytes flatten at the HBC target while rerank LSM misses and eviction churn rise.

Filtered prefix execution batches metadata for all candidate members of a leaf
through the sorted LSM path before vector scoring. Profiles expose candidates,
rejections, batch count, and batch-metadata time. This removes the former scalar metadata
point lookup and cache-lock cycle per quantized candidate; selective 1% and
10% benchmark lanes in the production-boundary matrix quantify the end-to-end
gain independently of unfiltered dense search.

Selective exact scoring follows the same storage geometry. Production indexes
whose full vectors live in embedding artifacts must never probe HBC's vector
namespace first: that namespace is not the authoritative full-vector store.
The scorer normalizes candidates, resolves HBC metadata in sorted batches, and
then scores bounded batches of at most 1024 external artifacts using one sorted
primary-store multi-get. Missing or recoverably corrupt artifacts reject only
their candidate. Direct-field indexes backfilled before vector-artifact
materialization use a second bounded, sorted primary-document batch only for
artifact misses; current writes and explicitly external embeddings never take
that compatibility path. A single reusable batch workspace is charged to
`dense_search_working_set` for its lifetime, while retained transaction pages
are observed once after each multi-get. Because exact scoring is single-pass,
its request-local decoded-vector cache is disabled; a governed shared-vector
cache may still own the request through a decoded-residency lease. Capacity is
reserved once for each bounded miss batch; a saturated request receives at
most its ResourceManager-derived replacement window before a safe pre-read
handoff to retained LSM ownership. Otherwise the primary-store transaction
uses normal retained block admission and does not populate decoded vectors.
Metadata cache reads never escape as unretained slices: result attachment uses
retained leases, while batched filtering keeps transaction-owned views for the
transaction lifetime. Exact-candidate preparation additionally bypasses both
metadata-cache lookup and population, avoiding a clone/lock/eviction cycle for
single-pass entries that the route never reuses.

The exact/HBC route is a cost decision, not a corpus-percentage threshold. The
planner compares storage-equivalent work for candidate full-vector reads with
HBC's resident quantized inspection plus external rerank reads. Resident
quantized bytes are bandwidth-normalized rather than charged as artifact I/O
only when exact scoring would cross into the external artifact store; built-in
indexes compare their resident exact and quantized payloads without that
conversion. The initial conservative external conversion is 11 resident bytes
per external-storage work byte, calibrated from the production-shaped 50k and
1M phase profiles and bounded by the candidate-linear built-in exact guardrail.
This preserves the bounded exact route for 50k/1% while preventing a cold
10,000-artifact exact scan at 1M/1%. Its HBC estimate also accounts for the
eligible-hit rate, requested result count, resolved search width, and leaf
size. The existing dimension-aware exact component ceiling remains a safety
bound, but there is no `active_count / 100` discontinuity. The telemetry field
names retain `estimated_*_storage_bytes` for API compatibility, but their
values are storage-equivalent work bytes, not predicted physical reads.
Composed public query profiles expose both estimates and the exact phases:
candidate prepare, metadata lookup, artifact key/read/decode, distance time,
batch geometry, workspace bytes, scalar versus batch reads, missing vectors,
request-cache entries, and LSM cache hits/misses.

The planner emits nanosecond work estimates from the first query. Its
dimension-aware cold priors cover filter membership, quantized scoring,
external artifact-miss service, and exact distance; the 11:1 byte model remains
the compatibility estimate, not the production decision whenever an index
snapshot is available. Each HBC index maintains a slow EWMA for those
components and rerank cache-hit rate. A component is replaced independently as
either route measures it, so an exact-only or HBC-only workload does not leave
the alternate route uncosted. Artifact read/decode is learned per cache miss
and then weighted by the observed hit rate. The planner incorporates the
ResourceManager HBC slice's current pressure and residency and retains the
prior route inside a 20% hysteresis band; exact wins an equal-cost tie. The
dimension-aware component budget remains authoritative. Profiles expose both
storage-equivalent estimates and `estimated_*_work_ns` values. External-vector
distance time appears in both the generic rerank total and its artifact
breakdown for profile compatibility; adaptive route learning selects the
artifact-specific value when present and never adds the same interval twice.

Filtered HBC treats resolved search width as a ceiling. The existing candidate
frontier is consumed in adaptive waves whose next size is based on observed
eligible vectors per explored leaf; traversal never restarts. For L2-squared,
posting nodes persist a Euclidean covering radius in the backward-compatible
`HBN2` packed-node value. `HBN1`, NaN/stale radii, cosine, and inner-product
fall back to the configured width. Mutable internal ancestors are expanded,
not trusted as proof objects, because a foreground posting append does not
rewrite the entire ancestor chain. Once the frontier consists of bounded leaf
postings, search may stop only when the next `distance(query, centroid) -
covering_radius` lower bound exceeds the retained result upper bound. Profiles
report wave geometry, eligible yield, bound resolutions/fallbacks, frontier at
stop, and both sides of the stopping inequality.

The flat RaBitQ centroid directory follows the same rule. It retains the full
compact posting frontier (IDs, quantized centroids, and radii), starts with the
configured probe count, and advances in yield-sized waves. Unknown legacy
radii sort ahead of bounded postings and force visitation. A fixed directory
probe count is therefore an initial work target, never an unsafe recall
cutoff.

External reranking consumes the quantized-error ambiguous set in sorted
batches of at most 128 vectors. After every batch it compares the kth-smallest
upper bound among scored/permanently retained candidates with the minimum
lower bound of the unscored set. Only a strict separation skips the remaining
artifact reads. Batch count, maximum batch size, and candidates skipped by the
proof are public profile fields.

Permanent coverage includes generation-crossing stale-fill rejection,
request-owned miss results, query-bounded decoded-residency leases, pre-read
handoff to retained LSM ownership when a lease cannot cover degraded work,
idempotent manager reattachment and ledger transfer, dynamic policy derivation,
envelope-based provisioned sizing, cross-namespace aggregate eviction, class-aware reclaim,
shared-cache warming during concurrent search, CLOCK hit refresh, exact byte
release, and local-cache concurrency safety. Benchmark rollout should compare
50k and 1M at matched recall, cold and warm phases, several concurrency levels,
and at least two explicit memory envelopes. Report cache bytes and miss ratios
with QPS; QPS alone cannot distinguish cache capacity from search quality or
CPU saturation.

When a request owns a decoded-residency lease, HBC marks its exact-vector LSM
reads as transient only after the complete upcoming miss batch has physical
cache capacity precharged. The LSM still retains indexes and bloom filters, and
it may reuse a block already resident for another reader, but a vector miss
does not publish a second serialized copy into the LSM block cache. If
decoded-vector caching is disabled, bypassed, pressured, cannot reclaim pinned
capacity, exhausts its bounded saturation window, or encounters a
degraded-path expansion, HBC switches before the next read to normal retained
LSM block admission and suppresses decoded writes for those reads.
`policy_bypasses` distinguishes this deliberate ownership choice from
pressure-denied `transient_serves`.

Reclaim dispatch apportions work among registered cache owners by weight and
rotates its starting owner. The shared HBC cache similarly computes weighted
namespace shares from the node target: a namespace may borrow unused capacity,
but when a peer needs room, an over-share namespace is the first victim source.
Protected cache classes and the aggregate node hard limit still take
precedence over fairness.

Further tuning requires benchmark evidence rather than another policy guess.
Derived mmap vector segments should be evaluated only if governed LSM/HBC
caching and progressive reranking still leave artifact I/O dominant.

The reproducible production-boundary driver is
`scripts/run_resource_manager_cache_matrix.sh`. It builds the production
standalone server and harness in `ReleaseFast` by default, assigns an explicit
process memory envelope to every child,
and records commands, environment, per-case logs, status, and JSONL summaries
under `zig/bench/results/resource-manager-cache-matrix/`. Its default matrix
covers dense and 1%/10% filtered-dense searches at 50k, 600k, 700k, 800k, and 1M,
full-text and graph endpoints at 50k and 1M, 2 GiB and 8 GiB envelopes, cold
first-pass versus later-pass latency, 1/16-thread endpoints through the
concurrency sweep, and a longer 1M/1% posting-maintenance/query soak. The
driver fails production runs below 150 QPS at 50k/1%, below 80% of matched
unfiltered QPS at 1M/1%, when 1% selectivity regresses below 70% of the matched
10% lane, when the maximum thread lane falls below 70% of one-thread QPS, when
the maximum load or search RSS across successful recorded cases exceeds 125%
of the explicit process envelope, when their maximum HBC-accounted bytes
exceed that envelope, when the maintenance soak falls below 70% of its
base lane, or without separately populated cold/warm phases. The release-blocker
recall suite runs before an evidence-producing matrix.
Run the reduced
endpoint/integration check before the evidence-producing matrix:

```sh
RESOURCE_CACHE_MATRIX_SMOKE=1 scripts/run_resource_manager_cache_matrix.sh
scripts/run_resource_manager_cache_matrix.sh
```

The driver records failures and continues the matrix. Resume an interrupted
rollout without repeating recorded cases by setting
`RESOURCE_CACHE_MATRIX_RESUME=1` with the same output directory.

The smoke matrix is a correctness check and does not close the performance
follow-up. A rollout is complete only when the retained JSONL and resource
logs demonstrate the elbow behavior at matched recall on representative
hardware; production weights and adaptive bounds must not be tuned from the
small smoke corpus.

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
- macOS process footprint reducing explicit-envelope availability;
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
Build and execution must use separate runner lifetimes for an envelope
calibration: compiler anonymous memory and active build-cache pages are sibling
working set by design, so running a large-model test after compilation in the
same leaf cgroup validates residual build pressure rather than the serving
envelope. The tested binary is transferred as an immutable short-lived artifact;
the execution job logs its cgroup baseline before starting inference.

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
