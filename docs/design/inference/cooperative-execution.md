# Cooperative CUDA execution ownership

Cooperative execution interleaves independently owned generation requests on a
shared backend. Its control is independent of row batching. This example selects
one-row interleaving; the shipped read-pool configuration enables two-row
batching by default:

```json
{
  "inference": {
    "cooperative_execution": {
      "mode": "on",
      "max_active_requests": 2,
      "short_context_burst": 8,
      "decode_handoff_wait_us": 5000
    },
    "generation_batching": {
      "mode": "off",
      "max_step_items": 1,
      "max_idle_prefill_chunk_size": 512
    }
  }
}
```

The general runtime defaults remain off. `generation_batching.mode: on` controls row batching
and does not enable cooperative execution. A cooperative request that also
requests more rows than the backend declares receives
`UNQUALIFIED_COOPERATIVE_BATCH`. The original qualified read profile uses two
active requests and one row per GPU step. Decode batching can combine
two rows when the resident backend declares its quantized numerical contract;
see [multi-request CUDA generation batches](cuda-generation-batching.md).
The earlier mixed two-row path did not pass its latency gate. Performance
promotion of the decode-only path uses its own controlled comparison gates.
The read-pool deployment selects batching for ready requests without a batch-fill
wait; this deployment choice does not turn a failed performance gate into a pass.
The decoder's backend declaration currently fences sharing to eagerly uploaded
dense Gemma 4 E4B CUDA geometry. Other backends default to exclusive execution.
Graph, prompt-cache, speculative, selected-KV and multimodal requests retain
exclusive ownership. Backend declarations carry both the sharing limits and the
measured immutable-weight inventory; the generic scheduler does not infer either
from architecture dimensions.
The inventory uses actual eagerly uploaded CUDA weight bytes. A BERT-specific
BF16 projection-mirror count can remain zero for a complete E4B load and is not
an eligibility condition. Startup logs report the sharing declaration and weight
inventory. `antfly_inference_generation_cooperative_capable_models`,
`antfly_inference_generation_cooperative_configured` and
`antfly_inference_generation_row_batching_configured` expose the distinction
between backend support, interleaving configuration and row batching.

## Ownership invariants

1. Generation admission precedes KV and scratch reservations. Its FIFO queue
   admits at most two cooperative owners, and an exclusive queue head fences
   later cooperative arrivals until existing work retires. Waiting requests
   own no generation KV or scratch. Admission leases issue distinct request
   identities; a backend never identifies an owner by an allocator address.
   Active execution and waiting capacity are separate limits: node
   `max_concurrent_requests=0` permits unlimited isolated native execution,
   while cooperative CUDA retains its explicit active-owner cap. The zero
   setting permits up to 64 queued requests behind an exclusive owner; finite
   settings bound the queue to the smaller of the request limit and 64.
   Unlimited active execution still respects FIFO exclusive-owner fences.
2. An execution scope acquires the execution lane and model mutex, binds its
   request budget, and enters the backend owner. Setup, GPU steps and cleanup
   use this same object. Failed binding unwinds budget, mutex and lane together.
3. Consecutive acquisitions by the same owner reuse backend metadata. An owner
   change calls the conservative stream drain and request reset. Exclusive
   request setup and final cleanup invalidate the remembered owner. A plain
   tensor release preserves the same owner; releasing a peer’s tensor
   invalidates the remembered owner. This avoids a whole request reset for
   every device-token free. Atomic
   `antfly_inference_generation_owner_{transitions,reuses,cleanups,releases,resets}_total`
   metrics expose the distinction without taking the execution mutex.
4. Claimed work and its borrowed request storage drain before retirement, even
   after cancellation or deadline expiry. Cleanup acquisition is non-cancellable.
   LightOnOCR asynchronous token readback yields and reacquires the actual
   execution scope; it does not fabricate a lane lease.
5. Only prepared work is runnable. Direct/manual work declares readiness while
   awaiting its turn and clears that declaration on cancellation or acquisition.
   A request doing CPU preparation cannot indefinitely block prepared GPU work.
6. Shorter contexts receive a bounded burst, with round-robin service after the
   burst and the existing prefill/decode fairness fence. A cheaper decoder may
   receive up to 5 ms to sample and publish its next step; the maximum permitted
   setting is 10 ms. Completion arms this deadline once. CPU progress and other
   owners do not extend it. Expiry, cancellation, and the burst limit let ready
   peers proceed. A lone request does not wait for an absent peer.

Every owner in the cooperative envelope has its own KV, transient admission,
request budget and cancellation lifetime. Its one-row step cannot borrow a
peer's budget. Widening active counts, physical rows, or the backend declaration
requires new fairness, output-equivalence and resource-accounting evidence.

## Physical device budget and recovery

`Node.init` acquires the process device-budget owner before constructing the
model manager. Standalone inference, Antfly's inference command, embedded/hosted
nodes and the engine function table all use that initialization path. Multiple
nodes in one process use the tightest live ceiling. An unbounded node cannot
widen an existing ceiling; adding a ceiling below live allocations fails before
loading models. Removing a node can widen the ceiling, but never clears charges.
The guard is per worker process, not a cross-process GPU allocator.

Logical admission remains separate from the final `DeviceBuffer` guard. Physical
allocation records reserve bytes and durable ownership metadata before the
CUDA allocation. An allocation failure rolls both back. A failed device free
keeps its pointer, primary-context identity, owner identity and byte charge in
the process ledger after the model's descriptor disappears. Before a retiring
wrapper can release its primary context, it transfers its existing CUDA retain
to quarantine whenever allocations remain. Owner metadata is reserved before
allocating device memory, so this transfer cannot fail from host allocation
pressure. Final wrapper cleanup also transfers its driver-library reference.
`CudaContext.deinit` is the final ownership boundary for both resources. Loaded
models defer that same finalizer until all `CudaCompute` host cleanup completes;
the earlier `releaseResources` call does not authorize closing the driver
directly. The model-free lifecycle suite exercises the actual compute teardown
with both successful and failed frees, then verifies that reload releases the
quarantined allocation, primary reference and driver-library owner together.

A retry requires either the still-active retiring owner or a quarantined owner
with an uninterrupted primary-context retain. A raw context handle alone is
never sufficient. An external CUDA user's final release cannot reset that pinned
lifetime. Once every allocation for an owner is physically freed, the ledger
releases its CUDA retain and library reference. An uncertain retain-release
result keeps the library until worker exit; the runtime never retries that
release against a possible successor context lifetime. This follows NVIDIA's
[primary-context lifecycle contract](https://docs.nvidia.com/cuda/cuda-driver-api/cuda_driver_api/group__CUDA__PRIMARY__CTX.html).

Unsuccessful stream draining is never promoted to safe retry. Such allocations
remain pinned and charged until the worker restarts. The runtime does not reset
a primary context shared with other CUDA users. An explicit external device
reset while managed work exists is outside the supported ownership contract.
Successful frees release charges; a confirmed inactive primary can reconcile
only owners without quarantined retains. Reusing a context address after actual
teardown does not resurrect an old allocation token.

For the nominal 40 GiB pool, the managed cap is 38 GiB, leaving 2 GiB for driver
and vendor allocations outside `DeviceBuffer`. Admission weight, scratch, KV and
prefix-cache counters describe overlapping reservations and allocations. Do not
sum them with managed allocations or NVML memory. Retained weights and retained
backend scratch have different owners and lifetimes.

Qualification requires managed live/peak/limit metrics and successful samples
in every workload phase, including short phases. Requesting cgroup attribution
also requires valid NVML memory for the service's GPU process IDs and valid
cgroup memory readings. Sampling failures, absent metrics, empty samples,
missing phase coverage or unavailable NVML produce incomplete qualification.
Physical-limit violations fail qualification. The memory monitor cannot turn
missing evidence into zero usage or a passing result.

## Validation and acceptance criteria

The reproducible targets are:

```bash
# Production engine create_node path, multiple-node ownership, no GPU required
# for the first test. The second test skips unless explicitly requested.
cd zig
zig build inference-device-budget-host-test -Dcuda=true -Dmetal=false
zig build install-inference-device-budget-host-tests -Dcuda=true -Dmetal=false
cd ..
python3 zig/pkg/inference/scripts/qualify_cuda_budget_host.py \
  --binary zig/zig-out/bin/inference-device-budget-host-tests \
  --output .benchmark-results/cooperative-execution/physical-pressure

cd zig/pkg/inference
zig build test-cooperative-admission -Dcuda=true -Dmetal=false
cd ../../..
python3 -m unittest discover -s zig/pkg/inference/scripts \
  -p test_qualification_memory.py
```

The hardware pressure test deliberately uses a 64 MiB managed cap, retains a
real 48 MiB allocation, rejects another 32 MiB, injects a failed free, confirms
primary teardown, then allocates and verifies 64 MiB after reload. Its harness
requires owned-process NVML measurements during rejection, failed-free retention
and reload, and checks the observed excluded allocation allowance. This exercises
the production guard safely on a shared machine. It does not establish behavior
at 38 GiB, driver OOM, or A100 physical exhaustion.

For performance, run `qualify_cooperative_execution.py --mode serialized` and
`--mode cooperative` against the same binary, model variants, resource ceilings
and workload. Supply the serialized `references.json` to the cooperative run.
Pass `--binary`, `--config` and `--cgroup` to each serving campaign. The harness
verifies the running executable against the supplied binary, the service port
and configuration path, GPU identity and relevant runtime environment. Comparison
requires identical identities and configuration apart from the cooperative switch.
Both runs warm and verify independent references before measuring overlapping
requests. Each run checks unequal independent contexts in streaming and buffered
responses, exact output equivalence, request-specific markers, smaller-model
outputs, idle reservation recovery and stable model inventory. Streaming also
records first content, long-request content events and maximum content gaps.
The short-completion snapshot must demonstrate long-request scheduler progress.
The harness asserts the one-row envelope through scheduler counters. Two further
rounds each drain seven requests through active shared owners, queued arrivals
and an exclusive prompt-cache route, checking output references and recovery.

Compare the two output directories using `--compare SERIALIZED COOPERATIVE`.
The acceptance gates, defined before measurement, are:

| Metric | Cooperative gate relative to serialized |
| --- | --- |
| Short request first content | At most 75% |
| Short request completion, both response modes | At most 75% |
| Long request completion, both response modes | At most 135% |
| Aggregate output tokens/second, both response modes | At least 90% |
| Embedding and Decide p95 | At most 200%, and at most 1 second each |

The short sharing preflight requires actual backend ownership transitions before
starting long generations. To exercise simultaneous decoding, run the same
harness with `--scenario decode-overlap`, supplying references from that mode's
passing mixed-context campaign. It submits the short request after the long
request enters decoding and requires telemetry observing two decoding owners
plus scheduled long-request decoding during the short request. Compare these
two late-arrival reports separately from the initial mixed-context reports.

These are experiment gates, not production SLOs. Shared GB10 GPU utilization
includes unrelated workloads and is recorded alongside the service's memory.
A100 production qualification requires measurements on that hardware. This
change makes no A100 performance or near-exhaustion qualification claim.


## Physical-cap and recovery evidence on GB10

The production engine function-table integration passes both the model-free
multiple-node ownership test and the explicitly enabled hardware test. The
current GB10 recovery test uses the production host/engine initialization path
and records ten valid NVML samples during guard rejection, ten during failed-free
retention, ten after the external CUDA retainer releases, and twenty after reload.
All fifty samples are valid with no sampling failures.

A real 48 MiB allocation remains charged under a 64 MiB cap when a further
32 MiB is rejected and when `cuMemFree` is forced to fail. The managed wrapper
then fully deinitializes, transferring its primary retain and library reference
to quarantine. A separate CUDA user releases its reference; the driver still
reports the primary active and the allocation remains charged at 48 MiB. Reload
retries the free under the retained lifetime, reduces managed live bytes to zero,
then allocates, writes and reads 64 MiB successfully. NVML peaks at 245,874,688
bytes; observed excluded allocations peak at 178,765,824 bytes, below the reserved
2 GiB. This is a scaled guard/recovery test, not exhaustion near 38 GiB.

Model-free coverage additionally simulates the external user's would-be final
release between managed owners, later actual context teardown, and reuse of both
the context handle and device address. Copied stale descriptors cannot free the
successor allocation. Unsuccessful draining preserves quarantine; an ambiguous
retain release is not retried. The seven lifecycle/budget tests pass in CPU and
CUDA builds. The existing 82 CUDA execution-batch tests also pass. The sustained
admission test's earlier 64-round result remains separate evidence.

Current hardware receipts are in
`.benchmark-results/cooperative-execution/review-primary-lifetime-final/` in the
qualification worktree. Its test executable SHA-256 is
`ae87475f991cc8c7a2eeb94699a0dde43390e3640f678ce71404444fc2b00a90`.
The earlier `physical-pressure-final/` teardown report predates quarantine pins
and is superseded for failed-free recovery safety. No A100 hardware result is
claimed.

## Controlled one-row serving results on GB10

These receipts predate mandatory model-artifact hashing. They remain historical
measurements; the updated harness requires fresh artifact-aware campaigns before
accepting reproducible performance comparisons. No hashes have been backfilled.

The one-row serving campaign used the ARM64 CUDA HTTP-only production entry point,
built with `zig build http-server -Dcuda=true -Dmetal=false
-Doptimize=ReleaseFast -Dstrip=true -Dcpu=baseline+sha2`. Its SHA-256 is
`a05fbc05a498539bcb886c903169566727742bec953c5f24b217d518cf54789e`.
Both modes use that exact executable, GPU, immutable model inventory, runtime
profile and configuration; only `cooperative_execution.mode` changes. Thinking
is disabled, temperature is zero, and output lengths are fixed at 64 and 32
tokens. Independent warehouse and garden prompts contain 31,332 and 1,956
prompt tokens and different verification codes. All streaming and buffered
outputs match their independent references exactly.

| Scenario / client-observed metric | Serialized | Cooperative |
| --- | ---: | ---: |
| Overlapping prefill: short first content | 85.92 s | 4.19 s |
| Overlapping prefill: short streaming completion | 86.69 s | 36.48 s |
| Overlapping prefill: short buffered completion | 87.89 s | 21.92 s |
| Overlapping prefill: long streaming completion | 82.75 s | 86.85 s |
| Late arrival during decode: short first content | 30.67 s | 4.74 s |
| Late arrival during decode: short completion | 31.44 s | 6.91 s |
| Late arrival during decode: long completion | 83.88 s | 87.65 s |

Both comparisons pass every predefined gate. Aggregate output throughput stays
within 0.3% of serialized serving. In the initial mixed-context campaign,
embedding p95 is 110.97 / 110.87 ms and Decide p95 is 290.42 / 270.34 ms
(serialized / cooperative). In the late-arrival campaign those values are
115.38 / 104.47 ms and 287.32 / 281.87 ms. Smaller-model outputs remain valid.

The late-arrival run observes two decoding requests simultaneously and 36
scheduled decode items during the short request, exceeding its entire 32-token
output allowance and proving long-request decode progress. Long-request maximum
content gap is 2.04 seconds during the overlap, versus 0.44 seconds serialized.
At short completion the cooperative ownership counters show 24 transitions and
120 same-owner scope reuses: those reuses do not run a whole request reset.
Reset counters also include ordinary requests on the other CUDA models and must
not all be attributed to Gemma.

Each mixed-context mode additionally completes two rounds of seven requests,
including queued arrivals and an exclusive prompt-cache request, with exact
output references and stable model IDs. The separate cancellation campaign
admits two active requests and queues twenty more: six receive retryable 503
responses and fourteen receive retryable 504 deadlines. Waiting depth peaks at
14 while the active KV reservation stays exactly 1,535,115,264 bytes. An
exclusive cache request and normal generation succeed afterwards. Disconnecting
a streaming request after first content returns active work and transient KV
to zero, followed by successful recovery.

The main cooperative campaign has 1,363 valid memory samples and the serialized
control has 1,365; both late-arrival campaigns have 368 each. There are no
sampling failures. Cooperative managed CUDA peak is 12,640,666,796 bytes
(11.77 GiB); service NVML peak is 12.11 GiB. These overlap and are not additive.
They are well below the 38 GiB guard; the separate scaled test supplies the
actual allocation-rejection evidence. E4B's measured eager weight inventory is
7,413,380,264 bytes, retaining a padded weight reservation of 7,876,716,530 bytes
from its 11,598,617,880-byte construction reservation. These are weight values,
not cached scratch or total process memory.

Raw reports and exact requests are under
`.benchmark-results/cooperative-execution/{final-serialized,final-cooperative,
decode-serialized,decode-cooperative,cancellation-final}/`. Gate reports are
`performance-gates/performance-gates.json` and
`decode-performance-gates/performance-gates.json` under that directory. Shared
GB10 measurements do not establish A100 performance, production SLOs, or behavior
near 38 GiB exhaustion.

## Write-pool repeat

The same serving binary passes all 102 write-pool requests with Whisper small,
LightOnOCR3 0.8B, EmbeddingGemma 2 and GLiNER2.5. All eight workload phases have
valid memory evidence (140 samples total), with no sampling failures and stable
model IDs. This includes mixed traffic, batch activations, eight-page OCR,
windowed audio, limits, silence, OCR deadline cleanup and recovery. Service NVML
peak is 5.64 GiB; managed CUDA peak is 5,684,287,036 bytes (5.29 GiB).

Warmed and recovered retained backend scratch are both 1,611,251,712 bytes;
retained host scratch is 67,637,248 bytes. Active KV and in-flight work return
to zero. The retained OCR cache is not reclassified as model weights or a leak.
Raw evidence is `.benchmark-results/cooperative-execution/write-final/`.
The earlier dense repeated-sentence Whisper stress failure remains outside this
passing synthetic-fixture qualification; broad ASR accuracy is not claimed.

The final targeted CUDA regression run passes 96 tests, and the CPU regression
run passes 82 overlapping tests. Full API/client regeneration, Go SDK unit tests, TypeScript SDK type checking,
license headers and the Apache product-boundary check pass. Eight telemetry
unit tests verify that missing measurements and missing phase coverage cannot
pass. The repository's remote PR CI still requires its normal human approval;
local results are not presented as a completed GitHub CI run.

## Immutable model identities in controlled evidence

Controlled campaigns hash the manifests, model configuration, tokenizers and
weight artifacts for every loaded model using SHA-256. The inventory determines
which model directories participate, including all shards and linked artifacts;
operational checksum/download caches are excluded and never trusted as hashes.
The capture checks file metadata around each read and verifies that loaded model
instances remain unchanged. At campaign completion it hashes the artifacts again
and rejects changed content or missing evidence.

Both serialized/cooperative and one-row/two-row comparisons require the same
complete `model_artifacts` identities. Decode-overlap reference reuse and the
HTTP microbatch campaign's native controls enforce the same boundary. Older
reports without hashes remain historical observations: they cannot be promoted
or compared by the updated harness. Hashes cannot be retroactively attached to
prove which files an earlier campaign loaded; fresh controlled runs are required.
Model files must remain immutable from loading through campaign completion.

Model-free checks:

```sh
cd zig/pkg/inference
zig build test-cuda-allocation-lifecycle -Dcuda=true -Dmetal=false --summary all
python3 -m unittest discover -s scripts -p test_qualification_identity.py -v
python3 -m unittest discover -s scripts -p test_qualification_memory.py -v
```
