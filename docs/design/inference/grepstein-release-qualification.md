# Grepstein inference release qualification

Qualify the release candidate and the deployed serving profile separately.
A CUDA backend, a ready pod, a downloaded model, or a passing synthetic
throughput test does not establish complete document extraction. Keep shared
inference as the execution plane; Grepstein does not need an instance-local pool.

## Record the exact candidate

Keep the source revision (and dirty patch digest if applicable), binary and
container digests, GPU model/compute capability, driver, model revisions,
effective resource/request ceilings, environment overrides, and fixture hashes
with every receipt. Historical DGX Spark measurements are not A100 results.
Run correctness before measuring throughput, and exclude competing GPU jobs
from timed runs. Report missing hardware and skipped tests as unqualified.

The Grepstein model selections are:

| Role | Pinned artifact | Required checks |
| --- | --- | --- |
| Reader | `lightonai/LightOnOCR-3-4B:safetensors@b71010f095dc3735de7ee6b9969bff8f8f50b39a` | Full-page completion, GPU recurrent dispatch, numerical parity, bounded memory |
| Alternate reader | `lightonai/LightOnOCR-3-0.8B:safetensors@36d8636925327c671bd8e479c517a004f7566bd9` | The same checks independently; changing model is a new extraction recipe |
| Embedder | `google/embeddinggemma-2:safetensors@914f7f89142e33e77833254d9c9b90c3cef7303b` | Document/query task preparation, dimension, normalization, identity fencing, retrieval ranking |
| Extractor | `fastino/gliner2.5-base-v1:safetensors@54785d51df8d86a0d8f2eb212fec15871aad2c4d` | Exact artifact/backend qualification, entity spans, relation endpoints and long-document coverage |

An installed GLiNER2 checkpoint does not substitute for GLiNER2.5. Request the
pinned model explicitly and verify discovery and the selected route agree.
Gemma 4 generation is a separate capability; enabling it is not a prerequisite
for the initial full-text, embedding, and GLiNER graph pipeline.

For focused Zig checks, use one compile-time substring (for example
`zig build test-bin -Dcuda=true -Dcuda-artifacts=fatbin -Dtest-filter=LightOn`),
then run the installed binary with `--test-filter LightOn`. Multiple Zig
compile-time filters intersect; the runtime runner combines its filters as a
union. `-Druntime-test-filter=true` deliberately compiles the full suite and is
expensive. Require named cases and nonzero runtime matches; declaration-wrapper
checks alone do not establish numerical or policy coverage.

## Reader execution and completeness

1. Run the placement, recurrent numerical oracle, ownership, and admission
   tests on the candidate. Test both checkpoint geometries. Default CUDA
   recurrence must remain device-resident; explicitly disabling the core must
   resolve to `host/operator_disabled` at model load. A resident device history
   must fail closed if device execution fails, rather than migrate to an empty
   host history. Full-prefix forwards without a decode cache must acquire
   temporary device state, release its admission on completion or failure,
   and match a fresh cached forward. Check placement logs and recurrent
   model metrics.
2. Compare the default serving profile with the explicit CPU recurrence
   baseline on identical inputs. Use `benchmark_concurrency.py --backend cuda
   --require-recurrent-placement`; omit `--cuda-recurrent-core` for the default
   and use `--no-cuda-recurrent-core` for the baseline. Keep resource ceilings
   and fixture hashes in the reports. The benchmark rejects missing or mixed
   placement diagnostics when this assertion is requested.
3. Include representative scanned DOJ pages: dense text, rotated scans,
   tables, low contrast, blank pages, and mixed native/scanned PDFs. Preserve
   page order and source/version identity. Compare full text including the
   last visible lines; a short nonempty response is not a completeness proof.
4. Force an output cap too small for a known fixture. Plain `/read` must reject
   incomplete output with `OUTPUT_TRUNCATED`; structured document output must
   preserve `truncated`/`unresolved` status. Materialization must not upgrade
   either to a complete searchable transcript. Exercise the configured accurate
   reader/refinement policy and verify exhausted budgets remain unresolved.

A 512-token diagnostic, matching the deployed allowance, rejected a cached dense
DOJ page on Glowworm. The Grepstein reader example now requests 2048 tokens; both checkpoint diagnostics
completed that page and retained the checked tail text. This is a bounded
sample, not proof that every corpus page fits. Do not retry an unchanged capped request forever.
Measure the failing pages, then qualify a larger admitted output allowance or
a bounded document refinement policy. A higher serving ceiling and the reader
request must agree; increasing only one does not recover the missing tail.
Also qualify the work-unit capacity: a single image with a 2048-token output
allowance currently needs at least 12 generation admission units, exceeding
the existing eight-unit serving profile. Raising only the token ceiling would
still reject it. Keep request concurrency bounded while qualifying that change.
The proposed reader-policy adjustment is below; retain the current memory,
pixel, and input-token limits and qualify it on the intended hardware before
applying it. Sixteen work units leave headroom above that twelve-unit minimum;
the independent concurrency ceiling still admits one request.

```json
{
  "resources": {
    "inference": {
      "admission": {"max_concurrent_requests": 1, "max_work_units": 16},
      "request_limits": {"max_output_tokens_per_item": 2048}
    }
  }
}
```

This is a partial configuration to merge into the existing policy, not a
replacement for its other limits. Use the native pool probe's
`--reader-max-output-tokens 2048` switch to check the intended ceiling and its
exact rejection boundary after deployment to a qualification environment.
Retry affected source versions after the policy change and require complete
artifact coverage before semantic or graph enrichment. Avoid automatic image
tiling without a tested ordering/deduplication policy.

## Large-page vLLM comparison

The reader throughput target is parity with vLLM, preferably faster, while
preserving complete extraction. A faster short response does not qualify.
Compare both LightOn checkpoints independently, using the original checkpoint,
identical image hashes, image-only prompts, greedy decoding, and the same output
allowance. Include dense full pages and mixed page lengths at concurrency one
and the proposed serving concurrency. Compare pages per second and latency
distributions; token accounting can differ by whether the terminator is counted.

Use `benchmark_vllm_offline.py` to produce a private reference report and
`scripts/qualify_reader_page_latency.py --reference-report REPORT` to compare
native HTTP responses without copying document text into public receipts. The
report records exact, whitespace-insensitive, and alphanumeric comparisons
separately. Only exact comparison is an automatic parity gate
(`--require-reference-parity`); an alphanumeric match alone cannot qualify
punctuation, layout, or correctness. Review every differing full output against
the source image. Record formatting-only differences explicitly rather than
stripping separators or punctuation from production responses to match an oracle.

Verify the GPU has no competing compute processes before each run and that the
server owns the measured GPU process. Drain and unload each model before stopping
its server. Keep uninstrumented timing runs separate from profiler runs and
report the HTTP versus in-process boundary. Disable reference prefix and processor
caches and use fresh media identifiers. Measure cached-weight startup, new-node
artifact download, and first-request compilation separately; a warm local model
load does not establish Cloud cold-start latency.

Qualify the final runtime defaults, not only a diagnostic environment profile.
Recheck mixed-page batching, model eviction/reload, complete tails, memory limits,
and overload behavior with those defaults. DGX Spark results establish a useful
kernel comparison but do not qualify the A100 serving deployment or its container
libraries. Require the paired released runtime/operator and an A100 canary before
changing the live shared reader pool.

### Current large-page investigation

The concurrent throughput target is not met yet. The isolated DGX Spark tests
use original BF16 checkpoints, identical hashes for a deposition page and a
longer dense page, image-only greedy decoding, and a 2,048-token output allowance.
Each four-request cohort contains two copies of each image. The same HTTP client
can now compare `/ai/v1/read` with an OpenAI-compatible reference using
`--api-shape openai-chat --url http://127.0.0.1:PORT/v1/chat/completions`.
Payload construction is outside timing in both modes; unknown or truncated chat
finish reasons cannot qualify, even if expected tail words appear.

| Four-page policy | LightOn 4B seconds | LightOn 0.8B seconds |
| --- | ---: | ---: |
| vLLM in-process, two fresh rounds | 33.01–33.05 | 7.08–7.12 |
| vLLM HTTP, same client and prompts | 32.93–33.02 | Not measured |
| Earlier native defaults, main-merged runtime | 44.70–44.84 | 12.24–12.31 |
| Current automatic capability policy | 40.68–40.71 | 9.77–9.87 |
| Best measured diagnostic profile | 35.24–35.44 | 8.18–8.21 |
| Two-image vision diagnostic, after geometry fix | Not measured | 11.58–11.75 |

The best 4B diagnostic uses recurrent-input packing, BF16 vision, an owned greedy
continuation, and optional decoder graphs. It captures four graphs and replays
766 times without graph admission denials over two rounds. Bounded 1,024-row
prefill chunks reduce its four-page time from 35.46–35.52 to 35.24–35.44 seconds.
The 0.8B diagnostic uses its independently selected eager profile and a four-token
executor burst; the burst improves 8.28–8.36 to 8.18–8.21 seconds. These complete
precision/chunk/burst configurations remain distinct from automatic policy.
Portable packing, graph prerequisites and owned continuation now select
automatically; BF16 vision and the diagnostic chunk/burst settings remain explicit.
Paired feed-forward weight copies add little improvement over recurrent-input
packing; projection-output BF16, bounded matrix tuning, and a simple batched
matrix-vector prototype did not improve on the selected path. They were not
promoted. Wider vision batching is correct on the tested pages but slower;
keep the single-image vision default.

The native 4B outputs are byte-identical to vLLM's batched outputs. The dense
page's vLLM serial output differs from its own batched output in line breaks and
a trailing Markdown separator. Its strict in-process serial-reference assertion
therefore still fails. The HTTP comparison passes functional completion checks,
with that exact-reference difference recorded separately; it is not an automatic
serial-reference correctness pass. Do not strip production output to hide the
difference. Native 0.8B outputs match both serial and batched references exactly,
including the new two-image vision path and the optional token burst. Every
successful run listed above retains complete tails; failed experiments are
excluded from those timing ranges. This is bounded same-page coverage, not a corpus-wide accuracy
qualification.

With the earlier defaults, the two 4B pages took 52.96–52.97 seconds serially,
compared with the earlier clean 57.22–57.23-second vLLM serial control. Four-page
native decoding improved from approximately 51 seconds before the packet changes
to 44.7 seconds in that earlier default-policy measurement. Diagnostic profiles must
remain distinct from these default-policy measurements.

Clean software-timestamp profiles attribute 34.52 seconds of GPU busy time to
the native graph/input-packing profile versus 32.86 seconds to vLLM. Matrix
kernels consume approximately 29.7 seconds in each. Native paged-attention
chunk kernels take 1.33 seconds versus 0.70 seconds for the reference decode
attention, and native also spends more time in prefill and vocabulary reduction.
The profile is diagnostic evidence, not a replacement for uninstrumented timing.
Attention variants measured at actual page contexts pass independent numerical,
canary and input-immutability checks. Kernel microbenchmarks establish kernel
behavior; uninstrumented full-page runs measure serving throughput. Throughput
parity remains open.

The long-context packet passes 64 independent GPU numerical and graph-replay
oracle cases, including sparse/permuted tables and contexts through 8,192 tokens.
It requires a CUDA 13 driver API; older drivers retain ordinary CUDA attention.
The projector now applies backend-specific per-image and aggregate vision bounds:
CUDA accepts up to 16,384 patches per image and 65,536 packed rows, while Metal
retains its 4,096-patch batching envelope. Brokers cap their work to that geometry.
A prepared page outside the batching envelope retains its serial encoder instead
of failing only because batching was enabled.

Canonical reader profiles authorize the executor-owned continuation independently
of multi-token bursts. Pure-greedy, callback, progress, cancellation and scheduler
ownership checks remain mandatory. A live functional check timed out one request
with `504 INFERENCE_TIMEOUT` while a peer completed with the expected full output
hash; all 38 readiness probes succeeded. The same check passed four-page 0.8B
batching and 4B eviction/reload. Compilation ran during this functional check,
so its timings are excluded from performance qualification. The final main-merged
binary separately passes serial and mixed-page requests, exact 0.8B reference
checks, model switching, readiness after each phase, and explicit unloading.

Decoder graphs now select automatically from compiled/runtime capabilities,
with explicit operator disables preserved. Projection-rounding modes whose
arithmetic is not capture-compatible retain eager CUDA execution. Immutable fusion copies consume
the device weight budget independently of scratch; device/combined admission and
release on model retirement remain mandatory. All measured native runs retain
the 20 GiB host, 34 GiB device, 4 GiB KV and 8 GiB scratch limits. The DGX uses
its reference environment's CUDA 13 libraries, not the deployment image's pinned
CUDA 12 libraries. No result here qualifies the A100 pool, full container image,
new-node artifact download, or production rollout.

## Capability-based defaults

Hardware selection and benchmark evidence are separate. LightOn graph capture,
FFN fusion, input projection packing, recurrent state pooling and device token
bursts must not depend on the GPU used for testing. Portable PTX 9.0 operations
require SM80+ and CUDA driver API 13.0; ordinary tensor-core and device-ID
operations use their own compiled kernel/library checks. Older drivers retain
ordinary CUDA instead of attempting unsupported PTX.

The runtime selects graph prerequisites automatically for resident LightOn CUDA
sessions and preserves each explicit environment disable. Startup logs include
`lighton_cuda_policy`, compute capability, driver, and a named graph decision:
unsupported compute/driver, disabled dependency, unavailable cuBLASLt, insufficient
plan cache, noncapturable arithmetic/execution, or enabled. Epoch ownership,
immutable weights, greedy eligibility, admission and cancellation still gate
individual claims. A bounded request can use eager CUDA even when the session
supports graphs. Projectors query the resolved backend vision policy instead of
rereading old opt-in environment flags; explicit disables apply at both caller
and execution paths. End-to-end checks require the selected path counters,
not just successful text responses.

Encoder BF16 mirrors, bounded DeBERTa attention, portable quantized routes and
generic graph replay support select from compiled tensor-core capability rather
than an L4 allowlist. Laya warp attention uses its loaded symbol and shape
contract instead of a GPU-model check. Per-operation symbols, shapes, numerical recipe and memory
checks remain authoritative. Explicit overrides continue to work.

Retained architecture restrictions correspond to actual compiled artifacts:
`sm_121a` fused vision/head kernels, the external SM12.1 recurrent-prefill
library, and layout-dependent attention kernels. Generated portable matmul
execution uses its multi-target build contract and common resource envelope;
the catalog retains the original SM89 evidence fingerprint. No benchmark record
is relabeled as an A100 measurement. SM121-specific cuBLASLt
heuristic tables also remain local to that architecture; other GPUs use native
cuBLASLt heuristics. Generated Flash-prefill kernels that still index opaque
WMMA fragments retain their implementation-specific layout restriction. Typed,
exhaustive renderer contracts declare portable SIMT versus layout-dependent
execution; command strings and benchmark labels are not execution contracts.
Arithmetic-changing precision profiles and experimental
algorithms retain their independent explicit settings. Resource capacities stay
bounded; no DGX benchmark memory or 128-row preset becomes a serving default.

The current automatic-policy run uses the same 20/34/4/8 GiB host/device/KV/
scratch envelope, four decoder rows, one image per request, and original pinned
checkpoints. Four mixed pages take 40.68–40.71 seconds for 4B and 9.77–9.87
seconds for 0.8B; two serial 4B pages take 51.68–52.06 seconds. Portable vision
QKV/MLP counters confirm execution without opt-in flags. Diagnostic precision,
chunk and burst configurations remain separate; throughput parity is still open.
The final binary SHA-256 is
`3cf76237db376d4dbf10038e2b5357b50ce576ed8709c594079b3abd5ee316b1`.
The isolated run passes all seven phases, including model switch/reload,
complete reference hashes, cancellation/recovery and unloading. An 8-second
request receives `504 INFERENCE_TIMEOUT` while its peer completes correctly;
all 37 readiness probes succeed. Automatic QKV and MLP counters each reach
576 calls; normalization-cache hits and pinned metadata uploads are positive.
Graph capture/replay is observed for both readers. No diagnostic optimization
flags are set. Startup is 10.1 seconds with locally cached artifacts, not a
new-node download measurement.
Use `scripts/check_cuda_generated_matmul.py --output REPORT` from the inference
package to check bundled portable Q4 kernels against a CPU FP64 reference,
including output guards and readonly inputs. A100 test coverage is an evidence
gap, not an additional software allowlist or a requirement for manually enabling compatible
paths.

Final focused CUDA validation selected 165 tests: 164 passed and one optional
BF16-prefill scratch case skipped. This includes the automatic policy and
explicit-disable checks, immutable evidence/portable execution contracts,
generated selectors, graph and cancellation ownership, and the A4B baseline
primitive smoke test. Additional focused resolved-vision policy and CUDA
recipe-pin regressions pass. A4B admission now checks its loaded resident primitives;
its exact geometry and memory envelope remain enforced. The GB10 smoke uses
small synthetic expert matrices, not a full 26B model qualification. Six
independent bundled Q4 matmul checks passed (maximum absolute error 2.33e-7),
as did 16 profile/benchmark Python tests and five artifact-policy tests.

## Portable EmbeddingGemma attention

EmbeddingGemma 2's online BF16 attention now uses the documented PTX
`mma.sync.m16n8k16` register layout for P×V accumulation and rescaling. It does
not index implementation-defined WMMA fragments. Q×K continues through public
WMMA load/store APIs, which preserve the logical matrix layout. Accumulators
stay in registers, and output normalization stores directly from those
registers. Two value tiles share one `ldmatrix.x4.trans` load. Launch bounds
prevent a small register increase from halving 512-thread block occupancy.
The default local/global routing follows bundled BF16 kernel support and the
existing geometry bounds, including the scalar route for partial query tiles.

A DGX GB10 comparison of standalone old/new kernels passed six cases bitwise,
covering local/global attention, batches, masks, fully masked input, and
64–2,048 tokens. Independent CPU FP64 SDPA checks passed (maximum absolute
error 0.00185, including the kernel's existing BF16 probability boundary).
The final bundled image additionally passed eight cases bitwise, including
65/129-token scalar query tails; output guards and readonly inputs passed.
Compiled SM80 through SM120 images each use 64 registers per thread.
Seven alternating-order rounds of
100 CUDA-event-timed launches per kernel measured:

| Batch / tokens / head dimension | Old ms | Portable ms |
| --- | ---: | ---: |
| 1 / 512 / 256 | 0.319 | 0.320 |
| 4 / 512 / 256 | 0.952 | 0.950 |
| 1 / 2,048 / 256 | 1.313 | 1.332 |
| 1 / 512 / 512 | 0.557 | 0.572 |
| 4 / 512 / 512 | 3.072 | 3.045 |

This preserves kernel performance within approximately 3% on this GPU. It
is not an A100 timing result or an end-to-end embedding/retrieval result. Run
`scripts/check_cuda_embeddinggemma_attention.py --artifact FATBIN --output REPORT`
on each target GPU; add `--baseline OLD_FATBIN --benchmark` for bitwise and
latency comparison. Use the existing encoder/retrieval qualification below
before changing an index's serving recipe.

## Embeddings and extraction

EmbeddingGemma 2's CUDA/BF16 execution recipe has a different `model_identity`
from CPU/F32, and optimized arithmetic profiles also have distinct identities.
Discover the serving identity, pin it in the index configuration, and use the
same compatible recipe for document and query vectors. Portable default
attention advances the CUDA recipe to `embeddinggemma2-cuda-bf16-mean-v3-portable-mma`:
compatible GPUs can replace a previous scalar route with BF16 tiled attention.
CPU/Metal artifact identities remain unchanged. Existing CUDA pins must use a
new index/identity after reindexing, including conservatively invalidating old
SM89 pins even though the tested kernel outputs match bitwise. A request with an old
identity must fail with a conflict, not silently produce vectors from a changed
recipe. Verify index recovery and query caches preserve the pin. Rebuild into a
new index when the recipe changes; do not mix the two vector spaces. Run the
existing `scripts/embeddinggemma2/qualify.py` against the matching pinned oracle,
and check a small labeled retrieval set using the real task prefixes.

For GLiNER2.5, retain the artifact/backend/feature/length checks in
`models/gliner_boundary_qualification.zig`. Check entity text and offsets,
typed relation endpoints, chunk boundaries, and repeated entities across chunks
against the independent fixtures. Reject requests outside supported contracts;
do not remove an artifact or numerical qualification gate to improve routing.
Graph publication must wait for complete current-version extraction, and a
retry must not duplicate graph edges or leave stale-version edges searchable.

## Cold starts, pressure, and recovery

Through the actual authenticated hosted proxy, exercise each model from a
naturally idle pool: execution discovery, advertised activation, model download,
loading, successful work, and a second warm request. Verify the returned
capability lease and model identity. Record activation and download time apart
from execution. Do not scale down a busy shared production pool for this test.

Test model-loading responses, stale capability leases, overload, deadline
expiry, client cancellation, and a retry after recovery. Check exact error
codes/retry guidance. A temporary loading failure must not become a permanent
document failure, and retries must retain source/version identity. Existing
single-flight activation tests cover the protocol; a release receipt must also
include the actual deployed proxy/operator path.

Under the intended per-role memory and concurrency ceilings, exercise long
prompts and outputs, mixed-size requests, model switching, and cancellation
while work is queued or GPU buffers are retained. Include host, device,
combined/unified, KV, scratch, persistent recurrent state, download staging,
and ephemeral storage. Verify refusal before unsafe allocation, release after
success and failure, sustained readiness, no eviction/restarts, and completion
of a later valid request. Keep request cardinality separate from native
microbatch width. DGX Spark's 128-slot preset and large unified-memory budgets
must not become the shared A100/L4 serving defaults.

## Publication gate

Keep original preparation, full-text extraction, embedding enrichment, and
graph publication as separate resumable stages. Run Grepstein's `qualify`
command against the sealed publication: index readiness alone does not prove
that every source page was extracted. Keep semantic/graph publication disabled
until the required source versions have complete artifacts and the corresponding
model checks above pass. Public hosting additionally needs the source
suppression and exposed-API bounds described in the Grepstein design.
