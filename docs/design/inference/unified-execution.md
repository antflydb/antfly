# Unified execution (#1166)

The migration introduces common owners for model loading, semantic programs,
prepared execution, physical storage and model state. Existing optimized kernels
and whole-model implementations remain the selected implementations behind those
owners. It adds no required external inference runtime.

The implementation branch is based on `origin/main` revision
`ae0b8856fdc62c7c164125ade966d3ed0cd2dc8e`. Hardware qualification uses CPU and
Metal. CUDA construction, planning, descriptor execution and capture ownership
are implemented and compile, with host-side regression coverage; CUDA numerical
and performance qualification still require a CUDA host.

## Ownership and preparation

| Owner | Responsibility |
| --- | --- |
| `architectures/model_definition.zig` | Backend-independent configuration and loaded weight sources; GGUF metadata/architecture mapping and safetensors sources |
| `models/session_contract.zig` | Resolved task, embedding processor, padding, pooling, normalization and modality semantics |
| `backends/session.zig` | Published contract and typed backend acquisition, preserved through session wrappers |
| `backends/compute_context.zig` | Common CPU/Metal/CUDA/WASM construction, exact typed imports and stable shared backend ownership |
| `execution/program.zig` | Canonical optimized IR, composed node mappings, named input/output/weight/state bindings and reusable analysis |
| `execution/executable.zig` | Common preparation and reusable execution, output policy and optional diagnostics |
| `execution/model_candidate.zig` | Common preparation and dispatch of existing opaque whole-model implementations |
| `execution/physical_buffers.zig` | Cached assigned-kernel bindings, planned outputs/transfers/workspace and bounded invocation frames |
| `graph/decode_state_runtime.zig` | KV/recurrent/paged state ownership and advancement, including borrowed cursors |

ONNX, direct CLIP/CLAP graphs and traced decoder graph caches prepare through the
same executable boundary. Already optimized cached graphs borrow their existing
analysis instead of cloning or optimizing it again. Static immutable bindings
retain CPU/CUDA storage across bounded shape specialization. Dynamic imported
models retain their requested backend and shared initializer context.

Whole-model selection is also shared: existing specialized Metal implementations
stay ahead of live CPU execution. A model candidate may decline during capability
preflight. Once it calls the runtime, errors propagate; callers cannot replay a
partially advanced decode through another implementation. Fresh prompts reset
state, while subsequent prefill chunks preserve it. Sampling keeps the same
commit boundary.

Task pipelines now consume runtime state and output contracts. Serving delegates
whole-model selection and reads the resolved processor/task contract. Architecture
construction publishes a typed backend provider and decoder configuration, so
wrappers do not need to preserve private vtable pointer identity.

## Physical execution and shared kernel descriptors

`ops/kernel_descriptor.zig` describes dense F32 unary/binary operations, linear
projections, normalization, softmax/log-softmax, reductions, contractions and
layout operations. `ops/kernel_geometry.zig` shares contraction/reduction geometry
between execution and legality checks. CPU and CUDA consume the descriptors;
Metal shares contraction geometry and unary identities while retaining its
existing region/command/arena planner.

Descriptors describe semantics and requirements, rather than imposing one target
schedule. Native execution keeps SIMD, threading, BLAS, source-weight conversion
and packed quantized routes. CUDA keeps cuBLASLt, training GEMM and existing launch
selection. Mixed/packed operations and auxiliary RMS representations retain their
existing routes when a descriptor cannot express their full requirements.

`ops/kernel_backend.zig` groups target-owned physical storage, descriptor,
completion and retention hooks. Device backends cannot inherit CPU tensor casts
through that table. Metal also clears inherited optional tensor callbacks before
installing its implementations; supported generic decompositions or explicit
host adapters handle the remaining operations.

Preparation caches each eligible kernel's backend, input IDs, output allocation
and workspace requirement. Per-backend scratch is bounded by the maximum selected
requirement. Logical lifetimes follow submission positions, including reordered
partitions, aliases and transfer-backed views. Eligible CPU and general CUDA
operations write into their planned storage. Other operations retain their
qualified allocation/arena routes, with observable fallback statistics.

Atomic storage leases keep views and published results alive after executable
or shape-cache eviction. Result metadata copies its representation owners, and
fallback results retain the runtime-owned native backend. Shared resident session
results retain the common backend context even after producing sessions close.
External backend contexts must otherwise outlive their tensor values.
Resident model-runtime outputs also retain their runtime owner across cache
retirement. Explicit host materialization releases that owner after all device
and temporary output tensors; host-only outputs do not pin runtime state. Pending
speculative decode work drains when cache ownership retires. Prepared and direct
Metal output tests cover eviction, materialization and allocation failures.
Deferred decoder tails snapshot their final hidden row before publication, so
later prompts cannot overwrite a value through reserved scratch storage. The
shared row-snapshot helper uses existing backend copies and stable CUDA physical
allocation, without activation readbacks. This copies one hidden row at the
prefill boundary; existing block, GEMM and greedy/sampling schedules remain.

The frame pool bounds retained storage to four frames. Outstanding outputs or
captured addresses pin a frame; execution selects another frame or reports the
compatibility fallback. CUDA completion events protect reuse across streams, and
cold eviction waits before retiring addresses. CUDA owned reshape/row views
retain their parents instead of leaving unpinned borrows.

CUDA capture retains warmed physical leases before recording addresses and also
retains operand/output/workspace leases encountered during recording. Executable
updates retire old owners after submission completion. Metadata-only resets
preserve executable ownership; aborted captures release pending references.
Backend teardown destroys captured executables before cache/weight owners. The
learned temporary arena remains for legacy kernels and preserves its capture
count/generation guards. This integration has host tests, not hardware capture
qualification.

This is the beginning of shared kernel descriptions, not a replacement shader
language or a rewrite of every optimized schedule. Quantized encoding recipes,
all auxiliary results and generated target source can migrate incrementally as
their existing routes are qualified.

## Placement, residency and observability

Inputs can supply their actual owning backend independently of the default
execution backend. Preparation distinguishes proven current storage from
predicted uploadability. Unknown owners fail before tensor access. Transfer-aware
refinement compares enclosed pure device regions with CPU execution, charging
launches and unique transfers once per destination. It preserves exposed outputs,
specialized/stateful plans and the conservative 4096-unit CPU work ceiling.

`.producer` outputs retain their producer. Explicit host export reads from that
producer; a final CPU fallback is not copied back to Metal merely to download it
again. Resident session requests preserve their declared single-backend contract.
F32 fallback downloads and CPU-to-CUDA uploads can use planned storage directly.
Integer and other typed transfers preserve exact bytes or fail explicitly.

Optional diagnostics report actual executor/region dispatch, physical allocation
and reuse, frame pressure, planned transfers and actual upload/download calls and
bytes. Request-scoped transfer audits cover CPU/Metal/CUDA/WASM entry points;
strict mode rejects device host access before the copy, including hidden fallback
reads. Nested policies cannot weaken an outer strict audit. Disabled diagnostics
do not collect partition counters or sample provider snapshots.

## Semantic consolidation and retired paths

- Imported ONNX now uses the fixed-point optimizer with composed public bindings.
  Static Loop/Scan handling implements supported complete signatures and rejects
  unsupported or unresolved control flow and incomplete multi-output semantics.
- ONNX export emits portable reciprocal square root, exact/tanh GELU, correct
  softmax axes and explicit contraction layouts. CPU grouped concatenation reads
  source-backed F32 initializers without cloning their full payloads.
- `architectures/bert_blocks.zig` is the common BERT embedding and encoder-layer
  composition for eager inference and differentiable graph construction. Target
  adapters preserve packed QKV, attention and fused normalization implementations.
  Activation choice and normalization epsilon come from the model.
- Retired the duplicated decoder trace/cache/execute and PJRT attachment block
  which ran the model again after logits were already available. One forward
  performs one head projection on both cache miss and hit.
- Retired per-forward graph backend/mesh/partition reconstruction in traced decode,
  the duplicate pipeline-owned decode cursor, and five Metal-specific whole-model
  dispatch wrappers in favor of common owners.
- Extracted loading and backend construction from the ONNX/session factory paths;
  deduplicated typed CUDA uploads and shared capability/cost policy.
- Metal host/device paired dense projections share encoding and existing multi-row
  kernel selection. This also fixes a pre-existing bug where a single-row shader
  left later prefill rows unwritten. Both projections still share one command
  buffer; optimized single-row and packed active-frame routes remain.
- Metal C entry points for planned compute scopes, frame-resource retirement, PLE residuals and the borrowed
  shared device drain temporary Objective-C ownership even on plain Zig threads.
  An accelerated allocation-stack probe reproduced growing compute contexts in
  the thread autorelease pool; scoped pools remove that retention. A conditional
  debugger backtrace located further shared-device autoreleases inside Metal
  buffer destruction during frame retirement. The final allocation probe reports
  zero entries and zero bytes in autorelease pools.
- Prepared decoder decline/error paths now release reserved hidden-state handles.
  Buffer metadata has one owner during partial construction, and every metadata
  allocation failure is exercised on actual Metal.

New execution owners live under `execution/`; retained graph/backend modules are
adapters or the existing implementation providers. Further physical file moves
can follow without maintaining duplicate implementations. The generic interpreter
remains the explicit reference/compatibility path. Kernel source generation and
hardware-specific schedules are separate qualification work.

## Qualification

Use repository-pinned Zig 0.17.0. Real Metal tests need access to the host GPU;
sandboxed runs may report no device and skip the hardware cases.

- ML standalone suite: 250 passed.
- ONNX standalone suite: 245 passed, four external-fixture skips.
- Final broad execution suite: 695 passed, zero skips. Earlier fixture-dependent
  suite: 178 passed, seven unavailable fixture/CUDA skips.
  Covers CPU/Metal dispatch, physical allocation/reuse, aliases, retained results,
  eviction, exact integer inputs, dynamic ONNX specialization, strict residency,
  state/cancellation and common opaque dispatch.
- Final focused lifetime and CUDA host-guard suite: 20 passed, zero skips.
- Final product `zig build check -Dmetal=true -Dcuda=true` passed.
- Browser `zig build wasm -Dwasm=true` passed. The native monolithic
  `check -Dwasm=true` step is not the browser artifact target.
- Shared BERT block and full-encoder gradients pass finite differences; prepared
  CPU/Metal outputs pass their numerical checks.

`bench-unified-execution` supplies identical pretokenized workloads and saves full
F32 outputs. Timing defaults to diagnostics disabled; use `--diagnostics true`
for a separate dispatch/transfer/memory measurement. `bench-unified-fixtures`
exports equivalent dense GGUF and portable ONNX artifacts from the BERT fixture.

Pinned fixtures:

| Fixture | Identity |
| --- | --- |
| MiniLM F32 | `sentence-transformers/all-MiniLM-L6-v2`, revision `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`; H384, six layers, 12 heads |
| Tiny Llama F32 | `IlyasMoutawwakil/tiny-random-llama`, revision `6097fb9db23325c29cb6556140bfd672503d57c4`; H16, two layers, four heads |
| DeBERTa F16/Q4_K | Local `antflydb/mxbai-rerank-base-v1` safetensors/GGUF variants |

Local fixtures are ignored under `.benchmark-assets/1166/`. The independent
standard-library oracle `scripts/validate_unified_decoder_reference.py` verifies
the tiny Llama weight SHA256 and compares every prefill/decode logit. It uses F64
KV and arithmetic; the production F16 KV cache requires a declared tolerance.
At tolerance `0.002`, CPU and corrected Metal pass all five 32,000-logit outputs.
Metal's maximum prefill error is `0.000297` versus `0.018709` on the original main
revision. Whole/chunked prefill and repeated reset outputs are byte-identical on
the corrected Metal path. The baseline bug is explicitly excluded from a claim
of numerical equality to baseline.

### Serialized checkpoint performance

ReleaseFast on an Apple M4 Max with 36 GiB RAM. Each ordinary case uses A/B/B/A
ordering, seven warmups and 41 measured requests per arm. CPU Q4_K uses 25
warmups and 1,001 requests per arm, with six arms per executable in
`ABBABAABABBA` order. The table reports the median arm p50 and the ratio of median
arm p95 values. Diagnostics are disabled during timing. Every complete output
is byte-identical to its baseline for the same backend and encoding. All 13
cases pass the 5% p50 and 10% p95 limits on the frozen executable after the
final host-ownership fixes (worst p50 ratio `1.0096`, p95 ratio `1.0242`).

| Workload | Baseline p50 (ms) | Candidate p50 (ms) | p95 ratio |
| --- | ---: | ---: | ---: |
| MiniLM safetensors, CPU, batch 1 / sequence 16 | 1.6170 | 1.6042 | 0.9612 |
| MiniLM safetensors, CPU, batch 1 / sequence 128 | 8.9975 | 8.9848 | 0.9974 |
| MiniLM safetensors, CPU, batch 2 / sequence 16 | 2.2959 | 2.3071 | 1.0242 |
| MiniLM GGUF, CPU, batch 1 / sequence 16 | 1.5101 | 1.5064 | 0.9980 |
| DeBERTa F16, CPU, batch 1 / sequence 16 | 78.1875 | 77.9264 | 0.9896 |
| DeBERTa Q4_K, CPU, batch 1 / sequence 16 | 11.4203 | 11.5298 | 1.0112 |
| MiniLM safetensors, Metal, batch 1 / sequence 16 | 2.4705 | 2.4609 | 0.9091 |
| MiniLM safetensors, Metal, batch 1 / sequence 128 | 3.2203 | 3.2181 | 0.9960 |
| MiniLM safetensors, Metal, batch 2 / sequence 16 | 2.5166 | 2.5234 | 1.0006 |
| MiniLM GGUF, Metal, batch 1 / sequence 16 | 2.4655 | 2.4571 | 0.8679 |
| DeBERTa F16, Metal, batch 1 / sequence 16 | 52.7800 | 52.6857 | 0.9989 |
| DeBERTa Q4_K, Metal, batch 1 / sequence 16 | 8.5865 | 8.5572 | 1.0042 |
| MiniLM ONNX, Metal, batch 1 / sequence 16 | 6.7825 | 6.7337 | 0.9944 |

Earlier short CPU Q4_K samples before the final pool fix exceeded the p95
budget (ratios `1.1281` and `1.2364`) despite stable median latency. The longer
protocol above was selected before the final series. Earlier receipts remain
in `acceptance-final/`, `acceptance-final-q4-cpu-recheck/` and
`acceptance-final-q4-cpu-extended/`; the pre-host-fix series remains in `acceptance-retirement-final/` and the final
series is in `acceptance-host-ownership-final/`.

Reported device payload peaks, allocations, final readback counts/bytes and
selected packed/fused routes match the corresponding baseline. MiniLM keeps six
packed QKV and six fused FFN calls per specialized request; F16 DeBERTa keeps 12
of each. Q4_K retains its quantized route and 12 FFN calls. Each Metal request
makes one final output download. Separate CPU process peak-RSS measurements range
from 1.3% lower to 0.34% higher across safetensors, GGUF, F16 and Q4_K workloads.
Full receipts, outputs, executable SHA256 values and frozen executables are kept
locally under `.benchmark-assets/1166/acceptance-retirement-final/` and the
earlier acceptance directories. Final host-ownership qualification is in
`acceptance-host-ownership-final/`. The final executable SHA256 is
`bab0189509937faa0dbffc47b84539e29768115df83f8e33000782ff639f38de`;
the baseline SHA256 is
`47bf7109c32f1d7c1e60633ed47f927c2f6ee6af2e7a3d54b67a67ec1e6405c8`.

ONNX/CPU cannot be timed against this baseline: it fails grouped source-backed
initializer concatenation. The corrected CPU path matches all 6,144 safetensors
values within `4.232e-6`; its separate diagnostic run records 164 assigned kernels,
zero warm payload allocations and 164 storage reuses. Generic ONNX execution
still has higher latency than the specialized architecture path; those existing
specialized paths remain selected for architecture-loaded checkpoints.

Cross-device F16 reranker logits differ by `3.577e-6` (tolerance `1e-5`). Q4_K
CPU/Metal accumulation differs by `0.002984` (tolerance `0.005`), exactly as on the
baseline. These are comparisons within one encoding, not assertions that Q4_K
and F16 models have identical numerical outputs.

### Prolonged qualification

The final persistent-session workloads alternate two distinct masked token inputs
and retain five resident results to exceed the four-frame pool. They compare each
held value against its own reference, enforce zero intermediate device readbacks,
check a memory plateau, and require zero tracked host allocations at teardown.
`--soak-allocator c` isolates library ownership from Zig SMP allocator free-list
slab retention; the default timing allocator stays unchanged. Process RSS is
sampled separately from tracked host/device payload allocations.
Stateful decoder workloads compare every prefill/decode logit, reset each prompt,
and evict/recreate the runtime every 256 iterations. Both run on CPU and Metal.

The 16-hour workloads started at 05:56 UTC on 2026-10-22 were stopped after
about one hour: an exit-time allocation probe found 4,320 leaked C host
allocations despite zero bounded-allocator bytes. These runs are superseded;
their receipts remain under `acceptance/pre-host-tensor-leak-soaks/`. Earlier
three-hour runs were also superseded after autorelease diagnostics found
retention outside payload counters; their receipts remain under
`acceptance/pre-autorelease-soaks/`.

Host materialization now requires a mutable tensor owner. Borrowed fallback
inputs use scoped read views that retain device storage and release private
host mirrors; existing caches are borrowed while their owner remains alive.
KV append suffixes are released after concatenation, and partial cache
replacement preserves the prior history. Fused decoder temporaries also have
cleanup on optional declines. Actual-Metal regressions cover these lifetimes.
The updated decoder's exit-time allocation probe reports zero leaks and zero
leaked bytes. Sustained runs additionally check host-mirror bytes against the
warmed baseline.

All four replacement workloads completed 57,600 seconds on the same frozen
executable, from 07:58 to 23:58 UTC on 2026-10-22. Each exited successfully,
reported zero tracked host allocations at teardown, and kept host-mirror bytes
at zero at every progress check. Alternating resident encoder outputs and every
decoder prefill/decode output retained their reference values throughout.

| Workload | Iterations | Runtime evictions | RSS range in final four hours (MiB) |
| --- | ---: | ---: | ---: |
| Encoder, CPU | 52,068 | — | 104.4–108.6 |
| Encoder, Metal | 51,785 | — | 39.8–40.0 |
| Decoder, CPU | 53,275 | 208 | 21.7–22.3 |
| Decoder, Metal | 49,982 | 195 | 65.6–68.3 |

Tracked host/device payload counts remained at their warmed baselines. The
controller recorded 1,045 positive RSS samples per process. RSS initially held
near 162/77 MiB for CPU/Metal encoders and 40/108 MiB for decoders, then dropped
across all processes later in the run. RSS measures page residency separately
from allocation ownership; the retained counters and teardown checks establish
ownership independently. The controller also verified frozen source and binary
hashes throughout.

Both final decoder dumps are byte-identical to their qualified smoke outputs.
The independent oracle passes all 160,000 logits per backend at tolerance
`0.002`, with maximum error below `0.000759`. Full logs, outputs, source hashes,
RSS samples and `prolonged-receipt.json` remain under
`.benchmark-assets/1166/acceptance-host-ownership-final/`.

The accelerated Metal encoder probe additionally completed 6,755 iterations
with zero tracked bytes at teardown; its allocation probe reported zero entries
and zero bytes in autorelease pools. The corrected decoder's separate exit-time
allocation probe reports zero leaks. CUDA construction, planning, descriptors
and capture ownership compile and pass host-side guards; CUDA numerical and
performance qualification still require a CUDA host.
