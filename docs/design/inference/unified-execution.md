# Unified execution (#1166)

The migration introduces common owners for model loading, semantic programs,
prepared execution, physical storage and model state. Existing optimized kernels
and whole-model implementations remain the selected implementations behind those
owners. It adds no required external inference runtime.

The implementation branch is based on `origin/main` revision
`ae0b8856fdc62c7c164125ade966d3ed0cd2dc8e`, refreshed through `origin/main`
`f95a5a5820` before the shared-recipe follow-on. Hardware qualification now covers
CPU/Metal on the M4 Max and CUDA on Glowworm's NVIDIA GB10. The kernel-consolidation
section records actual device, capture, lifetime and model performance checks,
including their model-admission limits and retained timing misses.

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
between execution and legality checks. CPU and CUDA consume the descriptors.
Metal caches descriptors in its prepared region plan and dispatches admitted
resident operations through them while retaining its region/command/arena
planner. Region fusions keep priority over individual kernels.

Descriptors describe semantics and requirements, rather than imposing one target
schedule. Native execution keeps SIMD, threading, BLAS, source-weight conversion
and packed quantized routes. CUDA keeps cuBLASLt, training GEMM and existing launch
selection. Planned native packed projections and CUDA mixed/packed projections
share the same implementation selectors as their allocating entry points.
Operator plans remain separate from semantic descriptors and are cached with
physical bindings. Invocations borrow the cached plan by pointer. Metal filters
static target legality during preparation and encodes already admitted device
views directly, without re-entering general tensor conversion/retention adapters. Unsupported layouts and specialized fusions retain their
qualified routes.

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

Auxiliary requirements declare typed representation or saved-result planes.
Preparation sizes and reuses each plane with its primary allocation's lifetime;
workspace remains separate. CUDA's fused F32/BF16 RMSNorm writes into a planned
mirror plane. Published tensors, dense row/reshape views and graph captures retain
that plane independently of the primary F32 storage. The generic protocol also
supports saved mean and inverse-standard-deviation planes without backend casts.

`graph/quant_recipe.zig` owns the scalar expression graph for twelve GGUF block
formats: Q4_0, Q4_1, Q5_0, Q5_1, Q8_0, Q8_1, Q8_K, Q2_K, Q3_K, Q4_K, Q5_K and
Q6_K. CPU full-block decoding specializes eight-lane SIMD groups at compile time;
partial tails use the same expressions. There is no runtime recipe interpreter.
Metal source lowering replaces twelve independent lane decoders. CUDA's generated
Q4_K kernels use the same recipe; host source conformance checks CUDA lowering
for all twelve formats. Q4_K header extraction and Q6_K scale/value components
let optimized Metal schedules hoist scales and reuse decoded weights across row
tiles without duplicating format formulas.

Twelve production CPU decoder bodies, twelve Metal lane decoder bodies and the
independent CUDA Q4_K unpacking helpers are retired in favor of shared recipes.
The frozen scalar codec under `gguf/testing/` is an independent test and benchmark
oracle and is not part of production decoding. Existing native dot/accumulation
kernels, GPU tile/reduction orders, CUDA DP4A/tensor-core helpers and specialized
attention/FFN schedules remain target implementations. Sharing mathematical
encoding rules does not impose a common machine schedule.

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
remains the explicit reference/compatibility path. Target schedules are retained
behind common semantic and storage boundaries; specialized schedules outside the
shared recipe vocabulary continue to require their existing qualification.

## Follow-on qualification commands

- `zig build bench-quant-recipes`: ReleaseFast comparison with the frozen scalar
  decoder, alternating arms and reporting median time per decoded value.
- `zig build quant-recipe-cuda-host-conformance`: compile generated CUDA scalar
  expressions as host C++ and compare 110,592 values with the independent codec.
  Includes negative, non-power-of-two, subnormal and zero scales. This checks
  source syntax and arithmetic; it is not CUDA device or performance evidence.
- `zig build quant-kernel-metal-runtime-conformance -Dmetal=true`: compile shaders
  through the actual Metal runtime and run all 50 conformance cases. This works
  with Command Line Tools installations lacking the offline Metal compiler.
  The existing offline artifact qualification gates are retained.
- `zig build test-inference-unit -Dmetal=true -Dcuda=true
  -Dtest-filter='Metal shared descriptors'`: real-device primitive, activation, binary,
  contraction, reduction, layout and softmax/log-softmax checks, repeated output
  reuse, input preservation and strict transfer admission.

Follow-on kernel qualification on the M4 Max:

- Twelve-format scalar/SIMD parity, including partial tails, passes against the
  frozen independent decoder with 64 packed patterns per format.
- CUDA host source conformance passes all 110,592 values.
- Metal device conformance passes all 50 cases; worst absolute error is
  `0.0000011`.
- The promoted Metal production regression gate passes all 14 routes, with five
  repeats and 500 measured iterations per case. All routes/providers are ready;
  no slow fallback is observed. Worst repeat speedup over the existing handwritten
  comparison kernels is `1.607x`.
- CPU SIMD block decoding takes `0.271–0.614x` the frozen scalar decoder's median
  time across the twelve formats. This measures decoding, not an inference or
  packed matmul speedup; the existing packed matmul schedules are preserved.

The final follow-on execution suite passes **386 tests with zero skips**. The
CPU/Metal/CUDA product check and browser artifact are also checked separately.
CUDA device numerical, capture and performance qualification still require a
CUDA host; host compilation and ownership tests do not establish those results.

### Follow-on model acceptance

Frozen ReleaseFast executable SHA256:
`60be8c48366d5f5b22341054a249f5c75f5749673ee35f62d7481c4a75d0de24`.
The original baseline below remains unchanged. All thirteen follow-on cases
produce byte-identical outputs to that baseline and meet the 5% p50 / 10% p95
limits. Worst ratios are `1.0322` and `1.0774` respectively. These are model
measurements, separate from the decoder throughput and shader gates above.

Ordinary cases use seven warmups, 41 measured requests and A/B/B/A ordering.
CPU MiniLM sequence 128, Metal Q4_K reranking and Metal ONNX use 25 warmups,
201 requests and eight A/B/B/A/A/B/B/A arms. CPU Q4_K uses 25 warmups and
1,001 requests across twelve A/B/B/A/B/A/A/B/A/B/B/A arms. Diagnostics are disabled
during timing. Longer balanced CPU sampling against both the original baseline
and the previous validated checkpoint independently confirms parity.

| Workload | Baseline p50 (ms) | Follow-on p50 (ms) | p95 ratio |
| --- | ---: | ---: | ---: |
| minilm-st-native-b1-s16 | 1.5005 | 1.4868 | 1.0774 |
| minilm-st-native-b1-s128 | 8.8043 | 8.7459 | 0.9941 |
| minilm-st-native-b2-s16 | 2.2469 | 2.2458 | 1.0040 |
| minilm-gguf-native-b1-s16 | 1.4337 | 1.4638 | 1.0116 |
| reranker-f16-native-b1-s16 | 79.3858 | 79.3124 | 1.0017 |
| reranker-q4k-native-b1-s16 | 11.6231 | 11.6560 | 1.0007 |
| minilm-st-metal-b1-s16 | 2.7093 | 2.3424 | 0.7999 |
| minilm-st-metal-b1-s128 | 3.1122 | 3.0970 | 0.9963 |
| minilm-st-metal-b2-s16 | 2.3929 | 2.3792 | 0.9997 |
| minilm-gguf-metal-b1-s16 | 2.3274 | 2.3540 | 1.0129 |
| reranker-f16-metal-b1-s16 | 53.0085 | 52.9190 | 0.9972 |
| reranker-q4k-metal-b1-s16 | 8.3293 | 8.3295 | 1.0003 |
| minilm-onnx-metal-b1-s16 | 7.6568 | 7.9031 | 1.0497 |

The independent tiny-Llama oracle passes every prefill/decode logit on CPU and
Metal at tolerance `0.002`. Follow-on receipts, full outputs and per-arm logs live
under the ignored `.benchmark-assets/1166/acceptance-shared-direct-binding-final/`
directory. Earlier checkpoint evidence below remains a separate qualification
of that executable; it is not substituted for follow-on results.

Four concurrent ten-minute follow-on stress runs pass on this same frozen
executable, with alternating encoder inputs, retained results exceeding the
four-frame limit, decoder cache eviction and output checks throughout. The Metal
encoder uses the ONNX fixture to exercise the shared descriptor path. Every run
reports zero tracked live host bytes at teardown and no denied allocation;
steady Metal device and host-mirror allocations remain bounded.

| Stress workload | Completed iterations | Runtime evictions |
| --- | ---: | ---: |
| encoder-native | 209,506 | — |
| encoder-metal | 45,193 | — |
| decoder-native | 949,906 | 3710 |
| decoder-metal | 16,257 | 63 |

## Kernel source consolidation after the shared-descriptor checkpoint

The next checkpoint builds on `3ee86b871d`. Common semantics now cover activation
expressions, packed-format components and explicit warp reduction trees as well
as physical descriptors. Target code retains vector widths, tile geometry,
register reuse, packed integer dot products and contraction policies. These are
compile-time templates or generated source; requests do not interpret recipes.

- `ops/kernel_math.zig` owns GELU, sigmoid, SiLU, QuickGELU, ReLU and ReLU-squared
  expressions. CPU scalar/SIMD adapters retain their selected exp/tanh routines
  and distinct GELU cubic multiplication order. GPU lowering retains precise
  versus fast CUDA exponential choice and the guarded packed-epilogue policy.
- `ops/kernel_reduction.zig` owns the explicit full-warp shuffle-down schedule
  `16,8,4,2,1`, sum/max choice and lane-zero/broadcast result contract. Five CUDA
  helper definitions share it without changing their reduction order.
- Packed recipes intern identical expressions without reassociation. Signed-byte
  loads, Q4_K integer headers and Q6_K integer/scale components serve both scalar
  decoding and optimized schedules, including CUDA DP4A packing. Metal Q4_K and
  Q6_K share their register-tile scaffold and retain format-specific scale/load
  hoisting.
- Native pair/triple Q8 and Q4/Q5_K panel kernels specialize a compile-time arity.
  Grouped raw/prepared/mixed dispatch also shares one resolver. The native compute
  file is 410 lines smaller including its new regression tests, which exercise
  both arities, mixed caches, bias, row tails, column prefixes/suffixes and
  untouched outputs. Existing production dispatch defaults remain in force.
- CUDA code generation owns one runtime region of 24 shared helpers and another
  of five promoted matmul bodies. Standalone qualification and runtime embedding
  now consume the same definitions. The five previously promoted standalone
  sources remain byte-identical to `3ee86b871d`; historical performance evidence
  and source fingerprints are not rewritten. Specialized attention, DP4A and
  tensor-core schedules remain target implementations.

The new `kernel-recipe-cuda-source` target generates a model-free NVIDIA test on
any development host. `kernel-recipe-cuda-conformance` compiles it with NVCC for
the installed GPU and runs packed formats, activation policies and full-warp
reductions. It always recompiles/runs because GPU identity is an input. Scalar
conformance disables contraction and subnormal flushing; production performance
qualification retains its normal flags. See `zig/pkg/inference/scripts/README.md`
for commands and the artifact regeneration contract.

### Consolidation qualification

On the M4 Max, the broad affected suite passes 208 tests with six explicit
CUDA/fixture skips; a final CUDA-host/recipe suite passes 122 tests with zero
skips. All 21 standalone code-generation tests pass. Generated host C++ checks
315,421 values against independent packed/activation references and the explicit
shuffle oracle. Actual Metal conformance passes all 50 cases, with worst absolute
error `0.0000011`. Product compilation with CPU/Metal/CUDA enabled and the WASM
artifact both pass.

The native packed-kernel gate covers ten geometries and three routes each,
using A/B/B/A/A/B/B/A arms, 25 warmups and 201 measured iterations. All 30 checksums
match and all cases meet the 5% budget; the worst candidate/baseline ratio is
`1.02411`. The candidate SHA256 is
`a0498ebeca7d301b4c3b6023212ea6cd39a01ab917f9a791b3ea9cbd39f77707`.
The baseline is the native compute source from `3ee86b871d`.

The promoted Metal production gate passes 14 routes with five repeats and 500
iterations per case, all providers ready and no slow fallback. Its worst repeat
speedup is `1.634x` over the existing handwritten comparison kernels; this is
separate from a comparison against the preceding checkpoint.

The final model executable SHA256 is
`ec34b775ae7efc7d392f5ea0ebeddbc11cdb2ddde38be17b68f96dbb6e12e2dd`.
Every complete output in the thirteen-case CPU/Metal matrix is byte-identical to
the original baseline. Ordinary cases use eight balanced A/B/B/A/A/B/B/A arms,
25 warmups and 201 requests; CPU Q4_K retains twelve arms and 1,001 requests.
Twelve cases pass immediately. Metal ONNX initially exceeds the p50 budget at
`1.05129` while passing p95. A preselected longer sixteen-arm balanced protocol
with 1,001 requests passes at p50 `1.02934`, p95 `1.02923` versus the original
baseline. The same longer protocol versus the preceding validated checkpoint
passes at `0.99796` / `0.97148`. All thirteen workloads therefore meet the 5%
p50 / 10% p95 limits under the documented protocols; neither short failed receipt
is discarded. The earlier four-arm series also had a Metal batch-2 timing spike,
resolved by the uniform eight-arm matrix.

Receipts, frozen binaries, source hashes and full outputs are under the ignored
`.benchmark-assets/1166/acceptance-kernel-consolidation/` directory:
`cpu-kernels/`, `models/`, `models-final/`, `onnx-prior-checkpoint/` and
`onnx-original-extended/`. Source-only CUDA changes after CPU qualification do
not change the qualified native arithmetic or packed-kernel schedules.

The final independent decoder oracle passes all 160,000 logits per backend at
`tolerance=0.002`. Four concurrent 120-second regression soaks also pass: native
encoder 41,969 iterations, Metal ONNX encoder 9,252, native decoder 187,694
(733 runtime evictions), and Metal decoder 3,391 (13 evictions). Every run checks
output stability and reports zero tracked live bytes at teardown, zero live host
mirrors, and no denied allocations. Decoder reset/chunked-prefill parity is exact.
These shorter kernel-consolidation regressions supplement the earlier prolonged
ownership qualification; they do not replace its receipts.

CUDA validation now uses Glowworm's NVIDIA GB10 (SM121), driver `580.178.04`.
The model-free test passes all 217,100 values with both the installed CUDA 13.0
and an isolated CUDA 13.2.86 toolchain. The pinned Zig 0.17.0 archive is verified
against the repository's SHA256 before use. System toolkit/driver configuration
is unchanged. Both baseline and candidate production PTX, seven-architecture
fatbin and SM89 cubin are regenerated with CUDA 13.2.86; candidate images are
copied back with SHA256 verification.

A single descriptor fixture now exercises admitted operations on CPU, Metal and
CUDA, with target-owned tensor setup. The device adapters verify strict transfer
admission, repeated assigned output reuse, null-output queries and unchanged
inputs; CPU and CUDA use physical leases and CUDA declares transpose workspace.
The two local CPU/Metal fixture tests pass. Fifteen selected CUDA tests pass with
zero skips, including actual retained graph replay with fresh request inputs,
cached BF16 GEMM replay, activation forward/backward parity, physical ownership,
auxiliary planes, and OOM reservation recovery. These are hardware checks;
NVIDIA model performance qualification uses the exact `3ee86b871d` source
snapshot as baseline, with both CUDA images rebuilt by the same compiler. All
four admitted cases (MiniLM batch 1 / sequence 16, batch 1 / sequence 128, batch 2 /
sequence 16, and Q4_K DeBERTa reranking) produce byte-identical complete outputs.
The initial eight-arm, 25-warmup, 201-request series passes three cases, with
sequence 128 missing p95 at `1.13005` despite p50 `0.99859`.

The retained longer series uses sixteen `ABBABAABABBAABBA` arms, 250 warmups and
1,001 requests per arm for every case. Both executables use the same 3.9-GHz CPU
core pool (`5–9,15–19`) on the hybrid-core Spark; GPU clocks and system scheduling
settings are unchanged. All four cases meet the original 5% p50 / 10% p95 limits.
Worst ratios are `1.00206` and `1.00025`. This controls host scheduling during GPU
wall-clock timing and does not establish default-scheduler tail latency for every
load. Baseline/candidate executable SHA256 values are
`ade00d651f8d916603c2fa10f83516b0ebb0730d649610b7a65ef5160c29db52` and
`ee30ab719c3838ee0d3e29d7f832a21f76f272af695eb1de594e8dd65dc19769`.

Cross-device full-output checks also pass: maximum absolute CPU/CUDA error is
`0.000003577` across the three MiniLM shapes and `0.000156284` for Q4_K DeBERTa,
within the declared `0.0001` and `0.005` tolerances for those encodings. These
comparisons supplement the exact baseline/candidate comparisons on each backend.

The tiny-Llama fixture is rejected by the existing CUDA architecture-admission
policy on both checkpoints before device execution. Its decoder oracle and
prolonged runtime qualification remain CPU/Metal evidence; this change does not
expand CUDA model admission. CUDA captures are exercised by the actual retained
replay and BF16 GEMM device tests above. GB10 measurements do not establish
runtime performance on SM89 or other GPU architectures.

The five promoted CUDA matmul bodies also retain identical generated machine
instructions and resource usage versus the same-compiler baseline on all seven
fatbin architectures (SM75, SM80, SM89, SM90, SM100, SM110 and SM120). This is
35 static comparisons, separate from actual GB10 runtime qualification.

The final CUDA descriptor and prepared-frame regressions pass on hardware,
including six retained outputs against the four-frame pool, reuse after a held
output is released, and exact output survival after executable eviction, under
strict device-host transfer auditing. Two concurrent 120-second strict-resident
model soaks also pass: MiniLM completes 41,086 iterations and Q4_K DeBERTa 10,184,
with alternating inputs, held outputs and zero tracked host bytes at teardown.
Process RSS is constant throughout the sampled final minute: 300,544 KiB and
710,924 KiB respectively. The benchmark's legacy device/mirror counters describe
Metal providers; CUDA zeroes there are not device-allocation plateau evidence.
Actual CUDA allocation/capture ownership is covered by the device tests.

Only test fixtures and CUDA-only images changed after the CPU/Metal frozen model
binary was qualified. Retained prefix-hash checks establish that CPU/Metal
production code is unchanged by those later additions. Detailed hardware logs,
source/artifact hashes and both short and longer CUDA series are retained in
`.benchmark-assets/1166/acceptance-kernel-consolidation-cuda/`, beside the CPU/Metal
receipts. Frozen executables and the 35 machine-code/resource comparisons are
included. The CPU/Metal directory also retains the exact protocol scripts and
post-qualification production-prefix checks.

## GPU kernel family consolidation

The next checkpoint consolidates maintained kernel bodies relative to
`f79d38374f4984c08d63384e1cc8639345350b69`. Native Metal/CUDA compile-time
specializations now share normalization, dense projection/FFN and duplicated
attention schedules. There is no runtime recipe interpreter or new source
emitter. Exported kernel interfaces, dispatch choices and model admission remain
unchanged. CPU production code is unchanged in this checkpoint.

Thirty-six Metal entry points share family implementations: eight LayerNorm
variants, six RMSNorm variants, twelve typed scalar/32-lane dense projections,
seven tiled projection/activation variants, and three q16 causal attention
variants. Thirteen CUDA entry points share implementations: seven RMSNorm,
two LayerNorm, pair/triple biased projection, and Q8/Q16 HD64 BERT prefill.

Compile-time policies retain scalar centered-variance versus parallel
sum/squared-sum normalization, residual/scale order and saved-sum behavior;
F32/F16/BF16 conversion; accumulator and tile dimensions; and attention indexing,
barrier order and full-tile versus ragged-tail staging. CUDA grouped projection
arity is fully unrolled. Metal attention retains direct half tile loads while
F32 retains its staging schedule. The distinct fast schedules remain explicit
specializations of these shared bodies.

The following counts include all added helpers and wrappers. The Metal file
includes host code; these are whole-file counts, not shader-only estimates.
Generated PTX changes are excluded from the maintained-source reduction.

| Maintained file | Baseline lines | Candidate lines | Lines removed | Bytes removed |
| --- | ---: | ---: | ---: | ---: |
| `backends/metal_kernels.m` | 70,937 | 70,545 | 392 | 30,132 |
| `ops/cuda/artifacts/inference_cuda_kernels.cu` | 25,186 | 24,910 | 276 | 9,452 |
| Total | 96,123 | 95,455 | **668** | **39,584** |

This is a reduction of repeated family bodies, not a wholesale collapse of the
GPU backend. Other quantized, tensor-core and architecture-specific schedules
still have separate implementations where their dataflow differs.

### Qualification of the consolidated families

Receipts, exact harness/protocol sources, frozen Metal executable, outputs and
artifact hashes are retained under the ignored
`.benchmark-assets/1166/kernel-families/` directory. CUDA's frozen executable
and full disassemblies also remain on Glowworm under
`~/antfly/.benchmark-assets/1166/kernel-families/`. The baseline CUDA artifacts
there were frozen from the qualified `f79d38374f` production snapshot before
regeneration. The private CUDA 13.2.86 toolchain regenerated PTX, the seven-target
fatbin and SM89 cubin; system CUDA was unchanged. All four local CUDA
source/artifact SHA256 values match the qualified remote snapshot.

- Metal's affected normalization, linear and dense causal-attention suite:
  150 passed, four unavailable-fixture skips (154 selected).
- Direct Metal baseline/candidate comparisons: all 36 changed entry points at
  edge and representative shapes, 72 cases total, byte-identical output. Fixed
  buffers, 100 warmups and eight ABBAABBA arms of 300 dispatches use GPU command
  timestamps. Every case passes the 5% budget; worst ratio is 1.03170. No pipeline
  loses maximum threads per threadgroup.
- Same-compiler CUDA disassembly/resource comparison: 439 functions on each of
  SM75, SM80, SM89, SM90, SM100, SM110 and SM120. All eleven refactored
  normalization/attention kernels retain identical instructions and resources
  on all seven targets (77 comparisons). Only pair/triple projection differs:
  register, shared-memory, local-memory and stack usage is unchanged, with the
  same or fewer instructions. Thus 3,059 of 3,073 whole-image comparisons are
  unchanged. On SM120 pair/triple instruction counts fall 376 to 368 and 504 to
  488; register counts remain 40/48, with zero local memory and stack.
- CUDA grouped projection: both arities at four edge/representative shapes,
  eight cases total, byte-identical to baseline. Independent F64 reference
  maximum error is 2.15e-7. CUDA graphs use 100 nodes, 40 alternating warm graph
  launches, then eight ABBAABBA arms of 500 kernels each. All cases improve by
  approximately 3.5–5.4% on GB10.
- Actual CUDA shared-descriptor and prepared-frame ownership regressions:
  two passed, zero skips, including retained outputs, pool pressure and eviction.
- Tiny-Llama Metal independent decoder oracle: all 160,000 logits pass at
  tolerance 0.002; maximum error is 0.00075894.
- Concurrent 120-second strict-resident Metal soaks: encoder completes 10,036
  iterations; decoder completes 3,402 iterations with 13 runtime evictions,
  exact reset parity and exact chunked-prefill parity. Both finish with zero
  tracked host bytes and no allocation denial.

All seven Metal model cases pass byte-identical output checks and the existing
5% median / 10% p95 budgets: MiniLM safetensors at batch1/sequence16,
batch1/sequence128 and batch2/sequence16; GGUF MiniLM; F16 and Q4_K DeBERTa; and
ONNX MiniLM. Eight ABBAABBA arms use 25 warmups and 201 measured requests. Worst
median/p95 ratios are 1.00955/1.01510. Baseline executable SHA256 is
`ec34b775ae7efc7d392f5ea0ebeddbc11cdb2ddde38be17b68f96dbb6e12e2dd`
(the previously qualified production-equivalent Metal snapshot);
candidate SHA256 is
`6c34b46fac4faa7e6914d2ce82ec0c6eb1a4fdd0649368db027cf5db971da709`.
The later CUDA image integration does not alter that qualified Metal shader.

On Glowworm's GB10 (SM121, driver 580.178.04), all four CUDA model cases pass
byte-identical output and the same budgets: MiniLM batch1/sequence16,
batch1/sequence128, batch2/sequence16 and Q4_K DeBERTa. Sixteen balanced arms
`ABBABAABABBAABBA` use 250 warmups and 1,001 measured requests, pinned to CPU cores
5–9 and 15–19. Median ratios are 0.99812, 1.00013, 0.92779 and 1.00068;
p95 ratios are 1.04465, 1.00006, 1.00953 and 1.00050. Baseline executable SHA256 is
`ee30ab719c3838ee0d3e29d7f832a21f76f272af695eb1de594e8dd65dc19769`;
candidate SHA256 is
`49885eade6fc58ae12760bd1d80a0d890780123dcc4a1dde586bf3b97f8923a8`.
Final CPU/Metal/CUDA product compilation and the WASM build pass. Generated
source freshness (67 files plus Metal/CUDA runtime regions), CUDA source policy
and whitespace checks pass; required cache collection completed on both hosts.

These are GB10 runtime results; the seven-target disassembly checks establish
static specialization/resource parity, not hardware throughput on other GPUs.

## Segmented and grouped Metal SDPA consolidation

Building on `b4b34bd03f1181af4ea9e15a448872a4e8001afd`, the three eight-key
block SDPA entry points now use one `termite_sdpa_f32_blocks_impl<Grouped, DM>`
schedule. Compile-time grouping selects KV-head mapping and caller-provided
score scale; staging stride specializes to 256 or 512. Each entry point retains
its original static threadgroup arrays. Online-softmax order, simdgroup matrix
operations, barriers, disjoint/overlapping range union, position/window checks,
empty-row behavior, ragged loads and 256/512-thread launch choices are preserved.
The separate sixteen-key schedule remains its own implementation. Host dispatch,
CPU and CUDA production code are unchanged.

This removes another **155 maintained lines / 13,038 bytes**, including all new
helpers and wrappers. `metal_kernels.m` falls from 70,545 to 70,390 lines. Combined
with the preceding GPU-family checkpoint, the reduction is **823 lines / 52,622
bytes** relative to `f79d38374f`; generated artifacts are excluded.

The retained direct harness in `.benchmark-assets/1166/sdpa-families/` compiles the
baseline and candidate full shader libraries with production `MTLMathModeSafe`.
Twenty cases cover every changed entry point, dimensions 256/512, queries
7/33/129, keys 19/257, one/two/four KV heads, both admitted threadgroup widths,
custom scales, negative positions, local/unbounded windows, overlapping ranges,
holes, empty rows and guarded output tails. All outputs are byte-identical to
baseline and pass an independent F64 reference (maximum error 1.34e-7).
Static threadgroup storage and maximum supported threads are unchanged.

Fixed-buffer GPU timestamp measurements use 100 warmup dispatches followed by
eight ABBAABBA arms of 300 dispatches. All twenty cases pass the 5% budget; the
worst ratio is 1.00241 (+0.24%). These are direct family measurements rather than
an unrelated model benchmark whose dispatch might bypass these kernels. Full
source snapshots, exact harness, output/resource/timing receipt and source hashes
are retained alongside the manifest. Product compilation and generated-source
freshness (67 files plus Metal/CUDA runtime regions) pass. The existing grouped
attention, segmented-audio and parallel local-attention reference suite passes
all four selected tests with no skips, covering admitted fallback, F64 masks and
scale, short/fused tails and invalid-key semantics.

## Typed Metal scans and CUDA rotary family consolidation

Building on `c56aeb5f727bce2a789fcb8c3642299d941e2fcf`, six Metal cumulative-sum
entry points share `termite_cumulative_sum_impl<T>`. Accumulator, prefix scratch
and output all retain the original F32 or unsigned 8/16/32/64-bit type; signed
integer entry points retain their bitwise wrapping representation. The
256-thread chunk schedule, prefix tree, barriers, strided indexing and
reverse/exclusive behavior remain unchanged. Static scratch stays in each
entry-point wrapper.

Four CUDA plain/scaled RoPE kernels share a compile-time scaled/decode family;
the fused RMSNorm+RoPE pair shares a second family. Plain rotary decode keeps
its optional device-scalar load before bounds checking, while fused rotary
normalization keeps that load after its original guard. Unscaled kernels do
not introduce a multiplication by one. Pair layout, partial rotary dimensions,
frequency arithmetic, reduction tree and scale placement remain unchanged.
Per-item and multimodal rotary schedules remain separate. There is no host
interface, dispatch or CPU production change.

Including every new helper and wrapper, this removes **105 maintained lines /
7,575 bytes**: Metal removes 85 lines / 6,630 bytes and CUDA removes 20 lines /
945 bytes. The three successive GPU-family checkpoints therefore remove
**928 maintained lines / 60,197 bytes** relative to `f79d38374f`. Generated
artifact churn is excluded from these counts.

Qualification receipts and exact protocols are retained under the ignored
`.benchmark-assets/1166/scan-rope-families/` directory, with baseline/candidate
source hashes and full Metal shader snapshots. CUDA full disassemblies and
baseline images also remain on Glowworm in the corresponding directory.

- All four existing Metal scan/reference tests pass without skips, including
  graph constants across all integer widths and dynamic reverse/exclusive axes.
- All 168 direct Metal cases are byte-identical to baseline and an independent
  sequential reference. They cover all six entry points, widths
  0/1/255/256/257/513/4097, both reverse/exclusive flags, two strided batches,
  wrapping integer patterns and guarded output tails. F32 reference inputs are
  exactly representable dyadic values. Production safe-math mode is used;
  static threadgroup storage and maximum supported threads are unchanged.
- The first eight-arm scan timing run had one failure among 48 timed cases
  (F32 forward exclusive, width 513, ratio 1.30217). The complete matrix was
  investigated with 250 warmups and sixteen balanced
  `ABBABAABABBAABBA` arms of 1,000 dispatches each, retaining per-arm timings.
  All 48 cases pass that longer protocol: worst ratio 1.00261 (+0.26%); the
  initially failing case measures 0.99993. Both initial and longer receipts
  are retained; the source did not change between them.
- All 120 direct CUDA cases on GB10 pass byte-identical output and independent
  F64 references (maximum error 5.09e-6). Coverage includes split/consecutive
  pairs, odd and partial dimensions, empty launches, null device scalars,
  scaling and fused normalization. Captured graphs observe device position
  changes without recapture, with guarded output tails preserved.
- CUDA 13.2.86 regeneration passes for portable PTX, the seven-target fatbin and
  SM89 cubin. All 439 kernels retain identical disassembled instructions and
  resource usage on SM75/80/89/90/100/110/120: **3,073 unchanged comparisons**,
  including all six refactored entry points. This is static specialization
  evidence; actual device execution was on GB10 SM121 with driver 580.178.04.
  No new cross-architecture hardware throughput claim is made.

Final CPU/Metal/CUDA product compilation, generated-source freshness (67 files
plus Metal/CUDA runtime regions), CUDA source policy and whitespace checks pass.
All four local CUDA source/image hashes match the qualified remote snapshot;
the SM89 cubin remains byte-identical. Required cache collection completed on
both hosts.

## Metal GELU backward family consolidation

Building on `d730ca8412c94a6d288225cf206eb35b2742c761`, standalone GELU backward
and four rank-one fused FFN backward entry points share one derivative body.
A compile-time upstream type preserves lazy device loads for standalone
nonfinite/saturated-negative paths and passes an already computed value in
fused paths. Three compile-time policies select a precomputed second branch and
whether second/upstream intermediates are saved. Kernel interfaces and host
dispatch remain unchanged; CPU and CUDA production code are unchanged.

The schedule retains stores before reading the GELU input and preserves the
original expression boundaries: a saved second branch is rounded before
addition, while the output-only branch keeps its single multiply/add expression
and permitted FMA contraction. An initial direct comparison caught rounding
changes from splitting that expression into a temporary; the final full matrix
passes after restoring the expression. NaN/Inf handling, finite-derivative
checks, tanh saturation returns and exact-GELU approximation remain unchanged.

Including all helpers and wrappers, this removes **60 maintained lines / 2,815
bytes**. `metal_kernels.m` falls from 70,305 to 70,245 lines. Across the four GPU
family checkpoints, the reduction is **988 lines / 63,012 bytes** relative to
`f79d38374f`; generated artifacts are excluded.

The exact harness, production-safe-math full shader snapshots, source hashes,
initial expression-failure receipt and final results are retained under ignored
`.benchmark-assets/1166/gelu-backward-families/`. All **80 direct cases** are
byte-identical to baseline, including every emitted branch intermediate and
output guards. They cover all five entry points, exact/tanh variants, empty and
ragged shapes through 64x1024, NaN/Inf upstreams and inputs, signed zero,
saturation and extreme finite inputs. Independent F64 analytic references check
finite upstream arithmetic (maximum absolute error 1.15e-6); nonfinite upstream
arithmetic uses baseline bits because rounding a tiny derivative to zero can
change an Inf product into NaN.

All **40 timing cases** pass the 5% budget using 250 warmups and sixteen balanced
`ABBABAABABBAABBA` arms of 1,000 dispatches. Worst ratio is 1.03900 (+3.90%);
static threadgroup storage and maximum supported threads are unchanged.

All six selected integration tests pass with zero skips after the final precision
fix: native activation parity, retained Metal training-session bindings through
backward, interpreter backward reference and rank-adapter backward paths. Final
CPU/Metal/CUDA product compilation, generated-source freshness (67 files plus
Metal/CUDA runtime regions) and whitespace checks pass.

## Original checkpoint qualification

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
