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

## Bulk consolidation campaign

Status: complete. All 24 packages have been implemented or retain documented
tuned specializations after schedule/resource comparison. Integrated numerical,
model and lifetime qualification is complete; environment limits are recorded
below.

Baseline: `dd1ee1b4ffd1a4a3a14878c648355b18a4096b8f`. Treat the following as one
implementation campaign with four work waves and one integrated acceptance
checkpoint, rather than another sequence of single-family requests. Keep the
existing `.worktrees/inference-unified-execution` branch and preserve its prior
qualified work; do not rebase the benchmark baseline during the campaign.

### Scope and accounting

The working target is **1,500–3,000 additional net authored production lines /
80–160 KB removed**, after counting all new helpers, templates, renderer code
and production descriptor metadata. This is a planning estimate, not a measured
saving or a reason to force incompatible schedules together. A separate target
is **1,000–2,000 fewer emitted CUDA bundle lines** by emitting already-shared
attention templates once. Do not add that generated-text reduction to authored
implementation savings. Report test/qualification infrastructure and total
repository change separately as well.

The source inventory is retained in ignored
`.benchmark-assets/1166/consolidation-plan/`. It identifies 13 Metal Q4_0/Q8_0
matrix entry bodies, 50 quantized row/projection candidates, 45 memory/indexing
candidates, 133 CUDA quantized linear candidates and 29 grouped-projection/FFN
candidates. These sets overlap and include thin wrappers or generated bodies;
they are candidate sets, not counts of independent duplicated implementations.
CPU triage covers prepared Q4_K/Q6_K workers, grouped projection adapters and
prepared tile ranges. In particular, the Q4_K/Q6_K parallel dispatchers have
matching row/column job construction, and Metal's handwritten/generated paged
attention still has separate authored bodies. Those are concrete ownership
opportunities. Repeated generated CUDA attention text already has shared
renderer/template owners and belongs in the generated-bundle accounting bucket.

### Work packages

Each package must identify its actual common schedule before editing. Keep
already-shared wrappers as wrappers, and do not replace a tuned inner loop merely
to make its syntax resemble another one.

| ID | Work package | Intended long-term shape and preserved specialization |
| --- | --- | --- |
| 01 | Baseline, source-owner ledger and precision contracts | Map entry points to authored owners and generated outputs. Extend existing schedule/lowering metadata only where needed to represent separate-product rounding, contractible multiply/add expressions, reduction order, scale/load placement, storage widths and tail policies. Keep mathematical identity in `kernel_descriptor.zig` separate from target schedule policy. |
| 02 | Reusable family qualification runner | Consolidate the existing direct harness protocols into reusable CPU/Metal/CUDA runners and fixture manifests. Cover entry points directly, retain raw timing arms and source/image hashes, and run the final model matrix against one frozen campaign baseline. |
| 03 | Metal Q4_0/Q8_0 matrix core | Share matching `linear_mm_sg` staging/MMA/store schedules with compile-time format loaders, preserving dequantization order, swizzles, tile geometry and explicit unrolling. |
| 04 | Metal Q8_0 v2/m64 matrix variants | Share each matching tile family across F32/F16 input and full/tail stores; retain distinct m32/m64 schedules and exact register-array sizes. |
| 05 | Metal K-family high-row matrices | Compare Florence Q4_K m64/n64 and Q6_K high-row matrices; share their matching staging and epilogue scaffold while retaining format-specific decode/load placement. |
| 06 | Metal paired quantized projections | Specialize projection arity, gate/up activation, RMS scaling and output representation for matching MMV schedules; preserve shared input reuse and separately rounded outputs. |
| 07 | Metal remaining quantized row projections | Fold repeated scalar/SIMD row and epilogue scaffolds into format/tile policies, reusing existing packed recipes. Retain different reduction trees and prepared-weight layouts as separate schedules. |
| 08 | CUDA Q4_0×Q8_1 multirow tiles | Share the matching tile4/w8 rows2/rows4 schedule with exact compile-time row count, staging extents and DP4A lane packing. Do not enlarge single-row accumulators to the maximum shape. |
| 09 | CUDA quantized paired/triple projections | Consolidate remaining handwritten Q4_0/Q8_0/K-family grouped schedules where dataflow matches, with compile-time arity and epilogues. Existing tensor-core and ordinary dot schedules remain distinct. |
| 10 | CUDA gated-down and activation-slice variants | Separate compile-time operand acquisition from common matching dot/reduction/store schedules. Preserve gate/up load hoisting, activation rounding and output addressing. |
| 11 | CUDA typed dense load variants | Unify F16/BF16 weight-load variants with the same launch topology. Include F32 only where its actual schedule matches; retain its different tiled algorithm otherwise. |
| 12 | CUDA plain/gated Q8_1 quantization | Share the matching row quantization body after compile-time source acquisition, including amax reduction, nearest-even conversion, clamp range and packed header bytes. |
| 13 | One owner for Metal paged attention bodies | Make runtime embedding and standalone renderer qualification consume one authored decode/prefill implementation. Preserve production ABI wrappers, parameter layout, admitted schedules and plan identities; avoid source moves being reported as duplication deletion. |
| 14 | Compact CUDA attention runtime emission | Emit existing split-K online and flash-prefill common template definitions once, then instantiate HD/window variants. Also compact matching rendered split-count variants where possible. Keep standalone artifacts self-contained and manifests/source checks fresh. This is primarily generated-bundle reduction. |
| 15 | Metal vision q32 full/tail attention | Share the observed vision HD64 flash q32 pair through compile-time full/tail policy, preserving full-tile direct loads and ragged staging. |
| 16 | Metal eight-query dense/paged attention | Compare the matching simdgroup MMA/online-softmax loops; use compile-time KV addressing/masking only if barriers, reduction and staging truly match. Otherwise share smaller identical components and retain distinct loop bodies. Depends on 13. |
| 17 | Metal remaining norm/clamp families | Share matching SIMD4 RMS/clamp reduction bodies with compile-time load/epilogue policies. Preserve variance formulas, clamp placement, epsilon, finite handling and saved intermediates; exclude the already-consolidated norm wrappers. |
| 18 | Metal argmax and sampling partial reductions | Reuse value/index combination and partial-store scaffolds where schedules match. Preserve lowest-index ties, NaN/Inf policy, suppression, seeded RNG and top-k ordering. Keep top-k and nucleus algorithms distinct. |
| 19 | Metal typed memory/indexing families | Share typed gather/copy/cast and packed get/set-row loops with format traits. Preserve 64-bit integer bits, saturating versus wrapping conversions, quantization headers, offsets and overlapping/duplicate-index behavior. Parallel scatter races are not made legal by consolidation. |
| 20 | CUDA typed memory/embedding families | Apply the same ownership boundary to matching embedding/gather/copy bodies, retaining vector width, alignment, tail checks and BF16 mirror semantics. Reuse format conversion owners rather than adding another conversion vocabulary. |
| 21 | CPU Q4_K/Q6_K prepared worker dispatch | Share worker contexts, typed entry adapters and row/column job construction using comptime format policy. Preserve scheduling thresholds, chunk boundaries, errors and thread-pool behavior. |
| 22 | CPU grouped projection adapters | Unify matching pair/triple dequantize-SGEMM and projection-result construction through comptime arity. Preserve prepared-cache identity, bias, partial failure cleanup and grouped input reuse. |
| 23 | CPU prepared quantized tile scaffolds | Share matching prepared tile/range loops and boundary handling across formats; retain format-specific SIMD decode, panel layout, accumulator width and summation order. Reuse existing generic panel kernels rather than reintroducing them. |
| 24 | Retire superseded bodies and qualify the composition | Delete replaced implementations and unused helpers, check every entry point has one intended owner, regenerate once per completed work wave, and run full integrated numerical/model/lifetime gates. Report authored, generated, binary and infrastructure changes independently. |

### Foundation and broad-family checkpoint (2026-10-23)

Packages 01–02, 11–12, 17–21 and the dequantize-SGEMM part of 22 now have a
qualified implementation. The mathematical descriptor remains separate from
native schedule policy; the helpers encode actual storage, arity, reduction and
addressing choices at compile time. No unused descriptor fields or new kernel
language were introduced.

- CPU shares Q4_K/Q6_K prepared-worker contexts/job construction and grouped
  dequantize-SGEMM acquisition, cleanup and dispatch for one/two/three outputs.
- Metal shares SIMD4 RMS/clamp, five argmax/sampling partial-tree entry points,
  and packed read/copy loops for nine formats: 25 entry points in total.
- CUDA shares F16/BF16 dense load variants, plain/gated Q8_1 quantization and
  dense/quantized embeddings with I32/I64 indices: 18 entry points.
- Reusable fixture-driven Metal/CUDA direct runners, a balanced model runner,
  seven-architecture CUDA instruction/resource reports and a source-owner
  ledger replace cohort-specific qualification executables. Six failure-gate
  tests cover the model runner.

Against frozen production baseline `dd1ee1b4ff`, this cohort removes **196 net
authored production lines / 14,524 bytes**, including its extra inline native
coverage. Qualification scripts/tests/documentation add **550 lines** separately;
generated production lines do not contribute to the authored saving. CUDA PTX
text changes without instruction/resource changes; the fatbin grows 16 bytes,
and the SM89 cubin is byte-identical. Source deletion is not a binary-size claim.

All 439 CUDA functions have identical instructions and resources on each of
SM75/80/89/90/100/110/120. All 48 direct captured CUDA cases pass exact outputs,
guards and the 5% timing gate on Glowworm GB10 (worst ratio 1.027802). All 63
Metal direct cases pass exact outputs, guards, pipeline limits, static scratch
and timing (worst ratio 1.038841 in the longer whole-cohort series). Native
prepared/SGEMM reference tests, Metal packed-row/RMS/sampling tests, product
checks and generated-source freshness pass.

All six CPU model cases have finite, byte-identical complete outputs and pass
5% median / 10% p95 ceilings in the final balanced whole-cohort series:

| CPU model | p50 ratio | p95 ratio |
| --- | ---: | ---: |
| MiniLM F32, b1/s16 | 0.998997 | 1.001543 |
| MiniLM F32, b1/s128 | 1.003679 | 1.029130 |
| MiniLM F32, b2/s16 | 1.000850 | 1.000861 |
| MiniLM GGUF, b1/s16 | 0.997080 | 1.021316 |
| Reranker F16 | 0.979493 | 0.965380 |
| Reranker Q4_K | 1.003329 | 1.004721 |

The first CPU series exceeded Q4_K p95; a longer series resolved that case but
exceeded F16 median. Both failures remain recorded. The final whole-cohort
protocol uses 32 balanced arms, 25 warmups, 201 measurements for ordinary cases
and 1,001 for Q4_K, alternating executables more frequently while preserving the
frozen binaries. Earlier Metal timing failures likewise remain alongside its
longer complete-cohort result. These are cohort gates; the full final
CPU/Metal/CUDA model and lifetime checkpoint is still required.

Ignored raw receipts, fixtures, source/image hashes and frozen executables are
under `.benchmark-assets/1166/bulk/`: `metal-wave1/long-receipt.json`,
`receipts/wave1-static/cuda-static.json`, `receipts/wave1/cuda-direct.json`, and
`receipts/wave1/cpu-models-interleaved/receipt.json`. Local model executable SHA256
is `a0f8cb070791c8b4e9d37292553ea12dac01f1a29cf63239010f67b2c21891b7`;
baseline SHA256 is
`8c1cff7bf5f0a17dedd90f2f98a50594034237775a9e166f094e1342d30140fa`.

### Compute and attention implementation audit

The compute and attention changes are implemented and qualified against the
original campaign baseline. This table records the ownership decision for each compute and
attention package; preserving an existing specialization is deliberate where
its arithmetic, storage or measured compiler resources differ.

| Packages | Implemented owner or retained specialization |
| --- | --- |
| 03–04 | Metal generic Q4_0/Q8_0 MMA uses format-typed decoding; Q8 m32 and m64 each share F32/F16 input and full/tail policy. The two tile geometries and explicitly unrolled Q4 schedule remain distinct. |
| 05 | One Metal 64×64×32 K-family scaffold retains Q4_K half staging, Q6_K float staging, each format's load placement and the F16-input wrapper. |
| 06–07 | Paired activation and RHS projections share output-width policies with exact accumulator extents. The matching Q4 row schedules share compile-time row count and simdgroup count; the historical `r8` entry still means five rows. Existing shared MMV arity policies remain in place; RMS-scaled and differently reduced schedules stay separate. |
| 08–10 | CUDA shares packed four/eight-row dot tiles, flat grouped versus ordinary coordinate acquisition, matching paired Q8-input schedules and gated-down column/warp policies. Packed rows2 keeps its lower-register specialization. The two F32 activation-slice exports retain their epilogue context: moving activation into a shared dot helper raised older-target registers, including unrelated libdevice callers. Restoring only those two contexts removes all resource increases. |
| 13 | Runtime and standalone Metal decode use one renderer with explicit ABI and masked-mass policy; the legacy post-max barrier and exponent behavior remain. Generic paged prefill has one native MSL template consumed by the runtime generator and standalone compatibility renderer. The low-memory HD256/512 page-contiguous schedules remain separate. The template migration is an ownership change, fully counted as authored source. |
| 14 | CUDA runtime emission owns one flash-prefill common definition and one split-K online common definition, with per-policy ABI wrappers. Standalone outputs remain self-contained. Export names, plan identities, default routing and cross-CTA volatile/publication ordering are preserved. |
| 15–16 | Vision q32 shares typed F16/F32 staging and preserves each tail-output scratch policy. Dense eight-query and generic paged attention retain different KV addressing, staging, masks and barrier schedules; they are not forced into a common loop. |
| 19–20 | Existing typed casts/gathers remain shared. Packed row encoders with different arithmetic remain format-specific. CUDA primitive F32/BF16 gather now shares indexing and the same storage-conversion owner used by dense embedding. Integer-index width and invalid/negative-index behavior remain unchanged. |
| 22–23 | CPU grouped output construction transfers one buffer at a time into shaped handles with separate cleanup for untransferred buffers and completed handles. Allocation-failure coverage exercises every partial state for pairs and triples. Q4_K/Q6_K share MR1/MR2 traversal and range boundaries while retaining SIMD decode/accumulate leaves and reduction order. Existing panel kernels remain the owners of their distinct layouts and arithmetic. |
| 24 | Superseded bodies and unused activation-slice policies are removed. Handwritten CUDA entry points also share 14 exact launch parameter lists across 110 explicit symbols; native CUDA bodies and generated ABIs remain separately owned. Final composition results and qualification limits are recorded below. |

The launch-declaration consolidation removes **972 lines / 18,872 bytes**, after
counting the entire new header. This is declaration duplication, reported
separately from compute-body savings. Before/after CUDA preprocessing produces
identical 554,535-token streams (SHA256
`2b4cac34df2be82468a42b7ef3ed3f493129a06d6c8c023757536f303487d594`).
There is no runtime interpretation or new kernel language.

The final Metal direct cohort qualifies 182 distinct matrix, attention, paired
and row cases in both math modes. Eight m64-tail fixture allocations were
corrected from 8 KiB to the production 16 KiB; the earlier undersized fixtures
are superseded. Initial timing outliers and the complete longer K-family
control are retained, as detailed at the integrated checkpoint.
CUDA executes the packed/grouped/gated
and typed-gather cohort on GB10, plus all four compacted attention exports with
exact uniform-score references and nontrivial paged fixtures. Seven-architecture
static comparison shows no register/shared/local/stack increases. SM100/110/120
SASS is unchanged; one HD256 flash-prefill body has different instruction
scheduling on SM75/80/89/90 with unchanged instruction count and resources. This
is not an SM89 hardware throughput qualification or a routing promotion.

The Mac reference/compiler suite passes 143 selected tests with zero skips.
The filtered Linux suite passes 141 with zero skips; the larger monolithic
AArch64 debug test compilation hit a toolchain `fixup value out of range` limit,
so its complete-suite result is not claimed. Product and browser WASM builds
pass. Final model, decoder, replay/lifetime, artifact-freshness and source-ledger
receipts are recorded at the integrated checkpoint below.

### Integrated bulk checkpoint (2026-10-23)

Integrated CPU/Metal/CUDA qualification is complete on the frozen candidate.
Production changes are committed as `71751d339e`; the replay/reference runner
improvements are committed separately as `b01ad350ab`. No routing/admission changes
or new runtime descriptor interpreter are part of this campaign.

Against `dd1ee1b4ff`, including the foundation cohort and every new production
helper/template plus inline tests, the ledger records **1,558 net authored
production lines / 74,764 bytes removed**. Of this, **972 lines / 18,872 bytes**
are CUDA launch declarations; the remaining **586 lines / 55,892 bytes** are
compute/dispatch/source-ownership consolidation. The source estimate was a
planning target; incompatible tuned bodies remain where sharing changes their
schedule or compiler resources. This does not count the earlier pre-campaign
988-line / 63,012-byte consolidation again.

Generated production source separately shrinks **648 lines / 19,025 bytes**.
The CUDA runtime attention region removes 885 generated lines; self-contained
standalone artifacts and the newly owned Metal compatibility region offset
part of that reduction. The whole CUDA `.cu` bundle shrinks 82,439 bytes.
Qualification scripts/tests/README add **606 lines** separately; design prose is
excluded from production savings. The fatbin grows 224 bytes, SM89 cubin size
is unchanged, and PTX text grows 19,062 bytes. This is source simplification,
not a claim that device binaries become substantially smaller. The frozen Mac
model executable is 14,944 bytes smaller than baseline.

The complete tracked campaign change, including planning/qualification prose,
test tools, PTX and binary images, removes 1,233 net text lines and 7,522 total bytes.
These repository totals are separate from the authored production ledger.

The final Metal qualification covers 182 distinct matrix, projection, row and
attention cases in both math modes. The corrected 32-arm cohort passes 181/182;
a tiny F16 K-matrix case initially exceeds the timing ceiling. The complete
18-case K-family control then passes with the unchanged source at 64 balanced
arms (worst ratio 1.014262, previously failing case 1.005414). A constant-address
parameter probe also passes, but no parameter change is adopted because the
unchanged control resolves the failure. The qualified composite uses the later
whole K-family receipt for all 18 cases and the original receipt for the other
164; its worst ratio is 1.042850. Exact bytes, guards, static scratch and thread
limits pass throughout. The original failure and the superseded undersized-tail
fixtures remain recorded.

CUDA's final 123 direct cases (111 compute/gather, 12 paged attention) pass on
GB10 with 32 balanced arms and 4,000 kernels per arm. Worst ratios are 1.039006
and 1.003594 respectively. Independent reference fixtures poison outputs, and
each captured version is checked separately after 100 consecutive kernel calls,
including persistent split-K counter rearming.

The first final Mac matrix passes 12/13 cases; Metal Q4_K reranker median
initially measures 9.6% above baseline, exceeding the ceiling, with p95 and
exact outputs passing. Its 16-row route cannot use the changed high-row K
matrix. The predeclared longer
whole seven-case Metal cohort uses 64 balanced arms, 25 warmups, 201 ordinary
measurements and 1,001 for Q4_K, on the same frozen binaries. All seven pass;
Q4_K is +1.93% median / +0.015% p95. No source change is adopted, and the first
failure remains recorded. The six CPU rows use the complete final 32-arm Mac
matrix; CUDA uses its complete final 32-arm four-case matrix. All 17 admitted
CPU/Metal/CUDA model comparisons pass with finite, byte-identical complete
outputs:

| Backend | Model | p50 ratio | p95 ratio |
| --- | --- | ---: | ---: |
| CPU | MiniLM F32, b1/s16 | 1.001803 | 1.002981 |
| CPU | MiniLM F32, b1/s128 | 0.999538 | 1.000977 |
| CPU | MiniLM F32, b2/s16 | 1.001485 | 0.997737 |
| CPU | MiniLM GGUF, b1/s16 | 1.000530 | 1.000717 |
| CPU | Reranker F16 | 0.977738 | 0.994024 |
| CPU | Reranker Q4_K | 0.995514 | 0.968572 |
| Metal | Reranker Q4_K | 1.019253 | 1.000146 |
| Metal | MiniLM F32, b1/s16 | 1.002107 | 1.002730 |
| Metal | MiniLM F32, b1/s128 | 0.992023 | 0.996942 |
| Metal | MiniLM F32, b2/s16 | 0.998721 | 1.001511 |
| Metal | MiniLM GGUF, b1/s16 | 0.999429 | 0.998993 |
| Metal | Reranker F16 | 1.013115 | 1.007932 |
| Metal | MiniLM ONNX, b1/s16 | 0.991093 | 0.999277 |
| CUDA | MiniLM F32, b1/s16 | 0.997062 | 1.010254 |
| CUDA | MiniLM F32, b1/s128 | 0.997074 | 1.000948 |
| CUDA | MiniLM F32, b2/s16 | 1.006004 | 1.091949 |
| CUDA | Reranker Q4_K | 0.996882 | 0.999308 |

Both admitted CUDA models complete simultaneous 120-second strict-resident,
alternating-input soaks: 41,294 encoder and 10,238 reranker iterations, stable
tracked memory and zero tracked host allocations after teardown. The initial
wrapper incorrectly expected soak mode to write an output file; the benchmark
returns before that writer. Separate finite-output probes correct that wrapper
assertion while preserving the completed soak logs and original failed receipt.
Focused Linux lifetime tests pass seven applicable cases, including prepared
physical-frame retention under pool pressure/executable eviction, with 17
Metal-only skips. Both baseline and candidate reject the optional TinyLlama
CUDA fixture with `UnsupportedCudaArchitecture`; CUDA decoder model admission
is not expanded or claimed. Paged attention still has the direct hardware gates
above.

The independent decoder oracle passes all 160,000 logits per CPU/Metal backend
at tolerance 0.002 (maximum absolute error 0.000668 native / 0.000759 Metal).
Both retain exact reset parity and pass chunked-prefill checks at 0.0001.
Five concurrent 120-second Mac soaks also pass with stable tracked host/device
memory and zero tracked allocations at teardown: native/Metal encoders, native
Q4_K reranker and native/Metal decoders. The decoders perform 679 / 12 runtime
evictions respectively while retaining output parity. These are representative
composition soaks, not a repetition of every earlier long-duration run.

The final local lifetime selection passes 21 tests with five hardware/fixture
skips using `-Dmetal=true -Dcuda=false`. The earlier local CUDA-enabled run
reaches a CUDA-only lifetime test and returns `CudaUnavailable`; that original
receipt is retained, and the same CUDA lifetime case passes on Glowworm. No
production/test logic is changed to bypass it.

Generated-source freshness passes for all 67 files and Metal/CUDA runtime
regions. The host has no offline `xcrun metal` toolchain, so the original AIR
compilation check is recorded as unavailable. A separate, explicitly identified
GPU-runtime compilation bridge validates the exact same 29 generated Metal
artifacts and 25 shadow-rendered v2 sources using `newLibraryWithSource`; all
54 compile successfully. It produces no offline AIR and is not reported as an
offline compiler pass. The changed full runtime shader also compiles and
executes in the 182 direct cases above. Pinned NVCC 13.2.86 fresh compilation
checks all three CUDA images; product CPU/Metal/CUDA and browser WASM builds,
formatter and whitespace checks pass. The temporary local validation worktree
and isolated build caches are removed after hash matching its complete source
to the committed production tree. Required local cache collection completes
after the build/test session; optional shared `uv` pruning is skipped when
other sessions hold its cache lock.

The Mac frozen executable SHA256 is
`f5ffbd69216287e4ec619cce600cec3a2979cf53ad2b07bf494d25aa7439b626`;
the CUDA executable is
`443f301cd3173a8242862f13b6119099a65ef8dccdec680e55283eb4f32fa47e`.
Both compare against the original campaign baseline, not the foundation commit.
Raw fixtures, initial failures, timing arms, source/image hashes and frozen
executables remain under ignored `.benchmark-assets/1166/bulk/`. Final receipts
are `final/ledger/source-ledger.json`, `final/metal-direct-composite.json`,
`final/mac-models/`, `final/mac-metal-long/`, `final/mac-soaks/`,
`final/runtime-metal-tool/receipt.jsonl`, `final/source-build-proof.json`,
`final/metal-source-fixture-hashes.json`, and the locally retained CUDA
archive/tree `final/cuda-qualified-receipts.tar.gz` / `final/cuda-qualified/`.

### Execution order

1. **Foundation: 01–02.** Freeze source, artifacts, models and executables once;
   extend the precision contracts and create the reusable qualification matrix.
   Do not build a general kernel language. Reuse `kernel_math.zig`,
   `kernel_reduction.zig`, `quant_recipe.zig` and existing target lowerings.
2. **Broad lower-risk families: 11–12, 17–22.** Consolidate typed loads,
   quantization, memory/reduction policies and CPU dispatch/adapters. This also
   exercises the common runner across all three targets before touching the
   busiest packed schedules.
3. **Quantized compute: 03–10 and 23.** Process compatible schedule cohorts
   together: matrix, MMV, grouped projection and fused FFN. Validate a cohort
   before building on it; do not ask for another user instruction per family.
4. **Attention ownership and compact emission: 13–16.** Reuse the already
   qualified math/staging policies, preserving full and tail paths. Compact
   generated emission only after identifying whether a body is already shared.
5. **Integrated acceptance: 24.** Freeze the final candidate and compare against
   the original campaign baseline, so small losses cannot accumulate unnoticed
   across individually passing cohorts.

Use a handful of reviewable commits organized by work wave or schedule cohort,
not one commit and user handoff per tiny helper. Keep the campaign running
through final acceptance; an incompatible family can retain a documented
specialized body while independent packages proceed. Source-size estimates are
revised from the ledger after the first quantized cohort, without counting an
unqualified implementation as achieved savings.

### Acceptance and stopping rules

- Every changed entry point gets direct representative and boundary coverage,
  including aligned/tail shapes, all affected storage types and outputs. Include
  aliases only where the public operation admits them, plus output guards,
  nonfinite values, rounding ties and partial dimensions as applicable.
- Prefer byte-identical baseline output where the old contract is deterministic;
  retain independent F64/integer references and existing operator tolerances.
  Arithmetic trees alone are insufficient: preserve full-expression FMA
  boundaries, intermediate stores, narrowing and reduction trees explicitly.
- CUDA compares instructions and register/shared/local/stack usage on all seven
  packaged architectures after each completed compute cohort. Execute changed
  kernels and captured-replay cases on Glowworm GB10; static SM89 parity is not
  presented as SM89 hardware throughput qualification. Do not promote an
  architecture-specific route or expand model admission during this campaign.
- Metal compares pipeline thread limits and static scratch as well as outputs
  and fixed-buffer GPU timings. CPU compares the actual SIMD/parallel schedules,
  with controlled worker counts, affinity and working-set shapes. Do not run
  competing builds/tests on a device during its timing series.
- Aim for neutral or better throughput. The established ceilings remain 5% for
  direct kernel/median model time and 10% for model p95. A reproducible hot-path
  loss is investigated even within that ceiling; retain a better specialization
  instead of accepting abstraction overhead merely to hit the deletion target.
  Investigate unstable results with a predeclared longer balanced whole-cohort
  series, retaining the original data as in prior checkpoints.
- Run the complete admitted CPU/Metal/CUDA model matrix once on the final frozen
  candidate, plus decoder oracle, strict-resident retained-frame/graph replay,
  eviction/reset tests and representative soaks. Repeat lifetime-specific
  checks when operand/output ownership changes; do not duplicate every earlier
  soak for source-only helper edits with unchanged lifetime code.
- Complete generated-source/artifact freshness, product and WASM builds,
  appropriate reference tests, whitespace checks and cache collection. Finish
  with committed source, refreshed images, retained receipts, actual net
  deletion figures and a clean worktree. There is no push/deployment step.

## Completed next bulk campaign: kernel bodies and typed launch ownership

Status: implementation in progress, following the completed 24-package
campaign. Baseline source, images and qualified executables are frozen;
package ownership and qualification receipts are tracked under ignored
`.benchmark-assets/1166/next-bulk/`. Freeze production baseline `0f10101b97825ecd9cb0cbf682ac426dec08f6da`
and preserve the existing work in `.worktrees/inference-unified-execution`.
Treat the following **36 packages as one campaign**, with six implementation
waves and integrated acceptance. Package boundaries are review/qualification
units; they are not requests for another user instruction per helper.

### Outcome and accounting targets

Remove repeated native implementation and launch preparation while keeping
backend schedules explicit. The planning envelope is **2,000–4,500 net authored
production lines / 100–200 KB removed**, with two separate working budgets:

- **60–110 KB of shader/device-body consolidation**, including all replacement
  loaders, helpers, policies, renderer code and format metadata.
- **40–90 KB of host dispatch, argument packing and result-adapter consolidation**,
  including the complete new ABI/symbol metadata and generation machinery.

These are estimates to verify during inventory, not achieved savings. Revise
them after the first ownership audit if the compatible schedules do not support
the range. Do not force a measured regression to meet a deletion target. Report
actual device-body savings, host plumbing, declarations, generated text, binary
images, tests/tooling and the whole repository delta separately. The preceding
campaign's 1,558 authored lines / 74,764 bytes are outside this new ledger.
Moving MSL out of Objective-C strings or generating wrappers earns no savings
unless the complete new authored owner is smaller. Generated copies that
already have one owner are not new implementation duplication.

The fresh ignored inventory is under
`.benchmark-assets/1166/next-consolidation-plan/`. It parses 533 Metal entry
definitions occupying 786,273 decoded body bytes, including generated regions,
macro-named entries and thin wrappers. It identifies 105 handwritten CUDA
bodies of at least 1,400 bytes in the main translation unit, and **275 CUDA
launch methods / 240 explicit argument-pointer arrays** in `kernels.zig`.
These are candidate pools, not counts of independent duplicate algorithms.
Token similarity supplies triage; actual loads, barriers, arithmetic and ownership
must establish each sharing boundary.

Concrete remaining examples include Metal Q4_0 scalar-reduce variants (11
entries / 23,161 body bytes), full Q6_K NSG variants, the Q8_0 matrix
F32/F16-input pair, paired-Q8 matrix F32/F16-output bodies, and within-format
quantize-copy/set-row encoders. CUDA examples include three MTP masked-selection
bodies (13,700 bytes), quantized projection/argmax tiles, scalar attention decode
with ordinary/device-scalar positions, and packed QKV/E4B FFN variants. Some
prefix groups contain previously shared wrappers: count their existing owners
once and exclude their wrappers from projected deletion.

### Long-term ownership boundaries

Use five small, connected ownership layers:

1. **Mathematics and geometry:** extend `kernel_descriptor.zig`,
   `kernel_math.zig`, `kernel_reduction.zig` and `kernel_geometry.zig` only when
   an actual migrated consumer needs the information. Keep mathematical
   identity separate from backend scheduling and launch representation.
2. **Storage/codec leaves:** reuse `quant_recipe.zig`, existing CUDA packed-dot
   helpers and format conversion owners. Decode, encode, scale and narrowing
   remain explicit. CUDA GGML Q8_1 raw-sum headers and the legacy/CPU activation
   contracts remain different types/policies even when their byte widths match.
3. **Native schedules:** Metal/CUDA templates and Zig comptime helpers own each
   genuinely shared traversal. Tile dimensions, subgroup counts, accumulator
   extents, staging precision and tail policies are compile-time choices.
4. **Typed launch ABI:** maintain backend-specific mappings from named operands
   to CUDA parameter order or Metal buffer/constant slots. Use ordinary native
   argument records and straight-line/comptime-expanded bindings. ABI metadata
   does not become a runtime operation interpreter or a second math language.
5. **Ownership adapters:** allocation, donation, output construction and error
   cleanup consume the common contracts. Physical frames, request epochs,
   prepared caches and device resources retain their existing lifetime owners.

Keep explicit exported symbols and recognizable native bodies. Hot dispatch
continues through typed fields/direct calls, with no string lookup, heap
allocation, reflection, virtual policy dispatch or maximum-sized temporary
arrays added per launch. Reuse existing pipeline constructors and already-shared
loops; do not create a parallel registry/descriptor vocabulary beside them.

### Work packages

Source aliases below refer to existing files under `zig/pkg/inference/src`:
`MSL` = `backends/metal_kernels.m`; `CU` =
`ops/cuda/artifacts/inference_cuda_kernels.cu`; `Launch` =
`ops/cuda/kernels.zig`; `CPU` = `ops/native_compute.zig`; renderers =
`graph/quant_kernel_{metal,cuda}_renderer.zig`. Changes to compiler-managed
regions flow through their existing generators.

| ID | Package and concrete starting point | Intended shared owner and protected distinctions |
| --- | --- | --- |
| N01 | Freeze baseline, owner inventory and source ledger | Record each full body, existing helper, generated copy, exported ABI and caller. Assign overlapping candidates one owner; budget net deletion after all new metadata/helpers. Keep the preceding completed campaign immutable. |
| N02 | Precision/schedule contracts for selected families | Inventory reduction trees, FMA boundaries, staged rounding, signed-zero/NaN/tie behavior, codec headers and exact array extents. Reuse existing policies; add fields only with consumers in this campaign. |
| N03 | ABI and descriptor ownership map | Map semantic operands to existing CUDA signatures and Metal slots/parameter layouts, distinguishing required/optional exports, mirrors, scratch and persistent state. Prototype against a small representative family before expanding the schema. |
| N04 | Qualification and dispatch-coverage matrix | Bind every proposed family to direct fixtures, an independent reference, applicable model/training graph and expected route counters. Establish host-launch/preparation measurements as well as GPU timings before editing hot launch plumbing. |
| N05 | MSL Q4_0 scalar MMV geometry | Share matching `termite_q4_0_linear_1x_reduce*` traversal across NR/NSG/NQ choices, with exact compile-time accumulator dimensions. Retain distinct lane packing/reduction trees and the separate unrolled MMA schedules. |
| N06 | MSL Q4_0 scalar storage/epilogue variants | Extend the N05 owner for the matching F32/F16 input/output and sum-of-squares variants. Preserve the precise narrowing point and sumsq tree; do not compute unused auxiliary outputs in ordinary variants. N05/N06 earn one combined deletion figure. |
| N07 | MSL Q6_K NSG scalar variants | Consolidate the full base/NSG4/NSG8 bodies of `termite_q6_k_linear_1x_reduce*` through subgroup policy. Reuse the already-shared odd-block owner; do not count or replace its thin wrappers again. |
| N08 | MSL ordinary Q8_0 matrix input types | Share `termite_q8_0_linear_mm` / `_in_f16` through typed input acquisition. These are distinct from the already-shared m32/m64 simdgroup-MMA families. Preserve traversal, vector widths and reduction order. |
| N09 | MSL paired Q8_0 matrix output types | Share `termite_q8_0_pair_activation_multiply_mm` / `_out_f16` with a typed store/epilogue. Keep all four exact eight-row accumulator arrays and gate/up reuse; retain the activation expression's rounding boundaries. |
| N10 | MSL RMS-scaled paired Q8 projections | Consolidate matching `*_rms_scale_1r_ext` F32/F16-output bodies and common acquisition with `*_1r_ext` only where dataflow matches. Scale placement remains an explicit policy, not an algebraic reassociation. |
| N11 | MSL Q4_K pair/activation reductions | Compare the `termite_q4_k_pair_*_1x_reduce*` bodies for a shared two-projection traversal and typed activation store. Preserve the distinct non-1x schedule and format-specific scale/min accumulation. |
| N12 | MSL quantize-copy/set-row encoders | Give each format's `*_set_rows` / `*_cpy_f32_to_q` pair one block-encoder owner, then share matching traversal/address scaffolds. Keep each format's packing, rounding, header bytes and invalid-index behavior. This does not merge different formats' quantizers or legalize duplicate-index scatter races. |
| N13 | MSL indexed/MoE Q4 projection scaffolds | Audit `termite_q4_0_linear_id_a4b_{gate_up,down}*` and slot projections. Share identical expert-address and dot components; retain grouped routing, atomic/segmented reduction, gate placement and separately tuned NQ variants. |
| N14 | MSL attention renderer common components | Compare the HD256/HD512 prefill renderers for identical page-address, mask and epilogue components with explicit geometry policies. Retain differing MMA/staging/barrier loops. This is an audit-dependent opportunity, not a promise to merge the dense-eight-query and generic paged schedules already kept separate. |
| N15 | CU MTP masked select/hidden/argmax | Share the matching ranking/selection core of `termite_gemma4_mtp_masked_*` with compile-time output policy. Preserve masked eligibility, index ties, selected hidden-vector publication and all nonfinite behavior. |
| N16 | CU quantized projection argmax tiles | Share matching `termite_linear_{q4_0,q8_0,q4_k,q6_k}_argmax_rows_stage1_tile*` scaffolds through format-dot and tile policies. Preserve packed-dot arithmetic and exact tile4/8/16 accumulator sizes; different reductions stay separate. |
| N17 | CU argmax partial merge widths | Consolidate matching `termite_argmax_reduce_rows_pairs_f32*` addressing/store logic with explicit warp-width reduction policies. Preserve the current tree, lowest-index ties, sentinels and publication order. Reuse the common value/index contract from N15/N16 without forcing one tree. |
| N18 | CU scalar attention decode positions | Share `termite_gqa_attention_decode_f32` / `_decode_scalars_f32` with compile-time scalar acquisition. Keep device-scalar reads inside the captured invocation at their original load points; retain masks, scaling, cache addressing and accumulation order. |
| N19 | CU TurboQuant attention staging | Compare decode/prefill and split-stage key/value acquisition, especially polar4/int8-identity variants. Share proven identical scaffolds with codec-specific leaves. Preserve staging widths, page mapping and split publication/counter behavior; incompatible loops remain specialized. |
| N20 | CU packed QKV projection tiles | Share matching `*_qkv_nobias_q8_1_f32_tile{4,8}*` and ordinary-QKV width variants via exact compile-time tile/warp policy. Preserve three-projection reuse, output offsets and separate-product rounding. Do not pull in the protected packed rows2 specialization. |
| N21 | CU E4B FFN warp/shape variants | Consolidate matching `*_pair_activation_q8_1_f32_tile4_w{5,8}_e4b_ffn` and fixed-width attention cases. Keep each width/warp schedule and Q8 header contract explicit; reuse existing helpers before adding another packed-dot abstraction. |
| N22 | CU quantized-output multirow FFN | Compare the `*_tile32_w5_e4b_ffn_rows4` / `_rows8_c2` schedules. Share identical dot/quantize pieces with exact row/channel extents; preserve distinct cross-warp staging and epilogue contexts where resource reports differ. |
| N23 | CU small warp attention siblings | Compare `termite_laya_attention_warp_f32` / `termite_gliner_encoder_attention_f32` for identical warp math and an addressing/masking policy. Keep local-window/block algorithms separate. No new architecture/model admission is part of this refactor. |
| N24 | Launch symbol registration and lifecycle | Reuse one consumed symbol/field specification for required/optional loading, initialization and clearing. Preserve ordinary typed `CUfunction` fields, fallback precedence, JIT replacement and capability predicates. Reuse existing Metal pipeline constructors rather than rebuilding an already-shared constructor layer. |
| N25 | Launch typed argument packing | Replace repeated local pointer/scalar conversions and parameter arrays with named, compile-time ABI records/helpers. Preserve exact CUDA wire types/order and per-launch stack lifetimes; no host heap or dynamic reflection. Connect to the existing CUDA parameter-list owner instead of adding another signature truth. |
| N26 | Metal typed binding scaffolds | Share repeated buffer/constant/scratch binding setup for selected matching families. Emit/inline the existing straight-line bindings; preserve slot numbers, buffer offsets, dynamic scratch, scoped/concurrent encoders, resource retention and command counts. Count metadata and generated glue fully. |
| N27 | Common geometry and launch preflight | Reuse `kernel_geometry.zig` and the descriptor's concrete consumers for repeated dimensions, minimum byte extents, overflow checks and output geometry. Preserve each entry's empty-input/error/decline semantics and validate before writes or submission. Backend-specific alignment and scratch remain schedule policy. |
| N28 | CPU and backend result-ownership adapters | Consolidate repeated allocating/into/consume result setup where the existing contracts match. Start with native RoPE/GEGLU/clamp adapters and analogous typed CUDA output construction. Audit overlapping `errdefer`/`makeOwnedBuf` cleanup contracts; prove every partial allocation failure and donation branch before migration. Existing grouped-result helpers remain their owner. |
| N29 | CPU packed normalization backward output | Add/reuse an into-style arithmetic leaf for the packed layer-norm backward result so the backend can fill its final allocation directly. Keep public/reference interfaces compatible, independent oracle coverage, summation order and zero initialization. Qualify eliminated temporary arrays/copies as a separate memory-traffic improvement. |
| N30 | Consume shared operation descriptors | Connect N03/N25/N27/N28 to the existing `Descriptor`, invocation, workspace and auxiliary contracts. Extend semantics only for actual existing consumers; keep target ABI, tile and precision schedules separate. Do not expand graph lowering/admission, change plan IDs or leave speculative descriptor fields unused. |
| N31 | Remaining Metal rotary scalar math | Share identical pair rotation/index fragments across ordinary, per-item, paired and vision rotary bodies where precision matches. Keep position-source/section mapping, table lookup, transcendental mode and head-RMS reduction distinct. CUDA scalar RoPE and CPU table leaves already have owners and earn no new math savings here. |
| N32 | Optimizer arithmetic versus storage policy | Compare Metal AdamW in-place / into kernels for one exact arithmetic leaf with compile-time storage policy. Preserve gradient mutation, moment update order, epsilon/decay placement, flags and transaction publication. Audit CPU/CUDA reuse of existing optimizer owners rather than forcing a common floating-point profile. |
| N33 | Remaining attention emission multiplicity | Audit serial/split-count/score-prework CUDA emission for common definitions still rendered more than once. Reuse existing renderers; keep standalone sources self-contained. Previously compacted flash-prefill and split-K common definitions are already done and excluded. Report authored renderer and generated-bundle changes separately. |
| N34 | Retire superseded owners and duplicate metadata | Remove replaced private bodies/helpers and stale per-family declarations; retain exported ABI wrappers and required compatibility symbols. Include header/metadata dependency closure in source/JIT/artifact identities and freshness checks. File splitting alone is not deletion. |
| N35 | Complete reusable fixture and ownership coverage | Extend existing runners for typed-ABI probes, auxiliary outputs, stateful replay and allocation failures as each package starts; finish the coverage audit here. Retain manifests/source hashes and actual dispatch evidence. Independent references must not silently become calls to the new production helper. |
| N36 | Qualify the full composition and publish the ledger | Freeze the final candidate, compare all changes to N01's original baseline, regenerate/check artifacts, run model/training/lifetime/soak gates, collect caches and commit coherent waves with a clean worktree. Report retained specializations and hardware/toolchain limits explicitly. No push/deployment step. |

### First implementation checkpoint: typed Metal and codec owners

N07–N10 and N12 now share 27 exported Metal entry points through 13 native
helpers: Q6_K subgroup count, ordinary Q8_0 matrix input storage, paired Q8_0
matrix/RMS output storage, and nine format-local set-row/copy encoders. Native
ABIs, exact accumulator sizes, arithmetic and format-specific packing remain
unchanged. This cohort removes **20,187 bytes** from authored Metal source,
after counting all helpers; it is outside the preceding campaign's ledger.

All 282 direct safe/fast cases pass baseline bytes, guards, resources and the
5% timing ceiling using 32 balanced arms / 2,000 dispatches. Seventy-eight cases
also compare against independent F32/F16 arithmetic references. The harness
rejects a deliberately incorrect reference. Nine existing codec reference tests
pass on the actual Metal device.

The first encoder helper version preserved bytes but regressed timing across
126 cases. Its receipts remain retained. Requiring `always_inline` for these
format-local helpers resolves the regression across the complete 180-case
encoder control and the complete 282-case final cohort. The attribute is a
necessary schedule/compiler constraint, not a relaxation of the budget.
Ignored baseline identities, sources, raw arms and receipts are under
`.benchmark-assets/1166/next-bulk/`; the full 36-package composition remains in
progress.

### Second implementation checkpoint: Q4 scalar schedules

N05/N06 share ten matching Q4_0 scalar exports through two forced-inline
owners: eight NR/NSG/input/output variants and two sum-of-squares input
variants. The separate NQ8 lane-packing schedule remains explicit. Exact
accumulator extents, narrowing points and the auxiliary reduction are retained.
This cohort removes **12452 authored Metal bytes** after counting both helpers.

All 88 safe/fast direct cases pass baseline bytes, independent arithmetic
references, guards, resources and the 5% timing ceiling with 32 balanced arms
and 2,000 dispatches. Fixtures cover partial output tiles, multiple row/block
sizes and empty rows. Full source compilation succeeds on the actual Metal
runtime compiler. Receipts are in `next-bulk/metal-q4-scalar/`; the completed
composition qualification follows below.

### CUDA launch ABI checkpoint

N03/N25 migrate 230 argument-pointer arrays without heap allocation or runtime
reflection. Of these, 217 preserve the original mutable slots directly; thirteen
quantized-linear launchers use named records checked and ordered from the
existing native `kernel_abi.cuh` parameter lists. The header remains the single
signature owner. Partially initialized arrays keep their explicit construction.

All launch adapters typecheck. On the DGX Spark, the typed-slot/named-record
probe captures fill and quantized linear operations, returns from their launch
stack frames, then verifies four replays against an independent dot reference
and output guards. Four CUDA model cases pass byte parity and the original
p50/p95 budgets across 32 balanced arms (warmup 25, measured 201); p50 ratios
range from 0.9925 to 1.0037. Receipts are in `next-bulk/cuda-host/`.
Symbol lifecycle consolidation and final composition remain separate gates.

### Q4_K and optimizer arithmetic checkpoint

N11 shares the exact eight-element Q4_K two-projection decode/dot leaf across
three paired linear/activation exports. The two-row scalar accumulator schedule
and four-row array schedule retain their original scratch layout, reduction
order and store type. N32 shares AdamW arithmetic through a compile-time storage
and nonfinite-publication policy; in-place gradient clearing and out-of-place
transaction publication remain explicit.

These two owners remove **2348 authored Metal bytes** after replacement helpers.
All 64 safe/fast direct cases pass bytes, guards, resources and timing across 32
balanced arms / 2,000 dispatches. Finite cases also pass independent Q4_K dot
and AdamW references; nonfinite optimizer cases verify baseline behavior and
flag publication. Fixtures include empty/tail outputs, both storage policies
and multiple projection shapes. Receipts: `next-bulk/metal-leaves/`.

### Native schedule audit and final-composition work

N13 shares the matching fixed A4B gate/up and down traversal with compile-time
input addressing and geometry; both retain exact four-element accumulators.
All twelve referenced safe/fast cases pass, including invalid expert IDs and
extra route threadgroups. N31 shares ordinary, paired and head-RMS rotary
indexing without merging position sources, transcendental evaluation or norm
reductions; all 28 referenced safe/fast cases pass. N26 replaces 98 repeated
three-buffer/constant binding blocks with one forced-inline native ABI owner.
Encoder/resource ownership, slots, offsets and submission counts stay at their
original callers. The integrated model and direct-kernel qualification below passes.

The N14 audit retains the existing prefill schedules. HD256 uses four simdgroups
and page-local eight-token loads, configurable KC32/KC64 staging and a direct
full-query-tile store. HD512 uses eight simdgroups, KC64 staging, and serializes
eight output owners through a shared tail tile with barriers between owners.
Both already consume the shared parameter-field and page-address owners.
Their mask and epilogue fragments are coupled to these different schedules;
another policy/helper layer would duplicate metadata without removing a common
traversal. The dense compatibility prefill remains a separate barrier schedule.

N22 likewise retains distinct row/channel acquisition and staging. Rows4
preloads four Q8 frames before the column loop; rows8/c2 loads a frame inside
the row loop. Their wave counts, scratch extents and output mapping differ.
Only the identical final quantized-output writer is shared. It explicitly owns
the zero auxiliary header used by these producers, preserving its distinction
from raw-sum GGML Q8_1. The protected rows2 and F32 epilogue contexts remain
separate. The writer passes all seven architecture resource checks and actual
CUDA device qualification.

N24 makes the 362 typed CUDA export fields the loading/clearing registry, with
63 required exports and explicit native-name/JIT-only exceptions. N27 shares
thirteen format-specific extent checks. N30 supplies dimensions to eleven named
CUDA records through the existing `Linear` descriptor, with no new operation
fields. Actual capture/replay and malformed/empty-input probes pass; integrated
model gates pass in the integrated qualification below.

N28 uses one native transfer owner for eight adapters and delegates CUDA BF16
mirror construction to the existing typed tensor constructor. Allocation-failure
probes cover partial construction, shape publication and both normalization
storage policies. N29 shares backward arithmetic and writes larger packed
results directly. Measurements reject the single-allocation policy for tiny
single rows; the faster original storage policy is retained below width 32.
All five matched-harness shapes pass byte parity and timing across 32 balanced
arms. This checkpoint observes P50 ratios of 0.891 (1×8), 0.867 (1×16),
0.632 (1×32), 0.861 (17×768) and 0.877 (128×2048). Final portable-candidate
measurements and model/training composition are recorded below.

N33 emits one serial attention implementation per complete head/thread/GQA
schedule identity, retaining every exported split-count entry. Standalone
artifacts contain the same owner and wrapper. Deterministic-source, export,
standalone/bundle and renderer checks pass; all 67 generated files and runtime
regions are fresh. N34 now includes shared contracts, the launch ABI consumer
and the complete local CUDA header closure in qualification/cache dependencies.
The transitive-include coverage check and all five artifact-script tests pass.

The expanded native/compiler/renderer selection checks 133 cases: 132 pass,
with one CUDA-device skip on the Mac. Actual CUDA ABI tests run on the Spark separately.
The completed composition, resource review, models, training/lifetime/soak
checks and final source ledger follow.

### Completed campaign and qualification (2026-10-23)

N01–N36 are complete. The campaign replaces matching native kernel bodies and
launch scaffolds with consumed owners, and qualifies the composition against
`0f10101b97825ecd9cb0cbf682ac426dec08f6da`. The preceding completed campaign's
1,558 lines / 74,764 bytes are excluded from these new savings.

The final authored-production reduction is **3,832 lines / 197,348 bytes**,
including every replacement helper, descriptor consumer, symbol exception,
requiredness list and native portability adapter. Positive figures below are
net removals; negative figures are additions.

| Maintained production source | Lines removed | Bytes removed |
| --- | ---: | ---: |
| Metal shader bodies | 17 | 37,037 |
| Metal native bindings | 280 | 11,588 |
| CUDA device bodies | 772 | 29,137 |
| CUDA launch/registration owners, including `launch_abi.zig` | 2,966 | 129,609 |
| CPU/result ownership and packed backward outputs | −70 | −3,488 |
| Authored generator owners | −74 | −3,741 |
| Shared contracts and build/cache dependencies | −43 | −2,097 |
| Native alignment and portable entry adapters | −16 | −697 |
| **Authored production total** | **3,832** | **197,348** |

Metal shader figures measure the maintained Objective-C string representation.
Decoded MSL removes 36,782 bytes; together with CUDA device source this removes
65,919 bytes of native shader/device language. Generated source separately
removes 92 lines / 4,234 bytes. Inline Zig tests add 241 lines / 14,012 bytes;
benchmark/test/build tooling adds 381 lines / 18,637 bytes. Documentation has
its own ledger category. The three packaged images together remove 347 bytes:
fatbin grows 3,368 bytes, SM89 cubin grows 512 bytes and PTX shrinks 4,227 bytes.
The thin exported wrappers continue to instantiate tuned native code.
Whole tracked-tree file content, including tests, tooling, documentation,
generated sources and images, decreases by 129,256 bytes.

`zig/pkg/inference/scripts/kernel_source_accounting.py` reproduces these
categories from the frozen revision, including staged, unstaged and untracked
source files. Its lexer separates Zig test blocks from implementation while
handling quoted/multiline literals and nested comments. The per-file ledger
retains baseline/candidate hashes. The retained receipt is
`next-bulk/ledger-final/accounting.json` under `.benchmark-assets/1166/`.

The completed ownership map includes 230 CUDA launch arrays (217 mutable-slot
packers and 13 named native-signature records), 362 typed export fields for
loading/clearing, 98 Metal binding blocks and eleven concrete `Linear`
descriptor consumers. Required CUDA exports retain their original error
priority across the profile gates. Clearing also covers the copy field and
GLiNER attention array. Capture tests replay after launch-stack locals have
expired; allocation-failure probes exercise partial tensor/shape construction.

The CPU result adapters are forced inline. An integrated native MiniLM case
initially regressed by about 6–7%, even with a matched benchmark harness.
Restoring the original forward-normalization wrapper did not resolve it.
Sampling and instruction comparisons found matching hot GEMM/attention bodies
at different linked offsets. Explicit 64-byte native entry alignment resolves
the regression: the final control against the original baseline has P50 ratio
0.9993, and an alignment-only comparison against the preceding inlined
candidate has P50 ratio 0.9437. Original failing receipts and controls remain.

WebAssembly rejects explicit function alignment. It selects the existing core
GEMM routines and an unaligned attention entry; native targets use aligned
entries over the same arithmetic owner. The portable rebuild preserves all
three native hot-body sizes and instructions apart from relocations, with no
out-of-line attention-body call. Six affected model cases and all five CPU
primitive cases pass again. Final primitive P50 ratios are 0.939 (1×8), 0.819
(1×16), 0.632 (1×32), 0.858 (17×768) and 0.874 (128×2048). The small-row
storage exception remains below width 32; larger packed results eliminate
three temporary planes and their copies.

| Qualification | Evidence |
| --- | --- |
| Full Mac model composition | 13 encoder/reranker cases, 32 balanced arms each; finite, byte-identical outputs and P50 ≤ 1.05 / P95 ≤ 1.10 |
| Decoder model latency | CPU and Metal, prompt 8 + four decode steps, 32 balanced arms; byte parity, reset/chunked-prefill checks and the same timing limits |
| Portable native follow-up | Four native encoder and two decoder cases pass all 32-arm gates; hot-entry instruction/alignment comparison passes |
| Final exact-source CUDA models | Four cases on Glowworm, 32 balanced arms; all numerical and timing gates pass |
| Metal direct composition | 656 safe/fast cases; exact outputs, guards, resources and timing pass; 262 cases check 352 independent reference outputs |
| CUDA direct composition | 281 cases: 83 new compute, 39 edge/serial, 111 quant regressions, 12 attention regressions and 36 larger MTP dtype/ordering cases |
| Independent decoder oracle | All 160,000 logits per backend; F64-reference maximum absolute error 0.000668 native / 0.000759 Metal, below 0.002 |
| Native/compiler/renderer selection | 132 pass, one CUDA-device skip on the Mac; the actual CUDA capture/ABI/ownership suite passes on Spark |
| Training, ownership and lifetime | Mac selection: 34 pass / five skips; two fixture-gated resident output/decoder tests rerun with pinned TinyLlama and both pass; final Spark subset: 14 pass / no skips |
| Final binary soaks | Five Mac and two CUDA runs, 120 seconds each, with separate finite probes, reset/eviction/retained-input checks, allocator release and RSS budgets |
| Product/browser | Native inference package check and browser WASM build pass |
| Source/artifact freshness | All 67 generated files/runtime regions fresh; all three CUDA images pass pinned NVCC checks; 54 Metal sources compile through the actual GPU runtime compiler |
| Qualification tooling | Seven artifact/source-accounting tests pass |

The direct timing limit remains 5%. The 111-case CUDA regression suite first
recorded one 6% timing failure in an unchanged-SASS kernel. Forward, reversed
and identical-image controls did not reproduce a consistent slowdown. A
recorded 500 ms alternating warmup and 20,000 dispatches per arm then passes
all 111 cases with the same limit; original failures and controls are retained.
The 36 larger MTP cases use this longer protocol across F32/BF16/F16, both
ordering representations, non-involutive permutations and partial-cluster tails.
Their references compute ranking independently from production helpers.

All seven packaged CUDA architectures (75/80/89/90/100/110/120) retain their
export counts with no register, shared-memory, stack or local-memory increases.
Q4_0 tile16 argmax uses fewer registers on 75/80/89/90. Actual CUDA execution
and performance validation use the Spark's GB10/SM121 with native SM120 code;
the other architectures receive compile/static-resource checks. The
CUDA TinyLlama architecture guard remains in force on both baseline and
candidate. MTP kernel fixtures cover its changed bodies; no full Gemma4 model
checkpoint is claimed. Optional published GLiNER/ModernBERT checkpoint tests
remain fixture-gated; tiny resident jobs, pinned Torch AdamW, partial resume,
clipping, allocator failures and backend-gradient coverage pass. The CUDA
prepared-frame case skipped on the Mac passes on Spark.

The Metal toolchain has no offline AIR compiler in this environment. The
54-source check records successful `newLibraryWithSource` compilation, with
source hashes and compiler receipts; actual device pipelines and their
resources/timings are separately exercised by the 656-case suite.

Retained specializations are explicit results of the audits: HD256/HD512
prefill barrier/staging schedules; rows4/rows8 FFN acquisition and scratch;
protected rows2/F32 and raw-sum/zero-aux Q8_1 contexts; NQ8 scalar and MoE
schedules; rotary position/transcendental/norm policies; and attention
producer/reducer emission that already has one authored renderer. These keep
one owner for each matching leaf without combining distinct schedules.

Receipts and immutable inputs are retained under
`.benchmark-assets/1166/next-bulk/`: `baseline/`, `mac-final-v3/`,
`mac-checks/`, `mac-portable/`, `native-candidate/`, `native-model-controls/`,
`native-profiles/`, `cuda-composition/`, `cuda-exact/`, `cuda-portable/`,
`cuda-mtp-shaped/`, `cuda-mtp-permutation/`, `ledger-final/` and `final-tools/`.
The full 13-case/direct composition and portable follow-ups have separate
receipts; their unchanged GPU sources and equal native hot-body evidence are
retained. Final source manifests include all changed shipping inputs and image
hashes. Baseline benchmark identities are frozen independently; the matched
CPU harness patch is saved with the baseline. Coherent implementation commits
include `aeed3b7587`, `08c89acdce`, `54cf359e36` and `3fa92014ce`, following
the five earlier implementation checkpoints above.

The matched-baseline worktree is removed after freezing its benchmark and
harness patch. Task-owned compiled caches/install prefixes are collected on
both hosts; frozen validation binaries, fixtures, source identities and raw
receipts remain. `make gc` runs with its active-worktree protection; optional
UV pruning reaches the existing cache lock timeout. Its lock holder is left
alone, and task-owned compiled output is collected directly.

### Six implementation waves

1. **Ownership and contracts — N01–N04.** Freeze the source/images/executables;
   identify actual source owners and high-confidence versus audit-dependent
   candidates. Start N35's fixture/ABI work before any affected implementation.
2. **Quantized compute and selection — N05–N13, N15–N17, N20–N22.** Process
   Metal scalar/typed/encoder families and CUDA projection/selection families
   together. Prefer high-confidence paired bodies first. Qualify each schedule
   cohort before combining it with another cohort.
3. **Decoder, attention and optimizer math — N14, N18–N19, N23, N31–N32.**
   Use the same precision/codec owners, with independent per-schedule evidence.
   Audit-dependent attention sharing has a lower confidence than typed twins;
   retain separate loops whenever the common schedule is not established.
4. **Launch and result ownership — N24–N30.** Start from already-qualified
   family ABIs and migrate in bounded groups. Measure host-side overhead,
   allocation/cleanup and graph capture, not just device event timings.
5. **Emission and retirement — N33–N34.** Compact remaining generated repetition
   after actual ownership is clear. Regenerate the canonical sources/images
   together and remove the old owners after caller and ABI audits.
6. **Integrated acceptance — N35–N36.** Complete the coverage audit and run the
   final frozen composition against the original baseline. No final handoff
   with unqualified new bodies or a dirty production worktree.

Use approximately six to ten reviewable commits, grouped by compatible source
owners and qualification boundaries. Run source/codegen checks per completed
cohort and seven-architecture compiler/resource comparison per CUDA compute
wave, including unrelated functions that NVCC may change. Avoid full artifact
regeneration per tiny helper, competing timing jobs on one device, or a user
handoff between individual packages.

### Qualification and performance protections

Retain the established ceilings: **5% direct-kernel/model median and 10% model
p95**, aiming for neutral or better throughput. Investigate a reproducible
hot-path loss even inside the ceiling. Predeclare balanced timing protocols;
retain every failed arm/series and use longer whole-cohort controls for unstable
results. Never switch baselines between waves or select favorable runs.

For numerical coverage, test exact complete baseline outputs, guards, tails,
empty/admitted boundary cases, integer bits, packed headers, narrowing ties,
signed zeros, NaNs/Infs and deterministic index ties where contracts admit
them. Keep independent F64/integer/finite-difference or pinned framework
references. Check Metal safe/fast modes, static/dynamic scratch and thread
limits; check CUDA instructions and register/shared/local/stack usage on every
packaged architecture, then execute directly on Glowworm GB10.

For launch/descriptor changes, validate argument names/types/order and Metal
slots against the native ABI, with independent sentinel-filled device probes.
Test optional-symbol absence, illegal shapes declining before writes, donation
and every partial allocation failure. Measure CPU argument preparation,
end-to-end small launches, cold initialization, allocation counts and actual
command/encoder/submission counts. A GPU-event-only benchmark cannot qualify
N24–N30. Preserve capture-time parameter copying, replay-time device scalars,
persistent counters and request-owned buffer/auxiliary lifetimes.

Retain the existing 17-case admitted CPU/Metal/CUDA model matrix and independent
CPU/Metal decoder oracle, then add bounded graph/model workloads that actually
exercise the newly changed MTP, argmax, codec and training families. Record
which cases dispatch each family: the existing encoder/reranker matrix alone
does not cover those decoder paths. Where GB10 cannot admit a particular model,
use independent direct references and captured replay, document the hardware
limit and retain any specialization whose performance cannot be qualified.
Do not broaden CUDA decoder admission to manufacture coverage.

Repeat retained-frame/pool-pressure, runtime/executable eviction, strict
resident, graph reset, mirror/auxiliary lifetime and representative soaks for
ownership changes. Train selected tiny resident graphs through backward and
optimizer updates, including exact durable resume and rejected-update recovery.
Keep the existing CUDA rows2 tile, F32 activation-slice contexts, dense/paged
attention schedule distinctions and differing Q8_1 headers protected unless a
new approach independently clears their original resource and numerical gates.
Already-shared CUDA RMSNorm/RoPE, Metal m32/m64 MMA, CPU prepared workers and
grouped SGEMM/result ownership are not counted as fresh opportunities.

Finish with canonical generated-source freshness, all three pinned-toolchain
CUDA images, product and browser WASM builds, appropriate reference suites,
whitespace/format checks, cache collection and committed source. Record offline
Metal toolchain availability separately from real GPU runtime compilation;
compile the full generated/rendered shader set through the available path and
do not report an unavailable AIR compiler as a passing offline check.

## Completed residual-kernel and ABI consolidation campaign

This campaign uses the independent baseline
`fdb6d7252f4d55ed8a9d1ef48869ad88dc0b0fc6`, after the completed N01–N36
campaign. The earlier 197,348-byte authored reduction and its whole-tree
ledger remain frozen at that revision and are excluded from the figures below.
All R01–R36 packages are complete: 28 implementation/qualification packages and
eight native-body audits retaining distinct schedules or existing shared leaves.

The original plan covered **36 packages in six integrated waves**, with
kernel-body consolidation as the first deliverable. Its provisional budget was
**40–80 KB net authored production reduction**, comprising roughly 25–50 KB
of native kernel bodies and 15–30 KB of host/ABI scaffolding. The retained
inventory, package contracts and acceptance gates below document that plan;
the final implementation record replaces the estimates with measured results.
All new CPU ownership code, helpers and contracts count against the total.

### Current-source inventory and exclusions

A brace-delimited inventory of explicit export bodies, excluding bodies below
700 bytes, identifies the following representative pool. Metal bytes are
decoded MSL; CUDA bytes include each selected handwritten declaration/body.
Each selected export occurs in one inventory group. Unique schedules are
included in this pool and must be separated from genuine duplicate components.

| Candidate pool | Substantial bodies | Exposed body bytes |
| --- | ---: | ---: |
| Metal legacy/IQ scalar projection families | 16 | 26,767 |
| Metal Q4_K/Q5_K projection families | 5 | 10,808 |
| Metal ternary single/paired projection families | 4 | 7,592 |
| Metal RMS families | 7 | 9,850 |
| Metal convolution families | 3 | 3,193 |
| CUDA activation-slice, QKV and packed-Q6 argmax | 11 | 34,289 |
| CUDA TurboQuant attention | 9 | 60,288 |
| CUDA training tensor-core/backward attention | 9 | 77,247 |
| CUDA KV suffix stores | 2 | 7,162 |
| **Representative pool** | **66** | **237,196** |

This inventory measures exposed source, not removable source. It is retained
with per-export sizes and its extraction script in
`.benchmark-assets/1166/residual-consolidation-plan/`. Helper-to-helper overlap,
host bindings, CPU adapters and parameter declarations are additional audit
surfaces outside this export-body pool.

Exclude already-completed work from new savings: Metal Q4_0 scalar owners,
Q6_K NSG owners, ordinary/paired Q8 matrix typing, quantize-copy/set-row
encoders and `termite_quant_rows_decode`; CUDA ordinary RMS/layer-norm,
quantized embedding lookup, gated-down tile wrappers, MTP ranking, ordinary
projection argmax and serial-attention emission already have owners. The
Q8 extended single/pair/activation families also already have individual
owners: R11 examines overlap *between those owners*, not their thin wrappers.
CPU primitive unary/reduction/softmax arithmetic already has into-style
owners; CPU packages below concern acquisition, result lifetime and views.

The previous audits remain binding: HD256/HD512 prefill schedules, protected
rows2/F32 FFN contexts, raw-sum versus zero-aux Q8_1 headers, small-row packed
normalization exceptions and target-specific floating-point profiles retain
their explicit contracts. Reopen a retained schedule only with a newly
identified matching component and fresh resource/performance evidence.

### Ownership and dependency rules

Keep the existing five layers: semantic descriptors/geometry, format-specific
codec/dot leaves, compile-time native schedules, native ABI mappings and
lifetime adapters. A native traversal can accept a small static format,
acquisition or publication policy; every instantiation retains its exact
accumulator/scratch extents and reduction tree. Build common leaves first and
move their consumers together in the same wave. R03/R04/R06 share one legacy
projection ownership map; R19/R20 share one TurboQuant load/codec map. Their
savings are counted once across the complete composition.

Reuse existing block-value/packing helpers before adding new ones. A block
value decoder is not automatically a valid dot policy: for example,
`d * sum(x*q)` and `sum(x*(d*q+m))` must retain their existing rounding and
scale placement. Array dimensions, warp counts, barrier topology, MMA shapes,
staged precision and unroll bounds stay in native schedule policy.

Native CUDA parameter lists remain the ABI owner for named host records.
Metal parameter-field ownership must emit the existing native and MSL layouts
with sizeof/offsetof evidence. Add semantic descriptor fields only with
concrete consumers in this campaign. Preserve direct typed dispatch, stack
argument lifetimes, replay-time scalar reads and existing admission/error
behavior. New owners add no per-launch heap allocation, runtime reflection,
virtual policies or maximum-sized scratch arrays. Native entry alignment and
WASM's unaligned/core selection remain explicit target policy.

### Work packages

Confidence: **H** means a concrete repeated scaffold is visible; **A** means
an audit/prototype must establish a profitable boundary. Both require device
qualification. An audit can finish by retaining the existing implementation
with evidence; it earns no deletion credit.

| ID | Package | Concrete implementation boundary and required evidence |
| --- | --- | --- |
| R01 | Freeze owners, ABI and source budget — H | Freeze `fdb6d7252f` source/images, current Mac/Spark model binaries, independent fixtures and dispatch counters. Inventory full owners, wrappers and candidate components, including helper/header costs. Keep all previous campaign figures separate. |
| R02 | Prototype and fixture matrix — H | Prototype one legacy Metal scalar traversal and one ordinary CUDA QKV/activation-slice family. Record baseline direct, host-launch/preparation and model timings before changing their plumbing. Bind every remaining package to independent references and actual route evidence. |
| R03 | Metal legacy scalar linear traversal — H | In `metal_kernels.m`, consolidate matching non-reduce `q4_1`, `q5_0`, `q5_1`, `q8_1`, `q8_k` row/output/block loops through format-specific dot/load policies. Reuse existing `*_block_value` leaves where their rounding matches. |
| R04 | Metal legacy scalar-reduce geometry — H | Share matching NR/NSG/NQ traversal and publication among the corresponding `*_linear_1x_reduce` bodies. Instantiate their actual NR2/NR4 and NSG2/NSG4 extents. Keep scale-after-dot and affine-per-product profiles distinct. R03/R04 share the codec map and one net ledger. |
| R05 | Metal K-quant acquisition and dot components — A | Audit Q4_K base/v2 and Q5_K projection bodies for common scale/min acquisition, addressing and publication. Reuse `unpack_q4k_scale_mins` and existing format leaves; retain different subblock loops, high-bit packing and reduction schedules. |
| R06 | Metal IQ4/MXFP4 scalar consumers — H | Connect IQ4_NL/IQ4_XS/MXFP4 generic/reduce consumers to R03/R04 only where traversal matches. Keep codebooks, exponent/sign decoding and scale application as distinct static leaves; do not add a format switch per element. |
| R07 | Metal ternary single/pair traversal — H | Share matching `i2_s_linear`, `i2_s_pair_linear` and prequantized `_i8` components. Own ternary unpacking once; keep on-the-fly quantization versus prequantized acquisition, independent projection accumulators and four-term expression order explicit. |
| R08 | Metal RMS precomputed-sumsq siblings — H | Consolidate matching `*_1x_sumsq` / `*_rows_sumsq` partial-stat acquisition and publication. Preserve partial-count rules, the original tree, epsilon placement and empty-width behavior. |
| R09 | Metal RMS residual/scale/storage siblings — H | Audit ordinary/add/add-scale/F16-input bodies for one exact per-element epilogue and matching traversal. Make norm scale, residual addition, output scale and narrowing separate compile-time stages in their original order. Reuse the existing SIMD4 owner. |
| R10 | Metal RMS row/three-plane components — A | Compare row-reduce, SIMD4 and triple-SIMD4 normalization. Share matching per-plane acquisition/store leaves; retain independent reductions, vector widths and exact one-/three-plane scratch. No maximum-plane accumulator allocation. |
| R11 | Metal Q8 extended owner overlap — A | Compare `termite_q8_0_linear_r_ext_impl`, pair-linear and pair-activation owners. Share matching input acquisition/dot loops with exact row/projection counts if profitable. Their existing R2–R5 wrappers and prior RMS/output typing earn no new credit. |
| R12 | Metal layer-norm backward components — H | Share matching row contribution/address pieces in `termite_layer_norm_bwd_dgdb` and `_partial`, and audit matching statistics loads with `_dx`. Preserve the direct row fold, 64-row chunk folds, final chunk reduction and packed output offsets. Validate independent VJPs at chunk boundaries. |
| R13 | Metal convolution indexing — A | Compare `conv1d_f32`, `conv1d_im2col_f32`, `conv2d_f32` and existing audio renderers for matching coordinate/validity leaves. Retain direct, im2col, tiled and matrix schedules; qualify grouped, padded, strided and tail shapes. |
| R14 | CUDA activation-slice F32 warp variants — H | Consolidate the full `linear_q4_0_activation_slice_last_dim_f32_tile4` and `_w4` traversal with exact warp-count/reduction policy. Preserve activation/slice addressing, bias behavior and per-lane accumulator dimensions. |
| R15 | CUDA activation-slice packed-output components — A | Compare `_activation_slice_last_dim_q8_1_f32_tile4` with R14 for identical acquisition/dot pieces. Keep quantization, header publication and its reduction context separate; reuse existing Q8_1 writers only for the same auxiliary-header contract. |
| R16 | CUDA ordinary-input QKV widths/warps — H | Share matching `qkv_nobias_f32_tile4`, `_tile4_w4` and `_tile8` scaffolds. Preserve Q/K/V input reuse, three output widths, full/tail branches and each exact reduction tree. The already-shared packed-input tile4/8 owner is a reuse dependency. |
| R17 | CUDA packed multirow/w8 QKV components — A | Audit packed `_tile8_rows4` and `_tile8_w8` against that owner for repeated acquisition/store pieces. Keep row reuse, warp allocation and scratch separate when they differ. Do not merge protected FFN rows2 contexts. |
| R18 | CUDA packed Q6_K argmax components — A | Compare generic, A4B-2816 and E4B packed-Q8 argmax for matching dot/ranking leaves. Preserve 8/6/5-warp scratch, inactive-tail treatment, the exact-2816 packer and each fold. Fixed-shape specialization must survive source sharing. |
| R19 | CUDA TurboQuant split-stage codec twins — H | Prototype common matched traversal for `decode_turboquant_split_stage1_f32` and `_polar4_int8_identity_f32`. Keep actual staging, page mapping, counter/publication and codec-specific load points. Require seven-architecture reports before expanding the prototype. |
| R20 | CUDA TurboQuant prefill components — A | Audit `_prefill_tiled`, `_prefill_mma`, `_prefill_mma_m32` and generic decode for common address/load/codec leaves. Reuse the current fast-online owner where semantics match. Retain MMA tile, online-update ordering, barriers and scratch; the previous broad-helper register regression is a mandatory counterexample. |
| R21 | CUDA GLiNER tensor-core acquisition/storage — A | Compare encoder/base/m64 and boundary/base/compact/m64 F16 attention for repeated Q/K/V conversion, relative-tile acquisition and publication. Keep MMA shapes, relative-product precision, masking and all barriers in their current schedule owners. |
| R22 | CUDA DeBERTa tensor-core components — A | Audit `deberta_attention_tc_f16_m16n32` / `_m32n16` and R21 for matching relative-bias, mask and epilogue leaves. Preserve fragment layout, staging footprint, softmax fold and F16 conversion points. Do not generalize the admitted model/architecture envelope. |
| R23 | CUDA attention backward score components — A | Compare `deberta_attention_backward_scores_f32` / `_backward_f32` for identical score/mask/relative-index pieces. Preserve gradient accumulation, atomics, saved statistics and separate schedules. Independent finite differences/Torch gradients must cover every moved leaf. |
| R24 | CUDA KV suffix position/store components — H | Share matching device-scalar token-range and addressing pieces in ordinary and TurboQuant suffix writers. Reuse existing codec encoders; retain dense/paged mapping and packed-byte publication. Replay with changed device positions and values to detect scalar hoisting. |
| R25 | CUDA index/copy geometry components — A | Audit generic/2D transpose, slice and gather paths for shared index/acquisition pieces using existing geometry contracts. Retain tiled/coalesced rank2 kernels, BF16 conversion and integer index width. Cover empty, noncontiguous and tail cases without a runtime operation interpreter. |
| R26 | Named CUDA projection ABI expansion — H | Migrate selected remaining mutable-slot projection/QKV/argmax launchers to native-signature records in family groups. Preserve callers that update slots between launches. Count added native macros, factories and records fully; retain tuple packing when a named record adds no useful shared contract. |
| R27 | Named CUDA attention/KV ABI expansion — H | Apply the same native-owned record pattern to migrated attention/KV families. Keep device pointers as device pointers through capture, and keep dynamic shared memory, scratch lifetime and workspace in native launch policy. |
| R28 | Named CUDA training/primitive ABI expansion — H | Migrate repeated normalization-backward, optimizer and index-primitive argument setup where the native signatures genuinely share structure. Check wire widths, argument order and stack lifetime from the header owner; no second signature catalogue. |
| R29 | Remaining Metal native binding families — H | Extend the current forced-inline three-buffer owner with actual repeated two-/four-/five-buffer families and operation-specific constants/scratch. Keep offsets, dynamic threadgroup memory, resource retention and encoder boundaries at their current owners. Measure host encoding cost and command counts. |
| R30 | Consume geometry/descriptors in real operations — H | Connect selected normalization, row-copy/index and convolution consumers to existing descriptor/geometry validation. Preserve each operation's empty/error order and validate before writes/submission. Add fields only for real consumers in this batch; keep schedules and ABI layouts separate. |
| R31 | Native result-owner expansion — H | Audit `primUnaryOp`, `primReduceOp`, softmax/log-softmax, quick-GELU and compatible allocation adapters for the current `OwnedDenseOutput` transfer contract. Existing arithmetic owners stay their owners. Force inline and prove every partial allocation/shape failure before migration. |
| R32 | Native dense-input lease scaffolds — H | Share repeated one-/two-/three-input `denseTensorView` acquisition/cleanup in selected arithmetic, convolution and selection adapters with fixed-size/comptime leases. Preserve mapping and borrowed/owned distinctions; aliased inputs must neither double-free nor gain eager copies. |
| R33 | Native row/view/copy result geometry — A | Compare concat/slice/copy row and last-dimension adapters for shared validated region math and ownership setup. Reuse existing row/view owners; preserve donation, logical shape, noncontiguous materialization and overflow behavior. Adopt only with measured net simplification or eliminated allocation/copy traffic. |
| R34 | Metal parameter-field emission ownership — H | Prototype `termite_metal_linear_params` and `termite_metal_apply_layer_norm_params`, whose native typedef and MSL field declarations are currently repeated. Reuse existing field-list conventions to emit both spellings, with sizeof/offsetof/byte-layout probes and full cache dependency closure. Expand only after the two real consumers qualify. |
| R35 | Close coverage and retire superseded owners — H | Finish the per-family direct/oracle/replay/failure/route matrix; remove replaced private loops and stale declarations in the same cohort. Check exported symbols, standalone sources, dependency closure and actual dispatched paths. Source moves/generated-only changes earn no authored-body credit. |
| R36 | Qualify composition, ledger and commits — H | Freeze the final candidate; run full CPU/Metal/Spark qualification against R01's immutable baseline, regenerate/check images, collect task caches and publish source/body/host/test/generated/image ledgers. Finish coherent commits with a clean worktree. |

### Six integrated waves and deliverables

1. **Foundation and proof — R01/R02.** Publish the frozen inventory, original
   timing controls, owner assignment and independent fixture matrix. Require
   two genuine body-sharing prototypes with net deletion after new helpers.
2. **Codec/projection bodies — R03–R07, R14–R18.** Land common codec/dot leaves
   and all selected consumers together, followed by their native ABI consumers
   from R26/R29/R30. Budget 15–30 KB net authored body deletion across these
   groups, subject to the prototype audit.
3. **Normalization, backward and CPU adapters — R08–R12, R23, R31–R33.**
   Process matching normalization/storage/gradient pieces together, with
   R28/R29/R30 consumers. Budget 5–10 KB body deletion plus separately measured
   ownership/memory benefits; include any added CPU source in the net total.
4. **Attention/KV/convolution components — R13, R19–R25.** Qualify one static
   schedule at a time, then migrate compatible neighbours. Budget a further
   5–10 KB body deletion only after resource-equivalent prototypes; audit
   outcomes that retain schedules are valid and have zero savings.
5. **Complete launch/parameter ownership — remaining R26–R30/R34/R35.**
   Fold surviving repeated host setup into proven native-owned mappings. Budget
   15–30 KB net host/ABI deletion, charging all headers, factories, emitters and
   validation metadata. Reuse already-migrated consumers rather than adding a
   second layer around them.
6. **Integrated acceptance — R36.** Rebase no measurements mid-campaign.
   Compare the complete composition against the original frozen baseline,
   publish retained specializations and all failed experiments/controls, and
   commit the final qualified result.

These are one campaign with six reviewable cohort deliverables, so each family
can be implemented with its dependencies and fixtures without another planning
round trip. High-risk attention prototypes follow the more reliable scalar and
projection work. If R01/R02 cannot support the initial body budget, revise that
budget openly before scaling out; do not substitute many tiny wrapper changes
for the missing kernel-body reduction.

### Qualification, performance and accounting gates

Use the current runners and retained pinned inputs. Add a fixture as each
package starts, before moving its body. Cover F32/F16/BF16 where actually
supported, nonfinite/tie behavior, zero/tail dimensions, non-involutive index
maps, chunk/tile boundaries, guard regions, auxiliary outputs, capture replay
and allocator failures. Every moved arithmetic leaf needs an independent
reference with explicit precision rules. Require actual dispatch evidence for
both sides of an A/B comparison; a fallback pass does not qualify the intended
kernel. Retain all baseline/candidate source, image and binary identities.

* Direct kernels: 32 balanced arms, unchanged 5% ceiling, resource and guard
  checks, and recorded warmup/dispatch counts. Start new tiny CUDA cases with
  the existing 500 ms / 20,000-dispatch stable protocol. Choose sufficient
  measurements for larger kernels before editing them and keep the same
  protocol for baseline/candidate. Preserve failed receipts and investigate
  with reversed/identical-image controls instead of changing the limit.
* Models: retain all 13 encoder/reranker and two CPU/Metal decoder cases,
  four applicable Spark cases, and add fixtures for changed routes where
  available. Require finite byte-identical A/B outputs, 32 balanced arms,
  P50 ≤ 1.05 / P95 ≤ 1.10, and the independent full-logit decoder oracle.
  CUDA model admission stays at its existing contract; unavailable full-model
  checkpoints are reported explicitly and do not count as qualification.
* Native shape/resource protection: compare all seven CUDA architectures and
  actual SM120 execution on Spark. Any register/shared/stack/local increase
  requires investigation and stronger occupancy/performance evidence before
  acceptance. Keep exact native arrays; no new spills. Recheck Metal resource
  limits and native hot-body code/alignment when host code layout changes.
* Ownership/training: use pinned TinyLlama to enable the two resident Metal
  lifetime tests, actual CUDA prepared frames/capture tests, independent
  gradient checks, pinned Torch AdamW, clipping/durable resume and all partial
  allocation failures. Donation, frame eviction and metadata errors must
  release exactly one owner. Run the five Mac/two Spark 120-second soaks with
  separate finite probes and the existing allocator/RSS budgets.
* Product/artifacts: force semantic compilation of every migrated launch body,
  including non-default routes. Run native inference package and browser WASM builds
  early and finally; validate all generated/standalone sources, all three
  pinned CUDA images and the native include/cache closure. Use actual Metal
  runtime compilation when offline AIR tools are unavailable and label it
  accordingly.

A family is accepted only after correctness, lifetime, dispatch and performance
checks pass with net simplification or a separately justified memory-traffic
benefit. Its fallback outcome is to retain the current specialized body and
move on to the next measured candidate. Count all new helper/schema code;
report decoded Metal/native CUDA bodies, host code, CPU ownership, generator
owners, generated copies, tests/tooling, docs and images separately. The final
ledger is against `fdb6d7252f`, with no reuse of either earlier campaign's
savings.

### Residual campaign implementation and acceptance record

The implementation uses the existing five ownership layers. Format-specific
folds, native schedules and lifetimes remain explicit; the new shared owners
remove repeated traversal and binding code. Package evidence, immutable inputs,
failed prototypes and final receipts live under
`.benchmark-assets/1166/residual/`; `packages.json` maps all R01–R36 packages
to their owners or audited retention decisions. Final composition qualification
is complete. `qualification-index.json` checks the final model/direct/soak
receipts and the identical 1,192-file shipping source closure across Mac and
Spark; baseline-confirmed broader test failures remain explicit below.

| Packages | Final ownership boundary |
| --- | --- |
| R03–R06 | Static Metal scalar projection traversal with format-specific block folds; exact NR2/NR4 and NSG2/NSG4 reduction geometry. Q4_K/Q5_K generic consumers join that owner; distinct reduce/v2 schedules remain. |
| R07 | Shared ternary scalar codec and three matching traversals; pair-I8 retains its original loop after slower generic/vector prototypes. |
| R08–R10 | Existing RMS tree extended for precomputed statistics and F16 input, preserving original expression order; shared SIMD4 statistics/inverse leaf with original triple vector stores. |
| R11/R12 | Actual row/projection extents in the extended Q8 owner; shared sequential layer-norm dg/db row contribution with distinct direct/chunk/final reductions. |
| R14/R16 | Shared CUDA activation-slice partials and ordinary QKV schedule; exported activation epilogues and native warp counts remain explicit. |
| R19/R24 | Static TurboQuant split-stage traversal with codec/mapping leaves; shared live suffix-start calculation with distinct dense/paged/TQ publication. |
| R21/R22 | One native body per matching attention family, directly included in the exported kernels with compile-time schedule constants. Relative-product/MMA phases retain their original folds, barriers and exact scratch. |
| R26–R28 | Named host value records derived at compile time from native parameter macros; mutable/reducer slots point into the same live record. Generated or unique tuples retain their existing owners. |
| R29/R34 | Forced-inline two-/four-/five-buffer Metal binding owners at 166 sites; native/MSL parameter field lists with compiler layout assertions. Constants, offsets, scratch, encoder boundaries and retention remain at callers. |
| R30–R33 | Consumed row/copy/convolution geometry, existing inline dense-output ownership, and existing dense-input views with shared cleanup. No new runtime operation interpreter or input bundle. |
| R01/R02/R35/R36 | Immutable baseline and matched benchmark instrumentation; independent references, device replay/failure tests, native include/cache closure and composition accounting. |

R13, R15, R17, R18, R20, R23, R25 and R33 finish their native-body audits by
retaining distinct schedules or reusing owners already present at the baseline.
They earn no new kernel-body deletion credit. Direct/im2col/grouped convolution,
scalar-F32 versus packed-Q8 DP4A, multirow/warp-specific reuse, 8/6/5-warp argmax,
HD256/HD512 prefill, saved-score backward, generic versus coalesced transpose,
and CPU views versus Metal device/F16 slices have different arithmetic,
scratch or lifetime contracts. R18 still gains native ABI ownership under R26;
R13/R33 consume the geometry shared under R30. The activation-slice kernel
called “packed-output” in R15 actually consumes packed Q8 and publishes F32.

The attention prototypes exposed a CUDA compiler constraint: a forced-inline
device function containing shared arrays can increase SM100/110/120 address
instructions and registers. Bundling arrays into an exact-sized workspace did
not consistently solve that. The final body includes keep shared declarations
inside the native kernel while owning the arithmetic once; they deliberately
have no include guard. Both headers participate in JIT qualification identity
and the generic transitive-include closure check. This is authored sharing,
with the entire added header cost charged against the removed export bodies.

The Metal scalar prototype also preserves branch-selected row accumulators and
performs all SIMD reductions before publication. Direct array indexing slowed
Q4_1. RMS callers retain their original expression epilogue after a staged
rewrite changed one bit. Pair-I8 keeps its original scalar schedule after
both generic and vector-decoder versions slowed it. These rejected experiments
remain in the receipts; none are credited as accepted kernel consolidation.

CPU primitive qualification also caught a 5 ns P50 regression at 1×8, despite
identical adapter instructions. Entry alignment and static-width packing
prototypes were slower and were rejected. The retained implementation keeps
its original short-row allocation classes and loops, with an explicit
`noalias` contract on the fresh backward-output planes. Both callers allocate
those planes separately from inputs; packed output subranges are disjoint.
All five primitive cases pass the original protocol, and the three short-row
cases also pass longer-warmup controls. The output/error behavior is unchanged.

### Residual source accounting

The complete authored production reduction against `fdb6d7252f` is
**55,668 bytes / 1,040 lines**, including every replacement owner, native ABI
macro, body header and shared contract. Previous campaign savings are excluded.
The original 40–80 KB planning range is replaced by these measured figures.

| Authored production region | Net bytes removed |
| --- | ---: |
| Metal shader source as stored in the C translation unit | 9,808 |
| CUDA device source, including ABI and both body headers | 17,819 |
| Metal host bindings | 21,980 |
| CUDA host launch ownership | 6,681 |
| CPU arithmetic/result ownership | 1,075 |
| Shared contracts, geometry and build identity | −1,695 |
| **Total** | **55,668** |

The decoded Metal shader separately shrinks from 948,535 to 936,666 bytes
(11,869 bytes), while growing from 7,508 to 7,642 physical lines. That decoded
measurement is not added to the authored total: C literal escaping and shared
field emission have different source costs. CUDA body moves receive credit
only after charging the entire added headers. Generator owners/generated
copies have zero new reduction. Inline tests, benchmark/qualification tooling
and documentation are reported separately in the complete source ledger.

Generated GPU image size is effectively unchanged: fatbin −552 bytes,
portable PTX +1,556 bytes and an identical SM89 cubin, for 1,004 bytes added
in total. Source sharing preserves native machine code; it does not claim a
corresponding GPU binary-size reduction. The final per-file hashes and complete
source/image ledger are retained in `.benchmark-assets/1166/residual/`.

### Residual qualification results

Final Mac binaries use `ReleaseFast`, Metal enabled and CUDA disabled, matching
the platform defaults. Both immutable-baseline and candidate benchmarks include
the same setup instrumentation outside timed loops. Results below use the final
CPU alias contract; earlier whole-model passes remain separate receipts.

| Final Mac check | Result |
| --- | --- |
| 13 encoder/reranker cases, 32 arms | Finite byte-identical outputs; maximum P50 ratio 1.0031, P95 1.0146. Actual backend and command-count profiles match. |
| CPU/Metal decoder cases, 32 arms | Exact A/B outputs; maximum P50 ratio 1.0004 and P95 0.9992. |
| Independent decoder oracle | All 160,000 logits per backend pass at `0.002`; maximum absolute error below `0.000759`. |
| Five CPU backward primitives | Exact outputs; maximum P50 ratio 1.0159 and P95 1.0118. Three longer-warmup controls also pass, maximum P50 1.0417. |
| Three separate host-encoding probes | Same submit/compute/blit profiles; maximum warmed host-encode CPU ratio 1.0026. Trace probes are separate from ordinary model timing. |
| Five 120-second soaks | Finite independent output probes, alternating/retained output checks, stable warmed payload/RSS counters and zero tracked host allocations at teardown. |
| Native hot-body protection | Both GEMM bodies and attention retain their 64-byte entries and identical non-relocation instructions. |

The 1,150 Metal cases all pass byte parity, guards and resource checks in both math profiles. Their identical-source control passes. Three
short-batch timing flags (one empty-row Q8 launch and two unchanged AdamW
profiles) are retained explicitly. Seventeen targeted cases using longer timed
batches pass identical-source, reversed and ordinary comparisons under the same
5% ceiling; maximum ordinary ratio is 1.0122. The original failed receipts are
not rewritten as passes.

All four final Spark model cases pass finite byte-identical outputs and
balanced 32-arm timing: maximum P50 ratio 1.0162 and P95 1.0030. Requested
backend and command profiles match. Three CUDA prepared-frame/request-reset
lifetime tests also pass without skips.
Both 120-second CUDA soaks pass finite probes, alternating/retained outputs,
stable warmed counters and zero tracked host allocations at teardown: 43,338
encoder iterations and 10,668 reranker iterations. Together with the Mac runs,
all seven required soaks pass.

All 360 direct CUDA cases pass exact A/B output, guards, captured replay and
the 5% timing gate. These include independent staged-F16 attention references,
changed device-position/value KV replay and the full packed-argmax schedule
fixtures. Nine native-fatbin ABI tests, three CPU backward checks and two
pinned Torch CUDA optimizer/clipping/resume tests pass without skips.

All seven CUDA architecture profiles expose the same 439 kernels with no register, shared,
stack or local-memory increase. GLiNER, DeBERTa, activation-slice and ordinary
QKV exports have baseline-identical SASS on every target. Two TurboQuant
split-stage exports and the dense KV suffix writer have changed SASS hashes on
some targets, with identical instruction counts and resources. Actual Spark
execution uses packaged SM120 code on GB10/SM121. The bad-reference and
missing-write controls both fail as intended before timing; A/B and replay
comparisons remain byte-exact even where independent staged-F16 oracles have an
explicit rounding tolerance.

The broader legacy training suites retain confirmed baseline failures. Legacy
GLiNER2 selects seven tests: three pass and four fail on unsupported integer
indices through the generic gather route, identically on the frozen baseline.
The required-hardware GLiNER2.5 selection has 78 passes, two skips and six
failures on both revisions, with identical failed test names: budget-error
mapping, three BLAS-counter expectations, fused-attention availability and
fused-trainer graph admission. These broader suites are not claimed as green;
the consolidation preserves their existing admission envelope.

Portable PTX runtime qualification is blocked by a pre-existing host/toolkit
mismatch: both the frozen baseline and candidate emit PTX 9.2, while Spark's
CUDA 13.0 driver reports support through PTX 9.0. Both direct load controls fail
with `CUDA_ERROR_UNSUPPORTED_PTX_VERSION`. Native fatbin execution is qualified
separately; this limitation is not recorded as a passing portable runtime test.
All three artifacts pass pinned `regen-cuda-artifacts.sh --check --all`.
The artifact contract remains pinned to CUDA 13.2. CUDA model admission also
retains its existing envelope, including the unadmitted TinyLlama fixture.

The final CPU/Metal training and lifetime selection has 46 passes and three
skips: two optional published GLiNER checkpoint fixtures are absent and the
CUDA-only frame check is disabled in this build. Both pinned TinyLlama resident
Metal encoder and decoder lifetime tests execute and pass.

All 67 generated-source checks and 54 generated/rendered Metal shader
compilations pass. The Metal result is actual device runtime compilation through
the retained bridge; offline AIR tools are unavailable. Native product and
browser WASM checks, all 15 artifact/accounting/qualification script tests, and
18 final allocation/geometry/native-binding consumer tests pass. Sampling,
large penalty histories, convolution and metadata/error paths run on their real
consumers rather than inferred wrappers.

## Proposed structural kernel and adapter consolidation campaign

This is a new campaign against `5f9bc994ed`, after the completed R01–R36
campaign. Its 55,668-byte / 1,040-line reduction is frozen and excluded from
new savings. The plan contains **40 work packages in eight waves**, targeting
**35–60 KB net authored production reduction**: approximately 20–35 KB of
native Metal/CUDA source and 15–25 KB of host/adapter code after charging every
new helper, contract and field list. These are provisional whole-campaign
ranges, not promised or additive per-package savings. S04 replaces them with
prototype budgets before broad migration. A slower or larger abstraction is
rejected even if that reduces the final deletion total.

The next boundary is matching execution phases across existing specialized
families: packed projection traversal, matrix staging, attention phases, and
validated layout geometry. Keep schedule dimensions, codecs, folds, masks,
epilogues, publication and ownership explicit. Reuse the current static owners,
`launch_abi.ArgumentsFor`, `OwnedDenseOutput`, dense-input leases and
`kernel_geometry`; add no runtime kernel interpreter or universal tensor owner.

### Inventory and exclusions

A fresh brace-delimited inventory identifies **37 distinct maintained exports /
121,071 exposed source bytes**. Generated regions are excluded and each export
occurs once. The inventory measures candidate declarations/bodies, including
unique phases; it is not a deletion estimate. Metal entries decode maintained C
string fragments for explicit bodies; this does not replace preprocessed full
shader accounting. The inventory script, source hashes and per-export sizes
are retained under `.benchmark-assets/1166/structural-consolidation-plan/`.

| Candidate family | Exports | Exposed bytes | Packages |
| --- | ---: | ---: | --- |
| Metal Q4_0 aligned/tail/unrolled matrix | 3 | 10,766 | S05/S06 |
| Metal Q8 MMV storage/single/pair/QKV | 5 | 14,296 | S07/S08 |
| Metal Polar4/Turbo3 attention spans | 2 | 7,913 | S09 |
| Metal segmented and ModernBERT forward attention | 4 | 18,792 | S10/S11 |
| Metal grouped matrix32/matrix64 attention | 2 | 12,325 | S12 |
| Metal vision HD64 Q8/Q32 attention | 2 | 7,882 | S13 |
| CUDA packed Q4_0 projection/activation | 4 | 10,070 | S14/S17 |
| CUDA fixed E4B attention/down projection | 5 | 10,814 | S15/S16 |
| CUDA Q8 ordinary-input pair/QKV | 2 | 4,328 | S18 |
| CUDA Q6_K E4B projection/argmax | 2 | 5,598 | S19 |
| CUDA ordinary argmax reduction/publication | 2 | 4,538 | S20 |
| CUDA block/local attention | 2 | 6,400 | S21 |
| CUDA dense fast prefill/device-scalar decode | 2 | 7,349 | S22 |
| **Total** | **37** | **121,071** | |

The source also contains 13 six-buffer and 15 seven-buffer contiguous Metal
binding runs, plus remaining smaller runs. Those counts include diagnostic or
benchmark callers and are upper bounds, not 28 promised runtime migrations.
The CUDA compute adapter has 99 matching device-allocation/errdefer scaffolds;
S29/S30 select compatible ordinary output owners rather than migrating all 99.

Several differences are especially narrow: packed CUDA tile4 versus tile4_w8
changes the exact partial-array warp extent and final reduction specialization;
the E4B attention 2048/4096 bodies differ only in their static block count.
Metal aligned versus aligned-tail matrix kernels share staging/MMA while
retaining different tail acquisition/publication. These are the first
prototypes. Similarity alone does not establish rounding or schedule parity.

Do not reopen R20's TurboQuant MMA/prefill merge, R17's multirow register-tile
merge, R18's generic/A4B/E4B argmax schedule merge, or the GLiNER encoder
short/M64 merge without new evidence. They are outside this batch. S19 instead
compares the same five-warp E4B dot phase in ordinary projection and argmax;
S20 shares only the matching reduction/publication after distinct codec dots.
S14/S17 compare packed-Q8 consumers with each other, rather than repeating
R15's rejected scalar-F32 versus packed-Q8 acquisition merge. Previously shared
scalar quantization, RMS, codec, ranking and generated schedule leaves earn no
new credit. No model, dtype or architecture admission is expanded by this plan.

### Wave A — freeze and prove the boundaries (S01–S04)

| ID | Work package and concrete result |
| --- | --- |
| S01 | Freeze `5f9bc994ed` sources, all three images, decoded/preprocessed shader, Mac/Spark binaries, fixtures and route counters. Preserve the R campaign as history; establish new A/B reference binaries with identical benchmark instrumentation. |
| S02 | Map every selected native body, matching phase and host call site to one source-budget owner. Separate generated text, parameter lists, new headers, inline tests and tooling. Record which of the 99 output scaffolds and remaining Metal binding runs have compatible lifetime/argument contracts. |
| S03 | Extend existing fixture builders for the new matrix/packed-projection/attention routes. Include independent references, poison/guard checks, partial-allocation failures, changed-device-scalar replay and forced route selection before source migration. Preserve existing timing protocols and failed receipts. |
| S04 | Prototype Metal aligned/tail Q4 matrix staging, CUDA packed tile4/warp4/8 traversal, and one resolved broadcast-geometry consumer pair. Measure net source cost, exact native arrays, seven-target CUDA resources/SASS and host/whole-model overhead. Admit owners separately; replace the provisional budget with measured cohort ranges. |

### Wave B — Metal projection and compressed-span owners (S05–S09)

All native shader changes are in `src/backends/metal_kernels.m`. S06 depends
on S05; S08 depends on S07. Each pair shares one budget.

| ID | Work package and preserved specialization |
| --- | --- |
| S05 | Share the aligned/aligned-tail `q4_0_linear_mm_sg` staging and MMA schedule. Keep full-row direct stores versus tail scratch publication, source-row clamping, barriers and existing scratch aliasing explicit static phases. |
| S06 | Reuse matching acquisition/staging pieces in `_unrolled`; retain its unroll and accumulator/fragment schedule. A common whole body is optional and requires identical code/resources. Do not merge distinct register schedules to force a deletion target. |
| S07 | Share `q8_0_linear_mmv` / `_in_f16` storage acquisition and matching `linear_rms_scale_mmv` phases through static input/epilogue leaves. Keep conversion, scaling, fold order and exact two-row accumulators. This is the MMV family, not the already-consolidated extended Q8 owner. |
| S08 | Reuse matching MMV block-dot and reduction phases for `q8_0_pair_linear_mmv` and the Q/KV branches of `q8_0_qkv_linear_mmv`. Instantiate actual single/pair projection extents; preserve unequal Q/KV widths, offsets and branch-specific scratch. Never allocate a maximum three-plane workspace for a two-plane caller. |
| S09 | Share Polar4/Turbo3 span traversal, masking, max/denominator folds and publication with static codec/query-preparation leaves. Turbo3 alone owns its 32 residual projections; Polar4 must acquire no residual array or extra live values. Preserve repeated-score arithmetic and all nonfinite handling. |

### Wave C — matching Metal attention phases (S10–S13)

These are phase owners for the listed schedules, not a single universal
attention kernel. Forward and training storage/signature differences stay at
their actual callers; existing backward kernels retain their schedules.

| ID | Work package and preserved specialization |
| --- | --- |
| S10 | Share matching `sdpa_f32_segments_tiled` / `modernbert_training_attention_flash_forward` Q/K/V staging, MMA and online-output phases. Keep packed versus separate QKV acquisition, signed versus unsigned range validation, fragment initialization and training statistics publication distinct. |
| S11 | Reuse matching scalar/row pieces between `sdpa_f32_segments` and `modernbert_training_attention_forward`. Preserve segment membership, local-window positions, training outputs and the original exponential/reduction sequence. S10/S11 share an inventory group, not an assumed common schedule. |
| S12 | Share matched phases in `sdpa_f32_grouped_matrix32` / `_matrix64`, with actual query/key tile extents and fragment counts. Retain each schedule's staging, barriers and online update. Exact scratch and register pressure are admission gates. |
| S13 | Share repeated HD64 vision Q8/Q32 address, mask and compatible score/output phases. Retain query-tile reduction/scratch distinctions. The Q16 flash kernel is excluded; add no wrappers for leaves already shared. |

### Wave D — packed CUDA projection phases (S14–S20)

Device bodies are in `src/ops/cuda/artifacts/inference_cuda_kernels.cu`, with
small private body headers only where they simplify ownership. Reuse existing
DP4A/codec/store leaves. No new exported kernel names or dispatch thresholds
are needed. S16 depends on S15, and S17 depends on S14.

| ID | Work package and preserved specialization |
| --- | --- |
| S14 | Own packed-input `linear_q4_0_q8_1_f32_tile4`, `_tile4_w8` and `_tile8` traversal once with static column/warp extents. Preserve full/tail branches, exact `[Cols][Warps]` partials, DP4A acquisition and original final sums. |
| S15 | Share fixed E4B `tile4_e4b_attn_2048` / `_4096` acquisition/dots with compile-time 64/128 block counts. Their exact model shapes and argument handling remain specialized. |
| S16 | Connect `tile4_w8_e4b_down`, `_w10_e4b_down` and `_w8_e4b_down_rows` at matching S15 phases. Preserve 8/10-warp scratch, fixed versus live row selection and all existing shape gates. No padded maximum-warp arrays. |
| S17 | Connect packed `activation_slice_last_dim_q8_1_f32_tile4` to S14's matching partial-dot owner. Keep the exported activation/slice epilogue if moving it changes libdevice code or registers, as in R14. No extra input quantization or header change. |
| S18 | Share ordinary-input Q8 `pair_nobias_f32_tile4` / `qkv_nobias_f32_tile4` input/dot phases with exact projection counts. Keep unequal output widths, tail masks, accumulators and warp folds explicit. This differs from R16's ordinary Q4 QKV owner. |
| S19 | Share only the five-warp, ten-block Q6_K E4B dot/partial phase between `q8_1_f32_tile8_e4b` and `q8_1_argmax_rows_stage1_tile8_e4b`. Keep linear output stores and argmax ranking/suppression contracts in separate original epilogues. Generic and A4B argmax schedules remain separate. |
| S20 | Share the matching four-column ordinary argmax block-reduction/ranking/publication phase in Q8_0 and Q4_K consumers. Distinct acquisition loops and codec folds remain. Reuse existing ranking leaves; preserve ties, suppression, inactive tails and the `[4][256]` scratch/fold exactly. |

### Wave E — CUDA attention phases and Metal host bindings (S21–S26)

| ID | Work package and preserved specialization |
| --- | --- |
| S21 | Share matching dot/reduction/value-fold phases in CUDA `attention_f32_block` / `laya_local_attention_f32`. Static range/mask/bias policy preserves full versus local traversal, head layout, `-FLT_MAX` versus `-INFINITY`, all-masked behavior and existing barriers. Keep different exponential validity checks explicit. |
| S22 | Share matching dense fast prefill/device-scalar decode online traversal in `gqa_attention_prefill_fast_f32` / `gqa_attention_decode_scalars_fast_f32`. Keep scalar acquisition live on every replay, original causal/cache ranges and online folds. TurboQuant prefill schedules are excluded. |
| S23 | Add actual six-/seven-buffer forced-inline Metal binding owners for repeated runtime multi-output/attention calls. Filter benchmark-only sites from production credit. Preserve every offset, parameter index, dynamic scratch binding, resource retention and encoder boundary. Leave the unique nine-buffer sequence alone unless another real consumer appears. |
| S24 | Migrate compatible remaining two-/three-/four-/five-buffer runs whose constants are separate or multiple. Extract buffer-only inline leaves beneath current binding owners where profitable; constants remain in caller order. No runtime argument list, side-effect reordering or duplicate helper catalogue. |
| S25 | Extend existing native/MSL field-list ownership to selected repeated QKV/head-norm parameter records used by this batch. Emit both layouts from one list and assert every offset/size. Charge macro/assertion cost; retain the existing representation if no useful shared contract results. |
| S26 | Apply the same field ownership to selected segmented/span attention records, preserving signed range/position wires and alignment. Validate native/MSL round trips on actual consumers. S25/S26 are drift prevention, not guaranteed source savings; their added bytes reduce the overall budget. |

### Wave F — CUDA ABI and ordinary output ownership (S27–S30)

| ID | Work package and concrete consumer |
| --- | --- |
| S27 | Extend native parameter macros plus `launch_abi.ArgumentsFor` to repeated forward RMS/layer-norm families in `src/ops/cuda/kernels.zig`. Reuse value-record factories only for identical signatures. Keep validation/conversion/error order and per-launch statistics at callers. Native normalization bodies already shared earn no credit. |
| S28 | Migrate repeated RoPE/device-scalar and compatible gather/transpose/broadcast launch argument setup to native-owned records. Retain unique tuples where a record only adds code. Never copy a mutable slot away from the storage used by a later launch or graph replay; device scalar pointers stay device pointers. |
| S29 | Introduce one private forced-inline ordinary CUDA output lease around the existing shape allocation, `allocDeviceBuffer` and `createTensorWithDType` transfer in `cuda_compute.zig`. Abort uses the original free/deferred-free policy; successful creation transfers shape/buffer exactly once. It adds no allocation, synchronization, stream change or device pool. Prove partial failures before migration. |
| S30 | Migrate compatible primitive unary/softmax/reduction and selected F32/BF16 normalization output adapters to S29. Measure allocator calls and source cost including the lease. Prepared outputs, pinned views, BF16 mirrors, captured frame leases and donated buffers keep their current owners unless separately proven compatible. No blanket migration of all 99 scaffolds. |

### Wave G — consume shared geometry in adapters and descriptors (S31–S36)

Put compact pure geometry in the existing `src/ops/kernel_geometry.zig`.
Backend adapters first resolve their own logical/storage shape conventions;
common records describe the resolved operation. The refactor preserves existing
error/decline order and does not unify backend dtype/admission policy.

| ID | Work package and actual consumers |
| --- | --- |
| S31 | Resolved broadcast geometry: rank, axis mapping, input/output extents and checked counts/strides consumed by CPU, Metal and CUDA `primBroadcastInDimOp`. Preserve graph rank-zero scalars versus CUDA `[1]` storage, duplicate-axis checks, existing singleton/divisible-extent rules and backend-specific dynamic-dimension resolution. Keep incompatible resolution paths local. |
| S32 | Resolved transpose geometry: checked permutation/output extents/strides consumed by primitive CPU/Metal/CUDA transpose adapters. Native coalesced rank-two and generic rank-eight CUDA kernels remain separate. Validate before allocation/submission and preserve integer data widths. |
| S33 | Resolved positive-stride slice geometry: shared output extents, source origin and strides in real CPU/Metal/CUDA slice adapters. Preserve CPU views, Metal donation/selective conversion and CUDA copies. Negative/unsupported stride or backend-specific clipping paths remain explicit. |
| S34 | Axis-gather geometry: prefix/axis/suffix products, scalar-index shape normalization and output shape shared by the compatible ordinary gather paths. CPU gather-ND/source-backed paths stay separate. Preserve integer versus F32 index wires; this does not silently enable CUDA's currently unsupported integer generic-gather route. |
| S35 | Replace duplicated layout derivation in `kernel_descriptor.lower` and assigned Metal/CUDA invocation with consumed S31/S32 records. Store only metadata needed by real consumers, inline in the existing tagged descriptor. Backend admission still owns quantization, dtype, device residency and auxiliaries; null-output admission remains allocation/readback free. CPU may retain its current layout decline. |
| S36 | Connect compatible CPU allocating slice/gather/concat results to existing `OwnedDenseOutput` while consuming S31–S34 geometry. Keep zero-copy/source-backed views at their current lifetime owner. Remove superseded local geometry/cleanup only after actual callers migrate; no new dynamic input bundle or ownership hierarchy. |

### Wave H — close and qualify the composition (S37–S40)

| ID | Work package and acceptance artifact |
| --- | --- |
| S37 | Close native include/JIT/watch dependencies, exact symbols, ABI offsets and owner coverage. Use the current transitive-include checks; preprocess Objective-C before extracting the full shader so field macros are present. Verify generated regions remain fresh and retire replaced private bodies in their owning cohort. |
| S38 | Run final direct/reference/error/replay/route tests for every moved phase and all existing Metal/CUDA regression cases. Require actual hardware execution and explicit route assertions for specialization-only cases. Preserve rejected candidates and timing controls; no rerun-until-pass acceptance. |
| S39 | Qualify final composition on Mac CPU/Metal and Spark CUDA: all 19 applicable model cases, independent decoder logits, CPU backward primitives, host-encoding probes, training/lifetime selection, seven 120-second soaks, native/WASM and generated/runtime shader checks. Regenerate/check all three pinned CUDA images and compare resources/code on all seven targets. |
| S40 | Publish net source and image ledgers against `5f9bc994ed`, owner/package outcomes and source/binary identities. Separate authored native bodies, host code, contracts, generated owners/copies, tests, tooling and docs. Finish coherent reviewable commits, clean worktree and cache GC on both machines. No earlier R/N savings are counted again. |

### Execution order and acceptance

After S01–S04, implement each native family as one measured cohort. S05→S06,
S07→S08, S14→S17, S15→S16, S29→S30 and S31/S32→S35 are explicit dependencies.
S36 follows the applicable geometry owners. Qualify a cohort's direct paths,
resources and relevant ownership checks before connecting further consumers.
Run the full model/soak composition at S39; avoid repeating every unrelated
full suite after each mechanical adapter migration. If an owner fails, keep its
specialized schedule and record zero deletion credit while continuing with the
independent cohorts. Do not let a failed common attention owner block profitable
packed projection or host-geometry work.

* Correctness: byte-identical A/B outputs/replay by default, independent oracles
  with their existing explicit precision contracts, output poison/guards, and
  meaningful allocation/metadata failures. Cover Q4 matrix rows 1/31/32/33/63/64/65,
  packed columns 1/3/4/5/7/8/9, actual 4/5/8/10-warp profiles, unequal Q/KV
  dimensions, exact 2048/4096 shapes, full E4B argmax, multiple/empty segments,
  all-masked/nonfinite attention, local-window boundaries, rank-zero/rank-eight
  geometry and overflow. Force unsupported paths to decline before writes.
* Performance: preserve the balanced 32-arm protocols, direct-kernel 5% ceiling
  and model P50 ≤ 1.05 / P95 ≤ 1.10. Collect identical-source/reversed controls
  where noise requires them under a declared equal protocol. Keep separate
  host-encoding/preparation probes and exact submit/compute/blit counts.
  Protect CPU GEMM/attention vector code and alignment and the tiny backward
  primitives that caught the R campaign's code-layout regression.
* Native shape: exact schedule-specific arrays, barriers, conversion sites and
  accumulation order. Prefer baseline-identical CUDA SASS; compare instruction
  counts, registers, shared/stack/local memory and occupancy across all seven
  targets. A register/spill/resource regression triggers redesign or retention,
  rather than acceptance based only on Spark's timing. Direct native body
  includes remain available when inlined device functions change shared-storage
  lowering; all header bytes and cache dependencies count.
* Ownership: no extra allocations, copies, synchronization or device readbacks
  in an admitted path. Verify fail-every-host-allocation plus device-budget
  failures, capture/reset, retained outputs beyond frame-pool capacity, BF16
  mirror lifetime and teardown. Keep fresh outputs' disjointness contracts and
  aliased/borrowed inputs distinct. Any claimed memory-traffic improvement must
  be measured independently of source deletion.
* Baseline limits: reproduce the existing legacy GLiNER2 integer-gather failures
  and six broader GLiNER2.5 admission/diagnostic failures rather than relabeling
  them as passes. New failures are regressions. Spark's current driver rejects
  both baseline and candidate portable PTX 9.2; qualify native fatbin execution,
  preserve that blocked-runtime result, and still require all pinned artifact
  freshness checks. Run portable runtime parity if a compatible driver becomes
  available. Keep the existing model/checkpoint admission envelope.

This batch is complete when every package has an implemented-and-qualified or
measured-retention outcome, superseded code is removed, the final combined
candidate passes its applicable gates, and the ledger includes all replacement
costs. The deletion target guides prioritization; it does not override the
kernel performance and ownership contracts.

### Structural campaign implementation record

The S campaign measures against `5f9bc994ed`. Earlier R/N reductions are
excluded. All 37 selected exports now consume a shared native body or matching
phase. The implementation also consolidates 72 Metal buffer-binding sequences,
21 CUDA launch adapters, 22 ordinary CUDA output implementations, and 12 CPU
allocating layout sites. S01–S39 implementation and final composition
qualification are complete; the ledger and qualification below close S40.

| Packages | Implemented ownership and retained specialization |
| --- | --- |
| S01–S04 | Frozen 1,192-file input closure, three CUDA images and qualified Mac/Spark binaries; unique-export and host-site inventories; independent fixtures and three qualified prototypes. |
| S05/S06 | Static Q4 matrix modes retain aligned, tail and scalar-unrolled acquisition, fragment extents, publication and the actual 6 KiB dynamic scratch contract. |
| S07/S08 | Q8 MMV single-input F32/F16/Q branch and pair/KV branch owners retain their two-row schedules. RMS MMV keeps its four-row fold and reuses existing codec leaves. |
| S09 | Polar4 and Turbo3 share span traversal through static codecs. Polar4 has no residual array; Turbo3 keeps exactly 32 projections and the existing scalar decoder. |
| S10/S12 | Five maintained source-fragment owners share segmented/training QK/PV and grouped-matrix range initialization, rescale and publication. Expanded native text is byte-identical; signed control records, dropout, fragment initialization, BK32/BK64 staging and every original array/barrier remain at the kernel. |
| S11 | Two source-fragment owners share scalar online weights and PV. Both complete expanded scalar kernels match the baseline byte-for-byte. A whole-body template was not kept after a raw empty-range timing flag near the 5% limit; the source-phase shape preserves the original native schedule. |
| S13 | Static one-/four-query-per-simdgroup vision specialization. The exported entries keep their exact 8/32-by-32 weight arrays and 32-by-64 KV staging array. Q16 flash remains separate. |
| S14–S17 | Packed Q4/Q8 partial traversal has static column/block/warp extents. Fixed E4B attention/down variants and activation consumers keep their original 4/8/10-warp arrays and epilogues. |
| S18–S20 | Three private native body headers share ordinary Q8 projection, five-warp Q6 E4B dot/partials, and ordinary Q8/Q4K argmax publication. Linear versus argmax epilogues, codec folds, suppression and ties remain distinct. |
| S21/S22 | Two private native body headers share block/local attention and dense fast online attention. Full/local mask sentinels and validity rules remain static; decode device scalars are reread before the original guards. |
| S23/S24 | Forced-inline two- through seven-buffer prefix owners serve 72 actual runtime sequences. Offsets, binding order, constants, dynamic scratch and resource lifetimes are unchanged. Diagnostics, unused legacy dispatchers, single-buffer runs and the unique nine-buffer sequence retain their original code. |
| S25/S26 | Four existing C/MSL field lists now own 37 QKV, head-norm/RoPE, span and segmented fields. Actual C/MSL compilation checks every field offset and record size. The added assertion/owner source is fully charged. |
| S27/S28 | Twenty native signature macros derive named records for ten norm, eight RoPE, two gather and one batched-dot launcher. The norm factory binds pointers by native field name. Unique transpose/broadcast/scatter tuples stay local where adding a record would add machinery. |
| S29/S30 | A private inline ordinary F32 result owner preserves shape allocation, existing device allocation, failed-launch cleanup and successful tensor transfer. Twenty-two compatible implementations include unary, softmax, reduction, norm and selected attention/RoPE results. Prepared frames/views, BF16 mirrors, donation and dynamic capture owners stay separate. |
| S31–S35 | Resolved broadcast targets, transpose permutations, positive slice axes and ordinary gather regions/output shapes have real CPU/Metal/CUDA consumers. Descriptor transpose lowering consumes the same validation; assigned accelerator invocations share native dimension conversion without storing another copy of target wire metadata. |
| S36 | Existing `OwnedDenseOutput` owns 12 allocating gather/slice/concat/transpose storage sites and 13 transfer branches. Source-backed and zero-copy views retain their previous owners. |

Geometry sharing follows each adapter's shape resolution and representation
admission. CPU gather-ND, integer storage, logical views, Metal selective
conversion, CUDA rank-zero logical scalars backed by `[1]`, divisible-extent
broadcasts, and Metal's historical slice clipping retain their own policies.
The positive-step slice calculation avoids signed overflow in ceil division.
No runtime kernel interpreter, maximum-sized schedule arrays, extra tensor
allocation, device synchronization or stream transition was introduced.

The new body headers are explicit JIT identity dependencies. Artifact checks
resolve native parameter macros and compare scalar PTX wire types, alongside
transitive-header coverage. The source ledger counts shader-fragment definitions
and their contract comments as maintained device source, and conserves all
bytes when separating shader, host bindings, tests, tooling and generated text.
A shorter expanded shader is reported separately from maintained-source savings.

The direct Metal protocol records both pipeline creation orders with identical
sources, shared timing buffers, math modes, dispatch counts and balanced timing
arms. The per-case gate remains 1.05 on the geometric mean of the two ratios;
both orders must pass references, guards and resource checks. An unchanged IQ4_XS
kernel exposed a 6–7% second-pipeline bias: reversing sources retained the bias,
while reversing creation order reversed it. The ten-profile crossover's maximum
source-effect ratio was 1.007822. All original timing flags remain in the
receipts, and final composition uses the frozen final shader in both orders.

Campaign receipts, failed prototypes and controls remain under
`.benchmark-assets/1166/structural/`. Important entries are `packages.json`,
`STATE.md`, `ledger-current.json`, `metal-mma-phase-identity.json`,
`scalar-native-identity.json`, `launch-abi-migration.json`,
`metal-binding-migration.json`, `output-migration.json`, and the final
`mac-composition/` / `cuda-composition/` results. `final-source-identity.json`
verifies 1,187 actual source inputs against both qualified binary snapshots;
backend image identities are recorded separately.

The final maintained-source ledger includes all replacement bodies, headers,
macros, field contracts and JIT dependencies. Positive numbers are removals.

| Production group | Net bytes removed | Net lines removed |
| --- | ---: | ---: |
| Metal shader source | 17,818 | 117 |
| CUDA device source, including five new headers | 16,712 | 452 |
| Metal host bindings | 5,690 | 176 |
| CUDA launch adapters | 1,470 | 22 |
| CPU/adapters and result ownership | 5,427 | 130 |
| Shared contracts and build identity | -5,123 | -91 |
| **Total authored production** | **41,994** | **806** |

Inline tests add 24,668 bytes / 427 lines; test and benchmark tooling adds 4,331
bytes / 75 lines. Documentation is separate. Generated regions are unchanged.
The fully expanded shader shrinks from 936,666 to 927,682 bytes; these 8,984 bytes
are not added to the maintained-source total. All three CUDA images were
regenerated with pinned NVCC 13.2.86 and pass `--check --all`. Their sizes remain
56,169,632 bytes (fatbin), 6,380,480 (SM89 cubin), and 11,492,064 (portable PTX).
The cubin and PTX are byte-identical to the baseline; source consolidation does
not imply smaller compiled specialization images.

Final acceptance:

- **1,332 Metal cases** pass independent references, guards and resource checks
  in both pipeline creation orders, with every paired source-effect ratio at
  most 1.05. This includes the existing 1,150 cases, 172 new cases and ten actual
  6 KiB scratch controls. The largest paired ratio is 1.038279 on an empty span
  case (about 69 ns); raw per-order timing flags remain preserved.
- **443 CUDA cases** pass independent references, guards and the 1.05 timing
  gate on Spark GB10, including 83 new cases. All 439 exports remain present on
  SM75/80/89/90/100/110/120. SASS hashes match everywhere except the SM120 E4B
  ten-warp down variant; that variant retains 328 instructions, 44 registers,
  zero stack/local bytes and 1,184 shared bytes. Its additional full-width
  N2560/K10240 case passes A/A and A/B numerical and timing controls. No target
  increases register, stack, shared/local-memory or instruction-count budgets.
- **All 19 model cases** pass byte-exact baseline comparison, route assertions,
  P50 <= 1.05 and P95 <= 1.10: 13 Mac encoder/reranker, two Mac decoder and four
  CUDA encoder/reranker cases. Worst P50 is 1.038906 and worst P95 is 1.030714.
  Five CPU primitive, three long-warm CPU and three host-encoding probes pass.
  The three protected CPU hot functions retain alignment, instruction lengths
  and all non-relocation instructions.
- Independent F64 decoder references pass for CPU and Metal across all 160,000
  logits per backend, with maximum absolute error 0.000758938 against a 0.002
  limit. **Seven 120-second memory soaks** pass (five Mac, two CUDA), including
  strict residency and alternating inputs, with final host allocator live zero.
- Final CUDA ABI/ownership/replay selection passes 16/16; backward 3/3,
  seeded training 2/2, lifetime 3/3 and assigned layout/descriptor 8/8 pass.
  The Mac native selection passes 219 tests with 13 explicit unavailable-CUDA
  skips; the actual layout selection passes 179 with six skips. Partial host
  allocation and CUDA device-budget failures are exercised.
- Native CPU/Metal/CUDA product checking, browser WASM, 67 generated-source
  freshness checks, actual runtime compilation of 54 generated Metal variants
  and the full 525-function Metal library pass. All 17 artifact/accounting/
  qualification script tests pass. The invalid pipeline-order negative control
  is rejected as expected. Offline AIR compilation is unavailable on this Mac;
  the shader checks use the actual Metal runtime compiler.

Known baseline limitations remain explicit. Spark's installed driver rejects
NVCC 13.2 portable PTX 9.2 with error 222 for both frozen and regenerated PTX;
portable execution is not reported as passing. The supplemental Tiny Llama
CUDA decoder probe is rejected during model-profile admission with
`UnsupportedCudaArchitecture` in both baseline and candidate, so no CUDA decoder
F64 oracle is claimed. The required four supported CUDA model cases pass.
Legacy GLiNER25 still reports the same six baseline failures (78 pass, two skip),
and GLiNER2 the same four baseline failures (three pass, three skip); exact
failure-name comparisons are preserved. They are not successful training runs
and do not expand this refactor into unsupported integer-gather/model routes.

Qualified model binary SHA-256 identities:

- Mac: `67b8ba19082979dc129e82a7d5eb3e723c1187d6629d5201d27c038949b49c5d`.
- Spark: `e30afb94a6a32208d2cdaa38c1f4335c380a75d91b50da3f49165f2c9e6a13d0`.

Cache collection completed on Spark. The Mac collector's cross-worktree scope
was rejected by automatic approval review; a scoped check of this task's
`.zig-cache` and `zig-out` found no eligible stale/over-budget output under the
same 30-minute activity, three-day age and 20 GiB size limits. Other worktrees
and user caches were left alone.


## Next campaign: native phases and adapter ownership (T01–T48)

This is an implementation plan, not an acceptance claim. Its production baseline
is `4883bc956c9446e5660391f7df676dbd78948538`, after the completed S campaign.
R/N/S savings are excluded. Execute it as one campaign with eight six-package
waves, cohort commits and one final composed qualification. A package finishes
with a qualified shared owner or a documented, measured reason to retain the
original specialization; retention earns no deletion credit.

The provisional planning envelope is **30–60 KB net authored production** after
charging every new helper, header, contract, signature, renderer and dependency.
This is an engineering estimate, not a deletion quota or a measured result.
T06 replaces it with prototype-backed cohort budgets before broad migration.
The intended outcome is fewer maintained native phases and adapter contracts;
compiled specialization counts and image sizes may remain unchanged.

### Fresh inventory and source ownership

A brace-delimited audit finds **38 distinct candidate exports**: 25 CUDA exports
with 73,887 exposed bytes and 13 Metal exports with 45,221 exposed bytes. The
119,108-byte total is candidate declaration/body exposure, including unique
work, not removable source. CUDA generated regions were excluded. Metal sizes
come from the qualified, fully preprocessed shader; C string escaping, existing
fragment owners and generated copies must be reconciled with maintained source
before credit. The complete symbol list, source hashes and discovery pairs are
in `.benchmark-assets/1166/next-structural-plan/{selected-exports,native-audit}.json`.

Specific new groups include four CUDA scalar argmax exports (6,320 bytes), three
Q8/Q4K/Q6K gated-down exports (4,949), three Q4K bias/activation variants (3,034),
four Metal BF16 matrix epilogues (13,675), and two Metal expert-routing kernels
(5,955). The corresponding pair similarities are discovery evidence, not proof
that arithmetic, scratch or ownership can be merged.

The host scan finds 191 `launch_abi.addresses` occurrences in CUDA launch code,
and 201 `allocDeviceBuffer` / 117 `createTensor` occurrences in CUDA compute.
These are whole-file syntactic occurrences, including helpers/tests and complex
owners, not migration counts. `host-audit.json` retains per-function locations.
T02 must classify production callers and exclude generated, prepared, donated,
mirrored, multi-output and graph-lifetime allocations before selecting them.

Do not mistake expanded duplicates for new maintained duplication. The S-owned
segmented/training attention phases and grouped matrix phases already share
source. TurboQuant fast prefill/decode already call `termite_tq_fast_online_impl`.
CUDA E2B 6144/12288 variants already share renderer functions. These discoveries
earn no new body-sharing credit. Older rejected full-body schedules remain
closed unless a package proposes a materially different, narrower phase owner.

| Budget owner | Provisional net authored reduction | Packages |
| --- | ---: | --- |
| Ordinary CUDA projection/argmax/RoPE phases | 5–9 KB | T07–T12 |
| Metal expert/indexed/BF16/small-M phases | 6–12 KB | T13–T18 |
| Packed and attention matching phases | 3–7 KB | T19–T24 |
| CUDA launch ownership | 6–11 KB | T25–T30 |
| Shared geometry and consumed descriptors | 2–5 KB | T31–T36 |
| Compatible result ownership | 3–7 KB | T37–T40 |
| Maintained renderer/compatibility ownership | 5–9 KB | T41–T44 |

These are non-overlapping planning buckets. T02 assigns every removed byte to
one owner. The renderer bucket has especially uncertain payoff because many
variant bodies already have one maintained emitter. Generated-output shrinkage
is always separate and cannot rescue a negative authored-source result.

### Wave A — baseline, contracts and prototypes (T01–T06)

| ID | Concrete work and completion condition |
| --- | --- |
| T01 | Freeze the baseline sources, all three CUDA images, decoded Metal shader, qualified Mac/Spark binaries, fixtures and route counters. Record hardware, compiler, flags, source/include closure and benchmark identities. Existing immutable fixture data may be reused by hash; its A/B baseline must be the T baseline. |
| T02 | Build a consumed-owner graph for the 38 exports, remaining launch packs and output sites. Mark already-shared/generated text, retained schedules, each exact scratch extent, dynamic scalar inputs and transfer/borrow lifetimes. Give each candidate a unique source-budget owner; classify actual production host eligibility. |
| T03 | Add independent projection/argmax/RoPE fixtures before editing bodies. Include codec boundaries, tail rows/columns, bias and activation modes, negative/duplicate/tied indices, all-masked and nonfinite inputs where supported. Define mathematical references independently of the candidate and assert selected routes. |
| T04 | Add expert/routing/BF16/packed fixtures: invalid or stale slots, unequal expert widths, top-k ties, empty/partial routes, repeated experts, arena offsets, BF16 narrowing, and one-/four-/eight-row packed outputs. Check poisoned guards, exact dynamic scratch, captured scalar mutation and retained inputs/results. |
| T05 | Prototype one CUDA ordinary projection owner (T08 or T10), one Metal BF16 matching phase (T16), and one launch/result-owner migration (T25/T37). Measure net maintained cost, host allocations, direct timing and whole-model controls. Reject abstractions that increase resource budgets or short-row overhead. |
| T06 | Admit prototypes independently, publish measured cohort budgets and freeze the expanded fixture/timing protocol. Prioritize accepted ordinary CUDA, Metal BF16 and host cohorts. Packed multirow and attention phases follow only after their narrower extraction is shown to preserve schedules. |

### Wave B — ordinary CUDA native bodies (T07–T12)

| ID | Concrete work and retained specialization |
| --- | --- |
| T07 | Share matching `linear_f32_qkv_nobias_tiled` / `linear_bf16_weight_f32_qkv_nobias_tiled` traversal with a static weight-load leaf. Keep conversion position, three projection widths, accumulator order, guards and output offsets identical. |
| T08 | Give `linear_q4_k_f32_tiled`, `_bias_` and `_bias_quick_gelu_` one matching dot traversal with static publication leaves. Bias addition and quick-GELU expression order stay native; no materialized intermediate or runtime epilogue dispatch. |
| T09 | Share Q4K pair/triple bias traversal through compile-time projection arity. Pair instances retain exactly two accumulator/weight/output planes. Do not allocate a third plane or route pair calls through triple publication. |
| T10 | Share compatible Q8/Q4K/Q6K gated-down traversal and the matching ordinary Q6K phase, with existing format codecs and static gate/no-gate publication. Keep each block width, fold, scalar load location and actual shared partial extent. Ordinary Q8/Q4K owners already shared elsewhere are reference owners, not new savings. |
| T11 | Consolidate `argmax_rows`, `argmax_last_row` and their suppression variants using static row selection and suppression leaves. Preserve tie ordering, NaN/infinity treatment, empty/suppressed outputs and exact index/value reduction trees. Test first/last ties and suppression changes in graph replay. |
| T12 | Extract only identical angle/pair transform leaves between Qwen3-VL MRoPE and vision RoPE. Positions/sections, indexing, dtype, row layout and parameter acquisition remain distinct. Retain bodies if the leaf adds source or registers; this does not merge all RoPE schedules. |

### Wave C — Metal indexed and matrix phases (T13–T18)

| ID | Concrete work and retained specialization |
| --- | --- |
| T13 | Share indexed Q4_0 pair-linear / pair-activation traversal with compile-time publication. Keep expert-ID mapping, bounds, weight strides, paired stores and activation order explicit. Reuse existing Q4 codec/fold owners. |
| T14 | Reuse matching Q4 acquisition/fold phases in `moe_slot_q4_0_gate_up` and `moe_slot_q4_0_down`, relating them to existing ordinary/indexed owners. Slot lookup, private-arena addressing, fail-closed zero publication and top-k row mapping stay at their native caller. Gate/up and down keep their different live accumulator extents. |
| T15 | Share matching selection/weight phases of `moe_route_select_tg_register` and `_v2`. Preserve their distinct reduction/publication strategy, tie behavior, normalization and register storage. No maximum expert array or generic sorting path. |
| T16 | Extract matching BF16 vector matrix load/decode/MMA phases from `apply_bf16_gate_up_mm_m64_n32_vec4` and `apply_linear_bf16_mm_m64_n32_vec4`. Keep exact tile arrays, barrier placement, narrowing and gate/up versus linear publication in native scope. |
| T17 | Apply the qualified phase ownership to the corresponding two non-vec4 exports, using their existing scalar schedule. Do not promote scalar loads to vec4 or combine scalar/vector scratch just to reuse T16. |
| T18 | Share the matching small-M `dot_general_2d_f32_small_m_rows4_cols8` / `_cols16` phases with static column extent. Preserve exact accumulator counts, tail masks and reduction order; demonstrate both small shapes and model-size controls. |

### Wave D — packed and attention phases (T19–T24)

| ID | Concrete work and retained specialization |
| --- | --- |
| T19 | Extract identical packed gate/up block-dot and quantization phases from E4B `pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4` / `_rows8_c2`. Keep four-row versus eight-row/column reuse schedules, actual arrays, barriers and quantization grouping local. Prefer native-scope fragments when inline helpers change CUDA shared-address instructions. |
| T20 | Reuse only proven matching phases in the corresponding single-row `_ffn` export. Its publication and five-warp ownership remain separate. Do not repeat the previously rejected wholesale single-/multirow schedule merge. |
| T21 | Share matching A4B `down_rows_q8_1_f32` / `_compact` block traversal with static row-map/output leaves. Keep compact row mapping, bounds, selected-expert offsets and output accumulation semantics exact. |
| T22 | Reuse compatible packed gate/up partials in A4B `gate_up_rows_q8_1_f32` from current qualified owners. Expert lookup and per-route input mapping remain caller work. Count only newly removed A4B source, not an existing ordinary owner's body. |
| T23 | Audit the two TurboQuant MMA prefill schedules for exact repeated staging/score/publication fragments. The proposed change is lexical/native-scope phase sharing, with expanded per-kernel identity as the first gate; it is not another whole-body template. Retain every different fragment, fragment count, barrier and shared offset. No change proceeds on resemblance alone. |
| T24 | Extend existing Metal source-phase ownership to matching portions of `attention_f32_dense_causal_sg_q16_f16kv_gqa2`. Keep its Q16/GQA2 storage and schedule separate from S11 scalar attention. Prefer exact expanded text; do not reintroduce S11's rejected whole-body template or count S-owned phases again. |

### Wave E — CUDA launch ownership (T25–T30)

Use `kernel_abi.cuh` and `launch_abi.ArgumentsFor`; native signatures remain the
single wire owner. New names or records must remove real local packing. Keep
unique tuples when added macros/factories cost more. T02/T06 set the eligible
count; the initial planning envelope is 60–100 regular launch-pack occurrences,
not all 191. Every macro, constructor and reflection helper is charged.

| ID | Concrete migration group and contract |
| --- | --- |
| T25 | Fill/copy/conversion launchers and repeated one-/two-buffer scalar arithmetic. Share only identical named field binding and count conversion, with existing byte-width admission and zero-work checks. |
| T26 | Dense and quantized embedding/take-row launchers. Derive index/storage pointer types from native signatures; preserve signed versus unsigned indices, BF16 widening and table-byte extents. |
| T27 | T07–T10 projection and fused-bias launchers. Reuse native dimension validation without a second operation catalogue. The output-count checks and failure order remain before launch. |
| T28 | T11 argmax and remaining compatible suppress/reducer launch packing. Mutable output/index slots must point into the same live named record between passes and during capture. No temporary copy of a live address pack. |
| T29 | Compatible copy/layout/vision packs, grouped by identical native ABI. Preserve branch-specific transpose packs, 29-field broadcast and scatter tuples unless measured sharing removes more code than it adds. |
| T30 | Compatible attention/expert packs associated with T14/T19–T24. Captured device scalar pointers, optional buffers, offsets and delayed mutation keep their lifetime. Add no generic runtime argument builder or extra launch dispatch. |

### Wave F — geometry and consumed descriptors (T31–T36)

| ID | Concrete work and completion condition |
| --- | --- |
| T31 | Introduce one checked ordinary concat-axis geometry consumed by CPU, Metal and CUDA: output extents, prefix, per-input axis span and suffix. Keep CPU source-backed/imported fallback resolution and Metal device/donation decisions local. Existing S slice/gather/transpose geometry is reused unchanged. |
| T32 | Share compatible resolved right-aligned binary/where output-shape calculation across actual adapters. Keep CPU's loose/imported compatibility, logical rank-zero behavior and backend dtype admission outside the common record. This only resolves metadata; it must not add broadcast materialization or readbacks. |
| T33 | Consolidate checked dense element/byte extent conversion at real allocation and launch consumers. Distinguish F32, BF16, integers and packed quantized block storage; never infer packed bytes from dense element width. Preserve overflow/admission error contracts. |
| T34 | Add a concat descriptor only with actual lowering and Metal/CUDA assigned invocation consuming T31. CPU primitive concat consumes the same geometry while retaining its optimized views/copies. Query/decline paths remain allocation-, readback- and submission-free. |
| T35 | Add ordinary positive-slice descriptor consumption using existing `SliceAxis`, in lowering and the assigned accelerator adapters. Preserve view versus copy, symbolic limits and Metal clipping; keep unsupported/negative-stride paths explicit. Store only logical metadata needed by real consumers. |
| T36 | Add ordinary axis-gather descriptor consumption using existing `GatherAxis` / `gatherShape`. Preserve index wire/dtype, scalar-index shape and backend bounds behavior. CPU gather-ND remains separate; do not enable currently unsupported CUDA integer generic gather as a side effect. |

### Wave G — compatible owners and renderer phases (T37–T42)

| ID | Concrete work and completion condition |
| --- | --- |
| T37 | Migrate compatible ordinary single-F32 CUDA copy/layout/embedding outputs to existing `OwnedF32Output`. Preserve shape-before-device allocation, buffer release policy and successful `createTensor` transfer; exclude source-backed, borrowed, prepared and multi-output lifetimes. |
| T38 | Migrate compatible single-result CUDA vision/attention adapters after T37 fault/resource qualification. Keep auxiliary workspaces separately owned and preserve optional decline before side effects. Do not generalize mirrors, saved gradients, pinned capture frames or donation into a universal owner. |
| T39 | Extend existing CPU `OwnedDenseOutput` to compatible ordinary activation/selection/conversion allocating branches. Dense-input view acquisition/cleanup stays ordered, and alias/view returns keep their owner. Preserve short-row allocation classes and CPU hot-loop instructions. |
| T40 | Delete superseded cleanup only after every selected T37–T39 caller uses its owner. Exercise every partial host allocation, device-budget denial, failed launch/transfer and optional fallback. Require unchanged allocation counts and no new payload copy, pool, sync or stream transition. |
| T41 | Audit repeated maintained CUDA renderer fragments across pair/down Q8 bodies and exact-F32 bodies. Extract matching emission phases while keeping codec-specific loops and schedules separate. E2B dimension variants already use one emitter and earn no new sharing credit. Prefer byte-identical per-kernel emission; charge every new generator helper. |
| T42 | Share identical Metal flash-prefill emission fragments between ordinary and HD512 renderers. Keep their distinct scratch partition, matrix folds and threadgroup ownership. Generated per-export text identity is the initial gate; do not merge HD256/HD512 schedules. |

### Wave H — compatibility retirement and final qualification (T43–T48)

| ID | Concrete work and completion condition |
| --- | --- |
| T43 | Reconcile handwritten Florence Q4K NR4 with the existing generated small-batch owner. Share the exact common traversal only if original export, guards, parameter order, four-row schedule and runtime selection are preserved. Edit the renderer/maintained owner, regenerate copies, and charge all helper emission. Similar generated text alone is not a saving. |
| T44 | Audit generated-to-runtime ABI/binding compatibility declarations against existing source contracts. Remove a duplicate declaration only when both real emitter and runtime launcher consume the existing contract. Keep one-off adapters if a new schema would add a parallel catalogue. Check every generated symbol and transitive include; close renderer buckets with measured retention if they add source. |
| T45 | Freeze final sources/binaries; close native include/JIT/watch dependencies and compiler field/ABI assertions. Regenerate/check all three pinned CUDA images and generated regions; compile all generated Metal variants and the full expanded shader with the actual runtime compiler. |
| T46 | Run the entire current 1,332-case Metal / 443-case CUDA direct corpus plus every new moved-phase fixture. Assert forced specialization routes, guards, scratch/resources, independent references, graph scalar mutation and failure cleanup. Run final static comparison for all 439 CUDA exports on seven targets. |
| T47 | Qualify the full composed CPU/Metal/CUDA candidate: all 19 supported model cases, CPU/Metal independent decoder logits, CPU primitive/host-encoding controls, affected backward/training/lifetime suites and seven 120-second soaks. Compare existing failing GLiNER targets to frozen baseline failures; record unavailable portable PTX/model routes explicitly. |
| T48 | Publish per-owner net authored/source/image ledgers against T01, final source/binary identities, package outcomes and all failed prototypes/controls. Complete coherent commits with a clean worktree and scoped cache cleanup. No previous campaign savings or generated-copy bytes enter the new authored-production total. No push/deployment. |

### Execution order and performance admission

Start T01–T06, then execute B and C as separate source cohorts. D follows its
specific T04/T06 prototype admission. E consumes the native contracts of B–D;
F may proceed independently after its geometry fixtures exist. G depends on
eligible T02 sites and F geometry, and H closes all accepted cohorts. T43/T44
can follow T41/T42 before the final freeze. Each cohort must delete superseded
maintained code in the same change that introduces its replacement.

Prefer fewer full qualification cycles: run independent direct/resource/error
checks after each native cohort, affected launch/adapter controls after E–G,
then one full model/soak composition at T45–T48. Early representative CPU,
Metal and CUDA model controls still run at T05 and after a cohort changes its
actual dispatch/ownership path. Do not overlap builds or GPU timings on the
same host. Independent Mac and Spark work may run concurrently.

Preserve the established gates: per-case direct source effect <= 1.05, model
P50 <= 1.05 and P95 <= 1.10, exact baseline results where current paths are exact,
and independent tolerances fixed before editing. Metal uses both pipeline
creation orders and preserves all raw flags; the paired metric does not replace
correctness/resource gates in either order. Establish A/A controls for new
fixtures before candidate timing. Diagnose unstable controls; never repeat a
failed benchmark until it happens to pass or widen a gate after seeing results.

All seven CUDA targets retain exact native scratch extents and must not
increase registers, stack, local/shared bytes or instruction counts without
separate performance evidence and an explicit documented design decision.
Matching source-fragment extraction should retain normalized instructions.
The protected CPU hot functions retain their current alignment and schedule.
No added tensor allocation, intermediate materialization, device readback,
synchronization or stream transition is acceptable merely to simplify a caller.
Capture/reset, retained results beyond frame capacity, strict residency,
allocator teardown and failed-allocation cases remain part of acceptance.

The baseline limitations are carried forward honestly: Spark cannot execute
pinned PTX 9.2 with its current driver; Tiny Llama has no supported CUDA model
profile; legacy GLiNER targets have known failures. Reproduce these at T01 and
compare them at T47. Any newly introduced failure blocks its cohort. Existing
limitations are not passing tests and are not silently expanded into unrelated
feature work.


## T campaign completion

The T01–T48 campaign is implemented and qualified against
`4883bc956c9446e5660391f7df676dbd78948538`. It removes **847 maintained
production lines / 16,746 bytes net**, after charging all replacement headers,
ABI contracts, descriptors, runtime bridges and dependency identities. This
falls within the prototype-backed 16–20 KB envelope, which replaced the original
30–60 KB planning estimate. Previous campaigns earn no credit here.

The source ledger separates generated output (**2,345 additional bytes** from
moving Florence NR4 into its renderer), inline tests and benchmark tooling.
All three freshly regenerated CUDA images are **byte-identical to baseline**.
The expanded Metal shader is 926,413 UTF-8 bytes versus 927,684 at baseline;
expanded text and maintained-source savings are different measurements.

| Packages | Completed outcome |
| --- | --- |
| T01–T06 | Frozen sources, binaries, fixtures and images; 38-export ownership audit; independent fixtures and qualified prototypes; measured budgets. |
| T07–T12 | Shared ordinary CUDA QKV, Q4K projection, gated-down, argmax and RoPE phases with static leaves and exact native extents. |
| T13–T18 | Shared Metal indexed/slot acquisition, routing phases, BF16 scalar/vector phases and small-M traversal, retaining distinct folds and storage. |
| T19–T24 | Shared E4B multirow/single-row publication, A4B Q8 acquisition, TurboQuant MMA visibility/staging and an exact Metal Q16 attention fragment. |
| T25–T30 | 69 native-derived CUDA launch packs; measured retention of small or mutable packs; unsigned 64-bit stride and live-slot validation. |
| T31–T36 | Shared checked concat/broadcast/dense-byte geometry; consumed concat, positive-slice and axis-gather descriptors in lowering and assigned Metal/CUDA execution. |
| T37–T40 | Existing result owners consumed at 26 CUDA and eight CPU ordinary sites; partial allocation, budget denial, optional decline and retained-input checks. |
| T41–T44 | Shared exact renderer input/signature phases; Florence NR4 consumes the existing K-family renderer; measured retention of unprofitable or already-owned phases. |
| T45–T48 | Final source/include/binary closure, generated/native compilation, direct/model/oracle/soak qualification, source/image ledgers, cohort commits and scoped cache audit. |

The per-owner maintained ledger charges additions as negative removals:

| Maintained owner | Net bytes removed |
| --- | ---: |
| CUDA device phases and native ABI headers | 10,256 |
| Metal shader owners | 8,470 |
| CUDA launch adapters | 10,097 |
| CPU and CUDA result ownership | 3,279 |
| Shared geometry, descriptors, runtime bridges and build identity | -15,015 |
| Renderer owners | -338 |
| Metal host bindings | -3 |
| **Total authored production** | **16,746** |

The native owners preserve accumulator extents, load/conversion order, barriers
and publication. Paired Q4K projections retain two planes. BF16 scalar/vector
staging stays separate, as do four-row and eight-row FFN acquisition schedules.
An initial FFN extraction increased SM75 instructions from 8,424 to 8,432 despite
unchanged resource budgets. Restoring original literal bounds alone did not
remove the drift. Exact native-scope publication restored identical code on all
seven targets. Both rejected prototype receipts remain available.

Native signatures remain the single CUDA wire owner: 22 launch packs reuse
existing projection contracts and 47 additional profitable packs consume native
contracts. Mutable address packs remain at their original call sites. Simple
fill/copy/conversion and small argmax tuples remain where replacement costs more.
No runtime argument builder or second operation/signature catalogue was added.

Concat geometry has CPU, Metal and CUDA consumers. The three new layout
descriptors invoke actual assigned accelerator outputs; legality queries perform
no allocation, readback or submission. Metal retains its contiguous-slice view
path. Overlapping outputs are rejected before encoding. Scalar-index shapes,
negative/duplicate gather indices, unsupported paths and retained inputs have
explicit coverage. Dense byte extents use the existing scalar dtype vocabulary;
packed storage remains codec-owned.

Result-owner migrations preserve allocation order, auxiliary lifetimes and
optional decline. Prepared, donated, mirrored and multi-output owners remain
separate. Renderer changes preserve Florence's original export, signature,
guards and complete native body. Remaining Q8 coordinate extraction would add
51 maintained bytes after its owner and references; HD256/HD512 folds and scratch
also remain distinct. Already-shared E2B variants earn no new deletion credit.

Final direct qualification covers **1,554 Metal and 558 CUDA cases**, including
all 38 selected exports and Florence. The 222 new Metal cases all have independent
references; 1,160 of the full Metal corpus have them. The other 394 legacy cases
use exact baseline parity and are explicitly marked without independent output.
All 115 new CUDA cases and the existing 443-case corpus pass their numerical,
replay, guard, resource and timing gates. All **439 CUDA exports retain identical
instructions and resources on seven targets**. The three protected CPU hot
functions retain their 64-byte alignment and non-relocation instruction words.

An initial full Metal run rejected one unchanged Q4_1 case at ratio 1.137251.
Its trace shows a shared transition between roughly 15 us and 11.6 us timing
modes; independent medians selected different modes. Baseline A/A and source
isolation controls pass. The explicitly versioned corrected estimator takes
the median of 16 adjacent opposite-arm B/A ratios per creation order, then their
geometric mean. The **1.05 gate is unchanged**. The full corpus passes the paired gate using
both creation orders under this protocol. All raw arms, original median flags and
failed receipts remain. Deterministic tests reject a real 1.06 regression,
unpaired/nonfinite/nonpositive arms, mismatched identities, missing orders and
failed references/guards/resources. Null reference metadata is allowed only for
legacy cases declaring zero independent outputs. Model and CUDA protocols stay
unchanged.

All **19 supported model cases**, **11 CPU primitive/host-encoding controls** and
**seven 120-second soaks** pass. CPU/Metal decoder logits pass the independent
reference at tolerance 0.002. Actual CUDA ABI/ownership/replay, backward, seeded
training, lifetime and assigned-layout suites pass. The additional 61-case
vision/attention/expert interface suite is compared against a fresh execution
of the frozen T baseline: 57 clean passes, two skips, one unchanged Laya routing
assertion and one unchanged vision-graph teardown error-log failure. Every test
status, failure reason and error message/count matches; candidate sources are
restored and hash-verified after the baseline overlay. Product checks, browser
WASM, all 67 renderer entries, 54 generated Metal variants, the complete 525-export runtime shader, pinned image freshness
and qualification-script tests pass. The broad Mac suite records 228 passes and
14 explicit capability/fixture skips; individual logs preserve every skip.

Baseline limitations remain explicit: Spark's driver rejects pinned PTX 9.2;
Tiny Llama reports `UnsupportedCudaArchitecture` for both baseline and candidate;
legacy GLiNER targets retain exactly the same six and four failures. These are
reproduced limitations, not passing CUDA decoder or portable-PTX claims.
The additional Laya test expects an SM89-only route on SM121, and the vision
graph test logs invalid frees during teardown on both versions. These newly
inventoried baseline findings remain explicit; they are not reported as passes.

The package register, consumed-owner graph, per-file ledgers, source/binary
identities, raw timing arms, failed prototypes and final verification are retained
under `.benchmark-assets/1166/targeted/`, including `packages.json`,
`consumed-owners-final.json`, `ledger-final.json`, `source-closure-final.json`,
`final-evidence.json` and `additional-host-interface-final.json`. Cache auditing
is limited to the local and Spark validation worktrees' caches under the normal 30-minute activity, three-day age and 20 GiB limits. No push or
deployment is part of this campaign.


## U campaign: baseline repair and larger shared owners (U01–U56)

The campaign starts from merge `b7568c5ffc8daaff4f4c6f0409a746052f71d172`,
which incorporates `origin/main` at `730999bf2daafc764a4f37168b1440494dfa42fa`.
`f33c3e406805f96172a2a394d509bdab54b68f61` is the preceding qualified T result,
not the U accounting baseline. The merged sources, binaries and reproduced
failures are frozen under `.benchmark-assets/1166/u-campaign/baseline/`.
Complete the correctness and capability repairs in independently reviewable
commits, then freeze that corrected baseline for the larger consolidation
cohorts. Report repair costs separately from consolidation savings. No T or
earlier savings enter the U ledger. The work packages below remain acceptance
conditions; a prototype or a passing subset does not complete the campaign.

The larger opportunities are CPU prepared-quantization kernels, Metal attention
runtime orchestration, CUDA attention/launch setup, and metadata repeated across
backends. Migrate complete eligible families and remove their superseded paths
in the same cohort. Adding another owner while retaining every ordinary caller
does not complete a migration.

### Baseline failures to repair first

| Finding | Priority and intended treatment |
| --- | --- |
| Vision graph teardown logs repeated invalid device frees | **P0 correctness.** Audit every resident-weight insertion and actual physical owner. `CudaCompute.deinit` currently forces buffer/shape/mirror ownership flags to true. Distinguish cache-owned allocations from borrowed aliases and fixture buffers; retire captured uses before the real owner releases storage. Simply ignoring driver errors or removing all frees could hide double frees or introduce leaks. |
| GLiNER2's four failures reach gather's F32-only index admission | **P1 supported training correctness.** Trace the concrete index dtype and graph/VJP contract, then implement exact typed index consumption. Preserve F32 activations and BF16-table widening. Do not cast integer indices through F32 or add a host readback to make training pass. |
| Laya's warp-route assertion expects an SM89 route on SM121 | **P1 test/capability contract.** Use the actual runtime eligibility predicate in the test. Verify the available fallback numerically on Spark; assert the warp route on eligible hardware. Explicit CUDA selection must still fail when CUDA itself is unavailable. This repair does not enable an unqualified SM121 fast path. |
| GLiNER25 memory-budget test expects a different error | **P1 error contract.** Determine which layer owns admission and which error callers are promised. Fix the implementation or stale expectation accordingly; retain the physical-budget, eviction and cleanup assertions. |
| Other GLiNER25 route counters, unavailable fused attention and resume assertion | **P1 triage and supported-route repairs.** Separate output/gradient/resume correctness from architecture-specific route expectations. Repair broken supported paths. For genuinely unavailable strict variants, assert explicit admission failure and validate the supported fallback separately. Do not broadly skip the suite or enable a route from symbol presence alone. |
| Driver rejects the pinned portable PTX; Tiny Llama has no admitted CUDA profile | **Separate environment/feature work.** Preserve explicit negative coverage. A driver/toolchain change or new decoder profile needs its own compatibility and performance qualification; neither is a source-deduplication fix. |

All these findings have baseline evidence under
`.benchmark-assets/1166/targeted/`. The extra interface suite has 57 clean passes,
two skips, one assertion failure and one error-logging failure on both versions.
Its runner's raw summary counts the logging test's successful return as well as
its failure; keep the per-test classification when assessing repairs. Require
zero unexpected error logs and no new skips after the ownership repair.

### Fresh discovery and architecture

An exact 16-line window scan excludes inline Zig tests and generated regions.
It identifies these maintained-source candidates:

| Candidate owners | Overlapping matching windows |
| --- | ---: |
| CPU/Metal `inferReshapeShapeWithNumel` | 137 |
| CUDA `gqaDenseAttention` / `gqaPagedAttentionWithCompressedDeviceKv` | 55 |
| Metal quantized decoder block allocating / `Into` wrappers | 51 |
| CUDA/Metal MTP verify/commit adapters | 47 |
| CPU K16 prepared Q8K ordinary / rows-three no-min activation | 38 |
| Metal quantized dense / paged decoder `Into` wrappers | 32 |
| CPU Q4K MR1 / MR2 accumulation | 24 |
| CPU packed QKV panel publication with / without prepared sums | 23 |
| Metal layer-norm / RMS-norm linear sample wrappers | 22 |
| CUDA split-K / TurboQuant score-prework launch setup | 20 |
| CPU legacy prepared pair / triple activation | 19 |
| CPU/Metal owned-F32 conversion | 18 |

These counts overlap and are discovery evidence, not independent occurrences,
removable lines or savings estimates. Native inline assembly, barriers, escaped
shader text and codec-specific arithmetic require manual boundaries. The full
scan is retained in
`.benchmark-assets/1166/next-campaign/maintained-duplication-audit.json`.

Extend existing `kernel_geometry`, `kernel_descriptor`, native ABI owners,
`quant_matmul.Format` and renderer plans. Share checked logical metadata and
proven matching native phases. Backend schedules, codec loads, reduction trees,
capture addresses, prepared caches and physical storage owners stay explicit.
No universal runtime argument builder, second operation catalogue or general
kernel language is part of this campaign. New metadata must have actual
production consumers and replace repeated work.

U08 must publish prototype-backed budgets before broad migration. Charge every
new contract, header, helper, bridge and include identity. Do not repeat the
previous unproven 30–60 KB estimate. The measured-negative T fill/copy/argmax
packs, Q8 coordinate extraction and already-shared E2B renderers remain closed
unless a materially different ownership change makes their total cost positive.

### Wave A — baseline repair and admission (U01–U08)

| ID | Work and completion condition |
| --- | --- |
| U01 | Freeze starting sources, binaries, images, fixtures, compiler identities and every baseline failure/status/error-log count. Maintain separate repair and consolidation ledgers. |
| U02 | Reduce vision teardown to owned, borrowed, aliased and captured-weight reproductions. Inventory every resident-cache insertion, shape/mirror owner and lease; include partial construction, eviction and context teardown. |
| U03 | Repair cache storage ownership using an explicit owner/borrow relationship. Preserve legitimate cache-owned frees, graph retirement order and admission release. Pass allocation-failure injection and repeated teardown with no invalid frees, leaks or retained-input corruption. |
| U04 | Repair Laya's capability-dependent route assertions using the production predicate. Exercise fallback outputs on Spark and keep eligible-route coverage explicit; do not count an unavailable route as tested. |
| U05 | Resolve GLiNER25's admission error contract. Check eviction-before-allocation, retained physical bytes and all partial-failure cleanup, rather than changing only an expected error name. |
| U06 | Classify and repair the remaining GLiNER25 supported-route/counter/resume failures. Keep independent numerical and gradient assertions, forced-route cases and honest architecture admission. |
| U07 | Pin the GLiNER2 gather failure to exact index dtypes, forward shapes and VJP requirements. Add independent large-index, negative/duplicate and invalid-index cases before implementing U12–U15. |
| U08 | Prototype shared reshape inference, one CPU prepared-kernel phase and one Metal allocating/Into attention owner. Admit only positive net source cost with unchanged hot schedules/resources and representative timing. Publish per-cohort budgets and freeze the corrected baseline after U15; apply the same gate to later GPU prototypes before broad edits. |

### Wave B — shared metadata and exact typed indexing (U09–U16)

| ID | Work and completion condition |
| --- | --- |
| U09 | Replace CPU/Metal duplicate reshape inference with one checked resolver. Preserve inferred dimensions, zero-copy views, empty/symbolic behavior and imported-shape policy at callers. |
| U10 | Consolidate the remaining loose three-input broadcast resolver with existing geometry policies. Retain the CPU stride pass and each backend's zero/symbolic admission; add no materialization. |
| U11 | Consolidate repeated Metal runtime/compute format mapping into the existing format vocabulary. Preserve unsupported-format distinctions and native packed byte extents; introduce no parallel format enum. |
| U12 | Define typed index admission using existing dtype and index-reader contracts. Consume it in real gather/scatter adapters; preserve float-index legacy semantics separately from exact I32/I64 semantics, normalization and backend bounds behavior. |
| U13 | Add CUDA I32/I64-index gather through static index-load leaves shared with compatible existing F32/BF16-table traversal. Test values beyond F32's exact integer range without converting indices to float. Keep existing F32-index behavior and output encoding unchanged. |
| U14 | Add matching typed-index scatter/VJP consumption using shared geometry. Preserve duplicate accumulation, atomic/reduction semantics and dtype admission; no CPU staging or index readback. |
| U15 | Route GLiNER2 forward/backward through those consumers and close the four supported training failures. Compare token/span/total-loss gradients and resident optimizer/resume trajectories to independent/native references. |
| U16 | Consolidate compatible CPU/Metal owned-F32 conversion metadata and cleanup. Keep source-backed reads, private mirrors, conversion arithmetic and host/device policies local; retire the repeated ordinary helpers. |

U12–U15 are correctness and capability work with a separately reported source
cost. They are not deletion credit. The broad ownership migrations in later
waves wait for U03 and supported training correctness to pass.

### Wave C — CPU prepared quantization bodies (U17–U24)

| ID | Work and completion condition |
| --- | --- |
| U17 | Share matching K16 prepared-Q8K activation phases between ordinary and rows-three no-min paths. Keep min/no-min arithmetic, row schedules and prepared activation lifetime explicit. |
| U18 | Consolidate matching Q4K MR1/MR2 accumulation through static row extent or narrow leaves. Preserve exact vector accumulator counts, narrowing and instruction order; no padded second row in MR1. |
| U19 | Share matching Q4/Q5K panel16 MR2 phases with/without prepared sums. Keep sum acquisition and format-specific corrections at their original points. |
| U20 | Apply qualified phase ownership to MR4 panel16 variants while retaining their own register/stack extents and row-tail handling. |
| U21 | Consolidate packed-QKV panel16 MR4 publication with/without prepared sums. Keep projection offsets, prepared metadata and exact live output planes; use existing dot owners. |
| U22 | Share only proven matching Q4/Q5K versus Q6K block8/block4 MR2 phases. Retain codec unpacking and correction order. Audit AArch64 SDOT siblings; retain distinct assembly schedules unless the expanded instructions match. |
| U23 | Give Q8_0 pair/triple workers a static-arity owner where profitable. Preserve two versus three accumulator planes, worker partitioning, cancellation and callback lifetimes. |
| U24 | Consolidate compatible legacy prepared pair/triple activation and quantized dispatch/parallel setup. Remove superseded paths; charge shared worker work once across U23/U24. |

### Wave D — Metal runtime orchestration (U25–U32)

| ID | Work and completion condition |
| --- | --- |
| U25 | Unify matching quantized decoder-block allocating/Into orchestration. Caller-owned outputs remain borrowed; fresh outputs retain cleanup responsibility until transfer. Preserve the sequence of encoder calls. |
| U26 | Share matching dense/paged quantized decoder Into phases. Keep KV providers, page bindings, offsets and storage admission in separate native leaves. |
| U27 | Consolidate matching compressed dense/gated direct decoder orchestration with static residual/gating choices. Preserve scratch acquisition, synchronization and fallback order. |
| U28 | Share layer-norm/RMS-norm linear sample setup and publication where identical. Norm math, saved statistics and kernel choice remain separate. |
| U29 | Share compatible F16/BF16 native embedding lookup setup through static storage/output policy. Preserve index wire, table extents and widening positions. |
| U30 | Consolidate matching paged-slot/quantized attention runtime readiness and binding setup. Preserve active-frame/flush policy and request-owned buffer lifetimes. |
| U31 | Share finite-check/debug view helpers with scoped borrowed reads. Diagnostics-disabled timing remains unchanged; diagnostics must not retain private host mirrors. |
| U32 | Retire superseded ordinary runtime wrappers and qualify the complete D cohort through actual runtime compilation, capture/reset, retained results, cancellation and fault injection. Exclude model-owned/prepared lifetimes that have not been proved equivalent. |

### Wave E — CUDA setup and shared request semantics (U33–U40)

| ID | Work and completion condition |
| --- | --- |
| U33 | Share matching dense/paged-compressed GQA metadata and ordinary output setup. Keep cache mutation, compressed KV representation and host fallback acquisition local. |
| U34 | Consolidate matching split-K-online/TurboQuant score-prework launch geometry and scratch binding. Preserve exact offsets, dynamic scalar addresses and physical scratch extents. |
| U35 | Share compatible Q4K triple-bias ordinary/tiled launch validation. Reuse native records and preserve distinct dispatch geometry; do not recount T's named packs. |
| U36 | Share Q8_0 rows/columns and tensor-core common metadata where profitable. Preserve hardware capability, vector width, tile choice and zero-work/error order. |
| U37 | Consolidate DeBERTa M16N32/M32N16 launch metadata using existing wire owners. Keep schedule-specific shared-memory sizes and block/grid calculation static. |
| U38 | Share CUDA/Metal MTP verify/commit logical geometry and checked request validation. Keep rollback, live scalar updates, storage ownership and backend encoding order local. |
| U39 | Share CPU where-consume-true/false traversal with static donation selection. Preserve aliases, retained-input values and ownership on every error path. |
| U40 | Remove replaced attention/setup helpers and measure the remaining profitable launch cohorts. Preserve previously measured-negative small tuples and mutable live-address records. |

### Wave F — existing descriptors with more real consumers (U41–U48)

| ID | Work and completion condition |
| --- | --- |
| U41 | Share checked ordinary layer/RMS normalization row geometry across CPU/Metal/CUDA. Keep representation and prepared-weight policy outside the record. |
| U42 | Make ordinary Metal normalization consume the existing norm descriptor and assigned output. Preserve saved-statistic auxiliary lifetimes; unsupported prepared/training cases must decline before submission. |
| U43 | Make ordinary Metal dense F32 linear consume the existing linear descriptor through an actual Into kernel. Prepared/quantized weight slots retain separate admission; queries perform no allocation or preparation. |
| U44 | Share compatible softmax/log-softmax row admission across real adapters using the existing descriptor. Preserve backend-specific reductions, masks and all-masked/nonfinite behavior. |
| U45 | Add ordinary RoPE descriptor consumption only with actual lowering and assigned CPU/Metal/CUDA execution. Store logical parameters; keep MRoPE/vision indexing, table representations and schedule-specific conversion separate. |
| U46 | Consolidate repeated activation identity mapping and matching native scalar leaves using existing unary/activation vocabularies. Keep exact/approximate GELU, FMA policy, libm/intrinsics and SIMD schedules distinct. No new expression language. |
| U47 | Route compatible ordinary allocating adapters through the qualified shared invocation/Into bodies and delete replaced paths. Add no temporary tensor, extra submission, readback or runtime registry. |
| U48 | Close descriptor legality, alias, query/decline and residency coverage for every new consumer, including unsupported shapes/dtypes and partial failures. A descriptor without an executed production consumer earns no completion or savings credit. |

### Wave G — composed qualification and retirement (U49–U56)

| ID | Work and completion condition |
| --- | --- |
| U49 | Reconcile ownership graphs and per-cohort source budgets. Verify that all admitted old paths are removed, new owners are consumed and no generated or prior-campaign bytes are credited. Publish measured retentions. |
| U50 | Freeze final source/include/JIT closure, binaries, fixtures and tool identities. Regenerate/check all three pinned CUDA images and generated regions; compile the full Metal runtime and all generated variants. |
| U51 | Compare every existing CUDA export's normalized instructions/resources on all seven targets; record new typed-index exports separately. Preserve old schedules unless a change has separate performance evidence and an explicit design decision. |
| U52 | Qualify CPU prepared kernels with independent format/boundary references, exact tails, cancellation and all supported SIMD routes. Inspect affected native hot functions for spills, stack/alignment and instruction changes. Cross-compilation alone is not runtime/performance qualification on an unavailable ISA. |
| U53 | Run all 1,554 Metal / 558 CUDA existing direct cases plus every new changed-phase/index fixture. Retain the declared paired Metal protocol, raw arms, both creation orders, mathematical/guard/resource gates and negative-reference controls. |
| U54 | Run all 19 supported model cases, 11 primitive/host controls and independent CPU/Metal decoder references on the full candidate. Assert forced routes and unchanged allocations/transfers. Correctness repairs use independent references where the starting baseline is wrong. |
| U55 | Run all repaired baseline targets and affected training/replay/lifetime/failure suites, plus seven 120-second soaks. Require zero unexpected driver error logs, zero ownership leaks and no new skips. Preserve explicit unsupported PTX/decoder/strict-route negative cases. |
| U56 | Publish separate repair/consolidation net ledgers, final source/binary identities, all 56 outcomes and failed prototypes. Complete coherent local commits, clean the worktree and perform scoped repository-policy cache cleanup. No push or deployment. |

Execute A and the required typed-index repairs first. Use a corrected baseline
before C–F broad migrations, admit each cohort on its own evidence, then run G
on the composed candidate. Timing/model/resource gates retain the existing
limits. Performance experiments may propose a faster schedule, but that is a
separate measured change from source ownership and cannot silently relax a
deduplication gate. Driver upgrades and a new CUDA decoder profile remain
separate optional follow-up work.


### U campaign qualified checkpoints

The merge and baseline repairs are committed separately as `b7568c5ffc`,
`94b6cc2f02` and `7d2e04b01e`. Resident-cache allocation-failure and teardown
coverage passes all 17 selected cases with no skips or leaks. The complete
GLiNER2 target passes seven CUDA cases with its three existing disabled-Metal
skips; GLiNER25 passes 85 cases with one existing skip. Exact I32/I64 gather
and scatter consume integer wires directly. Independent cuBLAS references bind
expected results to the qualified GB10 device and library version. Deterministic
SM120 attention passes independent F64 forward/gradient checks and exact replay;
eight paired arms cover ten shapes, with forward ratios 0.027–0.667 and backward
ratios 0.032–0.745. The four legacy SM8 image identities remain unchanged.

U09–U11 share reshape/broadcast policy and Metal's existing format vocabulary.
The reshape owner passes 100,000 seeded comparisons against frozen original
bodies and the 128 actual reshape/broadcast cases. U16 shares owned-F32
conversion, preserves logical prefixes and unaligned reads, and releases data
if shape allocation fails. Cold conversion and actual Metal output-ownership
checks pass with no skips. The preliminary metadata/Metal cohort passes all 19
supported model cases against merged main; the final composed candidate still
requires its own complete qualification.

U17 shares the ordinary and statically unrolled three-block K16 activation
owner. Its 130 independent F64 cases cover Q2_K/Q3_K, row and column tails,
prepared-sum policies, biases, guards and empty rows. All three standalone
specializations retain their original non-relocation instruction streams and
accumulator/stack extents. The three protected CPU hot functions retain their
64-byte alignment and instructions. The final 32-arm direct comparison covers
120 timing cases and has a worst median adjacent ratio of 1.004526. Its preceding
32-arm same-binary control has a worst ratio of 1.007320. Both use equal-length
binary paths, 31 warmups and nine samples of 201 calls. Earlier shorter-window
failures are retained alongside the corrected row-loop-predicate prototype;
they are not omitted or treated as passing runs. The 1.05 gate is unchanged.
Raw sources, binaries, controls and receipts are retained under
`.benchmark-assets/1166/u-campaign/cpu-k16-owner/`.


The next host-launch checkpoint consolidates Q4_K ordinary/tiled triple-bias
admission, statically selected DeBERTa query tiles, and dense/paged GQA route
accounting. It retains each native kernel, wire order, launch geometry and the
paged route's impossible-fallback guard. The existing MTP request now owns
checked verification length and borrowed-choice classification; CUDA and Metal
retain allocation, device-result selection and transfer only after successful
classification. Spark passes all 22 selected launch/geometry/MTP cases with no
skips, including actual ordinary/tiled Q4_K native output checks and captured
live-address ABI replay. The maintained production reduction for this isolated
checkpoint is 10,484 bytes before formatting; final accounting uses the formatted
sources and charges the shared request methods. Device images are unchanged.


A separate Metal fixture audit reproduces 24,576 retained private host-mirror
bytes after the four-row Q4_0 block reference fixture. Mutable copies of owned
tensor handles acquired mirrors that their original owners could not retire.
The repair replaces 104 such fixture reads, including the shared gated-FFN
helper, with scoped read views. The repeated block fixture now asserts that
live mirror bytes return to their starting value. Against the committed runtime
without the pending consolidations, all 232 runtime tests complete: 230 pass
and the two existing unsupported cases skip. The pending composed runtime also
passes its 234 cases (232 pass, the same two skip). These fixture repairs are
accounted separately from maintained production-source reductions.


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


U36 boundary discovery also repaired two pre-existing Q8 launch defects. The
four rows/columns adapters had reversed tile extents, leaving output rows
unwritten for asymmetric tiles. The pinned tile4-r2 bias export additionally
uses the residual wire ABI even though it ignores the residual; its host
launcher now supplies that slot. No native image changes are needed. The
independent F64 fixture covers 48 actual GPU invocations: three tail shapes,
four tile variants including tensor cores, and four bias/activation/residual
modes, with leading and trailing guards. All pass on Spark, together with both
launch-ABI suites (three selected, zero skips). Starting-baseline failures and
intermediate diagnostic logs are retained; this is correctness repair cost,
not consolidation credit.


The second main integration is merge `4057c27eb8`, incorporating
`origin/main` at `1727ce4198`. It preserves the original U accounting baseline;
upstream media/API additions are not consolidation savings.

The CUDA launch cohort shares Q8 raw/tensor-core setup with a static storage
policy and the existing selected-function record, including named tile extents
and the pinned bias ABI distinction. Split-K-online and score-prework consume
the existing paged-attention wire record; workspace offsets, route admission,
score/chunk policy and native parameter order remain local. The compressed GQA
output uses the existing owned-output transfer protocol. After charging all
owners and call bridges, these changes remove 112 production lines / 3,144
bytes; new tests are reported separately. Eleven focused CUDA cases pass with
zero skips, including all 48 Q8 numerical/guard invocations, live parameter
field-address checks, overflow rejection and all-export adapter typechecking.
Post-merge CPU/CUDA resident descriptor conformance passes, including twelve
independent F64 normalization shape/family cases per backend. The existing
Metal-disabled GQA executor case remains skipped on Spark. SM89 split-K route
performance is not claimed on SM121; its eligibility and native images remain
unchanged.

CPU legacy-projection prototypes demonstrate why arity alone is insufficient.
The aggregate-accumulator owner failed 17 timing cases; restoring only scalar
call ABIs still failed 23, with worst ratio 1.103701. Neither result is admitted.
The next shared owner retains the original named planes, row views and separate
load/dot/MAC phases, with a compile-time absent third plane for pairs. Its six
focused composed tests pass, including 16 actual parallel worker cases. Both
legacy specializations recover their original non-relocation instruction
streams exactly (1,215 / 1,829 instructions); final timing remains pending.
This retains less source reduction than the rejected owner in exchange for the
original hot schedules. All failed sources, binaries and raw paired arms remain
under the U campaign artifacts.


The schedule-preserving CPU/Metal cohort is qualified. The full 32-arm
same-binary control passes all 424 cases (worst median ratio 1.048957); its
32-arm candidate comparison also passes every case (worst 1.024176). The
protocol retains 31 warmups, nine samples, 201 CPU calls / 101 Metal calls per
sample, equal-length executable paths and the unchanged 1.05 limit. Separate
Linux ARM validation passes 27 cases with no skips, including 16 actually
parallel worker cases, where-consume aliases and CUDA output/cache failure
cleanup. The prior failed accumulator and ABI-only prototypes remain excluded.

The complete Metal runtime and first descriptor cohort pass 239 cases with
the two existing skips. Follow-up normalization/alias/query checks pass all
seven selected cases with no skips. Ordinary and assigned RMS normalization
share the existing native Into owner; layer normalization consumes the existing
resident norm kernel. Linear admission examines only already-prepared immutable
F32 slots and never prepares a slot or allocates during a query. Its Into owner
preserves the ordinary single-row native function and the separate multi-row
function; audio callers retain their original multi-row policy. Eight
shape/bias cases pass independent F64 and bitwise ordinary-output checks across
three replays, along with allocation-free query and mutable-weight rejection.
CPU, Metal and CUDA ordinary norm adapters consume the existing checked row
geometry. Softmax/log-softmax share one ordinary Metal descriptor adapter.

This cohort removes 1,185 production lines / 39,460 bytes after charging the
Metal descriptor capability cost; shared conformance helpers and inline tests
are separate test costs. Its frozen source receipts are under
`u-campaign/shared-cpu-metal-pre-rope/`. The subsequent RoPE descriptor work is
a separately accounted capability extension and is not part of that saving.
The full composed model, direct-kernel and soak gates remain required.


A further Metal correctness regression was reproduced during normalization
review: distinct immutable resident layer-norm parameters shared an empty-host-
slice cache key, so the second parameter set used the first set's weights.
The independent F64 test fails on that baseline (actual -1.1832132 versus
expected -2.1164265). The key now consumes the existing physical tensor identity,
and parameter retirement removes only the matching slot. The numerical,
retained-output and retirement regression and existing RMS identity case both
pass, with no skips or leaks. This repair is separate from consolidation credit.

### U campaign integration and qualification findings

Merge `df86c32e32` incorporates `origin/main` at
`27011cf1c3b4f0768cbd1578ceb82d8d74e52382`. Main-only changes remain
excluded from the U source ledger.

The first composed CPU/Metal/CUDA executables pass all 19 supported model
comparisons, both independent CPU/Metal decoder references, and all seven
120-second lifetime soaks. Actual device allocation, transfer, selected packed
QKV and fused-route counters match the corrected baseline. The ONNX Metal
case uses 299 compute encoders instead of 353; the other forced-route counters
match. Runtime cache-capacity snapshots are recorded separately from actual
device payload ownership because those snapshots also vary between identical
baseline arms.

The first composed CPU primitive gate rejected the 1-by-8 normalization
backward case. Failed alignment, getter, scratch, vector, static-width and
ownership prototypes retain their sources, executable identities and raw arms;
none is admitted. Making the benchmark entry non-inlined alone did not close
the gate: it still inherited stack alignment from the expanded CLI. Both the
corrected reference and candidate were then rebuilt with the identical
non-inlined entry and 64-byte-aligned benchmark state. The generated entry
explicitly aligns the stack. With the original production kernel restored, all
five primitive cases and all three extended-warmup controls pass the unchanged
1.05 p50 / 1.10 p95 limits. The 1-by-8 p50 ratios are 1.025 and 0.975807.

The aligned benchmark changes the absolute timing conditions for both arms;
it does not establish an absolute kernel speedup over the earlier harness.
The original failed arms remain alongside the matched reference/candidate
sources and executable identities under `u-campaign/norm-aligned-harness/`.
Complete final executable and model qualification remains required.

All 558 direct CUDA cases pass on Spark. The 24-case attention cohort first
exposed a stale independent reference: its full-block oracle used finite
`-FLT_MAX` scores for invalid keys, yielding a uniform distribution when every
key was masked. Both local and full-block references now use negative infinity,
with zero output for an empty softmax. Only two expected-output buffers change;
all input buffers, manifest fields, guards, tolerances and the other reference
buffers are byte-identical. The original failure, original generator and
references remain available beside the repair receipt under
`u-campaign/attention-reference-fix/`. The corrected 24-case run passes numerical,
replay, guard and timing gates with maximum ratio 1.000569.

The fresh native CUDA image audit compares all seven targets against the
original U baseline. Each target retains 438 existing exports' instructions and
resources exactly. The existing full-block attention export carries the
separately tested all-masked correctness repair, and six new typed gather/scatter
exports are accounted as capability cost. SM89 route performance remains
unqualified on the SM121 machine. The main portable PTX 9.2 image and CUDA
decoder remain explicit admission negatives; the pinned training PTX 8.7 image
loads successfully.


Normalization failure injection found a further CPU correctness issue: the
legacy non-fallible reader cleared strided inputs after allocation failure and
normalization returned InvalidTensorShape instead of OutOfMemory. The ordinary
backward adapter now prepares potentially fallible reads before consuming the
existing dense getter. Preparation returns no slice payload; the getter keeps
its register return ABI and the existing scalar arithmetic and allocation
schedule. Six operand/materialization combinations cover strided views and
integer sources, preserve source bytes, shapes and ownership, and retry against
the original heap-gradient reference. All five focused Linux ARM tests pass
without skips.

The corresponding frozen CPU candidate passes all 39 paired comparisons:
the original five controls, three extended-warmup controls and every short
width from 1 through 31, at the original limits. Its 1-by-8 p50 is 53 ns versus
57.5 ns for the matched aligned reference. Direct checked-slice returns, whole
accessor inlining, a global dense prefix and a separate error-union result
wrapper failed either timing or ABI gates; those candidates remain excluded.
An unused private alias helper is also retired, removing 29 production lines
and 1,081 bytes independently of this repair.

At commit c9c340e200, the U-only net is 1,626 production lines / 44,677 bytes
removed. The consolidation cohorts contribute 2,175 lines / 79,091 bytes before
the separately charged repairs and capabilities; the unused-path retirement is
reported separately. Metal shader source has zero U delta. CUDA device source
adds 287 lines / 12,593 bytes for typed indexing and correctness, rather than
contributing deletion credit. Inline tests add 2,073 lines / 127,870 bytes and
remain outside the production budget. Later main-only changes, earlier campaigns
and generated bytes receive no credit. The final composed executable results are recorded below.


The supplementary file-family ledger separates host orchestration from device
source. Positive values mean removed; shared contracts include the new executed
descriptor capabilities.

| Maintained file family | Net lines removed | Net bytes removed |
| --- | ---: | ---: |
| CPU source | 1,537 | 55,814 |
| Metal host source | 789 | 27,686 |
| CUDA host source | 238 | 4,834 |
| Shared source and build contracts | -651 | -31,064 |
| CUDA device source | -287 | -12,593 |
| **Total** | **1,626** | **44,677** |

Metal shader source has zero net change within this U-only ledger.

### Final U acceptance

All U01–U56 acceptance conditions are closed with the measured retentions
recorded in [the outcome register](unified-execution-u-outcomes.md). The final
executables together pass all 19 supported model cases, 11 required controls, 31 additional
short-width controls and both independent decoder references. All seven
120-second soaks finish with zero tracked ownership at teardown. The final
paired device suites pass all 1,554 Metal / 558 CUDA cases. Both creation orders,
all raw arms, guards, resources, failed prototypes and repaired baseline/oracle
witnesses remain in the evidence bundle.

The final native inspection retains 655 / 553 / 619 instructions and 64-byte
alignment for the protected GEMM/attention leaves. Four projection bodies retain
1,215 / 1,829 / 18,758 / 35,383 instructions. The four remaining attention address
changes resolve to identical data bytes. Full GLiNER2 CPU/Metal/CUDA gradient and
optimizer/resume coverage executes all ten cases across the two hosts; GLiNER25
passes 85 cases with its existing optional-fixture skip. No supported path gains
a skip, and no unexpected CUDA driver error or ownership leak remains.

Final implementation is c9c340e200. The frozen 5,553-input fingerprint is
ba6eea8662f24e7330e116ee0681a481638e1887f9cd8558047436cac40558c8.
Executable and fixture/tool identities, expected negatives and separate source
ledgers are linked from the outcome register. Documentation-only commits do not
change that compiled source closure. No push or deployment is part of this work.

### V kernel-source consolidation

The V01–V40 follow-up is recorded in [the V outcome register](unified-execution-v-outcomes.md). It removes a further 242 maintained production lines / 19,948 bytes relative to clean main merge 4eedf47291, excluding earlier campaigns and upstream changes. Shared Metal/CUDA device phases and CPU binary owners preserve specialized schedules; allocation-failure and overflow findings have regression coverage. The final composed and direct validation matrix passes without relaxing gates; the outcome register preserves an initial attention timing failure and its passing diagnostic/full-family confirmation. Earlier campaigns receive no new reduction credit.

### W kernel-source size work

[The W progress record](unified-execution-w-outcomes.md) separates maintained
source reduction from checked-in generated expansion and physical repository
size. CPU reaches the 20% maintenance-source target with Mac validation passing;
the individual CUDA/Metal targets and CUDA device qualification remain pending.

### Z native kernel-family consolidation

[The Z outcome register](unified-execution-z-outcomes.md) records native
compile-time CPU/Metal/CUDA numerical sharing and removal of stored CUDA
attention expansions. It separates maintained-source savings from physical
text savings and records performance and device-qualification limits.

### AA native projection policies and decode correctness

[The AA outcome register](unified-execution-aa-outcomes.md) records native
quantized projection policies, narrow CPU panel sharing, shared paged decode
and its masked-window fixes. It distinguishes maintained code, stored
expansions, measured performance and transferred byte-identical CUDA evidence.

### AB native CPU four-column consolidation

[The AB outcome register](unified-execution-ab-outcomes.md) records one native
Q8 family for blocks, panels and fused SDOT, with preserved scale-load and
publication order. It also records the fractional-timing correction, retained
failed prototypes and explicit full-kernel instruction audits.

### AC native CPU Q6 panel consolidation

[The AC outcome register](unified-execution-ac-outcomes.md) records one native
Q6 panel8 owner for ordinary and fused row variants, preserved accumulation
schedules, signed-extrema oracle coverage and fixed performance comparisons.

### AD native CPU integer-dot tiles

[The AD outcome register](unified-execution-ad-outcomes.md) records one native
integer-dot owner with preserved result and instruction policies, signed-byte
oracles, native and non-SDOT performance matrices, and retained compiler and
performance findings from rejected prototypes.

### AE native CPU Q4/Q5 panel accumulation

[The AE outcome register](unified-execution-ae-outcomes.md) records one native
panel16 accumulation owner across direct and cached MR1/MR2/MR4/MR8 entries,
including generated cached-result wrappers. It separates maintained and
generated reductions, independent oracles, native/non-SDOT timing, unchanged
machine-code controls and explicitly changed projection resource bounds.

### AF native CPU panel and block consolidation

[The AF outcome register](unified-execution-af-outcomes.md) records the main
merge, Q4/Q5 sharing across eight/sixteen-column panels, Q6 panel16 rows and
K16 four-column block traversal, with post-merge baselines and separate
source/performance accounting.

### AG shared ordinary and packed Q4/Q5 subblocks

[The AG outcome register](unified-execution-ag-outcomes.md) records one native
arithmetic owner across ordinary panels and packed QKV, fixed-extent pointer
views, removal of packed-specific helpers and shared projection traversal.

### AH native Q3 four-column row ownership

[The AH outcome register](unified-execution-ah-outcomes.md) records shared
Q3 subblock traversal across one, two and four rows, preserving two-row dot
tiles, arithmetic order and the existing private result contracts.

### AI prepared float four-column row ownership

[The AI outcome register](unified-execution-ai-outcomes.md) records shared
Q4/Q5/Q6 float-input weight decoding and accumulation, retaining format-specific
traversal and four-column MR1/MR2 contracts.
