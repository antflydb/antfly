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

## Proposed next bulk campaign: kernel bodies and typed launch ownership

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
