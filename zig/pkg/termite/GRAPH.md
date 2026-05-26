# Graph IR

Termite's graph IR is an XLA-inspired trace-once/cache/replay system for ML inference and training. Models are traced into a DAG of operations, optimized through compiler passes, and executed through backend-agnostic interpreters.

## Related Docs

These docs describe one runtime architecture from different layers:

- this file: generic graph IR, graph runtime, partitioning,
  compiled backend attachment, and memory planning.
- [GGML.md](GGML.md): GGUF/GGML format compatibility and the ggml execution
  shape Termite uses as a reference for packed quantized kernels.
- [METAL.md](METAL.md): the concrete pure-Metal backend plan, including
  backend-owned KV, quantized matmul, command planning, and scratch/frame
  lifetime.

## Architecture

```
trace → Pipeline.default(fold, fuse, cse) → cache → interpreter.execute()
```

1. **Tracing**: Model forward pass runs against `TracingCompute`, recording ops into a DAG (`lib/ml/src/graph/graph.zig`). Each node stores an `OpCode` (primitive or fused), input edges, output shape, and optional attributes.
2. **Optimization**: Compiler passes transform the graph before execution.
3. **Caching**: `src/graph/cache.zig` stores optimized graphs in an LRU cache (32 entries) keyed by `(config_hash, batch, seq_len, attention_mode)`. Weight tensors and reachability analysis are materialized on first execution and cached alongside the graph.
4. **Execution**: `src/graph/interpreter.zig` walks the DAG in topological order, dispatching each node to a `ComputeBackend` (BLAS, Metal, etc.). Intermediate tensors are freed based on precomputed last-use analysis.

## Generic Static Graph Runtime

The long-term execution model is a generic static graph runtime. ONNX is only
one frontend that can produce `ml.graph.Graph`; it should not own a separate
runtime architecture.

The intended flow is:

```
frontend/importer → ml.graph.Graph → graph.Runtime → backend executors
```

Examples:

```
ONNX file     → ml.graph.Graph → graph.Runtime → native / Metal / PJRT / ONNX executors
traced model  → ml.graph.Graph → graph.Runtime → native / Metal / PJRT / ONNX executors
debug capture → ml.graph.Graph → graph.Runtime → selected backend policy
```

The generic runtime should own:

- execution strategy selection
- shape and liveness analysis
- backend capability selection
- graph partitioning
- compiled executor attachment
- persistent constant/weight residency
- memory planning for intermediate tensors
- request input import and final output export

The runtime should support these strategies:

- `interpreter`: execute the whole graph through `interpreter.zig` and one `ComputeBackend`.
- `partitioned`: execute a capability-partitioned graph through `multi_executor.zig`, using eager backend ops unless a partition has an attached executor.
- `compiled_preferred`: attach compiled executors where possible and fall back to host partitions for unsupported islands.
- `compiled_required`: require one backend to own all compute partitions, otherwise fail instead of silently falling back.

CLI commands that run imported static graphs should expose this with
`--graph-runtime interpreter|partitioned|compiled|compiled-required`. The
environment variables `TERMITE_GRAPH_RUNTIME` and `TERMITE_ONNX_GRAPH_RUNTIME`
are compatibility/default fallbacks, not the primary user control surface.

This makes the layering:

- frontend/importer layer: ONNX import, traced generation graphs, debug captures
- graph runtime layer: planning, partitioning, liveness, memory, execution policy
- backend graph executor layer: native interpreter, Metal partition executor, PJRT/ONNX partition executors
- backend kernel/runtime layer: raw Metal kernels, native kernels, ORT, PJRT

`ModelRuntime` remains a higher-level model/session runtime. It is still needed
for decoder state, KV cache ownership, sampling, and phase-aware prefill/decode
packages. It should use the generic graph runtime when a model phase is best
represented as a static graph, but it should not be the only compiled graph
execution path.

For decoder serving, the intended layering is:

- graph IR expresses reusable structural operations and static phase graphs
- `ModelRuntime` owns stateful generation policy, KV mutation, token IO, and
  phase selection
- backend runtimes such as Metal own native buffers, packed weights, command
  submission, scratch lifetime, and device-specific kernels

This keeps backend-specific optimizations below the runtime boundary without
turning a backend helper into a second model executor.

The generic runtime seam lives in `src/graph/runtime.zig`.
`ImportedOnnxSession` is one caller of that runtime, not the owner of a separate
ONNX-specific execution architecture.

Explicit graph-runtime selection is authoritative for imported static graphs:
when a CLI/API path sets `--graph-runtime`, `SessionManager` routes `.onnx`
loads through Termite's imported graph runtime instead of the external ONNX
Runtime binding, and `ModelManager` preserves that strategy through per-backend
session-manager clones. This prevents a model phase from silently bypassing the
generic graph runtime while sidecar phases use it.

The stable public API remains `Session.run`, which returns host `Tensor` values.
Imported graph sessions now also expose an internal resident extension:
`runResident` returns backend-owned `CT` outputs, and `runResidentInputs` accepts
compatible backend-owned `CT` inputs. The embedding pipeline uses this for
ClipClap-style encoder/projection sidecars: text keeps encoder output resident
through masked mean pooling and projection, while image/audio keep encoder
outputs resident through CLS selection and projection. The encoder output no
longer needs to be exported to host and re-imported for projection on the native
graph path. The response boundary still exports the final embedding.

Imported ONNX sessions now use a ref-counted backend context. When a loaded
model opens sidecar sessions for compatible ONNX phases, those sidecars retain
the main session's context. This makes resident value passing safe for GPU
backends too: `runResidentInputs` still requires exact backend identity, but
Metal sidecars can satisfy that requirement by sharing the same backend
context instead of constructing independent contexts. Long term, broader
multimodal fusion should build on this shared-context model rather than
weakening resident tensor validation.

Backend-neutral model-serving contracts live in
`src/graph/backend_contracts.zig`. That file is the landing zone for request
shapes such as decoder layers, attention contexts, KV views, and decode batch
metadata. Concrete backend objects, device buffers, kernels, command buffers,
and API bridges stay in backend-specific modules.

### Metal-Resident Graph Execution

For Metal, the target is not "the eager interpreter using a Metal
`ComputeBackend`". The target is a compiled/static graph executor that can keep
the graph resident on Metal:

- upload constants and weights once when the runtime is created
- upload request inputs once per call
- execute supported graph partitions as Metal kernels
- represent reshape/transpose/view-compatible ops as metadata when possible
- keep intermediates in planned Metal buffers
- reuse scratch buffers from a liveness-based memory plan
- batch command submission per partition or whole run
- download only requested graph outputs

Unsupported ops should become explicit fallback islands:

```
Metal partition → transfer to native → native fallback partition → transfer back to Metal
```

That fallback is acceptable as an incremental path, but the goal for models like
CLIPCLAP is to shrink fallback islands to zero. The minimum useful Metal graph
coverage for that path is:

- dense `MatMul` / `Gemm` / linear without host materialization
- elementwise arithmetic
- metadata/view shape ops
- `LayerNorm`
- `Softmax`
- attention fusion after primitive correctness is proven
- CLAP audio frontend ops, including convolution/FFT-related paths

The current eager `MetalCompute` backend still preserves host-backed behavior
by default because existing unit tests and decoder fallbacks depend on it.
`TERMITE_METAL_UPLOAD_FLOAT32_INPUTS=1` is an explicit experiment knob for
runtime-input upload at `fromFloat32Shape`; it should eventually move behind a
graph-runtime policy rather than remaining a backend-wide environment flag.

For coverage diagnostics, the partition reporter can plan for one backend while
the process executes with another. This is the safe way to inspect Metal graph
coverage on machines or shells where constructing a Metal backend is unavailable
or risky:

```sh
TERMITE_GRAPH_PARTITION_REPORT=1 \
TERMITE_GRAPH_PARTITION_REPORT_TARGET=metal \
termite embed ~/.termite/models/antflydb/clipclap \
  --backend native --graph-runtime partitioned --text "hello world"
```

The report includes target/fallback node totals plus aggregated
`fallback_ops` and `metal_host_assisted_ops`. Missing-op work should be driven
from those aggregates, not from one failing runtime trace. Set
`TERMITE_GRAPH_PARTITION_REPORT_PARTS=1` only when the per-partition rows are
needed.

Benchmark and CI runs that validate resident graph paths should fail closed
instead of silently accepting slow partitions. Use
`TERMITE_GRAPH_RUNTIME_FAIL_CLOSED=1` to reject both native compute fallback
partitions and Metal host-assisted target ops. The narrower gates are
`TERMITE_GRAPH_RUNTIME_REQUIRE_NO_FALLBACK=1` and
`TERMITE_GRAPH_RUNTIME_REQUIRE_NO_HOST_ASSISTED=1`; compatibility aliases
`TERMITE_GRAPH_PARTITION_FAIL_CLOSED=1`,
`TERMITE_GRAPH_PARTITION_TARGET_REQUIRED=1`, and
`TERMITE_GRAPH_RUNTIME_FAIL_ON_FALLBACK=1` are also accepted. Parameter- and
constant-only host partitions are allowed by the fallback gate because they do
not represent compute fallback.

For CLIPCLAP-style imported graphs, Metal planning now treats parameters,
constants, and runtime inputs as uploadable static/runtime residency sources.
That keeps text, image, audio, and projection sidecars as single Metal
partitions in the planner. Remaining `metal_host_assisted_ops` are real kernel
residency gaps: the graph no longer has native fallback islands, but several
accepted ops still execute through host-assisted Metal backend implementations
until their native Metal kernels are completed.

Native CLIP/CLAP CPU model comparisons use the checked-in synthetic end-to-end
benchmark. The matrix mode follows the embedding measurement contract batches:
text runs `1/4/16/32`, image and audio run `1/4/16`. `--compare-quant` emits a
dense baseline row and a quantized candidate row for each target/batch pair.

```sh
zig build bench-clipclap-native -Donnx=false -Dmetal=false -- \
  --matrix --compare-quant --quant q5_k --warmup-iters 3 --measure-iters 15 \
  --format csv
```

CSV rows include `warmup_ms`, `avg_ms`, `p50_ms`, `p95_ms`, `min_ms`,
`max_ms`, `throughput_embeddings_s`, and native quant dispatch counters when
the build is run with `-Denable-native-quant-dispatch-stats=true`. The same
stats build also emits phase timing for native quant diagnosis:
`q8k_alloc_ms`, `q8k_quant_ms`, `q4q5_compute_ms`,
`q4q5_pair_compute_ms`, `q4q5_triple_compute_ms`, `dequant_fetch_ms`, and
`dequant_sgemm_compute_ms`. Use small layer counts such as
`--clip-text-layers 1 --clip-vision-layers 1 --clap-layers 1` only for smoke
checks; promotion gates should use representative model dimensions.

```sh
zig build bench-clipclap-native -Denable-native-quant-dispatch-stats=true \
  -Donnx=false -Dmetal=false -- \
  --target clip_text --batch 1 --clip-text-layers 1 --quant q5_k \
  --warmup-iters 1 --measure-iters 5 --format csv
```

For direct-quant-vs-cached-dequant diagnosis without shelling out twice, use
`--compare-dispatch`. It forces the CLIP/CLAP quant dispatch policy first to
direct quant and then to cached dequant-SGEMM in the same benchmark process, so
the rows share the same synthetic model dimensions and random seed.

```sh
zig build bench-clipclap-native -Denable-native-quant-dispatch-stats=true \
  -Donnx=false -Dmetal=false -- \
  --target clip_text --batch 1 --clip-text-layers 1 --quant q5_k \
  --compare-dispatch --warmup-iters 1 --measure-iters 5 --format csv
```

The lower-level exact-shape probe covers CLIP text, CLAP text, CLIP vision, and
CLAP audio-stage matrix sizes. It prints direct quant and cached
dense-dequant-plus-SGEMM timings for each supported quant/shape pair and skips
K-block formats whose block width does not divide the candidate input width.

```sh
zig build bench-clipclap-kernels -Donnx=false -Dmetal=false -- \
  --only-clipclap-quant-policy --warmup-iters 1 --measure-iters 5
```

Native graph/runtime execution now covers the measured GLiNER2 and CLIP/CLAP
encoder paths through planned native operator dispatch, including the GLiNER2
DeBERTa encoder plus head and CLIP/CLAP text, image, and audio graph paths.
Strict no-interpreter-fallback runs are the validation gate for covered encoder
paths. Native quant production policy lives in [NATIVE.md](NATIVE.md); GGUF
format and prepared-layout coverage lives in [GGML.md](GGML.md).

`bench-clipclap-kernels --only-clipclap-quant-policy` also prints
`clipclapDirectVariants` rows for supported Q4_K/Q5_K single-projection shapes.
These compare default direct against panel8 MR4, panel16 MR4/MR2, and cached
dequant-SGEMM. Q/K/V-specific sweeps stay isolated behind
`--only-packed-qkv`, `--only-clipclap-audio-quant`, or
`--include-direct-variants` so default benchmark modes remain comparable.

## Op System

Defined in `lib/ml/src/graph/node.zig`:

- **Primitive ops** (~30): elementwise unary/binary (add, mul, exp, sqrt, ...), reductions, reshape, transpose, gather, scatter, dot_general, conv, dtype conversion. These are the atoms that autodiff operates on.
- **Fused ops** (~40): linear variants, activations (gelu, silu, relu), rms_norm, attention, RoPE, MoE routing, softmax, argmax. Each fused op carries a `vjp_alternate` pointer to its primitive decomposition for gradient computation.

The builder API (`lib/ml/src/graph/builder.zig`) provides both levels. Fused ops are what model architectures emit; primitives are what autodiff and lowering produce.

## Compiler Passes

All in `lib/ml/src/graph/passes/`:

| Pass | File | What it does |
|------|------|-------------|
| Constant Folding | `const_fold.zig` | Evaluates pure-constant subexpressions on CPU at compile time |
| Fusion | `fuse.zig` | Pattern rewrites (identity elim, double-negation, inverse transpose) and multi-node fusion (QKV linear pair fusion) |
| CSE | `cse.zig` | Hashes `(opcode, attributes, resolved_inputs)`, deduplicates identical nodes |
| DCE | `dce.zig` | Backward walk from outputs, removes unreachable nodes, renumbers IDs |
| Memory Planning | `memory.zig` | Liveness intervals + greedy interval-coloring for buffer slot reuse |
| Pipeline | `pipeline.zig` | Composable pass sequences. `default` = fold → fuse → cse. `cleanup` = dce only |

## Execution Backends

Termite now treats graph execution as two layers:

- **Host backends**: own the baseline tensor runtime and can execute the whole graph through the interpreter.
- **Compiled graph backends**: optionally compile all or part of the graph into backend-specific artifacts/executors.

### Host backends

The `ComputeBackend` vtable (`src/ops/ops.zig`) abstracts tensor operations. Host backends implement fused ops for inference and optional primitive ops for training:

- **BLAS / native** (`src/ops/native_compute.zig`, `src/ops/native_compute.zig`): CPU backend using Accelerate/OpenBLAS and native helpers. Supports quantized weight formats (Q2_K through Q8_K, Q4_0, Q5_0).
- **Metal** (`src/ops/metal_compute.zig`): Apple GPU backend via Metal C API. Fused ops only (training primitives not yet implemented).

### Compiled graph backends

Compiled backends are registered under `src/graph/` and selected through the graph layer rather than generation-specific special cases:

- **ONNX** (`src/graph/compiled_onnx.zig`)
- **PJRT / XLA** (`src/graph/compiled_pjrt.zig`)

The shared interface lives in:

- `src/graph/compiled_backend.zig`
- `src/graph/compiled_registry.zig`
- `src/graph/execution.zig`

## Backend Partitioning

`src/graph/partition.zig` splits graphs across backends via capability declarations. Each compiled backend advertises:

- capability filter
- attach policy
- profitability / compile policy

The partitioner assigns maximal subgraphs to the highest-priority capable backend, with the selected host backend as the universal fallback.

Partitions track cross-partition edges (`external_inputs`) so the executor can transfer tensors between backends at partition boundaries.

This is useful for:

- experimental partitioned execution
- multi-device placement
- bounded compiled artifacts

It is not the intended final serving architecture for single-device decoder inference when it produces many small backend islands.

## Graph Cache Integration

The generation pipeline (`src/pipelines/generation.zig:graphForward`) uses caching to eliminate tracing overhead in autoregressive decode loops:

1. Build cache key from `(config_hash, batch=1, seq_len=1, mode=paged_decode)`
2. On miss: trace, optimize, cache
3. On hit: reuse graph, skip tracing and optimization entirely
4. Weight tensors and reachability analysis are computed once on first execution

This means the decode loop (which runs hundreds of times per generation) only pays tracing + optimization cost once.

Whole-model compiled execution has two lifetime layers in the same cache:

- `CacheEntry` owns shape-specific compiled executors for the traced graph shape.
- `GraphCache` can own a session-level `ModelRuntime`, so prefill and decode can share cache/KV state when the backend runtime is not just host-assisted input materialization.

## Offline Artifact Workflow

Compiled graph backends can now be materialized as offline artifacts rather than only through inline compilation.

CLI surface:

- `termite compile-artifact <model-dir> <prompt> ...`
- `termite run-artifact <artifact> <prompt> ...`
- `termite run-artifact <artifact> --validate`
- `termite generate ... --artifact-dir <path>`

Core files:

- `src/compiled_artifact.zig`: sidecar manifest format and lookup
- `src/native_compile.zig`: graph tracing + artifact export
- `src/native_run_artifact.zig`: validation and exact-shape execution

Artifact sidecars (`*.termite.json`) are termite-specific metadata. They record:

- backend
- artifact kind
- source model directory
- traced `seq_len`
- traced `query_seq_len`
- attention mode
- backend-specific metadata needed to run the artifact safely

This sidecar is required because the artifact file itself (`.onnx`, `.mlmodelc`) does not carry enough information for termite to know whether it matches a particular generation request.

Offline artifacts are currently exact shape buckets. A different prompt can use an existing artifact only when the rendered/tokenized prompt matches the artifact's recorded `seq_len`, `query_seq_len`, attention mode, and ABI. A prompt with a different token count needs a separate artifact or future dynamic/bucketed-shape support.

For ONNX whole-graph artifacts, `run-artifact --compare-host` compares artifact outputs against traced native graph captures for the same rendered prompt shape. The compare summary includes top-1 agreement plus last-logit and graph-output diffs, so a "sensible output" smoke test can be upgraded into an exact-shape correctness check.

### Debug Artifacts

Node-level artifacts are now part of the correctness workflow:

- `--node-index N --node-closure` exports the dependency cone for one traced node.
- `--debug-output-node N` adds traced nodes as extra artifact outputs for `run-artifact --compare-host`.
- `--onnx-reuse-initializers-from <base.onnx>` regenerates a small debug ONNX protobuf that reuses an existing external weight blob instead of writing duplicate multi-GiB weights.

Closure artifacts must preserve the same runtime ABI as the full graph for stateful attention. In particular, skip-KV/shared-KV `fused_gqa_causal_attention` K/V inputs are runtime cache inputs, not compile-time constants. Walking through those placeholder nodes creates false diffs in dependency-closed artifacts, so closure selection leaves them as external inputs and export keeps them materialized through the artifact input ABI.

## Key Files

| File | Purpose |
|------|---------|
| `lib/ml/src/graph/graph.zig` | Core DAG: nodes, outputs, parameters, constant pool, string table |
| `lib/ml/src/graph/node.zig` | OpCode definitions, PrimitiveOp, FusedOp, attribute structs |
| `lib/ml/src/graph/builder.zig` | High-level graph construction (fused + primitive ops) |
| `lib/ml/src/graph/shape.zig` | Shape, DType, ShapeConstraint (fixed/bounded/enumerated) |
| `lib/ml/src/graph/lower.zig` | Lowers fused ops to primitives via vjp_alternate |
| `lib/ml/src/graph/autodiff.zig` | Reverse-mode AD with ~25 VJP rules |
| `lib/ml/src/graph/passes/` | Compiler passes (see table above) |
| `src/graph/interpreter.zig` | Eager DAG executor, dispatches to ComputeBackend |
| `src/graph/cache.zig` | LRU graph compilation cache |
| `src/graph/partition.zig` | Capability-based backend partitioning |
| `src/graph/compiled_backend.zig` | Shared compiled-backend interface |
| `src/graph/compiled_registry.zig` | Compiled-backend registration |
| `src/graph/execution.zig` | Graph execution orchestration |
| `src/compiled_artifact.zig` | Offline artifact manifest and lookup |
| `src/native_compile.zig` | Offline artifact compiler CLI |
| `src/native_run_artifact.zig` | Offline artifact runner / validator CLI |
| `src/ops/ops.zig` | ComputeBackend vtable definition |
| `src/ops/native_compute.zig` | Native CPU backend implementation |
| `src/pipelines/generation.zig` | Generation pipeline with graph caching |

## Decisions

These are the current graph/backend architecture decisions:

1. Host backends and compiled backends are different layers.
   - `native` / `metal` are host execution backends.
   - `onnx` / `pjrt` are compiled graph backends.

2. Compiled backends should move toward offline compile + load-only runtime.
   - Request-path inline compilation is acceptable for experimentation and bounded partition artifacts.
   - It is not the target architecture for large decoder-serving backends.

3. Single-device decoder inference should prefer coarse ownership.
   - Many small compiled islands create boundary overhead and duplicate residency.
   - Reusable bounded artifacts are a stepping stone, not the destination.
   - The target architecture is whole-model compiled execution or very coarse block ownership.

4. Whole-model compiled backends need backend-native residency.
   - Keeping both full host weights and full compiled-backend weights live is the wrong steady-state architecture.

5. Shape-specific artifacts and manifests are first-class.
   - Compiled execution is shape-bucketed, not arbitrary per-request compilation.
   - Whole-model compiled execution is phase-aware: a model package can contain distinct prefill and decode entrypoints instead of forcing one graph artifact ABI to serve both phases.
   - The package manifest is the primary whole-model artifact surface; raw per-artifact sidecars are an implementation detail and fallback path.
   - Prefill artifacts stay bucketed by prompt/query shape; decode artifacts should use the stable `query_seq_len=1` decoder ABI with `input_ids` plus past/present state.

6. Whole-model vs partitioned attachment is an explicit runtime choice.
   - Compiled backends should be able to distinguish `partitioned` from `whole-model` attachment.
   - This belongs in shared graph/backend orchestration, not backend-specific env flags.

7. Export must not be built on execution tensors long term.
   - The correct export seam is a dedicated weight-export source with shape/dtype metadata and streaming access.
   - `ComputeBackend.getWeight(...)` remains an execution API, not the target architecture for large offline artifact export.
   - Exporters should be able to open a named weight as a stream in a requested target dtype and write it directly into an artifact sink.
   - Quantized sources should stay quantized until the final streaming conversion step instead of being widened into one large dense buffer first.

## Plan

Near-term graph/backend work:

1. Make package manifests the default compiled-backend surface.
   - `compile-artifact` should refresh one package index per backend/model/kind family.
   - `generate` and `run-artifact` should prefer package attach/lookup over raw manifest scanning.
   - Backend docs and user-facing examples should point at package manifests first.

2. Keep converging whole-model backends on shared `ModelExecutor` / `ModelRuntime`.
   - ONNX and PJRT/XLA already attach whole-model packages through that surface.
   - Compiled backend definitions now declare whole-model runtime strategy explicitly: unsupported or offline artifact.
   - Partitioned execution remains useful for experimentation, bounded artifacts, and stepping-stone adoption.

3. Keep moving runtime state ownership into the backend.
   - ONNX whole-model packages already retain ORT values for backend-owned past/present KV.
   - PJRT whole-model packages still need more of the decode path to stop being host-assisted.
   - Long-term whole-model backends should not require steady-state duplicate full-model residency.

4. Finish moving large-model export onto the exporter-facing weight source.
   - Dense native weights should stream at native width when the artifact dtype matches.
   - Quantized GGUF weights should dequantize row-by-row into the artifact sink.
   - Large export estimates and profiling should measure this path directly instead of inferring from execution tensors.
   - The first true quantized ONNX target should be weight-only `Q8_0`, not K-quants.
   - That first path should use backend-native source metadata plus ONNX `DequantizeLinear(block_size=...)`, not a fake dense fallback.

## Current Progress

Recent graph/backend progress is now best summarized at the architecture level rather than as the old ONNX node-by-node bisection log.

1. Offline artifact plumbing exists end to end.
   - `compile-artifact` emits backend artifacts plus termite sidecars and package manifests.
   - Default artifact directories now mirror the model namespace: `~/.termite/artifacts/<owner>/<model>/<backend>/...`.
   - `run-artifact` validates and executes exact-shape artifacts and can target a package manifest directly.
   - `generate` fast artifact lookup now prefers package manifests before raw sidecar scanning.

2. ONNX is the current whole-model proof backend.
   - Large ONNX artifacts use external data instead of inline multi-GiB protobuf initializers.
   - The exporter-facing weight source can stream dense, lazy, and `Q8_0` GGUF weights by name.
   - `q8_0_weight_only` stores Gemma4 E2B weights as ONNX block dequantization metadata rather than dense fallback bytes.
   - The reusable Gemma4 E2B q8 external weight blob is about `5.1 GiB`; per-shape ONNX protobufs are small metadata files beside it.

3. PJRT now has the same package-first whole-model shape.
   - Whole-model PJRT attach prefers phase packages through `ModelExecutor` / `ModelRuntime`.
   - Package manifests index prefill plus decode bucket chains by shape instead of relying on directory convention alone.
   - Whole-model generation now prefers matching `pjrt_executable` package entries, then matching HLO package entries, before falling back to inline graph compilation.

4. Exact-shape Gemma4 E2B ONNX correctness is currently clean for representative buckets.
   - `seq_len=1`, raw prompt `a`: top-1 matches and `graph_out0:max_abs_diff=0.00011062622`.
   - `seq_len=16`, chat prompt `What is the capital of France?`: top-1 matches and `last_logits:max_abs_diff=0.00011444092`.
   - `seq_len=27`, chat prompt `Explain ONNX in one sentence, then name one advantage and one limitation for local inference.`: top-1 matches and `last_logits:max_abs_diff=0.00015640259`.
   - These are traced-graph comparisons. Older full-runtime-forward deltas are not current ONNX lowering evidence.

5. The stale ONNX residual-drift bisection has been collapsed.
   - The old late-node mismatch trail was caused by artifact/debugger semantics around skip-KV/shared-KV state and partition boundaries, not a confirmed residual-path lowering failure.
   - Current debug tooling is `--node-range`, `--debug-output-node`, and `--onnx-reuse-initializers-from`; full dependency cones are avoided unless they reuse an existing external-data artifact.
   - If a new whole-graph diff appears, localized lowering work should start from fresh node-range or reused-initializer probes.

6. The next ONNX blocker is runtime memory and coverage breadth, not protobuf size or basic correctness.
   - ORT can still materialize a large full-graph working set, especially for debug-output compares.
   - More prompt lengths and attention modes need exact-shape coverage.
   - Export profiling should keep reporting source bytes, serialized bytes, lazy initializer counts, and stage timings.

7. The full compiled-backend architecture is still unfinished.
   - ONNX has the strongest proof path, and the shared graph layer now tracks `partitioned` vs `whole-model` as compiled attachment state.
   - Whole-model attachment is now explicit: ONNX and PJRT whole-model both prefer matching offline phase packages as the owner, while partitioned paths keep their stricter per-op eligibility rules.
   - `ModelExecutor` and `ModelRuntime` now define the shared type-erased whole-model runtime surface, distinct from `PartitionExecutor`; ONNX can attach a prefill artifact plus an optional decode artifact as one phase-aware executor, and PJRT whole-model runs use the same runtime surface.
   - Package manifests are now the primary whole-model attach surface for both ONNX and PJRT, with raw sidecar scans retained as fallback compatibility.
   - `GraphCache` now separates shape-specific compiled executors from session-level `ModelRuntime` state, which is the required lifetime model for KV/cache across prefill and decode.
   - Current compiled ONNX graph artifacts still cover prefill; the intended decode path is a separate `artifact_role=decode` ONNX entrypoint with a decoder-style past/present ABI, not retrofitting every traced/debug graph artifact to expose semantic KV.
   - The import bridge proved the runtime/package shape with existing semantic ONNX files; native GPT-2 artifacts now prove the same path with Termite-exported prefill/decode entrypoints.
   - Native `paged_decode` tracing already exposes current-token K/V projection nodes. The native semantic decoder entrypoint now turns those into ONNX `past_key_values.*` inputs and `present.*` outputs for equal-head attention and grouped-query attention; Gemma-style GQA now has a full native semantic prefill/decode package proof through `ModelExecutor` / `ModelRuntime`, retained ORT cache values, and shared q8 external weights.
   - ONNX whole-model phase packages now use ORT IO binding for backend-owned past/present cache state. Traced/debug graph artifacts with explicit node inputs remain host-assisted unless they expose the semantic decoder ABI.
   - Gemma GQA semantic decode has been validated for multiple decode steps from the same prefill cache; semantic prefill now covers the first multi-token buckets (`seq_len=2/query_seq_len=2`, `seq_len=3/query_seq_len=3`, `seq_len=4/query_seq_len=4`, and `seq_len=8/query_seq_len=8`) plus the chat-template France prompt bucket (`seq_len=16/query_seq_len=16`) with backend-owned ORT state.
   - `run-artifact --compare-host` still compares node-oriented ONNX graph artifacts; semantic phase artifacts need a `ModelRuntime`-based compare path.
   - ONNX whole-model generation now loads only manifest/tokenizer/config metadata before attaching the artifact package; the request path no longer keeps a native weight/session owner resident beside the ORT runtime.
   - PJRT/XLA now follows the same whole-model runtime surface, but backend-owned decode/KV is still incomplete and some shapes still fall back to inline graph compilation.
   - PJRT artifacts now report their load mode explicitly. Existing `pjrt_hlo` artifacts are still HLO compile-on-load through the configured plugin, while `pjrt_executable` artifacts deserialize plugin-native executables through the bound PJRT serialize/deserialize C API. Whole-model generation prefers matching `pjrt_executable` phase artifacts, then matching HLO phase artifacts, before falling back to inline graph compilation.
   - Bounded PJRT partition HLO and executable artifacts can now run through `run-artifact` with host-materialized graph inputs and `--compare-host`; the Gemma best-partition HLO and plugin-native executable proofs execute through the local CPU plugin and match native outputs within float noise.
   - GPT-2 whole-model prefill now exports as `pjrt_hlo`, but the default dense artifact embeds constants and is too large for reliable local CPU-plugin compile-on-load. Load-only executable export is wired and tested on a small executable, while bounded partition `pjrt_executable` export is proven; large dense whole-model executable export is budget-gated by `TERMITE_PJRT_MAX_EXECUTABLE_EXPORT_HLO_BYTES` until plugin compile/serialize capacity or external/offline constant handling is proven. `--xla-parameter-mode inputs` provides an explicit host-assisted validation bridge that shrinks HLO by passing graph parameters as PJRT inputs, and manifests record `pjrt_parameter_mode` so this bridge can coexist with embedded artifacts; it is not the final backend-owned weight residency model.
   - PJRT whole-model misses now report compute coverage directly, including unsupported node count, first unsupported op, and attention/RoPE blocker counts. Static 2D `fused_rope` now lowers to HLO constants plus a rotation matrix, and static batch-1 full-recompute GQA attention lowers to HLO dot/reduce/softmax. Offline semantic XLA export can lower static single-token skip-KV attention by adding past-KV parameters, concatenating past plus current K/V, and returning present K/V outputs. Remaining full-decoder misses should concentrate on dynamic/bucketed cache lengths, broader stateful attention, and dynamic RoPE layouts.
   - PJRT host-assisted single-token replay remains distinct from backend-owned KV/cache decode. PJRT artifact manifests now have semantic binding names for `input_ids` and past/present KV entries; semantic `present.*` outputs can populate the retained PJRT buffer cache, and semantic `past_*` inputs can feed retained buffers back into executable input slots. `PjrtModelRuntime` now shares one retained-buffer cache across attached prefill/decode HLO phases. GPT-2 semantic decode HLO now validates a real bucketed decode ABI with `input_ids`, 24 `past_key_values.*` inputs, and 24 `present.*` outputs; the remaining PJRT proof is executing the prefill/decode package without dense embedded HLO overwhelming the local plugin.

See also: [MULTIDEVICE.md](MULTIDEVICE.md) for multi-device inference, [TRAINING.md](TRAINING.md) for training support.
