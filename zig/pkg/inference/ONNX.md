# ONNX

This document describes the ONNX backend as implemented today plus the remaining plan for large-model whole-graph artifacts. Whole-model offline compile/export, package-first artifact discovery, external-data weight storage, and `Q8_0` weight-only quantized export are implemented and in active use (see Current State and Quantized ONNX Status below). Broader shape-bucket coverage, larger native semantic prefill buckets, `run-artifact --compare-host` support for semantic phase artifacts, and K-quant ONNX representations remain open work, tracked as phases below.

## Direction

The target ONNX architecture is:

- offline compile
- load-only runtime
- shape-bucketed phase artifacts
- package-first artifact discovery
- large weights stored as external data
- exporter-facing weight streaming instead of execution-tensor export

The point is whole-model ownership, not permanent reliance on small partition islands.

## Current State

What works today:

- `compile-artifact --backend onnx` emits full-model and partition ONNX artifacts
- the default artifact root now mirrors the model namespace: `~/.antfly/inference/artifacts/<owner>/<model>/onnx/...`
- artifact manifests now carry an `artifact_role`, defaulting old sidecars to `prefill` and reserving `decode` for decoder-style ONNX entrypoints
- whole-model ONNX artifact directories now refresh a package manifest (`*.antfly-package.json`) that indexes compatible prefill/decode entries, and `compile-artifact` prints that package path as part of the result
- `compile-artifact --onnx-import-from <onnx> --artifact-role prefill|decode` can package an existing semantic ONNX decoder file as a Antfly inference artifact manifest without copying the model file, and now refreshes the same package manifest; this is the current bridge until native semantic decode export exists
- `run-artifact --validate` reports ONNX runtime state ownership and decode capability for offline artifacts, and package manifests can be validated directly
- `run-artifact <package> <prompt>` can resolve the matching prefill entry from a package manifest directly
- `generate --backend onnx --compiled-target whole-model --artifact-dir ...` now routes matching exact-shape ONNX packages through the graph `ModelExecutor` / `ModelRuntime` path instead of the one-token artifact shortcut, and uses metadata-only model loading so the request path does not keep a duplicate native model session resident beside ORT
- ONNX `ModelExecutor` can own a phase-aware package: one prefill manifest plus an optional decode manifest loaded as one runtime owner
- the GPT-2 semantic ONNX bridge proof can attach imported prefill/decode manifests and run `generate --backend onnx --compiled-target whole-model --max-tokens 2` through the whole-model runtime path
- native-exported GPT-2 phase packages also prove the same package-first whole-model runtime path, so the import bridge is no longer the only whole-model proof
- non-compare full-model ONNX artifact execution is routed through the shared `ModelExecutor` / `ModelRuntime` surface and reports `host_assisted_inputs` runtime state for current compiled graph artifacts
- artifact-backed ONNX `ModelRuntime` now owns a loaded ORT session for its lifetime instead of opening a short-lived session inside every `prefill`
- the graph cache now has a session-level whole-model runtime slot, separate from shape-specific compiled executors, so ONNX decode can carry cache/KV state across prefill and decode once artifact ABI support exists
- `src/graph/onnx_kv_cache.zig` owns the legacy host-tensor past/present cache mechanics; decoder-style ONNX sessions with `input_ids` plus matching past/present tensors now prefer `backend_owned` state through retained ORT `OrtValue` handles and IO binding
- artifact-backed ONNX `ModelRuntime` has guarded prefill/decode support for backend-owned past/present sessions: prefill seeds empty past tensors, retains `present.*` outputs in ORT-owned values, and decode binds those values back as `past_key_values.*`; logits are still copied out for sampling
- ONNX export has node-output name overrides, and the Antfly inference ONNX compiler can thread those overrides into both the serialized ONNX graph and manifest-facing input/output names
- native ONNX semantic decoder export now works for GPT-style attention and Gemma-style grouped-query attention; the GPT-2 and Gemma proofs emit native prefill/decode artifacts, validate them as backend-owned ONNX state, and generate multiple tokens through the whole-model `ModelRuntime` package path
- full whole-model `paged_prefill` ONNX artifacts now select the semantic prefill ABI automatically when `seq_len == query_seq_len`; traced node/range/debug artifacts remain node-oriented unless `--onnx-semantic-entrypoint` is explicitly requested
- ONNX export supports external weight blobs instead of forcing all initializers inline
- streamed external-data export now loads parameters lazily
- native dense weights can stream at native width when the traced graph dtype matches
- ONNX export now prefers an exporter-facing weight source for native sessions
- ONNX export can now open `Q8_0` GGUF weights through that exporter-facing source as block data instead of only as dequantized dense streams

## Imported ONNX Execution

Imported ONNX models used by `antfly inference embed` are different from offline
`compile-artifact --backend onnx` artifacts.

Imported ONNX files are a frontend:

```
ONNX file → ml.graph.Graph → generic graph runtime → selected backend policy
```

Offline ONNX artifacts are a compiled backend target:

```
ml.graph.Graph → ONNX artifact/package → ONNX Runtime-backed ModelRuntime
```

`ImportedOnnxSession` should not own an ONNX-specific graph runtime. Its job is to
parse/convert ONNX into Antfly inference's graph IR, build input/output metadata, and
delegate execution to the generic graph runtime described in
[GRAPH.md](GRAPH.md). That runtime lives in `src/graph/runtime.zig`.

The default imported-ONNX path remains interpreter-backed for correctness.
Opt-in partitioned execution is available for validating the generic runtime
seam:

```sh
antfly inference embed ~/.antfly/inference/models/antflydb/clipclap --backend metal --graph-runtime partitioned --text "hello world"
```

That path currently routes the converted graph through the generic partition
executor machinery. It is not yet a fully compiled Metal-resident graph because
the generic Metal partition executor and full op coverage are still future graph
runtime work, not ONNX-specific work. `TERMITE_GRAPH_RUNTIME` and
`TERMITE_ONNX_GRAPH_RUNTIME` remain compatibility/default fallbacks for
imported-ONNX tests and local scripts.

For benchmark and CI validation of imported resident paths, use the graph
runtime fail-closed gates documented in [GRAPH.md](GRAPH.md). For `termite
embed`, `--resident-projection-required` also rejects encoder/projection
fallback inside the CLIPCLAP embedding pipeline.

When graph runtime is explicit, it wins over the external ONNX Runtime binding.
The `.onnx` backend now routes `.onnx` files through `ImportedOnnxSession` first
when `SessionManager.graph_runtime_strategy` is set, and `ModelManager` carries
that setting through the cloned managers used for main model-session loading.
This is required for ClipClap-style bundles: the main text encoder and the
projection sidecar must both use the same Antfly inference graph runtime selection for
partition reports, residency counters, and Metal graph debugging to be
meaningful.

The stable session contract still exports imported-ONNX outputs as host
`Tensor` values, but imported graph sessions now have an internal resident
extension for graph composition. `runResident` returns backend-owned graph
tensors and `runResidentInputs` accepts compatible backend-owned graph tensors.
ClipClap embedding paths use this to keep encoder outputs resident through their
projection sidecars: text performs resident masked mean pooling before
projection, and image/audio perform resident CLS selection before projection.
Only the final projected embedding is exported to host.

That composition is still conservative: resident values only cross sessions
when the backend identity matches exactly. Imported ONNX sessions now make that
possible for GPU sidecars by using a ref-counted backend context. `LoadedModel`
loads compatible ONNX sidecars with the main imported session's context, so
Metal encoder outputs can be passed to projection sidecars without
constructing a second unrelated backend owner. This is shared graph/session
composition, not a separate ONNX runtime path.

Imported graph partitioning now seeds uploadable parameters and constants as
resident for the selected graph backend. For Metal, runtime inputs are also
promoted to device-resident tensors when a Metal partition starts. This removes
storage-only native fallback islands from CLIPCLAP-style ONNX imports; remaining
Metal gaps should appear as `metal_host_assisted_ops`, meaning the graph planner
can keep the partition intact but the backend still needs native Metal kernels
for those accepted ops.

What the latest measurements mean:

- GPT-2 safe full-model export uses the intended path end to end:
  - `lazy_streamed_inits=124`
  - `lazy_raw_inits=0`
  - `lazy_f32_inits=0`
- Gemma4 E2B safe estimate-only export currently reports:
  - dense path:
    - `estimated_loaded_bytes=5359416460`
    - `estimated_serialized_bytes=20097365132`
  - `q8_0_weight_only` path:
    - `estimated_loaded_bytes=5359416460`
    - `estimated_serialized_bytes=5672990114`
    - `dense_source_parameters=263`
    - `quantized_source_parameters=278`
    - `q8_0_candidate_parameters=278`
    - `q8_0_candidate_serialized_bytes=5644321046`

That is the right new problem shape:

- source-side export memory is materially reduced
- true weight-only `Q8_0` export materially reduces serialized ONNX bytes for the Gemma4 E2B estimate
- the next question is whether full export/runtime remain operationally acceptable, not whether quantized ONNX is pointless

## Core Decision

Large-model ONNX export must not depend on `ComputeBackend.getWeight(...)` as the long-term seam.

The correct seam is an exporter-facing weight source that can provide:

- logical shape
- logical dtype
- storage kind
- streaming write access in a requested target dtype

This matters because ONNX export needs different behavior than execution:

- execution wants runtime tensors
- export wants artifact-oriented streaming

For quantized GGUF weights, the exporter-facing source can expose `Q8_0`
blocks either as dense `f32` (streamed row-by-row) or as their native block
representation: `u8` values with a per-block `f32` scale and
`zero_point=128`. When the block representation is used, the exporter emits
`DequantizeLinear` with `block_size=32` and raises the model opset to at
least 21, since block-wise dequantization requires that opset.

## Quantized ONNX Status

> **Relocated:** The dated Q8_0 export/validation campaign that previously lived here (143 lines) is preserved verbatim in [work-log/completed/inference/onnx-quantized-status-history.md](../../../work-log/completed/inference/onnx-quantized-status-history.md). Durable decisions from it are in Core Decision in this document.

## Debugger Flags

`--node-range START END` is the preferred localized ONNX correctness debugger when a new exact-shape graph diff appears. `--debug-output-node N` exposes traced nodes as extra ONNX graph outputs so `run-artifact --compare-host` can compare them against captured native graph values. `--onnx-reuse-initializers-from <artifact.onnx>` lets debug ONNX protobufs reuse an existing external weight blob instead of writing duplicate multi-GiB weights.

> **Relocated:** The debugger bisection history and status previously under "Debugger Status" (24 lines) is preserved verbatim in [work-log/completed/inference/onnx-quantized-status-history.md](../../../work-log/completed/inference/onnx-quantized-status-history.md). Durable decisions from it are in this Debugger Flags section.

## Quantized ONNX Plan

The plan is phased.

### Phase 1: Visibility and classification

- track whether each exported initializer came from:
  - dense native storage
  - quantized source storage streamed as dense `f32`
- keep export estimates and profiles split by source kind
- use this to decide which weights are worth targeting first

Status:

- implemented for the current ONNX export/profile path

### Phase 2: Exporter-facing quantized weight metadata

- extend the exporter-facing source to expose:
  - storage kind
  - quantization type
  - source byte size
- keep this metadata attached through ONNX export profiling and planning

Status:

- implemented for native sessions; keep extending as more source formats need direct artifact export

### Phase 3: Select an ONNX quantization representation

We need one explicit ONNX strategy, not an ad hoc dense fallback.

Candidate options:

- Q/DQ graph form using `QuantizeLinear` / `DequantizeLinear`
- block-quantized dequantization form where ONNX opset support is sufficient
- restricted support for only a subset of GGUF types first

The practical rule is:

- do not claim “quantized ONNX export” until the emitted ONNX graph still stores quantized weights in the artifact

Status:

- implemented for `Q8_0`

Current implementation direction:

- first target is `Q8_0`
- represent `Q8_0` as block quantization on the ONNX side
- emit quantized values as `u8` with `zero_point=128`
- emit per-block scales as `f32`
- use ONNX `DequantizeLinear` with `block_size=32` along the last axis before normal dense matmul
- require external-data ONNX export for this path initially
- require ONNX opset 21 or newer for this path

### Phase 4: First supported GGUF quant families

Start with the subset that is actually defensible and measurable.

Likely order:

1. dense `f16` / `bf16` whole-model export path refinement
2. `Q8_0` weight-only ONNX export
3. K-quants only after we have a precise ONNX representation strategy

Status:

- `Q8_0` is the current supported weight-only target; K-quants remain future work

### Phase 5: Whole-model large-artifact rerun

After a true quantized ONNX export path exists:

- rerun Gemma4 E2B estimate-only export
- rerun profiled full export
- compare:
  - source loaded bytes
  - serialized bytes
  - export wall time
  - ORT load time

Success means:

- serialized bytes drop materially below the current dense-export estimate
- whole-model artifact generation becomes operationally reasonable

Status:

- achieved for Gemma4 E2B `q8_0_weight_only` paged-prefill artifacts:
  - dense estimate: about `20.10 GiB` serialized ONNX bytes
  - q8 estimate: about `5.67 GiB` serialized ONNX bytes
  - reusable q8 external weight blob: about `5.1 GiB`
  - per-shape ONNX protobufs remain small enough to regenerate for exact buckets

### Phase 6: Exact-shape correctness broadening

- compile representative shape-bucket artifacts that reuse the q8 external weight blob
- compare whole-graph outputs against traced native graph captures with `run-artifact --compare-host`
- use `generate --backend onnx` to verify the user-facing artifact lookup path for those same buckets
- if a new graph-capture diff appears, localize it with `--node-range` or reused-initializer debug outputs before changing lowering code

Status:

- in progress; current clean Gemma4 E2B q8 buckets are `seq_len=1`, `seq_len=16`, and `seq_len=27`

## Immediate Next Steps

1. Keep the exporter-facing source as the single large-model export seam.
2. Broaden exact-shape q8 explicit-KV coverage across more prompt lengths and attention modes.
3. Keep `run-artifact --compare-host` as the correctness gate for full ONNX graph artifacts.
4. Use `--node-range` or reused-initializer debug outputs only when a fresh graph-capture diff appears.
5. Measure and reduce ORT runtime memory separately from export size and graph correctness.
