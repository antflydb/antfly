# ONNX

This document tracks the ONNX backend direction, current constraints, and the concrete plan for large-model whole-graph artifacts.

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
- the default artifact root now mirrors the model namespace: `~/.termite/artifacts/<owner>/<model>/onnx/...`
- artifact manifests now carry an `artifact_role`, defaulting old sidecars to `prefill` and reserving `decode` for decoder-style ONNX entrypoints
- whole-model ONNX artifact directories now refresh a package manifest (`*.termite-package.json`) that indexes compatible prefill/decode entries, and `compile-artifact` prints that package path as part of the result
- `compile-artifact --onnx-import-from <onnx> --artifact-role prefill|decode` can package an existing semantic ONNX decoder file as a Termite artifact manifest without copying the model file, and now refreshes the same package manifest; this is the current bridge until native semantic decode export exists
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
- ONNX export has node-output name overrides, and the Termite ONNX compiler can thread those overrides into both the serialized ONNX graph and manifest-facing input/output names
- native ONNX semantic decoder export now works for GPT-style attention and Gemma-style grouped-query attention; the GPT-2 and Gemma proofs emit native prefill/decode artifacts, validate them as backend-owned ONNX state, and generate multiple tokens through the whole-model `ModelRuntime` package path
- full whole-model `paged_prefill` ONNX artifacts now select the semantic prefill ABI automatically when `seq_len == query_seq_len`; traced node/range/debug artifacts remain node-oriented unless `--onnx-semantic-entrypoint` is explicitly requested
- ONNX export supports external weight blobs instead of forcing all initializers inline
- streamed external-data export now loads parameters lazily
- native dense weights can stream at native width when the traced graph dtype matches
- ONNX export now prefers an exporter-facing weight source for native sessions
- ONNX export can now open `Q8_0` GGUF weights through that exporter-facing source as block data instead of only as dequantized dense streams

## Imported ONNX Execution

Imported ONNX models used by `termite embed` are different from offline
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
parse/convert ONNX into Termite's graph IR, build input/output metadata, and
delegate execution to the generic graph runtime described in
[GRAPH.md](GRAPH.md). That runtime lives in `src/graph/runtime.zig`.

The default imported-ONNX path remains interpreter-backed for correctness.
Opt-in partitioned execution is available for validating the generic runtime
seam:

```sh
termite embed ~/.termite/models/antflydb/clipclap --backend metal --graph-runtime partitioned --text "hello world"
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
projection sidecar must both use the same Termite graph runtime selection for
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

## Quantized ONNX Status

ONNX supports quantized models. This repo now has a real `Q8_0` weight-only export path for GGUF weights, and exact-shape whole-graph comparisons are passing for representative Gemma4 E2B prompt buckets.

Current quantized source behavior:

- GGUF quantized weights can now be opened by the exporter-facing source
- the source can either stream them row-by-row as dense `f32`, or expose `Q8_0` blocks as:
  - `u8` values
  - per-block `f32` scales
  - `zero_point=128`
- the first ONNX weight-only `Q8_0` emission path is now wired into the ONNX exporter
- that path emits `DequantizeLinear(block_size=32)` and the ONNX exporter now raises the model opset to at least 21 when this form is present
- estimate/profile accounting now follows the `Q8_0` path when that export mode is selected
- end-to-end runtime validation is now unblocked for exact-shape q8 paged-prefill artifacts:
  - `fused_rope` now lowers to standard ONNX ops for fixed-shape export
  - `fused_linear_no_bias_pair` now lowers to two standard `Gemm` nodes plus an explicit second-output side channel
  - grouped-query attention now lowers to standard ONNX ops when `num_heads % num_kv_heads == 0`, avoiding ORT-specific `GroupQueryAttention` availability
  - primitive `dot_general` and linear-family paths now insert explicit `Cast` nodes when ONNX math inputs would otherwise be mixed precision
  - schema-correct `Slice` inputs are now emitted as tensor inputs, not attributes
  - blocked `Q8_0` zero-point export now uses rank-matched tensors instead of a scalar zero-point

So today we have:

- lower source-memory pressure
- a first real quantized ONNX representation for `Q8_0`
- measured large-model serialized-byte reduction on the export path
- explicit paged/shared-KV graph inputs for whole-graph artifacts
- and a remaining runtime-memory/coverage problem, not a basic export or current exact-shape correctness blocker

That is why the dense-path Gemma4 E2B estimate still sat at roughly `20.10 GiB` serialized ONNX data, while the new `Q8_0` estimate sits at roughly `5.67 GiB`.

The current Gemma4 E2B `q8_0_weight_only` export profile is:

- `actual_loaded_bytes=4931597452`
- `actual_serialized_bytes=5220005281`
- `/tmp/gemma4_q80_full.onnx.weights.bin` is about `5.3 GiB`

That means the weight-only `Q8_0` path is now real on the write side.

The latest explicit-KV paged-prefill artifact proof is:

- safe estimate-only export reports `estimated_serialized_bytes=5672990114`
- full q8 paged-prefill export writes a small `.onnx` protobuf plus an external weights blob of about `5.1 GiB`
- `run-artifact /tmp/termite-full-paged-explicit-kv-q8.onnx a --raw-prompt --validate` succeeds
- the validated artifact loads as `kind=onnx_graph`, `inputs=41`, `outputs=1`, `seq_len=1`, `query_seq_len=1`, `attention_mode=paged_prefill`
- whole-graph `run-artifact` now accepts that multi-input ABI and closes the metadata ORT session before native KV materialization
- plain whole-graph execution of that q8 explicit-KV artifact succeeds and returns `token_id=107` for prompt `a`
- non-debug `--compare-host` now compares ONNX graph outputs against the manifest output-node captures from the traced native graph, instead of mixing in the older runtime-forward logits reference
- ONNX full-graph compare infers logit width from the ONNX output tensor shape, with tokenizer vocab size only as a fallback
- full non-debug `--compare-host` now completes with matching top-1 and a small graph-output diff:
  - `host_top1=107`
  - `artifact_top1=107`
  - `graph_mapping=node_2788->2506`
  - `graph_out0:max_abs_diff=0.00011062622`

The latest longer-prompt proof is the Gemma4 E2B chat-template prompt
`What is the capital of France?`, traced as `seq_len=16`, `query_seq_len=16`:

- the regenerated q8 paged-prefill artifact runs as a whole ONNX graph and returns `token_id=818`, text `The`
- full `--compare-host` reports matching top-1:
  - `host_top1=818`
  - `artifact_top1=818`
  - `last_logits:max_abs_diff=0.00011444092`
  - `last_logits:mean_abs_diff=0.000030030321795493364`
  - `graph_mapping=node_4050->2506`
  - `graph_out0:max_abs_diff=0.0002975464`
- this is stronger than a "sensible output" smoke test because it compares the ONNX graph output against the traced native graph capture for the same rendered prompt shape

The next broadened shape-bucket proof is the chat-template prompt
`Explain ONNX in one sentence, then name one advantage and one limitation for local inference.`, traced as `seq_len=27`, `query_seq_len=27`:

- the artifact reuses the existing external q8 weight blob and writes only a `41 MiB` ONNX protobuf plus a `6.6 KiB` termite manifest
- `generate --backend onnx` finds the exact-shape offline artifact and emits the same single-token result, `token_id=1018`
- full `--compare-host` reports matching top-1:
  - `host_top1=1018`
  - `artifact_top1=1018`
  - `last_logits:max_abs_diff=0.00015640259`
  - `last_logits:mean_abs_diff=0.000051486163812342056`
  - `graph_mapping=node_4050->2506`
  - `graph_out0:max_abs_diff=0.0002784729`

Two exporter fixes were needed for that longer prompt:

- grouped-query attention with `num_heads % num_kv_heads == 0` now lowers to portable standard ONNX ops instead of emitting `GroupQueryAttention`, which ORT does not provide at opset 21 in the runtime we are using
- partial half-split RoPE no longer adds identity entries to active partner lanes in the permutation matrix; this fixed the global Gemma attention path where `head_dim=256` and active `rope_dim=128`

The confirming node-closure checks for that RoPE fix were:

- `node 1159 = fused_rope([16x4096])` changed from `max_abs_diff=310.35373` to `max_abs_diff=0.00021362305`
- `node 1160 = fused_gqa_causal_attention([16x4096])` changed from `max_abs_diff=2.4352207` to `max_abs_diff=0.000022888184`

So the current ONNX blocker is no longer “can we export and run this exact shape at all?”
The current blocker is broader coverage and runtime memory, not the exact-shape traced-graph proof:

- whole-model Gemma4 E2B `q8_0_weight_only` now exports
- validates in ORT
- carries explicit paged/shared-KV inputs in the artifact ABI
- runs and matches the traced native graph for the exact `seq_len=1`, `seq_len=16`, and `seq_len=27` proofs
- routes normal whole-graph artifact execution through `ModelRuntime.prefill`; `generate --backend onnx --compiled-target whole-model` uses the same graph-level executor for matching prefill artifacts, while `run-artifact --compare-host` still uses the older comparison path because it needs debug-output capture plumbing
- the older full-runtime-forward compare still differs from the traced graph, but that is now a separate native-runtime-vs-trace question rather than ONNX lowering evidence
- package manifests are now the primary whole-model attach surface; raw `.termite.json` sidecars remain for execution metadata and fallback compatibility
- `compile-artifact` now has a generic per-initializer weight export policy:
  - `--onnx-weight-mode MODE` sets the default initializer export mode
  - repeated `--onnx-weight-policy SUBSTRING=MODE` overrides matching parameter names
  - the policy is intentionally format-generic; `q8_0_weight_only` is one mode, not a special q8-only command path
  - estimate-only export reports selected dense/q8 parameter counts and per-policy-rule match counts so policy bisection can be checked before writing a large artifact
  - the early bisection estimates and dense-policy artifacts remain useful as export-economics checks
  - their old `19.449177` full-runtime-compare result is no longer current ONNX evidence, because the compare path now uses traced graph captures for ONNX graph artifacts

The next ONNX step is therefore broadening, not more blind whole-model export work:

- keep the exact-shape compare harness in `run-artifact --compare-host`
- keep traced/debug graph artifacts on their current node-oriented ABI; semantic decoder ABI belongs to decode entrypoints, not every localized graph artifact
- retire the import bridge once native-exported semantic prefill/decode packages cover the external comparison cases it was preserving
- keep ONNX whole-model generation on metadata-only model loading; do not reintroduce a native runtime owner for tokenizer/config plumbing
- use the shared whole-model diagnostics when an exact-shape artifact is missing; ONNX now logs the requested role, artifact directory, model directory, `seq_len`, `query_seq_len`, and attention mode before returning to the shared `MissingCompiledModelRuntime` failure
- broaden semantic prefill coverage beyond the first validated prompt buckets; `seq_len=2/query_seq_len=2`, `seq_len=3/query_seq_len=3`, `seq_len=4/query_seq_len=4`, `seq_len=8/query_seq_len=8`, and the chat-template France prompt at `seq_len=16/query_seq_len=16` now export backend-owned semantic prefill packages, while larger buckets still need validation
- use node-range or reused-initializer debug artifacts as the primary localized correctness debugger; full artifacts are for exact-shape proof, not broad probing
- repeat the q8 explicit-KV proof across more prompt shapes and sequence/query lengths
- keep regression coverage for standard-ONNX GQA lowering and partial half-split RoPE permutation
- track runtime-forward-vs-traced-graph differences separately from ONNX lowering correctness

Native semantic decode export task list:

- [x] Prove the runtime/package shape with imported semantic ONNX prefill/decode manifests.
- [x] Add exporter and compiler support for semantic ONNX value names instead of only `node_<id>` names.
- [x] Confirm native traced `paged_decode` has current-token K/V projection nodes that can feed `present.*` outputs.
- [x] Add a semantic decoder entrypoint builder for native exports. The exporter now preserves single-token GQA nodes for semantic decode, adds ONNX `past_key_values.*` inputs, computes attention over past+current K/V, and emits concatenated `present.*` outputs.
- [x] Emit native `artifact_role=decode` ONNX files with `input_ids`, optional masks/positions, and matching `past_key_values.*` / `present.*` tensors for GPT-2.
- [x] Run `generate --backend onnx --compiled-target whole-model --max-tokens > 1` using only native-exported GPT-2 prefill/decode artifacts.
- [x] Generalize semantic decoder export from equal-head GPT attention to grouped-query attention (`num_heads > num_kv_heads`). Cache inputs/outputs stay in compact KV-head shape, while K/V are expanded inside the ONNX attention subgraph with standard `Reshape` + `Tile` + `Reshape` ops.
- [x] Validate a native Gemma-style GQA phase package through `ModelExecutor` / `ModelRuntime`. The `gemma-4-e2b-it-Q8_0.gguf` proof uses native semantic prefill/decode artifacts, the ONNX past/present ABI, a shared q8 external weight blob, and `generate --backend onnx --compiled-target whole-model --max-tokens 2`, which returned `token_ids: 107 106`.
- [x] Move ONNX phase packages from copied runtime-owned host KV tensors to retained ORT `OrtValue` cache state. The GPT-2 proof runs through `generate --backend onnx --compiled-target whole-model --max-tokens 2` with backend-owned past/present binding and returned `token_ids: 64 64`; the Gemma GQA q8 proof reused one shared external weight blob and returned `token_ids: 107 106`.
- [x] Validate that the Gemma GQA semantic decode artifact can advance beyond its first decode position. The same `seq_len=1` prefill package plus decode package returned `token_ids: 107 106 107 106 106 106` for `--max-tokens 6`.
- [x] Extend native semantic prefill export beyond the original single-token GQA path. The exporter now accepts multi-token semantic GQA nodes and inserts a static current-token causal mask for initial prefill; Gemma `seq_len=2/query_seq_len=2` and `seq_len=3/query_seq_len=3` validate as `backend_owned`.
- [x] Auto-select the semantic prefill ABI for eligible full whole-model prefill artifacts. Gemma `seq_len=4/query_seq_len=4` was compiled without `--onnx-semantic-entrypoint`, validated as `runtime_state_ownership=backend_owned supports_decode=true`, and generated through the phase package path with `token_ids: 496 505 513`.
- [x] Validate a larger native semantic prefill bucket while reusing the existing q8 external weight blob. Gemma `seq_len=8/query_seq_len=8` compiled without `--onnx-semantic-entrypoint`, wrote a `26 MiB` ONNX protobuf plus manifest, validated as `runtime_state_ownership=backend_owned supports_decode=true`, and generated through the phase package path with `token_ids: 496 496 496`.
- [x] Switch the semantic prefill proof from only synthetic raw prompts to a normal chat-template prompt bucket. Gemma `What is the capital of France?` rendered to `seq_len=16/query_seq_len=16` with `chat_template_applied=true`, compiled without `--onnx-semantic-entrypoint`, wrote a `32 MiB` ONNX protobuf plus manifest, validated as `runtime_state_ownership=backend_owned supports_decode=true`, and attached through `generate --backend onnx --compiled-target whole-model` with the existing decode artifact.
- [ ] Teach `run-artifact --compare-host` how to compare semantic phase artifacts through `ModelRuntime`; the older compare path still expects node-oriented ONNX graph inputs and rejects semantic phase inputs with `UnsupportedArtifactInputs`.
- [ ] Validate larger native semantic prefill buckets where `seq_len == query_seq_len > 16`, reusing the existing q8 external weight blob.
- [ ] Decide when to retire the import bridge; keep it while it remains useful for comparing native exports against external semantic ONNX files.

## Debugger Status

The long residual-drift bisection is now historical context, not the current ONNX diagnosis. It produced useful tools and exposed real artifact-boundary bugs, but the original late-residual mismatch is no longer treated as an active root cause.

Current debugger conclusions:

- `--node-closure` now preserves skip-KV/shared-KV attention inputs as runtime ABI inputs instead of walking through placeholder K/V nodes and baking zero tensors into debug graphs.
- ONNX partition export materializes synthetic second outputs from `fused_linear_no_bias_pair` when a partition boundary cuts across the pair node.
- `--onnx-reuse-initializers-from <artifact.onnx>` lets debug ONNX protobufs reuse an existing external weight blob instead of writing duplicate multi-GiB weights.
- `--debug-output-node N` exposes traced nodes as extra ONNX graph outputs so `run-artifact --compare-host` can compare them against captured native graph values.
- `--node-range START END` is the preferred localized ONNX correctness debugger when a new exact-shape graph diff appears.

The current correctness baseline is whole-graph traced-output comparison, not the older full-runtime-forward comparison:

- Gemma4 E2B q8 explicit-KV paged-prefill artifacts match traced native graph captures for `seq_len=1`, `seq_len=16`, and `seq_len=27`.
- The old residual carry trail around nodes such as `2461`, `2210`, and `2220` was invalidated by the explicit-KV/debug-artifact fixes and should not drive new work unless it reproduces on a fresh artifact.
- The remaining ONNX work is broader exact-shape coverage and runtime memory control. Localized lowering/debug work should resume only when a new graph-capture diff appears.

Runtime memory remains separate from correctness:

- Full-graph ORT debug compares can still climb into roughly the `15-21 GiB RSS` range.
- Full-graph multi-output debug compares are intentionally guarded.
- Future localization should prefer node-range or partition-chain probes over full-graph debug-output artifacts.

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
