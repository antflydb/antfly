# Graph/backend current-progress history

> Relocated verbatim from `zig/pkg/inference/GRAPH.md` (lines 490–545 at commit 271838a195, prior to a lift pass that added an item to the Decisions section above) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`GRAPH.md`](../../../zig/pkg/inference/GRAPH.md). Durable decisions from this log were folded into that document before the move.

## Current Progress

Recent graph/backend progress is now best summarized at the architecture level rather than as the old ONNX node-by-node bisection log.

1. Offline artifact plumbing exists end to end.
   - `compile-artifact` emits backend artifacts plus antfly inference sidecars and package manifests.
   - Default artifact directories now mirror the model namespace: `~/.antfly/inference/artifacts/<owner>/<model>/<backend>/...`.
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
   - The import bridge proved the runtime/package shape with existing semantic ONNX files; native GPT-2 artifacts now prove the same path with Antfly inference-exported prefill/decode entrypoints.
   - Native `paged_decode` tracing already exposes current-token K/V projection nodes. The native semantic decoder entrypoint now turns those into ONNX `past_key_values.*` inputs and `present.*` outputs for equal-head attention and grouped-query attention; Gemma-style GQA now has a full native semantic prefill/decode package proof through `ModelExecutor` / `ModelRuntime`, retained ORT cache values, and shared q8 external weights.
   - ONNX whole-model phase packages now use ORT IO binding for backend-owned past/present cache state. Traced/debug graph artifacts with explicit node inputs remain host-assisted unless they expose the semantic decoder ABI.
   - Gemma GQA semantic decode has been validated for multiple decode steps from the same prefill cache; semantic prefill now covers the first multi-token buckets (`seq_len=2/query_seq_len=2`, `seq_len=3/query_seq_len=3`, `seq_len=4/query_seq_len=4`, and `seq_len=8/query_seq_len=8`) plus the chat-template France prompt bucket (`seq_len=16/query_seq_len=16`) with backend-owned ORT state.
   - `run-artifact --compare-host` still compares node-oriented ONNX graph artifacts; semantic phase artifacts need a `ModelRuntime`-based compare path.
   - ONNX whole-model generation now loads only manifest/tokenizer/config metadata before attaching the artifact package; the request path no longer keeps a native weight/session owner resident beside the ORT runtime.
   - PJRT/XLA now follows the same whole-model runtime surface, but backend-owned decode/KV is still incomplete and some shapes still fall back to inline graph compilation.
   - PJRT artifacts now report their load mode explicitly. Existing `pjrt_hlo` artifacts are still HLO compile-on-load through the configured plugin, while `pjrt_executable` artifacts deserialize plugin-native executables through the bound PJRT serialize/deserialize C API. Whole-model generation prefers matching `pjrt_executable` phase artifacts, then matching HLO phase artifacts, before falling back to inline graph compilation.
   - Bounded PJRT partition HLO and executable artifacts can now run through `run-artifact` with host-materialized graph inputs and `--compare-host`; the Gemma best-partition HLO and plugin-native executable proofs execute through the local CPU plugin and match native outputs within float noise.
   - GPT-2 whole-model prefill now exports as `pjrt_hlo`, but the default dense artifact embeds constants and is too large for reliable local CPU-plugin compile-on-load. Load-only executable export is wired and tested on a small executable, while bounded partition `pjrt_executable` export is proven; large dense whole-model executable export is budget-gated by `ANTFLY_INFERENCE_PJRT_MAX_EXECUTABLE_EXPORT_HLO_BYTES` until plugin compile/serialize capacity or external/offline constant handling is proven. `--xla-parameter-mode inputs` provides an explicit host-assisted validation bridge that shrinks HLO by passing graph parameters as PJRT inputs, and manifests record `pjrt_parameter_mode` so this bridge can coexist with embedded artifacts; it is not the final backend-owned weight residency model.
   - PJRT whole-model misses now report compute coverage directly, including unsupported node count, first unsupported op, and attention/RoPE blocker counts. Static 2D `fused_rope` now lowers to HLO constants plus a rotation matrix, and static batch-1 full-recompute GQA attention lowers to HLO dot/reduce/softmax. Offline semantic XLA export can lower static single-token skip-KV attention by adding past-KV parameters, concatenating past plus current K/V, and returning present K/V outputs. Remaining full-decoder misses should concentrate on dynamic/bucketed cache lengths, broader stateful attention, and dynamic RoPE layouts.
   - PJRT host-assisted single-token replay remains distinct from backend-owned KV/cache decode. PJRT artifact manifests now have semantic binding names for `input_ids` and past/present KV entries; semantic `present.*` outputs can populate the retained PJRT buffer cache, and semantic `past_*` inputs can feed retained buffers back into executable input slots. `PjrtModelRuntime` now shares one retained-buffer cache across attached prefill/decode HLO phases. GPT-2 semantic decode HLO now validates a real bucketed decode ABI with `input_ids`, 24 `past_key_values.*` inputs, and 24 `present.*` outputs; the remaining PJRT proof is executing the prefill/decode package without dense embedded HLO overwhelming the local plugin.
