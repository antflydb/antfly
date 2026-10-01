# ONNX quantized export status history

> Relocated verbatim from `zig/pkg/inference/ONNX.md` (lines 163–329 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`ONNX.md`](../../../zig/pkg/inference/ONNX.md). Durable decisions from this log were folded into that document before the move.

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

- the artifact reuses the existing external q8 weight blob and writes only a `41 MiB` ONNX protobuf plus a `6.6 KiB` antfly inference manifest
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
- package manifests are now the primary whole-model attach surface; raw `.antfly.json` sidecars remain for execution metadata and fallback compatibility
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

