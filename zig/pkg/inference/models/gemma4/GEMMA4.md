# Gemma 4

This note tracks Antfly inference's Gemma 4 generation support, especially Google's
Gemma 4 Multi-Token Prediction (MTP) assistant models.

## Current Status

Antfly inference supports Gemma 4 as a native decoder family through the shared GPT
runtime. The model config already carries Gemma 4-specific metadata such as
sliding/global attention, shared KV-tail metadata, grouped-query dimensions,
per-layer embeddings (PLE), MoE metadata, and final logit softcapping.

For MTP, Antfly inference has a generic native speculative decoding path:

1. A draft model proposes up to `speculative_k` tokens.
2. The target model verifies the drafted span in one forward pass.
3. Matching draft tokens are accepted.
4. On the first mismatch, the target's token is accepted instead.
5. If all drafted tokens match, the target supplies one bonus token.

This is exposed through the server API with `draft_model` and `speculative_k`.
The CLI also supports it:

```sh
antfly inference generate /path/to/google/gemma-4-E2B-it \
  "Explain speculative decoding in one paragraph." \
  --draft-model /path/to/google/gemma-4-E2B-it-assistant \
  --speculative-k 4 \
  --speculation-policy auto \
  --speculation-calibration positive \
  --backend metal \
  --print-timing
```

Calibrated auto policy requires `--speculation-calibration positive`; CLI
calibration otherwise defaults to `none`, which does not activate Gemma 4 MTP
auto mode. Metal auto policy is also runtime-default-off; set
`ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO=1` to evaluate it. The inherited adaptive
cap (`k=2`) and acceptance threshold remain unchanged.

Branch-added Metal MTP accelerators remain explicit rollout opt-ins until
current model-level token-parity and runtime evidence is checked in:

- `ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE=1` and
  `ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE_TARGET_ACTIVATION=1` enable deferred
  correction/bonus materialization and target-activation reuse.
- `ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1` enables Metal bonus-token acceptance;
  CUDA and native retain their inherited default.
- `TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_TAIL_FRAME=1` enables the prepared
  verify-tail LM-head/argmax frame.
- `TERMITE_METAL_ENABLE_DONATED_SLOT_ATTENTION_ON_FRAME=1` enables direct
  donated-KV slot attention on the draft frame. The inherited
  `TERMITE_METAL_DISABLE_DONATED_SLOT_ATTENTION=1` remains the master rollback.
- `TERMITE_METAL_ENABLE_Q6_K_R2_REDUCE=1` and
  `TERMITE_METAL_ENABLE_SMALL_ROWS_NORM_REDUCE=1` enable the small-row MTP
  verify kernels. Their corresponding `DISABLE_` variables override opt-ins.
- `TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_SMALL_BATCH=1` enables the
  rows-2-to-8 shared-read gate/up kernel; its `DISABLE_` variable overrides it.
- `ANTFLY_GEMMA4_MTP_ENABLE_METAL_PREFILL_HIDDEN_CAPTURE=1` enables the
  prepared-tail prefill/hidden-state handoff; its `DISABLE_` variable overrides
  it.

The hand-written Metal chunked flash-prefill path is enabled with
`TERMITE_METAL_ENABLE_PREFILL_SG_ATTENTION=1`; its contiguous direct K/V load
requires `TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD=1`. Both passed the E4B
long-prompt token gate, but remain opt-in because the Metal runtime switch is
process-wide rather than scoped to the loaded model. Their matching `DISABLE_`
variables remain rollback overrides.

The baseline M4 Metal path also has three independently reversible policies:

- Sliding-window attention clamps the generated flash kernel's K/V scan to the
  live window by default. `TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP=1` restores the
  full logical-history scan. The choice is captured once when each Metal
  runtime is created, so concurrent model runtimes cannot race on policy state.
- Prepared single-token decode frames use retained-reference command buffers
  on qualified Apple M4 devices. `TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME=1`
  restores the diagnostic-safe command-buffer path, and
  `TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS=1` forces that path whenever
  profiling or debugging requires it.
- Q4_0 row-one matvec dispatch selects a device/shape-qualified threadgroup
  portfolio. `TERMITE_METAL_Q4_0_MMV_VARIANT=auto|legacy|nr4-nsg2|nr8-nsg2|nr4-nsg4|nr8-nsg4`
  provides deterministic qualification overrides, while
  `TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO=1` is the master rollback to the
  legacy selector.

Singleton intermediate Gemma 4 prefill chunks use the planned Metal frame by
default. `TERMITE_METAL_DISABLE_SINGLETON_SCHEDULED_PREFILL_FRAME=1` restores
the scheduled mixed-context path for diagnosis. Final logits and MTP hidden
capture never use the planned intermediate-chunk frame.

The typed KV policy stores Gemma 4 sliding-attention layers in a fixed Metal
ring while global-attention layers retain full KV history. The ring is disabled
for prompt-cache requests, cache compaction, and non-paged attention;
`TERMITE_METAL_DISABLE_SPLIT_SWA_KV_RING=1` is the master rollback while the
long-context rollout gate remains experimental.

The drafter must use the same tokenizer vocabulary and special token ids as the
target. Speculative decoding is currently native text-only generation; it is not
enabled for multimodal prompts or the ONNX direct path.

The Metal pipelined decode frame and the Q4_0 pair-activation gate/up fusion
are both default-on for M4-qualified devices (`TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME=1`
and `TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_FUSION=1` are their rollbacks).
The Metal decode split-GQA attention floor (below which every layer falls back
to the serial `paged_1x` kernel) is a per-model, topology-qualified default
read once at decode-runtime creation from `TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV`
(an invalid or zero override falls back to the model default; the full
rollback is `TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT`): Gemma4 E2B (8 query
heads / 1 KV head) defaults to 192 tokens because lower floors change the
generated stream; Gemma4 E4B (8 query heads / 2 KV heads) defaults to 32
tokens; qualified A4B geometries default to 32 tokens with the model-specific
`TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT` rollback.

On Apple M4-family devices, E2B local flash prefill, aligned Q4_0
projections, fused Q4_0 gate/up, and resident nucleus sampling are also
default-on; their eligibility rules and rollbacks are in
[METAL.md, M4 Serving Defaults](../../METAL.md#m4-serving-defaults).

`TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK=q4_k` remains an explicit diagnostic
opt-in, not a default: a pinned live-logit quality campaign keeps failing the
99% top-1-agreement gate despite a real ~3-5% decode win, so it is not
promoted. Repacking the LM head to Q4_0 specifically is refuted outright — a
symmetric no-min 4-bit format on the embedding/output matrix causes instant
EOT-collapse (matches llama.cpp's practice of never quantizing
`output.weight` below Q6_K); only Q4_K is a candidate at all, and even that
stays opt-in.

`ANTFLY_GEMMA4_MTP_AUTO_DRAFT_DISCOVERY` (server auto-discovery of a sibling
MTP assistant when no `draft_model` is requested) defaults off. Any backend
that auto-promotes a request to the compiled whole-model contract must treat
`speculation_requested` (derived from the *effective* drafter after policy
resolution) as an exclusion from that promotion: MTP verification needs the
target's final hidden rows, and the compiled contract exposes only
logits/tokens, so a speculative request must stay on the eager decoder-runtime
path instead of silently losing MTP eligibility.

## Chat REPL

`antfly inference chat` is the ollama-style interactive path: it resolves a
friendly model name, pulls the model from HuggingFace when missing, loads it,
and starts a multi-turn REPL:

```sh
antfly inference chat gemma4-e2b
```

`gemma4-e2b` and `gemma4-e4b` (plus `gemma-4-*` and `*-it` spellings) resolve
to Google's official QAT conversions `google/gemma-4-*-it-qat-q4_0-gguf` — the
checkpoints production workflows already run on; any `owner/name[:variant]`
reference or local model directory also works. The REPL
supports `/set`, `/show`, `/clear`, `"""` multi-line input, and Ctrl-C to stop
a response without leaving the chat (see `antfly inference chat --help`).

Chat can keep the model's `PromptPrefixCache` active across turns with a fresh
paged decode state per turn (`--prompt-cache`), so turn N+1 only prefills the
previous reply plus the new user message; the per-turn footer reports the
reused prefix as `N cached`. The flag is **opt-in and currently experimental**:
attaching a cached prefix reproducibly degrades the attached tokens' KV on
metal (temp-0 A/B: the model loses early-prompt context and asks clarifying
questions instead of answering) and can hang generation on the native backend.
Suspected area: block-hash cache entries carry per-block
`storage_block_id: ?KvBlockId` while only the simple-mode attach re-attaches
retained storage blocks (`attachSequenceWithRetainedBlocks`,
`runtime/kv/prompt_cache.zig`). Until that attach path is fixed, chat defaults
to full re-prefill each turn. Chat is target-only generation: no MTP assistant is pulled or used
because speculative decoding disables prompt-prefix reuse (the
`!use_speculative` eligibility gate in `pipelines/generation.zig`) and forfeits
the multi-turn TTFT win. Use `generate --draft-model` for the speculative path.

### Sampling performance and temperature

Sampled decoding (temperature > 0, chat's default is 0.7) runs through the
backend-owned sampled decoder frame
(`decoder_gated_runtime.forwardSampledToken`: device-resident Gumbel/top-k
sampling with a prepared sampled tail), the same fused frame family as greedy
decoding. Reference numbers on an M4 Pro with the E2B QAT q4_0 checkpoint:
~52 tok/s sampled, ~80 tok/s greedy. Before this wiring, non-greedy configs
fell off the fused frame onto the per-op eager path (~5 tok/s), and the host
sampler's top-k/top-p were O(k·vocab) rescans — both are fixed, so do not
"optimize" chat by forcing temperature 0 for speed.

One temperature note for the QAT checkpoints: at temperature >= 0.7 the model
sometimes spends its whole turn in the thought channel and ends without a
public reply (chat prints an explicit notice instead of a blank response);
temperature 0-0.3 transitions to the public answer reliably.

### Channel transition conventions

The final-channel projection accepts two observed checkpoint conventions for
the thought→public transition:

1. The explicit `<|channel>final\n<channel|>` header (Harmony style). When a
   stream contains this header anywhere, it wins and streaming emits deltas
   live from the header onward.
2. A bare `<channel|>` that closes the prompt-opened private channel with no
   replacement header — the GGUF conversions (Google QAT and ggml-org) emit
   only this form. It is accepted **only when no explicit header exists in the
   stream** (`bareChannelCloseRange` in `pipelines/generation.zig`), so a
   bare close inside a header-emitting stream cannot leak private content.
   Streaming cannot look ahead for the header-absence proof, so bare-close
   streams surface their text once at end of turn via `GenerationResult.text`
   rather than token-by-token; the CLI and chat REPL both print that fallback.

Explicitly opened non-final channels stay private under both conventions, and
a stream with no recognized transition still projects to empty output.

## Google Gemma 4 MTP Design

Google's MTP assistants are not just arbitrary smaller language models. They
are paired with a target Gemma 4 checkpoint and are designed to reduce drafting
overhead:

- The assistant shares the target input embedding table.
- The assistant consumes target last-layer activations.
- The assistant concatenates target activations with token embeddings and
  down-projects into the drafter width.
- The assistant can share target-side KV/cache state instead of recomputing the
  whole prompt independently.
- E2B and E4B assistants include an efficient clustered embedder to avoid a
  full-vocabulary projection for every draft step.

## Source and Artifact Confirmation

Sources checked on 2026-05-05:

- Google's launch post and Hugging Face model cards describe Gemma 4 MTP as
  assisted/speculative decoding: an assistant proposes multiple tokens, and the
  target model verifies them in parallel while preserving target quality.
- Hugging Face exposes `google/gemma-4-*-assistant` as Transformers
  `AutoModelForCausalLM` artifacts, with `generation_config.json` marking
  `"is_assistant": true`, `"num_assistant_tokens": 6`, and a constant assistant
  token schedule.
- The public Transformers `v5.7.0` and `v5.8.0` Gemma 4 source does not expose
  `Gemma4AssistantForCausalLM` or `gemma4_assistant` implementation details.
  The public source trail for runtime behavior is currently LiteRT-LM, not the
  tagged Transformers Gemma 4 model files.
- LiteRT-LM's MTP drafter runtime loads a `tf_lite_mtp_drafter` model section,
  uses a base-model `verify` signature, and drafts greedily. It concatenates
  token embeddings with the verifier/base activations into an `activations`
  input, runs the drafter repeatedly, and verifies `G + 1` target positions in
  one pass. On mismatch it accepts the verifier token; on full match it accepts
  the verifier bonus token.

Confirmed assistant artifact structure:

- `antfly inference pull google/gemma-4-E2B-it-assistant` downloads the official
  safetensors assistant into
  `~/.antfly/inference/models/google/gemma-4-E2B-it-assistant`.
- `google/gemma-4-E2B-it-assistant` config:
  - `architectures`: `Gemma4AssistantForCausalLM`
  - `model_type`: `gemma4_assistant`
  - `backbone_hidden_size`: 1536
  - compact text stack: 4 layers, hidden size 256, 4 attention heads, 1 KV head,
    sliding attention for layers 0-2 and full attention for layer 3
  - `use_ordered_embeddings`: true, `num_centroids`: 2048,
    `centroid_intermediate_top_k`: 32
- E2B assistant safetensors header:
  - `pre_projection.weight`: `[256, 3072]`
  - `post_projection.weight`: `[1536, 256]`
  - `model.embed_tokens.weight`: `[262144, 256]`
  - `masked_embedding.token_ordering`: `[262144]`
  - `masked_embedding.centroids.weight`: `[2048, 256]`
- E4B assistant safetensors header:
  - `pre_projection.weight`: `[256, 5120]`
  - `post_projection.weight`: `[2560, 256]`
  - otherwise follows the E2B compact 256-wide, 4-layer drafter shape
- 26B-A4B assistant safetensors header:
  - `pre_projection.weight`: `[1024, 5632]`
  - `post_projection.weight`: `[2816, 1024]`
  - `model.embed_tokens.weight`: `[262144, 1024]`
  - no `masked_embedding.*` tensors in the inspected safetensors header

The projection shapes confirm LiteRT-LM's runtime contract: MTP drafter input is
`concat(token_embedding, verifier_or_target_activation)` at
`2 * backbone_hidden_size`, the compact assistant stack runs at its own hidden
size, and `post_projection` returns to target/backbone hidden size for the next
chained draft step.

Runtime findings from implementation:

- The assistant is query-only. It owns Q/O projections and MLP weights, but no
  K/V projections. All assistant layers must read target K/V banks.
- The 4 assistant layers do not map to target layers 0-3. They map by attention
  type to the target's last non-shared KV donor layers:
  - E2B target: sliding donor layer 13, full-attention donor layer 14.
  - E4B community LiteRT extraction reports the analogous banks as layers 22
    and 23.
- E2B/E4B `masked_embedding.token_ordering` is a full vocabulary permutation,
  and `masked_embedding.centroids.weight` is `[2048, 256]`. This supports a
  clustered output head: score centroids, keep the configured top 32 centroid
  groups, then select the best token inside those groups from assistant
  embedding logits.
- The official E2B assistant config says `tie_word_embeddings = true` and does
  not include an explicit `lm_head.weight`; the current implementation uses the
  assistant embedding matrix for logits, then applies the clustered mask when
  `masked_embedding.*` tensors are present.
- MLX-VLM's public Gemma 4 assistant implementation and the SeatownSin
  extracted PyTorch drafter both highlight runtime details that are easy to get
  subtly wrong:
  - the target activation passed to the drafter is the target hidden state that
    predicted the current token, not the hidden state after consuming that
    token;
  - the drafter position id is held constant during an autoregressive MTP draft
    block.
- The extracted PyTorch drafter captures the output of `text_model.norm`, so
  Antfly inference now uses final-RMSNorm target hidden states for both target logits and
  MTP drafter handoff. The older pre-final-RMSNorm handoff is retained only as
  implementation scaffolding for comparison.
- The public `masked_embedder.py` implementation treats
  `masked_embedding.token_ordering` as centroid-to-token ordering:
  `ordering[c * cluster_size .. (c + 1) * cluster_size]` is the token set for
  centroid `c`. That matches the current baseline implementation; the inverse
  interpretation is now only a debug experiment.

The current Antfly inference implementation uses the same acceptance/verification
algorithm. Phase 1 used an independent decoder drafter. Phase 2 now has a
Gemma-specific MTP draft step that consumes target hidden activations, reads
target K/V, and chains projected activations. Verification is still target-owned.

## Implementation

### Generic Assistant Drafters

Implemented for the native server API and CLI.

- Load an optional `draft_model` alongside the target model.
- Validate tokenizer compatibility before generation.
- Allocate a separate draft KV manager and decode state.
- Prefill target and draft with the same text prompt.
- Run the existing draft/verify speculative loop.
- Report speculative rounds, drafted tokens, accepted draft tokens, rejected
  draft tokens, corrections, and bonus tokens in CLI timing output.
- Disable direct ONNX and one-token artifact shortcuts when a drafter is
  requested so generation cannot silently ignore the assistant.

This should work with Gemma 4 `*-assistant` checkpoints if they are exported in
a format the native loader understands as a decoder-only model.

### Gemma 4 MTP Runtime

A Gemma-specific drafter runtime understands assistant checkpoints as MTP
heads instead of independent decoders, with Gemma-specific runtime ownership
and remaining acceptance-rate investigation (see Open work):

1. Model metadata parsing for MTP assistant structure covers:
   - `model_type = "gemma4_assistant"` and
     `architectures = ["Gemma4AssistantForCausalLM"]`,
   - `backbone_hidden_size`,
   - assistant layer count and hidden size,
   - `pre_projection.weight` and `post_projection.weight`,
   - clustered embedder metadata for E2B/E4B where present,
   - explicit target-model compatibility identifiers when available.
2. Target drafter activations are exposed from the target decode pass for
   native generation through `forwardAllLogitsAndHiddenHost` and
   `materializeAcceptedTokenKvAndReturnHidden`. The MTP path uses final
   RMSNorm hidden for the drafter handoff, matching the extracted PyTorch
   reference's `text_model.norm` hook.
3. A Gemma 4 MTP draft helper in `src/architectures/gemma4_mtp.zig`:
   - borrows or aliases target token embeddings at the target/backbone width,
   - consumes target final hidden activations,
   - builds drafter inputs from `concat(token_embedding, target_hidden)`,
   - runs the assistant transformer stack,
   - produces assistant logits and clustered candidate logits,
   - retains the drafter's `projected_activations`/post-projection output so the
     next draft step can chain from the prior assistant step without rerunning
     the target.
4. Independent draft prompt prefill is replaced with target-activation seeding
   for `gemma4_assistant` draft configs.
5. The existing verification path is unchanged: target-side verification is
   what preserves output quality and sampling semantics.
6. Telemetry is partial: `ANTFLY_GEMMA4_MTP_DEBUG=1` prints drafted
   token ids and verifier choices for acceptance debugging.
7. Gemma 4 runtime-specific construction lives in
   `src/architectures/gemma4_runtime.zig`:
   - the explicit backend contract is `gemma4_gated_ple_shared_kv`,
   - shared-KV layer specs, PLE slots, head-norm slots, and final/tail slots are
     built by the Gemma 4 architecture module,
   - per-layer output scales are resolved to scalar runtime metadata for the
     whole-frame path instead of retained backend tensors,
   - Gemma 4 MTP assistants skip standalone shared-decoder prewarm so valid
     assistant artifacts no longer emit the stale `MissingWeight` warning.

The runtime runs end-to-end and preserves target-owned verification, but MTP
acceptance against a local quantized GGUF target is still far below published
best-case numbers; likely causes are source/model pairing differences between
the official safetensors assistant and the local GGUF target, quantization
effects in the target, or a still-missing detail in the clustered output head
(see Open work).

> **Relocated:** The dated smoke-test acceptance narrative that previously
> lived here (27 lines) is preserved verbatim in
> [docs/design/inference/history/gemma4/mtp-cuda.md](../../../../../docs/design/inference/history/gemma4/mtp-cuda.md).
> Durable decisions from it are in the paragraph above and in Open work.

### CUDA Branch Status

CUDA MTP remains experimental. Its diagnostics are not a production-readiness
certification and are not a paired llama.cpp comparison; no throughput result
from that path should be described as superiority over llama.cpp. The CUDA
release contract covers target-only Gemma 4 QAT (`release_scope: target_only`),
while strict MTP certification and promotion remain follow-up work.

CUDA Gemma 4 defaults are owned elsewhere: tuned-profile defaults, the
promoted SM89 flash-prefill and BF16 tensor-core prefill routes, and the
SM89/E2B optimization track live in [CUDA_TUNING.md](../../CUDA_TUNING.md);
generated Q4_0 kernel opt-ins and their evidence in
[QUANT_KERNEL_COMPILER.md](../../QUANT_KERNEL_COMPILER.md); and TurboQuant KV
(`f32` default, `polar4` production-candidate opt-in, `turbo3` experimental) in
[CUDA.md, Gemma4 And TurboQuant KV Status](../../CUDA.md#gemma4-and-turboquant-kv-status).

Useful CUDA/MTP isolation flags:

- `ANTFLY_CUDA_DISABLE_GEMMA4_MTP_DEVICE=1`: use the host clustered-output
  fallback instead of the CUDA MTP masked-argmax kernel.
- `ANTFLY_CUDA_ENABLE_Q4K_DECODE_FAST=1`: enable the experimental Q4_K tile8
  decode path.
- `ANTFLY_CUDA_DISABLE_HEAD_NORM_ROPE_FUSION=1`: disable fused
  head-norm+RoPE.
- `ANTFLY_CUDA_ENABLE_ADD_MUL_SCALAR_FUSION=1` and
  `ANTFLY_CUDA_ENABLE_RMSNORM_ADD_MUL_SCALAR_FUSION=1`: enable experimental
  output-scale fusions.
- `ANTFLY_GEMMA4_MTP_ALLOW_UNSHARED_TARGET=1`: force experimental MTP against
  targets missing shared-KV metadata.

> **Relocated:** The dated per-branch update log and smoke-test history that
> previously lived here (219 lines) is preserved verbatim in
> [docs/design/inference/history/gemma4/mtp-cuda.md](../../../../../docs/design/inference/history/gemma4/mtp-cuda.md).
> Durable decisions from it are in the paragraphs above.

### Metal GGUF Runtime Status

The Metal GGUF path now routes explicit compiled generation through graph
execution instead of a separate live whole-model shortcut:

```sh
antfly inference generate ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf \
  hi \
  --backend metal \
  --mode compiled \
  --compiled-target whole-model \
  --max-tokens 4 \
  --print-token-ids \
  --print-timing
```

Under the graph route, Metal uses the resident decoder runtime directly for
whole-model prefill/decode. Pure greedy generation can return the selected token
without downloading full logits, so the short anchor prompt now reports
`prefill cached_logits=false greedy_token=true` and decode-side
`greedy_fallback=0`.

The generic quant runtime surface is separated from the Q8_0-specific kernel
implementation: public runtime scratch/setup exports, debug env vars, and
timing labels use `quant` names, and the Q8_0 fused kernels remain internal
fast paths. Adding Q4/K-quants should extend the quant-format dispatch
behind those generic entrypoints instead of creating more public `q80` API.
The direct whole-layer block planner follows the same shape: it asks for a
direct quantized block format and currently selects the Q8_0 implementation
only when every participating linear slot is Q8_0, falling back through the
staged generic quant linear path for unsupported or mixed formats. The
staged FFN side can still use existing fused Metal kernels for non-Q8
families (homogeneous Q4_K, Q6_K, I2_S, TL1/TL2, Q8_0, plus mixed
Q4_K/Q5_K-down, Q4_K/Q6_K-down, and Q4_0/Q8_0-down layouts), marked
direct-eligible rather than mixed/unsupported. The device-resident FFN
residual path follows the same generic shape: Q8_0 keeps the monolithic
fused kernel, while non-Q8 formats that have staged pair and single-stage
Metal kernels compose gate/up, activation, multiply, optional RMS norms,
down projection, and residual add without leaving device memory or adding
format-specific public APIs.

The Metal runtime keeps a materialized-logits argmax route rather than a
standalone prepared-tail greedy shortcut (an earlier direct `rms_norm +
quantized lm_head + argmax` shortcut outside a planned frame caused a SoC
watchdog reset under Metal API validation). The native Metal GGUF route keeps
`.metal` sessions on the native Metal provider/stream path.

> **Relocated:** The dated validator-smoke bisection history that previously
> lived here (53 lines, 2026-05-07) is preserved verbatim in
> [docs/design/inference/history/gemma4/mtp-cuda.md](../../../../../docs/design/inference/history/gemma4/mtp-cuda.md).
> Durable decisions from it are in the paragraph above and in Current Status.

Baseline, no-MTP prefill/decode optimization is tracked separately in
the [Metal Performance Plan](#metal-performance-plan) section below. That plan owns
the pinned llama.cpp comparison, current experiment ledger, promotion gates,
and ordered Metal kernel/runtime tranches. MTP speedups are additive and must
not be used to qualify the baseline model path.

## Correctness Rules

- The target model always owns final token acceptance.
- Sampling, repetition penalties, and grammar masks must be applied from the
  target logits during verification.
- Rejected draft suffixes must be rolled back from KV state.
- Correction and bonus tokens must be present in target KV before they are
  consumed by later target work. The supported deferred-materialization path
  may fold that materialization into the next verify round; it flushes any
  pending token before another operation that requires committed target KV.
  Gemma 4 MTP assistants have no drafter KV; they keep only the
  target-prediction activation needed to seed the next draft round.
- MTP must fall back to standard decoding if the assistant is missing,
  incompatible, or slower for the current backend.

## Open work

- MTP acceptance rate is still far below published best-case numbers (2 of 41
  drafted tokens accepted in one longer local smoke); likely causes include
  source/model pairing differences between the official safetensors assistant
  and local GGUF targets, target quantization effects, or a still-missing
  detail in the clustered output head.
- Telemetry beyond `ANTFLY_GEMMA4_MTP_DEBUG=1` (structured acceptance-rate
  reporting) is not built out.
- Prompt-prefix cache attach (`--prompt-cache` in `chat`) reproducibly
  degrades attached-token KV on Metal and can hang generation on native; see
  "Chat REPL" above. Chat defaults to full re-prefill each turn until fixed.
- Baseline (non-MTP) prefill/decode performance work is tracked separately in
  the [Metal Performance Plan](#metal-performance-plan) section below: speculative-k tuning, an
  acceptance-adaptive draft-length schedule, MoE-vs-dense benchmarking
  separation, and batched server benchmarking for MoE models.
- CUDA MTP remains experimental; strict MTP certification and promotion
  remain follow-up work (see "CUDA Branch Status" above).
- What is the exact public Transformers implementation for
  `Gemma4AssistantForCausalLM`? The tagged public Gemma 4 files do not yet show
  it, so implementation follows confirmed artifacts plus LiteRT-LM behavior
  until upstream source is visible.
- Do assistant checkpoints expose enough metadata to validate exact target
  compatibility, or do we need a local compatibility table?
- Should the experimental inverse `masked_embedding.token_ordering` environment
  override be removed now that MLX-VLM confirms centroid-to-token ordering?
- Should the clustered embedder move into a cached backend-native path? The
  baseline implementation currently materializes the ordering on host per draft
  step for correctness/debuggability.
- Should speculative scheduling be per-request, per-model, or learned from
  recent acceptance-rate telemetry?

## Metal Performance Plan

Scope: single-stream Gemma 4 E2B/E4B QAT Q4_0 baseline (no-MTP) generation on
Apple Silicon Metal. MTP speedups are additive and never qualify the baseline
path.

> **Relocated:** The original 2026-08-26 plan (§1–8: gap decomposition against
> the v0.2.1-rc0 circus benchmark, Phase 0–4 tranches, process review, and the
> CUDA cross-reference) → [history/gemma4/metal-perf-plan-phases.md](../../../../../docs/design/inference/history/gemma4/metal-perf-plan-phases.md);
> the §9–16 implementation and qualification ledgers →
> [history/gemma4/metal-perf-plan.md](../../../../../docs/design/inference/history/gemma4/metal-perf-plan.md).

### Comparator contract

- Reference machine: M4 Pro (Mac mini, 24 GiB, 273 GB/s). The fanless base M4
  Air (120 GB/s) throttles under sustained GPU load and is only for
  interleaved A/Bs; every ledger row records machine identity, thermal state,
  and swap/page-out deltas.
- Comparator: pinned llama.cpp `llama-completion` (v10342, `38278078c`) on
  byte-identical GGUFs.
- Protocol: fresh process per sample, interleaved AB/BA pairs with a 2-second
  cooldown, greedy, F16 KV, EOS ignored. Short case is 23 prompt / 256 output
  tokens, long case is 2,334 / 128. Antfly's strict metric is
  `(generated_tokens - 1) / decode_inner_seconds`, matching llama.cpp's 255/127
  decode evaluations. The A/B harness is
  `scripts/gemma4/benchmark_metal_gemma4_ab.py`.
- Every change: exact-token SHA-256 gate against the pinned campaign hashes,
  route-counter floors proving the new path engaged, rollback env verified in
  the same campaign, and the watchdog protocol
  (`scripts/perf_watchdog_experiment.sh`, one experiment per boot, ≥300 s soak)
  for barrier/concurrency changes.
- Never use `TERMITE_METAL_TRACE_FRAME_LIFECYCLE` for absolute short-frame
  numbers, only for span structure: per-frame stderr lands on the critical
  path and distorted short-context runs badly (48.5 vs 84.6 tok/s median)
  ([history](../../../../../docs/design/inference/history/gemma4/metal-perf-plan.md)).

### Current reference numbers (M4 Pro, 2026-08)

| Model / prompt | Antfly tok/s | llama.cpp tok/s | Antfly / llama.cpp |
|---|---:|---:|---:|
| E2B short | 80.72 | 108.47 | 74.4% |
| E4B short | 56.07 | 62.98 | 89.0% |
| E2B long | 84.50 | 104.19 | 81.1% |
| E4B long | 53.63 | 61.01 | 87.9% |

Short-case numbers use the qualified per-model split-GQA floor (E2B 192, E4B
32; see Current Status); long-case numbers select split-GQA throughout and are
unchanged by it. E2B short stays slower than E2B long because lower E2B floors
changed the greedy stream.

### Decisions and findings

- Host encode is not the bottleneck: steady E2B frames spend ~0.64 ms of a
  ~12.5 ms wall in CPU encode. Optimize GPU/frame work and dispatch
  efficiency.
- The Q6_K LM-head tail stays on the NSG2 AUTO schedule; NSG4/NSG8 were
  token-exact but failed the 1% paired-throughput floor.
- LM-head Q4_K repack stays opt-in (fails the 99% top-1 agreement gate); a
  Q4_0 head is refuted outright (EOT collapse).
- `MTLCounterSamplingPointAtDispatchBoundary` is unsupported on both the M4 Air
  and the M4 Pro; per-dispatch attribution needs Instruments or forced-route
  A/Bs.
- Known traps: metadata-only barriers caused a SoC watchdog reset; one
  persistent encoder for the whole frame regressed ~6×; blind concurrent
  dispatch was net-negative on A4B. ICB replay (measured ~4× slower), ANE
  decode, and tree speculation on Metal are deliberately skipped.
- Fused epilogues must build on the production sumy-folded masked-FMA Q4_0
  body, not the scalar-unpack body (the earlier sumsq fusion regressed because
  of kernel quality, not because fusion was wrong).

### Open work

Ranked; each item needs the gates above and a rollback env.

1. **Boundary-cost calibration and byte census** (next bounded experiment):
   sweep `metal_q4_0_linear` over ops-per-frame for serial and concurrent
   encoders, capture one production E2B frame in Instruments
   (`TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS=1`), and settle
   streamed bytes/token from the GGUF tensor table. The result chooses between
   items 2 and 3.
2. **A3 Q4_0 scales-plane/interleave relayout**: bit-exact replay spike first
   (promotion floor ≥1.15× kernel-level), then production MMV/pair kernels on
   a per-slot layout tag with staging-time repack.
3. **Concurrent planned dispatch with hazard-aware reorder**
   (`TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH`): conservative
   range-tracked barriers first, then consume the DAG `wave_index` to merge
   independent spans; watchdog-class.
4. **Dispatch-count fusion** (re-priced after item 3): Q4_0 fused-QKV epilogue
   (Q8_0-only today), PLE gate+activation collapse, seed+v_norm+k-rope cluster.
5. **Retire the split-GQA floor**: a bit-identical `split_count==1` direct-write
   variant so the split family serves KV=1 upward and `paged_1x` is pure
   fallback.
6. Longer-horizon levers from the original plan that remain open (already
   shipped: Q8_0 staging of `per_layer_model_proj` with
   `TERMITE_METAL_DISABLE_PLE_MODEL_PROJ_Q8` rollback, render/tokenize once per
   request, single JSON body parse, one-write SSE frames, and split-GQA
   below-floor telemetry):
   - Make layer-spec cache misses loud: `fillLayerSpecsCached`
     (`src/architectures/gemma4_runtime.zig`) rebuilds silently on a
     fingerprint miss; add a counter to the runtime snapshot and benchmark JSON.
   - Pool per-request decode state (KV, backend, and lease setup still happen
     per request).
   - Keyless/streaming prompt-prefix caching, blocked on the prompt-cache
     attach bug in the main [Open work](#open-work).
   - Quantized KV for long context: Q8_0 KV is not implemented on Metal; the
     compressed `polar4`/`turbo3` formats exist but are not defaults (see
     [TURBOQUANT.md](../../TURBOQUANT.md)).
   - MTP on by default on Metal: auto-draft loading of the registry companion
     assistant exists behind `ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO` (default
     off); promotion waits on the acceptance-rate work in the main Open work.
   - Prompt-lookup decoding, MatFormer E2B-in-E4B self-drafting, and FFN
     activation-sparsity spikes: not implemented; each needs a quality eval
     gate.
7. Release gates still open from the last qualification: rerun from a pristine
   zero-swap boot, resolve the E2B free-form fixture mismatch (164 vs a
   180-word floor), and retain evidence outside `/private/tmp` with hosted
   M-series perf CI on `decode_tok_s`, `planned_barriers`, and route counters.
8. **Cross-session GPU mutex**: the one-GPU-job rule is still convention; the
   2026-08-28 round saw two independent agents start GPU campaigns 24 s apart,
   and the ledger asks for an enforced lock file. No script under
   `scripts/gemma4/` takes one yet
   ([history](../../../../../docs/design/inference/history/gemma4/metal-perf-plan.md)).

The CUDA SM89/E2B track is owned by
[CUDA_TUNING.md](../../CUDA_TUNING.md#gemma-4-e2b-sm89-optimization-status).
