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
> [work-log/completed/inference/gemma4-mtp-cuda-branch-status.md](../../../../../work-log/completed/inference/gemma4/mtp-cuda.md).
> Durable decisions from it are in the paragraph above and in Open work.

### CUDA Branch Status

CUDA MTP remains experimental. Its diagnostics are not a production-readiness
certification and are not a paired llama.cpp comparison; no throughput result
from that path should be described as superiority over llama.cpp. The CUDA
release contract covers target-only Gemma 4 QAT, while strict MTP certification
and promotion remain follow-up work. Generated CUDA Q4_0 kernel opt-ins and
their promotion evidence live in `QUANT_KERNEL_COMPILER.md` (Current CUDA
State); the SM89 E2B long-context comparison against llama.cpp lives in
`CUDA_TUNING.md` ("Gemma 4 E2B SM89 optimization status").

Current defaults: the SM89 GQA flash-prefill F16 composites
(`attention_prefill_flash_sm89_hd{256,512}`) are the production runtime
default — with `ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE` unset, the runtime
auto-selects `flash-f16-sm89` whenever the qualified contract holds (SM89,
page-16 paged F16 K/V, GQA 8:1, q512/q3 query-length policy, matching
sliding-window/global geometry, symbols loaded) and otherwise silently keeps
the prior unset launch topology; `ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off`
is the rollback. `q4_0_tc_hmma_prefill` (BF16 tensor-core/WMMA prefill
projections) is promoted default-on for SM89 (compute 8.9): W4A16 handles
prefill projections (rows>1) while DP4A stays for decode (rows==1), because
BF16 activations track the F32-activation reference's argmax while the DP4A
q8_1 default does not (measured 100% vs 31% greedy-token match on a chunked
prefill run) — rollback is `ANTFLY_INFERENCE_CUDA_Q4_0_TC_HMMA_PREFILL=0`.
CUDA TurboQuant KV status, measurements, and validation steps live in
`CUDA.md` under "Gemma4 And TurboQuant KV Status"; Gemma4 CUDA defaults remain
`f32` KV for exactness, `--cache-dtype polar4` is the current
production-candidate opt-in compressed-K/compressed-V path, and
`--cache-dtype turbo3` is resident and functional but still experimental.

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
> [work-log/completed/inference/gemma4-mtp-cuda-branch-status.md](../../../../../work-log/completed/inference/gemma4/mtp-cuda.md).
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
`.metal` sessions on the native Metal provider/stream path rather than
depending on MLX availability, even in builds with both backends enabled.

> **Relocated:** The dated validator-smoke bisection history that previously
> lived here (53 lines, 2026-05-07) is preserved verbatim in
> [work-log/completed/inference/gemma4-mtp-cuda-branch-status.md](../../../../../work-log/completed/inference/gemma4/mtp-cuda.md).
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

This section was `GEMMA4_PERF_PLAN.md`; its implementation ledgers (§§9–16) are preserved in [work-log/completed/inference/gemma4/metal-perf-plan.md](../../../../../work-log/completed/inference/gemma4/metal-perf-plan.md).

Originally written 2026-08-26 against v0.2.1-rc0 circus benchmark (`https://circus.antfly.io/v0.2.1-rc0/#inference-generation`). Scope: single-stream Gemma 4 E4B/E2B QAT Q4_0 generation on Apple Silicon (Metal). The initial analysis was plan-only; §§9–16 record the subsequent implementation and qualification ledgers.

---

### 1. Where we are (measured + modeled)

Circus, E4B Q4_0, single prompt, 64 tokens, temp 0, serial:

| Engine | tok/s (e2e) | ms/tok | Effective GB/s* | % of M4 Pro BW (273 GB/s) |
|---|---|---|---|---|
| **Antfly** (internal decode) | **62.4** | 16.03 | 176 | 64.7% |
| **Antfly** (end-to-end) | **54.9** | — | — | — |
| llama.cpp Q4_0 | 72.2 | 13.85 | 204 | 74.8% |
| Ollama Q4_0 | 75.3 | — | — | — |
| vLLM-Metal (MLX 4-bit) | 86.6 | 11.55 | 227 (on ~7.5% fewer bytes) | 83.0% |
| **Roofline ceiling** (2.829 GB/token) | **~96** | 10.37 | 273 | 100% |

\* Effective GB/s = tok/s × bytes/token. Bytes/token from the actual GGUF tensor table: FFN Q4_0 1,858 MB (65.7%), **LM head Q6_K 550 MB (19.5%)**, attention Q4_0 330 MB (11.7%), PLE 86 MB (incl. a full **F16 55 MB `per_layer_model_proj` matvec every token**), norms/KV ~7 MB. E4B: 42 layers, only 24 own KV (18 shared-KV), 5:1 iSWA, head_dim 512 global / 256 SWA.

**Hardware caveat (important):** the published numbers are only physically possible on an **M4 Pro (273 GB/s)**. This machine is a **fanless base-M4 Air (120 GB/s, 16 GB)** — ceiling here is ~42 tok/s and it throttles in ~10 min of sustained GPU load. All A/Bs on this machine must be interleaved; all ledger entries must record machine identity (today they don't — see §6).

**Gap decomposition** (Antfly 16.03 ms/tok vs 10.37 ms floor ⇒ 5.66 ms excess):

| Bucket | Est. excess | Upside | Evidence |
|---|---|---|---|
| (a) Big-matvec efficiency, esp. **Q6_K LM head has no tuned decode kernel** (small-rows Q6_K r2-reduce exists but is an MTP-verify opt-in) | 2.5–3.5 ms | **+7–12 tok/s** | Blended 176 vs llama.cpp 204 GB/s; Q4_0 MMV auto-tuning engages only for two exact FFN shapes on M4 (`metal_kernels.m:11161-11170`) |
| (b) ~330 small elementwise dispatches + ~422 range-driven barriers per frame (norms/rope/residual/PLE) | 0.7–1.5 ms | +3–6 tok/s | METAL.md Q8_0 census (41 encoders, 422 barriers, planned_scopes=36); fusion levers exist but are opt-in |
| (c) Per-step submit→**wait**→encode bubble; pipelined decode frame appears opt-in in production (`TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME`, generation.zig:1633-1642 — **verify**, one analysis pass read it as default-on) | 0.3–0.6 ms | +1–2.5 tok/s | llama.cpp never waits on GPU except logits readback; commits first ~64 nodes early |
| (d) Sampling/logits | ~0 | — | Resident-logits Gumbel-max + in-frame argmax already merged and active |
| (e) Attention/KV reads at 64-token ctx | ~0 (grows with ctx) | long-ctx only | iSWA split ring default-on but disabled under prompt-cache/compaction; decode attention is non-flash 3-pass kv_1x |
| (f) e2e 54.9 vs internal 62.4 | ~156 ms fixed/request | +13% e2e at len 64 | Double Jinja+tokenize per request, no prefix cache (keyless/streaming excluded), per-request KV/backend/lease setup, double JSON parse, 3 syscalls/token SSE |

**Bytes gap to MLX**: matching MLX's 83% efficiency on our GGUF bytes gives ~80 tok/s; the last ~6 tok/s needs byte reduction — MLX 4-bit block weights are the same 4.5 bpw as Q4_0, its real win is the **tied embedding/LM head at 4.5 bpw vs our Q6_K 6.56 bpw (−172 MB/token)** and a quantized model-proj.

**The strategic fact**: llama.cpp/MLX parity is worth +10–24 tok/s. The **Gemma 4 official MTP drafter** (4-layer d=256 head that cross-attends the *main model's KV*) is worth **2–3×** (vLLM CUDA: 40.9→108.8 tok/s; llama.cpp E2B-drafting: 3.2×; mlx-serve E4B: 1.5×). Our MTP machinery exists but is default-off on Metal, no assistant is shipped in the registry — yet **the E4B MTP assistant is already downloaded on this machine** (`~/.antfly/inference/models/google/gemma-4-E4B-it-qat-q4_0-unquantized-assistant/`, 183 MB safetensors). Both tracks matter: kernel parity multiplies under speculation (verify cost is kernel-bound).

---

### 2. Phase 0 — Attribution & measurement hygiene (1–2 days, no code changes)

Run before any optimization; each experiment decisively splits a gap bucket.

1. **Stage timing**: `TERMITE_METAL_STAGE_TIMING=1` on E4B Q4_0 64- and 512-token runs; compare each decode bucket (attention/ffn/ple/tail/embedding) against its byte floor from §1. This alone confirms or kills the Q6_K-tail hypothesis. (Parsed by `scripts/gemma4/benchmark_metal_gemma4_ab.py`.)
2. **GPU-busy vs wall**: `whole_frame_gpu_nanos` vs per-step wall time → sizes bucket (c) exactly.
3. **Resolve the pipelined-frame discrepancy**: read `generation.zig:1633-1642` + `metal_kernels.m:48468` and A/B `TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME=1` (negative control: `TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME=1`).
4. **Q6_K vocab-matvec microbench** at [2560×262144] via the quant-kernel bench harness → achievable tail GB/s before writing any kernel.
5. **Kernel-route audit**: `TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT`, `..._DECODE_GQA_SPLIT_SCHEDULE` — confirm the tuned portfolio actually engages on all 210 MMVs/frame and that split-GQA decode attention runs (it silently falls back).
6. **Instruments capture** of one decode frame (`TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS=1`): per-dispatch achieved GB/s + inter-dispatch bubbles; produce the missing **Q4_0-E4B encoder/barrier census** (only a Q8_0 anchor exists in METAL.md).
7. **Baseline integrity check on the competition**: confirm the llama.cpp reference build executes the full Gemma-4 PLE pipeline (open issue #22243 claims some builds skip it — a build doing less work/token flatters its tok/s), and whether the Ollama number came from its new MLX engine (≥0.30) vs the GGML engine. Confirm whether vLLM-Metal's 86.6 already includes its Gemma-4 MTP proposer.
8. **Ledger fix**: record machine identity, thermal state, and `raw_decode_tok_per_s` in every benchmark row; rerun the pinned comparison on the M4 Pro box, using this Air only for interleaved A/Bs.

Exit criteria: a table attributing the 5.66 ms/token excess to buckets (a)–(e) with ±10% confidence.

### 3. Phase 1 — Kernel/runtime parity with llama.cpp (target: 62 → 72–76 decode tok/s)

Ordered by expected payoff; every item validated bit-identical (or logit-tolerance) + interleaved A/B.

1. **Tuned Q6_K LM-head decode kernel** (biggest single item, est. +4–8 tok/s). 550 MB/token, 19.5% of traffic, currently un-tuned. Apply the same simdgroup row-portfolio treatment the Q4_0 MMV got; reuse the existing sweep/codegen infra to find NR0/NSG for [2560×262144]; promote as a handwritten-production route. Also evaluate llama.cpp's Q6_K mask-unpack scheme (constant-mask 6-bit scale unpack, no shifts in the hot loop).
2. **Async, never-wait decode loop** (est. +1.5–3 tok/s). Adopt llama.cpp's contract: the only GPU sync is the token-id readback, and even that is removable via the existing device token handoff (`decoder_gated_runtime.zig:5573`). Make the pipelined decode frame default-on for M4-family (after Phase 0 confirms its status), keep `DISABLE` rollback. Encode token N+1 while N executes; llama.cpp additionally commits the first ~64 encoded nodes early so the GPU starts before encoding finishes — same idea applies to our planned frame (split the frame into 2 command buffers: layers 0–k committed immediately).
3. **Elementwise fusion + barrier reduction** (est. +2–4 tok/s). Qualify and default-on the already-written opt-in fusions: `Q4_0_LINEAR_RMS_ADD_SUMSQ` (matvec+RMS+residual), pair-activation gate/up fusion, `SMALL_ROWS_NORM_REDUCE`. Then add llama.cpp-style **hazard-aware reorder**: our concurrent-dispatch experiment failed as *blind* concurrency; llama.cpp makes it work by reordering nodes (64-node look-ahead over a reorder-safe whitelist) so Q/K/V projections and independent norms share one barrier-free concurrent span. Target: 422 → <150 barriers/frame. **Known traps (do not repeat):** metadata-only barriers ⇒ SoC watchdog reset; one persistent encoder for the whole frame ⇒ 6× regression (both documented in METAL.md).
4. **Broaden MMV auto-qualification** (est. +1–2 tok/s). Auto currently deviates from legacy only on M4 + two exact FFN shapes; attention/PLE/tail/down matvecs run legacy shapes. Ship per-device tuned dispatch tables generated by the existing offline `--sweep` (llama.cpp now ships exactly this: generated `ggml-metal-tuning` tables keyed by device/dtype/shape bucket). This is also the vehicle to finally merge value from the unmerged `codex/quant-kernel-runtime-jit` branch: keep the *offline sweep → checked-in table* part, drop runtime JIT.
5. **PLE micro-items**: quantize `per_layer_model_proj` F16 → Q8_0 (−27 MB/token, ~-0.1 ms), PLE row-stride hoist, fold PLE gate/act/proj into fewer dispatches (already flagged in METAL.md as the next collapse target).

### 4. Phase 2 — Beat llama.cpp, chase MLX (target: 76 → 84–90 decode tok/s)

1. **Byte reduction on the tail**: repack/tie the LM head to a 4-bit-class format (MLX-style affine group-64 with the qdot mask/FMA dequant, or Q4_K with QAT-aware requant of the head only; validate perplexity on the QAT checkpoint). −172 MB/token ≈ +5–7 tok/s. This is where MLX's remaining lead lives; block-weight bpw is otherwise identical.
2. **Flash-decoding attention for long context**: replace/augment kv_1x with a KV-split vec kernel (llama.cpp: 32 workgroups split the KV, each emits partial O + (S,M) stats, tiny merge-reduce kernel; 32-wide masked-chunk skip makes iSWA masks nearly free). Irrelevant at 64-token benchmarks, decisive at 4–32k. The existing opt-in split-GQA route is the starting point — qualify it default-on with the scan-clamp.
3. **Quantized KV cache (Q8_0 first)**: a *speed* feature once KV-bound (int4/8 KV outruns F16 KV on Apple Silicon in multiple 2025-26 reports). Requires dequant-in-register in the attention kernel; keep F16 as default until long-ctx evals pass.
4. **Graph/plan reuse with input re-binding**: llama.cpp's `can_reuse` path collapses per-token host work to re-binding; our `fillLayerSpecsCached` fingerprint cache is close — add telemetry for silent cache-miss rebuild-per-token and make misses loud.
5. **E2B pass**: repeat Phase 0 attribution on E2B (30-layer class, M4 Pro reference ~80 greedy / ~52 sampled); E2B is the latency flagship on 16 GB machines and everything above applies at smaller shapes. Add E2B to circus.

### 5. Phase 3 — Leapfrog: speculation + serving path (target: e2e ≥ decode, and 1.5–2.5× effective tok/s)

**MTP self-speculation (the headline lever).**
1. Add the Gemma-4 MTP assistant to the registry pull set (`gemma4-e4b` should fetch the assistant alongside the GGUF; it's 183 MB and already on this machine) and wire standalone-server speculation (today the standalone runtime has zero spec plumbing; the inference server needs an explicit per-request draft).
2. Enable `ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO` + prefill hidden capture by default for E-series on Metal once qualified; drop `AUTO_MIN_TOKENS=128` so short generations benefit (the circus benchmark generates 64 tokens — auto-MTP would never fire today).
3. **Sequential chain, γ=3–4, no tree verification** — EAGLE-style trees measured ~1.05× on Apple Silicon (batch-1 verify doesn't amortize; tree attention needs KV support we don't have). Qualify the existing opt-in accelerators: verify-tail frame, defer-materialize, donated-slot attention, accept-bonus.
4. Fix the interaction: speculation currently disables prompt-prefix reuse; both must compose.
5. **Prompt-lookup decoding (PLD)** as a free, model-agnostic second layer with acceptance-rate gating (2×+ on echo/RAG/agentic workloads; mlx-serve ships this as default).
6. Expected: E4B effective decode 76 → **~120–160 tok/s** on natural text at ~70–85% acceptance; report acceptance + effective tok/s in circus.

**Serving path (close the 12% e2e gap; independent, can start immediately).**
7. Tokenize + render the chat template **once** per request (reuse the admission-estimate result in the pipeline; today both run twice).
8. Single JSON parse of the request body.
9. Fix and enable prompt-prefix caching on Metal for keyless + streaming requests (currently requires explicit key AND non-streaming; attach path has a known KV-degradation/hang bug on Metal — root-cause `attachSequenceWithRetainedBlocks`).
10. Pool per-request state: reuse `ComputeBackend`/KV pool/decode-state across serial requests on the same model instead of rebuilding all of it per request.
11. SSE emission: buffer/coalesce (1 writev per token, not 3 syscalls; pre-sized JSON serializer, no per-token alloc/free); move emission off the decode-loop critical path.
12. Prefill: revisit the SG flash prefill with the Phase-1 barrier/reorder machinery in place (its loss to kv_1x predates that); adopt llama.cpp's bulk KV-dequant-to-F16-scratch for the prefill regime if quantized KV lands.

### 6. Phase 4 — Novel / research track (time-boxed spikes)

- **MatFormer E2B-inside-E4B self-drafting**: E2B is a nested submodel of E4B — a *free* draft sharing weights and (partially) KV. Nobody ships this; spike after MTP lands as a comparison arm (MTP likely wins on acceptance-per-drafted-FLOP, but MatFormer needs no extra artifact).
- **Activation sparsity**: Gemma E-series has trained-in FFN top-k sparsity (3n lineage); no production runtime exploits it. FFN is 65.7% of our bytes — even 25% effective skip ≈ +10 tok/s. High risk, high novelty; gate on a quality eval.
- **Metal 4 tensor ops / cooperative tensors** (macOS 26+): hardware 4/8-bit dtypes with block-scale planes make MXFP4-class dequant a tensor-unit feature; target prefill and W4A8 first. Track **M5 NAX** (vLLM-Metal already uses it for prefill attention) for the next hardware cycle.
- **Deliberately skip**: ICB replay (measured 4× slower here; industry agrees), ANE decode (fixed shapes, ~9 tok/s at 8B-class), tree speculation on Metal.

### 7. Gaps & process improvements (found during this review)

1. **Perf features die in opt-in purgatory.** 729 `TERMITE_METAL_*` flags; sweep-tuned kernels ship in the binary but never promote; `kernel_jit` defaults off; runtime-JIT work stranded on unmerged `codex/quant-kernel-runtime-jit`. → Define a promotion pipeline: candidate → shadow (dispatch-count parity) → qualified-per-device default-on with `DISABLE` rollback; review the flag inventory quarterly and delete dead gates.
2. **No perf CI.** Nothing guards decode tok/s, encoder/barrier counts, or route selection on merge. → Add a nightly M-series job running `bench_metal_gemma4_e2b.sh`/`compare_metal_gemma4_e4b_qat.sh` with regression thresholds on `decode_tok_s`, `hot_decode_tok_s`, `planned_barriers`, and quant-route counters.
3. **Benchmark/mode mismatch.** In-repo harnesses are non-streaming CLI; circus measures the streaming server — the per-token SSE tax is invisible to local benchmarking. → Add a streaming-server mode to the AB harness.
4. **Machine identity absent from the ledger** (M4 Air vs M4 Pro is a 2.3× roofline difference; the fanless-Air thermal trap is documented but not enforced). → Ledger schema: chip, BW, power state, interleaving.
5. **Known live bugs on the critical path**: prompt-cache prefix attach degrades KV / hangs on Metal; kv_compacted handling; silent fallbacks (split-GQA, layer-spec cache misses) with no counters surfacing them. → Make every silent fallback increment a logged counter; alert in the AB harness.
6. **Docs drift**: METAL.md census is Q8_0-only; the Gemma 4 A4B perf ledger is A4B-only. → Land the Phase-0 Q4_0-E4B census in METAL.md.

### 8. Sequencing & success criteria

```
Week 1      Phase 0 attribution + ledger/CI fixes (§7.2, §7.4)
Weeks 2–4   Phase 1 (Q6_K tail kernel → async loop → fusion/reorder → tuned tables)
            Serving-path items §5.7–5.11 in parallel (independent code)
Weeks 5–7   Phase 2 (head repack, flash-decode attention, quantized KV, graph reuse, E2B pass)
Weeks 6–9   Phase 3 MTP (registry + standalone wiring → Metal auto-on → PLD)
Ongoing     Phase 4 spikes, one at a time, time-boxed to 1 week each
```

Success criteria on the M4 Pro reference box, E4B QAT Q4_0, streaming server, 64- and 512-token runs:
- **P1 exit**: internal decode ≥ 72 tok/s (llama.cpp parity), e2e/decode ratio ≥ 0.95.
- **P2 exit**: internal decode ≥ 84 tok/s; long-context (8k) decode within 10% of short-context after iSWA/flash-decode.
- **P3 exit**: effective e2e ≥ 110 tok/s on natural-text prompts with MTP auto-on; no quality regression on the eval suite; acceptance rate reported in circus.
- Every change: bit-identical or logit-tolerance validated, interleaved A/B, rollback flag, ledger entry with machine identity.

### CUDA SM89/E2B track (parallel, cross-reference)

This plan's phases above are Metal-focused; the CUDA SM89 E2B path is tracked
separately in `CUDA_TUNING.md` ("Gemma 4 E2B SM89 optimization status").
An Nsight Systems request-window trace on the locked Gemma 4 E2B SM89 workload
attributes most GPU busy time to Flash attention with GPU utilization near
100%, leaving only a small device-idle upper bound — so CUDA Graph capture
helps CPU concurrency but is not the primary TTFT lever. The next production
priorities there are: an upload-packed SM89 W4A16 Tensor Core projection
engine with a documented numerical contract, model-shape cuBLASLt tuning with
admitted persistent workspace, fused gate/up activation output, and a
genuinely concurrent GQA Flash redesign that reuses K/V without the current
design's register/tail costs. Each remains independently gated; projected
savings overlap and must not be added together without end-to-end
device-event evidence.

---

> **Relocated:** The implementation and qualification ledgers (§9–16, 371
> lines, 2026-08-26 through 2026-08-28) that previously lived here are
> preserved verbatim in
> [work-log/completed/inference/gemma4-perf-plan-ledgers.md](../../../../../work-log/completed/inference/gemma4/metal-perf-plan.md).
> Durable decisions from them — pipelined decode frame default-on,
> pair-activation fusion default-on for M4, the LM-head Q4_K repack opt-in and
> its Q4_0-head EOT-collapse caveat, the per-model short-KV split-GQA floor
> policy, and the `ANTFLY_GEMMA4_MTP_AUTO_DRAFT_DISCOVERY` default — are in
> [`GEMMA4.md`](GEMMA4.md) (Current Status).
