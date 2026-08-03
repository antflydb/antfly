# Inference Chat Command (`antfly inference chat`)

**Date**: 2026-07-31
**Branch**: `inference_chat` (feature + `quant-kernel-long-context` merge + fixes)
**Hardware validated**: M4 Pro 24GB, Metal backend, gemma4 E2B QAT q4_0

## What

An ollama-style interactive path for generative models in the Zig inference
CLI: `antfly inference chat <model>` resolves a friendly name, pulls the
model from HuggingFace when missing, loads it, and runs a multi-turn REPL
with streamed output, per-turn stats, slash commands (`/set`, `/show`,
`/clear`, `/help`), triple-quote multi-line input, and Ctrl-C stop-response.

```
$ antfly inference chat gemma4-e2b
>>> whats the capital of oregon?
The capital of Oregon is **Salem**.
(88 tok · 51.6 tok/s · 42 prompt / 0 cached · stop)
```

## Design decisions

- **New `chat` subcommand** rather than overloading `run` (which starts the
  server). Implemented as `native_chat.zig` following the `native_<cmd>.zig`
  convention; wired into both dispatchers (`pkg/inference/src/main.zig` and
  `pkg/antfly/src/inference_runtime/runtime.zig`).
- **Friendly aliases live in `registry.zig`** (`resolveFriendlyRef`):
  `gemma4-e2b` / `gemma4-e4b` (+ spelling variants) resolve to Google's
  official QAT conversions `google/gemma-4-*-it-qat-q4_0-gguf` — the
  checkpoints production workflows run on. Unknown names pass through as
  `owner/name[:variant]` refs; paths and installed dirs short-circuit the
  pull. Auto-pull reuses `ModelRegistry.pull` (resume + SHA256 + progress).
- **Plain-stdin REPL** (no raw mode): `readerStreaming` (positional
  `File.reader` reports EOF on pipes), `std.debug.print` output convention,
  ANSI dim only, SIGINT handler that just sets an atomic flag polled by the
  token callback (second press exits).
- **Context management**: history renders through the model chat template
  and trims oldest user/assistant pairs against
  `min(model context, --max-context) - max_tokens` before each turn; the
  merge replaced the old silent 2048-token tokenizer truncation with
  `nativeGenerationPromptTokenLimit` + fail-closed encoding.
- **Speculative/MTP off in chat**: speculation disables prompt-prefix reuse
  (`!use_speculative` eligibility gate) and measured acceptance was poor;
  `generate --draft-model` remains the speculative path.
- **`--server` mode** reuses `native_generate`'s SSE client (made `pub`,
  plus an optional capture buffer for multi-turn history).

## Bugs found and fixed along the way

1. **gemma4 GGUF conversions produced empty text** (pre-existing): the
   checkpoints close the prompt-opened thought channel with a bare
   `<channel|>` and never emit the `<|channel>final\n<channel|>` header the
   projection required, so every generated token was stripped.
   `bareChannelCloseRange` (pipelines/generation.zig) accepts the bare-close
   transition only when no explicit header exists anywhere in the stream;
   explicit non-final channels stay private. See GEMMA4.md "Channel
   transition conventions".
2. **Metal 4-bit GGUF segfault** (pre-existing): fixed by the
   `quant-kernel-long-context` generated Metal quant kernels; verified on the
   previous crasher.
3. **Sampled decoding 15x slower than greedy** (5 vs 80 tok/s; surfaced by
   chat defaulting to temperature 0.7 while `generate` defaults to greedy):
   - `topK`/`topP` were O(k·V) full-vocab rescans → single-pass min-heap /
     candidate gather (backends/activations.zig).
   - Non-greedy decoding fell off the fused metal decoder frame onto the
     per-op eager path → now routed through the backend-owned sampled frame
     (`decoder_gated_runtime.forwardSampledToken`: device-resident
     Gumbel/top-k sampling, prepared sampled tail), with
     `forwardLastLogits` + host sampling as fallback.
   - Result: sampled 5.1 → 52.2 tok/s; greedy unchanged at 80.6.

## Known issues / follow-ups

- **Prompt-prefix cache attach is broken** — chat's multi-turn KV reuse is
  opt-in (`--prompt-cache`) until fixed: attaching a cached prefix degrades
  the attached tokens' KV on metal (temp-0 A/B loses early-prompt context)
  and can hang the native backend. Suspected gap: block-hash entries carry
  per-block `storage_block_id: ?KvBlockId` but only the simple-mode attach
  re-attaches retained storage blocks (`runtime/kv/prompt_cache.zig`).
  Fixing this restores the multi-turn TTFT win (turn N+1 prefills only the
  previous reply + new message).
- **Sampled-vs-greedy gap** (52 vs 80 tok/s) is the device sampler's real
  per-token work; further kernel fusion is optional headroom.
- **Two metal tests fail on M4 Pro** (`metal_runtime` paged-attention 1x
  kernel expectation, `quant_kernel_compiler` small-row split GQA gate) on
  the pure `quant-kernel-long-context` head as well — GPU-family-dependent
  dispatch expectations, not merge artifacts.
- The E2B checkpoint often stays thought-channel-only at temperature >= 0.7;
  chat prints an explicit notice instead of a blank reply, and temp 0-0.3
  transitions reliably. Streaming for bare-close checkpoints surfaces text
  at end of turn (header absence is only provable at turn end).

## Testing

- Unit: alias resolution, REPL command parsing, `/set` validation, history
  trim/clear, channel projection (both conventions + fail-closed edges),
  topK/topP equivalence vs scalar fallbacks.
- `scripts/test_chat_repl_smoke.sh`: scripted two-turn chat asserting real
  answers (Paris/Seine) and per-turn footers; `--prompt-cache` leg is
  env-gated until the attach fix lands.
