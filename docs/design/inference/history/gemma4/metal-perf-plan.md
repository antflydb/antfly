# Gemma 4 Metal Performance Plan Implementation Ledgers

> Summary of a verbatim log. Full text: [docs/design/inference/history/gemma4/metal-perf-plan.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/gemma4/metal-perf-plan.md). Originally relocated verbatim from `zig/pkg/inference/GEMMA4_PERF_PLAN.md` (lines 128–498 at commit 271838a195) on 2026-09-16; the living design is [`GEMMA4.md` (Metal Performance Plan)](../../../../../zig/pkg/inference/models/gemma4/GEMMA4.md#metal-performance-plan).

Implementation ledgers §9–16 of the Gemma 4 E2B/E4B Metal decode performance plan, 2026-08-26 to 2026-08-28: rounds 1–2 on a base M4 Air 16GB (~120 GB/s, not qualification-grade), then production-readiness, release-blocker, attribution, and short-KV split-floor rounds on the M4 Pro reference Mac mini (`Mac16,11`, 24 GiB). Outcome: PR-candidate code qualification PASS, full release promotion BLOCKED; short-context decode rose to 74.42% (E2B) / 89.03% (E4B) of llama.cpp v10342.

## Decisions
- **Pipelined decode frame default-on for M4-qualified devices** (`termite_metal_pipelined_decode_frame_device_default()`, `DISABLE` flag wins) — Air A/B E2B +9–12% (≈46 → ≈51.5 tok/s), E4B +5–8%, bit-identical tokens. (§9)
- **Serving path**: chat template rendered and prompt tokenized once per request; single JSON body parse; SSE chunk = one `sendAll`. Batch endpoint double-parse deliberately skipped (off hot path). (§9)
- **Flash-decoding split-GQA composes with pipelined frames** via a 2-buffer scratch pool alternated at frame submit — long-context (≈2k) E2B ~2×, E4B +41%, bit-identical. (§10)
- **Hazard scan skipped when barriers are suppressed** — tracker CPU 0.6→0.1 ms/frame. The live Q4_0 E-series decode frame already submits with planned_barriers=0 (whole-frame scoped suppression on a serial encoder; 1 compute encoder, 143 planned scopes); the 422-barrier census was the Q8_0 anchor. Barrier-elision experiments stay off the fanless laptop (METAL.md records a delayed SoC-watchdog hard reset). (§10)
- **Pair-activation fusion promoted default-on for M4** (`DISABLE_Q4_0_PAIR_ACTIVATION_FUSION` rollback) — bit-identical, −84 dispatches/frame; later measured +2.77% on the M4 Pro (correcting the Air's 0.5–1%). (§12, §15.5)
- **PLE model-proj Q8_0 staging default-on** (`TERMITE_METAL_DISABLE_PLE_MODEL_PROJ_Q8` rollback) — tokens identical; +1.10% E2B on M4 Pro. (§12, §15.2)
- **LM-head Q4_K repack stays opt-in** (`TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK=q4_k`) despite ~3–5% decode benefit — failed the reviewed top-1 ≥99% live-logit gate (below). A default needs a reviewed threshold change justified by external semantic/perplexity evaluation. (§14.2)
- **Q6_K tail: retain NSG2 AUTO**; NSG4/NSG8 failed the 1% floor (stable null, CV <0.2%). (§14.3)
- **MTP (Gemma4 BF16 assistants)**: correctness/fallback PASS, Metal-auto performance FAIL. Discovery (`ANTFLY_GEMMA4_MTP_AUTO_DRAFT_DISCOVERY`) and Metal-auto stay separate default-off gates; forced MTP remains for diagnostics. Server fix: `speculation_requested` (derived from the effective drafter) excludes whole-model compiled auto-promotion, since MTP needs the target's final hidden rows. (§9, §14.1)
- **Split-GQA min-KV floor made runtime-tunable** (`TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV`, read once at runtime creation; below-floor misses exposed in telemetry; `TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT` full rollback). Qualified defaults: E2B **192**, E4B **32**, A4B **32** (`TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT`). E2B floors ≤160 change the greedy stream at generated token 169, so E2B ships at the fastest exact floor. The C runtime now follows the same M4 pipelined-frame default as Zig. (§16)
- **Next target after §16**: §15 P1 — Instruments boundary-cost calibration plus a model-byte census, to choose between the A3 Q4_0 scales-plane relayout replay spike and watchdog-gated concurrent dispatch/reorder. (§16.5)

## Rejected approaches
- **Q6_K rows==1 MMV nsg4/nsg8 portfolio** — no repeatable Air win (legacy won 3/3; earlier +7–9% was warmup noise); AUTO stays legacy. (§9)
- **Q4_0 LM head** — instant-EOT quality collapse; Q4_K with the legacy kernel was slower than Q6_K despite −31% bytes. (§10)
- **Sumsq fusion** — −12% repeatable on E2B (47.7 vs 54.2); §15 attributes it to kernel quality (scalar nibble unpack, no sumy folding), not fusion itself, so new epilogues must build on the sumy-folded masked-FMA body. (§12, §15.3 P4)
- **Host-encode bottleneck hypothesis** — refuted: steady E2B 12.474 ms wall, 11.840 ms pipelined wait, only 0.640 ms CPU encode. (§13.2)
- **Q6_K tail-inefficiency hypothesis** — refuted at kernel level (98.7 GB/s, 82% of Air peak); the repack win was bytes. (§12)
- **Speculative submit for a 0.29/0.34 ms EOS bubble (§15 P0b)** — refuted in §16: the successor frame is already submitted; no code retained.
- **"E4B pair-activation never engages" (§15 P1b)** — refuted: the analyzer hard-coded E2B's `nr4-nsg4` counter; E4B reports 10,752 `nr4-nsg2` pair dispatches per 256-token sample.
- **32/64-token split floor for E2B** — not token-identical (see Decisions). (§16.1)
- **§12.3 attention-shape mystery (28.4 vs 69.9 GB/s)** — an isolation artifact of the solo microbench; measure in-frame rates only with `--ops-per-frame ≥64` or route-algebra A/Bs. (§15.2)

## Key measurements

Antfly vs llama.cpp v10342 (`38278078c`), M4 Pro, strict `(generated_tokens - 1) / decode_inner_seconds`, greedy, F16 KV, EOS ignored, fresh-process interleaved:

| Model / prompt | §14.4 Antfly | §14.4 llama.cpp | Ratio | §16.3 qualified floor | §16.3 llama.cpp | Ratio |
|---|---:|---:|---:|---:|---:|---:|
| E2B short (23 + 256) | 71.609 | 108.670 | 65.90% | **80.722** | 108.470 | **74.42%** |
| E4B short (23 + 256) | 47.407 | 62.950 | 75.31% | **56.069** | 62.980 | **89.03%** |
| E2B long (2,334 + 128) | 84.498 | 104.190 | 81.10% | (unchanged) | | |
| E4B long (2,334 + 128) | 53.632 | 61.010 | 87.91% | (unchanged) | | |

- **Short-context anomaly root cause (§15.1)**: the compile-time `TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV_TOKENS 512u` sent all sub-512 KV decode through `termite_paged_attention_kv_1x` (8/16 threadgroups on a 20-core GPU), costing 0.695 µs per KV-token per dispatch; split-GQA is 30–100× cheaper per KV-token. 512 was a scope gate in `f3f930ff27`, never a measured crossover. Split force-disabled at long context: 40.386 / 48.276 ms/tok.
- **Floor sweep (§16.2)**, E2B / E4B tok/s: 32 → 88.766 / 56.350; 192 → 80.732 / 52.352; 512 → 71.729 / 47.434. Gain vs 512: +12.84% E2B, +18.56% E4B, 6/6 pairs.
- **LM-head Q4_K repack live-logit gate (§14.2)**: E2B top-1 144/146 = 98.630% (PPL ratio 1.006927); E4B 142/146 = 97.260% (PPL ratio 0.992010); both FAIL `< 99%`. v3 rerun (2026-08-28) still 142/146 on E4B.
- **MTP pilot (§14.1, directional)**: E2B target-only 75.919 tok/s vs BF16 MTP auto K=2 63.682 (64% acceptance, self-disabled after 16-round probe); E4B 54.983 vs forced K=2 30.118. Q4 and Q8 assistants also slower.
- **llama.cpp ground truth (§15.2)**: E2B 819 nodes/token, ~686 memory barriers per token at 9.2 ms/tok, ~56% (E2B) / ~65% (E4B) of 273 GB/s — barrier minimization is not where its speed comes from.
- Bulk weight streaming priced at ~323 GB/s from the PLE Q8 marginal rate (§15.2).

## Process rules recorded
- `MTLCounterSamplingPointAtDispatchBoundary` is unsupported on both the M4 Air and the M4 Pro reference box; per-dispatch attribution needs Instruments or route-algebra A/Bs. (§15.5)
- `TERMITE_METAL_TRACE_FRAME_LIFECYCLE` badly distorts short-context runs (48.5 vs 84.6 tok/s); use it only for span structure. (§15.5)
- One GPU job at a time needs an enforced lock file; archive release-candidate binaries with evidence. (§15.5)

## Open release gates (as of §16)
- Re-run from a pristine zero-swap boot (the machine started with existing swap).
- Exact-v1 E2B free-form fixture mismatch: 164 observed words vs the reviewed 180-word floor; not to be waived implicitly. (§13.4)
- Retain raw evidence outside `/private/tmp` and reproduce gates in hosted M-series CI.
