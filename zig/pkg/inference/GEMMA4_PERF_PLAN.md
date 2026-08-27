# Gemma 4 E2B/E4B Metal Performance Plan — Closing the Gap with llama.cpp and vLLM-Metal

Date: 2026-08-26 · Baseline: v0.2.1-rc0 circus benchmark (`https://circus.antfly.io/v0.2.1-rc0/#inference-generation`)
Scope: single-stream Gemma 4 E4B/E2B QAT Q4_0 generation on Apple Silicon (Metal). No commits from this analysis; plan only.

---

## 1. Where we are (measured + modeled)

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

## 2. Phase 0 — Attribution & measurement hygiene (1–2 days, no code changes)

Run before any optimization; each experiment decisively splits a gap bucket.

1. **Stage timing**: `TERMITE_METAL_STAGE_TIMING=1` on E4B Q4_0 64- and 512-token runs; compare each decode bucket (attention/ffn/ple/tail/embedding) against its byte floor from §1. This alone confirms or kills the Q6_K-tail hypothesis. (Parsed by `scripts/benchmark_metal_gemma4_ab.py`.)
2. **GPU-busy vs wall**: `whole_frame_gpu_nanos` vs per-step wall time → sizes bucket (c) exactly.
3. **Resolve the pipelined-frame discrepancy**: read `generation.zig:1633-1642` + `metal_kernels.m:48468` and A/B `TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME=1` (negative control: `TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME=1`).
4. **Q6_K vocab-matvec microbench** at [2560×262144] via the quant-kernel bench harness → achievable tail GB/s before writing any kernel.
5. **Kernel-route audit**: `TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT`, `..._DECODE_GQA_SPLIT_SCHEDULE` — confirm the tuned portfolio actually engages on all 210 MMVs/frame and that split-GQA decode attention runs (it silently falls back).
6. **Instruments capture** of one decode frame (`TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS=1`): per-dispatch achieved GB/s + inter-dispatch bubbles; produce the missing **Q4_0-E4B encoder/barrier census** (only a Q8_0 anchor exists in METAL.md).
7. **Baseline integrity check on the competition**: confirm the llama.cpp reference build executes the full Gemma-4 PLE pipeline (open issue #22243 claims some builds skip it — a build doing less work/token flatters its tok/s), and whether the Ollama number came from its new MLX engine (≥0.30) vs the GGML engine. Confirm whether vLLM-Metal's 86.6 already includes its Gemma-4 MTP proposer.
8. **Ledger fix**: record machine identity, thermal state, and `raw_decode_tok_per_s` in every benchmark row; rerun the pinned comparison on the M4 Pro box, using this Air only for interleaved A/Bs.

Exit criteria: a table attributing the 5.66 ms/token excess to buckets (a)–(e) with ±10% confidence.

## 3. Phase 1 — Kernel/runtime parity with llama.cpp (target: 62 → 72–76 decode tok/s)

Ordered by expected payoff; every item validated bit-identical (or logit-tolerance) + interleaved A/B.

1. **Tuned Q6_K LM-head decode kernel** (biggest single item, est. +4–8 tok/s). 550 MB/token, 19.5% of traffic, currently un-tuned. Apply the same simdgroup row-portfolio treatment the Q4_0 MMV got; reuse the existing sweep/codegen infra to find NR0/NSG for [2560×262144]; promote as a handwritten-production route. Also evaluate llama.cpp's Q6_K mask-unpack scheme (constant-mask 6-bit scale unpack, no shifts in the hot loop).
2. **Async, never-wait decode loop** (est. +1.5–3 tok/s). Adopt llama.cpp's contract: the only GPU sync is the token-id readback, and even that is removable via the existing device token handoff (`decoder_gated_runtime.zig:5573`). Make the pipelined decode frame default-on for M4-family (after Phase 0 confirms its status), keep `DISABLE` rollback. Encode token N+1 while N executes; llama.cpp additionally commits the first ~64 encoded nodes early so the GPU starts before encoding finishes — same idea applies to our planned frame (split the frame into 2 command buffers: layers 0–k committed immediately).
3. **Elementwise fusion + barrier reduction** (est. +2–4 tok/s). Qualify and default-on the already-written opt-in fusions: `Q4_0_LINEAR_RMS_ADD_SUMSQ` (matvec+RMS+residual), pair-activation gate/up fusion, `SMALL_ROWS_NORM_REDUCE`. Then add llama.cpp-style **hazard-aware reorder**: our concurrent-dispatch experiment failed as *blind* concurrency; llama.cpp makes it work by reordering nodes (64-node look-ahead over a reorder-safe whitelist) so Q/K/V projections and independent norms share one barrier-free concurrent span. Target: 422 → <150 barriers/frame. **Known traps (do not repeat):** metadata-only barriers ⇒ SoC watchdog reset; one persistent encoder for the whole frame ⇒ 6× regression (both documented in METAL.md).
4. **Broaden MMV auto-qualification** (est. +1–2 tok/s). Auto currently deviates from legacy only on M4 + two exact FFN shapes; attention/PLE/tail/down matvecs run legacy shapes. Ship per-device tuned dispatch tables generated by the existing offline `--sweep` (llama.cpp now ships exactly this: generated `ggml-metal-tuning` tables keyed by device/dtype/shape bucket). This is also the vehicle to finally merge value from the unmerged `codex/quant-kernel-runtime-jit` branch: keep the *offline sweep → checked-in table* part, drop runtime JIT.
5. **PLE micro-items**: quantize `per_layer_model_proj` F16 → Q8_0 (−27 MB/token, ~-0.1 ms), PLE row-stride hoist, fold PLE gate/act/proj into fewer dispatches (already flagged in METAL.md as the next collapse target).

## 4. Phase 2 — Beat llama.cpp, chase MLX (target: 76 → 84–90 decode tok/s)

1. **Byte reduction on the tail**: repack/tie the LM head to a 4-bit-class format (MLX-style affine group-64 with the qdot mask/FMA dequant, or Q4_K with QAT-aware requant of the head only; validate perplexity on the QAT checkpoint). −172 MB/token ≈ +5–7 tok/s. This is where MLX's remaining lead lives; block-weight bpw is otherwise identical.
2. **Flash-decoding attention for long context**: replace/augment kv_1x with a KV-split vec kernel (llama.cpp: 32 workgroups split the KV, each emits partial O + (S,M) stats, tiny merge-reduce kernel; 32-wide masked-chunk skip makes iSWA masks nearly free). Irrelevant at 64-token benchmarks, decisive at 4–32k. The existing opt-in split-GQA route is the starting point — qualify it default-on with the scan-clamp.
3. **Quantized KV cache (Q8_0 first)**: a *speed* feature once KV-bound (int4/8 KV outruns F16 KV on Apple Silicon in multiple 2025-26 reports). Requires dequant-in-register in the attention kernel; keep F16 as default until long-ctx evals pass.
4. **Graph/plan reuse with input re-binding**: llama.cpp's `can_reuse` path collapses per-token host work to re-binding; our `fillLayerSpecsCached` fingerprint cache is close — add telemetry for silent cache-miss rebuild-per-token and make misses loud.
5. **E2B pass**: repeat Phase 0 attribution on E2B (30-layer class, M4 Pro reference ~80 greedy / ~52 sampled); E2B is the latency flagship on 16 GB machines and everything above applies at smaller shapes. Add E2B to circus.

## 5. Phase 3 — Leapfrog: speculation + serving path (target: e2e ≥ decode, and 1.5–2.5× effective tok/s)

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

## 6. Phase 4 — Novel / research track (time-boxed spikes)

- **MatFormer E2B-inside-E4B self-drafting**: E2B is a nested submodel of E4B — a *free* draft sharing weights and (partially) KV. Nobody ships this; spike after MTP lands as a comparison arm (MTP likely wins on acceptance-per-drafted-FLOP, but MatFormer needs no extra artifact).
- **Activation sparsity**: Gemma E-series has trained-in FFN top-k sparsity (3n lineage); no production runtime exploits it. FFN is 65.7% of our bytes — even 25% effective skip ≈ +10 tok/s. High risk, high novelty; gate on a quality eval.
- **Metal 4 tensor ops / cooperative tensors** (macOS 26+): hardware 4/8-bit dtypes with block-scale planes make MXFP4-class dequant a tensor-unit feature; target prefill and W4A8 first. Track **M5 NAX** (vLLM-Metal already uses it for prefill attention) for the next hardware cycle.
- **Deliberately skip**: ICB replay (measured 4× slower here; industry agrees), ANE decode (fixed shapes, ~9 tok/s at 8B-class), tree speculation on Metal.

## 7. Gaps & process improvements (found during this review)

1. **Perf features die in opt-in purgatory.** 729 `TERMITE_METAL_*` flags; sweep-tuned kernels ship in the binary but never promote; `kernel_jit` defaults off; runtime-JIT work stranded on unmerged `codex/quant-kernel-runtime-jit`. → Define a promotion pipeline: candidate → shadow (dispatch-count parity) → qualified-per-device default-on with `DISABLE` rollback; review the flag inventory quarterly and delete dead gates.
2. **No perf CI.** Nothing guards decode tok/s, encoder/barrier counts, or route selection on merge. → Add a nightly M-series job running `bench_metal_gemma4_e2b.sh`/`compare_metal_gemma4_e4b_qat.sh` with regression thresholds on `decode_tok_s`, `hot_decode_tok_s`, `planned_barriers`, and quant-route counters.
3. **Benchmark/mode mismatch.** In-repo harnesses are non-streaming CLI; circus measures the streaming server — the per-token SSE tax is invisible to local benchmarking. → Add a streaming-server mode to the AB harness.
4. **Machine identity absent from the ledger** (M4 Air vs M4 Pro is a 2.3× roofline difference; the fanless-Air thermal trap is documented but not enforced). → Ledger schema: chip, BW, power state, interleaving.
5. **Known live bugs on the critical path**: prompt-cache prefix attach degrades KV / hangs on Metal; kv_compacted handling; silent fallbacks (split-GQA, layer-spec cache misses) with no counters surfacing them. → Make every silent fallback increment a logged counter; alert in the AB harness.
6. **Docs drift**: METAL.md census is Q8_0-only; PERF.md is A4B-only. → Land the Phase-0 Q4_0-E4B census in METAL.md.

## 8. Sequencing & success criteria

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

---

## 9. Implementation ledger (worktree `worktree-gemma4-perf-plan`, 2026-08-26, base M4 Air 16GB / ~120 GB/s — qualification numbers must be re-run on the M4 Pro box)

**Landed and validated:**

1. **Pipelined decode frame default-on for M4-qualified devices** (was opt-in via `TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME`; Phase 0 resolved the audit discrepancy — it was opt-in at both Zig sites). New C export `termite_metal_pipelined_decode_frame_device_default()` reuses the fast-prepared-frame M4 qualification; `DISABLE` flag still wins. Interleaved A/B on this Air: **E2B +9–12% decode (≈46 → ≈51.5 tok/s), E4B +5–8% (≈25.4 → ≈27.0)**, token-ids bit-identical on both models. Files: `metal_kernels.m`, `metal_runtime.zig`, `pipelines/generation.zig`, `graph/metal_executor.zig` (+ updated unit tests).
2. **Q6_K rows==1 MMV portfolio** (`termite_q6_k_linear_1x_reduce_nsg4/_nsg8` + `TERMITE_METAL_Q6_K_MMV_VARIANT=auto|legacy|nsg2|nsg4|nsg8`, nil-pipeline fallback). Bit-identical outputs. Interleaved A/B on this Air showed **no repeatable win** (legacy won 3/3 rounds; earlier +7–9% was warmup noise) → **AUTO stays legacy**; qualification belongs on the M4 Pro box where the tail is bandwidth- not latency-bound.
3. **Serving path**: chat template rendered + prompt tokenized **once** per request (admission estimate transfers ownership into the pipeline; reuse guarded by token-limit equality + grammar-rewrite invalidation); single JSON body parse for kwargs-bearing requests (Value-parse → validate → `parseFromValue`, byte-identical error behavior); SSE chunk = **one** `sendAll` (≤8 KB fast path, wire-format-identical fallback) and per-token JSON into a reused buffer. Batch endpoint double-parse deliberately skipped (off hot path). Files: `server/server.zig`, `pipelines/generation.zig`, `zig/lib/httpx/src/server/server.zig` (+ httpx wire-format test, SSE-ordering tests updated).
4. **MTP enablement groundwork**: `antfly inference pull` of a Gemma4 QAT gguf now best-effort pulls the sibling `-unquantized-assistant` MTP drafter repo (recursion-safe, warn-only on absence; unit test added); server auto-discovers a local sibling assistant as draft when no `draft_model` is in the request — gated behind `ANTFLY_GEMMA4_MTP_AUTO_DRAFT_DISCOVERY` (default off) and sets `speculation_requested` for scheduler correctness.

**Measured on this Air (not qualification-grade):** MTP end-to-end on Metal WORKS (16 rounds, 50% acceptance, probe calibration) but is net-slow here (20.9 vs ~27 plain; accelerator flag stack made it worse, 14.7 @ 37% acceptance) — the adaptive `disabled_slow` gate behaves correctly. Auto-MTP also never fires below 128 generated tokens (`ANTFLY_GEMMA4_MTP_AUTO_MIN_TOKENS`) — the 64-token circus benchmark cannot benefit until that default is revisited.

**Not started from this plan:** Q6_K tail byte-reduction (head repack), flash-decode KV-split attention, quantized KV, fusion-flag promotion (needs M4 Pro A/B), graph-reuse telemetry, perf CI, standalone-server speculation plumbing.

## 10. Implementation ledger, round 2 (deep-work items, 2026-08-26, same worktree/machine caveats)

1. **LM-head 4-bit repack** (`TERMITE_METAL_ENABLE_LM_HEAD_Q4_REPACK=q4_k`, opt-in): streaming per-row Q6_K→Q4_K requant of the prepared tail slot (embedding lookup keeps Q6_K; one-row f32 transient instead of a ~2.7GB image) + a new ggml-style `termite_q4_k_linear_1x_reduce_v2` rows==1 kernel (masked-nibble FMA dequant, sumy-folded mins, 2 rows/SG pointer-bump; AUTO-selected for M4 vocab tails, `TERMITE_METAL_Q4_K_MMV_VARIANT=legacy` rollback). Interleaved A/B: **E2B 52.5→54.3-55.2 (+4-5%), E4B 28.9→30.1-30.2 (+4-5%)**, token-identical on all probes (ocean/tides/math × both models). Refuted: Q4_0 head (instant-EOT quality collapse — symmetric no-min 4-bit on an embedding matrix; matches llama.cpp's practice of never quantizing output.weight below Q6_K); Q4_K with the legacy kernel (slower than Q6_K baseline despite −31% bytes).
2. **Flash-decoding KV-split composes with pipelined frames**: the split-GQA scratch is now a 2-buffer pool alternated at frame submit, so the `submitted_frame_cb` exclusion is gone — the default opportunistic split route engages during pipelined decode. Long-context (≈2k) decode, interleaved: **E2B ~2× (43/34/31 vs 22/17/18 tok/s), E4B +41% (24.0 vs 17.1)**, bit-identical tokens, `fallbacks=0`, split-routes on-device oracle suite green, short-context unaffected. Also fixes the `RingKvRequiresPagedAttention` crash when `TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT` was set explicitly.
3. **Barrier work reframed by measurement**: the live Q4_0 E-series decode frame already submits with **planned_barriers=0** (whole-frame scoped suppression on a serial encoder; 1 compute encoder, 143 planned scopes) — the 422-barrier census was the Q8_0 anchor. Landed: the hazard scan is skipped whenever barriers are suppressed/disabled (tracker CPU **0.6→0.1 ms/frame**, appends/capacity flushes unchanged and strictly more conservative on suppression lift; tokens identical), plus RAW/WAR/WAW attribution counters in `metal_planned_access_profile` so the true llama.cpp-style lever — concurrent dispatch + plan-level reorder — can be qualified later. That experiment class (like any barrier-elision variant) stays off this fanless laptop: METAL.md records a delayed SoC-watchdog hard reset from the metadata-only-barriers experiment; qualify on the M4 Pro/CI box.

Updated expectations for the M4 Pro box: items 1-2 stack with round 1 (pipelined default-on). The long-context win (item 2) should be the headline in the next circus run if it includes any multi-hundred-token prompts; the 64-token single-prompt circus scenario mostly reflects round-1 + item-1 gains.

## 12. Roofline-efficiency round (2026-08-27, M4 Air, uncommitted on top of the review-fix diff)

Executed the approved roofline plan's Air-valid slice; M4 Pro items remain queued.

**Landed (all validated: builds green, tokens identical unless noted, interleaved A/Bs):**
1. **B1 fusion campaign (zero-code A/Bs → one default flip):** pair-activation fusion PROMOTED default-on for M4 (`DISABLE_Q4_0_PAIR_ACTIVATION_FUSION` rollback) — bit-identical E2B+E4B, +0.5–1% repeatable, −84 dispatches/frame. **Sumsq fusion REFUTED with data: −12% repeatable on E2B (47.7 vs 54.2)** — the first recorded number for why it sat in opt-in purgatory; this also downgrades B2 (fused-FFN scope) whose final op is this kernel — B2 DEFERRED pending census on capable hardware. Fused-QKV on E4B: neutral on Air (recorded; M4 Pro re-test).
2. **A1 PLE model-proj Q8_0 staging (default-on, `TERMITE_METAL_DISABLE_PLE_MODEL_PROJ_Q8` rollback):** new `prefer_q8_over_dense_bf16` slot tag through contract→options→bridge→runtime; bf16 branch stages pre-return; dense-path budget bypassed for tagged slots. Engages both models (E2B slot 351 8960×1536 — E2B's was dense F32, saving ~41 MB/token; E4B slot 421 10752×2560 bf16→Q8 ~13 MB). Tokens IDENTICAL both models; E2B +1–3 tok/s repeatable; E4B wash on Air (M4 Pro re-check). Also removes the per-frame dense encoder break.
3. **M0.1 GB/s census:** `approx_op_bytes`/`approx_gb_s` in `metal_q4_0_linear` bench. Air matrix (120 GB/s peak): FFN gate/up 80.7 (67%) · down 87 (73%) · pair 94.3 (79%) · **Q6_K tail 98.7 (82% — best stream; the tail-inefficiency hypothesis is REFUTED at kernel level; the repack win was bytes, not efficiency)** · attention-shape 2560×2048: 28.4 default vs **69.9 with nr4-nsg4 (2.5× microbench)** — but the e2e attention-workload override is NEUTRAL on both models, so in-frame attention selection/shapes differ from the microbench default; resolve on census-capable hardware. Reminder for that work: `--ops-per-frame 64`+ is mandatory (1 op/frame measures submit latency: 12.8 GB/s).
4. **M0.3 encode-CPU counter** (`encode_cpu_us` in the frame-lifecycle trace): E2B shows **~17.4 ms begin→commit on an ~18 ms frame period.** Ambiguous (window can include the pipelined wait on the prior frame) but if even half is real encode, the **M4 Pro is plausibly host-encode-bound** (its GPU frames are ~2.3× shorter while encode cost is CPU-constant) — this would reframe C1/C2 (residency sets / unretained CBs) and llama.cpp-style graph reuse as first-order, and could explain part of the 62-vs-72 circus gap. NEXT: split the counter into pure-encode vs wait spans before acting.
5. **M0.4 ledger v4:** AB harness metadata now records chip/hw.model/memsize/nominal-GB/s/thermal speed-limit. **M0.5:** `scripts/perf_watchdog_experiment.sh` (fsynced intent record, mandatory soak, panic/log sweep).

**Machine facts recorded:** `MTLCounterSamplingPointAtDispatchBoundary` is UNSUPPORTED on this base-M4 Air (`stage timing supported=0`) — all in-frame attribution (M0.2 census, stage GB/s) requires the M4 Pro. Peak decode after this round: **E2B 56.2–56.5 tok/s** (vs ~44–46 at branch start, +25%), E4B ~30.4.

**Deferred with reasoning:** M0.2 per-dispatch census (unimplementable/untestable on this device); B2 fused-FFN scope (inherits the −12% sumsq component; needs census first); A3 scales-plane/interleave relayout (the big lever — census justified it at 67–73% FFN efficiency; 1–2 week M4-Pro-validated workstream); A2 AUTO-table folding (env overrides ready; fold after M4 Pro sweep).
