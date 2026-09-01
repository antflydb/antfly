# Gemma 4 E2B/E4B Metal Performance Plan — Closing the Gap with llama.cpp and vLLM-Metal

Date: 2026-08-26 · Baseline: v0.2.1-rc0 circus benchmark (`https://circus.antfly.io/v0.2.1-rc0/#inference-generation`)
Scope: single-stream Gemma 4 E4B/E2B QAT Q4_0 generation on Apple Silicon (Metal). The initial analysis was plan-only; §§9–16 record the subsequent implementation and qualification ledgers.

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

1. **Stage timing**: `TERMITE_METAL_STAGE_TIMING=1` on E4B Q4_0 64- and 512-token runs; compare each decode bucket (attention/ffn/ple/tail/embedding) against its byte floor from §1. This alone confirms or kills the Q6_K-tail hypothesis. (Parsed by `scripts/gemma4/benchmark_metal_gemma4_ab.py`.)
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

## 11. Pre-handoff review + fix round (2026-08-26, originally uncommitted on top of b064b22de)

A high-effort multi-agent review (8 finder angles, 21 adversarially verified candidates) produced 10 findings; all were fixed and re-validated (perf wins intact: E2B 53.9→56.5 with repack, tokens identical, long-ctx split ~1.9x, split-routes oracle green, both -Dmetal variants build, targeted tests pass):

1. `-Dmetal=false` macOS link failure → `build_options.enable_metal` comptime guard on the new extern (empirically re-verified).
2. Double free on the repack OOM path → explicit ownership handoff, no errdefer armed across the transfer. (Same latent pattern pre-exists in the Q8 helpers — untouched, note for a follow-up.)
3-4/7. Auto-draft discovery restructured: runs BEFORE qualified-profile validation, admission, and the prompt estimate (speculation_requested now set at config build → estimate/limit/pre-encode-reuse consistent); reserves speculation slot units (degrading, not rejecting, when capacity is short — same at the native-generate lease); name-qualified like the registry companion rule (gemma-4 + -qat only) with manifest/config pre-validation; ALL draft-setup failures for a discovered drafter degrade to single-model generation (labeled draft_setup block) instead of failing the request. Verified live: flag + sibling present → request succeeds.
5. LM-head repack now gated on a real `lm_head` identity bit plumbed contract→rms-runtime→gated-runtime (tagged prepare) and across the metal_compute bridge (which had silently dropped the field — caught because the repack stopped firing), not on out_dim alone.
6. Repack env parse: `no`/`off`/`false`/`0` disable, unknown values warn+disable, `q4_0` warns loudly (kept for A/B evidence only).
8. Companion pull: never inherits --tasks/--capabilities (manifest inferred from its own plan) and skips when already installed.
9. One shared `pipelinedDecodeFrameEnabled()` policy in metal_runtime (cached env reads) used by both the pipeline and executor.
10. Q6_K/Q4_K variant env parsing case-insensitive with invalid-value warnings; forced nsg4/nsg8 gated on maxTotalThreadsPerThreadgroup; Q4_K v2 AUTO scoped to the TAIL workload via the descriptor's existing workload field (M4 + vocab-sized).

Review also REFUTED with evidence: JSON duplicate-key differential (Value parser errors on duplicates too), early-free of the parsed Value tree (typed request ALIASES it — the defer is required), prompt-cache/speculation pairing (blocked downstream by `!use_speculative`), executor-gate weakness (arm path re-checks the full gemma+PLE contract), suppression-mode barrier race (concurrent mode forces the scan on), split-scratch memory doubling (~2 MiB, trivial).

Known accepted residue for the M4 Pro follow-up: single-threaded repack at load (~seconds; parallelize with the cold-load worker pool), Q8 helpers' pre-existing errdefer pattern, batch endpoint double-parse, per-hazard attribution only visible in concurrent mode.

## 12. Roofline-efficiency round (2026-08-27, M4 Air, uncommitted on top of the review-fix diff)

Executed the approved roofline plan's Air-valid slice; M4 Pro items remain queued.

**Landed (all validated: builds green, tokens identical unless noted, interleaved A/Bs):**
1. **B1 fusion campaign (zero-code A/Bs → one default flip):** pair-activation fusion PROMOTED default-on for M4 (`DISABLE_Q4_0_PAIR_ACTIVATION_FUSION` rollback) — bit-identical E2B+E4B, +0.5–1% repeatable, −84 dispatches/frame. **Sumsq fusion REFUTED with data: −12% repeatable on E2B (47.7 vs 54.2)** — the first recorded number for why it sat in opt-in purgatory; this also downgrades B2 (fused-FFN scope) whose final op is this kernel — B2 DEFERRED pending census on capable hardware. Fused-QKV on E4B: neutral on Air (recorded; M4 Pro re-test).
2. **A1 PLE model-proj Q8_0 staging (default-on, `TERMITE_METAL_DISABLE_PLE_MODEL_PROJ_Q8` rollback):** new `prefer_q8_over_dense_bf16` slot tag through contract→options→bridge→runtime; bf16 branch stages pre-return; dense-path budget bypassed for tagged slots. Engages both models (E2B slot 351 8960×1536 — E2B's was dense F32, saving ~41 MB/token; E4B slot 421 10752×2560 bf16→Q8 ~13 MB). Tokens IDENTICAL both models; E2B +1–3 tok/s repeatable; E4B wash on Air (M4 Pro re-check). Also removes the per-frame dense encoder break.
3. **M0.1 GB/s census:** `approx_op_bytes`/`approx_gb_s` in `metal_q4_0_linear` bench. Air matrix (120 GB/s peak): FFN gate/up 80.7 (67%) · down 87 (73%) · pair 94.3 (79%) · **Q6_K tail 98.7 (82% — best stream; the tail-inefficiency hypothesis is REFUTED at kernel level; the repack win was bytes, not efficiency)** · attention-shape 2560×2048: 28.4 default vs **69.9 with nr4-nsg4 (2.5× microbench)** — but the e2e attention-workload override is NEUTRAL on both models, so in-frame attention selection/shapes differ from the microbench default; resolve on census-capable hardware. Reminder for that work: `--ops-per-frame 64`+ is mandatory (1 op/frame measures submit latency: 12.8 GB/s).
4. **M0.3 encode-CPU counter** (`encode_cpu_us` in the frame-lifecycle trace): E2B shows **~17.4 ms begin→commit on an ~18 ms frame period.** Ambiguous (window can include the pipelined wait on the prior frame) but if even half is real encode, the **M4 Pro is plausibly host-encode-bound** (its GPU frames are ~2.3× shorter while encode cost is CPU-constant) — this would reframe C1/C2 (residency sets / unretained CBs) and llama.cpp-style graph reuse as first-order, and could explain part of the 62-vs-72 circus gap. NEXT: split the counter into pure-encode vs wait spans before acting.
5. **M0.4 ledger v4:** AB harness metadata now records chip/hw.model/memsize/nominal-GB/s/thermal speed-limit. **M0.5:** `scripts/perf_watchdog_experiment.sh` (fsynced intent record, mandatory soak, panic/log sweep).

**Machine facts recorded:** `MTLCounterSamplingPointAtDispatchBoundary` is UNSUPPORTED on this base-M4 Air (`stage timing supported=0`) — all in-frame attribution (M0.2 census, stage GB/s) requires the M4 Pro. Peak decode after this round: **E2B 56.2–56.5 tok/s** (vs ~44–46 at branch start, +25%), E4B ~30.4.

**Deferred with reasoning:** M0.2 per-dispatch census (unimplementable/untestable on this device); B2 fused-FFN scope (inherits the −12% sumsq component; needs census first); A3 scales-plane/interleave relayout (the big lever — census justified it at 67‒73% FFN efficiency; 1–2 week M4-Pro-validated workstream); A2 AUTO-table folding (env overrides ready; fold after M4 Pro sweep).

## 13. M4 Pro production-readiness ledger (2026-08-27, final uncommitted source at `e92beae1101b4852ba98f031112eb2bf1a205de7`)

This round inspected the pushed roofline changes, requalified both real Gemma4 models against llama.cpp on the M4 Pro reference machine (Mac mini, 24 GiB), fixed the production defects exposed by the review, and repeated the benchmark with the exact final Metal binary. No commit or push was made.

### 13.1 Final pinned comparison

Protocol: fresh process for every engine run; interleaved Antfly/llama.cpp pairs with a 2-second cooldown; five measured repetitions; greedy generation, F16 KV, whole-model Metal, EOS ignored; 23-token/256-output short case and 2,334-token/128-output long case. Antfly's strict metric is `(generated_tokens - 1) / decode_inner_seconds`; llama.cpp's is its reported eval throughput over the matching 255/127 decode evaluations. CV is across the five strict samples.

| Model / prompt | Antfly median tok/s | CV | llama.cpp median tok/s | CV | Antfly / llama.cpp | Gap |
|---|---:|---:|---:|---:|---:|---:|
| E2B short | 71.150 | 0.355% | 109.120 | 0.212% | 65.20% | -34.80% |
| E4B short | 47.170 | 0.105% | 63.280 | 0.122% | 74.54% | -25.46% |
| E2B long | 85.349 | 0.137% | 105.050 | 0.093% | 81.25% | -18.75% |
| E4B long | 53.836 | 0.060% | 61.380 | 0.128% | 87.71% | -12.29% |

All Antfly repetitions emitted identical token IDs within each scenario and remained exact versus the first campaign on the pushed commit. Route evidence showed default PLE Q8 staging, E2B pair activation, long-context split-GQA, and zero prepared-frame fallbacks. The final short-case movement versus the first pushed-commit campaign was -1.31% E2B / -0.74% E4B; long was +0.87% / +0.30%, within the campaign-level thermal/run-order envelope rather than a claimed speedup from the safety fixes.

Pinned identities:

- Antfly Metal ReleaseFast SHA-256 `c3128c25c999c27769928727a3c64857af54fca0c8236010924a88d39494b0d0`.
- llama.cpp `llama-completion` version 10342 (`38278078c`), SHA-256 `92dcad3c204b0574c99611af7a1f64d69ad0506c3abeba56bef8e4ec57fa0bc8`.
- E2B GGUF SHA-256 `fa401b55b07ee70a54c6dae3903c783a6e65064312529ea57175cb5f8dec6634`.
- E4B GGUF SHA-256 `676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee`.
- Machine-readable diagnostic: `/private/tmp/antfly-gemma4-e92-vs-llama-v1/final-vs-llama-v2/benchmark-summary.json`. This is transient diagnostic evidence, not an immutable release artifact.

### 13.2 Findings addressed in source

1. **Host-encode attribution corrected:** frame telemetry now separates wall, pipelined-wait, and pure CPU encode spans. Steady E2B medians were 12.474 ms wall, 11.840 ms wait, and only 0.640 ms CPU encode. The suspected host-encode bottleneck is refuted; optimization should stay focused on GPU/frame work and command-plan/dispatch efficiency.
2. **Q8 staging ownership hardened:** the pushed dense-to-Q8 preparation path had an armed `errdefer` across an ownership-transfer call, leaving a latent failure-path double-free. Ownership is now transferred only after all fallible sibling allocations complete; the matching large-mmap test exercises the pattern.
3. **Auto-MTP admission unwind made exact:** failure fallback previously released `reserved_units - admission_units`, which could discard separately-grown media reservation. The server now tracks and releases only the auto-draft increment and reacquires the correctly-sized scheduler lease when the second backend disappears.
4. **Capacity-one model publication fixed:** speculative prewarm is skipped when publishing the primary model must first evict the only resident model, avoiding the predictable `ResourceTemporarilyUnavailable` warning; post-eviction lazy preparation remains intact.
5. **Server CLI contract fixed:** `--max-loaded-models` was preparsed but rejected by the main option loop. It is now consumed and covered by a focused parser test.
6. **Expected SSE disconnect reclassified:** client cancellation/reset/write-close errors are operational info events, while OOM, oversized-event, and parser failures remain errors. Live cancellation now leaves a clean log and a healthy server.
7. **Production-route tests brought back in sync:** the aggregate suite exposed a stale split-GQA source census that still expected one scratch allocation after the two-buffer pipeline fix, plus two FFN tests that assumed split gate/up always outranked the new M4 pair+activation default. The tests now assert both scratch buffers and alternation, reject the removed submitted-frame exclusion, and validate the route that actually executed while requiring incompatible counters to remain unchanged.

### 13.3 Verification matrix

- Pinned Zig 0.16.0 ReleaseFast builds passed with Metal (`-Dmetal=true -j1`) and without Metal (`-Dmetal=false`); final non-Metal binary SHA-256 `2b2db1f960c543834d86aaafe33d9cf465ab714e3ba1dd36a29d6d729cc77552`.
- The final aggregate ReleaseFast Metal unit gate exited zero: 3,167 library tests selected (3,148 passed, 19 intentional skips), 11/11 CLI-root tests passed, and 23/23 generated-kernel checks passed. It completed in 524.92 seconds with 12.45 GB maximum RSS and zero swaps.
- Focused regressions passed for CLI consumption, Q8 mmap ownership, exact auto-draft unwind, capacity-aware prewarm, and streaming disconnect classification. The incoming pair/Q8/admission/companion suite also passed (23 dependent tests).
- On-device split-GQA oracle passed every E2B/E4B geometry with maximum absolute error at or below `1.1e-6` and rollback route count zero. The Q4_0 pair-activation oracle passed exact-hash default/portfolio/rollback checks.
- The reviewed exact-v1 quality campaign ran 48 generations: two models × four 2,051-token cases × default/pair-off/PLE-Q8-off × two repetitions. Generated token IDs were exact across every default and rollback lane, and all required route toggles were observed.
- Live HTTP qualification with `--max-loaded-models 1` passed E2B→E4B→E2B eviction/reload, two concurrent 128-token completions, intentional streaming cancellation, health, documented retryable immediate recovery, and successful recovery after the lease drain. All final responses were valid and the server log contained no warning/error events.

### 13.4 Release decision and remaining gates

**Code-level result: PASS. Full production/release promotion: BLOCKED.** Pair activation and PLE Q8 are token-exact with working rollback controls, and the exercised CLI/server/resource failure paths are clean. The following gates prevent an industry-grade release claim:

1. **Strict zero-paging failed on this machine state.** The final 40-run comparison had 36 zero-delta engine runs but campaign deltas of 61 pageouts and 8 swapins; the quality campaign added 2 pageouts and 204 swapins. Existing swap use was approximately 1.35 GiB. Re-run from a zero-swap, zero-pageout state before promotion.
2. **The full exact-v1 semantic suite remains fail-closed.** All structured canaries and E4B free-form output passed, but E2B's deterministic free-form response was 164 words against the fixture's 180-word floor in all six default/rollback samples. Because the outputs are identical across lanes, this does not implicate either new default, but the model-level release gate is still red until the fixture expectation is adjudicated or the model behavior changes.
3. **Auto-MTP cannot be release-qualified here:** no matching Gemma4 unquantized-assistant GGUF is installed. Discovery remains opt-in and unpromoted.
4. **LM-head Q4_K repack remains opt-in:** token probes are insufficient for this lossy conversion; it still needs a pinned logit/perplexity quality campaign before any default flip.
5. **Evidence durability/CI remains open:** current comparison and server artifacts live under `/private/tmp`; promotion requires retained artifacts plus hosted CI/repetition on a clean machine.

The competitive priority is now unambiguous: short-context GPU/frame efficiency, especially E2B, not host encode. Long-context split-GQA already closes E4B to 87.7% of llama.cpp. The next performance round should therefore use the M4 Pro dispatch census to localize the remaining short-frame gap, then qualify command-plan reuse/concurrent dispatch and the A3 Q4_0 scales-plane/interleave relayout behind exact-output and watchdog gates.

## 14. Release-blocker closure: MTP, LM-head repack, and competitive target (2026-08-27)

This round starts from the user's committed `a3c645c6f130601a039341a7bb5b9db9f5cce51a` and leaves all follow-up source and evidence changes uncommitted. The reference machine is a Mac mini `Mac16,11`, Apple M4 Pro, 24 GiB. The final hardened executable used by the current MTP HTTP, LM-head quality, and Antfly-versus-llama.cpp campaigns has Metal ReleaseFast SHA-256 `690224e4c9f7a7f855eeb03270ecd9a2e1d478ea9bc61b458d58f575924b8619`. The Q6_K scheduling experiments below used the earlier `d527a813e01633d7de7521f99389192734ec4c928260507a27989643950b6db5` binary; the immediately preceding performance-only rerun used `f1601dd27fb17e7163ba1faca07723b7c32fdcd1d6b1918a8f84b4dc57ebf6f0`.

### 14.1 MTP assistants and server correctness

The official BF16 assistants are installed as matching sibling models:

- E2B: `~/.antfly/inference/models/google/gemma-4-E2B-it-qat-q4_0-unquantized-assistant/mtp-gemma-4-E2B-it-BF16.gguf`, 162 MiB, SHA-256 `72d948683dbd8b4da9c9a1714406a2dc6db3bd7c94afd59b65389605015d5db6`.
- E4B: `~/.antfly/inference/models/google/gemma-4-E4B-it-qat-q4_0-unquantized-assistant/mtp-gemma-4-E4B-it-BF16.gguf`, 164 MiB, SHA-256 `cb70f7a55c900e01911deb881c742d752cd63e047002da95750a99bf13c41516`.
- Source repositories: `https://huggingface.co/unsloth/gemma-4-E2B-it-qat-GGUF/tree/main/MTP` and `https://huggingface.co/unsloth/gemma-4-E4B-it-qat-GGUF/tree/main/MTP`; sibling repository identities match Google's `gemma-4-E2B-it-qat-q4_0-unquantized-assistant` / `gemma-4-E4B-it-qat-q4_0-unquantized-assistant` naming contract.

Both inspect as `gemma4-assistant` with four layers, hidden size 256, the expected backbone identity, and a full-vocabulary projection. The E4B projector-metadata server failure reported on the smaller laptop did not reproduce.

The live server exposed one production defect: automatic Metal selection promoted requests to compiled whole-model execution before speculation was resolved, but MTP needs the target's final hidden rows and the compiled contract exposes only logits/tokens. The server now treats `speculation_requested` as a whole-model auto-promotion exclusion and keeps speculative requests on the qualified eager decoder-runtime path. Auto-discovered AUTO/NONE requests are also upgraded to probe calibration before validation/admission so they cannot enter the uncalibrated fallback. Focused tests cover both contracts.

Final hardening also derives `speculation_requested` from the *effective* drafter after policy resolution. A request that names `draft_model` with `speculation_policy=off` therefore remains a normal request instead of silently losing compiled-path eligibility. The final binary revalidated the current-source HTTP path with nonempty visible output and exact 192-token length accounting:

| Model / policy | HTTP | Completion | Decision | Visible output |
|---|---:|---:|---|---|
| E2B policy off | 200 | 192, length | no speculation status | 286 bytes |
| E2B auto + Metal probe | 200 | 192, length | `disabled_slow` / `mtp_auto_cost_probe_slow` | exact versus policy off |
| E4B auto + Metal probe | 200 | 192, length | `disabled_slow` / `mtp_auto_cost_probe_slow` | 377 bytes |

Final response artifacts and SHA-256 identities:

- `/private/tmp/antfly-gemma4-prready-6902-mtp-policy-off-e2b-192.json`: `6ea07329b0efe1bc2e1f11c8e83a9000de881851aa50ea94187daf0d955911af`.
- `/private/tmp/antfly-gemma4-prready-6902-mtp-auto-metal-e2b-192.json`: `90c17900a1c811218d1f74a29385de5b2d164c9c16b80c4558d3a7ff9e13a81e`.
- `/private/tmp/antfly-gemma4-prready-6902-mtp-auto-metal-e4b-192.json`: `56093a53bd7a6880f770eb7273c25a80015e7ea1f1843deb81c150df16357352`.

Post-fix HTTP qualification used explicit two-model host/backend budgets and produced valid responses:

| Model / policy | HTTP | Completion | Decision | Visible-output parity |
|---|---:|---:|---|---|
| E2B auto + Metal probe | 200 | 191, stop | `disabled_slow` / `mtp_auto_cost_probe_slow` | exact versus force K=2 |
| E2B force K=2 | 200 | 191, stop | `forced` | exact versus auto |
| E4B auto + Metal probe | 200 | 94, stop | `disabled_slow` / `mtp_auto_cost_probe_slow` | exact versus force K=2 |
| E4B force K=2 | 200 | 94, stop | `forced` | exact versus auto |

Response artifacts and SHA-256 identities:

- `/private/tmp/antfly-gemma4-d527-mtp-e2b-auto-metal-192.json`: `0ba7cf3e8d0e9908bdb4ac50f1c963f45c6ef754e474a0bd104f7a8a41560180`.
- `/private/tmp/antfly-gemma4-d527-mtp-e2b-force-k2-192.json`: `6ebf7caca7208fc4adc6bba5c455a99522a240bb5f61898c4cd8fbdd429af0a2`.
- `/private/tmp/antfly-gemma4-d527-mtp-e4b-auto-metal-192.json`: `df9e38ba012bcc8323995920867661a0c28405e5f6c54cbece172031d5c6f9e8`.
- `/private/tmp/antfly-gemma4-d527-mtp-e4b-force-k2-192.json`: `cdd624bc5e874c554905d18b5b0be33d311c6ec9cc8f9c20d4b006d2f25f59a0`.

The automatic default memory envelope also rejected a plain, non-speculative E2B control with the same 507 `MEMORY_BUDGET_EXCEEDED`, proving that this is conservative capacity policy rather than an MTP unwind defect (diagnostic SHA-256 `5230f22605fe6ba962425cc8c9c69dd87de875c7dd2a060d83c932097314ec8c`). The current server error names all six applicable controls (host, backend, combined, KV, scratch, and process), and `run --help` documents every accepted memory-budget override; focused and CLI-root tests enforce both contracts.

An earlier standalone CLI pilot explains the auto decision; treat its throughput as directional because its JSON did not record a binary identity. E2B target-only measured 75.919 tok/s over 128 tokens; BF16 MTP auto K=2 measured 63.682 tok/s, accepted 16/25 drafts (64%), and disabled itself after the 16-round cost probe. E4B target-only measured 54.983 tok/s over 64 tokens; forced BF16 K=2 measured 30.118 tok/s at 63.4% draft match. Q4 and Q8 assistant variants were also slower. **Decision: MTP correctness/fallback PASS; Metal-auto performance FAIL.** Discovery and Metal-auto remain separate default-off gates; forced MTP remains available for diagnostics.

### 14.2 LM-head Q4_K repack quality gate

Token probes were replaced with a pinned live-logit campaign. The fail-closed harness dumps the exact 262,144-way F32 host logits from the production Metal runtime and teacher-forces 146 continuation tokens across 13 cases (factual, arithmetic, reasoning, science, sequence, Python, Zig, JSON, tools, multilingual, safety, and production chat). It runs two alternating AB/BA repetitions, checks determinism and full-history/ring-prefill identity, and evaluates perplexity, KL, top-10 overlap, and top-1 agreement. The final hardening requires the paired logit dump whenever teacher forcing is present, rejects any teacher sequence that would be truncated, reads the diagnostic environment once per generation, requires a new evidence directory outside the source tree, pins binary/model/suite/script/source identities at both campaign boundaries, fixes the reviewed Gemma 4 vocabulary and repetition floor, and permits thresholds to become stricter but never looser. The reviewed suite SHA-256 is `f9f9240bbb6ec8ce0f0284053ac210f156711ff1fd050e7f019484ffbae52393`; the historical v2 harness SHA-256 is `d1b87d058f68b1c3ae0c2c8e85d5fea874fa3ff850a80932c720f083feb6d6e8`.

| Model | Top-1 agreement | PPL ratio | Mean / max KL | Mean top-10 overlap | Result |
|---|---:|---:|---:|---:|---|
| E2B | 144/146 = 98.630% | 1.006927 | 0.005443 / 0.029011 | 94.247% | FAIL `< 99%` |
| E4B | 142/146 = 97.260% | 0.992010 | 0.003910 / 0.031328 | 94.658% | FAIL `< 99%` |

Every other threshold passed, both repetitions were deterministic, and the ring/full-history prefill logits were byte-identical. The failures are nevertheless real argmax changes, including production-chat/safety cases; lower perplexity on E4B does not waive the top-1 contract. Evidence:

- E2B summary `/private/tmp/antfly-gemma4-prready-6902-repack-quality-e2b-v4/summary.json`, SHA-256 `900379ff90785914ec5495b7798c693b0f3b099ba358ebd9b71e13fb2769e0e5`.
- E4B summary `/private/tmp/antfly-gemma4-prready-6902-repack-quality-e4b-v3/summary.json`, SHA-256 `cdeeb8611325d98e17409d08d47e2001fe7fcb2ecb45b4a2c3708fd56c25d8b6`.

Both summaries report deterministic repetitions, byte-identical ring/full-history prefill logits, and stable campaign identities. The final build graph includes this contract under `test-metal-gemma4-lm-head-repack-quality` and the aggregate `test-metal-gemma4-benchmark-contracts` step.

**Decision: do not promote Q4_K LM-head repack.** It remains an explicit diagnostic opt-in despite its approximately 3-5% decode benefit. A future default requires a reviewed threshold change justified by an external semantic/perplexity evaluation, not a smaller token probe.

Review revalidation on 2026-08-28 upgraded the evidence contract to v3 and closed the later full-logit redirect hole: candidate evidence explicitly selects the transformed Q4_K slot, every candidate step must differ from Q6_K, and Q4_K top-8 nomination plus exact-Q6_K rescore must match the exact argmax at every step. The fresh E4B diagnostic rerun attested 146/146 distinct candidate dumps and 146/146 refined-argmax matches, but still failed the unchanged reviewed top-1 gate at 142/146 = 97.260% (PPL ratio 0.992014). The v3 harness SHA-256 is `5aa44bb21d5fc369a3c604114fbd2f815689cd619e707ce36cfdcd99f38df0e1`; diagnostic summary `/private/tmp/antfly-review-repack-quality-v3-e4b-prefill/summary.json` has SHA-256 `3f098bbe8fee7106d1011439229d1cde893b0f1d1dc2b05a219462c59869cd73`. The installed E2B control GGUF was not an eligible Q6_K-to-Q4_K repack target, so the v3 harness aborted before producing E2B evidence rather than accepting an inactive repack.

### 14.3 Competitive Q6_K target

The M4 Pro A/B harness was corrected to the production frame contract (N prepared frames for N emitted tokens, N+1 Q6 tail calls including prefill, N-1 public decode API entries), short-context paged-attention routing, and bounded pipelined-frame retention. Schema v5 tests pass 17/17.

Six-pair exact-token E4B comparisons tested the alternate Q6_K tail schedules against the current NSG2 default:

| Candidate | Baseline tok/s | Candidate tok/s | Paired throughput ratio | Wins | Result |
|---|---:|---:|---:|---:|---|
| NSG4 | 47.478 | 47.495 | 1.000185 (+0.0185%) | 3/6 | FAIL 1% floor |
| NSG8 | 47.381 | 47.416 | 1.000741 (+0.0741%) | 6/6 | FAIL 1% floor |

Tokens and required routes were exact and CVs were below 0.2%, so the null result is stable rather than noisy. NSG4 summary SHA-256: `91e954828be3b8827adf0e85efdfb9137c8188241c497b279d0d66bebab46bc3`; NSG8: `5463e7b02f46dd049cc33c1a7ba4b1f4716d36606c7cb2e925ee621e41fd18e4`. **Decision: retain NSG2 AUTO; do not promote NSG4/NSG8.** The competitive next target moves to short-frame command-plan/dispatch efficiency and the A3 Q4_0 scales-plane/interleave relayout.

### 14.4 Final Antfly versus llama.cpp measurement

Protocol is identical to §13.1: fresh process per sample; five interleaved samples per engine; two-second cooldown; greedy, F16 KV, EOS ignored; strict 255/127 decode-evaluation accounting. All Antfly outputs remained exact versus the prior campaign and every route contract passed. All 40 measured engine runs had zero page-out, swap-in, swap-out, and swap-used deltas; the whole campaign also had zero deltas.

| Model / prompt | Antfly median tok/s | CV | llama.cpp median tok/s | CV | Antfly / llama.cpp | Gap |
|---|---:|---:|---:|---:|---:|---:|
| E2B short (23 + 256) | 71.609 | 0.406% | 108.670 | 0.218% | 65.90% | -34.10% |
| E4B short (23 + 256) | 47.407 | 0.096% | 62.950 | 0.199% | 75.31% | -24.69% |
| E2B long (2,334 + 128) | 84.498 | 0.152% | 104.190 | 0.147% | 81.10% | -18.90% |
| E4B long (2,334 + 128) | 53.632 | 0.095% | 61.010 | 0.148% | 87.91% | -12.09% |

The final PR-candidate rerun is performance-neutral versus the immediately preceding `f160` campaign within variance: E2B short +0.225%, E4B short +0.204%, E2B long -0.067%, and E4B long +0.042%. Machine-readable summary: `/private/tmp/antfly-gemma4-prready-6902-vs-llama-v1/benchmark-summary.json`, SHA-256 `8c01f5e592ef2e089ec71b8d17c4daa007313add31564ee30738bcf1dca6d0ff`. Comparator identity remains llama.cpp v10342 (`38278078c`), SHA-256 `92dcad3c204b0574c99611af7a1f64d69ad0506c3abeba56bef8e4ec57fa0bc8`.

### 14.5 Final verification and release decision

- Pinned Zig 0.16.0 ReleaseFast Metal build/link passed; final executable-evidence binary SHA-256 `690224e4c9f7a7f855eeb03270ecd9a2e1d478ea9bc61b458d58f575924b8619`. The exact current source also passed a Metal-disabled build/link in an isolated prefix; binary SHA-256 `45e62a68de8936f695502a4c1fa09aad26707971c98df17f52ba96a1ac5809be`.
- Aggregate ReleaseFast Metal gate: **3,169 selected; 3,150 passed; 19 intentional skips**. The final CLI root passed 12/12 after the help-contract hardening; combined focused server/native regressions passed 5/5. The aggregate benchmark-contract step passed 42 long-output, 17 A/B, and 12 LM-head quality-harness tests; all modified/new Python scripts compile.
- Production hardening is fail-closed: teacher forcing requires paired logit evidence and cannot truncate; quality thresholds cannot be loosened; reviewed campaign dimensions and identities are pinned; policy-off drafts do not alter backend selection; speculative requests cannot auto-promote into an incompatible compiled contract; and all six memory-budget controls are tested in both help and failure output.
- On-device split-GQA oracle passed all schedules, defaults, boundaries, ring wrap, invalid-override fallback, and disable rollback with maximum absolute error `1.1e-6`. Pair-activation default/portfolio/rollback paths produced exact output hashes.
- `git diff --check` passed after the final ledger update; the final process-table audit found no inference server, llama.cpp, or benchmark process running.

**PR-candidate code qualification: PASS. Full performance-plan release promotion: BLOCKED.** The blockers are explicit and safe: MTP and repack remain default-off because their respective performance/quality gates failed, while the normal Metal path is build-clean, broadly tested, deterministic, and HTTP-qualified. Remaining full-release gates are:

1. Re-run from a truly pristine zero-swap boot. This final benchmark had zero VM deltas in all 40 samples and across the campaign, but the machine began with 933.19 MiB already allocated swap, so the strict pristine-zero-swap provenance gate is not satisfied.
2. Resolve the existing exact-v1 E2B free-form fixture mismatch from §13.4 (164 observed words versus the reviewed 180-word floor) or change model behavior; do not waive it implicitly.
3. Retain raw evidence outside `/private/tmp` and reproduce the gates in hosted M-series CI.
4. Close the measured short-context llama.cpp gaps. The next bounded experiment is command-plan reuse/concurrent dispatch; the next kernel/layout workstream is A3 Q4_0 scales-plane/interleave. Both require the existing watchdog, exact-token, route, and quality gates.

## 15. Round-3 attribution and plan: the short-context anomaly is solved; the residual is priced (2026-08-27, M4 Pro reference box, investigation only — zero source changes, no commits)

This round ran a 10-agent investigation (six code/docs/competitor mappers, two independent on-GPU attribution campaigns totaling ~70 fresh-process runs, a three-lens hypothesis panel, plus independent verification of every load-bearing claim) against HEAD `bada963a6f` (clean tree). Binary: local ReleaseFast Metal build SHA-256 `19021e9e90695922537d2acbc3534cec841e8c9b8eb282fc68d5c0770e75e7b6` — matches none of the §13/§14 pinned hashes (none are archived; see §15.5.7) but was validated by **byte-identical token-ID streams vs the §14.4 pinned campaign on all four scenarios** and throughput within −1.3%/+0.2%. Evidence bundle (durable, per release-gate 3): `~/Documents/af/antfly-gemma4-round3-evidence/` (six mapper reports, measurement report, 11 scored candidates, 49 run JSONs).

### 15.1 Headline: the short-context anomaly — named, root-caused, measured, and cheap to fix

**The anomaly no prior round named:** Antfly's own short-context decode is absolutely slower per token than its long-context decode (§14.4: E2B 13.96 vs 11.83 ms/tok; E4B 21.09 vs 18.65), while llama.cpp scales physically (9.20 short < 9.60 long). A 23+256-token KV is strictly less work than 2334+128 — this was a structural defect, not a "short-context gap vs llama.cpp."

**Root cause (code + measurement, triple-confirmed):** `TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV_TOKENS 512u` (`metal_kernels.m:66`, enforced in the selector at `:19439` and the dispatch gate at `:19595`) is a **compile-time policy floor with no env override** — `TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT=1` measured as a no-op below it. Below KV 512 every layer decodes attention through `termite_paged_attention_kv_1x` (`metal_kernels.m:10041-10048`), dispatched as only `q_len × num_heads` = **8 (E2B) / 16 (E4B) threadgroups on a 20-core GPU** (`:51687-51690`), with a per-thread serial `for (ki=0; ki<kv_tokens; ++ki)` V-accumulation over strided gathers. Measured cost: **0.695 µs per KV-token per dispatch on BOTH models** (E2B 24.3 µs/KV-token/frame over 35 attention dispatches, E4B 29.2 over 42; independent second campaign: 26.3/25.2) ≈ 3–6 GB/s effective KV read on a 273 GB/s machine — occupancy/latency-bound, not bandwidth. The split-GQA route costs ~0.2–0.35 µs/KV-token/frame — **30–100× cheaper** — and its `key_chunk=32` schedule (`:19376`) is valid at any KV; git provenance shows 512 landed inside the split-GQA feature commit (`f3f930ff27`) as a scope gate, never a measured crossover.

Decisive measured cells (this binary, campaign protocol, medians; full grid in the evidence bundle):

| Cell | route | E2B ms/tok | E4B ms/tok |
|---|---|---:|---:|
| short64 (KV 23–87) | paged_1x | 11.817 | 18.381 |
| short256 (KV 23–279) = benchmark | paged_1x | 14.153 | 21.182 |
| mid606 (606+128) | split-GQA | **11.465** | **18.047** |
| long (2334+128) | split-GQA | 11.803 | 18.626 |
| long, split force-disabled | paged_1x | 40.386 | 48.276 |

The KV→0 paged intercept (11.817) equals the long split frame (11.803): **there is no fixed short-route overhead at all** — the entire +2.33 ms/tok (E2B) / +2.80 (E4B) short-context excess is the paged_1x slope integrated over mean-KV 151. The linear model is exact (predicts 14.15 vs measured 14.153). Split is *faster* at KV 606 than paged at KV 23, so the crossover is far below 512. llama.cpp ground truth (§15.2): it has **no short-context special case whatsoever** — flash-attn vec with `nwg=32` KV-split + reduce unconditionally from KV=1 (`ggml-metal-ops.cpp:3232-3238`), KV padded to ≥256. Our split route is already their design; we just refuse to run it when it matters most.

Also measured and root-caused this round: a **0.287 (E2B) / 0.340 (E4B) ms/frame GPU-idle bubble** — the pipelined loop encodes frame k+1, waits on k, checks `shouldStopOnEos`, and only then commits k+1 (`pipelines/generation.zig:5944-5957`, order verified in source). Under the benchmark's `--ignore-eos` the deferral is provably pure waste; the general path can speculative-submit + discard-on-EOS.

**What remains after both fixes:** a flat, model-size-independent residual vs llama.cpp of ~2.2–2.6 ms/frame at every context (KV→0: 11.82 vs 9.20 E2B, 18.38 vs 15.89 E4B), attribution still open between serial-encoder dispatch-boundary drain (~525/~655 dispatches per frame, in-repo estimates disagree 5×: 1.7–2.5 vs 10–13 µs/boundary), split-GQA KV-read efficiency at long context, and blended kernel efficiency — §15.3 P1 is designed to split exactly this.

### 15.2 Competitor ground truth and route-audit discoveries (all verified against source or pinned evidence)

1. **llama.cpp v10342 measured/read from its exact source** (`~/.unsloth/llama.cpp` = build 10342; live runs of the pinned binary): E2B is **35 layers, only 15 own KV** (20 shared-KV layers skip K/V projection and KV writes — Antfly's lowering does the same, 13-op vs 16-op layers, parity confirmed); decode graph = 819 nodes/token in 2 compute CBs + 1 blit, first ~81 nodes committed early; graph reuse 161/162 tokens (enabled by padding n_kv to multiples of 256) but **all 819 nodes are still re-encoded every token**; one CPU-GPU sync per token; sampling ~27 µs. Concurrent encoder with **~686 memory barriers per token** (only 16% of nodes overlap) — at 9.2 ms/tok. **Barrier/dispatch minimization is NOT where llama.cpp's speed comes from**; this caps expectations for pure dispatch-overlap work (§15.3 P3) and its e2e bandwidth efficiency is only **~56% (E2B) / ~65% (E4B) of 273 GB/s** — there is ~2× roofline headroom above llama.cpp, so kernel work is how we eventually go *past* them.
2. **E4B pair-activation fusion never engages in production.** The pinned §14.4 campaign JSON shows `routes.pair_activation = 0` in every E4B sample (short and long) vs 8960/4480 on E2B (= 35 layers × tokens; the E2B zero-rows are the deliberate pair-off rollback lanes). The fusion is promoted device-wide for M4 (`metal_kernels.m:22849-22857`) and E4B dims pass the `%32` checks; the C-side eligibility (`:49546-49557`) can bounce to the split gate/up path with **no counter** (rc −2..−6 silent fallback). E4B's entire 1239 MB/tok gate/up stream runs the census-worst 67% route with ~84 extra dispatches/frame. Diagnosis-first item, est. −0.6 to −1.0 ms/tok on both E4B workloads (§15.3 P1b).
3. **E2B runs 100% of its Q4_0 traffic on untuned legacy-heuristic routes.** The M4 AUTO table covers exactly two shapes — E4B's FFN pair/down (`metal_kernels.m:14007-14015`). E2B (hidden 1536; FFN 6144 layers 0-14 / 12288 layers 15-34 — per-GGUF verified; down-proj on NR8_NSG2 by the `in>4096` heuristic) matches nothing and its shapes were never microbenched. The worst competitive case has had zero shape-tuning attention.
4. **The §12.3 attention-shape mystery (28.4 vs 69.9 GB/s, e2e-neutral) is resolved as an isolation artifact:** at 28.4 GB/s in-frame, E4B's 330 MB/tok of attention weights alone would take 11.6 ms of a 21.1 ms frame — arithmetically impossible. The solo microbench measures launch/occupancy latency of a 16k-thread dispatch, not the in-frame regime. Retire the mystery; measure in-frame rates only via dilution (`--ops-per-frame ≥64`) or route-algebra A/Bs.
5. **Measured on the M4 Pro flat testbed (KV≈580, split engaged, CV<0.2%):** pair-activation fusion is worth **+2.77%** here (ledger previously recorded 0.5–1% from the Air — correction); PLE model-proj Q8 staging **+1.10%**, and its marginal rate prices bulk weight streaming at **~323 GB/s** — big-stream bandwidth is at/above nominal peak, which constrains how much of the flat residual can be bulk-stream inefficiency and honestly deflates A3's central case (hence the S0 gate in P2); LM-head Q4_K repack −1.65% (still quality-blocked, unchanged).

### 15.3 Ranked plan (each item: expected gain, gates, rollback; sequence as numbered)

**P0 — Split-GQA floor: runtime-tunable, sweep, lower default (S; the round's headline).** Replace the `#define` with a runtime field read once at decode-runtime creation (`TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV`), consumed at `:19439`/`:19595`; add a `below_min_kv` counter at the currently-silent early-out; sweep {32, 64, 128, 256, 511} on both models; ship the empirical crossover (expected ~64 or lower). Extend the on-device split-GQA oracle below KV 512 (geometry sweep to KV 23) and add a threshold-discontinuity probe (~470 vs ~530 prompts must show no step). Expected: E2B short 71.6 → **84–87 tok/s** (65.9% → 77–80% of llama.cpp), E4B short 47.4 → **54–55.4** (75.3% → 86–88%); long lanes unchanged; the anomaly inverts to physical ordering. Pure route swap — bit-identical tokens expected (measured identical across forced flips). Rollback: env to 512 or `TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT`. Caution: do not combine sweep lanes with explicit `ENABLE_DECODE_GQA_SPLIT`/`*_VARIANT` overrides (arms the strict-failure rc −15 path).

**P0b — Speculative submit (S).** Under `ignore_eos`, commit frame k+1 immediately after encode (before the wait on k); general path: speculative submit + discard-on-EOS (the KV speculation/truncation contract already exists via `appendGeneratedToken`/`cancel_pending`). Expected: −0.287/−0.340 ms/tok on **all four** workloads (+1.9–2.5%). Gates: exact-token sha256s; lifecycle-trace bubble <0.1 ms on a control lane; an EOS-terminating unit test on the discard path. Rollback env.

**P1 — Boundary-cost calibration + byte census (S; information, prices P3/P4).** The two in-repo estimates of serial-encoder per-dispatch drain disagree 5× and llama.cpp's 686-barriers-at-9.2ms is counter-evidence to the high end. (a) `metal_q4_0_linear` sweep over `--ops-per-frame` {16..512} at fixed bytes → fit b_serial; (b) same sweep on a bench-only concurrent encoder + scoped barriers → b_concurrent; (c) one Instruments GPU-timeline capture of a production E2B frame (`TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS=1`) — **MTLCounterSampling dispatch-boundary is now confirmed unsupported on this M4 Pro** (supported=0; §12's Air-only attribution was wrong), Instruments is the only per-dispatch instrument; (d) GGUF tensor-table byte census to settle streamed-bytes/token (in-repo accountings disagree: 1394 vs 1662 MB E2B). Decision rule: b_serial ≥4 µs ⇒ P3 carries ≥2 ms upside, proceed aggressively; ≤1.5 µs ⇒ cap P3 at ~0.5 ms and shift weight to P2. Also re-census FFN kernel GB/s at Gemma4 shapes on this box (Air census may not transfer; the 323 GB/s PLE datum says bulk streams are already near peak).

**P1b — E4B pair-activation engagement + E2B AUTO sweep (S).** (a) Instrument the rc −2..−6 fallback (add the missing counter regardless of outcome), find why E4B never routes pair-activation, fix or record the reason; expected −0.6 to −1.0 ms/tok on both E4B workloads if engagement wins (byte-scaled from the measured E2B +2.77%). (b) Microbench E2B's six FFN/attention shapes across the portfolio, fold winners into AUTO (guarded ≤+2%; prior per-kernel e2e overrides were null). Gates: exact-token, route-profile floors (E4B pair dispatches ≈ 42×tokens in the candidate lane), interleaved A/B.

**P2 — A3 scales-plane relayout, staged behind a replay gate (S spike → L core).** S0 (1–2 days, no runtime change): add a scales-plane-split variant kernel + pack fn to the existing bit-exact replay harness (`termite_metal_a4b_common_q4_replay`, `pack_q4_0_tile4` infra) and run at Gemma4 shapes on this box, co-running the M4 Pro baseline re-census from P1. Promotion floor ≥1.15× kernel-level (the tile4 row-interleave half alone measured only 1.02–1.03× — the unclaimed value is the scales-plane separation; the Q6_K tail's aligned interior planes at 82–98% are the in-repo evidence layout matters). If S0 passes: production mmv + pair variants on the split layout, per-slot layout tag (PLE-Q8 plumbing as template), staging-time repack, AUTO fold. Expected if it holds: −0.4 to −0.8 ms/tok E2B, −1.3 to −1.9 E4B — **the one lever sized to put E4B long past llama.cpp** (18.65 → ~16.8; llama 16.39). Honest null risk: the 323 GB/s marginal-rate datum argues the M4 Pro may already stream near peak; S0 decides for two days' work.

**P3 — Concurrent planned dispatch + hazard-aware reorder, two-phase, watchdog-class (L; priced by P1).** All machinery exists: range-tracked per-dispatch read/write declarations + RAW/WAR/WAW counters, resource-scoped barrier emission forced-on under concurrent mode, a static DAG lowering with per-node `wave_index` that nothing consumes yet, and `TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH` (env alone can force the concurrent encoder; the DAG/reorder additionally needs the `configure_a4b_dag_scheduler`-equivalent wired for Gemma4 — today it gates on `qualified_a4b_model`, verified `metal_kernels.m:12485-12499`). Phase A (S-M): concurrent encoder + conservative range-tracked barriers, no reorder — cheap kill (the A4B precedent was net-negative 59.3 vs 60.9). Phase B: consume `wave_index` to cluster genuinely independent spans (QKV triple, q-rope‖k-rope, PLE-gate‖attention-input), merging ~150–300 of ~525 boundaries. Expected: 0.4–1.2 ms/tok central (both models, all contexts), capped by the llama.cpp counter-evidence; upside 2–3 ms only if P1 confirms the high boundary constant. **Full watchdog protocol** (`scripts/perf_watchdog_experiment.sh`, one experiment per boot, ≥300 s soak — metadata-only-barrier reset precedent stands). Exact-token gate catches any missed hazard deterministically.

**P4 — Dispatch-count fusion (M; re-price after P3's verdict to avoid double-counting).** Q4_0 fused-QKV epilogue (today hard-gated Q8_0-only at `:15471-15480`, `:15553-15559`; −30/−48 dispatches incl. the 0.22 MB attn_k/v occupancy disasters), PLE gate+activation epilogue collapse (−35/−42; the Q4_0 proj+rms_add fused kernel already exists at `:37032-37060`), seed+v_norm+k-rope cluster (−15/−24). **Mandatory constraint from the sumsq post-mortem:** the −12% was a kernel-quality regression (scalar nibble unpack, no sumy folding, threadgroup-memory round trip), not a fusion refutation — every new epilogue builds on the production sumy-folded masked-FMA body (`:6244-6254`), never the scalar-unpack body (`:6236-6242`). This also un-blocks §12's B2 deferral on the same basis. Per-merge rollback envs + A/Bs (pair-fusion pattern).

**P5 — Retire the floor entirely (M, after P0's sweep).** split_count==1 direct-write stage variant (skip the reduce at tiny KV; must be bit-identical to stage+reduce or it stays opt-in) so the split family serves KV=1 upward and `paged_1x` becomes pure fallback insurance; repair paged_1x's serial scan only if something blocks P0 (strictly dominated otherwise — but note the measured rollback cliff: split force-disabled = 24.8/20.7 tok/s at long context).

**Stretch probes (cheap, unordered):** controlled A/B of the fast-prepared-frame path (one uncontrolled pair this round showed the descriptor path *faster*, 87.9 vs 78.4 at KV 600 — free tok/s if real); cold-first-run mechanism (10–20% first-run penalty on paged-heavy cells, absent on split cells — powermetrics co-capture); cache the ~5 uncached per-token `getenv` calls in the decode admission path (hidden today, surfaces as frames shorten).

### 15.4 Projections and success criteria

Composed medians (each stage assumes the prior lands at its central estimate; llama.cpp fixed at §14.4):

| Stage | E2B short | E4B short | E2B long | E4B long |
|---|---|---|---|---|
| Today (§14.4) | 71.6 (65.9%) | 47.4 (75.3%) | 84.5 (81.1%) | 53.6 (87.9%) |
| P0 + P0b | ~86–89 (79–82%) | ~55–56 (87–89%) | ~86.7 (83%) | ~54.6 (89%) |
| + P1b(a) E4B pair | — | ~57–60 (91–95%) | — | ~56.5–57.8 (93–95%) |
| + P2 A3 (if S0 passes) | ~89–93 (82–86%) | ~61–65 (97–103%) | ~90–93 (86–89%) | **~59–65 (97–106%)** |
| + P3 (if P1 prices high) | ~93–105 (86–97%) | parity+ | ~93–105 (89–101%) | past |

- **P0 exit:** short ≤ long ms/tok on both models (anomaly inverted); E2B short ≥ 84, E4B short ≥ 54; token IDs byte-identical; below-floor counter = 0 on benchmark lanes; long lanes within noise.
- **Round exit (realistic):** E4B at/past llama.cpp on both contexts (needs P1b(a) + one of P2/P3); E2B ≥ 85% both contexts. E2B *parity* additionally requires P2 and P3 to land near their upper estimates — llama.cpp's 9.20 ms at 56% efficiency implies our 1394 MB/tok needs ~57% blended; we exit P0b at ~46%.
- Every item: exact-token sha256 gates vs the four pinned campaign values, fresh-process interleaved A/B (`benchmark_metal_gemma4_ab.py` v5), route-counter floors proving engagement, rollback env verified in-campaign, watchdog protocol where flagged, ledger entry with machine identity.

### 15.5 Process and ledger corrections (bind on the next round)

1. **Stage-timing correction:** `MTLCounterSamplingPointAtDispatchBoundary` is unsupported on the M4 Pro reference box too (`supported=0`), not just the Air — §12/§13.4/§14.5's "M4 Pro dispatch census" plan must use Instruments or route-algebra A/Bs (this round's method: forced-route cells on a flat KV testbed).
2. **Cross-session GPU mutex:** two independent agents ran GPU campaigns 24 s apart this round (no contamination, verified by timeline) — the one-GPU-job rule needs an enforced lock file, not convention.
3. **E4B GGUF durability:** the reference E4B lives only at `/private/tmp/antfly-gemma4-e4b-qat-b064/` (sha `676c3507…`); it must be re-staged under `~/.antfly` before P0's E4B lanes (a reboot orphans the pinned comparison).
4. **Documentation cleanup:** the missing §11 fragment was consolidated into this canonical plan; stale `GEMMA4_METAL_PERFORMANCE.md` links and obsolete Metal trace-flag names were corrected.
5. **Ledger value corrections:** pair-activation fusion = +2.77% on the M4 Pro (not the Air's 0.5–1%); PLE Q8 staging = +1.10% E2B here; the §1(e) "3-pass kv_1x" description is one dispatch with three barrier-separated internal phases.
6. **Telemetry gaps to close while touching these paths:** counter on the pair-activation rc −2..−6 fallback; counter on the split-GQA below-floor early-out; `TERMITE_METAL_TRACE_DISPATCH_PROFILE` appears inert on the planned-frame route.
7. **Binary archival:** none of the four pinned ledger binaries is archived anywhere on disk; archive release-candidate binaries (or record source→binary reproducibility) alongside the evidence dir.
8. **Trace perturbation warning:** `TERMITE_METAL_TRACE_FRAME_LIFECYCLE` distorts short-context runs badly (48.5 vs 84.6 tok/s median) — per-frame stderr lands on the critical path exactly when frames are cheap; never use it for absolute short-frame numbers, only for span structure.

## 16. Round-4 implementation and correction: qualified short-KV split policy (2026-08-28, M4 Pro reference box, uncommitted)

This round implemented the actionable part of §15, independently checked the two secondary claims, and hardened the benchmark contract. No commit or push was made.

### 16.1 Finding disposition

| §15 finding | Disposition | Production result |
|---|---|---|
| The fixed 512-token split-GQA floor causes the short-context anomaly | **Confirmed** | Replaced by a runtime-tunable, topology-qualified floor with route telemetry and rollback. |
| A 32/64-token floor is token-identical on both models | **Partially refuted** | E4B is exact at 32. E2B floors at or below 160 change the established stream at generated token 169, so E2B ships at the fastest exact floor, 192. |
| The generation loop leaves a 0.29/0.34 ms EOS-check submit bubble | **Refuted** | The runtime has already submitted the successor frame before control returns to the loop. The traced interval is the pipelined GPU wait; an added submit would duplicate an in-flight frame. No speculative-submit code or flag was retained. |
| E4B pair-activation fusion never engages | **Refuted** | The external analyzer hard-coded E2B's `nr4-nsg4` counter and ignored E4B's qualified `nr4-nsg2` lane. Live E4B runs report 10,752 pair dispatches per 256-token sample and zero pair fallbacks. |

### 16.2 Qualified policy and safety controls

`TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV` is read once when the decode runtime is created. A valid positive integer overrides the model policy; an invalid or zero value warns and falls back. The selected threshold and the formerly silent below-threshold misses are exposed in the runtime snapshot, logs, and benchmark JSON. Existing `TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT` remains the full rollback.

The qualified defaults are:

- Gemma4 E2B (8 query heads / 1 KV head): **192 tokens**.
- Gemma4 E4B (8 query heads / 2 KV heads): **32 tokens**.
- Qualified A4B geometries: **32 tokens**, with `TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT` as the model-specific rollback.

The C runtime now follows the same M4 pipelined-frame device default as Zig, and split-GQA frame scratch follows that policy. This removes a split-brain configuration in which Zig could pipeline while the C-side scratch/route policy remained opt-in.

The full single-run crossover sweep was:

| Floor | E2B tok/s | E2B established hash | E4B tok/s | E4B established hash |
|---:|---:|:---:|---:|:---:|
| 32 | 88.766 | no | 56.350 | yes |
| 64 | 88.612 | no | 56.177 | yes |
| 96 | 87.791 | no | 55.664 | yes |
| 128 | 85.906 | no | 54.889 | yes |
| 160 | 83.962 | no | 53.748 | yes |
| 192 | 80.732 | yes | 52.352 | yes |
| 224 | 78.359 | yes | 50.673 | yes |
| 256 | 74.440 | yes | 48.818 | yes |
| 320 | 72.419 | yes | 47.355 | yes |
| 512 | 71.729 | yes | 47.434 | yes |

The E2B quality boundary is real rather than a nondeterministic run: floors 32 through 160 share one alternate hash and first differ from the established 512 stream at generated token 169. A broader six-prompt × 256-token campaign was exact for E2B 192 versus 512 and E4B 32 versus 512 on all 12 paired cases; median throughput ratios were 1.1344 and 1.1886 respectively.

### 16.3 Final strict performance result

Protocol: 23 prompt tokens, 256 requested output tokens, greedy, F16 KV, EOS ignored, one warmup, six interleaved AB/BA fresh-process pairs, two-second cooldown. The strict metric is `(output_tokens - 1) / decode_inner_seconds`, matching llama.cpp's 255 decode evaluations; the harness now records this definition in schema v8.

| Model | 512 floor tok/s | Qualified default tok/s | Gain vs 512 | CV | Fresh llama.cpp tok/s | Antfly / llama.cpp |
|---|---:|---:|---:|---:|---:|---:|
| E2B | 71.539 | **80.722** | **+12.84%** | 0.276% | 108.470 | **74.42%** |
| E4B | 47.292 | **56.069** | **+18.56%** | 0.342% | 62.980 | **89.03%** |

Both qualified lanes won 6/6 pairs and reproduced the established 256-token hashes (`94dd3a2a…` E2B, `0262c06e…` E4B). E2B 192 recorded 3,080 split calls, 5,880 below-floor calls, and zero prepared-frame/pair fallbacks per sample; E4B 32 recorded 10,416 split calls, 336 below-floor calls, 10,752 `nr4-nsg2` pair calls, and zero fallbacks. This closes most of the anomaly but deliberately does not claim §15's projected E2B 84–87 tok/s: the faster floors failed the pinned greedy-quality contract. E4B short is now faster per token than its pinned long lane; E2B short remains about 4.7% slower than its pinned 84.498 tok/s long lane because its exact-output floor stays at 192.

The llama.cpp refresh used five fresh processes with the pinned v10342 (`38278078c`) binary: E2B median 108.470 tok/s at 0.241% CV and E4B median 62.980 at 0.213% CV. The earlier pinned long-context numbers were not rerun because both the 512 and qualified policies select split-GQA throughout KV≈2,334: Antfly/llama.cpp remain 84.498/104.190 E2B and 53.632/61.010 E4B from §14.4.

### 16.4 Verification and evidence

- Pinned Zig 0.16.0 ReleaseFast Metal build/link passed with `-j1`. Campaign binary SHA-256: `7567166aaffcf2a40ef838cd8d1cf54077c4563f58c14adddc69c30f5a55a598`; its pre-hardening exact-source rebuild was `1381c281d2cf47cba075a91c141b730adaaca18398b7c6ced357ea950c6c5e93`. Those two Mach-O layouts and 20,449,220-byte executable `__text` sections are byte-identical; their full-file difference is the test-source fingerprint, UUID/link metadata, and ad-hoc signature. The final hardened binary is `30ebf20777b57e2ba77ddda97d94d22956d9aed4206a0021185820b5a81b2cfb`; it adds only decimal validation in the once-per-runtime floor parser, so the decode throughput campaign remains representative.
- The on-device split-GQA oracle passed 1,692 schedule-policy cases plus real tensor checks for E2B, E4B, and A4B, including KV 23 for q_len 1 and 2, both qualified boundaries, all schedule variants, ragged pages, ring wrap, long context, explicit floors, invalid override fallback, and disable rollback. Maximum short-case absolute error was below `2.4e-5` against a `1e-2` limit.
- Focused Zig policy and telemetry tests passed; the aggregate benchmark-contract suites passed 80/80 (43 core, 23 A/B, 14 LM-head quality). The schema-v8 harness derives prefill row buckets from the observed prompt length, exposes explicit split rollback profiles (including the E2B pair-decode composition), independently pins the M4 A4B default, and requires the selected floor, below-floor count, and repack sampling-rejection telemetry in both logs and JSON.
- The review-remediation exact-Zig-0.16.0 ReleaseFast Metal binary has SHA-256 `1747b6bce4d637f504d9ff6eee0f6d1bf12ece6c2e918fd25520a49aa4845e79`.
- E2B final summary: `/private/tmp/gemma4-final-qualified-e2b-v2/summary.json`, SHA-256 `80ee499ed498c868afa6e0fa77cf51132d80cb6a80c8d725818db567348fab2b`.
- E4B final summary: `/private/tmp/gemma4-final-qualified-e4b-v3/summary.json`, SHA-256 `5d873fa90686a202c43cf99864e4d0bd4a26fd49d7b806fbaf12aaa5139acc60`.
- Floor sweeps: `/private/tmp/gemma4-split-floor-e2b-256-v1/` and `/private/tmp/gemma4-split-floor-e4b-256-v1/`; broader exact-output campaign: `/private/tmp/gemma4-split-floor-quality-v1/`; fresh llama.cpp logs: `/private/tmp/gemma4-final-llama-short-v1/`.

These are strong PR diagnostics, not immutable release evidence: the final artifacts remain under `/private/tmp`, and the machine entered this round with existing swap allocation. The pristine-zero-swap and retained/hosted-evidence promotion gates from §14.5 remain open.

### 16.5 Competitive next target

P0 is ready for review. P0b and the proposed E4B pair-engagement work should be removed from the queue. The next bounded experiment remains §15 P1: Instruments boundary-cost calibration plus a model-byte census. That result should choose between the A3 scales-plane replay spike and watchdog-gated concurrent dispatch/reorder; neither should be promoted without the existing exact-output, route, thermal, and watchdog gates.
