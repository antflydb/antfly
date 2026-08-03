# Review Handoff — CUDA Gemma 4 E2B prefill optimization arc

Branch: `quant-kernel-long-context-cuda`. Target: Gemma 4 E2B QAT (Q4_0), SM89 / NVIDIA L4, CUDA 13.2.
Goal: close the prefill/TTFT gap vs llama.cpp (decode is already within ~4%; TTFT ~2.26× is the whole gap).

This document scopes the review to the CUDA prefill work landed in this arc. The branch has 41 commits / 251 files
vs main (much is prior work: SDK codegen, pipelines, KV, scheduler). The reviewable units below are what this arc added.

---

## TL;DR for the reviewer

- **Net shipped result:** frontier (`gemma4-e2b-sm89-flash-splitk-v1`) paired vs llama.cpp = **total ratio 1.170, TTFT 740 ms / 110 tok/s vs 328 ms / 114 tok/s** (unchanged from baseline; decode within 4%). Promotions are net-neutral-or-positive and verified.
- **Two promotions to production defaults (SM89):** (1) flash-prefill attention automatic default; (2) a ~25-knob DP4A/LM-head/capture tuning battery; (3) W4A16 bf16 tensor-core prefill projections (default-on, but DEFERS to the faster cuBLASLt mirror when present).
- **New tooling:** non-perturbing per-op CUDA profiler (was adding ~390 ms/35% of measured prefill via per-op blocking sync; now ~0.8%), wired to CLI + server; a bf16 tensor-core Q4_0 kernel.
- **Verification:** `zig build test -Dcuda=true` = 2546 passed / 98 skipped / 0 failed; 307 Python tests; `quant-kernel-local-check`, `cuda-artifacts-check` green; artifacts regenerated (CUDA 13.2 sm89).
- **1 file uncommitted:** `src/ops/cuda/cuda_compute.zig` (the W4A16→mirror defer fix — REQUIRED; without it W4A16 regresses the frontier).

---

## Change areas + review focus

### 1. W4A16 Q4_0 tensor-core prefill kernels (CORRECTNESS-CRITICAL)
- **What:** added the Q4_0 arm to the existing WMMA engine `termite_qtc_hmma_tile` (previously Q8_0/Q4_K only) in `src/ops/cuda/artifacts/inference_cuda_kernels.cu`. New device entry points `termite_linear_q4_0_{,bias_,bias_gelu_,bias_add_}f32_tc_hmma` (f16) and `..._bf16` (bf16 fragments), plus a strided `termite_activation_multiply_fused_gate_up_f32`. New host layout `packQ4_0TensorCore` + `q4_0_hmma` enum in `cuda_compute.zig`; fn-ptr table + `launchLinearQ4_0TcHmmaF32`/`...Bf16F32` in `kernels.zig`.
- **Why:** the FFN gate/up projection was ~52% of prefill running on DP4A (SIMT int8), not tensor cores.
- **Review focus:**
  - The `q4_0_hmma` pack layout contract (scales region then quants region, 18 B/block, nibble order) — `packQ4_0TensorCore` in cuda_compute.zig vs `termite_q4_0_tc_value_at` in the .cu must agree byte-for-byte.
  - The bf16 variant templating: `termite_qtc_hmma_tile<MODE, FMT, WmmaElem=half>` + `termite_qtc_tile_ops<T>` specialization. **Claim to verify: the f16 path is byte-identical (SASS diff of all 12 pre-existing kernels reported identical).** Confirm no behavior change to Q8_0/Q4_K.
  - **`#if __CUDA_ARCH__ >= 800` guards on the 4 bf16 kernel bodies** — bf16 WMMA fragments are an incomplete type on sm_75; without the guard the multi-arch fatbin (compute_75/80/89/90) fails to compile. Verify all 4 are guarded.
  - Evidence: prototype `src/ops/cuda/prototypes/q4_0_tc_hmma{,_bf16}_sm89.cu` (GPU-run, max_abs ~1e-4 f16 / bf16 finite-on-overflow); microbench `--q4-0-tc-hmma-e2b` in `src/bench/cuda_microbench.zig`.

### 2. W4A16 promotion + mirror-defer (CORRECTNESS-CRITICAL, 1 file UNCOMMITTED)
- **What:** `q4_0_tc_hmma_prefill` promoted default-on for compute 8.9 in `CudaTunedRouteGates.promotedDefaultsForTarget(8,9)`. Routed via a leading guard in `linearNoBias`, `linearNoBiasPair`, and a defer in `linearNoBiasQkv`.
- **THE fix (uncommitted):** all three guards now include `weightBf16MirrorForRows(...) == null` / `!use_hybrid_bf16` so W4A16 **defers to the BF16 mirror** when present.
- **Why:** measured ordering (2051 tok): cuBLASLt mirror 3284 ms < W4A16 3766 ms < DP4A 6419 ms. W4A16 beats DP4A but loses to the mirror; the initial promotion wrongly took priority over the mirror and regressed the frontier (ratio 1.72 → fixed back to 1.170).
- **Review focus:** confirm the defer condition is correct in all 3 sites (mirror present ⇒ W4A16 skips ⇒ mirror branch runs); confirm rows>1 gating (decode rows==1 unaffected); rollback env `ANTFLY_INFERENCE_CUDA_Q4_0_TC_HMMA_PREFILL=0`.
- **Quality evidence:** greedy 96-tok vs F32-activation reference — W4A16 96/96 (100%), DP4A 30/96 (31%). W4A16 is higher quality than the old default; the mirror is equal quality to W4A16 and faster (hence defer).
- **VRAM note / possible follow-up:** with the mirror on, the W4A16 pack (+~1.8 GB) is still created at upload but unused (mirror wins). A follow-up could skip `packQ4_0TensorCore` when a mirror will attach. Not a correctness issue; frontier fit in 23 GB.

### 3. Non-blocking per-op profiler (CORRECTNESS-CRITICAL: event lifecycle)
- **What:** `src/ops/cuda/context.zig` split the end path into `recordProfileEnd` (async) + `readProfileEventPairUs` (no sync); `cuda_compute.zig` added a lazily-allocated `CudaProfilePool` (cap 8192, free-stack + pending ring) drained by ONE `cuStreamSynchronize` in `drainCudaProfile`. 4 new buckets: rope, kv_write, elementwise, embedding. Drain wired in `native_generate.zig` + `server.zig` (both the direct path AND the SSE `streamGenerate` path).
- **Why:** the old per-op `cuEventSynchronize` added ~390 ms (35%) to measured prefill and broke async overlap; ~53% of prefill was unattributed.
- **Review focus:**
  - Event pool lifecycle: no double-free, no use-after-recycle, ring-full mid-request drains correctly, disabled-profiling path allocates nothing (zero overhead).
  - Non-nesting assumption: per-category sums are only valid if profiled scopes don't overlap on the stream. The code preserves a pre-existing intentional nest (`.staging` inside `.bf16_linear`) via distinct slots — verify the new buckets don't introduce accidental nesting/double-count.
  - `streamGenerate` emit uses `pipeline.session` which is `?Session` (optional) — must unwrap (it does); the direct path uses non-optional `model.session`.
- **Evidence:** profiler ON now adds 0.8% (was 35%); attribution ~88%. New GPU-free unit test in cuda_compute.zig.

### 4. Flash-prefill promotion + knob battery (committed earlier this arc)
- **What:** flash-prefill CUDA artifacts flipped `production_enabled`/`runtime_default_enabled` (`src/graph/quant_kernel_compiler.zig`), new `automatic` `GqaPrefillProfile` unset-default (`kernels.zig`), catalog/renderer, release gate `FROZEN_PROFILE` de-pin+scrub (`scripts/gemma4_cuda_l4_release_gate.py` + test). Knob battery → `CudaTunedRouteGates` (SM89-scoped code defaults, env-overridable both ways). cuBLASLt `sm89-prefill` tuning default-on (rows≥32).
- **Review focus:** the automatic-mode fallback (unset ⇒ attempt flash, silently fall back to prior `off` behavior when ineligible; `off` = rollback); the SM89-only scoping of the knob defaults (non-8.9 targets get historical defaults — verified by `std.meta.eql` tests).
- **Evidence:** `quant-kernel-cuda-paged-prefill-diff` 90 cases bitwise-identical (in CI); pinned test counts updated for +2 promoted artifacts.

### 5. Fused gate/up (SHIPPED but DEFAULT-OFF — reviewer can lightly scrutinize)
- **What:** `tryFusedGateUpBf16` in `runGatedFfnResidualOp` (concat gate+up BF16 mirrors → one cuBLASLt GEMM → strided act-multiply), gated `fused_gate_up_bf16` (default off).
- **Why NOT promoted:** measured NOT a win (+329 ms; equal FLOPs, negligible launch-overhead savings at prefill scale, one-time concat build). Correct + identical output, but no speedup → stays opt-in.

---

## Verified findings (no code — context for the reviewer's judgment)
- **Prefill is GPU-bound** (nsys: 99.7% device utilization). The earlier "46% host overhead" was un-bucketed GPU kernels, not host idle. Do not chase host overhead.
- **Flash attention is NOT the bottleneck** (~7-10% of real prefill; Phase-0's "59%" was the blocking-profiler artifact). gqa2 head-sharing prototype is bitwise-identical to the production flash but ~2% SLOWER (L2 already absorbs the "redundant" KV) — a dead end.
- **Projections are already optimal on the frontier** (cuBLASLt mirror). The remaining TTFT gap vs llama.cpp is in non-projection prefill work — now measurable via the fixed profiler on the mirror-path frontier config.

## Open decisions for the reviewer / owner
1. **Mirror-as-default?** On 24 GB L4 the cuBLASLt mirror is the fastest projection path (+3.6 GB). Should it be the SM89 default instead of W4A16 (mid speed, +1.8 GB)? Current promotion makes W4A16 the default and defers to the mirror when the user opts into it.
2. **Split-K decode** stays deferred (token drift after output token 136; decode already within 4%).
3. **Exact-output policy vs quantized reality:** the gate/up default (DP4A q8_1, and now W4A16 bf16) is already non-bitwise-vs-f32; W4A16 is *more* accurate (96/96 vs F32). Confirm this reframing is acceptable for the promotion gate.

## How to re-verify
- Build: `zig build install -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast` (zig 0.16.0; CUDA 13.2; driver can't JIT 13.2 PTX so sm89 cubin is used).
- Tests: `zig build test -Dcuda=true` (2546), `python3 -m unittest discover -s scripts -p 'test_*.py'` (307), `zig build quant-kernel-local-check`, `zig build cuda-artifacts-check`.
- Paired benchmark: `python3 scripts/benchmark_gemma4_long_e2e_server.py --model <gguf> --cuda-execution-profile gemma4-e2b-sm89-flash-splitk-v1 --collect-only` (llama.cpp `llama-server` at /tmp/llama.cpp; `passed=false` is only the split-K determinism check, collect-only). ALWAYS use the warm-server harness for frontier absolute numbers, not the CLI (CLI lacks graph-capture/prewarm and reads ~4× slower).
- W4A16 microbench: `./zig-out/bin/antfly-inference bench-cuda --q4-0-tc-hmma-e2b`.
