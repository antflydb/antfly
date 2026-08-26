# Gemma 4 26B-A4B Performance Analysis

## 2026-08-25 frame-owned split-GQA and HD256 flash-prefill follow-up

**Status**: frame-owned split-GQA scratch is promoted in the opt-in A4B
high-memory bundle and closes the prepared long-context cliff.  A4B local
HD256 flash-prefill is implemented and tensor/route qualified, but remains an
explicit Class-C candidate pending the agreed transcript re-baseline and human
sign-off; the umbrella alone does not activate it.

### Frame-owned split-GQA scratch: promoted

The prepared decoder can encode frame N+1 while frame N is submitted.  The old
single private split-K scratch buffer therefore rejected every N+1 dispatch and
fell back to paged-1x until the submitted frame retired.  The runtime now owns
two private scratch slots.  `begin_frame` selects the slot not owned by the
submitted frame, `submit_frame` transfers active-slot ownership, `wait_frame`
retires only the submitted slot, and cancellation retires only the active slot.
Reset waits for an exceptional orphaned submitted frame before making either
slot reusable.  The maximum incremental allocation is 2,105,344 bytes.

Admission is part of `TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH`; rollback
is `TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT_FRAME_SCRATCH=1`.  The parity
runner now fails closed unless a long prepared run records exactly 30 split-GQA
dispatches per encoded output frame and no decode-time paged fallback.

The balanced three-pair lane used a 618-token prompt and generated 128 tokens:

| Lane | Median tok/s | Relative to candidate | Attention dispatch contract |
|---|---:|---:|---|
| Compiled control | 49.474 | 82.70% | split 3,810; paged local prefill 25 |
| Prepared + frame-owned scratch | **59.821** | 100% | split **3,840**; paged local prefill 25 |
| Prepared single-scratch rollback | 34.709 | 58.02% | split 30; paged fallback 3,835 |
| llama.cpp | 66.350 | 110.91% | pinned comparator |

The candidate is **1.7235x** the rollback, **1.2091x** the compiled control,
and **90.16%** of llama.cpp.  It wins 3/3 pairs; candidate CV is 0.31% and
rollback CV is 0.28%.  Every Antfly lane and run shares token hash
`2d566c990f13ae359a6d89b20225c26208ea8f37e82498fd9cd00bbfc7069330`.
All candidate runs record split/paged counts of 3,840/25; every rollback records
30/3,835.  This is a scheduling/ownership Class-A promotion with an exact
real-model token gate.

### A4B local HD256 flash-prefill: qualified, explicit-only

The existing generated Metal flash kernel now admits the exact A4B local shape
`16 Q heads / 8 KV heads / head_dim 256 / sliding window 1024` behind
`TERMITE_METAL_ENABLE_A4B_FLASH_PREFILL_HD256=1`.  Its dedicated disable and
the global generated-flash disable both win.  It is intentionally not inherited
from the high-memory umbrella until Class-C sign-off.

The generated-kernel oracle adds a 16-query, 1,040-KV, reversed-page fixture
that crosses the first 1,024-token sliding-window boundary.  It passes with
maximum absolute error `1.1e-6` over 25 measured iterations.  The short
real-model gate records exactly 25 local HD256 flash calls plus five existing
HD512 global calls, preserves the three-lane 64-token hash, and has no route
fallback.

On the 618-token prompt, explicit flash routes all 25 local layers and changes
the generated sequence at token index 3 relative to both flash-off lanes.  The
flash-on/off hashes are respectively
`5096b8cbe01eb169767333d2787d8fc4bc7a7426a2a3dd6751238f03934831fc`
and `2d566c990f13ae359a6d89b20225c26208ea8f37e82498fd9cd00bbfc7069330`.
One paired measurement improves full prefill from 9,523 to 9,413 ms (1.16%),
which is below the 2% promotion floor and is not enough evidence to re-baseline
tokens.  Promotion therefore remains blocked on the pre-agreed eight-prompt
external-quality gate and human transcript sign-off, not on kernel correctness
or routing coverage.

### Verification and artifacts

- ReleaseFast Metal build with Zig 0.16 and `-j1`: PASS.
- Focused Metal policy tests: 2/2 PASS.
- Python benchmark/profile contracts: 35/35 PASS.
- Generated Metal runtime check, including the A4B flash boundary fixture:
  PASS.
- Split-GQA tensor oracle: all four schedules pass both A4B local/global shapes;
  worst error `1.3e-6` against the `1e-2` ceiling.
- `git diff --check` and Python syntax checks: PASS.

Machine-local evidence:

- `/private/tmp/gemma4-a4b-frame-scratch-long-3pair-final-20260825/`
- `/private/tmp/gemma4-a4b-frame-scratch-long-final1-20260825/`
- `/private/tmp/gemma4-a4b-flash-hd256-explicit-short-final1-20260825/`
- `/private/tmp/gemma4-a4b-flash-hd256-explicit-long-final1-20260825/`

---

## 2026-08-25 route, pipeline, concurrency, and LM-head hill climb

**Status**: the promoted high-memory lane is exact and materially faster, but
the llama.cpp throughput target is **not met**.  The final strict three-prompt
gate measures **60.1145 tok/s** for Antfly versus **68.1200 tok/s** for the
pinned llama.cpp comparator: **88.25% parity**.  All nine candidate/rollback
pairs win and all Antfly token hashes match, but the 95% aggregate and 90%
per-prompt throughput gates remain red.

This pass started from the 42.4749 tok/s prepared-executor result below and
implemented the measurement, route-selection, execution-policy, dispatch
consolidation, Q4_0 schedule, and LM-head phases.  The largest accepted change
is an exact one-SIMD-group route selector: its adversarial replay preserves IDs
and weights bit-for-bit while improving the original serial selector from
216.738 to 27.800 us/layer.  A register-resident follow-up removes the eight
per-slot threadgroup barriers and reaches 18.852 us/layer.  Together with the
pipelined decode frame, A4B-only concurrent hazard tracking, and the winning
Q6_K LM-head row width, the real model is now roughly 1.42x faster than the
42.47 tok/s starting point.

### Promoted high-memory bundle

The following winners are members of
`TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH`; the umbrella remains opt-in
and its default-on decision is still separate.

- `termite_moe_route_select_tg_register` keeps four probabilities and used
  bits per SIMD lane.  It retains the qualified serial max, denominator,
  division, tie-break, and selected-weight accumulation order.  Rollback:
  `TERMITE_METAL_DISABLE_A4B_ROUTE_SELECT_REGISTER=1`.
- The prepared A4B executor uses the existing range-declared planned encoder
  with concurrent dispatch and conservative buffer-scope barriers.  Admission
  is model-qualified and prepared-executor-only.  Rollback:
  `TERMITE_METAL_DISABLE_A4B_CONCURRENT_HAZARD=1`.
- The Q6_K vocabulary head uses NR4/NSG1 for the exact A4B
  `2816 x 262144` shape.  This is Class B: nine-pair token hashes hold, while
  the older NR2 reduction order remains behind
  `TERMITE_METAL_DISABLE_A4B_LM_HEAD_NR4_NSG1=1`.
- The previously promoted pipelined frame remains in the umbrella with
  `TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME=1` as its rollback.  The A4B
  prepared executor itself still requires
  `TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE=1`.

The expanded-bundle promotion campaign records 60.752 tok/s candidate versus
58.065 tok/s with all three new rollbacks, a 1.0463x ratio, 9/9 wins, stable
hashes, and CV below 3%.  The no-individual-enable umbrella contract measures
61.644 versus 58.934 tok/s (1.0460x) and observes every required marker.

### Final strict parity result

| Runtime | Median tok/s | ms/token | Relative to llama.cpp |
|---|---:|---:|---:|
| Antfly compiled control | 51.1364 | 19.555 | 75.07% |
| Antfly prepared, three-feature rollback | 57.4294 | 17.413 | 84.31% |
| Antfly promoted high-memory bundle | **60.1145** | **16.635** | **88.25%** |
| llama.cpp | 68.1200 | 14.680 | 100% |

Per-prompt promoted medians are 60.1145, 60.1719, and 59.8291 tok/s;
matched llama.cpp medians are 68.34, 68.36, and 68.05 tok/s.  The resulting
ratios are 87.96%, 88.02%, and 87.92%.  Candidate CV is 0.40%, the candidate
wins 9/9 rollback pairs, all three Antfly lanes share the expected token hash
per prompt, prepared coverage is complete, and mapped/Q4/frame fallbacks are
zero.  A favorable canonical-prompt contract reaches 61.644 tok/s and 90.16%
parity, but it is not used as the release verdict because it does not
generalize across the three-prompt gate.

The long-context lane uses an observed 661-token prompt and generates 128
tokens.  The promoted candidate measures 33.642 tok/s versus 32.757 tok/s for
its three-feature rollback (1.0270x), with exact hashes and candidate CV below
0.1%, so the change-specific <=2% regression gate passes.  However, the
non-prepared compiled control reaches 49.244 tok/s and llama.cpp reaches 65.80
tok/s.  The feature bundle is not the source of that cliff, but prepared
long-context execution remains a separate production blocker and must not be
hidden by the candidate/rollback PASS.

### Post-promotion ledger and rejected paths

The reconciled three-frame profile contains 1,902 operation records and 666
dispatches/token.  Median profiled frame time is 17.274 ms.  The largest
per-token buckets are the LM head (3.151 ms), routed gate/up (2.511 ms), shared
FFN (1.968 ms), routed down (1.313 ms), attention Q/O projections (2.462 ms),
router projection (0.956 ms), and route top-k (0.672 ms).  Concurrent mode
records 182 conservative range-triggered barriers/token; resource-barrier DAG
mode regresses to 59.322 versus 60.870 tok/s and remains disabled.

No other experiment cleared its promotion contract:

- NR1 LM head regresses 3.45%; NR4/NSG2, NR6/NSG1, and NR8/NSG1 do not beat
  NR4/NSG1.  NR4/NSG1 itself is a stable 1.0127x incremental gain with 9/9
  wins and is promoted only as part of the aggregate 1.0463x bundle.
- The exact selector v2 reaches 18.084 us/layer but saves only about 0.023
  ms/token over the promoted register selector, so it remains experimental.
- Route-slot-map folding never activates on the production prepared path,
  which already consumes expert IDs directly; its apparent one-pair change is
  noise and it is not promoted.
- Routed/shared NQ8 schedules, fused MoE activation/reduce epilogues,
  selective FFN interleave, unretained command buffers, and the router SIMD
  projection are slower in real-model rollback tests.
- Packed QKV + direct KV write + embedding-scale fusion regresses as a batch
  (60.000 versus 61.047 tok/s).  Therefore the approved Class C re-baseline
  event has no performance winner, no hashes were re-pinned, and no human
  transcript sign-off is requested.

### Verification and artifacts

- ReleaseFast Metal build: PASS with Zig 0.16, `-j1`.
- Python profiler/parity/benchmark tests: 35/35 PASS; syntax checks and
  `git diff --check`: PASS.
- Exact route replay: 24 adversarial cases, exact IDs and weights, 11.50x for
  the promoted register selector versus the original serial selector.
- Final strict parity command exits nonzero only on the throughput gates; its
  execution, marker, exact-token, residency, prepared-frame, rollback, and
  fallback contracts pass.

Machine-local evidence:

- `/private/tmp/gemma4-a4b-register-concurrent-bundle-9pair-20260825/`
- `/private/tmp/gemma4-a4b-lm-head-nr4-nsg1-9pair-20260825/`
- `/private/tmp/gemma4-a4b-expanded-bundle-9pair-20260825/`
- `/private/tmp/gemma4-a4b-expanded-umbrella-contract-20260825/`
- `/private/tmp/gemma4-a4b-post-register-concurrent-profile-20260825/`
- `/private/tmp/gemma4-a4b-expanded-parity-final-20260825/`
- `/private/tmp/gemma4-a4b-expanded-long-context-700tok-20260825/`

---

## 2026-08-24 prepared-executor and full-roofline follow-up

**Status**: prepared whole-model execution PASS behind its A4B opt-in; exact
tokens and route coverage PASS; packed common-Q4 and concurrent-DAG promotion
FAIL; speculative target verification remains fail-closed; llama.cpp parity
promotion FAIL.

This pass deliberately changed optimization strategy after the row-one kernel
search plateaued:

1. A fail-closed full-operation Metal ledger now covers the model embedding,
   all 30 decode layers, KV writes and paged attention, every routed and shared
   FFN phase, the final norm, LM head, and argmax. It records shapes, logical
   bytes, dispatch labels, barriers, layer kind, and KV position.
2. A dev replay tests an offline packed `[tile4][block][row][18]` Q4_0 layout
   for the common gate/up and down matrices.
3. A model-specific seven-node command DAG declares resource dependencies and
   lowers them into five execution waves with an opt-in concurrent scheduler.
4. Explicit compiled whole-model A4B requests may use the backend-owned
   prepared executor only when
   `TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE=1` is set and the loaded model
   passes the exact A4B runtime geometry check. Other compiled requests retain
   the existing executor.
5. Draft and target state are isolated for speculative decoding. The A4B
   target rejects multi-token verification before mutating target KV state
   until that target path is genuinely qualified.

### Balanced end-to-end result

The v3 parity runner uses three prompts, three fresh-process runs per prompt,
and balances the order of three Antfly lanes before comparing the same Q4_0
GGUF with llama.cpp. It validates exact Antfly token hashes, executor markers,
63/63 prepared frames when requested, zero prepared/frame/mapped/Q4 fallbacks,
model-wide residency, and the selected rollback policy.

| Runtime | Median tok/s | ms/token | Relative to llama.cpp |
|---|---:|---:|---:|
| Antfly default compiled control | 39.2824 | 25.457 | 57.73% |
| Antfly prepared, specialized-ID rollback | 41.6530 | 24.008 | 61.21% |
| Antfly prepared + specialized ID | **42.4749** | **23.543** | **62.42%** |
| llama.cpp | 68.0500 | 14.695 | 100% |

Prepared execution is an **8.13%** improvement over the true no-prepared
compiled control. The exact specialized-ID kernels add another **1.97%** over
the prepared rollback. Per-prompt candidate/llama.cpp ratios are 62.29%,
62.50%, and 62.17%; candidate medians stay in a narrow 42.249--42.532 tok/s
range. The remaining median gap is 25.575 tok/s, so the 95% aggregate and 90%
per-prompt parity gates correctly remain red.

`/usr/bin/time` reported median maximum RSS of 713 MiB for the default Antfly
control, 594 MiB for both prepared lanes, and 14,320 MiB for llama.cpp. This is
not a substitute for allocation accounting: Antfly's Metal telemetry reports a
14,937,423,872-byte model-wide residency set backed by no-copy mapped storage.

### Full-operation profile

The final compiled-request profile is routed through the same single-owner
prepared executor as the candidate above. Three sampled decode frames cover KV
positions 37, 53, and 69, all 25 local and five global layers, and 1,902
operation records. Operation time plus unattributed time reconciles exactly to
73.277 ms of frame GPU time; unattributed time and observed barriers are both
zero.

Largest aggregate attributions across the three frames are:

| Operation | Layer kind | GPU time | Occurrences | Effective bandwidth |
|---|---|---:|---:|---:|
| Router top-k selection | local | 18.254 ms | 75 | n/a |
| LM head | model | 9.748 ms | 3 | 186.69 GB/s |
| Routed gate/up | local | 6.288 ms | 75 | 214.43 GB/s |
| Shared FFN | local | 4.799 ms | 75 | 157.47 GB/s |
| Router top-k selection | global | 3.650 ms | 15 | n/a |
| Routed down | local | 3.238 ms | 75 | 209.23 GB/s |
| Attention Q projection | local | 2.780 ms | 75 | 175.78 GB/s |
| Attention output projection | local | 2.652 ms | 75 | 184.29 GB/s |
| Router projection | local | 2.367 ms | 75 | 46.06 GB/s |

The ledger uses whole-frame GPU time apportioned by per-encoder counter ticks;
it does **not** claim direct per-operation GPU timestamps. These numbers are a
prioritization signal, not standalone kernel microbenchmarks. They nevertheless
change the next optimization target: route selection accounts for about 7.30
ms/token in this instrumented path, while the main routed Q4 reads are already
near the machine's practical bandwidth. The next hill climb should first
remove route-selection encoder/dispatch overhead or fuse routing with its
consumer, then remeasure with direct Metal counter samples if available.

### Experiments not promoted

- The packed common-Q4 replay is exact, but gate/up improves only 1.0282x,
  down 1.0142x, and their combined GPU time 1.0228x. That misses its 1.15x
  promotion floor, so production continues to use the existing layout.
- The concurrent A4B DAG preserves exact tokens and completes all prepared
  frames with no fallbacks, but measures 42.000 tok/s versus 42.453 tok/s for
  its same-binary rollback (-1.07%). An earlier reverse-order median was
  -1.46%. It remains only behind
  `TERMITE_METAL_ENABLE_A4B_DAG_SCHEDULER=1`; the default and high-memory
  umbrella do not enable it, and
  `TERMITE_METAL_DISABLE_A4B_DAG_SCHEDULER=1` is the rollback.
- A real E2B draft reaches the A4B target verification boundary, where the
  target fails with `Gemma4A4bMetalSpeculativeVerificationNotQualified`
  before target KV mutation. This validates isolation and failure semantics,
  not speculative speedup.

### Verification and artifacts

- Final `ReleaseFast` Metal inference binary built successfully from this
  source; SHA-256
  `3f1df1b1990a9ebfd88bb58e19c3e04c26bbd837f8e72a98531fcba1b3ae8b39`.
- Profiler/parity contract tests: 18/18 PASS.
- Baseline/candidate benchmark contract tests: 17/17 PASS.
- Python syntax checks and `git diff --check`: PASS.
- Focused planner tests: 37/37 PASS; focused speculation tests: 4/4 PASS.
- The final 3-prompt x 3-run parity command exits nonzero by design because
  the llama.cpp parity checks fail; all execution, exact-token, route,
  fallback, memory, prepared-coverage, and rollback validations pass.
- A later focused Zig test retry hit Zig 0.16 compiler `SIGSEGV`/`SIGBUS`
  without a source diagnostic. The already-built final binary and live route
  evidence are retained; this toolchain failure is not recorded as a test
  pass.

Machine-local evidence is retained at:

- `/private/tmp/gemma4-a4b-prepared-parity-final-20260824/`
- `/private/tmp/gemma4-a4b-roofline-20260824-final8/`
- `/private/tmp/gemma4-a4b-dag-current-20260824/`
- `/private/tmp/antfly-a4b-e2b-speculative-validation-20260824.log`

---

## 2026-08-24 exact expert-id Metal follow-up

**Status**: exact-geometry kernel correctness PASS; specialization retention
PASS; dense-model non-regression PASS; llama.cpp parity promotion FAIL.

This pass implemented the five-step routed-MoE optimization plan without
changing the public inference contract:

1. A selected-layer Metal timestamp profiler separates routed gate/up,
   activation, down, and reduction while folding those buckets back into the
   stable coarse FFN timing ABI.
2. A dev-only replay executes the exact A4B Q4_0 shapes: 128 experts, top-8,
   hidden 2816, intermediate 704, merged gate/up output 1408, and down output
   2816.
3. Two exact-shape `MUL_MV_ID` kernels remove dynamic row/stride/address
   arithmetic from merged gate/up and down. They require
   `TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH=1` or the narrower
   `TERMITE_METAL_ENABLE_A4B_SPECIALIZED_ID=1`.
4. The same binary runs paired specialization/rollback E2B and E4B positive
   controls. `TERMITE_METAL_DISABLE_A4B_SPECIALIZED_ID=1` is the immediate
   rollback.
5. A fail-closed multi-prompt runner gates exact Antfly candidate/rollback
   tokens, prepared-frame coverage, model-wide residency, zero mapped/frame/Q4
   fallbacks, RSS, per-prompt throughput, aggregate llama.cpp parity, and dense
   controls.

### Exact-shape replay and differential profile

Three fresh replay processes each alternated generic/specialized order for 30
measured iterations after three warmups. The table reports the median of those
process medians. Generic and specialized outputs matched bit-for-bit with zero
maximum absolute error in every run.

| Exact dispatch | Generic GPU time | Specialized GPU time | Speedup |
|---|---:|---:|---:|
| Merged gate/up, 8 routes x 1408 | 299.237 us | 257.237 us | **1.1633x** |
| Down, 8 routes x 2816 | 194.187 us | 172.374 us | **1.1265x** |

The real-GGUF profiler sampled layers 0, 15, and 29 over three fresh processes
and three decode frames per process: 27 timed layer frames per lane. The
profiler itself splits one selected MoE layer into additional encoders, so its
total frame time is diagnostic and is not used as the throughput headline.

| Selected layer stage | Specialized median | Rollback median |
|---|---:|---:|
| Merged gate/up | 79.680 us | 86.993 us |
| Activation | 6.268 us | 5.794 us |
| Down | 41.731 us | 52.776 us |
| Reduction | 5.629 us | 5.882 us |
| **Routed total** | **133.308 us** | **151.445 us** |

That is an **11.98%** routed time reduction. Each sampled layer won
independently, with reductions from 8.58% to 12.93%. Scaling the aggregate
measurement across 30 routed layers gives 3.999 ms/token versus 4.543
ms/token, a 0.544 ms/token saving. Every profiled candidate sample recorded 30
dispatches of each specialized kernel; rollback recorded none. All samples
produced the same 128-token hash.

### Final paired A4B result

The promotion runner used three fixed raw Gemma prompts, three fresh-process
runs per prompt, greedy 128-token decode, FP16 KV, model-wide resident mapped
experts, the prepared decode frame, and the same local Q4_0 GGUF for Antfly and
llama.cpp.

| Runtime | Median tok/s | ms/token | Relative to llama.cpp |
|---|---:|---:|---:|
| Antfly specialized | **42.4465** | 23.559 | **62.38%** |
| Antfly rollback | 41.6257 | 24.024 | 61.17% |
| llama.cpp | 68.0500 | 14.695 | 100% |

The exact A4B specialization is a **1.9719%** end-to-end win over its one-build
rollback and 0.61% above the preceding 42.189 tok/s prepared result. Per-prompt
llama.cpp ratios were 62.33%, 62.58%, and 62.01%. Candidate and rollback token
hashes matched within every pair; all nine candidate samples and all nine
rollback samples completed 127/127 prepared decode frames with zero prepared,
Metal-frame, mapped-weight, and Q4 policy fallbacks.

This is a measured winner and remains in the opt-in high-memory bundle, but it
does **not** clear the default promotion thresholds of 95% median parity and
90% parity on every prompt. The remaining median gap is 25.603 tok/s.

### Dense positive controls

The standard ggml-org Q4_0 E2B and E4B exports were run for three paired
128-token samples. Both candidate/rollback pairs produced exact token hashes,
neither dense model selected an A4B-only kernel, and both cleared the 0.99
non-regression floor.

| Control | Candidate | Rollback | Candidate / rollback | llama.cpp | Parity |
|---|---:|---:|---:|---:|---:|
| E2B Q4_0 | 82.254 | 82.307 | **0.9994x** | 106.070 | 77.55% |
| E4B Q4_0 | 50.138 | 50.079 | **1.0012x** | 61.240 | 81.87% |

The A4B change is therefore dense-neutral on this machine. The live standard
model ratios do not reproduce the previously expected near-parity dense
result, so the independent 90% dense llama.cpp control remains red and should
be reconciled against the earlier model/build/benchmark contract before using
E2B/E4B as a release-positive claim.

### Reproduction and artifacts

```sh
zig build -j1 -Doptimize=ReleaseFast a4b-metal-id-replay -- \
  --warmups 3 --iterations 30

python3 scripts/gemma4/profile_metal_gemma4_a4b.py \
  --binary zig-out/bin/antfly-inference --model "$MODEL" \
  --out-dir /tmp/a4b-profile --layers 15 --runs 3 --specialized

python3 scripts/gemma4/benchmark_metal_gemma4_parity.py \
  --antfly-bin zig-out/bin/antfly-inference \
  --llama-bin "$LLAMA_COMPLETION" --a4b-model "$MODEL" \
  --out-dir /tmp/a4b-parity --runs 3 \
  --control e2b="$E2B_MODEL" --control e4b="$E4B_MODEL"
```

Machine-local evidence is retained at:

- `/private/tmp/gemma4-a4b-profile-20260824-final-specialized-all/`
- `/private/tmp/gemma4-a4b-profile-20260824-final-rollback-all/`
- `/private/tmp/gemma4-a4b-parity-20260824-final-gate/`
- `/private/tmp/gemma4-a4b-parity-20260824-final-controls/`

The final Antfly binary SHA-256 is
`b342bb0e995d1860db343320d7e1d0c2d2db38ade444a5efc845f1500cdb9431`;
the llama.cpp comparator binary SHA-256 is
`92dcad3c204b0574c99611af7a1f64d69ad0506c3abeba56bef8e4ec57fa0bc8`.
The downloaded E2B and E4B model SHA-256 values are respectively
`8e30dff3ac4c8434c49a7036fa15564bdbb6044e42bf04550bf1a096ad7e6a52`
and `a555b900214b477d8880e7832e0b8925e139b0159640036b09fe472b6f2097f2`.

---

## 2026-08-24 Q4_0 high-memory Metal qualification

**Status**: correctness PASS; throughput improved substantially, but llama.cpp
parity is not yet reached.

**Hardware**: Mac mini (Mac16,11), Apple M4 Pro, 12 CPU cores, 24 GB unified
memory

**Model**: `gemma-4-26B_q4_0-it.gguf`, 14,439,363,584 bytes,
SHA-256 `3eca3b8f6d7baf218a7dd6bba5fb59a56ee25fe2d567b6f5f589b4f697eca51d`

**Build**: Zig 0.16.0, `ReleaseFast`, Metal enabled

**Comparator**: llama.cpp build 10342 at `38278078c`, built by Unsloth

The high-memory bundle remains behind one umbrella flag. The backend-owned
prepared qLen=1 frame is still a separate experimental opt-in, so the
throughput result below uses both flags explicitly:

```sh
TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH=1 \
TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE=1 \
  ./zig-out/bin/antfly-inference generate "$MODEL" "$PROMPT" \
  --backend metal --mode compiled --compiled-target whole-model \
  --cache-dtype f16 --a4b-residency-mode resident \
  --a4b-memory-budget-mb 16384 --backend-budget-mb 16384 \
  --combined-budget-mb 16384 --raw-prompt --temperature 0 \
  --ignore-eos --max-tokens 128
```

The flag bundles only the measured wins: one no-copy model-wide Metal buffer
with a residency set, mapped routed-expert weights, fused expert gate/up
activation, cached adjusted norm weights, zero-bias elision, shared-FFN
fusion, SIMD-group RMSNorm/head-RoPE kernels, triple parallel-FFN pre-norm,
parallel-FFN post/residual fusion, RMSNorm/residual fusion, and the qualified
M4 Q4_0 schedules. It now also admits the split-GQA decode kernel for A4B's
exact local (16 query heads, 8 KV heads, head dimension 256, window 1024) and
global (16 query heads, 2 KV heads, head dimension 512) geometries at KV
lengths of 512 or greater. Every component has a
`TERMITE_METAL_DISABLE_A4B_*` rollback override. Both opt-ins remain off by
default.

### Paired deterministic decode result

All measurements use the same raw 29-token Gemma chat prompt, greedy decode,
F16 KV, ignored EOS, 128 generated tokens, and a fresh process per sample.
The throughput metric is decode-only and covers 127 timed decode steps after
the first generated token.

| Runtime | Samples (tok/s) | Median | ms/token | Relative to llama.cpp |
|---|---:|---:|---:|---:|
| Antfly branch baseline | 8.516867 | 8.516867 | 117.414 | 12.37% |
| Antfly high-memory lane before this pass | 39.481801, 39.530574, 39.433148 | **39.481801** | 25.328 | **57.33%** |
| Antfly prepared high-memory lane | 42.189, 42.189, 42.189 | **42.189** | 23.703 | **61.26%** |
| llama.cpp | 69.21, 68.73, 68.87 | **68.87** | 14.520 | 100% |

This is a **4.954x** speedup over the paired Antfly branch baseline (+395.4%)
and 6.86% faster than the preceding 39.482 tok/s high-memory result. The
remaining median gap to llama.cpp is still substantial: 26.681 tok/s. The
planned 50 tok/s / 72.5% parity target was not reached.

All three final Antfly runs emitted the same 128 token IDs. The SHA-256 of the
space-separated IDs without a trailing newline is
`d4ee583f092062e7177069de1f35a9cefbaac24848d11d09b64237bc9209b68e`.
The visible answer also matches llama.cpp:

> LSM trees are optimized for write-heavy workloads because they transform
> random writes into sequential I/O by buffering data in memory and flushing
> it to disk in sorted runs. This approach avoids the expensive, immediate
> disk seeks required by B-trees, significantly increasing overall write
> throughput.

The final runs reported 127/127 prepared-frame hits, zero prepared-frame,
Metal-frame, and quant-kernel fallbacks, and zero mapped-weight failures. The
ordinary row-one Q4_0 linears used NR4/NSG4 for all 19,050 dispatches; the
shared FFN gate/up pair used NR4/NSG2 for all 3,810 dispatches, with no policy
fallbacks.

### Real long-context GQA gate

A separate 603-token prompt was used to cross the split-GQA admission
threshold. Each lane generated 32 greedy tokens, timing 31 decode steps. Three
fresh-process runs with the split enabled measured 40.050 tok/s each; rollback
runs measured 28.070, 28.120, and 28.046 tok/s (28.070 median). The retained
kernel is therefore **42.68% faster** than paged-attention rollback at this KV
length.

All six runs emitted the same token IDs, with SHA-256
`19cce12188964614e383884966bd1658fd90e9210e25e8a747d2c239bf075abd`.
The enabled lane recorded 930/930 split dispatches: 775 across the 25 local
layers and 155 across the five global layers, with zero fallbacks. Setting
`TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT=1` restored all 930 calls to the
paged kernel. The 29-token/128-output short guard recorded zero split
dispatches, confirming that KV lengths below 512 retain the prior path.

### Memory and bottleneck profile

The model-wide no-copy Metal allocation is 14,302,248,960 bytes. The complete
Metal residency set contains 9 allocations totaling 14,937,423,872 bytes
(13.91 GiB), rather than the old roughly 2 GiB compact envelope. Reusing the
tied embedding/output allocation removed one allocation without changing
tokens.

Six sampled decode frames across the three final short runs average:

| Stage | Average per token |
|---|---:|
| GPU total | 23.559 ms |
| FFN | 14.125 ms |
| Attention | 6.375 ms |
| Tail | 3.054 ms |
| Host/submit gap to wall time | about 0.143 ms |

llama.cpp averages 14.520 ms/token end-to-end and reports 126 reused decode
graphs. Backend-owned preparation has nearly removed the measured host/submit
gap, so the remaining short-context deficit is GPU work, dominated by FFN and
then attention. More residency alone cannot close it.

The routed Q4_0 portfolio did not produce a winner. A 16-combination 64-token
sweep and four three-run 128-token finalists all preserved exact tokens, but
the finalist medians (41.721--41.899 tok/s) remained below the retained
NR4/NSG2 route. A source-level port of llama.cpp's Q4_0 block arithmetic
regressed from a same-binary 41.953 tok/s control to 41.451 tok/s, while a
typed-half scale load changed throughput by only +0.03%. Those experimental
kernels and environment flags were removed. Future work should target fewer
expert weight reads or a materially different routed-FFN layout, not more
launch-shape variants of the same arithmetic.

### Verification

- `zig build -Doptimize=ReleaseFast -Dmetal=true`: PASS
- Full inference suite: 3,151 passed, 19 skipped
- Split-GQA production-route oracle: PASS (540 policy cases and 36 on-device
  tensor routes, including both exact A4B geometries)
- Q4_0 pair-activation route portfolio: PASS
- Final post-cleanup 128-token Metal guard: PASS, 3/3 exact token hashes
- 603-token enabled/rollback guard: PASS, 6/6 exact token hashes
- `git diff --check`: PASS

Raw timing JSON, logs, rejected portfolio measurements, and the long-context
guard are retained in `/private/tmp/gemma4-a4b-parity-20260824-next/`; the
paired llama.cpp evidence is in
`/private/tmp/gemma4-a4b-parity-20260824/` for this machine-local run.

---

## Historical report: 2026-04-08 Q5_K_M lane

**Date**: 2026-04-08
**Commit**: dfaca28
**Model**: gemma-4-26B-A4B-it Q5_K_M GGUF
**Hardware**: Apple Silicon (M-series), 36GB unified memory
**Build**: ReleaseFast, Metal backend

## Current Numbers (30 tokens decode)

| Metric | Non-graph | Graph mode | Notes |
|--------|-----------|-----------|-------|
| Decode total | 232.7s | 242.5s | ~7.8s/tok vs ~8.1s/tok |
| Eval overhead | 96.3s (214ms × 450) | 3.4s (113ms × 30) | 96% reduction |
| FFN (tracked) | 101.8s | 4.7s | Graph only tracks tracing pass |
| Attention (tracked) | 2.0s | 1.7s | Same — tracing only |
| Shared expert FFN | 0.9s | 0.9s | Same |
| Unaccounted | 32.6s | 232.7s | Interpreter replay dominates |
| MoE grouped | 300ms (900 calls) | 39ms (30 calls) | Only during trace |

### Per-token breakdown (non-graph, 30 tokens)

```
Total per token:              ~7,760ms
  Eval sync barriers:        ~6,400ms  (82%)  ← 15 evals/token × 214ms
  FFN compute (MoE+dense):   ~3,400ms  (44%)  ← overlaps with eval
  Attention compute:          ~  67ms   ( 1%)
  MoE routing overhead:      ~   5ms   (<1%)
  Norm + other:               ~   3ms   (<1%)
```

The eval barrier (214ms) is where Metal actually executes the accumulated
lazy computation graph for 2 layers. The 214ms includes GPU matmul time
for all ops in those 2 layers.

### Per-token breakdown (graph mode, 29 replay tokens)

```
Total per token:              ~8,100ms
  Interpreter dispatch:       ~8,000ms  (99%)  ← 2011 ops dispatched
  Eval overhead:              ~  100ms  ( 1%)  ← single eval at end
```

Graph mode eliminates per-layer eval barriers but the interpreter
dispatches 2011 Metal lazy ops per token, each going through vtable →
Metal C API → lazy graph construction. The actual GPU work is identical.

## Why graph mode doesn't help (yet)

The interpreter replaces 15 eval barriers with 2011 vtable calls.
The eval barriers force GPU sync (214ms each = 3.2s/token), but the
vtable dispatch overhead is higher (~8s/token) because:

1. Each of 2011 ops goes through: Zig vtable → C function pointer →
   Metal C wrapper → Metal C++ lazy array construction
2. Metal builds its own internal lazy graph regardless
3. The single eval at the end materializes the same computation

Graph mode would help if the interpreter could emit Metal commands
directly (bypass Metal lazy evaluation) or batch operations.

## Bottleneck Analysis

### Where time actually goes (GPU)

For each of 30 layers per token:

| Operation | Count | Est. time |
|-----------|-------|-----------|
| Attention QKV linear (Q5_K) | 3 | ~33ms |
| Attention output proj (Q5_K) | 1 | ~18ms |
| Dense FFN gate+up+down (Q6_K) | 3 | ~30ms |
| MoE router projection | 1 | ~1ms |
| MoE expert gate+up+down × top_k=2 | 6 | ~10ms |
| RMSNorm × 5 | 5 | ~1ms |
| Activations, adds, rope | ~10 | ~1ms |
| **Layer total** | | **~94ms** |

30 layers × 94ms = 2.8s compute. But actual is ~7.8s/token because:
- Metal kernel dispatch overhead per operation
- Memory bandwidth for loading 26B of quantized weights from unified memory
- Eval sync barriers (GPU↔CPU round-trips)

### Weight memory bandwidth

At Q5_K (5.5 bits/param), 26B params = ~17.9 GB of weights.
Apple Silicon M-series memory bandwidth: ~200 GB/s (M2 Ultra) to ~100 GB/s (M3 Pro).
Theoretical minimum: 17.9 GB / 200 GB/s = ~90ms per token (bandwidth bound).
Actual: ~7.8s per token → **87x slower than bandwidth limit**.

The gap is from:
- **930 separate Metal kernel dispatches** per token (each has fixed overhead)
- **Eval barriers** forcing GPU↔CPU sync 15 times per token
- **Lazy evaluation overhead** in Metal's graph construction

## Optimization Opportunities

### High Impact

1. **Fuse operations in Metal kernels** (target: 2-4x speedup)
   - Fuse RMSNorm + linear into single kernel (eliminate intermediate tensor)
   - Fuse gate+up linear pair for dense FFN (already have Q5_K kernel, need Q6_K)
   - Fuse silu(gate) * up into the linear kernel
   - This reduces 930 kernel dispatches to ~200-300

2. **Increase eval stride / reduce eval count** (target: 30-50% eval savings)
   - Current: eval every 2 layers = 15 evals/token × 214ms = 3.2s
   - Try eval every 4-6 layers for decode (seq_len=1, memory is small)
   - Risk: larger lazy graphs might have diminishing returns

3. **Expert weight residency** (target: reduce weight loading latency)
   - Current: 16 of 128 experts resident, rest lazy-loaded from host memory
   - Gemma 4 uses top_k=2 from 128 experts → most accesses hit non-resident
   - Increase resident budget or implement LRU expert caching

### Medium Impact

4. **Continuous batching** (target: higher throughput, not lower latency)
   - Process multiple requests simultaneously
   - Share weight loading across batch → amortize memory bandwidth
   - Requires paged KV cache (already implemented)

5. **Speculative decoding** (target: 2-3x latency improvement)
   - Use a small draft model (e.g., Gemma 4 2B) to propose tokens
   - Verify in parallel with the 26B model
   - Already have infrastructure in generation.zig

6. **Graph interpreter → Metal command buffer** (target: eliminate dispatch overhead)
   - Instead of dispatching Metal lazy ops, emit Metal compute commands directly
   - Pre-compile the compute pipeline from the graph
   - This is the long-term path for graph mode to actually help

### Lower Impact

7. **Q4_K quantization** (target: ~20% faster, slight quality loss)
   - Reduces weight memory by ~20% (4.5 vs 5.5 bits/param)
   - Less memory bandwidth → faster decode
   - May need to requantize the model

8. **GPU softmax for MoE routing** (target: eliminate 1 sync per layer)
   - Currently downloads router logits to CPU for softmax
   - Move softmax to GPU, only download top_k indices

9. **Shared expert pair matmul** (target: minor FFN speedup)
   - Fuse gate+up for shared expert dense FFN
   - Blocked on Q6_K pair kernel (only Q5_K exists)

## Raw Timing Data

### Non-graph mode (30 tokens)

```
generate_timing_ms: prompt_format=0 tokenize=0 prefill=11818 decode=232729 total=244547
gpt_timing_ms: attention=1994 attn_norm=32 attn_qkv=1238 attn_core=111 attn_rope=46 attn_gqa=62 attn_out_proj=609 ffn=101832
gpt_moe_timing_ms: grouped_attempts=900 grouped_successes=900 moe_grouped=298
gpt_overhead_ms: eval=96259 eval_count=450 shared_expert_ffn=928 norm=59
metal_quant_counts: provider_calls=6180 provider_grouped_calls=2700 device_native_moe_grouped_calls=2700
```

### Graph mode (30 tokens)

```
generate_timing_ms: prompt_format=0 tokenize=0 prefill=11657 decode=242462 total=254119
gpt_timing_ms: attention=1683 attn_norm=17 attn_qkv=1077 attn_core=5 attn_rope=1 attn_gqa=4 attn_out_proj=582 ffn=4659
gpt_moe_timing_ms: grouped_attempts=30 grouped_successes=30 moe_grouped=39
gpt_overhead_ms: eval=3392 eval_count=30 shared_expert_ffn=904 norm=28
metal_quant_counts: provider_calls=6180 provider_grouped_calls=2700 device_native_moe_grouped_calls=2700
```

## Key Insight

The dominant cost is **Metal kernel dispatch overhead** for ~930 small
single-row matmuls per token across 30 layers. Each matmul is tiny
(1×dim matrix-vector multiply) but carries fixed kernel launch cost.
The path to 30 tok/s requires fusing these into fewer, larger kernels
or switching to a direct Metal compute pipeline that avoids per-op dispatch.
