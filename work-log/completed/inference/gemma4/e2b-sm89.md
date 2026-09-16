# Gemma 4 E2B SM89 optimization status

> Relocated verbatim from `zig/pkg/inference/docs/CUDA_TUNING.md` (lines 872–973 and 329–362 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`CUDA_TUNING.md`](../../../../zig/pkg/inference/CUDA_TUNING.md). Durable decisions from this log were folded into that document before the move.

## Gemma 4 E2B SM89 optimization status

The locked CUDA tuning workload is Gemma 4 E2B QAT on an NVIDIA L4: eight
query heads share one KV head (the 8:1 MQA endpoint of GQA), the rendered prompt
is 8,251 UTF-8 bytes / 2,051 tokens, prefill uses four 512-row chunks plus a
three-row tail, and decode is fixed at 300 tokens with an F16 paged KV cache.
This topology is an exact route constraint, not a model-neutral assumption.

The 2026-07-30 warm-server collection measured Antfly at 726.81 ms TTFT and
113.424 decode tokens/s, versus llama.cpp at 316.99 ms and 116.167 tokens/s.
Total latency was 3,362.98 ms versus 2,890.91 ms, a 1.1633 Antfly/llama.cpp
ratio. This was exploratory one-pair evidence, not a superiority result. It
shows that decode is within 2.4%, while the approximately 410 ms TTFT deficit is
the dominant remaining gap.

The 2026-07-31 ten-pair warm-server collection (`benchmark_gemma4_long_e2e_server.py
--cuda-execution-profile gemma4-e2b-sm89-flash-splitk-v1 --collect-only`, two
warmups, balanced AB/BA) confirmed that gap with paired statistics: Antfly
measured 740.8 ms median TTFT and 110.24 decode tokens/s versus llama.cpp at
329.5 ms and 114.38 tokens/s; total latency was 3,454.5 ms versus 2,945.1 ms, a
1.1730 median ratio with a [1.1700, 1.1753] paired bootstrap 95% CI. Decode is
within 4%; the 2.25x TTFT deficit is the entire remaining end-to-end gap. This
profile includes the collect-only split-K decode candidate, so it is a
performance-frontier measurement, not an exact-output configuration.

The promoted-routes-only production configuration is materially slower on the
same workload class. In the tuned pair-harness CLI config (F16 caches, 512-row
prefill chunks, a 1,457-token prompt, 511 greedy tokens) Antfly measured 76.7
decode tokens/s with 5,002 ms median prefill versus `llama-completion` at 131.3
tokens/s and 201 ms prompt eval. Those figures predate the 2026-07-31
flash-prefill promotion: the SM89 flash prefill route is now a production
runtime default through the automatic profile selector (unset
`ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE`; rollback `off`). The remaining
still-unpromoted surface is split-K decode,
`ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL`, and the capture/readback
extras in the reviewed flash profile environment. With F32 K/V
caches the same workload decodes at only 32 tokens/s: every score-prework
selector is F16-only, so the automatic route is ineligible and F32 long-context
comparisons measure the legacy decode path by construction.

Two operational notes for reproducing these numbers. The harness's default
`model-neutral-v3` execution profile is a deliberately untuned reviewed
baseline — it measures a 7.6x total ratio on this workload — so frontier
claims require the versioned flash profile. The pair harness requires the
`llama-completion` binary; current llama.cpp `llama-cli` builds ignore
`-no-cnv` and enter interactive conversation mode, which hangs the run, and the
harness's fixed `-c 2048` llama.cpp context caps prompt plus output at 2,048
tokens.

The SM89 split-K online decode prototype reduces the locked Antfly decode path
from approximately 74 to 114 tokens/s and replays safely through the persistent
CUDA graph. Its 64-way online-softmax/value regrouping is deterministic, but it
changes the generated token stream after output token 136 relative to the
legacy chronological recurrence. FP64 merge coefficients do not materially
reduce that drift. Consequently `splitk-online-sm89` remains default-off and
collect-only; exact-output policy must not be weakened just to promote it. The
exact alternative, parallel canonical-order score generation followed by the
canonical tiled64 consumer, is bitwise-identical but projects to only a 1.021x
attention speedup. A future concurrent runtime must also make split-K
score/counter workspace request- or stream-owned; the current module-owned
workspace is valid only for batch one with continuous batching disabled and a
serialized stream.

For Flash prefill, wider q32/q64 and serial two-/four-head grouped designs were
qualified and rejected: all were slower on the locked matrix. The retained
q16/k16 kernel now uses the compile-time `kThreads == 256` stride in its four
hot cooperative loops after launch validation. This lets ptxas unroll K/V
loads without changing launch geometry, shared-memory layout, or occupancy.
The regenerated production template passed all 90 guard, page-table,
adversarial-input, and determinism cases with bitwise-identical output. Its
alternating L4 A/B projects a 1.0355x attention speedup (8.81 ms over 35
layers), clearing the dedicated aggregate 1.02x micro-optimization gate. The
1.20x algorithmic gate for a new Flash design remains unchanged.
The follow-up concurrent q16/k16 two-head CTA was also bitwise-identical across
all 90 qualification cases, but its best launch-bounded build projects only a
1.0652x speedup and spills in HD512. It therefore remains standalone; a new
grouped design must reduce persistent-fragment register pressure and handle the
three-row tail separately before another production screen.

The bounded PLE-gate BF16 mirror-first profile is also implemented as a typed,
default-off SM89/E2B candidate. It routed all 175 eligible prefill projections
through the admitted BF16 mirror, preserved all 140 decode projections on the
fused Q4 path, and recorded no eligibility misses. On the locked 2,051-to-300
workload it reduced TTFT from 867 to 813 ms, but decode throughput regressed
slightly (114.811 to 114.460 tokens/s) and the generated stream first diverged
at zero-based output index 154, with 50 of 300 positions differing. Strict
parity therefore rejected promotion after the first pair. Keep
`ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE=off` in production; the
candidate exists for controlled numerical-quality experiments, not as a tuning
default.

An Nsight Systems request-window trace attributes 332.9 ms of 562.8 ms GPU
busy time to Flash attention and 229.8 ms to non-attention work. GPU utilization
is 97.7%, leaving only a 13.3 ms device-idle upper bound, so CUDA Graph capture
is useful for CPU concurrency but is not the primary TTFT solution. The next
production priorities are: an upload-packed SM89 W4A16 Tensor Core projection
engine with a documented numerical contract, model-shape cuBLASLt tuning with
admitted persistent workspace, fused gate/up activation output, and a genuinely
concurrent GQA Flash redesign that reuses K/V without the v3 register/tail
costs. Each remains independently gated; projected savings overlap and must not
be added without end-to-end device-event evidence.

## Score-prework and split-KV decode validation

The validator locks F32 values, disables the handwritten split experiment and
dense generated attention in both arms, alternates execution order, compares
every token ID, requires persistent graph replay, and requires the dedicated
`launch_attention_gqa_decode_score_prework` counter. On the E2B QAT model and
SM89 L4, the three-prompt corpus measured 98.918 versus 82.183 tok/s median at
256 outputs (1.1999x) and 67.422 versus 44.483 tok/s at 1024 outputs (1.5157x).
All six pairs had exact token IDs, persistent replay, zero fallback, and no
graph discard or capacity skip. The F32-cache production control remained
healthy at 91.103 tok/s with 251 persistent replays. These results justified
retaining the candidate; the later F16 bitwise-parity and paired-throughput
qualification promoted the route to the default automatic selector on the
qualified SM89 Gemma 4 F16 geometry. Enabling it beyond that geometry still
requires broader model/context coverage via the explicit gate.

The generated serial decode path now matches the production decode-scalar
contract exactly: it passed three prompts at 64 and 256 output tokens with a
0.9995x median paired throughput ratio, and three prompts at 1024 output
tokens with zero graph-capacity skips. Split-KV is substantially faster in the
current short-output corpus (about 1.28x at 256 tokens with a 128-token
threshold), and those runs had exact token IDs. It is still experimental: the
same split-128 candidate changes one token at position 915 for one prompt in a
three-prompt 1024-token corpus, despite a roughly 2x throughput gain. Do not
enable split-KV in a production profile or promote the dense split-KV
generated attention catalog entries until a long-output, multi-model parity
corpus passes; only the exact score-prework composites carry promotion.

The split-count sweep on the SM89 L4 keeps split-8 as the short-context winner:
at 256 output tokens it measured 115.8 tok/s versus 89.9 tok/s for the
baseline (1.288x median) with exact tokens on the three-prompt corpus. Split-2
and split-4 both changed the prime-number prompt at token 232. At 1024 outputs,
split-8 measured 103.3 tok/s versus 51.4 tok/s (2.01x median) but retained the
sky-prompt change at token 915; raising its threshold to 512 reduced throughput
to 1.54x without moving that divergence. These are development measurements,
not production tuning defaults.
