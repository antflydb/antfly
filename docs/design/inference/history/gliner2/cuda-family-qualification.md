# GLiNER2.5 Multi and Decide-1B CUDA Qualification Evidence

> Summary of a verbatim log. Full text: [docs/design/inference/history/gliner2/cuda-family-qualification.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/gliner2/cuda-family-qualification.md). Originally relocated verbatim from `zig/pkg/inference/CUDA.md` (lines 326–735 at commit 3ce5aff4ea) on 2026-10-12; durable decisions are in [CUDA.md](../../../../../zig/pkg/inference/CUDA.md#gliner25-multi-and-decide-1b).

CUDA profiles for `fastino/gliner2.5-multi-v1`, `fastino/GLiNER2.5-multi-Decide`, and `fastino/GLiNER2.5-Decide-1B` (Ettin/ModernBERT, native 7,999 encoder tokens), plus an offline CUDA route for the split Q8_0 Decide-1B bundle. At the time of the move these families remained **pending release qualification**: diagnostic tools grant no release capability rows or registry pins. Pins, oracles, and evidence live in `testdata/gliner25/family/` (`manifest.json`, `evidence/summary.json`).

## Decisions

- The nested encoder config selects the architecture; a span wrapper no longer implies DeBERTa.
- Decision rows are grouped by encoded length; default physical batches are bounded to 64 rows and 16,384 padded tokens.
- Managed loads: `cuda_precision` `auto` resolves to FP32; explicit `fp16`/`bf16` are rejected with `UnqualifiedGlinerCudaPrecision` until release evidence exists. Precision is part of resident and in-flight model identity.
- An aggregate speedup does not override per-cell regression failures (10% guard). Compare against the fastest quality-valid Fastino profile; reports distinguish `reference_dtype` from `reference_weight_dtype`.
- BPE tokenizers apply declared NFC normalization before pre-tokenization (found by a Hindi token mismatch in the 1B holdout).
- Holdout compatibility policy (`--source-span-policy original_text`): exclude Fastino predictions whose spans include its synthetic terminal period beyond the original text; does not clip spans or change native decoding; does not grant release qualification.
- CUDA boundary requests queue before reserving device scratch; a queued disconnect drains with no device reservation.
- Release still requires serving concurrency, long-window performance, cancellation/OOM recovery, and the 99.5% per-language holdout gate. CUDA validation is independent of public serving qualification; the Q8 bundle gets no CUDA `/decisions` qualification from the Metal PR #1033 or offline benchmarks.

## Rejected approaches

- Multi FlashDeBERTa FP16 candidate — changed extraction output structure, failed the quality gate.
- Multi FP16 — failed per-language holdout (Arabic 198/200, Hindi 196/200, Chinese 198/200) despite passing ten extraction fixtures.
- cuBLASLt bias-epilogue fused with FP16 matmul — changed two Arabic selections (Multi-Decide 1,597/1,600, Arabic 198/200); removed from dispatch.

## Key measurements (NVIDIA L4)

Fixture campaigns (30 pairs, 200 tail samples per cell):

| Campaign | Fastino reference | Native geometric speedup (95% interval) |
|---|---|---|
| Multi, ten extraction cases | FP32 eager | 5.44× (5.41–5.47×) |
| Multi-Decide FP16, eight capacity cells | FlashDeBERTa FP16, FP16 resident weights | 1.53× (1.52–1.54×) — fails 512/B8 and 2048/B1/B8 at 1.22×, 1.22×, 1.49× Python latency |
| Decide-1B FP16, ten capacity cells | SDPA FP16, FP16 resident weights | 1.123× (1.120–1.126×) — all cells pass; 7,999 tokens/B1 1,073 ms vs 1,142 ms (0.940×) |

Q8 Decide-1B with FP16 projection mirrors (`ANTFLY_CUDA_GLINER_1B_Q8_F16_MIRRORS=1`), driver 580.159.03, prepared-core medians:

| Tokens, batch 1 | Previous mirrors | Tiled-attention mirrors | PyTorch FP16 | CUDA/Python |
|---|---:|---:|---:|---:|
| 25 | 9.81 ms | 9.81 ms | 23.47 ms | 0.418× |
| 512 | 77.38 ms | 32.08 ms | 32.02 ms | 1.002× |
| 2,048 | 688.78 ms | 145.48 ms | 137.58 ms | 1.057× |

Tiled attention gave paired speedups of 2.409× at 512 and 4.745× at 2,048 tokens. Resident weights: 3.61 GB with mirrors vs 2.73 GB Q8 + tensor-core packs vs 1.74 GB direct Q8. Long-context route passes all 14 published cases at 0.002 logit tolerance (max error 0.001394); only five of eight multilingual holdouts through 7,999 tokens pass that bound (max 0.004900), so FP16 numerics on broader inputs are not qualified.

Public MASSIVE holdout (200 examples × 8 languages, agreement with Fastino FP32, not gold accuracy):

- Decide-1B FP16: 1,600/1,600 after the NFC fix; gold-intent accuracy 643/1,600 (40.1875%), same as Fastino FP32.
- Multi-Decide revised FP16: 1,599/1,600. Multi FP32: 1,600/1,600 classification, 1,599/1,600 entities; structured strict parity failed French 198/200 (Fastino spans past input), 1,600/1,600 under the compatibility policy.
- Fastino FlashDeBERTa FP16 reference itself: 1,598/1,600.
- HTTP concurrency 1/4/16: Multi-Decide FP32 passes with every lease released.
