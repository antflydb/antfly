# Qwen performance

The retained changes enable qualified Q4_K/Q6_K matrix shapes for Qwen3-VL
prefill, keep an eager decode frame across layers, and specialize dense causal
attention for 128-wide heads. Other shapes and existing fallback paths remain
available. See [Qwen3-VL support](QWEN3VL.md) for serving contracts.

BF16 projections now use a 64x32x32 simdgroup matrix kernel on supported Apple
GPUs, with exact BF16-to-F32 expansion and F32 activations/accumulation. The
same selection covers host/device projections and both dense MLP stages;
smaller shapes and unavailable pipelines retain their existing paths.

Dense Qwen3 embedding and text reranking share prepared Metal prefill, with
resident final-row selection and a compact yes/no reranker classifier. Qwen3
Q8 weights avoid redundant dense copies, native BF16 uses bounded conversion
panels, and embedding metadata resolution avoids unnecessary tokenizer JSON
parsing. New BF16 vector loads and gate/up/SiLU fusion default only to the
measured base Apple M4 shapes; other devices and shapes remain explicit-only.

> **Relocated:** The dated ReleaseFast/ReleaseSafe measurement campaigns, pass counts, and validation-limit tallies that previously lived here (122 lines, 2026-09-08 and earlier) are preserved verbatim in [work-log/completed/inference/qwen-performance-evidence.md](../../../../../work-log/completed/inference/qwen/performance-evidence.md). Durable decisions from it are in Resource and correctness checks in this document.

## Reproduce endpoint measurements

Use ReleaseFast for both sides of new performance comparisons and for PR
metrics. From `zig/pkg/inference`, build the focused server with
`zig build bench-server -j1 -Doptimize=ReleaseFast -Dmetal=true`. It uses the
production parser, supervisor, model manager, admission controls, and HTTP
routes. The historical ReleaseSafe results require remeasurement before
making ReleaseFast claims.

The retained endpoint tools accept separately launched resident servers and
record model/build identities, paired timings, and output checks:

- [Embedding endpoint benchmark](../../scripts/qwen3_embedding/benchmark_qwen3_embedding_endpoint.py)
  follows [the embedding protocol](../../scripts/qwen3_embedding/BASELINE.md). The
  checked-in exact-token recipe covers 20, 256, 511, 2551, 4096, and 8192 tokens
  with 24 distinct prefixes per length, enough for three warmups and 20 pairs.
- [OCR endpoint benchmark](../../scripts/qwen3vl/benchmark_qwen3vl_ocr_endpoint.py)
  checks complete `/ai/v1/read` responses against the frozen
  [fixtures](../../scripts/qwen3vl/fixtures/ocr/fixture.json) and
  [Q4_K_M golden](../../scripts/qwen3vl/fixtures/ocr/golden_q4_k_m.json). Supply
  `--fixture`, `--golden`, `--url`, `--model`, and candidate process provenance;
  add `--reference-url` and reference provenance for a paired Antfly comparison.
  It does not implement a llama.cpp OCR comparison.
- [Resource guard](../../scripts/benchmark_resources.py) wraps the launcher so both
  servers, clients, and inference workers share bounded resource accounting.

Use each tool's `--help` for its full CLI. A passing endpoint report establishes
its output and provenance checks; performance claims also require independent
campaigns, paired analysis, and successful resource guards. The latest
50-request-per-campaign llama.cpp confirmations use the separate launchers and
paired-block analysis preserved with their evidence below. Historical campaign
orchestration, fixture expansion, and retention gates are archived below rather
than maintained as a second checked-in benchmark interface.

## Rollback and attribution

| Optimization | Baseline-only environment setting |
| --- | --- |
| Q4_K high-row matrices | `TERMITE_METAL_DISABLE_Q4_K_HIGH_ROW_MM=1` |
| Q6_K high-row matrices | `TERMITE_METAL_DISABLE_Q6_K_HIGH_ROW_MM=1` |
| Cross-layer eager decode frame | `TERMITE_METAL_ENABLE_QWEN3VL_FORWARD_DECODE_FRAME=0` |
| 128-wide dense causal attention | `TERMITE_METAL_DISABLE_DENSE_CAUSAL_HD128=1` |
| BF16 simdgroup matrices | `TERMITE_METAL_DISABLE_BF16_MM=1` |
| Dense Qwen3 embedding/reranker prepared prefill | `TERMITE_METAL_DISABLE_QWEN3_PREPARED_PREFILL=1` |
| BF16 vector loads | `TERMITE_METAL_DISABLE_BF16_VECTOR_LOADS=1` |
| BF16 gate/up/SiLU fusion | `TERMITE_METAL_DISABLE_BF16_FUSED_GATE_UP=1` |

For an OCR ablation, launch the same candidate binary for both endpoints with
only the selected control set on the reference server. Use the OCR endpoint
tool with `--case portrait --warmup 3 --iters 20`, repeat in fresh processes,
and retain the resource reports and exact model/build/fixture identities.

In the historical ReleaseSafe campaign, two runs of each individual OCR
ablation passed: Q4_K reduced median latency 31.2–31.4%, Q6_K 19.9–20.2%, and
decode frames 32.5–33.0%. These isolated effects do not add to the combined
improvement. The Qwen3-VL prefill-frame experiment
(`TERMITE_METAL_ENABLE_QWEN3VL_PREFILL_FRAME`) remains opt-in; dense Qwen3
embedding/reranker prepared prefill is enabled for eligible contracts.
BF16 disable controls take precedence over enable flags. Enable
`TERMITE_SERVER_GENERATE_TIMING=1` or
`TERMITE_EMBED_TIMING=1 TERMITE_METAL_STAGE_TIMING=1` only for separate profiles.

## Resource and correctness checks

The guard covers the launcher and worker process group: at most 8 GiB sampled
RSS, at least 15% free memory and 1 GiB free disk, zero swapout growth, and a
bounded deadline. Serialize builds, GPU requests, and CPU oracle work. Preserve
failed or interrupted reports; never relax these limits to obtain a speedup.

The frozen OCR PNGs, manifest, and golden responses are test inputs. Keep their
bytes unchanged for comparisons; use the endpoint tool's `--capture-golden`
only when deliberately establishing a new baseline. The embedding fixture
records tokenizer verification including EOS, and its loader verifies the
expanded text/token digest. See
[the embedding protocol](../../scripts/qwen3_embedding/BASELINE.md) for tokenizer,
pooling, normalization, and independent-reference requirements.

For long CPU FP32 references, `transformers_embedding_oracle.py --attention sdpa`
disables unnecessary autoregressive K/V caching and records the attention
implementation. Qualify SDPA against eager first; the recorded 2551-token check
had cosine 0.9999999999876741 and maximum absolute difference 5.7e-7. Then use
`qualify_qwen3_embedding_metal.py --oracle /path/to/oracle.json --tier q8_0`.
Reference generation and qualification also run under the resource guard.

### Performance promotion gate

A measured optimization is only confirmed as a win when it passes quality
checks and its paired-block latency-ratio upper bound is <=0.95 in every
individual measurement campaign and in the pooled result; gates are never
waived, and reductions from different implementation stages are never added
together. BF16 embedding is not enabled by default for this reason: its
pooled latency-ratio upper bound has exceeded 0.95, and it separately misses
the <=1.10 latency-ratio target against llama.cpp. Only the measured base
Apple M4 shapes get the BF16 vector-load and gate/up/SiLU fusion defaults;
other devices and shapes stay explicit-only until they clear the same gate.

> **Relocated:** The evidence-artifact ledger (local report paths, evidence
> archives, and executable/model SHA-256 digests) that previously lived here
> (76 lines) is preserved verbatim in
> [work-log/completed/inference/qwen-performance-evidence.md](../../../../../work-log/completed/inference/qwen/performance-evidence.md).
> Durable decisions from it are in Performance promotion gate in this document.
