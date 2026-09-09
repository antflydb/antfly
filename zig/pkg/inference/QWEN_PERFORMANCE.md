# Qwen performance

The retained changes enable qualified Q4_K/Q6_K matrix shapes for Qwen3-VL
prefill, keep an eager decode frame across layers, and specialize dense causal
attention for 128-wide heads. Other shapes and existing fallback paths remain
available. See [Qwen3-VL support](QWEN3VL_SUPPORT.md) for serving contracts.

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

## Latest ReleaseFast results (2026-09-08)

The final embedding build (v10) matches the 12 implementation source files
committed in `97a723bd13`. Measurements use Apple M4 / 16 GiB / macOS 26.5,
stripped ReleaseFast builds, Metal, kernel JIT off, and complete resident HTTP
latency with model loading excluded. All rows use Qwen3 0.6B, 511 total prompt
tokens and batch one: one text for embedding or one document for reranking.
Each comparison matches model artifacts, token IDs and output contracts.
Embedding uses last-token pooling and L2 normalization; the BF16 comparison
uses the same converted BF16 GGUF in both engines, not the catalog F16 model.
Reference checks verify full-prompt evaluation without prompt/KV reuse.

| Workload | Antfly mean | llama.cpp mean | Antfly / llama latency, 95% CI | Relative latency |
| --- | ---: | ---: | --- | --- |
| Q8 embedding, v10 | 362 ms | 427 ms, b10809 | 0.8478 [0.8173, 0.8679] | **15.2% lower** |
| BF16 embedding, v10 | 395 ms | 354 ms, b10809 | 1.1167 [1.0915, 1.1353] | 11.7% higher |
| Q8 reranker, earlier v7 snapshot | 293 ms | 425 ms, b8990 | 0.6892 [0.5841, 0.7829] | **31.1% lower** |

Each row has two separate process campaigns of 50 measured requests per
engine, totaling 100 requests per engine and ten paired ten-request blocks.
The embedding campaigns reuse the same 50 unique measured inputs. Only one
engine is resident at a time. Embedding blocks have two warmups and two
preconditions; reranker blocks have three warmups. Intervals use 10,000
bootstrap resamples of paired blocks, keeping requests within each block
together, and a sample-count-weighted ratio of latency sums. Absolute latency
varied substantially across campaigns; these means are descriptive, not SLOs.

The reranker timing remains attributed to v7, before the embedding metadata
optimization. It also confirms **58.3% lower latency** versus the older generic
path (703 ms mean; ratio 0.4166 [0.3768, 0.4582]). All 38 saved diagnostic score
occurrences match v7/v10 bitwise; this binds correctness evidence, not timing.
Final-v10 reranker latency was not remeasured.

The metadata optimization confirms a **6.7% Q8 reduction** versus v7. BF16's
pooled reduction is 7.3%, but it fails the repeated >=5% win criterion: the
second campaign's upper ratio bound is 0.9551, above 0.95. A confirmed win
requires passing quality and an upper bound <=0.95 in each campaign and pooled.
BF16 also misses the <=1.10 latency-ratio target against b10809. No gate was
waived and reductions from different implementation stages must not be added.

## Validation and limits

- Focused v10 ReleaseFast tests: **214 passed / 2 skipped** with Metal API
  validation; **203 passed / 13 skipped** with Metal and system BLAS disabled.
  The portable configuration ran on macOS; Linux and full CI remain pending.
- Q8 and BF16 each pass **78/78 embedding checks** against saved independent
  11-case Transformers oracles, including multilingual inputs, truncation,
  MRL, normalization and batching. All 100 v7/v10 vectors per format match
  bitwise. Minimum cosine versus b10809 is 0.9999971300 Q8 / 0.9999761491 BF16.
- Final-v10 reranking passes **66/66 diagnostic checks** against v7 and b8990,
  with maximum score error 0.0009833574 and zero ordering inversions. The
  fixture covers 16 hand-written judgments across four queries; it does not
  establish representative retrieval quality.
- Gemma4 E2B Metal and GLiNER2 CPU/Metal before/after screens preserve outputs
  within each backend and show no consistent slowdown. These are small
  regression screens. GLiNER2 used an isolated fixture with the legacy
  manifest type corrected from `extractor` to `recognizer`; weights and the
  installed bundle were unchanged. Cross-backend parity remains unqualified.
- All reported confirmation and regression resource guards pass with zero
  swapout growth. Broader concurrency, reload/recovery and other GPU families
  remain outside these checks.

The b10809 reranker comparison still exceeds the 0.001 score tolerance:
maximum error **0.0014788508**, with no ordering inversions. Two selected
independent exact-Q8 FP32 oracle cases support Antfly, but do not waive that
failed comparison or qualify broader retrieval. The b8990 timing win applies
only to its named reference. CPU, long-context and OCR workloads have no
confirmed final-build comparison with llama.cpp in this campaign.

## Historical ReleaseFast measurements

These earlier branch measurements predate the final implementation above.
They retain their original builds, baselines and sample counts, and must not
be presented as current-head performance. All are resident HTTP medians on
Apple M4 / 16 GiB, batch one, with matching before/after artifacts and inputs.

| Workload | Baseline → candidate median | Reduction | Measurements |
| --- | --- | --- | --- |
| Qwen3-Embedding 0.6B Q8, 8K | 18.60 s → 13.76 s | 26.1% | 20 requests/build, ABBA blocks |
| Qwen3-Embedding 0.6B BF16, 511 tokens | 1.187 s → 470 ms | 60.4% | 20 requests/engine, before/after/llama/llama/after/before blocks |
| Qwen3-Embedding 0.6B BF16, 2,551 tokens | 7.938 s → 3.399 s | 57.2% | 10 requests/engine, same six-block order |

The Q8 baseline is the reconstructed pre-optimization branch baseline; the
BF16 baseline is `e7a47976`, before the simdgroup kernel change. Every compared
before/after embedding was bitwise identical, and all complete resource guards
passed with zero swapout growth. Block results are descriptive estimates.

The earlier short-input llama.cpp comparisons, CPU screens and complete
block distributions remain in the historical evidence archive below. The
2,551-token BF16 run slowed over time, so retain both blocks with its aggregate.

The standalone Q8 8K median was 9.91 s, versus 13.76 s in the sustained branch
comparison. Do not combine the faster standalone latency with the branch
speedup percentage. Current 8K llama.cpp parity remains unqualified.

## Historical ReleaseSafe results

Apple M4 / 16 GiB / macOS 26.5 / Zig 0.16 ReleaseSafe. These are complete HTTP
request medians with resident models and fixed fixtures; startup, model loading,
and instrumented profiles are excluded. Baseline and candidate use identical
model artifacts, inputs, output contracts, and server settings, with alternating
AB/BA requests and one active request at a time.

| Workload | Baseline → candidate | Median reduction | Measurements |
| --- | --- | --- | --- |
| Qwen3-Embedding 0.6B Q8, 8K tokens | 16.45 s → 11.77 s | 28.4% | 100 paired requests |
| Qwen3-VL 2B Q4_K_M, portrait OCR | 17.02 s → 9.35 s | 45.0% | 20 pairs; reproduced at 44.9% in a second run |
| Qwen3-Reranker 0.6B Q8, 4K rendered tokens | 6.49 s → 5.20 s | 19.8% | 20 pairs |
| Same reranker, second fresh run | 7.70 s → 6.18 s | 19.6% | 20 pairs |

All runs above passed their resource guards with zero swapout growth. OCR text
and usage matched exactly; reranker scores matched exactly in both 4K runs.
Embedding passed independent CPU FP32 qualification at 4K and 8K, including
query formatting, multilingual inputs, MRL, batching, and retrieval. The 4K
reranker speedup intervals were 1.210–1.286× and 1.228–1.268× (paired 95% CI).

The OCR 100-pair confirmation had an unexplained slower baseline; use the
repeatable ~45% result rather than its larger reduction. The 8K reranker attempt
hit the system swap guard and is excluded. These measurements do not establish
ReleaseFast performance, other-GPU qualification, final-head CI, or independent
reranker score parity. Later build wiring and OCR admission fixes did not change
the measured reranker or Metal implementation.

## Reproduce branch A/B workloads

Run commands from `zig/pkg/inference`. Set `models_dir`, `embedding_gguf`,
`embedding_tokenizer`, `vl_model_dir`, and `baseline_server` to the exact local
artifacts and an immutable baseline executable. Use ReleaseFast for both sides
of new performance comparisons and for PR metrics. The historical ReleaseSafe
results above require remeasurement before making ReleaseFast claims.
The focused target uses the production parser, supervisor, model manager,
admission controls, and HTTP routes. This checked-in runner reproduces the
original paired branch A/B protocol. The latest 50-request-per-campaign
llama.cpp confirmations use separate launchers and paired-block analysis in
the evidence directory below; the checker here does not reproduce their
confidence intervals or stricter repeated-win criterion.

```sh
zig build bench-server -j1 -Doptimize=ReleaseFast -Dmetal=true
campaign_dir=$(mktemp -d /tmp/qwen-campaign.XXXXXX)

# The checked-in recipe has 24 prefixes, enough for 3 warmups + 20 pairs.
# Generate one 128-prefix fixture for the entire campaign, including confirmation.
# This requires the pinned Python environment's tokenizers package, not weights.
python3 scripts/qwen3_embedding/generate_exact_token_fixture.py \
  --fixture scripts/qwen3_embedding/fixtures/qwen3_embedding_0_6b_exact_tokens.json \
  --tokenizer-file "$embedding_tokenizer" --prefixes 128 \
  --output "$campaign_dir/embedding-fixture.json"

for run in 1 2; do
  python3 scripts/benchmark_resources.py \
    --output "$campaign_dir/embedding-run${run}-resources.json" --timeout 2400 -- \
    python3 scripts/benchmark_qwen_pair.py \
    --baseline "$baseline_server" --candidate zig-out/bin/antfly-inference-bench-server \
    --models-dir "$models_dir" --model-file "$embedding_gguf" --phase embedding \
    --fixture "$campaign_dir/embedding-fixture.json" \
    --length 20 --length 256 --length 511 --length 2551 --length 4096 --length 8192 \
    --batch-lengths 20,511 --batch-lengths 20,256,511,2551 \
    --warmup 3 --iters 20 --output "$campaign_dir/embedding-run${run}"

  python3 scripts/benchmark_resources.py \
    --output "$campaign_dir/ocr-run${run}-resources.json" --timeout 7200 -- \
    python3 scripts/benchmark_qwen_pair.py \
    --baseline "$baseline_server" --candidate zig-out/bin/antfly-inference-bench-server \
    --models-dir "$models_dir" --model-dir "$vl_model_dir" --phase ocr \
    --golden scripts/qwen3vl/fixtures/ocr/golden_q4_k_m.json \
    --warmup 3 --iters 20 --output "$campaign_dir/ocr-run${run}"
done
```

Repeat the embedding command with only `--length 8192`, no `--batch-lengths`,
and `--iters 100`, writing `embedding-confirm-8192`. Repeat OCR with
`--case portrait --iters 100`, writing `ocr-confirm-portrait`. Keep the same
fixture and other arguments; write each adjacent `<run>-resources.json` too.

```sh
python3 scripts/check_qwen_performance.py --campaign-dir "$campaign_dir" \
  --output "$campaign_dir/retention-gates.json"
```

The checker reproduces statistics from every raw pair and verifies identity,
workload coverage, parity, and resource limits. Primary wins require at least
5% lower median latency and a paired 95% interval excluding no improvement;
confirmed regressions above 3% median or 5% p95 fail. A runner's `pass` field
alone only establishes output parity.

## Rollback and attribution

| Optimization | Baseline-only environment setting | OCR ablation label |
| --- | --- | --- |
| Q4_K high-row matrices | `TERMITE_METAL_DISABLE_Q4_K_HIGH_ROW_MM=1` | `q4` |
| Q6_K high-row matrices | `TERMITE_METAL_DISABLE_Q6_K_HIGH_ROW_MM=1` | `q6` |
| Cross-layer eager decode frame | `TERMITE_METAL_ENABLE_QWEN3VL_FORWARD_DECODE_FRAME=0` | `decode_frame` |
| 128-wide dense causal attention | `TERMITE_METAL_DISABLE_DENSE_CAUSAL_HD128=1` | — |
| BF16 simdgroup matrices | `TERMITE_METAL_DISABLE_BF16_MM=1` | — |
| Dense Qwen3 embedding/reranker prepared prefill | `TERMITE_METAL_DISABLE_QWEN3_PREPARED_PREFILL=1` | — |
| BF16 vector loads | `TERMITE_METAL_DISABLE_BF16_VECTOR_LOADS=1` | — |
| BF16 gate/up/SiLU fusion | `TERMITE_METAL_DISABLE_BF16_FUSED_GATE_UP=1` | — |

For each OCR ablation, pass the candidate binary as both endpoints and set only
that control with `--baseline-env SETTING`. Use `--case portrait --warmup 3
--iters 20`, repeat in fresh processes, and save
`ocr-ablation-<label>-run{1,2}` plus adjacent resource reports. Keep the same
candidate and settings as `ocr-run2`, which supplies the default-path reference.

```sh
python3 scripts/check_qwen_performance.py --ocr-ablations \
  --campaign-dir "$campaign_dir" --output "$campaign_dir/ocr-ablation-gates.json"
```

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
only when deliberately establishing a new baseline. The embedding generator
verifies every expanded token sequence, including EOS. See
[the embedding protocol](scripts/qwen3_embedding/BASELINE.md) for tokenizer,
pooling, normalization, and independent-reference requirements.

For long CPU FP32 references, `transformers_embedding_oracle.py --attention sdpa`
disables unnecessary autoregressive K/V caching and records the attention
implementation. Qualify SDPA against eager first; the recorded 2551-token check
had cosine 0.9999999999876741 and maximum absolute difference 5.7e-7. Then use
`qualify_qwen3_embedding_metal.py --oracle /path/to/oracle.json --tier q8_0`.
Reference generation and qualification also run under the resource guard.

## Evidence artifacts

### Latest implementation and regression evidence

The local report is `/tmp/qwen-implementation-report.md`; its campaign is
`/tmp/antfly-qwen-competitive-_bj2oazs`. Key records are:

- `candidate-v10-source.json`: source hashes matching `97a723bd13`, based on
  `ce9a2abb39`, with debug-symbol stripping as the only build-only delta.
- `q8-metal-511-metadata-confirmation-v10-b10809.json` and
  `bf16-metal-511-metadata-confirmation-v10-b10809.json`: final embedding
  means, intervals, decisions, model/fixture hashes and raw campaign paths.
- `rerank-metal-511-confirmation-v7.json`: the earlier reranker timing.
- `reranker-quality-v10-b8990/results.json` and
  `reranker-fp32-oracle-v10-binding.json`: final score checks, saved-score
  provenance and the retained b10809 score failure.
- `final-confirmation-independent-audit-v10-b10809.json`: independent
  recomputation of final embedding statistics without sample exclusions.

The regression report is `/tmp/qwen-gemma-gliner-regression-97a723bd/report.md`;
its adjacent `summary.json` binds model runs, resource guards and source hashes.
These are local evidence paths, not checked-in or published artifacts. Preserve
the reports, referenced launchers, raw results and guards with the PR evidence.

| Build | Executable SHA-256 |
| --- | --- |
| Antfly v10, final embedding and correctness | `ae2b3099a8431fbff5c803bb01649ddbfe1f2c35f3363d943bfba719f81c7a99` |
| Antfly v7, earlier reranker timing | `e86b8b8adf2b95c75f530e01606b24212d05a6c011bdbe267eaf0e09e0656e08` |
| llama.cpp b10809, revision `5266f24da75dc449bd56cbed7addb9c8e4a6a73e` | `d707b6db4c1397a7383176fba12d339e5b33c7513669d74c8fbc2a76f6979a72` |
| llama.cpp b8990, revision `660b1b4bdc6fedc18e8c3d87a945ffb51f91c547` | `8eee1b1fa1c65d919c94116dd286134a4a9498059c21c1ee448c45fb87f2c590` |

### Historical archives

The earlier comparison report and reranker checks are in
`qwen-performance-report-evidence-2026-09-08.tar.gz`
(SHA-256 `f109bd1e38daae9339a911e2426f7209fe7c81e9b3eb1e52b0705881366288ff`). This bundle also includes the
previous ReleaseFast evidence archive below; model weights and executables are
excluded. The standalone report is `/tmp/qwen3vl_q4-performance-report.md`.

The ReleaseFast comparisons and BF16 kernel work are preserved in
`qwen-releasefast-evidence-2026-09-08.tar.gz`
(SHA-256 `ac30ce41747e220efd74f0e138385a0868b3e11fe107ae511465af4641bcaccb`).
It includes raw vectors/timings, resource guards, build/model identities, the
runtime patch, temporary benchmark launchers, and rejected kernel probes.

Generated reports, logs, source snapshots, and profiling output belong in a
separate benchmark artifact, not the scripts source tree. Preserve all samples,
resource reports, model/build/fixture digests, commands, and source identity
alongside the review. The historical OCR/embedding campaign and reranker checks
were preserved in `qwen-performance-evidence-2026-09-08.tar.gz`:

`SHA-256: 8fbcd0eee6827485e70a7ab7e653c7d5531f60e12bf453c82fb69ea9e4796a61`

The archive contains the original evidence index and source reconstruction
instructions, all removed reports, the original fixtures, and the reranker
runner and raw confirmation results. It excludes model weights and executables.
Historical executable digests are:

- Baseline: `9f645aca65d0e27071d22d2cd80043ba41a702fc397e48815179316e3eba7ca2`
- Candidate: `dfd388801260a146070827bae3d1ba6bd0f49958ad52e8a1f7dd7f4db6d1c5c0`

Pinned model digests are in the immutable model catalogs; the artifact records
identify exactly which files and revisions were measured. The reranker checks
cover the 0.6B Q8 artifact `22c9979ce4fbcdc5acdc310c6641c32797eff1aa980b8f7a2db8a8ea23429a48`.
The converted embedding BF16 GGUF has SHA-256
`bd8dae3fb527951d8376165b5ca5019656368759d6c64277297fd4ee19b2b359`;
its source revision and conversion provenance are recorded with the campaign.
