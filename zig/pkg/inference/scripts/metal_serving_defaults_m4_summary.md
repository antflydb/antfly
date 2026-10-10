# Metal serving default qualification — Apple M4, 2026-10-02

This receipt covers the merged branch plus the default-promotion changes. The
candidate runs use no Qwen or Gemma enable flags. Reports, Gemma request/output bodies, server
logs, host samples, executable/model hashes, and failed attempts remain locally
under ignored `.benchmark-results/default-promotion-20261002/`. Qwen reuses the
exact fixtures in `.benchmark-results/qwen3-metal-gap-20261001/`; their hashes
are recorded in each stage provenance receipt.

## Default policy and rollback

The new defaults require an Apple M4-family Metal device (Apple GPU family 9 and
an `Apple M4` device name). CPU and other Apple generations retain their prior
defaults. Explicit enable flags remain available for qualification elsewhere.
Device qualification is independent of prepared-frame rollback switches.

| Feature | Default eligibility | Set to `0` to roll back |
|---|---|---|
| Qwen Q8_0 short projections, norms, and encoder scope | Eligible dense text embedding; admitted bounded workspace; existing row/shape checks | `TERMITE_METAL_ENABLE_Q8_0_SMALL_ROWS` |
| Qwen head norm/rotary | Bounded 9–64 rows, HD128, full rotary, supported head count/SIMD width | `TERMITE_METAL_ENABLE_QWEN3_HEAD_NORM_SG` |
| Qwen bounded batching/admission | Eligible resident dense Qwen3 embedder; dynamic unpadded tokenizer; no projection | `TERMITE_METAL_ENABLE_QWEN3_EMBED_BATCHING` |
| Gemma4 E2B local flash prefill | Existing exact E2B MQA HD256/window512 geometry | `TERMITE_METAL_ENABLE_E2B_FLASH_PREFILL_HD256` |
| E2B aligned Q4_0 projections | Existing E2B dimensions, dtype, buffer alignment and pipeline checks | `TERMITE_METAL_ENABLE_E2B_Q4_0_MM_SG_ALIGNED` |
| E2B fused Q4_0 gate/up | Existing E2B FFN dimensions and pipeline checks | `TERMITE_METAL_ENABLE_E2B_Q4_0_PAIR_ACTIVATION_MM` |
| E2B resident nucleus sampling | Exact E2B model geometry and supported compiled Metal sampling request | `ANTFLY_INFERENCE_METAL_RESIDENT_NUCLEUS` |

Empty, `false`, `no`, and `off` also disable these flags. Existing feature-specific
`DISABLE` flags still win; restart the server after changing controls. Missing
optional default pipelines use the existing fallback; explicit pipeline requests
retain fail-closed qualification. M64 short-row comparisons remain opt-in.

API sampling defaults are unchanged. Greedy requests use the greedy path;
resident nucleus requires temperature > 0, top-k 0, 0 < top-p < 1, no min-p or
penalties, and no grammar/static suppression. Other requests use their existing
sampling path. E4B/A4B policies are unchanged.

## Qwen3-Embedding-0.6B Q8_0

Three alternating rounds, 20 measured requests per endpoint/round, three warmups,
identical GGUF bytes, exact tokenizer fixtures, cache-neutral inputs and required
Metal execution. Short/capacity comparisons use llama.cpp; regression comparisons
use the fixed merged Antfly baseline with serving controls explicitly `0`.

| Input | Tokens | Candidate mean latency | Worst 95% throughput lower bound vs llama.cpp |
|---|---:|---:|---:|
| Document | 20 | 22.59–23.17 ms | 1.028× |
| Document | 32 | 22.81–23.70 ms | 1.030× |
| Document | 64 | 33.92–34.56 ms | 1.165× |
| Query | 32 | 22.60–23.25 ms | 1.040× |
| Query | 64 | 34.20–34.34 ms | 1.163× |

- **15/15 short cells passed:** worst p95 ratio 0.974×; minimum cosine 0.99999776.
- **3/3 capacity rounds passed:** 32×256 mean 4.633–4.685 s; worst throughput
  confidence lower bound 1.199×; minimum cosine 0.99998990.
- **18/18 baseline regression cells passed:** singleton 256/511/2551 and batches
  8×20, 32×20, 8×256; worst lower bound 1.008× (required ≥0.95×).
- **25/25 HTTP correctness/recovery checks passed:** ragged ordering, reduced
  dimensions, query-prefix bounds, per-item/fail-fast errors, overload,
  disconnect and actual eviction/reload with one loaded model.
- **Unconditioned 30-minute soak passed:** 291 requests / 9,312 vectors, eight
  workers, zero failures/restarts/swap growth, final-half process-tree RSS range
  0.531 MiB (limit 32 MiB), minimum same-binary cosine 0.9999999999999998.
  Wall duration including drain was 1,828.2 s. Only the normal geometry warmup
  preceded measurement; no extra RSS conditioning was used.

Short, capacity and soak used process 4096 MiB, derived scratch 384 MiB and
combined 2048 MiB. The correctness and long-singleton regression lanes used
explicit scratch 1536 MiB / combined 3072 MiB. These caps are part of the receipt.
The earlier historical unconditioned RSS failure remains preserved; this is a
fresh passing run, not a reclassification of that attempt.

## Gemma4 E2B QAT Q4_0

| Workload | Baseline → default | Estimate and 95% interval |
|---|---:|---:|
| Short greedy decode | 56.692 → 56.497 tok/s | 0.9966× [0.9896, 1.0011] |
| Long greedy decode | 53.406 → 53.292 tok/s | 0.9979× [0.9896, 1.0027] |
| Long greedy TTFT | 19.889 → 8.102 s | 0.4074× [0.3972, 0.4266], 59.3% lower |
| Short sampled decode | 20.283 → 54.008 tok/s | 2.6628× [2.6472, 2.6758] |

All paired promotion gates passed; every greedy output SHA256 matched. The
20-request long-input sampled run completed 10,240 output tokens with zero
errors/restarts, at **44.008 tok/s**, TTFT **11.666 s median / 12.398 s p95**.
Decode ranged from 39.378 to 51.151 tok/s. Two-second process-tree physical
footprint samples peaked at 2488.034 MiB; final-half range was 139.438 MiB
(no physical-footprint plateau gate was imposed). Host swap and swap-outs did
not grow; `pmset` reported no thermal/performance warning before or after.
These host readings do not establish the cause of the timing variation.

The historical absolute long-sampling targets of 55 tok/s and 7 s TTFT remain
unmet. This promotes measured, correct and reversible improvements; it does
not declare those absolute targets achieved.

Separate default diagnostics recorded 192 resident nucleus draws, 84 generated
local flash-prefill dispatches, 35 aligned projections and 35 aligned fused
FFNs (cumulative warmup + measured request), with zero compiled-frame or scratch
fallbacks. Seven serving checks passed: warmup, natural EOS, three unsupported
sampling configurations, early disconnect and subsequent greedy recovery, with
unchanged worker PIDs. Unsupported top-k, penalties and top-p 1 produced no new
resident nucleus draws. Explicit `0` rollback restored greedy baseline hashes
and zero resident nucleus draws for sampling.

Five alternating fresh-server A/B pairs, 64-token warmup, thinking/prompt caching
off, identical checkpoint, max idle prefill chunk 2048. Short input is 27 tokens;
long input is 4031 tokens; each measured response completes 512 output tokens.
Sampled runs use temperature 0.8 / top-p 0.95 / top-k 0. Decode rate is
(completion tokens − 1) / (last-token time − first-token time). TTFT includes the
HTTP request and prefill. Confidence intervals resample complete A/B pairs
(4000 bootstrap draws, seed 20261002); greedy promotion requires output hash
parity and decode lower bound ≥0.97×, with improved long-input TTFT.

The previous 5.55% long-greedy slowdown did not reproduce in the fresh prefill-only
five-pair control (53.139→53.020 tok/s; ratio 0.9978, lower bound 0.9929).
The cause of the historical difference was not established.

The old sampled long-input baseline exceeded the retained 180-second deadline
with only 79 content chunks. It has no complete-output throughput ratio. Sampling
speedup therefore uses complete short-input pairs; the long-input candidate is
qualified separately by complete sustained requests. The first serving harness
attempt hit configured one-request admission during asynchronous cleanup; the
passing check uses the timed workload's one-second request gap.

Server caps were process 6144 MiB, combined 5632 MiB, KV 512 MiB, scratch 512 MiB;
one loaded model and one concurrent timed request. Diagnostics are separate from
timed runs. These relative promotion gates do not establish an absolute throughput
or TTFT target for every workload.

## Validation and provenance

- Metal ReleaseFast build passed (14/14 steps). Frozen baseline/candidate
  executable and model hashes were attested by each endpoint receipt.
- Focused Metal suite: 34 selected, **32 passed / 2 skipped**, plus **29 supporting
  tests passed**. The two skips are optional published E2B decoder/projector
  fixture checks. Physical short-row/tail, HD128 norm/rotary, bounded workspace,
  full-vocabulary/tie/softcap nucleus and cancellation oracles ran and passed.
- Production-size E2B prefill routes: **8/8 passed**, bit-identical output hashes
  against independently disabled routes and exclusive expected counters.
- Metal-disabled build: **7 selected / 7 passed / 0 skipped**, plus **29 supporting
  tests passed**. Qwen Python suite: **132 passed**; local Gemma HTTP helper:
  **7 passed**. Formatting and diff whitespace checks passed.
- Final-binary explicit Qwen rollback: **6/6 cases bit-identical** to the fixed
  baseline, covering short document/query and batch shapes. Gemma rollback
  greedy parity and canonical sampled fallback also passed.

Physical qualification used a **MacBook Air M4, 16 GiB (Mac16,12), macOS 26.5**.
It covers the above model bytes and workload/caps. CI, other Apple hardware,
larger Qwen models, other precision tiers and full 32K context were not qualified
here; M4 Pro/Max share the device policy but were not measured on this host.

| Artifact | SHA256 |
|---|---|
| Baseline ReleaseFast Metal executable, clean `4d794fb77843c12ce051655ac1a909611f151f48` | `ed465ba4992441f5b20f868f5c3c67d99091a4c3b25de432be0c386fe40daaed` |
| Default ReleaseFast Metal executable, same HEAD plus promotion source changes | `bf18762e96bd5db21546454c1c5b011c84937bc9079e877cf5025cb8bda28a07` |
| Frozen promotion patch (`default-source.patch`) | `46dbc01b697d06f7ec009f1420afedb4e21ec6af30df39a3cee39f1ba56b1a02` |
| Qwen GGUF (639,150,592 bytes) | `06507c7b42688469c4e7298b0a1e16deff06caf291cf0a5b278c308249c3e439` |
| Gemma E2B QAT GGUF (2,620,370,976 bytes; Q4_0/F32 tensors) | `e531007218dfab990486a5de7676a6932d6ea8dea233d1f698d7c21cf8a16889` |
| Raw-receipt manifest (`final-receipts-sha256.json`) | `1750db80adb29fffd5dac26ff2fdf80f0ec4e2b8f7b48e4c7058d7bd9b24c379` |

`default-build-provenance.json` records individual production-source hashes;
they were checked before each staged qualification and again at completion.
Qwen stages use the tracked `qwen3_embedding/run_qwen3_embedding_gap_qualification.py`.
The local Gemma HTTP/pair/stage drivers and exact prompts are preserved alongside
the receipts and covered by the manifest. No full JSON results or weights are
added to Git.
