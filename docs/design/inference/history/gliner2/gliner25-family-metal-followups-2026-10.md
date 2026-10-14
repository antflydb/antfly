# GLiNER2.5 family Metal and Decide-1B performance follow-ups (2026-10)

> Relocated verbatim from `zig/pkg/inference/models/gliner2/GLINER25_FAMILY.md` (lines 97–142 and 144–184 at commit a7c147e446) on 2026-10-12. Durable decisions were folded into [GLINER25_FAMILY.md](../../../../../zig/pkg/inference/models/gliner2/GLINER25_FAMILY.md#performance) before the move.

## Metal performance follow-up (2026-10-06)

Local FP32 Apple M4 measurements use the exact artifacts above and a two-thread
CPU math budget. Multilingual Metal times below include the in-process HTTP
handler; Python includes schema construction, preprocessing, model execution,
and decoding, with synchronized MPS and CPU fallback disabled. The 1B Metal
rows use the direct pipeline boundary described above. Each row contains
twenty warm samples after three warmups; these are descriptive serial
latencies, not concurrent throughput or service SLOs.

| Request | Prepared tokens | Metal median ms | Python MPS median ms |
| --- | ---: | ---: | ---: |
| multi-Decide Spanish entities | 61 | 16.76 | 28.18 |
| multi-Decide described choice | 97 | 25.68–25.91 | 31.82 |
| multi-Decide choice/score/NoUL | 167 | 40.32–40.37 | 45.81 |
| Decide-1B described choice, direct pipeline | 87 | 122.04 | 128.88 |
| Decide-1B choice/score/NoUL, direct pipeline | 158 | 177.42 | 200.11 |

Packed boundary QKV reduces three projections to one matrix multiplication
and a split/bias epilogue. Same-binary AB/BA measurements showed about 5–6%
lower extraction latency. Its additional immutable resident storage is
81 MiB for the 12-layer, 768-wide base/multilingual geometry and is included
in model admission. The diagnostic disable flag retains this storage, so it
compares execution cost rather than the previous memory envelope.

The strict FP32 resident FFN combines bias/exact GELU and bias/residual/centered
LayerNorm epilogues while reusing the admitted product workspace. Same-binary
AB/BA Decide measurements showed a further 4–6% median reduction in both
orders. Paired synthetic FFN frames at 97–218 rows showed approximately 3–7%
lower time with exact outputs; the 61-row result was inconclusive. Missing
fused pipelines fall back before acquiring weights or changing scope state.
Both optimizations retain their scalar numerical contract and cancellation
boundaries.

Host GPU scheduling caused large swings in some short extraction runs; all
samples remain in the receipts. The 1B encoder counters attribute most time
to GPU execution, with fourteen production frame submissions. No wider
cancellation cadence or attention prototype was promoted. Standard English
base/Decide measurements use different requests and timing boundaries and
are historical context rather than a matched family comparison. Local
receipts are under `.benchmark-results/gliner25-metal-opt-2026-10-06/`.
The final multilingual matrix passed 21 tests without skips, and the physical
fused-FFN suite passed four tests without leaks. The final canonical 1B direct
pipeline job also passed without skips or leaks, with source identity unchanged
during the run. Its two p95 values were 123.68 and 178.83 ms; scheduling outliers
in the longer multilingual AB/BA runs still preclude a service-tail claim.

## Decide-1B production-allocator follow-up (2026-10-07)

The standalone `inference-bench-server decide-bench` command measures the
loaded pipeline, encoder/head, direct Node call, and in-process HTTP handler
separately with the production allocator. The typed Decide span route avoids
the extraction JSON round trip, tokenizer listing projects marker metadata
without allocating the vocabulary, and native/Metal compute implement the
existing packed exact-GeGLU hook. Native M-RoPE also shares each token's phases
across heads; Accelerate attention specializes single contiguous segments up
to 198 tokens with 64-wide heads and retains the general fallback.

The latest completed exploratory Metal block below includes the typed route,
tokenizer projection, and packed GeGLU. It predates the final CPU attention and
M-RoPE changes and subsequent review fixes. These numbers are not final-source
qualification. Both processes use the pinned FP32 checkpoint, two CPU threads,
three warmups, and twenty measured samples per case on an Apple M4. Python is
3.12.3 with torch 2.9.1 and transformers 5.17.0. Antfly includes complete HTTP
handler dispatch, admission, and serialization; Python measures the loaded
pipeline. Neither includes socket transport.

| Request | Tokens | Antfly Metal median ms | Python MPS median ms |
| --- | ---: | ---: | ---: |
| Binary short holdout | 40 | 105.99 | 69.72 |
| Four-label holdout | 79 | 134.50 | 125.18 |
| Described choice | 87 | 136.87 | 130.23 |
| Score/NoUL holdout | 93 | 141.06 | 131.23 |
| Choice/score/NoUL | 158 | 199.70 | 206.82 |
| Longer mixed holdout | 174 | 210.67 | 221.16 |

All six cases passed the run's output and ownership checks. Performance
acceptance failed: only one of six required paired blocks ran, shorter cases
still regress, several p95 ratios exceed the 5% ceiling, and whole-process host
swap grew by 0.62 MiB. This does not establish an overall win over Python.
The final production build was cancelled before fresh full-model CPU/Metal
runs. The final CPU optimizations have focused numerical and allocation-failure
coverage, but their full-model performance remains unmeasured.

Campaign instructions and acceptance boundaries are in the
[development tools README](../../../../../zig/pkg/inference/scripts/gliner25/README.md#decide-1b-production-allocator-comparison).
The local exploratory receipt is
`.benchmark-results/gliner25-win-2026-10-07/metal-packed-exploration/comparison.json`.
