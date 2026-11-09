# Native and Python latency after tokenizer and FP32 projection optimization

The native managed HTTP API now edges the Python reference on the fixed warm
F32 benchmark: **0.821 s versus 0.826 s median**, a **0.6% latency reduction**.
A Python control using the same cuBLAS libraries measures **0.829 s**; native
is **1.0% lower** in that comparison. Both native pass medians are below both
Python pass medians in either comparison. All generated token sequences match.

This is near parity with a narrow observed win on this workload. It is not a
general throughput or statistical superiority claim. A paired case-bootstrap
diagnostic for the packaged Python comparison gives a 95% interval of
approximately -17 to +21 ms for Python minus native median latency. An earlier
single-pass Python run measured 0.819 s, reinforcing the need to retain the
individual receipts and the comparison scope.

## Matched measurements

Each engine executes two serial passes over the same frozen 24 questions: 48
timed requests per engine. Both use the released Stage3 reader, the pinned
Qwen3-Embedding-4B and Qwen3-8B models, F32 weights and activations, F32 KV,
disabled TF32, greedy decoding, the native system instruction, disabled
thinking, and the same persisted native document vectors. The maximum is 48
new tokens; actual completions contain 2–6 tokens including EOS. An initial
warmup, model loading, startup verification and document ingestion are excluded.

| Engine | Combined median | Pass 1 median | Pass 2 median |
| --- | ---: | ---: | ---: |
| Native supervised loopback HTTP API | **0.821018 s** | 0.822116 s | 0.821002 s |
| Python Transformers, packaged BLAS | 0.825994 s | 0.825354 s | 0.826165 s |
| Python Transformers, native BLAS libraries | 0.829052 s | 0.828036 s | 0.829262 s |

Native measures the complete HTTP request, including validation, artifact
checks, admission, question encoding, reader execution, framing, decoding and
request cleanup. Python measures its resident in-process encoder, reader,
framing and decoder pipeline with CUDA synchronization. This is not a sum of
separately timed components. Both Python runs match all 48 native answers and
token-ID sequences, which also match the previous native diagnostic.

The device is NVIDIA GB10, compute capability 12.1, driver 580.178.04. The Zig
compiler is 0.17.0; Python uses Torch 2.9.1+cu130 and Transformers 4.53.3.
Native loads system cuBLASLt 13.4.1.3; packaged Python loads cuBLASLt 13.0.0.
The library-matched control preloads both system cuBLAS and cuBLASLt in its
subprocess and verifies their loaded paths and version. It does not alter the
Python environment or installed packages. Native and Python execute separately
with both base models resident; they do not compete for GPU memory. Builds
finish before the final timing runs.

## Changes

Warm requests previously reparsed separate encoder and decoder tokenizers even
when their models already retained admitted tokenizers. `GenerationAssets`
now borrows the resident tokenizer, template and verified manifest through a
model handle. Full artifact-generation checks precede the borrow. Cold preflight
still owns and validates its manifest and tokenizer independently, allowing
invalid cold requests to reject before GPU loading. Generation-config overlays
are preserved when structural config is borrowed from the loaded decoder.

Warm execution also uses the manager's existing lookup for the exact cached
backend and publication. It avoids constructing an unused cold-load flight key;
cache misses retain normal coordinated loading. These changes introduce no
additional persistent cache or request KV retention. The model's existing
tokenizer lease remains charged, and the borrow pins its lifetime.

The qualified Memorilla decoder enables a bounded FP32 cuBLASLt projection
policy on GB10. For selected 4096/12288-wide projections with 33–128 rows, a
256-row descriptor query or the second valid zero-workspace FFN recommendation
selects a faster algorithm. Every algorithm is checked against the actual
matrix layouts, types, pointer alignments and workspace before submission.
The actual GEMM uses the original row count: no padding or probe computation
executes. Unsupported recommendations retain the ordinary validated fallback.
Other shapes, dtypes and model profiles retain their existing policy.

There is no request-time tuning, added CUDA workspace, reduced-precision
arithmetic, graph replay or answer caching. The previous reader-residency
change remains bounded to one admitted reader per loaded decoder.

| Native stage | Warm API median |
| --- | ---: |
| Reader residency, start of this continuation | 1.404 s |
| Resident tokenizer reuse | 1.040 s |
| FP32 projection selection | 0.904 s |
| Verified metadata and resident-model lookup reuse | **0.821 s** |

Intermediate API runs contain one pass of 24 questions; the final run contains
two passes. The reduction from the start of this continuation is approximately
41.5%. Flushed verifier events measured request preparation falling from about
70–75 ms to approximately 1–10 ms, and prefill plus three cached steps falling
from about 0.856 s to 0.711 s. These intervals include boundary checks and
diagnostic overhead; they are not isolated GPU kernel durations.

## Qualification and limits

The final build passes 115 focused native tests with zero skipped tests. The
managed numerical verifier passes its unchanged tolerances for the exact native
query, memory rows, injected embeddings and all 151,936 vocabulary logits at
prefill and three cached steps. It also passes cancellation after prefill,
bitwise A/B/A isolation, physical DeviceBuffer limits, and the 64-document,
128-token execution bound. New assertions prove that tokenizer borrowing pins
the decoder during preflight, that the FP32 policy executes, and that decoder
eviction destroys the reader and refunds its admission.

Live HTTP checks pass all 48 output comparisons, cold over-limit rejection
without GPU loading, Stage3/Stage2/Stage3 switching, concurrent profiles, static
and in-flight reader drift, and deadline recovery. Drift returns retryable 503
without answer text and recovers in the same supervisor. All 30 source-verified
HTTP contract requests pass, including known answers, concurrent A/B/A,
deadline recovery and the legacy latent wire schema.

An initial unfiltered test-build attempt exposed three existing download-test
fixtures missing `host_caches`; that is not claimed as a passing full-repository
suite. The final focused suite includes the previous 111 tests plus FP32 policy
and cuBLAS fallback coverage. Earlier failed diagnostic, build and test attempts
are preserved separately from the final qualification receipts.

The previous 117-request hybrid quality qualification remains historical
evidence. This work does not rerun that entire corpus or qualify latent factual
QA. Released Stage3 still scores 0/22 on the frozen factual diagnostic; the
unchanged output sequences preserve that failure. No checkpoint is promoted,
the operational heldout split remains sealed, and the route stays disabled by
default. Mixed/BF16 arithmetic and F16 KV retain their separate failed or
unqualified status.

## Evidence

Evidence is under
`.benchmark-results/memorilla/performance-native-target-20261109/`:

- `metadata-http/result.json`: two native timing passes and robustness checks.
- `metadata-http/verified-http-smoke.json`: all 30 contract requests.
- `metadata-composed/`: final numerical, physical and lifetime traces.
- `metadata-native-tests.log`: 115 selected, 115 passed, zero skipped.
- `python-f32-final/` and `python-f32-library-matched/`: independent references.
- `python-final-comparison.json` and `python-library-matched-comparison.json`:
  exact-output and latency comparisons.
- `metadata-source/`, `metadata-binaries/` and `metadata-source-snapshot.json`:
  frozen sources and executable artifacts.
- `validation-final.json`: aggregate validation, receipt hashes and cleanup.

The API binary SHA256 is
`a38c3bc05319821d612f069fa812270e904ed5d63430f3e0b28127be4a014378`.
The numerical verifier SHA256 is
`44e02a157ef922cc5ce5b2fbbcf8db1ef0d280dfd4f0c91356e2d130f091a220`.
The 83-file source snapshot SHA256 is
`2894c69dc65f2d33147f5337b71bf860e55528044f426cec25c31c3a4f087b40`.
Source files and archived copies match the working runtime. Builds use the
worktree's own caches and the existing `zig-out` prefix.

Original managed model assets remain unchanged. Owned supervisors, GPU work,
drift probes and driver caches are retired, and `make gc` respects the build
cache safeguards. The [earlier reader-residency report](2026-11-09-native-performance.md)
and [latent operational audit](2026-11-09-latent-operational-qualification.md)
retain the preceding measurements and quality findings.
