# Native Memorilla reader residency

The native managed API now retains a released reader between requests. On the
same 24-question F32 diagnostic, warm median latency fell from **3.680 s to
1.404 s**, a **2.62× speedup** and **61.9% reduction**. All output token IDs and
answer strings match the previous native run. This improves execution latency;
the released reader's exact operational-fact accuracy remains unqualified.

## Change and resource lifetime

Previously every request opened and hashed the reader bundle, converted its
weights to F32, initialized CUDA, uploaded the weights and destroyed the owner.
A three-iteration load diagnostic measured approximately 1.932 s for warm bundle
verification, 0.211–0.271 s for conversion, 0.206–0.208 s for CUDA initialization,
0.043 s for upload and 0.15 s for teardown. These are isolated load-phase timings,
not additive components of the API median.

Each loaded decoder now owns at most one reader cache entry. The entry retains
the complete existing admission lease, including 776,495,104 bytes of device
weights, 128 MiB of CUDA workspace and the conservative host conversion peak.
An idle reader remains charged. A profile switch destroys the old reader before
reserving its replacement. Decoder eviction destroys the reader before
refunding its lease; active decoder handles pin that lifetime.

Cache misses retain full bundle cryptographic verification and the released
checkpoint allowlist. Hits check canonical bundle paths and the inode, size,
mtime and ctime of the directory, manifest, config and weights. Requests verify
that generation again after computation. Drift invalidates the entry and
returns retryable `MODEL_ARTIFACTS_CHANGING` without an answer. An invalid reader
is destroyed after its request's GPU scopes drain. Admission runs outside the
cache mutex so model eviction cannot reenter a held cache lock.

The reader borrow serializes access to its mutable backend. Request tensors,
document projections, execution budgets and decoder KV retain separate request
lifetimes and cleanup. Managed base-model authority checks, F32 arithmetic,
F32 KV, physical allocation ceilings and operator limits are unchanged.

## Timing protocol

The native timings measure serial loopback HTTP requests through the supervised
worker on NVIDIA GB10. Both native revisions use the same released Stage3
reader, Qwen3-Embedding-4B, Qwen3-8B, stored document vectors, question framing,
greedy decoding, disabled thinking and 48-token maximum. Actual answers contain
2–6 tokens including EOS. A warmup request is excluded. Document ingestion,
startup verification and cold model loading are excluded.

| Path | Warm median | Range |
| --- | ---: | ---: |
| Previous request-owned reader | 3.680 s | 3.445–4.042 s |
| Retained reader, final run | 1.404 s | 1.166–1.754 s |
| Fresh Python Transformers F32 GPU | 0.870 s | 0.600–1.378 s |

A separate first run measured 1.400 s and matched all 24 outputs. Its API
contract test incorrectly expected HTTP 409 for artifact drift; the existing
server contract returns retryable 503. That incomplete receipt is preserved.
The final run saves error responses before asserting their status and requires
the existing 503 contract.

The comparison with Python measures the full resident question encoder,
reader, prompt framing and decoder in Transformers CUDA eager mode, with F32
and TF32 disabled. It excludes document ingestion. Native timing also includes
its serving and lifecycle checks. These measurements do not establish a kernel
speed ratio, vLLM throughput, concurrent throughput or long-answer latency.

The fresh Python run matches every native answer and token sequence. Native
latency is now **1.61×** the Python median. The previous audit's Python median
was 0.819 s; the fresh run is retained separately rather than replacing that
historical receipt. Both Python runs use Torch 2.9.1+cu130 and Transformers
4.53.3. The fresh median components are 0.167 s for question encoding, 0.006 s
for the reader, 0.325 s for prefill and 0.364 s for cached decoding. Component
medians do not sum to the end-to-end median. Reader initialization no longer
dominates warm native requests; the remaining gap needs decoder and request
setup profiling.

## Qualification evidence

The CUDA build and 111 selected native tests pass with zero skipped tests.
The actual managed composition verifier passes with the original numerical
tolerances: memory rows, injected embeddings and all 151,936 vocabulary logits
at prefill and three teacher-forced cached steps have zero out-of-tolerance
elements. It also passes cancellation after prefill, exact A/B/A isolation,
physical allocation budgets and the 64-document/128-token execution bound.
An active decoder handle prevents eviction; releasing it permits decoder
eviction and returns the reader's admission credit.

Live tests pass all 24 previous output sequences, Stage3/Stage2/Stage3 profile
switching, concurrent profiles, static and in-flight reader-directory drift,
and deadline recovery. Drift returns 503 without answer text and serving
recovers in the same supervisor. The separate source-verified HTTP suite passes
all 30 requests, including known factual answers, exact concurrent A/B/A,
deadline recovery and the unchanged legacy latent response schema. The previous
117-request hybrid quality qualification is historical evidence; this change
does not claim to have rerun that entire corpus.

Flushed verifier stage events measure warm reader acquisition at
**0.44–1.10 ms**, compared with the previous 2.280–2.287 s reader-load interval.
The same retained-reader run measures question encoding at 0.119–0.121 s,
prefill plus three cached steps at approximately 0.841 s, and the 128-token
case at 17.991 s. These are host observation intervals, including stage checks,
rather than isolated GPU kernel timings. Cold reader acquisition still takes
2.287 s in that run.

The completed aggregate receipt is
`.benchmark-results/memorilla/performance-20261109/validation.json`. Original
managed encoder, decoder and Stage3 assets match all 31 recorded file metadata
entries. Owned drift probes, server processes and GPU work are retired. The
task's CUDA cache was removed. `make gc` completed and respected the recent-write
safeguards for the worktree's Zig build output. Source hashes, Zig formatting
and `git diff --check` pass. The API remains disabled by default.

The [latent operational qualification](2026-11-09-latent-operational-qualification.md)
records the independent accuracy failure: released Stage3 is 0/22 on the frozen
factual diagnostic in both native and Python F32. Matching those outputs proves
execution preservation, not factual correctness. No trained candidate is
promoted and the sealed operational test split remains unopened.

## Reproducibility

The optimized inference binary SHA256 is
`42d0533715202844af4ee9da8b497156de2eb460626d78028d64a658ef497038`.
The final composed verifier SHA256 is
`4079d67a66216df27ed678602f89f6fa1bc3c30f60bf7cb93c28c8731f421efd`.
Both binaries are archived under the evidence root's `binaries/` directory.
The 83-file source snapshot SHA256 is
`3a195fa53e9b2fe18723470abc16cb810b1cfb087e9d6da0baea98fc0ac8820a`;
its files are archived under `source/` and match the working runtime sources.
The final verifier adds cache pin/eviction assertions to the earlier composed
verifier; the shared runtime sources match the installed API binary.

Build commands use the worktree's own Zig caches, `-Dcuda=true`,
`-Doptimize=ReleaseFast`, `-Dinference-compile-memory-mb=32768` and `-j2`.
The targets are `install`, `build-memorilla-composed-verify`,
`build-memorilla-load-bench` and the existing filtered native test executable.
The [checkpoint README](../../../../../zig/pkg/inference/scripts/memorilla/README.md)
documents the composed numerical verifier and isolated load diagnostic.
