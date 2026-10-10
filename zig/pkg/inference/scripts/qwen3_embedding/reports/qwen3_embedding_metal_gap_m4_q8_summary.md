# Qwen3 Metal gap qualification summary

Historical local results for Apple M4, 16 GiB RAM, macOS 26.5, and
Qwen3-Embedding-0.6B Q8_0. These measurements cover the recorded executables,
not the current merged branch. See the later
[default qualification receipt](../../metal_serving_defaults_m4_summary.md)
for the current M4 serving policy and fresh qualification.

## Protocol and provenance

- Identical GGUF bytes, last-token EOS pooling, and 1024-dimensional output.
- Three rounds per shape, three warmups, and 20 measurements per endpoint per round.
- Alternating AB/BA requests; cache-neutral fixtures; rendered queries include
  the full instruction prefix and EOS. Reference query slots are erased outside timing.
- Every measured vector is checked against the reference; cosine gate: 0.995.
- Short gates: 95% throughput-ratio lower bound >=1.0 and p95 ratio <=1.05.
- Capacity gate: lower bound >=0.90. Fixed-baseline regression gate: >=0.95.
- Capacity/short lanes: 4 GiB process, default 384 MiB scratch and 2 GiB combined caps.
- Long-singleton regression lane: explicit 1536 MiB scratch and 3072 MiB combined caps.
- llama.cpp physical microbatch: 2048 tokens, or the full singleton length when longer.
- Measured GPU requests are sequential, with no competing compilation or GPU work.

Ratios are candidate/reference. Throughput ratios are better above 1; latency
ratios are better below 1. Tables retain the least favorable bounds across rounds.

| Artifact | SHA-256 |
|---|---|
| GGUF | `06507c7b42688469c4e7298b0a1e16deff06caf291cf0a5b278c308249c3e439` |
| llama.cpp b8990-660b1b4bd executable | `8eee1b1fa1c65d919c94116dd286134a4a9498059c21c1ee448c45fb87f2c590` |
| Document fixture | `0cb1dbeea97f75cdd01d0c58d4ba0aefac16b61545fa8fbe408aaaa3e3c9933b` |
| Rendered query fixture | `161e2c0be8c53885bd88a935f196e1640ab4d9612536130b0cc23e4366499fe3` |
| Original qualified executable | `35b506fd745cc2fb7434f53fd85a3c2c48d791129abf9a8c76411f09d7334839` |
| Review-fix executable | `cf8e16ceb7b7644d2a31c3125931ea5ebcd9c2f8ef2077a9f3f519067d28a7b4` |
| Original tracked source patch | `7f159e845c9f5cdf777b1bda70f4f21175020d9cd3b8d80c49e7bd9b58233bfc` |
| Review-fix incremental patch | `a8846387eee58b3dd4106ee69a27f5648ce2c60df824ca7d51779057bc1a55e5` |

Both measured candidates were dirty worktrees over `60dbc97e27648e9744b91a1449482576e8684d97`.
Complete source snapshots and per-file hashes are retained in the archived raw receipts.

## Review-fix executable: 2026-10-02

Includes scoped embedder workspace admission and independently owned warm-request
metadata, releasing cached model pins before execution/recovery.

| Input | Tokens including EOS | Mean latency across rounds | Worst 95% throughput lower bound | Worst p95 ratio |
|---|---:|---:|---:|---:|
| Document | 20 | 23.813–23.973 ms | 1.014813 | 1.018502 |
| Document | 32 | 23.811–24.308 ms | 1.023829 | 0.990996 |
| Document | 64 | 35.433–35.533 ms | 1.149870 | 0.879855 |
| Rendered query | 32 | 23.465–23.609 ms | 1.017575 | 0.999546 |
| Rendered query | 64 | 34.666–35.602 ms | 1.142984 | 0.886624 |

- 15/15 short cells passed; minimum cosine 0.9999977587.
- 25/25 Metal HTTP checks passed: ordering, singleton/EOS parity,
  reduced dimensions, query-prefix limits, both error policies, partial indexes,
  disconnect/overload recovery, and eviction/reload.
- 7/7 focused admission/ownership regressions passed, zero skips; 29/29 supporting tests passed.
- Capacity throughput, passage regression, and the full soak were not repeated on this executable.

## Original executable: 2026-10-01

15/15 short cells passed; original short measurements are tabulated in
[BASELINE.md](../BASELINE.md#short-query-and-capacity-results--2026-10-01).

| Lane / shape | Mean request latency across rounds | Worst 95% throughput lower bound | Worst p95 ratio |
|---|---:|---:|---:|
| Capacity vs llama.cpp: 32×256 | 6.364–7.115 s | 1.216163 | 0.805054 |
| Fixed-baseline regression: 8×20 | 118.67–120.59 ms | 1.283629 | 0.776698 |
| Fixed-baseline regression: 32×20 | 430.40–433.43 ms | 1.056553 | 0.943250 |
| Fixed-baseline regression: 1×256 | 185.04–188.58 ms | 1.215237 | 0.834827 |
| Fixed-baseline regression: 8×256 | 1447.45–1513.87 ms | 1.011004 | 0.990907 |
| Fixed-baseline regression: 1×511 | 357.83–381.58 ms | 1.100044 | 0.906494 |
| Fixed-baseline regression: 1×2551 | 2372.37–2539.12 ms | 0.990349 | 1.009964 |

3/3 capacity and 18/18 regression rounds passed. Minimum capacity cosine: 0.9999899002.
Original HTTP checks: 25/25. Python harness: 132 passed. HF tokenizer: 82 passed,
one optional SPLADE fixture skipped. Focused checks: 28/28; physical short-kernel
checks: 4/4; existing head-normalization fallback replay: 1/1, with no skips.

### Soak: retain both outcomes

| Run | Duration | Requests / vectors | Request failures | Final-half RSS range | Plateau result |
|---|---:|---:|---:|---:|---|
| Initial unconditioned | 1835.94 s | 243 / 7776 | 0 | 110.031 MiB | Fail |
| Explicitly conditioned repeat | 1829.51 s | 310 / 9920 | 0 | 2.203 MiB | Pass |

Both runs used eight client workers, retained server worker identity, and had no
host swap growth. The first run failed the unchanged plateau gate because of
downward RSS transitions; their cause remains unproven.
The repeat used fixed 600-second conditioning (113 requests, zero failures),
then an independent 1800-second measurement phase. The plateau gate is final-half
max-minus-min RSS <= max(32 MiB, 5% mean). Only the explicitly conditioned
steady-state run is qualified; the default driver does not add this conditioning.

## Later cleanup and qualification limits

A subsequent cleanup removed unused workspace row state/parameters, scoped the
compiler reservation to macOS Metal, and fixed double tensor cleanup in the quota test.
It passed 5/5 focused tests (including physical Metal quota checks), zero skips,
29/29 supporting tests, and the ReleaseFast Metal server build (14/14 steps).
These checks also precede the later main merge; they do not establish merged-head
HTTP, performance, or soak qualification.

Canonical CI, other Apple hardware, precision tiers, 4B/8B models, and full 32K
context remain unqualified. Qwen measurements do not establish Gemma performance
or retrieval-quality benchmark scores.

## Raw evidence outside Git

The complete receipts are preserved locally, byte-for-byte, under the ignored
repository-root directory `.benchmark-results/qwen3-metal-gap-20261001/raw-receipts/`.
They retain all measured rounds, source snapshots, request/provenance records,
conditioning driver source, and successful/failing memory trajectories.

| Archived receipt | SHA-256 |
|---|---|
| `qwen3_embedding_metal_gap_m4_q8_20261001.json` | `d55774c637b08f3b4178f226ab4ca44b6daaa4bf70b8f7e349bead5a029f75ad` |
| `qwen3_embedding_pr_review_fixes_m4_q8_20261002.json` | `b0fef7d1797fb12a3cfbe4acae9d2318999dff0b6676e904a5c72df5a58f0ddc` |
