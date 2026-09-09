# Precommit sharing and oversized-image qualification (internal)

Follow-up to [render-control qualification](RESULTS-2026-09-08-RENDER.md).
The same Circus/OHR-Bench research/non-commercial restrictions apply. These
are local engineering checks, not OCR/retrieval accuracy or marketing claims.

## Frozen implementation

Initial production source: `c51c9f2f78a4bf61b1bf269ab28343f069e9fd41`;
executable SHA-256:
`7a9e21588427027f95ce5b9c4b1f763257f736070cda7961cb2cb0f8a3016c3a`.
Final tail-sharing source: `9b1e26b7ed74019cc4e4ba8a7c7a05fbffbf7327`.
Final executable SHA-256:
`938570441fe1a9034394f70ace8c784d53f532ecd3f5364399d5e85371e11f57`.
The Metal ReleaseFast build uses ONNX disabled and the same pinned Florence/BGE
models as the earlier comparisons. The explicit build label remains
`pdf-render-aad55a472`; source revision and executable digest identify the build.

Precommit OCR/reader/generator consumers share bounded rendered windows through
invocation-local typed-result staging. Replay retains its existing durable
staging sink. No precommit public artifacts are published before their normal
atomic batch. Staging has a 64 MiB hard limit and node admission; optional
sharing falls back under pressure. Mutable scheduler/recovery state is not
installed on the live replay runtime.

Image streaming retains native RGBA plus predictor rows/inflate history instead
of complete decoded color and soft-mask planes. The path covers 8-bit device
color spaces with raw/single-Flate samples and same-size 8-bit soft masks,
including predictors, Decode, color keys and Matte. Other layouts/codecs retain
their existing guarded decoders. Native RGBA must still fit the image working-set
limit; no DPI or memory ceiling was relaxed.

## Regression checks

- 462 PDF tests in Debug and ReleaseFast, plus native OCR coordinator integration.
- 168 selected enrichment regressions. The shared-window integration checks
  independent reader/generator results, borrowed attachment identity, private
  precommit staging, pressure handling, source-change invalidation and teardown.
- `lib-pdf-bench -- render-compare` on academic `2305.14160v4.pdf` page one:
  150 DPI, 1241×1754 page raster, exact pixel equality with the serial reference
  under the unchanged 256 MiB scratch cap. Observed worker scratch peak was
  145,195,008 bytes. Both paths report the same twelve missing-glyph fallback
  runs; streaming introduces no pixel or render-quality-warning difference.

The academic image's 98,743,134 decoded RGB bytes and same-size gray soft mask
previously failed the 64 MiB materialized-stream limit. Streaming processes
those rows without retaining either complete sample plane. General materialized
streams still enforce that limit. Tests reject truncated/surplus samples,
bad zlib checksums, cancellation and insufficient image-tree memory.

## End-to-end qualification

`stream-precommit-c51-throughput-20260909` passes the complete 51-page cohort:
academic 16/16, administration 17/17 and law 18/18, with zero OCR failures.
All source documents have converged artifacts and searchable vector coverage.
It uses forced OCR, reader capacity four, four requested render workers,
prefetch on, the unchanged 256 MiB renderer cap and `full_index`. Its 18.956 s
profiled elapsed time is diagnostic, not comparative timing evidence. No failed
document was removed; this is the previously disqualified cohort.

`stream-precommit-c51-consumers2-20260909` passes both consumers with identical
retained text/geometry and 25 searchable vectors each, but records twelve page
renders rather than nine. The scheduler shares the first four academic pages,
then unnecessarily declines the terminal three-page window because it is not a
multiple of the model's batch width. That consumer rerenders the final three.
This retained diagnostic is not proof of complete render reuse.

The final source corrects terminal-tail enrollment for both precommit and replay.
A consumer that declined an earlier nonterminal window stays disabled even at
the final page. Native integration verifies a full batch plus its terminal tail
and the declined-prefix case. Precommit reuse is also bound to the full source
SHA-256, separately from short model/telemetry fingerprints, without an extra
source hashing pass.

The final binary passes these additional checks with the same controls:

- `stream-tail-9b1-consumers2-20260909`: both precommit consumers pass, with
  identical retained text and geometry and 25 searchable vectors each. Exactly
  **nine physical page renders** serve both consumers, including the terminal
  three-page academic window.
- `stream-tail-9b1-replay-consumers2-20260909`: durable replay also passes both
  consumers with nine renders and 25 searchable vectors each. This is a separate
  regression check, not a timing comparison against precommit.
- `stream-tail-9b1-throughput-20260909`: all **51 pages pass**, with zero OCR
  failures and 123 searchable vectors across all three sources. No failed page
  is excluded. The 20.219 s profiled elapsed time is diagnostic only.

Render counts come from `phase=pdf_render ` log records, not the number of
consumer page results or window callbacks. Both consumer probes preserve the
existing newspaper dimension clamp (requested 150 DPI, effective 138 DPI) and
existing missing-glyph warnings. Neither is changed to obtain the improvement.

## Paired precommit timing

The before subject is the **previous PR implementation**, source
`29dd963a403479df56ceae5b3571801f1412bff9`, executable SHA-256
`373ba11c401c1a431e0f8b97489505af12a676ac3c982cf9debe36588cf77650`.
It is not `origin/main` or rc.5; the comparison harness's `main` label means
this pinned before subject. The after subject is the final binary above.

The run `precommit-before-after-9b1-20260909` uses the nine-page text cohort,
two consumers, forced OCR, `full_index`, reader capacity four, four requested
render workers, prefetch on and the same 256 MiB renderer cap. Profiling is off.
Two pairs reverse process order, with three trials per process. Every trial
must pass artifact/vector coverage and exact retained text/geometry equivalence
before reporting a ratio. Other host workloads remain active; this is an
internal, load-qualified sample rather than an isolated performance claim.

All twelve trials pass and both pairs have no comparison errors. The harness
reports medians across per-process phase medians:

| Phase | Before | After | Elapsed reduction | Before/after |
| --- | ---: | ---: | ---: | ---: |
| First process trial | 13.917595 s | 12.585203 s | 9.57% | 1.106× |
| Warm process trials | 13.937411 s | 12.573341 s | 9.79% | 1.108× |

Warm paired ratios are 1.093× and 1.123×; observed one-minute host load ranges
from 6.33 to 11.10. First-process trials are not cold-filesystem trials. This
sample supports reduced end-to-end work, not a general throughput guarantee.
Rendering reuse does not eliminate task-specific inference and embedding for
each consumer, so halving render work does not imply halving ingestion time.
There is no 51-page before/after speed ratio: the before implementation failed
that cohort's oversized image, and failed work is not a valid timing baseline.

Reproduce the timing comparison from the worktree, after building the pinned
executables and preparing the corpus as described in the README:

```sh
.benchmark-results/pdf-verification/venv/bin/python scripts/bench/pdf/compare.py \
  --work-dir .benchmark-results/pdf-verification \
  --circus-dir /Users/ajroetker/go/src/github.com/antflydb/antfly-circus \
  --main-binary .benchmark-results/pdf-verification/pr-render-29dd963a4/bin/antfly \
  --main-revision 29dd963a403479df56ceae5b3571801f1412bff9 \
  --pr-binary .benchmark-results/pdf-verification/pr-stream-tail/bin/antfly \
  --pr-revision 9b1e26b7ed74019cc4e4ba8a7c7a05fbffbf7327 \
  --name precommit-before-after-9b1-20260909 \
  --suite text --mode always --pairs 2 --trials 3 \
  --consumers 2 --sync-level full_index --reader-batch-size 4 \
  --render-workers 4 --render-prefetch 1 --render-memory-bytes 268435456 \
  --timeout 180 --port 29700
```

Use a fresh run name for a repetition. Raw results, logs, frozen configuration,
model identity and per-page output hashes remain under the ignored benchmark
work directory; no research PDFs or model weights are added to the repository.
