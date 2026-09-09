# Render scheduling qualification (internal)

Follow-up to [the quality-matched main/PR comparison](RESULTS-2026-09-08.md).
Same internal research/non-commercial corpus restrictions apply. These are local
engineering observations, not OCR accuracy or marketing claims.

## Final pinned-main comparison

`main-pr-render-29dd-20260909` compares main
`98d911a88cff9fd29fd9cc3b78c22ebb78b20df5` against production source
`29dd963a403479df56ceae5b3571801f1412bff9`. Main is an ancestor of the PR.
Main executable SHA-256:
`7e68a73bb6dcfb4baa6e9e9acb3b4ba9fdea17f1a559d4231f4054a9685d107a`;
PR executable SHA-256:
`373ba11c401c1a431e0f8b97489505af12a676ac3c982cf9debe36588cf77650`.
Both are Metal ReleaseFast builds with ONNX disabled, using identical model
files and the nine-page text cohort, forced OCR, reader capacity four, four
requested render workers, prefetch on, a 256 MiB renderer cap and `full_index`.

All 12 trials pass the structural gates and exact retained-text, geometry,
warning, artifact-count and vector-count comparisons. Two fresh-process pairs
alternate main/PR then PR/main; each process ingests three fresh tables.

| Measurement | Main | PR | Lower elapsed | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| First-process trial | 8.757 s | 6.789 s | 22.5% | 1.290× |
| Warm-process trials | 8.826 s | 6.574 s | 25.5% | 1.342× |

Warm per-process medians are main 9.085/8.567 s and PR 6.619/6.529 s;
paired throughput ratios are 1.373× and 1.312×. This is the final quality-matched
end-to-end result for this cohort, not an isolated rendering microbenchmark.
Host activity and filesystem caches remain uncontrolled (recorded one-minute
load 6.82–8.20); first-process trials are not cold-filesystem trials.
Do not infer general OCR/retrieval accuracy or
speedups for the disqualified larger cohort from these measurements.

Reproduce with the frozen binaries and full revisions above using `compare.py`
with `--suite text --mode always --pairs 2 --trials 3 --reader-batch-size 4
--render-workers 4 --render-prefetch 1 --render-memory-bytes 268435456`.
The driver also supplies `--batch --verify-unit-text` to both subjects.

## Final render-control results

Production source `29dd963a403479df56ceae5b3571801f1412bff9` passes all 36
trials in `render-matrix-29dd-text-20260909`, including exact retained text,
render geometry/warnings, artifact counts and searchable-vector counts. The
same nine pages, reader batch four, 256 MiB renderer cap, Metal models and
`full_index` semantics are used throughout. Configuration order reverses in
round two; each warm value is the median of two per-process medians.

| Requested workers | Prefetch | First-process trial | Warm elapsed |
| --- | --- | ---: | ---: |
| 1 | on | 7.434 s | 7.181 s |
| 1 | off | 7.856 s | 7.666 s |
| 2 | on | 6.789 s | 6.643 s |
| 2 | off | 7.107 s | 7.004 s |
| 4 | on | 6.711 s | 6.553 s |
| 4 | off | 7.015 s | 6.896 s |

Four workers with prefetch measure 8.7% lower warm elapsed than one with
prefetch (1.096× throughput). At four workers, prefetch measures 5.0% lower
warm elapsed. These are same-binary ablations, not main/PR comparisons.
The host was not quiet: the one-worker/prefetch baseline's per-process warm
medians were 6.810 s and 7.552 s, while four workers measured 6.564 s and
6.542 s. Treat the aggregate as an observed improvement, not an isolated
causal estimate or a linear worker-scaling claim.

Separate final-binary diagnostics confirm actual four-worker admission and
two-consumer replay reuse (nine physical renders instead of eighteen), with
identical text/geometry and 25 vectors per consumer and no worker restart.
Precommit `full_index` consumers still traverse independently. The larger
51-page cohort now fails only its intentionally rejected oversized image
stream; it remains disqualified from performance comparisons.

## Initial experiment controls

Production source `61a08302ff07e94f1292a6e5d9cd998f1ea8a25e`, executable SHA-256
`2c9196567dc518165e113d63c68e5bbac2fcb73de765fa4fff6c0186cd8e0760`; same Metal
ReleaseFast build and models as the preceding report. No production changes
were made for this experiment. Requested DPI remains 150, with the same explicit
spatial caps (the newspaper remains 138 effective DPI). Reader microbatch
capacity is four. Process budget is 16,000 MiB and renderer cap 256 MiB.

`render_matrix.py` varies only render workers 1/2/4 and next-window prefetch
0/1, reversing configuration order in round two. Each process ingests three
fresh tables; nine pages per table. The baseline is one worker, prefetch on.
All render controls are recorded explicitly, not inherited from the environment.
Profiling is disabled for timing; separate diagnostics record admission and
execution. Exact retained text, geometry, warnings, counts and vectors gate each
comparison. Host activity and filesystem caches remain uncontrolled.

## Admission finding

`render-controls-profile-w4-p1-20260908` completed successfully. Four requested
workers yielded **one actual worker in every render window**, not four. The
profile's `requested_workers` field is already reduced by the wave planner;
operator request four is recorded separately in benchmark provenance.

This is explained by the implementation, not just this input's raster sizes:
`estimatePreparedPageRenderWaveScratchBytes` charges the entire default 128 MiB
decode-working-set allowance per lane, plus fork metadata and raster reserve.
Two lanes therefore exceed the entire 256 MiB cap before retained output credit.
`PdfRenderSession.estimatePreparedWaveScratchBytes` takes the maximum of that
estimate and learned scratch usage, so a low observed peak cannot enable more
lanes. `planPdfRenderWave` consequently reduces concurrency to one.

The parallel renderer exists, but the tested default admission policy prevents
its CPU parallelism from activating. Increasing only the worker setting does
not test a parallel speedup. Nor is silently lowering DPI or raising the memory
budget an equivalent-work result.

The long-term correction is to separate the per-lane hard decode ceiling from
the concurrency reservation estimate. A measured/content-aware estimate could
admit additional lanes under one unchanged hard aggregate allocator ceiling,
with bounded serial fallback and learned backoff when the estimate is too low.
Validate adversarial pages, failed allocations, cancellation and unchanged
pixels before enabling it. This is an identified production follow-up, not an
optimization measured in the initial matrix. The follow-up below implements
and qualifies that correction separately.

## Initial measurements (before the admission fix)

`render-matrix-text-20260908` passed all 36 trials and exact output gates.
Each warm value below is the median of two per-process medians (two warm trials
per process), with reversed configuration order across the two rounds.

| Requested workers | Prefetch | First-process trial | Warm elapsed |
| --- | --- | ---: | ---: |
| 1 | on | 6.951 s | 6.731 s |
| 1 | off | 7.505 s | 7.265 s |
| 2 | on | 7.042 s | 7.064 s |
| 2 | off | 7.815 s | 7.468 s |
| 4 | on | 7.469 s | 7.801 s |
| 4 | off | 8.772 s | 8.295 s |

At one worker, prefetch reduced warm elapsed by 7.4%. Requested worker increases
did not help; the fixed admission cap still permits only one lane. Do not
attribute all between-configuration differences to software: host load varied
(first-round one-minute load 7.68–10.89), and this is reversed ordering rather
than a randomized, quiet-host trial. These are same-PR ablations, not new
main-versus-PR speedup measurements.

### Larger corpus: failed qualification retained

`render-matrix-throughput-20260908` attempted two rounds of prefetch on/off with
the fixed 51-page cohort. All four processes stopped after their first failed
trial. No timing ratio is qualified. The academic PDF's page-one image is
5194 by 6337, RGB8, Flate-compressed: 98,743,134 decoded bytes, beyond the
unchanged 67,108,864-byte per-stream limit. It fails in both configurations.

The administration document separately fails on eight pages with prefetch on
and passes all pages with it off. `render-throughput-profile-w1-p1-20260908`
confirms `RenderBatchAdmissionExceeded` with zero launched workers for the
affected windows. Its partial scratch grant was about 106.7 MB, below the
incorrect 128 MiB minimum. This is a correctness finding, not merely an
unfavorable performance sample. Which eight pages fail can depend on overlap.

### Two independent consumers

`render-consumers2-profile-20260908` passed structural and per-consumer output
checks using independently named extraction, chunk and vector pipelines.
However the diagnostic records 18 rendered pages and eight render windows:
the two consumers each traverse the nine-page corpus. It therefore does not
demonstrate avoided rendering, and the profiled elapsed time is not timing
evidence. Sharing-decision diagnostics are being added to identify the declined
path rather than infer a reuse benefit from the presence of the scheduler.

### Follow-up implementation

Source checkpoint `963e86c82` separates the geometry/fork estimate from decode
ceilings and lets parallel scratch failures retry failed identities serially
under the same grant. Speculative scratch failures carry bounded page-plan
metadata until the prior window drains, then reacquire scratch and retry only
failed pages once, retaining successful buffers and output credit. Native
integration verifies preserved attachment identity and exact regenerated PNG
bytes. Its Metal executable SHA-256 is
`aa15f18814fada7ac448057db69be40857df233c06bd4433473ae82735f824c5`.

`render-controls-963-w4-p1-20260908` now records four launched/parallel workers
for academic pages 1–4 and three for pages 5–7, within the unchanged 256 MiB
cap. All nine pages pass structural/output checks. This profiled run is not a
timing comparison. `render-throughput-963-w1-p1-20260909` restores all 17
administration pages with prefetch on; all 18 law pages also pass. The academic
document still has the one intentional decoded-stream-limit failure, so no
51-page speedup is qualified.

`render-consumers2-963-20260909` confirms the full-index ingestion path never
enters shared-window scheduling: generated artifacts are precomputed before
commit, whereas the scheduler belongs to durable replay. The harness now pins
`sync_level`; switching it is a separate experiment, not a valid timing ablation.
The `write` probe `render-consumers2-963-replay-20260909` enters sharing with
matching source/transform and batch width four, but times out after transport
calls repeatedly return `ResourceTemporarilyUnavailable`. Full log inspection
shows a Metal assertion, `A command encoder is already encoding to this command
buffer`, followed by worker restarts (generations 1–7). This is a shared mutable
provider race, not proof of simple admission pressure. The run is retained as a
failed diagnostic, not reuse evidence.

Follow-up checkpoint `ee01e41e2` preserves the original request/lifecycle guards
across speculative retries and distinguishes pre-dispatch RPC/request-slot
denial (`QueueFull`) from post-execution response-capacity failure. The former
can use the scheduler's existing bounded drain-and-retry path. Response failures
retain their generic retryable status: they cannot prove execution did not
occur. These error-contract fixes alone do not address the Metal assertion.

Checkpoint `aad55a472` extends the model-store provider mutex into an execution
lease covering compute lifetime and frame teardown. Competing requests receive
`QueueFull`; they cannot concurrently encode the same cached command buffer.
Fused batches are preserved, different model stores have independent lanes,
and the cached provider remains reusable. A Metal-backed regression test covers
overlapping-frame rejection, untouched owner state, unfinished-frame cleanup,
subsequent provider reuse, and independent stores.

Verification to date: 460 PDF tests in Debug and ReleaseFast plus native PDF
coordinator integration; 168 selected enrichment regressions; 120 selected Metal
compute tests (none skipped); 107 standalone tests and the production-linked
ABI probe; 20 benchmark-script tests, Black and Ruff. The standalone compiler's
9.93 GB observed peak exceeded its old 9 GiB scheduling claim; its macOS test
compile claim is now 11 GiB, without changing service memory limits. A closed
RPC offer wakeup also remains a transport failure rather than `QueueFull`.

The final production subject is `29dd963a403479df56ceae5b3571801f1412bff9`.
Its build label remains `pdf-render-aad55a472` to reuse unchanged storage codegen;
the full source revision and executable digest, not that label, identify the
benchmark subject. Its Metal ReleaseFast executable SHA-256 is
`373ba11c401c1a431e0f8b97489505af12a676ac3c982cf9debe36588cf77650`.

### Final real-corpus regression checks

`render-consumers2-29dd-replay-20260909` passes both independent consumers with
identical retained text and geometry and 25 searchable vectors each. There are
exactly nine rendered pages and three windows (7+1+1), not eighteen pages.
The academic window launches four workers. The inference worker stays at
generation one: no Metal assertion, restart, or repeated document traversal.
Its 11.085 s profiled elapsed time is diagnostic only, not a qualified speedup.

`render-throughput-29dd-w1-p1-20260909` restores all 17 administration pages
and all 18 law pages with prefetch enabled. The academic PDF retains precisely
one `DecodedStreamTooLarge` failure among its 16 pages. The 51-page experiment
remains disqualified; no failed document has been removed to report a ratio.

The final unprofiled render ablations and pinned-main comparison are reported
above; all requested timing trials finished and passed their output gates.

Results are retained under `.benchmark-results/pdf-verification/` with commands,
run order, provenance, model hashes, logs and failed outputs. Do not substitute
successful subsets for the failed 51-page experiment.
