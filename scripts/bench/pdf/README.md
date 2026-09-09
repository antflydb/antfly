# Remote-PDF ingestion comparison

Internal engineering benchmark using the OHR-Bench corpus and adapter from
`antflydb/antfly-circus`. The harness serves unchanged PDF bytes over loopback
HTTP; **Antfly** fetches, parses, OCRs, chunks, embeds, and indexes them. `pypdf`
only counts pages before timing. No mock inference or pre-extracted text is used.

OHR-Bench has conflicting license metadata. Follow Circus's stricter
research/non-commercial restriction; do not publish these as marketing results.

## Setup

Requires Python 3.11+, `pypdf>=5,<7`, `cryptography`, a read-only Circus checkout
containing `benchmarks/OHR-Bench-harness`, and two matched native Antfly builds.
Use the same Zig version, optimization, CPU target, Metal/BLAS options, models,
and host. Preserve each executable with its source revision, build command,
and SHA-256. This is a source-build comparison, not a release-package benchmark.

Keep downloaded assets and results outside git. Example (replace these paths):

```sh
uv venv /tmp/pdf-ab/venv
uv pip install --python /tmp/pdf-ab/venv/bin/python 'pypdf>=5,<7' cryptography
curl -fL --retry 3 https://huggingface.co/datasets/opendatalab/OHR-Bench/resolve/main/pdfs.zip -o /tmp/pdf-ab/pdfs.zip
antfly inference pull BAAI/bge-small-en-v1.5:safetensors --models-dir /tmp/pdf-ab/models
antfly inference pull antflydb/Florence-2-base:safetensors --models-dir /tmp/pdf-ab/models
/tmp/pdf-ab/venv/bin/python scripts/bench/pdf/benchmark.py prepare \
  --work-dir /tmp/pdf-ab --circus-dir /path/to/antfly-circus
```

`prepare` verifies the pinned archive SHA-256, extracts only the 11 curated PDFs,
and writes their hashes and page counts to `corpus.json`. Models are hashed for
every run; do not change the model directory between subjects. `--models-dir`
can override the default `WORK_DIR/models`.

## Run

To isolate renderer admission without model loading, run from `zig/`:

```sh
zig build lib-pdf-bench -Doptimize=ReleaseFast -- render-window /path/to/input.pdf
zig build lib-pdf-bench -Doptimize=ReleaseFast -- render-window /path/to/input.pdf 0 268435456
```

This renders the first page with an exact retained-RGBA allowance. The first
optional argument selects model-input dimensions (for example, `768`); omitted
or zero preserves requested DPI. The final argument supplies scratch bytes;
omitted or zero uses the planner estimate.
It reports geometry, scratch/output limits and render quality, and fails on a
page error. Comparing an estimate with a configured ceiling helps diagnose
content-dependent scratch growth; neither run measures end-to-end performance.
Use `render-compare` with the same arguments to additionally compare exact pixels
and render quality against the serial reference renderer. The reference run is
not memory-admitted by the bounded window and is diagnostic only; a pixel or
quality mismatch fails the command.

```sh
/tmp/pdf-ab/venv/bin/python scripts/bench/pdf/benchmark.py run \
  --work-dir /tmp/pdf-ab --circus-dir /path/to/antfly-circus \
  --binary /path/to/frozen/antfly --revision FULL_GIT_SHA \
  --name rc5-auto-batch --suite small --mode auto --batch --trials 3
```

Repeat with the branch executable and a distinct `--name`. Each invocation
starts its own standalone server/database; each trial uses a fresh table.
An existing run directory is rejected. Only this harness's child process is
terminated. The fixed default ports are 29680/29681; use `--port` to change them.

- `--suite scan`: the single required-OCR scan, for quick model qualification.
- `--suite embedded`: the seven-page document and newspaper, 8 pages total.
  A separate embedded-text diagnostic; not a replacement for reporting failures
  or unequal OCR output in the mixed suites.
- `--suite text`: 3 text-bearing/mixed PDFs, 9 pages. Includes the seven-page
  document, newspaper, and textbook page. Auto mode can still invoke OCR.
- `--suite small`: 4 PDFs, 10 pages, including a required-OCR scan and a
  seven-page document. Start here to qualify the pipeline.
- `--suite qualification`: all 11 curated PDFs, 252 pages, including encrypted,
  large, long, and URL-encoded fixtures. Not a full OHR retrieval evaluation.
- `--suite throughput`: a fixed 51-page cohort: the 16-page academic,
  17-page administration and 18-page law PDFs. Selected by corpus role before
  measuring, not by successful run outcomes. Report any failed qualification.
- `--mode auto`: normal embedded-text extraction with OCR fallback.
- `--mode always`: OCR every page to exercise rendering and inference batching.
- `--ocr-model` and `--embed-model`: explicit model identities; defaults pin the
  Florence and BGE safetensors variants. Embedding dimension remains BGE's 384.
- `--batch`: submit all source rows together; omit for one request per PDF.
- `--consumers 2`: create two independently named extraction/chunk/vector
  pipelines on each source row. Completion and parity checks include both;
  this is not two indexes referencing one already-produced artifact. Use a
  separate profile to count actual render work rather than assuming sharing.
- `--reader-batch-size`: explicitly pin reader microbatch capacity to 1, 2, 4,
  8 or 16 on both subjects; omitted uses each subject's default. Record separate
  experiments for each size, including any output divergence.
- `--read-profile`: enable per-stage reader diagnostics and record the override
  in provenance. Use for failure diagnosis, not timing comparisons.
- `--render-workers`: request 1, 2, 4 or 8 render lanes; actual admitted workers
  may be fewer. `--render-prefetch 0|1` controls the next-window overlap.
  `--render-memory-bytes` pins the renderer cap explicitly. All three overrides
  are recorded in provenance; ambient settings are still removed.
- `--trials`: fresh tables within one server process. Model/runtime caches can
  be warm after the first trial. The first trial is **not** a cold-filesystem run.

Both subjects receive identical configuration, including a 16,000 MiB process
budget. Ambient `ANTFLY_*` variables are removed and their names recorded;
`--read-profile` explicitly reinstates only `ANTFLY_INFERENCE_READ_PROFILE=1`.
The only remote-content exception is the local byte origin. GPU acceleration
must be checked in runtime logs; compiling Metal support alone is not proof.

## Measurement and gates

`results.json` records insert-to-completion wall time, write acknowledgement,
startup, and table-setup time separately. Completion requires converged artifacts,
exact page coverage, nonempty chunks, zero OCR failures, selected OCR for the
scan, ready indexes, complete/healthy source coverage for every submitted
document, and published vectors. The byte-origin log must prove every PDF fetch.
The write API defaults to `sync_level=full_index`; `--sync-level write` exercises
durable replay instead. Both wait for full completion; acknowledgement alone is
insufficient. Do not change sync level between compared subjects.

Raw manifests, index telemetry, origin access logs, server logs, exact table
configuration, model/binary hashes, host load, and Circus revision are retained.
A failed run writes `failure.json` and is **not a throughput result**. No retrieval
accuracy or OCR text-accuracy score is implied by the structural gate.

For performance conclusions, use a quiet host, alternate subject order, run
multiple fresh-process lifecycles, and compare the same page/chunk/vector/error
counts before computing speedups. Report first-process and warm trials separately.
Do not conflate model loading, filesystem caching, polling granularity (250 ms),
or background builds with batching improvements. Host load is recorded, not
controlled. This harness does not claim peak-memory measurements.

```sh
python3 -m unittest discover -s scripts/bench/pdf -p 'test_*.py' -v
```

## Paired main-versus-PR comparison

Use frozen source worktrees for both subjects. Finish both matched Metal builds
before starting timing; pin full commit SHAs and preserve binary hashes. The PR
should contain the exact baseline main commit so unrelated main changes do not
confound attribution.

```sh
/path/to/venv/bin/python scripts/bench/pdf/compare.py \
  --work-dir /path/to/pdf-assets --circus-dir /path/to/antfly-circus \
  --main-binary /path/to/main/antfly --main-revision FULL_MAIN_SHA \
  --pr-binary /path/to/pr/antfly --pr-revision FULL_PR_SHA \
  --name main-pr-text-always --suite text --mode always --pairs 2 --trials 3
```

Pairs alternate `main, PR` then `PR, main`, always using fresh server processes;
trials within each process use fresh tables. Run `--mode auto` separately for
normal extraction and `--mode always` for forced rendering/OCR. The driver uses
the existing harness with `--batch --verify-unit-text`. Retained unit text is
hashed after the timed interval; volatile metadata is excluded from those hashes.
Both subjects must have identical per-document page/chunk/OCR counts, total
published vectors, retained-text hashes, page/DPI metadata and render-quality
warnings. Corpus, model, table and server
configuration must match, and runtime logs must confirm Metal for invoked models.
Every requested pair must finish successfully; failed/unequal-output pairs are
retained, not silently removed. No speedup is calculated for such an experiment.

Each experiment retains commands, per-process logs, full results and
`summary.json`. The summary separates first-process-trial timings from per-process
medians of warm trials, records paired ratios, and rejects binary/model drift
between pairs. These are not cold-filesystem measurements or OCR accuracy scores.
Host load remains uncontrolled and must be reported; use more alternating pairs
on a quiet host before making performance claims.

## Render-control ablations

`--sync-level full_index` (the default) measures precommit enrichment.
`--sync-level write` measures durable replay; the timer still waits for all
artifacts, indexes and vector coverage, not just the write acknowledgement.
Never compare ratios across these paths. Render-window sharing currently lives
in replay, so use `benchmark.py run --consumers 2 --sync-level write --read-profile`
to diagnose reuse. The same two-consumer full-index probe tests the separate
precommit path, which currently renders independently for each consumer.

`render_matrix.py` uses the same executable, PDFs, models, requested DPI,
reader capacity four and 256 MiB renderer cap for every configuration. It varies
only requested render workers (1/2/4) and prefetch (on/off), reverses the full
configuration order in the second round, and uses fresh processes/databases.
The baseline is one worker with prefetch on; `main`/`pr` keys inside its reused
summary format mean baseline/configuration, **not different branches**.

```sh
/path/to/venv/bin/python scripts/bench/pdf/render_matrix.py \
  --work-dir /path/to/pdf-assets --circus-dir /path/to/antfly-circus \
  --binary /path/to/frozen/pr/antfly --revision FULL_PR_SHA \
  --name render-text --suite text --rounds 2 --trials 3
```

Use `--profile --rounds 1 --trials 1` with another name for diagnostic admission
logs. Profiled experiments deliberately produce no timing ratio. Raw per-window
grants, requested/effective concurrency and OCR batches are retained; render
window elapsed time can include prefetch lifetime and is not CPU service time.
For a larger-workload check, use `--suite throughput`; a reduced `--workers 1`
matrix isolates prefetch off/on. No failed configuration is dropped from the
overall qualification. Exact text, DPI, warnings, counts and vector gates still
apply to every timing comparison; memory-limit changes are not allowed ablations.

The initial pinned production build (`61a08302f`) estimates each lane using the 128 MiB decode ceiling
plus metadata and raster memory. Consequently two lanes cannot fit its 256 MiB
renderer cap. A requested worker count above one is not evidence of concurrency;
check the profile. That build's learned scratch hints only increase the estimate.
The corrected policy separates hard decode ceilings from concurrency estimates,
retains the aggregate allocator cap, and retries scratch-pressure failures with
bounded serial work. Its correctness, actual concurrency, replay reuse and
quality-matched timings are recorded in
[the render qualification report](RESULTS-2026-09-08-RENDER.md).
