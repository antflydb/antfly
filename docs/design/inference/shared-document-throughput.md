# Feeding a shared write pool

This work builds on [write-pool batching (#1517)](https://github.com/antflydb/antfly/pull/1517)
and its [GB10 experiment](write-pool-40g-gb10.md). The broker owns physical
inference batching. Database document preparation supplies independent work;
it does not introduce another model scheduler or change durable replay order.

## Bounded cross-document preparation

A generated replay pass can retain the current document and one future root PDF.
It refills the lookahead before joining the current task, including across a run
of one-page PDFs, while ordered publication remains on the foreground worker. The future task downloads at most 16 MiB, verifies the configured
checksum, borrows the shared prepared-source cache, inspects at most eight
physical pages, and renders the first pending OCR page. Born-digital pages do
not trigger speculative OCR. Existing manifests, privacy-managed artifacts, causal graph/prior-context inputs and derived/template inputs
retain their normal repair and idempotent paths. Equal source URLs are excluded
from overlap so that mutable PDF sessions are not used concurrently.

With a model revision, the task can also execute that first page through the
normal asset producer. Reuse requires a successful result reporting the revision
actually dispatched, matching the discovery expectation. The native adapter binds
remote readers and generators to the exact server-enforced capability lease used
for dispatch; a changed revision is rejected before inference. Linked readers and
attachment generators require an immutable registry model reference. A catalog's revision
or embedding-pinning support flag alone never establishes execution identity.
Unknown or mixed execution revisions are not reusable. The owner rechecks source
allocation identity, OCR/render/quality policy, page identity, native text and current model capabilities before copying the
selected unit. Providers unable to establish execution identity retain render-ahead
only. Failed speculative calls fall back to ordinary owner execution and do not record document retry debt,
completion credit, artifacts or replay advancement.

All workers join before source-cache destruction or ownership transfer. Cancellation
combines the lifecycle, visibility guard and producer cancellation. PDF/render and
provider allocations use existing resource admission. Speculative discovery and
execution require cooperative deadline/cancellation hooks and the same synchronized
provider circuit checks as foreground work. Legacy callbacks run only through
the ordinary replay path. Actual execution bytes (including metadata and raster
bytes) and pixels are checked against the narrowed policy before dispatch.

Ready optional state is reclaimable before foreground memory admission, including
selected text, rendered pages and source/PDF pins. Reclaim callbacks never wait on
cache locks or worker joins. Transferred windows and units being copied are
protected until their owner releases them. The future task never
mutates the scanner's active request identity or progress state.

Set `ANTFLY_ENRICHMENT_DOCUMENT_PREFETCH=false` on data nodes to disable this
optimization. This leaves the ordinary single-document pipeline intact. The
optimization does not fetch through a staging bucket or copy remote sources.

## Metrics and interpretation

`antfly_document_pipeline_phase_duration_ns` exposes bounded histograms for
`source_fetch`, `pdf_prepare`, `pdf_render`, `media_encode`, `reader_http`,
`vision_prepare`, `vision`, `prefill`, `decode`, `mixed_step`, `publication`, and
`prefetch_wait`. These measure inclusive monotonic wall time, including failed
attempts. They overlap; summing them does not produce exclusive CPU or GPU time.
`reader_http` includes response decoding. Direct and scheduled generation work is timed at dispatch after coordinator
waits; merely having a coordinator no longer suppresses singleton timings.
Prepared multimodal and eligible pending-token paths are included. A declined
direct fast path and its fallback share a single decode-step sample, without
including a subsequent coordinator wait. Counts describe execution scopes, not
generated-token counts. GPU utilization and
memory must still be sampled separately. Each histogram uses a short per-phase
critical section for both observation and capture, so count, sum and cumulative
buckets describe the same observations even during concurrent scrapes. Formatting
and cross-owner aggregation run outside those critical sections.

`antfly_document_prefetch_events_total` distinguishes started, ready, failed,
consumed and reclaimed tasks/windows. It has no document, URL or model labels.
Native storage and enrichment compute expose versioned snapshot getters through
their owning compiled-unit ABIs, so the data metrics endpoint does not report a
host-side duplicate of their counters. Snapshot buffers follow existing owner
buffer destruction contracts.

## Reproduction and limits

Run the native PDF orchestration smoke (real PDF parser/rendering, deterministic
fake reader with a pinned revision, four independent one-page PDFs, exact-once
first-page reuse, DPI provenance and no speculative publication):

```sh
cd zig
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build pdf-ocr-integration-test -j1
```

Run the CUDA inference HTTP comparison against an isolated server with the pinned
LightOn model installed under its `models_dir`:

```sh
python3 zig/pkg/inference/scripts/qualify_document_pipeline.py \
  --url http://127.0.0.1:18879 --model LightOnOCR-3-4B \
  --output .benchmark-results/document-throughput --requests 8 --rounds 3 \
  --concurrency 2
```

The client uses the same AFATT001 binary envelope as native remote readers. It
warms every fixture, alternates serial/concurrent order, requires the fixture's
critical values, and checks exact normalized output parity across modes. Reports
include request timings, image hashes and before/after server metrics. Enable
`ANTFLY_BENCH_LIGHTONOCR3=1` on an isolated server to collect actual cohort and
batched-step diagnostics, rather than interpreting concurrent HTTP requests as
proof of physical batching.

These four synthetic fixtures are a smoke corpus, not a representative document
accuracy benchmark. A GB10 inference-only result does not establish A100
throughput, six-model residency, source-fetch speed or durable publication rate.
Validate the full extraction path and mixed-workload memory envelope separately.

The corresponding Colony configuration permits four concurrent requests and four
public items, with two generation rows, 512 query tokens per step and a 1 ms decode
wait. Aggregate decoded pixels are raised from 4M to 16M so a four-item request
retains the existing per-page allowance. Model pins, pool capacity, memory budgets,
scale-to-zero and routes stay under their existing owners. Promoting this source
requires a new immutable release; an image-only runtime updater must reject these
configuration changes rather than silently expanding its scope.

## DGX Spark inference comparison

The [recorded GB10 result](shared-document-throughput-gb10.json) has three
alternating eight-request rounds per mode after four warm-ups. Median throughput
was 0.2747 pages/s serial and 0.4524 pages/s at concurrency two (1.647×), with exact
normalized output parity and critical fixture values preserved. Native diagnostics
show nonzero batched decode steps with a physical two-row limit.

The binary was built from the recorded foundation archive and overlay before the
final database speculative-OCR implementation; the receipt includes binary and
weight hashes. This establishes an inference batching benefit, not the final
database pipeline's speedup. All six-model residency, sustained utilization and
A100 claims remain outside this small experiment.
