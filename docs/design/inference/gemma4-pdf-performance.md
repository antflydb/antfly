# Gemma4 real-PDF performance and GPU rendering experiment

The production change keeps image-only EmbeddingGemma 2 visual soft tokens,
prompt embeddings, encoder activations and pooled vectors on their model backend.
Metal downloads only the requested final vector. CPU image decoding and
model-exact bicubic resizing remain shared with the retained host baseline.
Mixed media prompts retain their existing path. CUDA uses resident row operations
and reductions but needs separate device qualification; local execution is Metal.

The PDF experiment is a separate benchmark executable. Its Metal painter supports
simple opaque fills and axis-aligned opaque nearest-sampled images. Unsupported
pages use the exact CPU renderer. CPU parsing/codecs and a host raster copy remain
in the measured GPU case. This experiment supplies evidence for a future resident
raster/preprocessing interface; it does not change production PDF rendering.
See [the batching design](../../../zig/pkg/inference/BATCHING.md#gemma4-pdf-measurement-and-resident-embedding-handoff)
for ownership, admission, geometry and fallback rules.

## Reproduce

Build with the repository's Zig 0.17 toolchain:

```sh
cd zig/pkg/inference
zig build build-gemma4-pdf-bench -Dmetal=true -Doptimize=ReleaseFast
zig build pdf-gpu-test -Dmetal=true -Doptimize=ReleaseFast
zig build test-bin -Dmetal=true -Doptimize=ReleaseFast \
  -Dtest-filter='embeddinggemma2 resident' -Dtest-filter='gemma4 image batching'
zig-out/bin/antfly-inference-tests
cd ../../..
python3 -m unittest discover -s tools/embeddinggemma2 -p test_pdf_campaign.py
```

Supply real PDFs from the intended workload and the HF EmbeddingGemma 2 model:

```sh
python3 tools/embeddinggemma2/pdf_campaign.py \
  --binary zig/pkg/inference/zig-out/bin/antfly-gemma4-pdf-bench \
  --model /path/to/embeddinggemma-2 \
  --pdf /path/to/text-and-figures.pdf --pdf /path/to/scanned.pdf \
  --pdf /path/to/vector-heavy.pdf \
  --output-dir .benchmark-results/gemma4-pdf/campaign \
  --batches 1,2,4 --workers 4 --dpi 150 --max-dimension 1536 \
  --warmups 2 --iterations 10 --max-pages 12 --machine-state idle
```

The campaign runs child processes sequentially, retains JSON and logs, and checks
every candidate against the serial CPU-render/host-handoff vectors. Each batch
size has its own same-batch host comparison. A render-only GPU pixel preflight
runs before model measurements and is excluded from the timing summary.
`--gpu off` runs only the CPU-render
handoff comparison; `--backend native --gpu off` exercises CPU compatibility.
Use a GPU-accessible process. A missing Metal device fails the GPU case explicitly.
No retry substitutes a different renderer after GPU execution or parity fails.

For controlled eligibility fixtures, run
`python3 tools/embeddinggemma2/pdf_fixtures.py /tmp/gemma4-pdf-fixtures`.
These generated PDFs contain a grid of fills and a small opaque image. They are
synthetic parity fixtures and must be reported separately from real documents.
The grid qualifies at 144 DPI; 150 DPI exercises conservative coordinate fallback.

## What the measurements mean

Warm throughput includes page scene extraction/rendering, CPU preprocessing,
vision, prompt packing, the embedding encoder and final vector readback. Initial
model load and document parse/template creation are separate observations.
Tokenizer loading and prototype renderer setup occur outside warm timing and
are not included in `model_load_seconds`; full cold-process startup is unqualified.
The run is bounded to one render wave and one model cohort at a time; it does
not overlap the next render wave with GPU inference. CPU workers each instantiate
an independent reader from an immutable template. Final output IDs follow the
round-robin page schedule regardless of renderer assignment or geometry splitting.
The optimized painter prepares GPU scenes in those same bounded workers and
overlaps GPU painting with CPU fallback within the render wave. No prepared-scene
or raster cache removes page preparation from the timed boundary.

`preprocess_ns`, `vision_ns`, `pack_ns`, `encoder_ns` and `pool_readback_ns`
observe CPU boundaries around potentially asynchronous GPU operations. Use them
for attribution, not isolated GPU timing. `gpu_paint_seconds` measures the Metal
command buffer; render wall time additionally includes CPU scene extraction,
buffer allocation/upload and host output copy. Shared-memory transfer counters
measure logical copies, not discrete-GPU PCIe traffic. Provider peaks omit model
weights and runtime pools. Total RSS and CUDA counters are not inferred from these.
`prepare_seconds_sum` and `cpu_render_seconds_sum` add worker durations and can
exceed wall time because workers run concurrently. `gpu_wall_seconds` includes
buffer staging, GPU submission/completion and host output copy.
`gpu_cached_buffer_bytes` reports retained prototype buffers (at most 512 MiB)
separately from model-owned tensors and excludes transient allocation overlap.

The strict pixel gate is outside measured wave time and runs once per selected
GPU page; warmups avoid measuring its effects. The embedding gate rejects missing
or reordered outputs, zero/nonfinite vectors, shape changes and cosine < 0.9999.
Passing relative parity does not establish independent model quality.

## Local evidence, 2026-10-11

Real inputs are the [Attention Is All You Need paper, v7](https://arxiv.org/pdf/1706.03762v7)
and the [W3C scanned OCR example](https://www.w3.org/WAI/WCAG22/working-examples/pdf-ocr/ocr-example.pdf).
Their first pages exercise text/figures and scanned content. Synthetic fill/image
PDFs additionally exercise the GPU path and cross-document batches. Raw samples,
vectors, manifest hashes and logs remain under
`.benchmark-results/gemma4-pdf-2026-10-11/`; the compact report records conditions.

An initial two-page Metal check matched host/resident vectors at cosine above
0.999999. Tensor downloads decreased from 3,801,088 bytes and five readbacks to
6,144 bytes and two readbacks. Exact parallel CPU rendering took about 47 ms in
a roughly ten-second pipeline observation. This makes rendering a small fraction
of that run and does not support a large GPU-rendering speedup for those pages.
The machine had concurrent development activity; this is correctness and transfer
evidence, not a controlled throughput claim.

The first full-corpus GPU pixel gate failed on the scanned page (maximum byte
error 255, MAE 4.12). Investigation found that the CPU renderer snaps the scaled
page extent to the integer raster plan; using the fractional PDF upper edge
shifted sample rows in the prototype. The painter now uses that same snapped
origin/extent and has an independent fractional-origin image regression.
The failed campaign and its diagnostic log remain in the raw artifacts.

The final same-binary campaign used an Apple M4 Max with 36 GiB RAM, one warmup
and three measured passes of all four selected pages at requested 144 DPI,
maximum dimension 1536. GPU painting matched the scan and both synthetic pages
byte-for-byte; the paper used CPU fallback. Every vector in every case matched
the serial host reference at cosine >= 0.9999999999992. Batch 4 executed one
embedding cohort for the four pages; vision geometry groups may subdivide.

| Renderer / handoff | Requested batch | Four-page p50 s | Four-page p95 s | Pages/s |
| --- | --- | --- | --- | --- |
| CPU / host | 1 | 10.014 | 11.115 | 0.393 |
| CPU / resident | 1 | 10.484 | 10.748 | 0.390 |
| GPU with CPU fallback / resident | 1 | 11.397 | 11.427 | 0.359 |
| CPU / host | 4 | 11.434 | 11.493 | 0.358 |
| CPU / resident | 4 | 16.744 | 20.376 | 0.228 |
| GPU with CPU fallback / resident | 4 | 8.490 | 8.852 | 0.466 |

A counterbalanced CPU-render repeat then observed batch-4 host/resident p50 of
8.743/9.133 seconds and throughput of 0.456/0.442 pages/s. The resident gap moved
from about 36% to 3%. Concurrent activity and three samples prevent attributing
these differences to the handoff. These observations establish neither a runtime
speedup nor a controlled regression; the large variation remains unresolved.
The sample p95 is the slowest of three passes, not a deployment tail estimate.

Resident tensor downloads stayed at 12,288 bytes and four readbacks per four-page
pass. Host downloads were 7,414,784 bytes at batch 1 and 7,540,736 bytes at batch 4.
That is about 99.8% fewer copied bytes. Device-owned tensor peaks at batch 4 were
about 320.5 MB for host and 321.6 MB for resident, excluding weights/runtime pools.

Render-only p50 for four pages was 84.3 ms CPU versus 84.8 ms GPU at batch 1, and
42.6 ms parallel CPU versus 77.6 ms GPU at batch 4. The prototype did not beat
parallel CPU rendering on this sample. Its CPU extraction, allocation and host
output copy remain inside that boundary; it is not a measurement of an all-device
PDF pipeline. Rendering was a small part of the multi-second embedding runs.

[The compact results report](gemma4-pdf-performance.json) retains hashes, all
case results, stage/copy/memory observations, repeated comparisons and limits.
Validation passed 38 focused CPU/Metal tests with three CUDA execution skips,
three GPU PDF tests, 24 Python tests and six client tests. The resident fixtures
cross-compiled for Linux CUDA; CUDA hardware was not available for execution.
The client tests needed local networking outside the filesystem sandbox.

## GPU renderer optimization, 2026-10-11

The original kernel visited every command for every pixel, and the benchmark
serialized scene preparation, CPU fallback and GPU painting. The revised painter
bins commands into 32x32 tiles, preserves paint order in each list, and rejects
commands outside conservative bounds. Tile storage is capped at 16 MiB/page;
dense pages use the bounds-culled kernel without a tile list. Four synchronous
Metal buffer slots reuse storage after GPU completion, capped at 512 MiB.

Bounded workers now prepare GPU scenes in parallel using independent readers.
They publish classification before CPU fallback finishes, allowing the painter
to overlap that remaining work. Unsupported pages reuse their private reader for
fallback. Every exit joins workers and releases owned scenes and rasters.
Scene/raster caching does not remove preparation from the timed boundary.

A preserved baseline binary and the new binary were compared with two warmups
and 20 measured passes per case, in both process orders. Each GPU-painted selected
page matched native pixels exactly. CPU control timings remained broadly stable.
These are local rendering observations under concurrent development activity.

| Input group | Old GPU p50 ms | New GPU p50 ms | Parallel CPU p50 ms |
| --- | --- | --- | --- |
| Four-page mixed batch | 61.6–62.0 | 38.0–38.1 | 38.7–39.3 |
| Three GPU-eligible pages | 44.8 | 38.1–38.2 | 38.9–39.1 |
| Synthetic fill-grid page | 7.4–7.5 | 0.8–1.0 | 6.6 |
| Real scanned page | 35.5 | 37.2–37.5 | 38.7–39.0 |

Mixed-batch GPU command-buffer time fell from about 10.4–10.7 ms to 0.66–0.67 ms.
The new GPU staging/submission/completion/output-copy boundary averaged about
1.5 ms, and retained prototype buffers occupied 26.1 MiB. The mixed render batch
is about 1.6x faster than the old GPU version and now roughly matches parallel CPU.
The much larger fill-grid gain applies to the synthetic vector fixture.

The scan remains CPU-preparation bound: about 36.6–36.7 ms before roughly 0.18 ms
of GPU painting. Its new GPU total is slightly slower than the old GPU path,
which retained a main reader across passes; the new path prepares a bounded
private reader each wave. It remains slightly faster than CPU rendering in this
sample. That campaign did not isolate codec costs; the preparation follow-up below
profiles them. The result does not establish an all-device PDF decoder.

Four GPU tests passed, covering unsupported content, distinct batched pages,
fractional origins, tile-boundary clipping/paint order, and buffer reuse/clearing.
One-worker 150-DPI and two-worker mixed-page checks preserved GPU/CPU fallback order. The mixed real-model
batch retained exact rendered pixels and matched the previous serial host vectors
at cosine >= 0.9999999999992. That pass verifies correctness, not an overall
embedding throughput gain. All 24 Python checks also passed.

[The renderer results report](gemma4-pdf-render-performance.json) records both
binary hashes, inputs, all 32 cases, GPU versus wall observations, buffer bytes,
validation and limits. Raw JSON, logs, the preserved baseline and campaign script
are under `.benchmark-results/gemma4-pdf-render-2026-10-11/`.
For a render-only comparison, run the benchmark with `--scope render`, identical
PDF/DPI/dimension/worker arguments, and `--renderer cpu` or `--renderer gpu`.
Model loading is unnecessary for that scope.

## JPEG preparation follow-up

A three-second macOS `sample` profile of the real W3C scan attributes the
remaining preparation cost to the shared CPU JPEG decoder, including canonical
Huffman symbol decoding and block reconstruction. The Metal painter is already
below one millisecond; moving more paint work onto the GPU does not address
that codec cost.

The shared decoder now builds a 256-entry, eight-bit prefix table per canonical
Huffman table. Codes of at most eight bits decode in one lookup; longer codes and
short tails retain canonical bitwise decoding. Coefficient loops borrow those tables:
the compiler otherwise copies the expanded 850-byte table per symbol, as confirmed
in the initial candidate's ARM64 assembly. The public by-value API remains available
for compatibility. Lookahead preserves stuffed bytes,
pending restart markers and available tail bits rather than requiring eight bits
at a scan boundary. Each table adds 512 bytes; decoder working sets remain bounded
by the existing per-component table count. This changes CPU JPEG preparation in
both the normal PDF renderer and the benchmark GPU path. It introduces no decoded
page cache or alternative image quality setting.

Direct JPEG tests now participate in `lib-image-test` and can also run through
`zig build lib-image-jpeg-test`. Previously, importing the image module did not
discover those codec tests. Wiring them in exposed stale color tests: two old
baseline calls lacked the current scaling/color options, one DeviceCMYK expectation
used JPEG display inversion, and a component-ID test used a grayscale fixture and
changed frame selectors without changing scan selectors. The failures reproduced
with the original decoder; correcting the tests leaves color conversion unchanged.

The prefix test compares all 65,536 sixteen-bit reservoirs against an independent
bitwise decoder, including eight- and sixteen-bit codes and unused prefixes.
Boundary tests cover short scan tails, JPEG byte stuffing and pending restart
markers. An independent old/new decoder comparison on the real scan verifies
byte-for-byte RGBA output at full resolution and maximum dimensions 1536, 768
and 384. The temporary comparison harness, saved binary, profiler output and raw
measurements live under `.benchmark-results/gemma4-pdf-preparation-borrowed-2026-10-11/`.

The final paired render campaign uses the saved `5d3f01e4a7` binary, 20 measured
waves after two warmups, 144 DPI, maximum dimension 1536, four workers, and both
variant orders. All GPU-selected pixels match the CPU output exactly.
Concurrent development activity remains a limitation; these are local observations.

| GPU render case | Baseline p50 | Final p50 | Wall-time reduction |
| --- | --- | --- | --- |
| Mixed four-page wave | 49.0–53.7 ms | 43.5–45.9 ms | 11–15% in paired orders |
| Real W3C scan | 47.7–53.4 ms | 40.6–41.9 ms | 12–24% in paired orders |
| Four copies of the real scan, independent readers | 58.3–59.5 ms | 45.9–50.0 ms | 16–21% in paired orders |

The four-scan batch measures concurrent preparation of the same real document,
not four different scan layouts. CPU controls vary with machine load and show
smaller or inconsistent gains; the table does not establish a general GPU-versus-CPU
speedup. An isolated old/new JPEG ABBA comparison (20 fresh decodes per variant)
observes about a 9% decode reduction in both orders: 40.8–44.5 ms before versus
37.1–40.5 ms after. It also verifies exact old/new pixels at all four resolutions.

All 172 direct JPEG tests pass in ReleaseSafe, along with the shared image,
PNG and JPEG 2000 suites. Four actual Metal renderer tests pass in ReleaseFast.
[The preparation results report](gemma4-pdf-preparation-performance.json) records
binary/input/source hashes, individual paired render cases, isolated decode timing,
end-to-end observations, validation and measurement limits.

The final embedding comparison keeps GPU rendering and the resident model handoff
identical across binaries, runs baseline/candidate and then candidate/baseline,
and uses two warmups plus three measured four-page waves per process. The candidate
p50 is 8.76 s versus 10.58 s in the first order, then 10.64 s versus 9.86 s in the
reverse order. Vision-stage observations vary with the process timing, while
rendering accounts for well under 1% of total wall time. These observations do
not establish an overall PDF embedding speedup. All final vectors match the earlier
serial CPU-render/host-handoff reference at cosine >= 0.9999999999992, and all
GPU-selected page pixel comparisons remain exact.

## Next decision

Keep production rendering on CPU while the prototype measures supported-page
coverage and end-to-end benefit. If rendering becomes material on a representative
corpus, first compare overlapped bounded CPU rendering against a device-raster
prototype that avoids the host copy and reproduces model-exact preprocessing.
Own completion/cancellation and admission through the last raster consumer before
enabling that path in the service. Extending fonts/transparency/complex clipping
requires its own pixel qualification; a faster simple-fill fixture is insufficient.

The [PDFium public API](https://pdfium.googlesource.com/pdfium/+/refs/heads/main/public/fpdfview.h)
exposes bitmap rendering and requires serialized library calls. Selecting its Skia
renderer alone therefore does not establish a GPU-resident tensor contract.
This prototype instead uses Antfly's existing parser/render runs and a dedicated
Metal painter. [Metal command-buffer timing](https://developer.apple.com/documentation/metal/mtlcommandbuffer/gpustarttime)
is kept separate from CPU enqueue and synchronized end-to-end timing.
