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

`preprocess_ns`, `vision_ns`, `pack_ns`, `encoder_ns` and `pool_readback_ns`
observe CPU boundaries around potentially asynchronous GPU operations. Use them
for attribution, not isolated GPU timing. `gpu_paint_seconds` measures the Metal
command buffer; render wall time additionally includes CPU scene extraction,
buffer allocation/upload and host output copy. Shared-memory transfer counters
measure logical copies, not discrete-GPU PCIe traffic. Provider peaks omit model
weights and runtime pools. Total RSS and CUDA counters are not inferred from these.

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
