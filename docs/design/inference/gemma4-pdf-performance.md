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

See the [media history](history/gemma4/media-throughput-2026-10.md#pdf-resident-handoff)
for the source-pinned diary and failed/repeated trials. Resident four-page
handoff reduced tensor downloads to 12,288 bytes/four readbacks (~99.8% fewer
bytes), with cosine >= 0.9999999999992. Contention moved the apparent batch-four
resident regression from ~36% to ~3%; three-sample campaigns established neither
a controlled regression nor a full PDF embedding win.
[Original evidence](gemma4-pdf-performance.json) retains scopes and raw summaries.

## GPU renderer optimization, 2026-10-11

32×32 command tiles preserve paint order, with conservative culling and a
16 MiB/page list cap; dense pages retain the bounds-culled fallback. Four
synchronous Metal buffer slots reuse at most 512 MiB. Bounded private readers
publish scene classification before CPU fallback completes so painting can
overlap fallback. Cleanup joins all workers. No page/raster cache excludes
preparation from the measured boundary.

Mixed four-page rendering improved from ~62 to ~38 ms, roughly matching parallel
CPU. Synthetic fills improved substantially; scanned-page preparation remained
CPU-bound. This is a renderer observation, not full embedding throughput.
[Renderer evidence](gemma4-pdf-render-performance.json) and
[history](history/gemma4/media-throughput-2026-10.md#gpu-renderer-optimization)
retain the scope, source/binary pins, pixel failures and parity correction.

## JPEG preparation follow-up

The shared CPU JPEG decoder uses borrowed canonical Huffman tables and an
eight-bit prefix lookup, retaining bitwise fallback for long codes/short tails.
Stuffed bytes and restart markers keep their original semantics. No image-quality
change or decoded-page cache is introduced. Old/new RGBA matched at four
resolutions; the isolated paired decode observation was ~9% faster.
Full PDF embedding times reversed direction between process orders, so no
end-to-end speedup was established. [Preparation evidence](gemma4-pdf-preparation-performance.json)
and [history](history/gemma4/media-throughput-2026-10.md#jpeg-and-webp-preparation)
retain exact conditions and rejected trials.

## Next decision

Keep production rendering on CPU until supported-page coverage and an end-to-end
benefit are measured on representative PDFs. If rendering becomes material,
compare bounded overlapped CPU rendering with a device-raster interface that
avoids host copies while preserving model-exact preprocessing. Own admission,
completion and cancellation through the last raster consumer. Fonts, transparency
and complex clipping require independent pixel qualification; synthetic fill
speedups are insufficient. The current benchmark still uses CPU parsing/codecs,
a host output copy and CPU preprocessing.
