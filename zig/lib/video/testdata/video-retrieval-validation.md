# Reference-preservation and pipeline qualification — 2026-10-09

This campaign tests whether Antfly preserves the pinned EmbeddingGemma 2
reference on real clips and queries. It supplies no relevance labels and makes
no claim about suitability for an end user's retrieval workload.

## Inputs and gates

The [six-document corpus](../../../../tools/embeddinggemma2/testdata/retrieval/README.md)
contains three two-second Sintel excerpts, each encoded as H.264 High and MJPEG,
at 480×320/24 FPS. Four unlabelled queries exercise full cosine-score matrices,
rankings and top-k sets. Corpus manifests pin source provenance, license, encoder
commands and every clip hash. The official checkpoint and upstream F32 source
are the same revisions recorded in [pretrained qualification](video-pretrained-validation.md).

The [52-second corpus](../../../../tools/embeddinggemma2/testdata/retrieval-long/README.md)
has 1253 frames and enables H.264 deblocking, CABAC, B-pictures and four references.
The upstream processor selects 32 frames; the independent reference retains
14,745,600 RGB bytes, rather than all decoded frames. Its one-document rankings
are trivial and cannot establish ranking preservation by themselves.

The harness validates checkpoint receipts and corpus hashes before loading a
model. It gates every vector at max absolute error 1e-4 / cosine ≥0.9999 and
scores at 1e-4, rejects non-tied ranking inversions, and reports exact ranking and
all requested top-k overlaps. Tied order differences are reported explicitly.
These tolerances were not widened after the real-clip failures.

## Preparation defect found and corrected

Native H.264 integer planes matched independent FFmpeg for all 144 short-clip
frames, but old float/nearest YUV→RGB differed from the pinned ARM conversion by
one byte on some channels. That changed two non-tied query rankings and exceeded
the vector gate. The video model now chooses independently implemented signed
Q13 component-floor arithmetic on CPU and Metal. Generic preparation retains its
old default. Four matrix/range golden cases and device comparisons qualify the
new policy. Higher-depth preparation keeps its existing arithmetic.

The model recipe is `embeddinggemma2-f32-mean-v2-q13-video`; current pretrained
identity is `ba4732081640dc8db20c48eb87ca17da5bb5bc4fe21b32b584d126e83f61bb8f`.
Index migration must honor that identity change. This qualifies the recorded
reference converter, widths, ranges and SDR metadata, not every platform's
FFmpeg SIMD dispatch or HDR/rotation policies. MJPEG uses independent Pillow/
libjpeg decoding; its policy remains distinct from FFmpeg's default JPEG IDCT.

## Executed results

[Machine-readable receipts](retrieval-validation/receipts.json) bind each report
and its original measurement artifact by SHA256. Full vectors and score matrices
are retained, not only aggregate pass flags.

| Backend / scope | Worst cosine-score error | Rankings | Top-k overlap |
| --- | ---: | --- | --- |
| macOS native CPU, six documents / four queries | 4.69e-7 | All four exact | 1.0 at every requested k |
| macOS Metal, device positions and vision helpers, same corpus | 3.46e-7 | All four exact | 1.0 at every requested k |
| PyTorch MPS against pinned CPU, same corpus | 5.52e-7 | All four exact | 1.0 at every requested k |
| Actual Linux ARM64 native CPU, same corpus | 5.26e-7 | All four exact | 1.0 at every requested k |

macOS native CPU and Metal each completed twelve video requests at concurrency
two and a disconnect/retry check. Linux completed six at concurrency two and
its retry check. A peer disconnect is requested during execution; successful
retry does not prove cancellation completed, because this HTTP response cannot
observe the canceled request's teardown. Initial Linux capacity pressure also
exposed grouped HTTP transient errors being mapped to 400. The handler now
preserves retryable 503 capacity and 507 memory-budget responses; the historical
failure is not reclassified as a passing run.

Actual Linux also passes the six pretrained cases, exact token usage and HTTP
128-dimensional truncation under the current Q13 identity, with worst vector
error 3.47e-7.

The production Metal server passed all six original pretrained cases, exact
prompt-token usage and HTTP 128-dimensional truncation/normalization after the
Q13 change, with worst vector error 2.69e-7. Its sampled server-plus-worker RSS
peak was 2,332,000,256 bytes. Actual Linux's real-clip pipeline peaked at
1,591,648,256 sampled RSS bytes. Shared mappings may be counted more than once;
unmapped GPU private allocations are excluded. Early macOS real-clip reports
sampled parent-only RSS and are explicitly labelled unsuitable for pipeline peak.

All 1253 long-clip native integer-plane hashes match independent FFmpeg. Another
96 real frames across Baseline/CAVLC, Main/CABAC, High10 and High444, with
references, weighting, B-pictures and deblocking, match exactly. The video suite
passes 114 tests with two explicit platform skips. The media suite passes 47,
including transactional fMP4 configuration changes, HLS parsing/reload/epoch
delivery and allocation rollback. Two additional production HTTP tests cover
authentication, exact ranges, redirect refusal and live configuration transitions. The
Python qualification suite passes 27 tests.

## Completed timing scope and limits

Recorded short-clip H.264 case `scene-0-h264`, three warm samples:

| Route | Median completed seconds |
| --- | ---: |
| PyTorch CPU | 2.900 |
| PyTorch MPS | 0.466 |
| Native CPU HTTP | 5.159 |
| Metal HTTP before additional vision residency | 1.722 |

The full long-clip pinned CPU reference median is 17.802 seconds; MPS is 8.856.
PyTorch timings include independent decode, preparation, model execution and
synchronized final readback. Native timings also include HTTP. Cold measurements
start a new model/server process but checkpoint verification already warms the
artifact file cache. Samples and host conditions are retained. Other workloads
were active: these are qualification observations, not production capacity or
clean-machine speedup claims. Short native timings exceed the corresponding
PyTorch reference; complete residency/batching remains a performance gate.

The first long native run failed the software decoder's single-span dependency
packet bound. `h264_sparse.decodeSelected` now uses verified IDR runs with both
per-run and aggregate bounds, preserves duplicate/presentation slots, and tests
pixel equivalence, callback failure and lease cleanup. The rebuilt sparse CPU
route and resident Metal route both pass the 52-second clip against the pinned
CPU reference, including subsequent retry and sustained requests. The maximum
score errors are 3.88e-7 (CPU) and 2.91e-7 (Metal). This long corpus has one
candidate document; its rankings/top-k are trivial, while vector/score comparison
and resource behavior remain useful gates. First completed document requests
were 78.303 seconds CPU and 23.116 seconds Metal; these controls request one
sample with no warmup, rather than a warm median. Later CPU retries/load took
longer under shared host load. Worker-inclusive sampled RSS peaks were
1,770,749,952 bytes CPU and 2,222,391,296 bytes Metal, excluding unmapped GPU private
memory. Full receipts are `long-native-sparse.json` and `long-metal-resident.json`.

The refreshed CPU real-clip campaign also passes all vectors/scores/rankings,
with maximum score error 4.69e-7 and worker-inclusive sampled RSS peak
1,815,166,976 bytes (`native-real-memory.json`). Its short H.264 warm HTTP median
is 7.702 seconds under shared host load; all 12 concurrent requests returned 200.

The resident vision helper changes now pass all six pinned pretrained cases and
the real six-clip/four-query corpus: exact rankings and top-k overlap, maximum
score error 6.66e-7. Q/K and V normalization, vision RoPE, spatial pooling and
sequence mean pooling execute on device. The short H.264 warm HTTP median is
1.098 seconds across three samples, with worker-inclusive sampled RSS peak
2,343,452,672 bytes. These observations come from a different shared-host run
than the earlier 1.722-second baseline; they are not an isolated speedup
experiment. Receipts are `metal-pretrained-resident.json` and
`metal-real-resident.json`. An injected execution capacity denial returned 503,
`Retry-After: 1` and `MODEL_RESOURCE_BUSY`; retry passed the pinned vector.

The subsequent video patch projection and learned-position additions also stay
on Metal. Row lookup flattens leading axes through a retained local view without
changing a model-owned weight handle; packed BF16/F16 rows use native row kernels.
This removes per-frame full-position-table host copies and projected-patch
readbacks. Actual GPU tests cover device/model-owned F32 and packed BF16 tables,
non-square coordinates, unchanged weight shape, invalid indices/counts and zero
additional intermediate host-mirror download bytes. Three existing row-lookup
regressions also pass. Source/binary/log hashes are retained in
`metal-position-unit.json` and its logs.

The rebuilt ReleaseSafe production binary passes all six pretrained cases,
token usage, HTTP 128-dimensional normalization and forced 503/retry, with maximum
coordinate error 2.76e-7 (`metal-pretrained-positions.json`). Fresh real-clip
qualification preserves all four exact rankings and top-k overlaps, with maximum
score error 3.46e-7 (`metal-real-positions.json`). Its short H.264 warm median is
1.163 seconds across three samples and sampled server-plus-worker RSS peak is
2,307,768,320 bytes. Shared-host conditions and a separate CPU correctness request
preclude an isolated speedup claim. The common projector ownership refactor also
passes the native CPU two-video pretrained case (`native-projector-common.json`);
that receipt is explicitly a single-case gate.

The final device-position binary also passes the complete long-clip campaign
(`long-metal-positions.json`), including retry and sustained requests, with maximum
score error 3.17e-07. Its first document request completes in 17.082 seconds
(one sample, no warmup); sampled server-plus-worker RSS peaks at 2,363,670,528
bytes. The same shared-host, memory-scope and trivial single-candidate ranking
limits apply.

Monochrome host staging and direct native Metal texture import match CPU at
8/10/12/14 bits across both rounding policies, ranges and rotations. Actual
CoreVideo P010 pixel-buffer import and host staging preserve MSB-aligned 10-bit
samples, match a separately right-aligned CPU reference and survive producer
release. This qualifies surface import/preparation; it does not qualify a
VideoToolbox High10 decoder or HDR tone mapping.

## Remaining gates

Batched resident vision/projector execution, final device normalization, qualified
logical field/partition indexing and model VFR/display/HDR policies, broader HLS
and DASH ingestion, VP8 interframes and pure Zig VP9/AV1 decoding remain open.
Device video token composition/backbone inputs and bounded decode/preparation/
projection overlap are delivered by the continuation below. Native HTTP fMP4 HLS ingestion and pure Zig VP8 displayed
keyframes now have separately bounded and tested library entry points; they do
not qualify model WebM inputs or complete streaming protocol/codec coverage.
Further device helper changes must pass pretrained and real-clip gates after
rebuilding; prior receipts do not qualify subsequent residency changes. CUDA/NVDEC remains
in the separate accelerator plan.


The manual `.github/workflows/zig-video-qualification.yml` repeats portable video,
native authenticated HLS, pretrained capacity/retry and matched local
PyTorch/native measurements on Linux x86-64, with an optional long-clip campaign.
It pins the repository Zig toolchain, Python, PyTorch/Torchvision and Transformers
revision, verifies the checkpoint receipt and retains tool versions and reports.
Its FFmpeg package/version is recorded independently. Model acquisition may use
the caller's repository `HF_TOKEN` secret. The workflow was linted locally but
has not been dispatched; it adds repeatability, not a new Linux execution receipt.


## Device composition and projection scheduling continuation

`composition-gates.json` binds the final production binary and source hashes.
`metal-composition-unit.json` and its log qualify mixed-media concat/gather values
and zero intermediate host-mirror downloads on actual Metal; that helper binary
predates subsequent scheduling edits and is not a full-pipeline receipt.

The final binary passes all six pinned cases, token usage, HTTP 128 dimensions,
503/capacity retry and a corrupted second selected MJPEG frame followed by a valid
retry on both Metal and native CPU. Maximum coordinate errors are 2.76e-7 and
4.77e-7 respectively (`metal-pretrained-composition.json`,
`native-pretrained-composition.json`). Preparation destruction now fences pending
GPU work before releasing its resource/admission owner. The bounded inference
allocator serializes accounting across producer and projection worker.

`metal-real-composition.json` preserves all ten vectors, four exact rankings and
all top-k sets against pinned PyTorch CPU, with maximum score error 3.46e-7.
Concurrent/sustained requests and disconnect/retry pass. The short H.264 warm
median is 1.064 seconds across three samples; sampled server-plus-worker RSS peaks
at 2,303,279,104 bytes. These shared-host measurements do not isolate a speedup.

`long-metal-composition.json` passes the complete 52-second clip's vector/score,
retry and sustained gates. Its first document request completes in 15.127 seconds
(one sample, no warmup), retry in 30.719 seconds and sustained request in 14.711
seconds; sampled server-plus-worker RSS peaks at 2,352,824,320 bytes. The single
candidate corpus has trivial rankings. Peer disconnect/retry still does not
expose cancellation completion. RSS excludes unmapped GPU private memory and
can count shared mappings twice. No fresh Linux pretrained run is claimed.

Portable logical-picture duration sampling, orthogonal display/SAR geometry and
static DASH SegmentList ingestion have their own library tests; they do not
broaden the default model policy or qualify VP-family decoding. Batching,
VP8 interframes, VP9/AV1 and broader DASH addressing/live scheduling remain open.
