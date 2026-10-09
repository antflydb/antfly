# Native video inference and EmbeddingGemma 2

Status: phase 1, the independent phase 2 decoder/preparation library, the phase 3
scheduling subset, stage 4 portable MJPEG/Metal preparation, independent
efficiency/admission/source work and a pure Zig H.264 subset delivered,
2026-10-08. Phase 2 model/API integration, phase 3 resident model execution, and
the remaining stage 4 routes remain pending; full video embedding is not yet
available. Tim's PR #1014 remains a separate dependency
at the user's request. The initial design was written
against `origin/main` commit `cdf572a7467d581f6f1b39bcf514878488555f11`;
fetching a newer remote head was unavailable because GitHub DNS resolution failed.

Library contracts live in [MEDIA.md](../../zig/lib/media/MEDIA.md) and
[VIDEO.md](../../zig/lib/video/VIDEO.md). Existing foundations are
[AUDIO.md](../../zig/lib/audio/AUDIO.md),
[IMAGE.md](../../zig/lib/image/IMAGE.md), and the
[bounded multimodal document pipeline](../../zig/PDF.md).

## Problem and intended behavior

Antfly needs an in-process video preparation path that can supply ordered visual
tokens to native inference without FFmpeg subprocesses, full-resolution RGB
materialization, or repeated decoding for overlapping indexing windows.
The first model integration is EmbeddingGemma 2, while the media libraries stay
independent of that model and available to later video-capable models.

Tim's [PR #1014](https://github.com/antflydb/antfly/pull/1014) proposes native
CPU/Metal EmbeddingGemma 2 text, image, audio, and ordered-group embeddings, and
explicitly excludes video. This plan builds on its proposed model identity,
resident weights, admission, ordered-content, and pooling contracts without
assuming the PR has merged. Recheck its final interfaces before implementation.

A video input should return one joint embedding of the selected ordered frames,
optionally interleaved with text and explicitly requested audio. Independent
image embeddings averaged afterward are a different representation. Long-video
moment retrieval should index bounded clip windows with source intervals so a
match can resolve to a playback position.

## Model contract and evidence scope

At design time, the processor configuration carried in PR #1014 specifies:

| Setting | Pinned processor value |
| --- | --- |
| Sampling | 1 FPS |
| Frame cap | 32 selected frames |
| Overflow | Uniform resampling across the input |
| Video soft-token budget | 140 per frame |
| Patch geometry | 16-pixel patches, spatial pooling kernel 3 |
| Resize | Aspect preserving, bicubic with reference antialias behavior |
| Pixel rescale | `1 / 255` |
| Timestamp text | Disabled |
| Context | 8,192 tokens shared across all modalities |

These are checkpoint processor settings, not universal library defaults. The
Transformers processor class has its own fallback values; load the actual pinned
checkpoint configuration. The [reference video processor](https://raw.githubusercontent.com/huggingface/transformers/main/src/transformers/models/embedding_gemma2/video_processing_embedding_gemma2.py)
defines patch layout, positions, sampling, and frame-group batch metadata.
Pin its revision with the model/processor artifacts when producing fixtures.

Google's [model card](https://huggingface.co/google/embeddinggemma-2) describes
140 tokens per video frame and roughly 58 frames within an otherwise unused
context. That arithmetic ceiling differs from the processor's 32-frame cap;
actual expansion includes required special tokens and any other content. A long
video can be sampled over its full duration. There is no two-second video limit
established by these sources. The approximately 2.03-second number in Tim's PR
is an 8K prepared-token encoder timing, excluding HTTP and preparation.

Match video-specific placeholders, frame grouping, soft tokens, patch masks,
position IDs, backbone masks, active-token mean pooling, learned projection,
dimension truncation, and normalization. Do not expand a video into ordinary
image parts and assume parity. Preserve the PR's safe F32 execution initially;
the model card warns against FP16 activation execution. Any BF16 or quantized
route requires independent numerical and retrieval qualification.

## Execution architecture

```text
bounded source / range reader
  → lib/media tracks, timestamps, packet index
  → model adapter sample policy → lib/video dependency plan
  → decode required reference pictures → leased YUV/device surfaces
  → selected-frame preparation → bounded batched vision execution
  → resident projected tokens → ordered joint backbone sequence
  → masked pooling / projection / truncate / normalize → embedding
```

Sampling happens before pixel decoding where possible. Decode necessary
dependencies once, but preprocess and run vision only for selected outputs.
The default efficient Apple route is VideoToolbox → CoreVideo surfaces → Metal
preparation/vision. NVDEC → CUDA is a later route and also requires a qualified
model backend; PR #1014 does not establish EmbeddingGemma CUDA support.
Pure Zig software decode remains a separately scoped portable backend.

The adapter reads the pinned processor, calculates target aspect-preserving
patch geometry, and writes directly into caller-owned patch storage. Group
frames by compatible patch geometry and execution budget, retaining original
clip/frame order. Vision batches may span requests only if model generation,
device, processor policy, admission, and cancellation semantics agree. Backpressure
prevents an entire video's pixels from collecting in memory.

Retain projected vision tokens on-device until joint backbone execution. Shared
GPU tensors need a direct model input path; reading them to host and uploading
again defeats the intended route. Preserve token sequence and masks while
microbatching vision. Joint backbone splitting cannot assume independent chunks
because global attention changes the result.

The PR reports image/audio paths approximately 6.1×/11.6× slower than PyTorch MPS,
outside its selected text performance qualification. Treat resident vision
execution as a first-class optimization milestone. Fast decode alone cannot
establish fast video embedding; the existing text timing is not a video estimate.

## Metal implementation track

Metal is a required first implementation target alongside CPU correctness, not
an optional follow-up to decoder integration. VideoToolbox supplies decoded
pictures; Metal executes selected-frame preparation, the vision encoder and
projector, and the joint embedding backbone. Codec reconstruction stays in
VideoToolbox or the pure Zig software decoder rather than becoming a Metal
H.264 implementation.

The Metal work has these concrete deliverables:

1. Import compatible CoreVideo YUV planes as textures on the inference device.
   Also accept host YUV planes from the pure Zig decoder through one bounded,
   accounted upload path. Software decode must still be able to use Metal
   preprocessing and model execution.
2. Implement reference-compatible color conversion, displayed orientation,
   antialiased bicubic resize, rescale, patch packing, position IDs, and padding
   masks. Establish separate intermediate kernels before qualifying fusion;
   preserve reference rounding and conversion order. Write into caller-owned
   Metal patch buffers without a full-resolution RGB readback.
3. Keep vision/projector weights resident and batch compatible frame geometries
   through the existing Metal inference provider. Profile attention, projection,
   normalization, and activation operations to eliminate per-operation host
   fallbacks and avoidable submission/wait boundaries. Use bounded scratch reuse
   with explicit resource access registration; do not assume all frames fit one
   command buffer or one vision batch.
4. Accept resident projected frame tensors directly in ordered backbone input
   assembly. Preserve video tokens and masks without host readback/re-upload.
   Run pooling, projection, dimension truncation, and normalization on Metal,
   and read back only the requested final vector plus necessary diagnostics.
5. Fence surface and scratch lifetimes across decoder callbacks and Metal
   command completion. Connect device allocations and imported backing resources
   to admission; cancellation stops future submissions and releases outstanding
   leases only after their consumers complete.

Preparation kernels and model changes belong in the inference Metal backend;
surface import/capability/lifetime abstractions belong in `lib/video`.
Container and sampling modules remain device-independent. Unsupported import
formats may use an explicit qualified upload/preparation route, with telemetry
and limits; they must not silently change dtype, sampling, or image quality.

Qualify both VideoToolbox → Metal and software decode → Metal against identical
prepared-frame references. Capture preparation, vision, backbone, submission,
and synchronization costs separately, then compare synchronized raw-file
end-to-end latency to official PyTorch MPS. Require numerical/retrieval parity,
bounded retention under concurrency/cancellation, and measured absence of pixel
or projected-token host round trips on the resident route before promotion.

## Serving, admission, and indexing

Extend the existing inference content-part contract with an explicit video part
and optional clip/sampling controls. Exact OpenAPI field names remain to be
chosen against the merged ordered-group API. Support the same authorized source
resolution as other media and an internal borrowed-frame/surface path. Keep
device handles internal to the process; public JSON cannot represent them.
Generate SDK contracts from the canonical schema when implemented.

Validate container metadata and model capabilities before expensive loading or
decode. Reserve parser/read buffers, dependency decode surfaces and scratch,
prepared patch batches, resident vision outputs, and joint backbone workspace.
Admission must use source geometry/reference counts as well as final token
counts. On unified memory, charge aliases once while retaining them for all
users. Enforce aggregate request and concurrent limits, cancellation, original
deadlines, stable result ordering, and existing per-item error semantics.

Predict expanded tokens before vision when metadata permits, then validate the
actual expansion. Reduce concurrency/batch sizes to fit reservations without
silently changing frame selection or visual quality. If a requested frame policy
cannot fit, return a clear limit/capacity error or apply only an explicitly
requested overflow policy. Never drop frames to turn a denial into success.

Video audio is opt-in. Use the same source interval/timeline as selected visual
content, preserving gaps and priming. Joint audio/video sequences draw from the
same context budget. Visual-only requests do not load the audio encoder.

For long-video indexing, use deterministic clip windows with source version,
track ID, half-open source interval, actual frame IDs/PTS, processor revision,
sampling/preparation policy, model generation, output dimensions, and optional
audio policy in artifact/cache identity. Keep a forward decoder across nearby
windows and reuse identical prepared/projected frames under a bounded cache.
Joint embeddings remain per-window; reusable frame tokens do not imply reusable
contextual backbone outputs. Publish through the existing fenced enrichment
lifecycle, with interval provenance sufficient to return a moment in the source.
Changes to preparation policy require invalidating affected artifacts.

## Implementation stages and exit criteria

### 1. Shared media foundation and reference fixtures — delivered

Extract MP4/WebM container machinery into `lib/media` behind existing audio
entry points. Preserve all currently qualified audio timing/PCM results. Add
video metadata, packet/timeline indexes, exact pinned sampling fixtures, and
bounded range readers. Qualify non-fragmented MP4 first; other container shapes
remain explicitly unsupported.

Delivered in `lib/media` and `lib/video`: shared ISO BMFF/EBML primitives,
existing MP4/WebM audio adapters, bounded immutable sources and packet leases,
signed timeline mapping, a non-fragmented AVC MP4 index, and frame selection.
Six synthetic MP4 fixtures pin FFprobe packet/timeline receipts; 77 sampling
cases execute a hashed upstream method snapshot with NumPy 2.4.4. Runtime code
has no FFmpeg or Python dependency. Generalized codec dependency
plans remain later work; static-avc1 IDR planning is delivered in stage 3.

Exit validation: the original and migrated MP4 audio suites each pass 68 tests,
and the original and migrated WebM suites each pass 16 tests. Root `test-media`
and `test-video`, inference `test-audio`, and wasm32-WASI compile checks qualify
packet/timeline/selection parity, bounded metadata/source buffers, and
cancellation/lease/allocation failures. See the library docs for the precise
implemented API and unsupported shapes.

### 2. Apple decode and correct EmbeddingGemma video — library delivered, integration pending

Delivered independently of PR #1014: native VideoToolbox H.264 sessions with
owned selected NV12 surfaces, explicit hardware enforcement/route receipts,
CoreVideo-to-Metal imports, and portable CPU/Metal quantized bicubic patch
preparation. Tests qualify progressive fixture decode, CPU/Metal prepared values,
rotation/SDR policies, drain/retry, owned metadata/surface lifetimes, limits and
allocation failures. Linux and wasm builds exclude Apple dependencies, and the
portable selection/host-preparation suites execute on WASI. See
[VIDEO.md](../../zig/lib/video/VIDEO.md) for the implemented API and limits.

The decoder defaults to packet zero; optional verified-IDR dependency planning
is delivered in the independent stage 3 subset below. Full native-workspace
accounting, generalized color/display policies and model preparation geometry
parity require later qualification. Model/backend code from
Tim's current head `7a7b63f63da5e772309597ba2409a9ef50860a59` is not merged here.

Remaining after that dependency is available: integrate video-specific ordered
tokens with the final PR #1014 interfaces. Add internal borrowed surfaces and
canonical API/SDK video contracts. A host preparation path can establish the
oracle before device fusion is enabled.

Exit: CPU-reference/Metal prepared patch and embedding comparisons, end-to-end
HTTP/group parity, qualified source-format capabilities, and drain/retry tests.
Do not claim an efficient route while it still round-trips full pixels or vision
tokens through host memory.

### 3. Metal resident vision and production scheduling — scheduling subset delivered

Delivered independently of PR #1014: portable bounded static-avc1 decode plans
that verify IDR payloads rather than trust sync hints; overlapping timestamp-window
plans that reuse unique pictures; borrowed-frame streaming outside native
callbacks; and a bounded decode-to-Metal preparation queue. Completed commands
release source imports/intermediates while retaining output buffers. Results own
window mappings/PTS/configuration identity, output buffers, metadata and work/
queue counters. Cancellation and allocation/consumer failures drain/fence work.

Qualification: a 60-picture closed-GOP range source selected at packet indexes
12 and 59 uses 13 submissions/reads and 14,137 payload bytes, versus 60 and 61,067
from the start, with exact pixel parity. Open-GOP recovery hints fall back to the
initial IDR. Overlap reuse, depth-one/two bounds, CPU/Metal output comparisons,
reader teardown, allocation failures and cancellation/retry are tested. Linux
compiles the portable APIs; WASI executes portable planner/host tests. These
counts establish reduced work, not end-to-end latency or retrieval parity.

Still pending after the model dependency: qualify preparation geometry against
the final processor, implement batched resident vision/projector execution,
direct device-token backbone inputs, and device pooling/normalization. Qualify
resident model execution on the delivered software-decode → Metal and
VideoToolbox → Metal preparation routes. Extend the delivered decode/preparation
queue to overlap resident vision execution, with atomic admission across
concurrent source/surface/model reservations and any persistent token cache.
Tune seek-gap thresholds from measured backend/source latency. Establish a
synchronized full-pipeline baseline against official pinned PyTorch MPS using
equivalent sampling and dtype.

Exit: retrieval parity plus demonstrated end-to-end benefit on short clips and
long sparse/windowed inputs; no unbounded memory growth under concurrent load,
cancellation, decode failures, and repeated retries. Set hardware-specific
latency/throughput targets from the baseline before promoting defaults.
The default Metal route must read back only the final vector, with transfer
telemetry proving that decoded pixels and projected vision tokens remain on-device.

### 4. NVIDIA and portable codec coverage — MJPEG CPU/Metal lanes delivered

Delivered independently of PR #1014: complete baseline 8-bit MJPEG samples in
static MP4/MOV `jpeg` tracks, pure Zig selected-picture RGBA decode using
`lib/image.jpeg`, and CPU timestamp-window patch preparation with overlap reuse.
Actual live decode allocations, compressed bytes, pixels and final output bytes
are bounded; cancellation flows through JPEG/resize kernels and source reads.
An original two-second 4:4:4 fixture matches independent FFmpeg RGBA within three
byte levels, with exact packet clocks. Sparse selection reads only its payload;
ten window references prepare eight unique frames. Malformed/unsupported shapes,
allocation failures, source teardown and cancellation/retry are qualified.
Portable decode/preparation executes on WASI and compiles for Linux. See the
[portable lane contract](../../zig/lib/video/VIDEO.md#implemented-portable-mjpeg-lane).

Also delivered independently: owned packed-RGBA staging on the caller's Metal
device, shared quantized resize/patch kernels, and bounded software-decode/Metal
window jobs. CPU/Metal values match within `2e-6` across all rotations and both
centering modes. Input staging/total work/output caps, depths one/two/eight,
producer and reader teardown, cancellation/retry and deterministic allocation
failure are qualified. Separate-window versus shared-window jobs decode/prepare
10 versus 8 pictures and stage 122,880 versus 98,304 RGBA bytes. Counters report
logical staging, not physical bus transfers or demonstrated embedding latency.
See the [software-to-Metal contract](../../zig/lib/video/VIDEO.md#implemented-software-decode-to-metal).

The independent follow-up delivers resident Metal resize coefficients,
synchronized decode/preparation benchmarks, atomic shared reservations, version-pinned
remote range transport adapters, bounded read coalescing, fragmented MP4 indexing,
WebM VP8/VP9/AV1 indexing and the first pure Zig H.264 subset. The software decoder
qualifies progressive 8-bit 4:2:0 Baseline IDR pictures, CAVLC Intra16x16/I_PCM and
explicitly disabled deblocking, with bit-exact independent NV12 oracles. It executes
on Linux and WASI and supplies shared-window host patches. See the detailed
[video contracts](../../zig/lib/video/VIDEO.md#implemented-independent-efficiency-and-portable-h264-work)
and [media contracts](../../zig/lib/media/MEDIA.md#implemented-independent-container-and-source-extensions).

Remaining: NVDEC/device preparation with separately qualified CUDA model execution;
broader codec qualification and unsupported transition/tool combinations; streaming protocol integration,
WebM video decoders, HEVC, VP9 and AV1. Each route requires separate qualification.

Exit: capability-specific decoder, processor, and model qualification for each
advertised route; CPU-only builds retain no platform framework dependency.

## Validation and performance evidence

Pin source videos, reference decoder version, processor/model revision, precision,
hardware, and runtime build. Compare intermediate selected frame IDs/PTS,
decoded color planes/RGB, resized pixels, patch layout/positions/masks, vision
tokens, and final embeddings. Include text-to-video retrieval rankings and moment
localization quality; cosine proximity alone is insufficient application evidence.

The corpus includes two-second clips, clips beyond the 32-frame cap, long sparse
videos, overlapping windows, B-frames/open GOPs, VFR, short/empty intervals,
rotation, odd dimensions, color metadata, optional audio gaps, malformed inputs,
capacity denial, mid-decode cancellation, device failure, and retry recovery.
Exact same prepared frames isolate model performance; raw-file benchmarks then
include source I/O, parsing, sampling, decoding, preparation, inference, and vector
readback. State both timing boundaries rather than comparing them as one metric.

Record p50/p95 latency, clips/second and selected frames/second, bytes read,
dependency pictures decoded, peak host/device memory, decoder/queue high-water
marks, transfer bytes by direction, vision/backbone stage timings, and cache
hits. Use alternating fresh-process comparisons and warm steady-state runs;
report cold initialization and page/swap activity separately. GPU timings include
completion synchronization. Copy-free claims require measured transfer evidence.
Store dated benchmark receipts separately from these living contracts.

## Open decisions

- Final content-part fields and capability advertisement after PR #1014 settles.
- Broader production-stream qualification beyond the implemented static H.264
  tool subset documented in `lib/video/VIDEO.md`. Multi-slice, CAVLC/CABAC I/P/B,
  scaling matrices, native depth/chroma, MBAFF, fragmented standalone PAFF fields
  and field MMCO 1–6 are implemented. Bounded buffering across unrelated coded
  pictures is a separately qualified assembly extension with frozen prediction
  snapshots. Non-complementary field pairs and dynamic parameter sets remain
  outside the declared subset. CAVLC
  table provenance and BSD license are pinned.
- Reference decoder RGB conversion policy, VFR sampling compatibility, and
  accepted cross-backend numerical/retrieval tolerances.
- Admission estimates for opaque hardware decoder allocations, minimum surface
  pools, and re-admission on configuration changes.
- Default long-video window size/overlap and configurable vision-token budgets,
  based on retrieval evaluation rather than an assumed two-second limit.
- Hardware-specific promotion targets and fallback policy when efficient device
  decode is unavailable.

When the cross-cutting rollout completes, move lasting serving/inference
contracts into the appropriate inference design and guide documents, preserve
dated evidence under history, and remove this plan per the documentation rules.

### Independent codec/container expansion — delivered 2026-10-08

The portable H.264 route now adds bounded IDR-delimited SPS/geometry epochs,
non-IDR PPS changes, frame-number gap inference, monochrome, independent colour
planes, Extended-profile data partitions and 8-bit 4:2:0 SP/SI. FFmpeg qualifies
native samples and media clocks on original Sintel content and profile variants;
official JM decoder receipts qualify tools FFmpeg cannot decode reliably. Differential
checks, deterministic mutations, pure Zig coverage-guided targets, allocation failures
and cancellation supplement generated vectors. This remains an explicitly bounded
tool qualification; see [the expansion receipt](../../zig/lib/video/testdata/h264-expansion-validation.md).

Metal imports native integer textures at 8–14 bits and 4:2:0/4:2:2/4:4:4 and can
stage native host planes without a CPU RGB conversion. The hardware VideoToolbox
and CoreVideo contract remains 8-bit 4:2:0. Portable SIMD is enabled for measured
LLVM row packing; scalar transforms remain after a slower vector experiment.
Full-trailer completed CPU/GPU measurements accompany kernel receipts.

Shared media now supports validated WebM Cues, unknown-size Cluster boundaries,
bounded sequential spooling and immutable caller-framed live fMP4 segments. This
adds neither VP8/VP9/AV1 decoding nor an automatic network streaming protocol.
Remaining work includes non-IDR SPS transitions, cross-packet data partition assembly,
broader mixed-tool corpus qualification, hardware P010 import, VP-family decoding,
CUDA/NVDEC, and model/API integration after the independent model dependency merges.
