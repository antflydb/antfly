# Native video inference and EmbeddingGemma 2

Status: native video/media foundations, portable codec expansion and scheduling
are delivered. Latest `origin/main` at `755cb12a68`, including PR #1014 at
`d409767dc7`, is integrated, 2026-10-09.
Ordered EmbeddingGemma 2 video groups, native/Metal projector execution, canonical
media/attachment API inputs and direct completed Metal patch-buffer handoff are
implemented. Six pretrained F32 video cases qualify CPU/Metal numerical parity.
Fully resident vision/token/backbone optimization and remaining CUDA/NVDEC routes
remain open; see the dated integration
contract in VIDEO.md. The original design started at `cdf572a7467d581f6f1b39bcf514878488555f11`.

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

Tim's [PR #1014](https://github.com/antflydb/antfly/pull/1014) merged native
CPU/Metal EmbeddingGemma 2 text, image, audio, and ordered-group embeddings.
This branch adds video through its final model identity, resident weights,
admission, ordered-content, and pooling interfaces.

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

### 2. Apple decode and EmbeddingGemma video — integration and pretrained numerical qualification delivered

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
parity require later qualification. The final model/backend code from PR #1014
is merged from origin/main.

Delivered model integration: explicit video token 258884, pinned 140-token geometry,
1 FPS / uniform 32-frame sampling in presentation order, one ordered joint sequence,
native CPU and Metal vision/text execution, canonical MP4/MOV media and attachment
groups, family-scoped HTTP handling and processor identity validation. Eligible
static H.264 uses VideoToolbox→Metal; portable codecs use host→CPU/Metal preparation.
A completed retained MTLBuffer enters inference patch projection without prepared
pixel readback. Existing model projector/token host boundaries remain. Unit/lifecycle
qualification is automated; six official-checkpoint F32 cases and HTTP/group
comparisons now qualify both backends under the declared RGB decoder policy.
See the [pretrained receipt](../../zig/lib/video/testdata/video-pretrained-validation.md).

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

Reference preservation now compares six real H.264/MJPEG documents and four
unlabelled queries against pinned PyTorch CPU/MPS, native CPU and Metal, with
actual Linux ARM64 execution. All recorded rankings and top-k sets match. This
qualifies preserving the reference, not application usefulness. The 52-second
clip also qualifies all 1253 software-decoded integer planes and upstream
32-frame reference sampling. See `lib/video/testdata/video-retrieval-validation.md`.

Still pending: implement batched resident vision/projector execution,
direct device-token backbone inputs and final device normalization. Vision spatial
pooling and active-token sequence mean pooling already execute on Metal. Qualify
resident model execution on the delivered software-decode → Metal and
VideoToolbox → Metal preparation routes. Extend the delivered decode/preparation
queue to overlap resident vision execution, with atomic admission across
concurrent source/surface/model reservations and any persistent token cache.
For device sequence assembly, give the frame capture owned projected tensors
indexed by presentation slot, sharing repeated selections. Concatenate scaled
text embeddings and projected media tensors, then gather the final ordered rows
using bounded integer indices. This avoids downloading text embeddings or video
soft tokens to overlay them. Keep CPU and mixed image/audio fallbacks explicit,
and retain tensors through submission completion and cancellation. Qualify the
sequence before adding geometry-compatible vision batching; batch boundaries
must preserve independent spatial attention and frame positions.

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

- Logical-picture indexing for model sampling of field/partition transport,
  plus broader application-level pretrained video qualification beyond six cases.
- Broader production-stream qualification beyond the implemented static H.264
  tool subset documented in `lib/video/VIDEO.md`. Multi-slice, CAVLC/CABAC I/P/B,
  scaling matrices, native depth/chroma, MBAFF, fragmented standalone PAFF fields
  and field MMCO 1–6 are implemented. Bounded buffering across unrelated coded
  pictures is a separately qualified assembly extension with frozen prediction
  snapshots. Non-complementary field pairs and incompatible predicted-picture
  geometry transitions remain outside the declared subset. CAVLC
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

The portable H.264 route now adds bounded SPS/geometry epochs with qualified compatible non-IDR transitions,
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
bounded sequential spooling and immutable automatically framed live fMP4 segments. This
adds neither VP8/VP9/AV1 decoding nor an automatic network streaming protocol.
Non-IDR compatible SPS transitions, fresh intra geometry epochs, cross-packet A/B/C
partition gathering and automatic moof/mdat media framing are delivered. Remaining
work includes broader mixed-tool corpus qualification, hardware P010 import,
VP-family decoding, CUDA/NVDEC and fully resident model execution/parity.


### Reference-preservation follow-up (2026-10-09)

The reference harness checks complete vectors, cosine scores, non-tied rank
inversions, exact rankings and top-k overlap. It pins corpus bytes, original
checkpoint receipts, upstream source and binary identities; failure reports retain
measurements. Native campaigns exercise bounded concurrency and peer-disconnect
retry. A successful retry does not prove cancellation completion. Completed
PyTorch MPS timings synchronize final readback; HTTP native timings include
transport, decoding and preparation. Owned-server memory samples include supervised
worker descendants and explicitly exclude unmapped GPU-private allocations.
Historical parent-only RSS results are labelled and cannot establish pipeline peak.

The real-clip gate found a preparation arithmetic defect, rather than a decoder or
model weight defect: separate Q13 component rounding is now explicit CPU/Metal
policy in the video model recipe. Its recipe identity is bumped so previously
indexed vectors cannot silently mix with newly generated ones.

Live fMP4 accepts validated transactional initialization changes through
`Ingest.reconfigure`. Configuration epochs retain old snapshots/leases and require
a decoder/timeline reset; lifetime work and reconfiguration counts remain bounded.
HTTP transport, HLS playlists and DASH manifests remain implementation work.

VP8/VP9/AV1 runtime decoding must remain pure Zig, per the user's clarified
constraint. Optional libvpx/dav1d adapters are outside this plan. Each codec needs
real conformance vectors, independent differential decode, mutation/fuzz campaigns
and bounded request/resource behavior before inference exposure.


The native HTTP fMP4 HLS library now implements authenticated same-origin fetches,
exact byte ranges, bounded explicit reloads, immutable overlapping playlist
entries, initialization/discontinuity epochs and retry ownership. Production
`lib/httpx` localhost tests qualify this path; master selection, encryption,
low-latency HLS and DASH remain open. Pure Zig VP8 displayed-keyframe decoding
reuses `lib/image` and returns native YUV planes with byte-exact independent
fixtures, shared admission and allocation-failure coverage. VP8 interframes,
VP9/AV1 and WebM model integration remain implementation work.


The subsequent resident Metal Q/K/V normalization, vision RoPE and spatial
pooling changes pass pinned pretrained cases and real-clip retrieval. Sparse CPU
and Metal both preserve pinned long-clip vectors/scores. Native-depth monochrome
Metal surfaces and actual CoreVideo P010 import/staging now have CPU parity and
producer-lifetime tests. Surface coverage does not imply hardware High10 decoding
or HDR qualification. Batched vision, device composition/backbone inputs and
final normalization remain open, as do whole-pipeline overlap and broader codecs.


A manual Linux qualification workflow now runs local PyTorch and native routes
on the same machine, verifies pinned checkpoint/source receipts and uploads
reports, server logs and tool versions. It has been linted but not dispatched;
existing actual Linux receipts remain distinct from this CI configuration.


Video patch projection and learned-position addition now remain on Metal too.
The position table uses a local flattened row view that preserves model ownership;
selected packed BF16/F16 rows use native kernels. Device/model-owned F32 and packed
BF16 fixtures pass with unchanged weight shapes and no intermediate host-mirror
downloads. The rebuilt production route passes all six pretrained cases and the
real-clip score/ranking/top-k gates. Frame/projector batching and final normalization still need implementation;
projected-token composition is delivered by the continuation below.


### Resident composition and scheduling continuation

Implemented owned video projection tensors and Metal composition of the ordered
text/media sequence, with repeated sampled-frame reuse and direct joint-backbone
inputs. A device mixed-media regression verifies host-reference values without
intermediate tensor downloads. A bounded worker overlaps selected-frame decoding
and preparation with projection; two queue slots, one executing projection and one
producer-held submission have explicit teardown and an additional admitted
384 MiB preparation ceiling. CPU/raster-batch host contracts remain supported.

Portable APIs now expose duration-based logical-picture sampling with explicit
gap/repeat policies and orthogonal display/SAR geometry. These do not broaden the
pinned model processor policy or establish field/partition indexing automatically.
Native authenticated DASH ingestion now supports static single-representation
fMP4 `SegmentList` MPDs, with ranges and clocks, through the shared HTTP/live
fragment state machine. Templates/timelines, dynamic availability, representation
selection and other DASH extensions remain work.

Next required implementation: frame-independent batched vision/projector
execution, device final normalization, qualified model display/VFR/logical-picture
policies, broader DASH addressing/live timing, VP8 interframes and pure Zig VP9/AV1
reconstruction. No VP-family interframe decoder is advertised by the new transport
or sampling APIs. New end-to-end receipts must bind the rebuilt binary and full
vectors; existing receipts continue to qualify their own earlier binaries.


### Frame batching, model policy qualification and live DASH continuation

Implemented two-frame vision/projector batching on native CPU and Metal. Frames
share linear operations, but SDPA, position coordinates and pooling preserve
independent frame axes. Mixed geometry and final singletons flush safely;
repeated sampling reuses owner projections. Metal retains decode/preparation
worker overlap and device result slicing. Its conservative request workspace
reservation grows from 1152 to 1920 MiB to account for the second executing frame.
CPU uses one bounded pending patch frame without a second execution thread.

The model qualification campaign verifies VFR average-FPS index parity against
unchanged pinned compressed pixels and the existing independent pretrained
vector, and verifies rejection of rotation, non-square pixels and PQ HDR. It does
not broaden the model rendering recipe or qualify timestamp sampling/fields.
Actual-GPU positions/RoPE/pooling tests compare batches with single-frame results
and require no intermediate downloads; production gates qualify final vectors.

DASH adds representation-level SegmentTemplate, SegmentTimeline, bounded repeat
expansion, Number/Time/RepresentationID/Bandwidth substitution and numeric widths.
Dynamic snapshots use a caller-synchronized clock, availability start, period
start, presentation offset and time-shift window. Only fully available segments
are exposed; expiration skips history arithmetically. Shared authenticated HTTP
preserves retry/epoch/gap behavior, checks immutable timing identity and exposes
minimumUpdatePeriod to the caller. BaseURL, inherited addressing, selection among
representations/periods, DRM and low-latency availability remain unsupported.

VP8 interframes, VP9 and AV1 reconstruction remain unimplemented. They still need
pure Zig reference-picture state, entropy/block reconstruction, filtering and
independent libvpx/dav1d pixel fixtures; no empty decoder API is substituted for
that work. Device final normalization and broader qualified model display/field
policies also remain outstanding.

### Stateful VP8 and positive presentation/HDR qualification

The next implementation adds pure Zig VP8 interframes through a sequential WebM
session API. Transactional reference/entropy updates, hidden frames, motion modes,
split prediction, residual/filter reconstruction, budgets, cancellation and retry
are implemented. Independent generated versions 0–3 and a real Sintel excerpt
qualify 174 displayed plus three hidden coded pictures. Reference sharing is a
remaining performance improvement. VP9, AV1 and model WebM ingestion remain
unimplemented; container support alone is not presented as decoder support.

The model presentation policy now renders orthogonal rotation and uses SAR for
its target aspect, without an intermediate stretched raster. PQ/HLG YUV has a
pinned fixed-exposure Reinhard/sRGB policy, with explicit gamut declarations and
conflict checks. The recipe changes to `embeddinggemma2-f32-mean-v3-display-hdr`.
CPU uses high-precision transfer before quantization; Metal caches transfer tables
inside the existing admitted coefficient budget and keeps rendering/resizing on
GPU. Independent float64 pixel fixtures and positive pretrained CPU/MPS/native/
Metal comparison campaigns qualify the declared cases. Mastering/dynamic HDR,
hardware HDR imports and broader real-world HDR qualification remain separate.

The manual Linux workflow now generates its own pinned PyTorch rendering oracle
and compares the native model against it. Cross-compilation is not Linux runtime
evidence, and prior runtime receipts remain tied to their earlier source/binary.

## Maintained VP9/AV1 decoder implementation

The codec implementation gates above now have native reconstruction, rather
than packet parsing alone. `video.codecs.Session` owns maintained Zig source
ports of dav1d 1.5.1 and libvpx 1.15.2. The checked-in Zig builds without codec C,
runtime libraries or downloads. Antfly's allocator/control/admission services
replace libc allocation, worker management and VP9 non-local error exits.
Upstream BSD notices, authorship and VP9 PATENTS remain with the owned source.

Initial qualified fixtures cover VP9 profiles 0–3, 8/10/12-bit 4:2:0/4:2:2/4:4:4,
interframes, odd geometry and multiple visible superframe pictures. AV1 is
qualified for Main profile, 8/10-bit 4:2:0 low-overhead OBUs, reference updates,
film grain and odd geometry. Every displayed picture matches an independently
executed reference decoder: 144 pictures across 12 pinned synthetic streams.
Regeneration reproduces the coded bytes and reference picture hashes.

WebM packet leases, timestamps and AV1 configuration OBUs connect to the new
session API. Decoded native-depth/chroma surfaces use existing CPU/Metal
preparation; actual-device parity includes odd dimensions and 12-bit/4:2:2/4:4:4
VP9 output. The semiplanar boundary uses portable Zig vectors with scalar tails.
This does not add model WebM ingestion or hardware VP9/AV1 decode.

Memory, pixel, packet and output bounds share existing admission. Sessions have
stable ownership and reclaim every allocation after partial initialization or
failed decoding. Independent sessions run concurrently with atomic once setup;
internal codec workers remain disabled. Preflight failures are retryable, but
post-entry errors invalidate the decoder and require a new session/replay from
a synchronization point. Tests cover allocation failure, cancellation, bounded
mutations, truncation, concurrency and output lifetime.

Local validation: 136/138 ReleaseSafe video tests pass on macOS with actual Metal
(two existing platform skips); Linux x86_64 and ARM64 musl compile/link without
libc or codec libraries. Actual Linux runtime qualification remains a separate
workflow receipt. Broader real-world codec conformance, coverage-guided fuzzing,
AV1 additional profiles/layer policies and measured throughput remain explicit
qualification work. See `src/codecs/PORTING.md` for the maintained-source contract
and `testdata/vpx-av1/README.md` for reproducible reference fixtures.


### Remaining non-CUDA execution scope (2026-10-11)

The next implementation connects native WebM VP8/VP9/AV1 reconstruction to model
sampling and CPU/Metal projection. Logical decoder-output indexing, bounded
replay and HTTP MIME support are implemented. Pretrained CPU/Metal and actual
Linux ARM64 parity pass across eight vectors and three exact retrieval rankings;
current receipts are in `zig/lib/video/testdata/webm-validation`. Full completion additionally requires all of:

- Broader real-world differential streams, sustained coverage-guided fuzzing and
  corruption recovery.
- Broader pretrained model qualification of expanded AV1 depth/chroma/layer
  policies. The native format, Annex B and operating-point implementations have
  independent AOM fixtures and actual Linux ARM64 execution receipts.
- Realistic codec benchmarks, measured portable SIMD and reduced retained
  reference copying.
- Actual Linux execution of current codec and pretrained model paths.
- Broader WebM display/Colour qualification and decoded/projected frame reuse
  across model window groups. Logical AVC indexing and explicit model timestamp
  windows are implemented; their current qualification is recorded below.
- Measured cross-request scheduling/reservation tuning and cross-group frame
  reuse. Coordinated preprocessing/model budgets and bounded preparation before
  the model gate are implemented below, as is final Metal normalization.
- Broader live/HLS coverage. DASH inherited templates/BaseURL, representation
  selection, automatic multi-period playback and stable live period cursors
  are implemented and qualified in the continuations below.

CUDA remains outside this execution scope. Passing one implementation gate does
not close the other items or establish general codec conformance.


### Logical model timeline continuation (2026-10-11)

Completed AVC assemblies now supply logical sampling metadata for fields,
partitions, separate planes and changing SPS/PPS epochs. The model adapter offers
an explicit rational timestamp policy with bounded uniform/truncated selection,
exclusive window endpoints and separate embeddings through separate input groups.
Reference average-FPS/index behavior remains the default; the embedding recipe
identity advances to v4. The portable route serves field/partition inputs on CPU
and Metal. The independent qualification tool selects from FFprobe picture PTS
and feeds selected pixels to pinned PyTorch without resampling. The campaign
covers VFR, PAFF, VP9/AV1 timestamps and separate intervals of a 52-second clip;
see `tools/embeddinggemma2/testdata/sampling`. This does not close device final
normalization, whole-pipeline scheduling, cross-group reuse, streaming inheritance
or the broader codec/performance qualification items above.


The logical/timestamp sampling continuation now also passes pretrained execution
on actual Linux ARM64: ten vectors, maximum coordinate error 5.30e-7, exact
rankings/top-k and eight successful concurrent-load requests. The static musl
server is bound by its binary SHA in the sampling receipt. This closes the Linux
ARM64 execution gate for these policies, not x86_64 execution or general codec
conformance.


### Device final normalization implementation (2026-10-11)

Metal now has a masked mean/truncation/normalization primitive retaining private
storage through final response readback. The multimodal, text and raster routes
use it; requested dimensions reach the flat text/media pipeline before readback.
Peak-scaled reduction, resource barriers and invalid-output validation cover the
numerical and ownership boundaries. Actual device unit tests pass, including
long masked sequences, extreme finite scales, retained views and planned scopes.
The updated production server builds with task-local Zig caches after a
shared-cache object failure. Pretrained CPU/Metal vectors and rankings, twenty
Metal dimension cases and the original mixed/video retry campaign now pass;
receipts are in `tools/embeddinggemma2/testdata/resident-normalization`.
This implementation does not close cross-request scheduling, window reuse,
streaming inheritance or the broader performance/codec qualification scope.


### Inherited DASH addressing continuation (2026-10-11)

A bounded native XML tree now preserves MPD hierarchy, resolves namespaces and
decodes predefined/numeric entities without fetching external resources. DASH
selects video representations by IDs or a bandwidth ceiling, resolves inherited
BaseURL chains and merges SegmentTemplate/SegmentList attributes and child
addressing from Period through AdaptationSet to Representation. Returned URI
references are owned; decoded signed queries remain valid after the MPD and
temporary parser storage are released. The existing timing expansion engine
still computes static/live segment availability. Authenticated HTTP supplies
the actual manifest URL and preserves origin pinning, immutable reload
descriptors, cancellation, admission and retry-safe ingestion. Selected
representation/configuration changes fail during reload.

Automatic multi-period playback, inferred preceding-period timing and stable
live period cursors remain implementation work. That work must preserve period
identity as earlier Periods disappear and must handle segment-number resets
without silently skipping unconsumed media. Broader HLS and the other outstanding
non-CUDA requirements above remain active.

The final macOS gate passes 67 portable media tests, four production authenticated
HTTP tests and five object-store/HTTP tests. Actual Linux ARM64 execution passes
the same 67 portable tests. Signed-query lifetime, BaseURL alternatives, bandwidth
selection, inherited timeline overrides, unsupported critical features and
exhaustive allocation-failure cleanup have direct tests. Production HTTP verifies
inherited SegmentList selection/BaseURL with authentication; the fake-transport
retry/origin test verifies no cross-origin fetch and preserves the next sequence.
Source hashes and logs are in
`zig/lib/media/testdata/dash-inheritance/report.json`. These gates do not close
the remaining multi-period/live/HLS or broader non-CUDA requirements.


### Multi-period DASH and stable live cursors (2026-10-11)

Selected Periods now form one bounded presentation while retaining period-local
segment numbers. Start/duration inference uses declared neighboring timing and
MPD presentation duration, following the [DASH-IF timing model](https://dashif.org/Guidelines-TimingModel/).
Duplicate identities, ambiguous timing and overlapping Periods fail. Period
identity is separate from the presentation clock and selected configuration, so
older Period removal does not change the identity of retained content. Metadata
limits for Periods and timeline entries are independent of the global available
segment limit, including zero-segment early snapshots and future timelines.
Each Period releases its temporary arena immediately; a separate cumulative
planning-byte budget bounds repetition of inherited/root attributes.

A transactional cursor retains the last consumed timing and learned Period end
and successor identity. Numbering resets, announced gaps and future/incomplete
segments are handled without renumbering live snapshots. Missing pending media,
skipped announced Periods, rollback and configuration mutations fail explicitly.
Dynamically learned end times survive removal of the prior Period; published
clock formatting is normalized before identity checks. A Period transition
reconfigures the decoder even when initialization bytes/URI are unchanged.
Returned `dash_timing` supplies exact rational presentation milliseconds and
Period clipping metadata; packet timestamps retain their track clock.

Broader HLS, the remaining DASH extensions listed in MEDIA.md, cross-request
scheduling, performance/reference-copy improvements and the broader codec/model
qualification requirements remain active. This continuation does not close
those requirements or establish general DASH/CMAF conformance.

The final source-bound macOS gate passes 75 portable media tests, five production
authenticated HTTP tests and five object-store/HTTP tests. Actual Linux ARM64
execution passes the same 75 portable tests. The parser and live-reload campaigns
inject every allocation failure through period combination, transactional reload
and decoder epoch reconfiguration. Logs, source/fixture hashes and the Linux
binary hash are in `zig/lib/media/testdata/dash-periods/report.json`.


### Coordinated pipeline admission continuation (2026-10-11)

Video sources, WebM decoder sessions and Metal preparations now use a media
sub-ledger within the already-acquired process-wide model execution permit.
MJPEG has an owned frame reservation; Metal command/output ownership releases
capacity after fencing rather than at submission. The preparation estimate
accounts for two consumer jobs plus two queued jobs and a producer, staged
source planes, persistent coefficients and native decoder surfaces. The
conservative parent video workspace bound becomes 3344 MiB; the host
preprocessing allocation ceiling stays 512 MiB. Shared-admission failures map
to the pipeline's memory-budget error contract.

The focused model gate passes 17 tests; the full macOS video suite passes
156 with two platform skips. The production server passes all six original
CPU/Metal pretrained video/mixed cases, 128 dimensions, forced admission retry
and damaged-selected-frame cleanup/retry. Its eight WebM vectors preserve all
three reference rankings and top-k sets on both backends; concurrent/sustained
and disconnect/retry checks pass. The continuations below implement shared
preflight and preparation before the model gate. Measured/tuned reservations
and the full non-CUDA scope remain active. These changes must not be described as completed whole-pipeline
scheduling until those remaining paths are implemented and qualified.

Actual Linux ARM64 executes the updated portable video suite: 134 passes and
24 platform skips. Source/binary/artifact hashes and the macOS CPU/Metal
pretrained reports are in
`tools/embeddinggemma2/testdata/pipeline-admission/qualification.json`. The current
Linux pretrained binary still needs its own execution receipt after these
admission changes; the older Linux pretrained results retain their prior scope.


### Shared preflight and geometry-derived admission (2026-10-11)

Grouped HTTP/direct video validation now reserves bounded host inspection work
against the ModelManager's shared physical domain before loading a model. The
inspector uses a reclaiming bounded allocator and media sub-ledger, releases
metadata/decoder ownership before returning, and preserves backing OOM versus
budget-denial attribution. Transient preflight admission denial aborts the
request rather than becoming a per-item partial response.

The model pipeline separately admits host-only planning before acquiring model
execution capacity. Preparation reservations derive from selected geometry,
unique pictures (up to five simultaneous owners), copied source-plane bounds
and native decoder surface estimates. Sequential video parts take a maximum
rather than summing transient peaks. The model's 1536 MiB Metal execution bound
and 16 MiB coefficient ceiling remain separate. CPU inspection does not require
Apple surface capacity. Unsupported hardware configurations can fall back to
software; allocation failure during that probe propagates.

Nineteen model tests pass, including exhaustive allocation-failure cleanup.
The substantive shared-capacity/denial/retry test and its 33 import gates pass.
The production CPU/Metal server passes six original cases, reduced dimensions
and admission/decode retries; eight WebM vectors preserve all reference
rankings/top-k and concurrent/sustained/disconnect retry checks. Evidence is
in `tools/embeddinggemma2/testdata/preflight-admission/qualification.json`.
Preparation before the model gate is implemented in the following continuation;
broader non-CUDA qualification/performance/streaming work remains active.


### Preparation ahead of model execution (2026-10-11)

The first video part in a grouped request now starts a request-owned producer
before acquiring the model gate or Metal provider execution lease. Shared
execution admission and geometry-derived budgets precede producer startup.
The producer owns decode/preparation and a two-slot queue; only the consumer
projects frames and operates the model backend. CPU uses owned patch jobs;
Metal uses prepared buffers and device imports. Later video parts execute
sequentially, preserving the maximum-transient-budget contract. Library calls
without a runtime keep their existing synchronous path.

Process protection spans producer startup through joined/fenced cleanup. Queue
closure wakes a blocked producer; abandonment, allocation failure and cancelled
consumer entry reclaim metadata, patches and GPU reservations. A completed
producer retains its preparer until queued imports have been consumed or drained.
CoreVideo imports also retain their originating cache independently of its
wrapper. Cold Metal preparation obtains the same process device through atomic
once initialization without creating or leasing a model provider.

The focused model gate passes 24 tests, including preparation before model access,
preparation while a compute provider lease is held, full-queue abandonment,
allocation-failure cleanup and cancelled consumer entry. A separate actual-Metal
regression verifies imports/preparations survive their creating cache/preparer.
The full macOS video gate passes 157 tests with two platform skips. Current
production Metal and actually executed Linux ARM64 native binaries each pass
six original cases/retries, eight WebM vectors and ten timestamp/window vectors.
All reference rankings/top-k match; concurrent sustained and disconnect/retry
checks pass. Source/binary/report hashes are recorded in
`tools/embeddinggemma2/testdata/pipeline-overlap/qualification.json`. These are
correctness/lifetime checks under shared-host load, not speedup measurements.

Measured overlap tuning, cross-group frame reuse, broader codec/display/live
qualification and the other non-CUDA requirements remain active. Backbone
execution still serializes by model; this continuation overlaps preparation
with another request's execution rather than concurrently mutating model state.


### Larger codec benchmarks and VP8 reference sharing (2026-10-11)

A dedicated WebM benchmark now measures maintained native VP8/VP9/AV1 decode and
CPU/Metal preparation on full-length 480p real clips and 1080p excerpts. Completed
GPU fencing/time, first/warm setup, copy/upload counts and allocation/RSS/device
observations are separate metrics. Timed processes run sequentially without owned
encoders/builds overlapping; background host activity remains uncontrolled.

VP8 immutable reference slots share owners, copy a refreshed picture once, and
commit reference/entropy/segmentation state transactionally. Returned output stays
independent. Full corpus differential testing found an existing true-motion edge
sentinel defect, now corrected and retained as a small portable regression. All
4623 pictures across six streams match independent reconstruction. A source-pinned
copy baseline includes the same fix and matches every VP8 picture. Matched
reference-copy volume falls by two-thirds and decoder allocation peak by 28.6%;
six-sample warm medians improve about 3.4% at 480p and 4.2% at 1080p on this host.
These observations do not establish production capacity or other architectures'
performance. The full macOS suite passes 160 tests with two platform skips;
actual Linux ARM64 passes 137 with 25 platform skips. All 4623 independent
picture hashes agree on both platforms, and current CPU/Metal pretrained WebM
parity preserves all eight vectors and three rankings/top-k sets, with concurrent
and disconnect/retry checks. The focused model gate passes 24 tests. Source,
binary, input, oracle and measurement fingerprints are recorded in
`zig/lib/video/testdata/benchmarks/webm/qualification.json`.

Measured SIMD work, H.264 reference copying, WebM indexing/replay costs and
cross-group reuse remain performance work. The full codec/display/streaming and
non-CUDA qualification requirements stay active.


### Header-only WebM planning continuation

Header-only displayed-picture indexing is implemented for VP8/VP9/AV1, with an explicit full-reconstruction oracle and index comparison benchmark. This removes reconstruction from the default model preflight/planning indexes while retaining reconstruction for selected replay. Focused tests compare metadata against decoded output across VP9 profiles and AV1 depth/chroma/grain/scalable policies. The full macOS gate passes 168 tests with two platform skips; actual Linux ARM64 passes 145 with 25 skips. All 4,623 unique larger-corpus picture records and sampled IDs match full reconstruction on both platforms. The metadata campaign passes 101,937 runs with correctly Smith-serialized seeds; separate acceptance gates prove 13 decoder and five metadata seeds reach valid output. Metal preserves all eight pretrained vectors and three exact rankings/top-k sets, but four of five concurrent load requests receive retryable admission denials under shared-host pressure. Current CPU pretrained parity also preserves all eight vectors and three exact rankings/top-k sets, with five successful concurrent load requests. Metal rejects three zeroed-first-packet cases and preserves clean-retry vectors; broader corruption recovery remains active. The header-only checkpoint also passes actual Linux ARM64 pretrained parity: eight vectors, three exact rankings/top-k sets, disconnect retry and five concurrent requests. `zig/lib/video/testdata/header-index-validation` records the gates and diagnostic timing limits; the broader non-CUDA scope above remains active.


### Codec-verified selective WebM replay

Logical indexes retain bounded restart records separately from displayed-picture
metadata. VP8/VP9 points require parsed key pictures at the start of a packet;
AV1 additionally records the latest sequence OBU span. Replay initializes codec
private data, reads only the required prior sequence span, and starts at the
verified key packet with the original logical output ordinal. Container sync
hints do not authorize a restart. Reader identity and AV1 operating-point/layer
policy must match the index. Selected replay retains entropy/pixel validation.

Model producers and synchronous encoding use indexed replay. Restart metadata
is included in shared admission and releases with the index. A synthetic
regression compares all three codecs against packet-zero replay, including AV1
without codec-private data or repeated sequence headers, false sync hints, duplicate
samples, policy mismatch and allocation-failure cleanup. The `replay_compare`
benchmark hashes 32 selected outputs in the final quarter of each real clip and
checks their ordinals, timestamps, geometry/depth/chroma and bytes. All six corpus comparisons pass on macOS and actual Linux ARM64. Full gates
pass 174 macOS tests (two skips), 150 Linux tests (26 skips) and 24 model tests.
Nineteen expanded-format replay fixtures, bootstrap allocation failures and
101,920 metadata fuzz runs pass. Six new late-window vectors plus ten existing
sampling vectors preserve pinned PyTorch results, exact rankings/top-k and
load/retry checks on CPU, Metal and actual Linux ARM64. Source-bound receipts
and diagnostic timing limits are in `zig/lib/video/testdata/replay-validation`. Broader non-CUDA scope remains active.


### RGB24 row quantization and recipe v5

Late 854-pixel-wide windows exposed a preprocessing parity gap shared by CPU
and Metal, despite matching native sample bytes and replay ordinals. Independent
RGB24 extraction identified scalar luma-domain Q16 quantization for rows outside
the 16-pixel vector alignment. The preparation API now includes `q16_luma`,
implemented with bounded integer arithmetic on CPU and Metal. Model presentation
uses Q13 component rounding for aligned rows and Q16 for nonaligned rows; high
depth/HDR paths retain their separately declared conversion policies. This adds
no runtime conversion-library dependency. Independent 26-pixel-wide fixtures
cover BT.601/709/2020 and both ranges; actual Metal staging also matches CPU.

The embedding identity is `embeddinggemma2-f32-mean-v5-logical-timestamps-rgb24`,
so cached vectors cannot silently retain the older output policy. Pinned late
window references remain unchanged and now pass on CPU, Metal and Linux ARM64,
as do the ten existing sampling vectors and current resource/lifetime gates. Integer reference details are recorded by the fixture generator and
[FFmpeg's RGB24 reference arithmetic](https://github.com/FFmpeg/FFmpeg/blob/n9.0.2/libswscale/yuv2rgb.c).


### Portable integer RGB vectors

CPU integer RGB preparation now processes eight pixels with Zig portable
vectors for Q13/Q16 profiles, retaining scalar tails and fallback for rotation,
HDR, higher depth and floating rounding. Planes retain explicit padded strides
and monochrome/4:2:0/4:2:2/4:4:4 geometry. The scalar preparation entry point
remains the numerical/benchmark oracle. Exact patch-array comparisons cover
short rows, vector boundaries and tails, padded strides, all chroma layouts,
matrices and ranges; SIMD RGB bytes also match independent Q16 fixtures.
Matched ABBA `cpu`/`cpu_scalar` runs hash every prepared F32 byte equally.
Four warm samples per mode/geometry show preparation medians lower by 26.9%
at 480p and 29.0% at 1080p on this host; decoding dominates, so completed
decode/preparation improves about 1.0% and 1.6%. Background load remains
uncontrolled, and these observations do not establish production capacity.
Producer-abandonment checks also exposed a VP9 getter that could poll
cancellation after a caller guard and return null before dereference. Both
frame-view helpers are now pure, with a discovered boundary regression that
fails on the previous implementation. Final source-bound receipts are in
`zig/lib/video/testdata/simd-validation`; broader non-CUDA work remains active.

### WebM presentation and Colour through inference

Track metadata now retains crop/display dimensions, projection pose and Colour.
The model applies crop in coded coordinates before quarter turns and resizing,
without a cropped staging allocation on Metal. Implicit display aspect follows
Track PixelWidth/Height even when a later codec picture changes resolution.
Explicit display dimensions/physical units provide an aspect ratio rather than
an inferred screen size. Primaries/transfer/matrix declarations are reconciled;
depth, chroma and range mismatches fail. VP8 container declarations supply its
RGB conversion matrix and range, while VP9/AV1 explicit codec signals must agree.

The embedding recipe is now `embeddinggemma2-f32-mean-v6-webm-presentation`;
previously ignored metadata can change input patches and embeddings, so v5 and
v6 identities must stay distinct. The independent oracle uses unmodified codec
payloads, explicit RGB conversion, crop/roll processing, pinned HF target geometry
and pinned pretrained PyTorch. This extends the reference processor explicitly;
it does not assume upstream applies WebM display metadata automatically.
Synthetic metadata-only fixtures retain identical compressed Cluster hashes.
Native/Metal preparation, RGBA staging and P010 imports have crop tests; defaults
across coded geometry changes have a separate portable regression. Unsupported
stereo/spherical projections, nonorthogonal pose, derived Range and explicit
chroma siting remain explicit rendering-policy failures. Broader real-world
HDR/display, scheduling, sustained performance, codec and live ingestion work
remain within the active non-CUDA objective.

Final presentation gates pass 77 container tests on macOS and Linux ARM64,
180 video tests on macOS (2 skipped), 155 on Linux ARM64 (27 skipped), and
25 model policy tests. All three pretrained cases preserve prompt-token counts
on native CPU, Metal and Linux ARM64. Maximum vector error is below 2.8e-7 on
macOS and 2.9e-6 on Linux ARM64; every cosine exceeds 0.9999999997. These are
synthetic SDR policy oracles, not universal real-world/HDR qualification.
Source/binary/report receipts are in
`zig/lib/video/testdata/presentation-validation`. Broader non-CUDA work remains
active.

### HLS master discovery continuation

Native HLS ingestion now accepts master playlists and preserves variant and
rendition declarations. Explicit constraints choose a stable video URL; DEFAULT
or named alternate-camera selection supplies its media playlist. Initial discovery
is transactional across both playlist reads, and subsequent reloads retain that
URL. Initialization/media references resolve from the selected playlist while
credential origin checks remain unchanged. Held packets survive reconfiguration,
and allocation/cancellation failures reclaim temporary metadata and admission.

The public Apple sample master is pinned for metadata-only parsing/selection;
its MPEG-TS references are not downloaded. Synthetic authenticated fMP4 HTTP
fixtures exercise alternate video selection, nested URI bases and reload/epoch
transitions. Dedicated portable and HTTP test installation steps permit actual
Linux execution, and serialized fuzz seeds have positive selection checks before
coverage-guided mutation. Encrypted, MPEG-TS, low-latency and separate-track
assembly remain implementation work; this continuation does not complete the
full non-CUDA scope.

Final HLS master gates pass 90 portable and six production HTTP tests on both
macOS and actual Linux ARM64. A fresh bounded parser/selection campaign passes
117,703 runs with four positively accepted Smith seeds, including the public
Apple master fixture. Source/binary/report receipts are in
`zig/lib/media/testdata/hls/master-validation`; these are protocol/ownership
results, not codec, model, performance or arbitrary HLS conformance claims.

### AES-128 HLS ingestion continuation

Identity-key AES-128 CBC/PKCS7 HLS resources now decrypt natively through Zig
before fMP4 ingestion. Maps freeze key/IV declarations; media can use explicit
or sequence-derived IVs and rotate or clear its key. One admitted key is reused
within a snapshot and refreshed after successful reload. Secret key/response
storage is wiped on release, with the full response allocation retained even
when its plaintext view is shorter. Cancellation/padding failures precede input
publication, and cursor/initialization retry semantics and held packets survive.

Independent OpenSSL fixtures reproduce byte for byte. The final phase passes
108 portable and seven production HTTP tests on macOS and actual Linux ARM64.
Fresh independent fuzz reports cover 109,230 declaration runs and 100,046
ciphertext runs; serialized seeds have positive plaintext/declaration gates.
The DASH failure campaign now forces the same deterministic arena-allocation
fallback already used by its other campaigns, avoiding heap-placement-dependent
allocation counts after the shared segment descriptor grew. Source/binary/report
receipts are in `zig/lib/media/testdata/hls/aes-validation`.

This phase does not change codec/model execution or the v6 embedding recipe.
SAMPLE-AES/CBCS, DRM processing, MPEG-TS, low-latency and separate-track ingestion,
broader real-world codec/model qualification, scheduling and performance remain
within the active non-CUDA goal.


### MPEG-TS AVC/HLS continuation

Native bounded TS demuxing now validates complete PAT/PMT tables, CRCs,
continuity, PES payloads and 33-bit clocks. The video adapter normalizes Annex B
AVC into the existing reader/decoder API, retaining native timestamps without
serializing MP4. No-map HLS uses a codec callback after authenticated resource
fetch/AES decryption. State/cursors commit only after successful ingestion;
discontinuities open epochs while held packet leases remain valid. Scratch,
configuration parsing and normalized storage participate in shared admission.

Independent FFprobe/FFmpeg closed-GOP and B-frame fixtures exercise every
picture and packet clock, with production localhost HTTP, retry/epoch,
allocation-failure and bounded fuzz checks. See
`zig/lib/media/testdata/transport/validation` for final evidence. This delivers
a portable library/HLS path rather than pretrained TS model parity. External
TS maps, arbitrary NAL/PES/access-unit splitting and timestamp inference,
SAR/VFR qualification, other TS codecs and raw/model input routing remain
open alongside the broader original non-CUDA scope.

Final TS-phase gates pass 115 media tests and seven existing HTTP tests on
macOS and actual Linux ARM64, plus the separate TS HTTP test on each. Video
gates pass 184 macOS tests (two skipped) and 159 Linux ARM64 tests (27 skipped).
The final bounded TS fuzz campaign adds 101,456 runs to the previous 102,382
(cumulative 203,838); positive serialized seeds decode both fixture transports.
All generated fixtures and the manifest regenerate byte for byte. The media
allocation-failure helper forces resize fallbacks after actual Linux exposed
heap-placement-dependent counts; production allocation behavior is unchanged.


### Raw MPEG-TS model input continuation

EmbeddingGemma 2 model input now accepts `video/mp2t`. Byte detection routes TS
through the owned native AVC normalizer during preflight, synchronous encoding
and queued preparation, before existing logical-picture sampling and model
execution. Normalized snapshots outlive packet/configuration readers and queued
patch production; teardown releases them after decoder owners. Temporary
normalization/configuration storage participates in the existing request pool.
Malformed TS clocks/tables/continuity and ambiguous picture timing return
HTTP 400. Preflight uses the existing `INVALID_REQUEST` response; decoder
execution errors use `INVALID_VIDEO`. Neither path publishes an embedding.

Closed-GOP and reordered B-frame fixtures retain the same selected pictures,
geometry/admission plans and exact prepared-patch hashes as their original MP4
payloads. The video processor recipe remains v6: existing container outputs and
presentation policies are unchanged. Pretrained qualification is recorded in
`zig/pkg/inference/src/testdata/video/transport-validation`, including pinned
PyTorch, native CPU, Metal and actual Linux ARM64 source/binary receipts. These
synthetic cases do not qualify TS SAR/HDR/VFR policies or arbitrary PES layouts.
HLS playlist orchestration into model groups remains separate from raw TS input.

Final raw-TS model gates pass 27 macOS tests and 25 actual Linux ARM64 tests
(two Metal skips). Both TS/MP4 pairs preserve pinned pretrained vectors on
native CPU, Metal and Linux; maximum error is below 3.6e-7, every cosine exceeds
0.999999999997, rankings/top-k are exact, and video token counts match (824/266).
TS/MP4 vectors are identical within each backend. Four concurrent-load requests
per backend return 200, with disconnect/retry checks recorded; these small
shared-host checks do not establish sustained production capacity. Corrupt PAT
requests return preflight HTTP 400 on all three backends without embeddings.


### AVC access units across TS PES boundaries

Native TS demuxing now preserves whether a PES actually carries timestamps.
Untimed continuation records carry no inferred PTS/DTS and do not advance the
unwrap clock. The AVC adapter scans the concatenated elementary bytes, so start
codes, AUD/SPS/PPS and coded slices may span any number of PES packets. AUDs
identify complete access units independently of transport boundaries. Clock
ownership follows the PES containing the beginning of the AUD byte-stream NAL;
one PES timestamp cannot be reused for multiple access units, and an unowned
clock is rejected. Annex B leading zero bytes remain accepted. Normalized reader
packets and durations are indexed by access units rather than PES records.
Single-access-unit HLS duration can come from EXTINF even when that unit spans
multiple PES packets. Metadata/scratch reservations and cancellation cover the
extra scans and access-unit index; retained readers remain independently owned.

The independent repacketizer creates 480 PES records for 60 closed-GOP pictures
and 158 for 20 B-frame pictures, deliberately splitting prefixes, parameter sets
and coded bytes. FFprobe retains identical complete access-unit hashes and
FFmpeg retains every NV12 picture. FFprobe's frame parser drops timestamps for
59/19 pictures in these tiny split-prefix fixtures: the manifest records this
limitation rather than claiming complete FFprobe clock agreement. Native checks
verify all anchors against the original pinned independent timing oracle, and
model producer patches match the original MP4 and aligned TS bytes exactly.

This follows H.222.0 sections 2.4.3.7 and 2.14.1: AVC timestamps apply to the
first access unit commencing in a PES and AVC carriage requires AUDs. See
[ITU H.222.0](https://www.itu.int/rec/dologin_pub.asp?id=T-REC-H.222.0-201410-S%21%21PDF-E&lang=e&type=items).
Legacy PES-aligned streams without AUD remain supported. Multiple access units
sharing a PES without their own recoverable clocks still fail explicitly;
SEI/HRD/POC timing inference, cross-segment partial-unit buffering, external TS
maps and broader real-world/display qualification remain work. This continuation
does not establish arbitrary transport or pretrained split-layout conformance.

Final assembly gates pass 116 media tests and seven existing HTTP tests on both
platforms, plus authenticated split-PES TS HTTP. Video gates pass 188 macOS
tests (two skipped) and 163 actual Linux ARM64 tests (27 skipped); model gates
pass 27 macOS and 25 Linux tests (two Metal skips). Independent fresh campaigns
pass 103,200 demux and 110,618 normalization runs with four positive serialized
seeds each. Source/binary/fixture/report bindings are recorded in
`zig/lib/media/testdata/transport/assembly-validation`.


### Explicit WebM entropy-corruption recovery

`video.webm_pictures.walkRecoveringIndexed` adds bounded recovery after a
successfully parsed header index. It catches only coded-data decode failures,
destroys the failed decoder owner, and advances to the next codec-parsed key
restart. AV1 restores that restart's verified sequence bootstrap. Original
logical picture IDs and timestamps remain intact; the required gap callback
reports the failed packet, first lost picture, next restart packet/picture and
reason. A restart event identifies the planned target, which must still decode
successfully. Corrupt restart pictures can trigger another bounded advance.
A missing later restart returns `UnrecoverableVideoData`; exhausted recovery or
skipped-packet budgets return `RecoveryLimitExceeded`.

Source reads and frame/gap callbacks remain outside the recoverable decode
boundary. A control watcher preserves the provenance of cancellation, deadline
and arbitrary provider control errors, even if their error name also resembles
coded-data corruption. Admission/allocation/unsupported-policy failures remain
terminal. Packet leases and decoder owners release before the gap callback or
replacement owner. Independently retained output bytes remain valid across
replacement. Container sync hints never authorize a restart.

Inference continues to use strict `walkIndexed`: damaged selected inputs fail
rather than silently embedding a reduced clip. The explicit recovery API is for
callers that handle reported gaps. It requires valid header metadata; malformed
headers/container indexing and undetected/concealed corruption are separate
qualification/recovery work. H.264/TS resynchronization, broader mixed-tool
streams and sustained corruption/fuzz campaigns remain in the original active
non-CUDA scope. This does not change the v6 embedding recipe or establish new
pretrained output parity.

Qualification includes VP8/VP9/AV1 entropy mutations, a Sintel-derived VP8 clip,
false sync hints, retained frames, corrupt restart/tail handling, gap/frame
callback failures, source/control/cancellation errors, shared admission denial
and allocation-failure cleanup. Surviving picture hashes match clean decoding;
this is restart-equivalence evidence, not an independent codec conformance
oracle. Source/binary/report receipts are in
`zig/lib/video/testdata/recovery-validation`.

Recovery fuzzing exposed and retained VP8 and VP9 transform overflow inputs.
VP8 multiplication and VP9 8-point ADST intermediates now use 64-bit arithmetic;
VP9 checks the upstream valid coefficient bounds through the existing owned
error path, returning coded-data failure instead of panicking. Both inputs have
portable regressions. Exploratory failed campaigns remain separate from final
passing reports; a successful Zig build summary does not establish fuzz success.

The final fresh recovery campaign passes 13,497 executions (801 unique), with
four accepted Smith seeds and no panic. All 4,623 clean corpus pictures across
six VP8/VP9/AV1 streams retain independent FFmpeg NV12 hashes after the fixes.
This bounded campaign does not establish sustained-production fuzz coverage.

Final gates pass 12 focused recovery tests, 196 macOS video tests (two skipped),
171 actual Linux ARM64 video tests (27 skipped) and 27 macOS model tests.

### Progressive H.264 reference sharing follow-up

Native progressive H.264 now retains reconstruction payloads directly in the
DPB, with transactional workspace detachment and copy-on-write for late slices.
The copying path remains selectable for differential qualification. PAFF/MBAFF
completion still uses copies. This advances the retained-reference performance
requirement without changing decoded output ownership or relaxing admission.

Qualification includes snapshot lifetime and allocation-failure checks, output
equality for Baseline, Main/B, High and high-depth 4:4:4 fixtures, model tests
and the pinned Sintel trailer. Measurements and their limits are retained in
`zig/lib/video/testdata/reference-storage-validation`. Remaining cross-group
model reuse, broader codec/fuzz coverage, actual Linux follow-up and streaming
qualification stay in the full active non-CUDA objective.

### Cross-group frame projection reuse

Grouped requests now retain exact frame projections across repeated and
overlapping sampling windows, including multiple video parts inside one group.
The cache is bound to one loaded session and keyed by immutable clip content
and sampled picture identity. Native CPU arrays and retained Metal device
tensors survive temporary backend teardown; device reuse adds no host readback.
The model lock is released between groups, and existing preparation/model
overlap remains active for missing frames. Metadata and necessary dependency
replay remain part of strict input validation.

Capacity is bounded to 128 entries and 64 MiB of retained payload/buffer spans,
with shared per-entry admission held across execution permits. Whole-buffer
charges account for retained views. Producer tickets prevent eviction while
queued and return ownership during cancellation and failure. Device cleanup
keeps its process protection guard until the retained buffers are released.

The pretrained qualification compares ordered repeated/mixed-hit groups and
multiple video parts against the pinned uncached binary and PyTorch oracle,
including VFR, PAFF, VP9, AV1 and long Sintel windows. Receipts live under
`zig/lib/video/testdata/frame-cache-validation`. Actual Linux ARM64 now passes
174 decoder tests (27 skips) and 31 model/cache tests (three skips), closing the
previous progressive H.264 runtime follow-up. Broader sustained scheduling,
cache tuning, real-world codec/display coverage and streaming integration stay
in the full active non-CUDA objective.

The final pretrained grouped/multipart preservation run passes on native CPU,
Metal and actual Linux ARM64. All 18 ordered groups and both multipart groups
match their pinned uncached binaries exactly; reference rankings/top-1/3/5
overlap are exact, with maximum cosine score drift below 3e-7. The frame-cache
receipt binds the source, binaries, checkpoint, oracle and runtime artifacts.

### Partial fMP4 HLS ingestion qualification

The native media reader now consumes published partial fMP4 units, exposes part
identity, and issues explicit bounded blocking reloads without changing relative
resource/authentication resolution. A constant-space sample commitment verifies
completed-parent overlap before suppressing duplicates or recovering a missing
tail. Failed fetch, allocation and admission operations retain retry position;
old packet leases remain valid. Preload hints are parsed but never speculatively
consumed. Independently encrypted AES-128 partial objects are qualified.

The media receipt at `zig/lib/media/testdata/hls/partial-validation` records
macOS, actual Linux ARM64, production HTTP and bounded fresh parser-fuzz results.
Selected semantics follow the work-in-progress HLS second-edition draft 22.
TS partials, CBC byte-range chaining, date-range delta updates, rendition coordination
and model-level live window orchestration still require implementation and
qualification. The full non-CUDA objective remains active. Codec source ports
are owned and maintained in-tree; no runtime codec-library dependency is allowed.

### Direct model inference over published packet windows

The typed native model API now accepts borrowed packet readers and exposes
`embedLiveWindow` for finite published HLS/DASH units. Joining copies encoded
samples and metadata into an independently owned reader without MP4 remuxing.
Original segments may then be released. Mixed configuration epochs/DASH periods
and overlapping decode clocks are rejected. Shared admission covers staging and
retained storage; model preflight and execution continue through existing CPU/
Metal permits, cancellation, bounded queues and logical timestamp selection.

The qualification receipt is `zig/lib/video/testdata/live/validation`. Current
scope proves reader preparation/ownership and model entry, rather than pretrained
streamed retrieval parity. Caller-supplied presentation watermarks and preroll
remain necessary. Automatic transport reload-to-window scheduling, decoder-order
watermark derivation, cross-window retention/eviction and pretrained streamed
CPU/Metal/Linux comparisons are the next implementation/qualification gaps.
The full non-CUDA objective remains active.

### Bounded live window pumping and model commit

The request-local coordinator now connects existing authenticated HLS/DASH
readers to bounded windows, explicit reload and typed model inference. It retains
failed fetched units, drains epochs before clock resets, preserves complete
logical preroll and future presentation samples, and advances the window only
after fallible work succeeds. Sparse intervals are explicit gap events. Exact
source-time sampling keeps frame identity stable across preroll eviction.

Automatic frontiers validate supported AVC assemblies and enforce a
monotone decode-clock/`PTS >= DTS` contract; MJPEG also uses the scheduled clock.
Field-coded and in-band configuration streams cap that clock at open assemblies. Existing clip-relative sampling is unchanged. `embedNextLive` runs the
ordinary CPU/Metal pipeline with shared model admission, reports source and
manifest timing, and preserves the retry cursor on inference failure.

Qualification lives at `zig/lib/video/testdata/live/coordinator-validation`.
Broader pretrained field/dynamic streamed CPU/Metal/Linux parity,
cross-window projection reuse and measured reduction of repeated frontier
validation remain required. Current native decoded frontier probing establishes
correctness, rather than maximal throughput. The full non-CUDA objective stays
active, including broader codec/fuzz, performance and live/container scope.

### Pretrained live-window reference preservation

The typed live adapter now executes successfully with pinned pretrained weights
on native CPU, Metal and actual Linux ARM64. Three two-second windows from the
same six-second real Sintel excerpt match complete-clip source-time inference
exactly, including 246 encoder tokens per window. Two text queries preserve
pinned PyTorch cosine scores, rankings and top-1/top-3 overlap; maximum score
drift is below 5e-7 on this fixture. Pinned PyTorch MPS also passes F32 vector
gates against the CPU oracle and supplies completed diagnostic timing. This
measures reference preservation rather
than application relevance.

The qualification executable uses the existing supervised worker and request
hard-cancellation watchdog, retaining protection through model load/close. The
Python driver verifies checkpoint identity, exact window coverage, encoder
counts, finite normalized vectors, baseline equality and pinned PyTorch gates.
Source, fixture, binary and oracle receipts live in
`zig/lib/video/testdata/live/pretrained-validation`. Timings cover completed
adapter/complete-clip inference on a shared host; file acquisition/model load,
peak memory and sustained throughput are outside this timing scope.

Broader pretrained field/dynamic layouts and larger
stream corpora, cross-window projection reuse and reducing repeated frontier
decode work remain required by the full active non-CUDA objective.


### Streaming AVC assembly barriers

Automatic live watermarks now use the native logical-picture walk for field-coded
and in-band configuration AVC too. `h264_pictures.streaming` returns completed
pictures and the earliest pending picture's timestamp/packet. The decode-clock
frontier is capped at that timestamp until its remaining fields/slices arrive.
Explicit watermarks cannot cross an open assembly, and EOF with an unfinished
picture fails without finishing the coordinator. An IDR cannot silently discard
an unfinished streaming assembly. Corrupt NALs, missing data partitions and
resource failures remain errors.

Source-time model sampling accepts completed earlier windows with unfinished
future field context, but rejects a window crossing the pending barrier. Ordinary
reference and clip-relative decoding keep their existing behavior. Tests cover
interleaved 10-bit 4:2:2 fields split across published units, adaptive 8-bit field
pairs, source sampling, complete dynamic geometry and separate-plane/partition
indexes. These are generated-fixture correctness checks; pretrained field/dynamic
stream parity and reducing repeated GOP validation remain required. No codec or
transport runtime dependency is added.

Qualification: `zig/lib/video/testdata/live/field-frontier-validation`.


### Bounded HLS delta snapshots

`hls.parseUpdateControlled` merges `EXT-X-SKIP` segment-only updates against the
committed playlist. Every skipped complete segment must be present; skipped
partial-only parents cannot stand in for complete history. Map/key declarations,
explicit identity `METHOD=NONE`, discontinuity state and implicit byte-range
continuation carry forward. Delta playlists own all URI references independently
of both manifest bodies, with bounded planning memory, shared admission,
cancellation checks and allocation-failure cleanup.

`hls_live.Reader.reloadDelta` explicitly requests `_HLS_skip=YES` only when the
server advertises skipping and a configured monotone clock proves the committed
snapshot is no older than half the skip boundary. Otherwise it performs a full
reload. Authentication query bytes remain intact; conflicting/encoded delivery
directive aliases fail. Missing skipped history causes exactly one full request
without the directive. HTTP/allocation/validation failure preserves the old
playlist and delivery cursor. Overlapping descriptor mutations still fail.

The real authenticated HTTP test delivers two successive fMP4 units, retains an
older packet lease, then recovers unavailable skipped history without repeating
media or initialization fetches. Final ReleaseSafe checks pass 139 media tests
and nine production HTTP tests on macOS and actual Linux ARM64. A bounded delta
fuzz campaign passes 20,456 runs with positively accepted inherited-key seeds.
These checks qualify segment-only `YES` deltas; date-range `v2`, rendition
coordination, TS partials and CBC range chaining remain work. No runtime codec
or transport dependency is added. Protocol basis: [HLS 2nd edition draft 22,
sections 4.4.5.2 and 6.3.7](https://datatracker.ietf.org/doc/html/draft-pantos-hls-rfc8216bis-22).

See [qualification receipts](../../zig/lib/media/testdata/hls-delta-validation/README.md).


### Reuse validated live AVC metadata

The coordinator retains the completed/pending logical-picture index from each
successful append. Preparation owns an admitted metadata snapshot; commit uses
that snapshot and still validates IDR payloads before moving the cursor. Whole-unit
preroll eviction records a packet base, and snapshots rebase only retained
pictures. Packet-count/selector bounds and pending barriers prevent invalid
suffixes. Appending or draining an epoch replaces/releases the old index only
after fallible validation succeeds. Delivered metadata outlives coordinator
mutation and source release independently.

Logical indexes now retain shared admission for owned metadata. Construction
accounts conservatively for ArrayList growth/reallocation, then shrinks the
reservation to retained picture bytes after temporary owners are released.
Snapshots acquire their own reservation before copying. A regression prepares
and commits with only 128 KiB admission headroom, which cannot admit a fresh
native decoding workspace. Existing IO/cancellation/allocation failure and
field-assembly checks still pass.

Source-pinned before/after coordinator measurements preserve source intervals,
eligibility and ordered packet clock/payload digests. The small real Sintel case
reduces validation walks from 9 to 6 and pooled warm median time from 218.6 to
132.1 ms. Normalized-clock interleaved field packets reduce walks from 72 to 24
and median time from 29.4 to 6.6 ms. A remux-only 52-second, 480x320 real clip
delivers 27 identical windows with 54 versus 27 walks; its single warm diagnostic
sample changes from 40.5 to 24.4 seconds. These measure the serial control plane,
including owner release, rather than full model throughput. They exclude file
acquisition. Admitted peak rises by roughly 0.8 MiB because metadata construction
is now conservatively charged; this is not an RSS measurement.

Current pretrained CPU, Metal and actual Linux ARM64 live windows remain exactly
equal to complete-clip source-time vectors on all three recorded Sintel windows,
with 246 tokens each. Pinned PyTorch rankings/top-k and score gates pass. The
qualification reports expose coordinator validation/snapshot counts separately
from model work. Wider field/dynamic pretrained parity, append-time incremental
decoding, repeated model planning, cross-window projection reuse and full-pipeline
performance remain required by the full active non-CUDA scope.

See [qualification receipts](../../zig/lib/video/testdata/live/metadata-reuse-validation/README.md).


### Content-bound live model selection

`embedding_gemma2.ReaderSelection` owns sampled transport selectors derived from
a validated logical-picture index. Its SHA-256 binding covers all track metadata,
packet descriptors, encoded packet bytes and the complete sampling policy using
explicit value serialization. It does not depend on source URLs, addresses or
caller read counters. Changed bytes, timing, display geometry or policy reject
reuse. The caller retains immutable reader metadata/storage and the selection
until synchronous encoding completes or `Prefetch.deinit` joins the producer.
The logical index passed to construction is trusted validated library metadata;
this is an SDK object, not an untrusted serialized client hint.

The typed live adapter constructs one admitted selection from its independently
owned coordinator snapshot. Model preflight, asynchronous preparation and the
synchronous fallback reuse those selectors after checking the binding. They no
longer reconstruct logical pictures just to select frames. Parameter-set/display
qualification and actual selected-frame decoding remain in their existing paths;
this does not add borrowed-reader projection caching or incremental decoder state.
Selector admission covers temporary metadata/selection construction and shrinks
to retained selector bytes after temporary owners are released.

Final ReleaseSafe tests pass 45 CPU/Metal model checks on macOS and 40 on actual
Linux ARM64 (five Metal skips). Native and Metal prepared field-patch hashes match
the ordinary selection path. Tests reject mutated packet bytes, PTS, display aspect
and sampling policy, verify caller read counters are unchanged, and exhaustively
check construction allocation failures and reservation cleanup. A field planning
regression succeeds with 256 KiB admission headroom while the ordinary logical
walk fails admission, proving the decode walk is avoided.

Fresh pinned pretrained CPU, Metal and actual Linux live vectors still match
complete-clip source-time inference exactly for three real Sintel windows with
246 tokens each. Pinned PyTorch score/ranking/top-k gates pass. Timings remain
shared-host diagnostics; this change has no throughput claim. Broader pretrained
field/dynamic stream parity, repeated parameter planning, incremental append
decoding, cross-window projection reuse and the full non-CUDA scope remain active.

See [qualification receipts](../../zig/lib/video/testdata/live/reader-selection-validation/README.md).


### Charge retained MP4 indexes by allocation

MP4 readers charge packet/list allocations during construction, including old/new
buffer overlap for moving growth. Denied growth preserves the old allocation;
cleanup releases every construction charge. After temporary lists are freed,
the retained reservation equals the final packet-array bytes. Metadata leases
remain separately admitted. The configured index limit still bounds returned
packets; it is no longer reserved in full for every buffered live unit.

The six-fragment Sintel coordinator comparison preserves exact window payload
digests and reduces peak admitted host bytes from 101,990,501 to 1,328,885.
This is admission accounting rather than an RSS or throughput claim. macOS and
actual Linux ARM64 pass 142 media and nine HTTP tests, including allocation
failure, shared denial and forced-copy overlap checks.

See [qualification receipts](../../zig/lib/media/testdata/index-admission-validation/README.md).


### Materialize only selected VP9/AV1 outputs

Selected WebM replay now reconstructs and drains every dependency while copying
only requested visible outputs. `Session.decodeOutputs` reports every native
output count and preserves per-packet ordinals on owned frames. Superframes,
scalable layers and container-invisible packets retain logical identity and
reference state. Native output-count limits bound skipped pictures too; owned
output-byte limits charge selected frames. Full materialization remains the
differential oracle. Corruption and cancellation retain fail-closed cleanup.

Matched real Sintel replay preserves selected pixels, metadata and packet counts
on macOS and actual Linux ARM64. Output copies fall from 209.7 MB to 19.7 MB at
480p and from 298.6 MB to 99.5 MB at 1080p. These are copied bytes, not RSS or a
full-pipeline throughput claim. Fresh pretrained CPU/Metal WebM vectors and
retrieval gates pass against pinned PyTorch. The original non-CUDA scope remains
active; this does not add incremental live AVC validation or projection reuse.

See [qualification receipts](../../zig/lib/video/testdata/selected-output-validation/README.md).


### Admit actual VP9/AV1 core and batch allocations

Decoder sessions now charge their object footprint and each live core allocation
instead of retaining the entire configured decode ceiling. The shared tracked
allocator admits allocation headers, normalization and old/new growth overlap
before allocation. Independently retained batch metadata and pixel frames own
separate tokens. Failure sweeps core storage; the failed object remains charged
until destruction. Shared denial preserves peers, and Annex B preflight denial
remains retryable without masking subsequent corruption.

Matched real-clip replay preserves selected-output digests and packet/copy counts.
Peak shared admission drops from 303–305 MB to 38–70 MB across the 480p/1080p
VP9/AV1 corpus. These are admitted bytes rather than RSS or throughput. Debug and
ReleaseSafe pass 225 macOS video checks; actual Linux passes 200, with platform
skips. CPU/Metal model and shared media/HTTP gates pass. The original non-CUDA
scope remains active.

See [qualification receipts](../../zig/lib/video/testdata/core-admission-validation/README.md).


### Reuse reader-backed frame projections

Validated logical pictures now retain their completion packet alongside the first
packet. Reader selections own visual keys over track/display values and all
available coded dependencies through completion. Appending future packets keeps
earlier keys stable; changed dependencies/display values invalidate them. Preroll
eviction conservatively misses. Field/partition and separate-plane assemblies
retain complete dependency identity when metadata is cloned and rebased.

Synchronous encoding and asynchronous preparation use the bounded CPU/Metal
FrameCache for borrowed readers. `embedNextLiveCached` accepts a caller-owned
request-local cache tied to one immutable model session. Keep its control/watchdog
alive through all calls and producer joins; close the cache before its session.
Tickets, admission, eviction and cancellation retain their existing contracts.

Fresh real-clip CPU, Metal and actual Linux overlap each record a cache hit and
exact cached/uncached 246-token vector equality. Ordinary live windows remain
exact against complete-clip inference and pass pinned PyTorch retrieval gates.
47 macOS model checks and 42 actual Linux checks pass, with platform skips. The
full goal remains active; this does not add incremental live decoder state.

See [qualification receipts](../../zig/lib/video/testdata/live/reader-cache-validation/README.md).


### Explicit rolling and sparse window advance

Live coordinator windows accept an optional positive `advance_ns`, independent
of duration and within-window frame sampling. Null preserves consecutive
windows; smaller advances overlap and larger advances sample sparse intervals.
Commit retains preroll for the next start, with all fallible probing before
cursor/ownership mutation. EOF tails clip to the published end; old epochs drain
before reset clocks. Sparse pumping evicts skipped units to validated preroll
under the existing byte/unit/packet limits, preserving failed-transfer retries.

Fresh CPU, Metal and actual Linux pretrained comparisons pass six rolling and
two sparse real-clip windows, all exactly equal to complete-clip vectors and token
counts. The matching original intervals pass pinned PyTorch retrieval gates.
Live cache telemetry reports conservative misses after one-second GOP eviction;
no across-eviction reuse or sustained-throughput claim is made. MacOS passes
230 video, 19 live and 47 model checks; actual Linux passes 205 video, 19 live
and 42 model checks with platform skips. The original scope remains active.

See [qualification receipts](../../zig/lib/video/testdata/live/advance-validation/README.md).


### IDR-bound projection reuse across rolling-window eviction

For static AVC parameter sets, the reader projection hash restarts at a validated
complete single-packet IDR picture. Track/configuration/display values, source
clocks, packet sizes/sync values and coded payloads through picture completion
remain bound. Byte offsets are normalized and picture selectors are relative to
the reset, so repacking identical packets or evicting an earlier GOP preserves
identity. The full ReaderSelection binding still checks the original reader and
sampling policy; this change does not permit transferring that selection owner
to a different reader.

Incomplete or crossing assemblies, unknown completion metadata and in-band
parameter-set streams retain the conservative dependency chain. Container sync
hints alone never establish an independent boundary. Changed retained dependency
bytes or display policy invalidate projections; changes before a proven IDR do
not. Request-local CPU/Metal cache ownership, admission and cancellation contracts
remain unchanged.

Fresh pretrained CPU, Metal and actual Linux ARM64 qualification preserves all
six rolling and two sparse vectors/token counts exactly against complete-clip
inference. The rolling fixture now records five hits and six stored projections
rather than zero hits and eleven stores; sparse intervals correctly have zero
hits. Pinned PyTorch score/ranking/top-k gates cover only the three matching
original intervals. The checked-in rolling verifier enforces window identities,
cache counts and baseline equality and rejects the historical zero-hit report.

ReleaseSafe model gates pass 49 CPU/Metal checks on macOS and 44 checks with five
Metal skips on actual Linux. Source/fixture/binary receipts record the exact
scope. This continuation does not establish arbitrary stream conformance or
sustained throughput; append-time incremental reconstruction and broader
qualification remain active work.

See [qualification receipts](../../zig/lib/video/testdata/live/idr-cache-validation/README.md).


### Persistent static-AVC live validation

The coordinator now defaults to `incremental_validation = true` for static AVC
parameter sets. An owned `h264_pictures.Stream` retains reconstruction workspaces,
prediction references and unfinished assemblies across appended packets, using
the same native decode loop as ordinary replay. Separate colour planes retain
independent lanes. Completed logical metadata escapes only through independently
owned snapshots; source eviction rebases selectors and cursor state.

SHA-256 binds immutable track values and normalized packet descriptors/payloads.
Changed prefixes reject reuse before reconstruction. All eviction probes precede
state mutation. If reconstruction or a later append probe fails, the coordinator
discards the mutated decoder owner while preserving committed metadata, cursor
and incoming unit ownership; retry reconstructs retained preroll. Shared admission
covers the stable owner, retained reconstruction resources and actual metadata
allocation/growth. Cancelled or failed construction frees those owners.

Full replay remains selectable as `incremental_validation = false`, and in-band
SPS/PPS configuration streams continue to use it. Automatic pending-picture
watermarks and incomplete EOF checks remain active. Replay now reports other
pictures completed in a packet before returning its unfinished-selection error;
otherwise streaming metadata could lose a completed interleaved field pair.

ReleaseSafe macOS gates pass 234 video tests (two skips), 20 live checks and 49
CPU/Metal model checks. Actual Linux ARM64 passes 209 video tests (27 platform
skips), 20 live checks and 44 model checks (five Metal skips). The replay and
persistent paths preserve identical logical metadata through unfinished fields;
allocation failures, prefix mutation and denied eviction retain clean ownership
and retry behavior. Independently held snapshots survive owner advancement.

Fresh CPU, Metal and actual Linux pretrained incremental/replay comparisons
preserve all eight rolling/sparse vectors and token counts exactly, including the
124-token clipped tail. Each policy reconstructs 60 packets rather than 150;
rolling reconstruction payload bytes fall 81,132 to 33,431 and sparse bytes fall
85,762 to 33,431. Coordinator walk/snapshot counts and projection cache behavior
match. These are successful native reconstruction counts, excluding compressed
prefix hashing and transport reads; shared-host timings do not establish
sustained throughput or RSS improvement.

Fresh pinned PyTorch CPU and MPS oracles now cover every rolling/sparse interval,
including the previously uncovered odd windows and clipped tail. CPU, Metal and
Linux preserve exact rankings and full top-k overlap for both queries across all
eight documents. Maximum vector-coordinate error is below 2.08e-6. The reference
generator retains independent FFmpeg picture/timestamp selection and pinned
checkpoint/implementation hashes; all dependencies are offline qualification
tools. The verifier rejects missing reference cases, altered policies, incomplete
oracles and unchanged reconstruction work. Historical subset oracles remain
supported with their explicit narrower scope.

Dynamic incremental parameter/geometry handling, persistent-owner fuzzing,
large-clip sustained measurements, prefix/parameter planning and the original
broader non-CUDA scope remain active. The modified ordinary reconstruction loop
passes 101,170 coverage-guided H.264 fuzz runs; this does not qualify sustained
persistent-owner fuzzing. Source, binary and fixture receipts bind the scope.

See [qualification receipts](../../zig/lib/video/testdata/live/incremental-validation/README.md).


### Persistent dynamic-AVC library state

`h264_pictures.Stream` now accepts in-band configuration readers. Its stable
parameter registry scans appended packets only, retains effective SPS/PPS history
and configuration epochs, and supplies the same decoder loop with per-packet
configuration contexts. Compatible PPS/SPS changes preserve references; storage
changes replace admitted native owners after checking unfinished assemblies.
Independent colour-plane lanes and native integer depths remain available.

`parameterBootstrap` returns independently owned length-prefixed parameter NALs
effective before a retained packet. Initial avcC entries need no duplication.
Registry rebasing prunes obsolete parameter history, keeps the last pre-boundary
value of each ID and rebases future events/epoch selectors. Allocation precedes
mutation; an empty current epoch remains after complete packet eviction. The
bootstrap establishes parameter availability, not coded-picture independence:
ordinary I-picture suffixes still fail without required prior references.

Differential checks compare every appended prefix against fresh registry planning
and full reconstruction for dynamic geometry/depth/chroma and PPS changes. Each
packet is parameter-scanned and reconstructed once. Allocation-failure campaigns
cover replacement owners, registry/history growth, snapshots, bootstraps and
rebasing. A PAFF reset suffix independently reconstructs with its owned bootstrap;
a non-IDR suffix retains its reference error. Prefix bindings now hash byte slices
in bulk rather than encoding every payload byte as an integer.

ReleaseSafe passes 237 video tests (two skips), 20 live checks and 49 CPU/Metal
model checks on macOS. Actual Linux ARM64 passes 212 video tests (27 platform
skips) and 20 live checks. This phase does not add fresh pretrained inference or
persistent-owner fuzz claims. Automatic live coordination still selects replay
for dynamic streams: independently owned bootstrap checkpoints must be carried
through prepared-window construction, preroll eviction and decoder-failure retry
before enabling the new library path there. That integration, bounded retained
configuration-entry compaction and the full original non-CUDA scope remain active.

See [qualification receipts](../../zig/lib/video/testdata/live/dynamic-library-validation/README.md).


### Owned dynamic live checkpoints and inference

Automatic live coordination now enables persistent reconstruction for in-band
AVC configuration streams. A separately admitted parameter checkpoint survives
native decoder invalidation. Joined probe/prepared readers own bootstrap NALs
prepended to the first retained payload, preserving packet selectors and clocks.
Prepared snapshots borrow neither registry parameter memory nor released units.

Commit and sparse pumping construct the next effective checkpoint and canonical
suffix before mutating retention. Native state rebases to that canonical content
binding only after every fallible probe/allocation succeeds; checkpoint replacement
then becomes infallible. Failed appends discard mutated decoder state, preserving
the prior checkpoint, metadata, cursor and incoming unit for replay/retry. Epoch
transitions release old checkpoints after draining old outputs. Full replay uses
the same owned suffix policy as the differential oracle.

Retention now qualifies complete dynamic PAFF reset packets through their parsed
active configuration, including an IDR first field with a non-IDR complementary
second field. Container sync hints alone are insufficient. Native pixel comparisons
cover changing geometry/depth/chroma before and after eviction, independently
held old prepared windows, and malformed-append recovery. Allocation-failure tests
cover construction/replacement and actual pixel replay from prepared storage.

ReleaseSafe macOS passes 240 video tests (two skips), 23 live checks and 49
CPU/Metal model checks. Actual Linux ARM64 passes 215 video tests (27 skips), 23
live checks and 44 model checks (five Metal skips). A real avc3 Sintel fixture
remuxes the same encoded pictures with inline SPS/PPS at each IDR. CPU, Metal and
Linux preserve all six rolling/two sparse vectors exactly against complete-clip
and replay inference, including the 124-token clipped tail. Each policy reconstructs
60 rather than 150 packets; rolling bytes fall 81,888 to 33,683 and sparse bytes
fall 86,518 to 33,683. Successful reconstruction counters exclude prefix hashing
and transport reads; shared-host timings do not establish sustained throughput.

Fresh pinned PyTorch CPU/MPS independently qualify all eight avc3 intervals.
Every backend preserves exact rankings/full top-k overlap (1, 3, 5, 8), with vector
coordinate error below 2.08e-6. Dynamic projection keys intentionally retain the
conservative prefix policy: rolling windows record zero hits/eleven stores and
sparse windows zero hits/four stores. The verifier declares this policy separately
from static IDR reuse and rejects a dynamic report under the static cache gate.
No dynamic cache throughput benefit is claimed.

Retained configuration-entry compaction, stronger mixed/dynamic pretrained
qualification, persistent-owner fuzzing, sustained full pipelines and broader
original non-CUDA requirements remain active. No runtime dependency is added.

See [qualification receipts](../../zig/lib/video/testdata/live/dynamic-coordinator-validation/README.md).

### Collect obsolete live AVC configurations

Live registry rebasing now frees configuration maps and AVCC payloads no longer
reachable from retained packets, epoch initialization or the active configuration.
Selector and epoch IDs compact together after allocation/index checks, preserving
transactional eviction and native reference state. Retained array capacity remains
bounded by the configuration limit.

A parameter-planning test activates 600 distinct PPS configurations under a
two-entry limit, returns to one entry after each eviction and verifies admitted
byte reclamation. Activation prefixes omit macroblocks; this is not codec
conformance evidence. Exhaustive allocation failures and existing dynamic pixel,
held-window and append/retry tests cover ownership and reconstruction separately.

ReleaseSafe macOS passes 242 video, 23 live and 49 model checks; actual Linux
ARM64 passes 217 video, 23 live and 44 model checks (platform skips documented).
Fresh native CPU, Metal and Linux pretrained windows retain exact complete-clip
and prior incremental vectors across all eight rolling/sparse intervals. Previously
pinned CPU/MPS oracles preserve rankings and full top-k overlap (1, 3, 5, 8).
This phase reuses those oracles and does not rerun full-replay inference.

Broader changing-tool pretrained qualification, persistent-owner fuzzing, sustained
full-pipeline measurements and remaining original non-CUDA scope remain active.
No runtime dependency is added. See [qualification receipts](../../zig/lib/video/testdata/live/configuration-compaction-validation/README.md).

### Stateful AVC fuzzing and corrupt pending-tail rejection

A maintained Zig coverage-guided target preserves MP4 indexes while mutating coded
payloads. Six seeds cover prediction, dynamic geometry/PPS, interleaved split PAFF,
partitions and high-depth separate colour planes. Chunked appends and accepted
evictions compare logical metadata/pending assemblies with full-prefix replay;
held snapshots remain unchanged. Destruction verifies zero shared admission and
packet retention. Resource/deadline/allocation bounds are excluded from successful
output equivalence. This compares metadata, not mutated pixel conformance.

The first campaign found replay swallowing a corrupt incomplete colour plane when
another plane had reported pending metadata. A distinct pending-tail error now
identifies recoverable selected field/partition packet boundaries. The recovered
930-byte input fails before the fix and now explicitly requires corruption
rejection, alongside replay/incremental equivalence.

ReleaseSafe macOS passes 244 video tests (two skips), 23 live checks and 49
CPU/Metal model checks. Actual Linux ARM64 passes 219 video tests (27 skips) and
23 live checks. The new campaign log records actual execution/coverage counts;
the initial failing campaign and before-fix reproduction remain in the receipts.
No new pretrained or performance claim is made. Broader real-stream/conformance,
changing-tool pretrained, full-pipeline performance and protocol work remains
active. No runtime dependency is added. See [qualification receipts](../../zig/lib/video/testdata/live/persistent-fuzz-validation/README.md).

#### Expanded AV1 pretrained gap measured

The packet-preserving nine-format WebM corpus and completed pinned PyTorch/native
CPU/Metal runs now demonstrate a failing reference-preservation gate despite exact
independent decoded planes. All video vectors fail the existing 1e-4 coordinate
gate; both query rankings change with top-1 overlap zero. The harness retains
scores/rankings before checking vectors. Next implementation work must reconcile
monochrome gray/range behavior, high-depth RGB quantization and 4:2:2/4:4:4
conversion policies through independent pixel oracles, then rerun pretrained
CPU/Metal and actual Linux. Existing successful 8-bit 4:2:0 qualification cannot
be generalized to these formats. Layer model policy and all other active scope
remain required. See `zig/lib/video/testdata/av1-model-validation/README.md`.

The monochrome preparation defect now has an explicit gray-sample policy in
native CPU/Metal software WebM inference. Exhaustive independent pixel oracles
pass, as do both monochrome pretrained vectors. Seven color-format vector cases
and full-corpus rankings still fail. Next work remains independent chroma
reconstruction and high-depth color quantization, followed by complete pretrained
CPU/Metal/Linux qualification; this fix does not establish expanded-format parity.
The retained capacity-denial/retry reports and correction receipts are in
`zig/lib/video/testdata/av1-gray-model-validation`.

Native-depth 4:4:4 color preparation now combines Q13 matrix products before
rounding, with portable Zig SIMD for 8-bit rows and matching Metal arithmetic.
A 54-case independent RGB oracle and actual Linux/Metal checks qualify this
policy; a reproducible full Sintel AV1 High 4:4:4 workload preserves all 1,253
independent picture hashes and measures scalar/SIMD/Metal preparation. Its
shared-host preparation measurements exclude pretrained inference. The model
harness preserves vector/ranking evidence before later stress failures.
Filtered chroma reconstruction for 4:2:2/high-depth 4:2:0, complete pretrained
CPU/Metal/Linux parity, wide-gamut display/layer policies and the full remaining
non-CUDA scope remain required. See `zig/lib/video/testdata/av1-combined-model-validation`.

The corrected 8/10/12-bit 4:4:4 pretrained vectors pass unchanged CPU/Metal gates.
Four 4:2:0/4:2:2 vectors and full rankings remain failing. CPU serial/retry
requests complete, while Metal records all vectors before a capacity-denied
retry. Neither the preparation benchmark nor partial vector correction closes
the full-pipeline qualification requirement.

### Subsampled native-depth chroma preparation qualification

Native Zig SDR chroma reconstruction now preserves centered sample positions,
fixed-point cubic coefficients, border folding and reference RGB quantization.
CPU filters U/V together with portable SIMD; Metal reconstructs chroma on the
device and reuses uploaded coefficients across matching geometry. Header-only
AV1/VP9 picture metadata carries depth/chroma, so model preflight admits the
correct reconstruction scratch before execution.

The independent 108-case RGB oracle covers odd/even geometries, 4:2:0/4:2:2,
8/10/12-bit samples, three matrices and both ranges. BT.2020 primitive arithmetic
is tested; the existing SDR BT.2020 display rejection remains. Scalar/SIMD/Metal
checks include strides, rotation, coefficient reuse and allocation cleanup.
macOS ReleaseSafe passes 250 video checks (two skips) and 50 model checks.
Actual Linux ARM64 passes 222 video checks (29 skips); that binary predates the
additional allocation-failure test, which passes in the final macOS suite.

All nine expanded AV1 video vectors and both queries now pass the unchanged
pretrained parity gates on macOS native CPU, Metal and actual Linux ARM64 native
CPU. Both query rankings are exact, with top-1/5/10 overlap 1.0. This supersedes
the earlier four-format numerical failures. Linux uses the pinned PyTorch CPU
reference generated on macOS; it does not establish fresh Linux PyTorch parity.
The corpus consists of generated clips and measures reference preservation,
not end-user retrieval usefulness.

Metal completes all numerical comparisons, then its cancellation/retry request
is capacity-denied (retryable HTTP 503) under shared memory admission. Its overall
stress report remains failed; no admission gate is disabled. CPU serial sustained
and disconnect/retry checks complete, but peer disconnect alone does not prove
cancellation completion. Broader display/layer policies, mixed real-world streams,
full-pipeline load/performance and the remaining non-CUDA scope remain active.

See the [current qualification receipts](../../zig/lib/video/testdata/av1-chroma-model-validation/README.md).

### Scalable AV1 model presentation qualification

Model WebM planning and selected replay now share operating point zero with
highest-spatial-layer presentation. Lower layers remain decoded as dependencies;
they do not become extra model sampling pictures. Explicit all-layer output
remains available to library callers. Admission preflight, cache producers and
ordinary inference all use the same model policy.

A packet-preserving WebM corpus adds the pinned two-spatial/three-temporal-layer
AV1 clip to the nine depth/chroma controls. Independent FFmpeg output matches
all 12 top-layer AOM picture hashes; repeated generation reproduces all 12 corpus
artifacts byte-exactly. The regression compares the library's 24-picture timeline
with the model's 12-picture timeline, selected IDs 0/6, admitted pixel count and
independent planar hashes. The complete macOS model suite passes 51 tests.

The old server fails the scalable embedding gate (maximum coordinate error
0.03649, cosine 0.95742) and changes one query's ranking. The corrected server
preserves all ten video vectors, both queries, exact rankings and top-1/5/10
overlap 1.0 on native CPU, Metal and actual Linux ARM64 CPU. Gates are unchanged.
Linux compares native execution with the pinned PyTorch CPU reference generated
on macOS; it is not a fresh Linux PyTorch run. These generated fixtures establish
reference preservation, not end-user relevance or arbitrary scalable conformance.

Metal completes every vector/ranking comparison, then its retry is capacity-denied; its overall stress report remains failed.
CPU/Linux disconnect/retry and ten serial sustained requests complete. Peer
disconnect does not prove cancellation completion. Broader scalable streams,
operating-point switching, display policies, real-stream/fuzz campaigns,
full-pipeline load/performance and the remaining non-CUDA scope stay active.

See the [qualification receipts](../../zig/lib/video/testdata/av1-layer-model-validation/README.md).

### Observable cancellation cleanup and bounded capacity retry

HTTP grouped embeddings now expose active, completed, cancelled, timed-out and
failed request metrics. An outer trace publishes its outcome only after the
inner operation releases frame-cache fences, model handles and arena storage.
The qualification also waits for weighted request admission to return to zero.
These metrics describe HTTP grouped requests; they do not cover every direct
embedding API or identify the GPU kernel active when a reset arrives.

The client now observes an active grouped request before an abortive TCP reset.
It verifies a new cancelled outcome after cleanup, zero active/admitted requests
and correct retry vectors. A separate write-half-close request must complete
normally with the correct vector and no new cancellation. This distinguishes
hard disconnects from legal HTTP half-close semantics. Earlier blind
shutdown/immediate-retry failures remain historical evidence; no inference
memory or concurrency limit was relaxed to make these checks pass.

Capacity retry honors the server's declared millisecond delay, with one 30-second
request/retry deadline and at most 16 attempts. Every denial is retained. Invalid,
non-capacity, non-retryable and transport failures stop immediately; deadline and
attempt exhaustion remain failures. A focused video probe uses the existing
explicit test-only run-admission hook to force one 503, waits one second, verifies
pretrained vector parity and checks released admission while real limits remain.

Fresh native CPU, Metal and actual Linux ARM64 reports pass all ten video vectors,
both queries, exact rankings and full top-1/5/10 overlap against the pinned
PyTorch CPU reference. Each passes observed reset cleanup, retry, legal
half-close and ten serial sustained requests. Each also passes the real forced
video capacity-denial probe. The cleanup metric unit test passes on macOS and
actual Linux; 31 Python contract tests cover bounded retry, retained failures and
cleanup attribution. Linux compares against the macOS PyTorch reference, rather
than a fresh Linux PyTorch execution.

These checks establish request-level cleanup for this generated corpus and
owned-server workload. Timings and memory samples are shared-host diagnostics;
they do not prove production throughput, arbitrary GPU-stage cancellation or
all concurrent load patterns. Broader display/scalable/real-stream qualification,
full-pipeline performance and the remaining non-CUDA scope stay active.

See the [qualification receipts](../../zig/lib/video/testdata/cancellation-model-validation/README.md).

### Identity-matrix RGB video preparation

Native video preparation now accepts full-range 4:4:4 identity-matrix planes in
GBR order. CPU reorders 8-bit channels with portable SIMD; Metal performs color
conversion on device from native integer textures. Ordinary preparation retains
full-range RGB scaling. The explicit `gbr_reference` model profile also matches
the pinned processor's high-depth RGB24 compatibility arithmetic: a native Q15
RGB-to-Q6 intermediate followed by the existing Q13 output transform. No external
scaler/codec runtime is added. VP9 sRGB metadata now supplies BT.709 primaries and
sRGB transfer alongside its identity matrix and full range.

The model and maintained benchmark select this profile consistently. Model
preflight recognizes declared identity color; selected decoding retains metadata
validation. Subsampled/limited-range identity input, incompatible reference modes,
HDR identity and unsupported RGB primaries remain explicit errors. This does not
qualify arbitrary wide-gamut or HDR RGB rendering.

Six AV1 and six VP9 clips cover odd/even geometry and 8/10/12-bit identity RGB.
All 144 native decoded pictures match independent FFmpeg planar hashes, including
color metadata. The independent 16-case RGB oracle spans four geometries and
8/10/12/14-bit codes, including every code value in the largest geometry.
Scalar/SIMD/Metal preparation matches the independent RGB raster through padded
strides, crop and rotation. Ordinary RGB and invalid-policy paths remain covered
by the broader preparation tests. All 12 coded clips and the corpus regenerate
byte-exactly; the independent pixel fixture and its provenance do as well.

The old native server rejects valid identity input with `UnsupportedVideoColor`.
Fresh native CPU, Metal and actual Linux ARM64 runs now preserve all 12 video
vectors, both queries, exact rankings and full top-1/5/10 overlap against pinned
PyTorch CPU. Each also passes observed reset cleanup/retry, legal half-close and
12 serial sustained requests. Linux uses the macOS PyTorch reference; this is
not fresh Linux PyTorch qualification. macOS passes 253 video checks (two skips)
and 52 model checks; actual Linux passes 226 video checks (29 platform skips).

These generated clips establish codec and reference-preservation behavior, not
application relevance or arbitrary real-world color conformance. The separate
encoded-pixel arithmetic prototype is a development diagnostic; production
claims rely on the native decoder, preparation and pretrained gates. Timings are
shared-host diagnostics, without a production throughput claim. Broader display,
codec/fuzz/load/performance and the remaining non-CUDA scope stay active.

See the [qualification receipts](../../zig/lib/video/testdata/gbr-model-validation/README.md).

### HLS date-range v2 follow-up

Delivered owned date-range/program timestamps, immutable augmentation,
END-ON-NEXT retention, v2 removals and combined blocking/delta delivery with
capability/freshness checks. Incomplete deltas fall back to full requests;
failed updates preserve committed cursor, metadata and shared admission.
Qualification: 153 media plus 10 production HTTP checks on macOS/actual Linux
ARM64 and 100,879 bounded fuzz runs. [Receipts](../../zig/lib/media/testdata/hls-daterange-validation/README.md)
bind sources/results and describe supported timestamp/protocol limits.
The broader non-CUDA plan remains active; this phase adds no model parity or
production-throughput claim.

### Strict completed load qualification on a full real clip

The maintained video harness now requires correct completion of every concurrent
request, declared bounded capacity retry, stable model identity, released
admission and completion accounting. It retains individual failures and denial
attempts, records executor queue versus request time and samples lifecycle
counters. An explicit normal-request deadline also covers reset retries and
half-close. Failed retries are included in denial totals; quiescence is separate
from server completions that clients did not acknowledge.

Thirty-seven Python contract checks pass. A fresh 52-second Sintel campaign
preserves completed baseline vectors/scores on native CPU, Metal, pinned PyTorch
CPU/MPS and actual Linux ARM64. Mac CPU/Metal pass all four concurrent requests,
reset cleanup/retry and legal half-close. Diagnostic clip times are about
66.12/14.64/14.55/10.89/137.67 seconds respectively; these are single measurements
with different HTTP/threading scopes, not normalized speed or production claims.

Linux's concurrency-two workload remains failed: two 180-second client timeouts,
one 16-attempt exhaustion and one correct result, with 28 declared denials.
Two server completions outlive client deadlines; final admission is zero. The
numerical baseline/lifecycle pass does not qualify this load. No memory/admission
or vector gate was raised. The retained measured harness predates an accounting-
only correction; raw results and current reanalysis are separately bound.
Portable CPU performance, deadline/overload recovery and broader sustained
qualification remain concrete gaps. GPU private-memory/transfer telemetry and
all broader non-CUDA requirements remain active. See
[receipts](../../zig/lib/video/testdata/concurrent-model-validation/README.md).

### Declared inference budgets and video deadline recovery

Standalone inference POST requests can declare one
`X-Antfly-Inference-Timeout-Ms` header (1–3,600,000 decimal milliseconds). Route
dispatch rejects malformed/duplicate values before admission and narrows any
existing application deadline. The budget reaches existing decode/preparation,
model loading/execution and cancellation controls. Python qualification requests
send their remaining per-attempt allowance, preserving one capacity-retry budget.

Fresh CPU, Metal and actual Linux ARM64 probes reject invalid metadata, return
one-second known-clip timeouts in about 1.05/1.01/1.20 seconds after resource
cleanup, then preserve the pretrained full-clip vector and model identity on a
correct retry. Forty Python contracts and two parser checks on each host pass.
These functional retries use explicit 600-second budgets; the earlier 180-second
native retry failure is retained. A cold Metal hard-deadline exit86/restart is
also retained as a failed initial probe. The existing unsafe-driver hard watchdog
is unchanged and can close a connection; full hard-stop retry and exact GPU-stage
cancellation remain unqualified. Counters are worker-generation scoped.

Stack sampling identifies active vision SDPA/portable sequential GEMM as a CPU
profiling target. Retry timings are shared-host diagnostics, with final CPU/Linux
runs overlapping. Application/transport timeouts are distinct; the previous
Linux concurrent-load failure remains failed. CPU SIMD/performance, overloaded
scheduling, broader codec/display/fuzz and the full non-CUDA scope stay active.
See [qualification receipts](../../zig/lib/video/testdata/deadline-model-validation/README.md).

### Bounded portable vision attention and ARM64 SIMD

Native bidirectional SDPA now schedules independent head slabs on the caller's
existing Io owner, with at most eight lanes and a cached CPU-budget bound.
Scratch is allocated before dispatch (at most about 512 KiB); workers do not
allocate or share output rows. Query/key tile boundaries check cancellation,
and all lanes drain before scratch or output is released. Short sequences and
callers without Io retain serial execution. ARM64 transposed GEMM uses a
four-wide portable Zig SIMD 4x4 tile, selected by paired shape measurements;
ordinary GEMM and other architecture dispatch are unchanged.

The 53-case linalg suite passes on macOS ARM64 (50 pass, three platform skips)
and actual Linux ARM64 (52 pass, one skip). It checks independent F64 GEMM
agreement, tails, f16 inputs, exact serial/parallel attention with masks and bias,
allocation rollback, cancellation/drain and retry. Fresh CPU servers preserve
pretrained real-clip vectors and pass reset/retry, half-close and concurrent
completion accounting. In a sequential before/after macOS comparison, the
52-second Sintel clip request falls from 60.96 to 40.17 seconds and the
four-request concurrency-two batch from 147.53 to 86.40 seconds. These are
small shared-host samples, not production throughput estimates. The optimized
Linux server also passes that four-request load gate within the unchanged
6 GiB envelope and 180-second per-request budgets; requests take 118–128
seconds. Its previous failed campaign remains historical evidence.

See the [source-bound attention qualification receipt](../../zig/lib/video/testdata/attention-model-validation/README.md) for raw measurements,
expanded-format model checks, reproduction and limitations. Broader sustained
load, hard-stop recovery, codec/display/fuzz coverage, transfer telemetry and
the complete non-CUDA objective remain open.

### Metal bridge telemetry for completed request qualification

The inference metrics route now exposes worker-process saturating atomic
counters for explicit buffer upload/download/copy bytes and calls, completed
runtime command buffers, command failures, GPU nanoseconds and missing GPU
timestamps. Shared memcpy and Private staging both contribute logical bytes;
copy-pair calls contribute two copies. Commands contribute timing only in their
completion handlers. Scraping adds no model lock, runtime ownership or GPU fence.
Disabled-Metal builds expose zeros without platform linkage.

The maintained video harness offers `--collect-metal-telemetry`: it requires
complete unsigned counters, rejects duplicates/invalid values and interval
regressions, records saturation, and retains before/after/delta for measured
requests, lifecycle checks and concurrent workload. Scrapes are outside HTTP
request timing. Existing parity, cleanup and completion-accounting gates remain
mandatory. Counters are process intervals, not per-request attribution; separate
preparation, direct kernel convenience allocations, mapped aliases and legacy
direct command buffers remain outside this instrumentation. Logical byte counts
are not measured bus traffic. Comprehensive GPU memory/transfer attribution and
the complete non-CUDA scope remain open.

See [source-bound receipts](../../zig/lib/video/testdata/metal-telemetry-validation/README.md) for qualification and reproduction.

The fresh actual Linux ARM64 PyTorch real-clip reference now also passes:
FFmpeg 9.0.2 reproduces the exact oracle RGB hash and selected IDs, and five
pretrained vectors preserve the macOS oracle (maximum coordinate error 5.72e-7).
The earlier FFmpeg 7.1.5 run failed video parity with differing decoded RGB and
is retained. The reference harness now rejects recorded pixel/presentation
mismatches before expensive model execution. The separate reference image/context
recipe pins source/package identities and includes no model weights or credentials.
Direct comparison with the fresh Linux oracle also preserves current Metal and
previously captured native Linux vectors; the latter is explicitly historical
binary evidence. Current native Linux execution passes the expanded-format suite.
No numerical gates were relaxed. Broader Linux formats/x86_64, codec/display/fuzz,
GPU memory/transfer attribution and the full non-CUDA scope remain open.

### Stateful decoder sequence qualification

The ordinary video test root now replays VP9 and AV1 packet sequences in
addition to individual reconstruction packets. Fifteen twelve-packet streams
cover depth/chroma variants, grain, scalable layers, Annex B and textured 480p
Sintel interframes. The harness retains four independently owned picture batches
and checks their geometry and SHA-256 bytes across subsequent decoding, skipped
output, cancellation, decoder failure and decoder destruction. Deterministic
selection/cancellation checks also verify materialized-output counts. Mutated
tapes have packet, pixel, allocation and control-check bounds; poisoned cores
are never reentered. These ownership and accepted-sequence gates execute in
ordinary CI on macOS and Linux, independently of instrumented fuzz campaigns.

Fuzz reports are recorded per explicit target with the exact harness snapshot,
fresh cache and actual run/coverage counts. The historical synthetic VP9 report
does not qualify AV1 or the later textured-picture harness. Bounded campaigns
add evidence; exhaustive codec conformance, broader independent sources and
sustained production fuzz coverage remain open. See the
[source-bound sequence receipts](../../zig/lib/video/testdata/sequence-fuzz-validation/README.md) for platform results,
independent picture oracles, seed attribution and reproduction. The complete
non-CUDA objective remains active.

### Include VP8 in stateful sequence qualification

Ordinary CI and instrumented fuzzing now include a VP8 packet-sequence target,
using the maintained pure Zig decoder and a textured twelve-packet 480p Sintel
seed. Twelve independent FFmpeg native VP8 picture hashes match exactly; skipped
outputs still update all interframe references and entropy, preserving every
later oracle picture. Cancellation of the second packet preserves the first
independently retained picture. Four retained outputs are checked across later
decoding, failure and decoder destruction, including padded rows. Live decoder
and retained-output allocations share a 32 MiB test budget, with bounded packet,
pixel and control work. Failed cores are not reentered. The full sequence suite
passes 13/13 on macOS ARM64 and in the pinned Linux ARM64 container.

The maintained qualification driver runs VP8, VP9, AV1 and Annex B separately,
and also fingerprints video/image Zig sources used by the VP8 path. A scratch
image packages the static x86_64 Linux executable for a separate architecture
emulation check; this is not native x86 hardware performance evidence. Actual
fuzz and architecture results are retained in the [source-bound receipt](../../zig/lib/video/testdata/vp8-sequence-validation/README.md).
Broader independent streams, sustained campaigns and the complete non-CUDA
objective remain active.

### DASH complete-segment availability offsets

Live DASH now accepts signed decimal/scientific availabilityTimeOffset values
with exact nanosecond arithmetic. The selected BaseURL path contributes each
applicable offset; nearer SegmentTemplate declarations override inherited values
and contribute once. The resulting upper availability boundary affects request
eligibility without changing presentation clocks, sequence IDs, retention expiry
or decoder configuration identity. Offset-only reloads preserve the live cursor,
transport retries and already retained packet leases.

INF supports finite advertised repeat/Period extents from availabilityStartTime;
it never invents an unbounded future timeline or makes data available before
that global instant. Existing segment/planning limits and checked int64 offset
sums apply. Sub-nanosecond nonzero offsets and availabilityTimeComplete=false
remain explicit unsupported policies. This is complete-segment request timing;
incremental partial-DASH delivery and broader live-origin qualification remain
in the active non-CUDA scope. See [source-bound qualification](../../zig/lib/media/testdata/dash-availability/README.md)
for exact-boundary, inheritance, live reload, cleanup and platform evidence.

### Incremental authenticated HTTP fragment delivery

The native HTTP adapter can now feed response body pieces directly into automatic
fMP4 framing and deliver complete fragments before the response finishes. Header
validation precedes publication. Callback backpressure bounds the producer;
FragmentDelivery.take transfers an independent immutable snapshot, while untaken
deliveries are released on callback return. Entered failures terminalize ingest
without implicit replay, preserving already taken snapshots. Pre-entry admission
denial does not enter transport. Exact ranges, cancellation and work ceilings
remain enforced. The retained framer capacity is reported independently of full
process memory and conservative transport admission.

Pending input grows geometrically within the existing byte ceiling, charging
actual old/new capacity overlap. Tight shared budgets can fall back to exact
required growth; allocation failures remain visible. One-byte input for a real
10,409-byte fragment uses fifteen allocations and 32,767 allocated bytes while
retaining identical coded bytes. A production authenticated chunked server
withholds its tail until the first fragment callback, making whole-response
buffering fail the gate. See [source-bound qualification](../../zig/lib/media/testdata/http-fragments/README.md)
for packet parity, ownership, range/failure/cancellation, growth and platform checks.
AvailabilityTimeComplete=false and direct model coordinator delivery remain
active implementation work. Resumable HTTP resources are described below.

### Prefix-verified HTTP replay and live resource growth

`http_stream.Adapter.streamFmp4Resuming` reconstructs automatic fMP4 framing on
an explicit retry while retaining a constant-space caller-owned FragmentLedger.
The ledger binds the resolved URL, byte range, initialization bytes and decoder
epoch. Ordered native DTS/PTS, duration, size, sync flags, fragment boundaries and
coded packet bytes commit the accepted prefix. A retry verifies that prefix
before delivering any new fragment. Changed or shorter completed prefixes reject
with SourceVersionChanged; cancellation, transport and admission failures retain
accepted progress. A completed body is idempotent until the caller explicitly
reopens it to poll an append-only live resource. Reopening preserves attempts,
work charges and accepted-prefix verification. Snapshot identities bind the
resource configuration without storing authentication query strings in them.

Callback success acknowledges delivery. `FragmentDelivery.acknowledge` permits a
consumer to record acceptance before a subsequent fallible side effect. Taking a
snapshot transfers ownership independently: a taken but unacknowledged delivery
can recur on retry, and the caller must reconcile or release that earlier owner.
The ledger is single-consumer in-process state; durable effects still require
caller-managed acknowledgements. Shared admission, byte/request/fragment/attempt
ceilings, cancellation and producer backpressure remain active. HTTP EOF completes
a body version; higher-level manifest segment completion is still caller work.

Portable tests verify full MP4 packet-byte/timestamp parity after retry, ownership,
acknowledgement boundaries, changed payload/native clocks/init/range/epoch,
shortened resources, cancellation, reentrancy, work limits, allocation failure and
unchanged/appended completed-body polling. A production authenticated chunked
server closes without the terminating chunk after one accepted fragment, then
withholds the retry tail until the first new callback. Retry verifies and skips
one prior fragment and publishes five new independent snapshots. See
[replay qualification](../../zig/lib/media/testdata/http-replay/README.md) for final sources, executable hashes,
raw logs and macOS/actual Linux ARM64 execution.

Manifest `availabilityTimeComplete=false`, Reader-selected resource cursors and
direct live model coordinator delivery remain active implementation work. The
complete non-CUDA objective stays open.
