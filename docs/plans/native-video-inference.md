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
TS partials, CBC byte-range chaining, delta playlists, rendition coordination
and model-level live window orchestration still require implementation and
qualification. The full non-CUDA objective remains active. Codec source ports
are owned and maintained in-tree; no runtime codec-library dependency is allowed.
