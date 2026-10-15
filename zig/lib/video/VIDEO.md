# Native video decoding and inference

`lib/video` owns native codec reconstruction, logical-picture indexing, bounded
frame selection, surface lifetime and CPU/Metal preparation. It consumes
[lib/media](../media/MEDIA.md). Inference adapters own model geometry, tokens,
normalization and embedding policy. Runtime codec paths use maintained Zig source
or explicit Apple platform APIs; FFmpeg, libvpx and dav1d are offline reference
tools only. CUDA/NVDEC remains planned.

See [the completion audit](../../../docs/plans/native-video-noncuda-audit.md) for
qualification results, reproduction and limitations. This document describes the
current contracts; historical progress and raw reports live in Git history.

## Decoder routes

| Route | Qualified contract |
|---|---|
| Portable MJPEG | Complete baseline 8-bit JPEG samples from MP4/MOV; pure Zig image decoding. |
| Portable H.264 | Bounded avc1/avc3 tool subset described below; native-depth semiplanar output. |
| Portable VP8 | Versions 0–3, stateful interframes and hidden references; planar owned output. |
| Portable VP9 | Profiles 0–3, 8/10/12-bit 4:2:0/4:2:2/4:4:4, interframes and superframes. |
| Portable AV1 | Profiles 0–2, 8/10/12-bit monochrome and 4:2:0/4:2:2/4:4:4, interframes, show-existing and film grain; low-overhead and Annex B framing. |
| Apple VideoToolbox | Eligible static progressive limited-range avc1 8-bit 4:2:0; fixed-format VP9/AV1 WebM 8/10-bit 4:2:0 with one visible picture per packet. Library hardware-only default; model automatic selection explicitly permits Apple software. |

Compile-time [capabilities](src/capabilities.zig) distinguish built routes from
runtime hardware/profile availability. Unsupported tools fail explicitly; the
table is bounded qualification, not universal codec conformance. The maintained
VP9/AV1 ports, upstream identities and licenses are documented in
[PORTING.md](src/codecs/PORTING.md) and [third-party notices](THIRD_PARTY_NOTICES.md).

### H.264 reconstruction and logical pictures

[h264.zig](src/h264.zig) and its reconstruction modules support multiple I/P/B
slices, CAVLC/CABAC, Intra4x4/8x8/16x16, transforms and custom scaling matrices,
weighted prediction, quarter-pel motion, spatial/temporal direct, deblocking,
reference reordering and frame/field MMCO. POC types 0/1/2, progressive FMO maps,
ASO and redundant copies are supported. Redundant slices do not recover missing
primary slices. Missing/overlapping primary macroblocks fail.

The declared profile subset includes Baseline/Main/High, High 10, High 4:2:2,
High 4:4:4 Predictive and the documented Extended tools. Samples are 8–14 bits,
subject to profile constraints, with monochrome/4:2:0/4:2:2/4:4:4 and separate
colour-plane reconstruction. Unequal component depths outside monochrome fail.
SP/SI is qualified for 8-bit 4:2:0. Data partitions require category A first;
B/C can arrive in later transport packets before the next picture/configuration
boundary. Missing, duplicate, orphan or ambiguous partitions fail.

MBAFF supports mixed frame/field macroblock pairs. PAFF accepts complementary
fields together or split across packets, including bottom-first pairs and
field-specific marking. A separately qualified assembly extension buffers pairs
across unrelated coded pictures; this extends H.264 consecutive-access-unit
ordering and requires unique validated picture identities. Prediction snapshots
remain frozen while a field is unfinished. Complement completion updates shared
storage with copy-on-write when retained predictions still reference it.

No incomplete field is published. Packet membership identifies actual picture
members rather than an index range. Woven PTS is the minimum member PTS and
duration spans member intervals; unrelated intervening pictures retain their
own identity. Output remains interlaced samples; no deinterlacing is implied.

[h264_dynamic.Session](src/h264_dynamic.zig) bounds parameter registries and
decode epochs. Layout/POC-compatible SPS updates can retain references;
incompatible geometry/depth/chroma changes require a complete intra picture
with an empty DPB. These non-IDR transitions are qualified extensions rather
than arbitrary SPS-sequence conformance. PPS updates occur between complete
pictures; transitions during pending assembly or prediction from unavailable
references fail. Frame-number gaps infer bounded non-existing references and
never invent missing sample data.

`decodeFrame` returns an owned native semiplanar frame. `Frame.nv12` is a legacy
name: inspect bit depth, chroma and byte strides through `Frame.host()`. Samples
above eight bits are right-aligned little-endian u16. `decodeSelected` shares a
bounded DPB and lends selected pictures to a callback; copy or prepare them before
returning. `software.prepareWindows` returns independently owned host patches.
Defaults bound dependency spans to 256 packets, references to 16, pending pictures
to 16 and slices to 256 per assembled picture. Pixel, payload, work and actual
allocator high-water limits remain independent. Progressive references share
immutable samples with transactional copy-on-write.

### VP8, VP9 and AV1 sessions

[vp8.Session](src/vp8.zig) consumes coded packets, including hidden updates;
returned frames own their samples independently. VP8 state updates commit after
successful reconstruction/allocation, preserving retry on failure. The older
random-access `decodeFrame` entry point remains keyframe-only.

[codecs.Session](src/codecs/runtime.zig) consumes sequential VP9/AV1 packets and
returns owned batches of visible pictures. Hidden updates retain references.
VP9 superframes drain all visible outputs. Each output inherits caller PTS and
duration; callers needing distinct timestamps must supply framed access units.
Outputs can outlive the session. Internal codec workers are disabled; separate
sessions can run concurrently.

AV1 `av1_framing` selects low-overhead OBUs or Annex B. Nested Annex B sizes and
structure are validated before core entry and normalization is budgeted. WebM
requires low-overhead framing. Explicit operating-point selection rejects an
unavailable index. `av1_all_layers=false` selects the highest spatial layer;
supply complete temporal units to establish that policy. Excluded temporal
layers do not create timestamps or pictures.

Packet, pixel, core-memory, output-byte and output-count limits include skipped
outputs. Preflight/admission errors remain retryable. A failure after VP9/AV1
core entry invalidates/releases the session; recovery creates a new session and
replays from synchronization. The explicit recovery walker reports gaps;
ordinary model inference remains strict.

## Selection, seeking and windows

[sampling](src/sampling.zig) provides pinned processor FPS/index sampling and a
separate native timestamp policy. Timestamp selection chooses the earliest PTS
at/after each target in a half-open interval, breaks ties by decode identity and
deduplicates pictures. Source presentation time remains distinct from raw media
time. Logical indexing collapses fields, slices, partitions and separate planes;
transport packets are not automatically model frames. VFR and reordered pictures
preserve presentation identity.

[decode_plan](src/decode_plan.zig) uses container sync hints only as candidates.
Nonzero AVC starts require validated IDR payloads; ordinary I pictures,
recovery-point SEI and open-GOP flags are insufficient. Probe leases are reused,
search/read/work bounds apply separately and overlapping dependency runs merge.
From-start decoding remains the differential oracle. WebM header indexing and
codec-verified replay drain dependencies while materializing selected outputs;
output ordinals preserve layered/superframe and invisible-container identities.

[windows](src/windows.zig) owns source-bound mappings from requested windows to
unique selected pictures. Reader-backed model selections bind track/display
metadata, packet descriptors/payload and sampling policy by hash. Projected-frame
reuse is bounded and request-local. Changed dependencies, display metadata or
unproved preroll eviction invalidate reuse.

[live_windows](src/live_windows.zig) and [live_coordinator](src/live_coordinator.zig)
own published packet windows through source release and preroll eviction. Pending
field/slice/partition barriers cap automatic and explicit watermarks. Completed
earlier windows may proceed; intervals crossing unfinished pictures fail.
Persistent AVC validation retains admitted decoder/configuration state and
independent checkpoints. Failed delivery retains output for retry; commit follows
successful delivery. Discontinuities drain epochs and report gaps explicitly.

## CPU and Metal preparation

[preparation](src/preparation.zig) converts declared native-depth/chroma surfaces
to the model RGB8 policy, resizes and packs patch-major floats. CPU is the portable
reference; measured integer RGB paths use Zig vectors with scalar tails. Resize
policy is explicit: the model uses Torchvision antialiased bicubic, while the
library also exposes its Pillow recipe. Geometry, centering, matrix/range and
chroma conventions participate in policy and cache identity.

Metal imports eligible CoreVideo NV12 planes without host pixel materialization.
Native integer R8/RG8 or R16/RG16 textures and host-plane staging support
monochrome and 4:2:0/4:2:2/4:4:4. Uncompressed P010 import is separately qualified;
it does not establish high-depth compressed VideoToolbox decoding. Native
high-depth samples retain depth through decode and quantize under the declared
RGB preparation policy.

The preparer caches coefficients per rotated source/target geometry and resize
policy. A hit avoids coefficient regeneration/upload and rechecks scratch limits;
replacement preserves buffers retained by submitted commands. Color/range and
centering remain command parameters where they do not alter coefficients.

`Prepared` owns imports, commands, intermediates and output. `wait` checks caller
control while polling; destruction fences submitted GPU work even after
cancellation. `releaseSource` drops completed inputs/intermediates while retaining
output. The completed buffer is borrowed by the next resident consumer;
`readback` is explicit. The bounded preparation queue defaults to two, maximum
eight, with separate imported-plane and retained-output byte caps.

Display policy handles orthogonal rotation, SAR and validated WebM crop/display
metadata. Unsupported transforms/conflicting declarations fail. PQ/HLG uses an
explicit BT.2020/BT.709, 100-nit exposure, global Reinhard shoulder and sRGB/RGB8
policy; HLG assumes 1000-nit display, gamma 1.2 and zero black. Mastering/dynamic
HDR metadata does not determine exposure. CPU float64 transfer generation and
cached Metal transfer tables preserve the declared policy; qualification is
bounded to the reference inputs, not arbitrary HDR content.

## Inference, admission and telemetry

[Model-aware media chunking](../../../docs/plans/model-aware-media-chunking.md)
adds opt-in long-video retrieval windows through `/ai/v1/chunk`; `/embed` retains
one vector per explicit group. Chunk windows preserve cadence, half-open source
clocks, VFR holds and exact selected picture IDs. MP4 retains its index and verified
preroll/assembly context; WebM reuses a logical index and verified restart points.
Request-local projected reuse and bounded preparation overlap apply on CPU/Metal.
Descriptors, model identity, gaps and terminal coverage travel with the vectors.
Overview sampling remains distinct from retrieval coverage; audio inclusion stays
opt-in. See the design for source references, admission bounds and qualification.

`pipelines/embedding_gemma2_video.zig` connects logical MP4/WebM/normalized TS
pictures to the pinned default 1 FPS, uniform 32-frame policy and explicit sampled
windows. Video uses 140 pooled 16x16/3x3 soft tokens per frame, distinct from image
geometry. Mixed text/image/audio/video preserves order and one embedding per
group; the total sequence bound is 8192 tokens. Video audio is not implicitly
embedded. Equal-geometry frames batch in pairs with bounded projected reuse.

Metal retains vision/projector results, token composition, backbone inputs and
final pooling/normalization until response readback. EmbeddingGemma 2 selects
full-F32 tiled vision attention, preserving float inputs and softmax probabilities.
CPU validation and response serialization remain host operations.

The provider retains 114 eligible immutable vision/projector F32 matrices.
Vision/language each have a 640 MiB ceiling and share model/process admission and
a combined 512-entry bound. Relevant pressure yields idle vision weights, then
optional language weights only if the unchanged request remains denied. Each
class is attempted once with nonblocking native locks; no lock spans admission.
Active/submitted frames, aliases and shared zero biases cannot be discarded.
Unified-memory BF16 widens directly into final shared storage using Zig SIMD.
Owned vision frames retire activation views after their existing wait; outer
frame owners keep their own completion boundary.

Shared media/model admission covers source, decoder, preparation and retained
outputs with actual construction growth where implemented. Preparation starts
before the model gate and overlaps admitted execution. Cancellation joins/drains
producers before releasing resources. Forced/permanent/unrelated denials, request
reservations, process limits and external retry policy retain their contracts.

Runtime Metal telemetry separates owned private/host-visible allocatedSize,
logical aliases, initialization bytes and explicit transfers; command timing
measures completed GPU execution. These gauges exclude preparation textures,
decoder surfaces and opaque driver storage. They are not physical residency,
bus traffic or whole-pipeline memory.

## Verification

Use the repository Zig toolchain. From the repository root:

```sh
zig build --build-file zig/lib/video/build.zig test-video test-live-windows -Doptimize=ReleaseSafe
zig build --build-file zig/pkg/inference/build.zig test-video-model -Doptimize=ReleaseSafe
python3 -m unittest discover -s tools/embeddinggemma2 -p 'test_*.py'
```

Portable Linux execution, independent per-picture goldens, retained-frame fuzz
seeds, attribution and generators remain in [testdata](testdata/README.md).
[Qualification and reproduction](../../../docs/plans/native-video-noncuda-audit.md)
records platform/source scopes, final production checks and benchmark limitations.
Write generated run output under ignored `.benchmark-results/`.

## Optional Apple decoder selection

`decoding.Options.policy` selects `automatic`, `portable`, or `apple` independently
of CPU/Metal model execution. `embedding_gemma2.Options.video_decoder` propagates
that policy through synchronous encoding and preparation workers. Optional atomic
`Telemetry` counters distinguish portable, Apple hardware and Apple
software routes and capability fallbacks. Request-local projection cache keys
isolate explicit decoder policies. The standard release retains Zig decoders.

Automatic selection tries the eligible Apple path and falls back for unavailable
sessions or unsupported layouts before any picture reaches the consumer. Native
corruption, cancellation, allocation/admission failures and consumer failures do
not trigger replay. Once a consumer has been called, a later native failure is
returned; partially published model work is never silently replayed or mixed.
Portable replay reconstructs from the original verified dependency boundaries.

Apple WebM replay binds surfaces to the existing header index's logical picture
IDs and starts at a verified key packet, restoring AV1 sequence OBUs when needed.
It retains NV12/P010 surfaces through callback completion, imports them directly
into Metal, and maps them only for CPU preparation. Format/color changes,
monochrome/4:2:2/4:4:4/12-bit output, multi-frame VP9 superframes, multiple visible pictures per
packet, AV1 multiple operating points and film grain currently use Zig. The
Apple bridge requests hardware preference and checks actual session receipts;
codec availability probes alone do not establish stream acceptance.

Release Apple Silicon artifacts explicitly target macOS 15.0 and carry matching
`macosx_15_0_arm64` wheel tags. The release SDK remains pinned at 15.5. CI compiles
against that SDK and executes decoder/CPU model compatibility on macOS 15 and 26,
recording hardware and actual decoder receipts. Unavailable codec tests are skips,
not successful decoder qualifications. These jobs must pass before claiming
qualification on those hosts; local hardware results establish only the tested
Mac. Hosted VM capability skips do not establish GPU qualification; a manually
selected self-hosted Apple Silicon job executes the full Metal suite. Intel builds
and macOS releases below 15 are not release-qualified here.
