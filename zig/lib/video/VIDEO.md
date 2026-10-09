# Native video decoding and sampled surfaces

Status: phase 1, the independent phase 2 decoder/preparation library, the
independent phase 3 scheduling subset, phase 4 portable MJPEG lane, and
software-decode-to-Metal preparation, resident resize coefficients, shared
admission, synchronized benchmarks and a pure Zig H.264 subset are implemented,
2026-10-08. EmbeddingGemma
2 model-token integration, HTTP/SDK video inputs, and resident vision/backbone
execution remain pending. Tim's PR #1014 is kept separate as requested; its model
code is not incorporated into this branch.

Related documents:

- [Shared media containers and timelines](../media/MEDIA.md)
- [Image codecs and preprocessing](../image/IMAGE.md)
- [Audio codecs and PCM](../audio/AUDIO.md)
- [Native video inference implementation plan](../../../docs/plans/native-video-inference.md)

## Goal and boundary

`lib/video` decodes qualified AVC container packets into timestamped, leased surfaces and
execute explicit frame-selection plans. It owns codec capability negotiation,
reference-picture lifetime, presentation reordering, and bounded decoder work.
It consumes `lib/media`; model adapters own vision budgets, tensor layouts,
normalization, media tokens, and embedding policy.

The fastest native path uses dedicated hardware decoding and device-resident
preprocessing. Pure Zig container parsing and orchestration call platform APIs
in-process through narrow C ABI bindings. This path has no FFmpeg subprocess or
libavcodec dependency, but its codec algorithms are supplied by the platform.
A pure Zig software decoder is a separate portable backend. Neither path should
be described as the other.

## Implemented frame selection

[sampling.zig](src/sampling.zig) implements EmbeddingGemma 2 frame-index/FPS
selection with uniform or truncate overflow and an output allocation bounded by
the cap. Missing FPS/duration follows the inspected processor's fallback. The
separate native timestamp policy selects the earliest PTS at or after each target
inside a half-open clip interval, resolves ties by decode index, and deduplicates
selected pictures. It preserves presentation order for reordered/VFR packets.

Seventy-seven golden cases compare exact indexes against a checked-in Hugging Face
method snapshot executed with NumPy 2.4.4. The receipt pins the method by SHA-256;
a remote commit lookup was unavailable. This qualifies the inspected snapshot,
rather than every Transformers release. Large-candidate, allocation-failure,
invalid-policy, cancellation/deadline, and VFR tests enforce bounded behavior.
See [fixture provenance](testdata/README.md). Run `zig build test-video` from
`zig/` with Zig 0.17. The sampling module does not decode pictures or return embeddings.

## Implemented decoder and preparation boundary

The public `antfly_video` module exports `sampling`, `avc`, `apple`,
`preparation`, `decode_plan`, `windows`, `apple_jobs`, `mjpeg`, `mjpeg_metal`, and
compile-time
`capabilities`. Its own `build.zig` supports
`test-video` and `check-video`; root and inference builds register the same tests.
`build_support.attach` takes the consumer's shared media and image modules to
reuse their types and controls without compiling a file into two Zig modules.
`check-video` also compiles a consumer that imports these libraries together.

[apple.zig](src/backends/apple.zig) orchestrates VideoToolbox through narrow
[platform bindings](src/backends/apple_video.m). `decodeSelected` accepts a media
MP4 reader, unique decode indexes in the desired output order, and budgets.
By default it decodes from packet zero through the latest selected index and
retains only selected NV12 pictures. Explicit `seek_mode=verified_idr` uses the
qualified dependency planner described below. It copies source PTS, duration, timescale, display matrix,
pixel aspect, and raw color metadata into an owned batch. Frame surfaces and
metadata survive reader destruction. Non-IDR open-GOP recovery points never
authorize a skipped dependency region.

Each call owns its session. Native sample buffers copy packet bytes into owned
CoreMedia blocks so source leases release after submission. Decode uses neither
asynchronous nor temporal flags; callbacks complete before submission returns.
Success drains delayed output; every failure and cancellation drains callbacks
and invalidates the session before releasing callback state or retained frames.
The original control/deadline is checked between reads, packets and callbacks.
The native drain itself is a blocking platform call, not a preemptible deadline.

Hardware is required by default, using a real CFBoolean specification and a
post-decode hardware-property check. Explicit `require_hardware=false` permits
platform software decode and records the actual route in `Batch.hardware`.
The initial lane accepts static `avc1` Baseline/Main/High 8-bit 4:2:0 configuration;
malformed lengths, unsupported profiles, and in-band parameter-set changes fail
before packet submission. CoreMedia-derived SPS geometry must match container
geometry before session allocation; dynamic output geometry is rejected.
This qualifies the checked-in progressive fixtures, not every legal profile tool.

Selected-frame count, packet bytes/work, retained native plane bytes, source
pixels, and preparation allocations have limits. Decoder picture-pool admission
uses a conservative estimate; opaque OS decoder workspace is not charged through
the Zig allocator. Queued imports and outputs have separate limits; atomic
admission across independent decode/preparation requests is implemented below;
model workspace admission remains separate. `Surface.fromBorrowed` retains an existing
CoreVideo pixel buffer without copying. `Surface.map` is an explicit host lease.

`MetalCache` accepts the inference backend's existing `id<MTLDevice>` and imports
the NV12 planes into R8/RG8 textures without host pixel materialization. Imports
retain pixel buffers and texture wrappers independently of decoder/cache owners;
the consumer fences GPU work before releasing them.

[preparation.zig](src/preparation.zig) provides portable `HostSurface` NV12 input,
CPU `referenceHost`/`reference`, and Metal `submit`. Callers supply target geometry,
rotation and an explicit BT.601/BT.709 SDR matrix; surface format determines full
or video range. The eventual model adapter must resolve color metadata, chroma
siting, pixel aspect, and geometry policy. HDR, arbitrary display transforms and
color-policy inference are not qualified by these APIs. Chroma sampling currently
uses the explicit nearest 4:2:0 cell convention shared by CPU and Metal paths.

The Metal kernels convert sampled NV12 cells to quantized RGB, apply two-pass
Pillow bicubic resizing with shared 22-bit integer coefficients and byte clipping,
and pack patch-major `[patch_y, patch_x, channel]` float values. Dimensions are
multiples of 48 for patch size 16/pooling 3; the default budget is 140 soft tokens.
Inputs can remain in `[0,1]` or be centered to `[-1,1]` for the vision patch linear.
Target geometry is supplied by the pinned processor adapter, not guessed here.
The bounded horizontal RGB intermediate stays on the GPU; no full-resolution
host RGB image is constructed by `submit`.

`Prepared` owns its surface import, command and Metal patch buffer. `wait`
checks caller control while polling with `std.Io`; destruction fences in-flight
work even after cancellation. `buffer` is a borrowed `id<MTLBuffer>` after
completion for the next resident consumer. `releaseSource` discards completed
input imports, commands and intermediates while preserving the output; it is
idempotent after completion. `readback` is an explicit reference/
debug path. This library handoff does not yet execute a vision tower or embedding.

Qualification includes independent FFmpeg NV12 comparisons (maximum byte error
2), a hardware-only 640×360 B-frame fixture, CPU/Metal resized patch comparisons
across four rotations and both SDR matrices (one byte per channel after resizing),
full-range colored borrowed planes, lease/queue lifetime, cancellation/retry,
retention limits, and allocator-failure campaigns. Shader values allow floating
point tolerance while resize coefficients and byte clipping are shared. Fixture
hashes and offline tool provenance are in `testdata/decode-oracle.json`.

From `zig/lib/video/` with Zig 0.17:

```sh
zig build test-video -Doptimize=ReleaseSafe
zig build check-video -Dtarget=x86_64-linux-musl -Doptimize=ReleaseSafe
zig build test-video -Dtarget=wasm32-wasi -Doptimize=ReleaseSafe -fwasmtime
```

Native macOS decode/GPU tests require access to platform services. Hardware-only
qualification skips when the platform cannot create the requested decoder;
portable-target tests explicitly skip Apple routes and test unavailable errors.
Linux has packet indexing, frame selection, CPU preparation and the pure Zig
MJPEG and the qualified pure Zig H.264 subset below. NVDEC/CUDA remains planned. Apple frameworks
and Objective-C sources are omitted from non-macOS builds.

## Implemented independent scheduling

[decode_plan.zig](src/decode_plan.zig) builds bounded, ordered decode runs for
unique selected packet indexes. The portable planner probes container sync hints
and validates length-prefixed AVC NALs. A nonzero run starts only at a sample
containing IDR slices and no ordinary VCL slices. Container keyframe flags,
ordinary I pictures and recovery-point SEI alone are insufficient. Full bitstream
syntax remains the decoder's responsibility; this is a conservative static-avc1
planner, not a general H.264 dependency parser. Missing usable hints fall back to
packet zero. `from_start` remains the reference/default for `decodeSelected`.

Probe candidates, aggregate probe bytes, individual packet bytes, backward search
steps, selections and submitted packets are bounded independently. Accepted probe
leases are reused during submission, so those packets are not read twice. Plans
are move-only and retain source leases until `deinit`; providers must outlive them.
Overlapping dependency ranges merge. `merge_gap_packets` optionally bridges small
gaps; its default zero minimizes packet submissions, without claiming optimal
latency on every device/source. The native session drains between disjoint runs
and the next verified IDR resets references. Source timestamps remain unchanged.
Batches report submitted/skipped packets, probe bytes and peak retained plane
bytes. `max_decode_packets` applies to actual planned submissions.

[windows.zig](src/windows.zig) applies the native timestamp policy to bounded
half-open clip windows, preserving each window's presentation order and mapping
it to a request-local union of unique picture indexes/PTS. The owned plan records
hashed immutable source identity and AVC configuration, track ID and timescale.
Reuse is limited to one source/track/request and one preparation policy. It is
not a persistent URL cache, model FPS parity claim or vision-token cache. Empty
windows fail explicitly; window count, selections and unique pictures are capped.

`apple.decodeTo` sends borrowed selected frames to a sink outside the native
callback, releasing each surface after the sink returns. Consumers retain or
import pictures they need longer. Errors/cancellation drain the decoder before
callback state is freed. It returns an owned metadata/counter batch with no
retained frames; sink order follows decoder delivery, not window presentation.

[apple_jobs.zig](src/apple_jobs.zig) connects that stream to the caller's existing
`preparation.Metal` device/queue. `prepareWindows` prepares each unique picture
once and returns completed Metal patch buffers plus owned window mappings, PTS,
source stamp and decode metadata. Window entries index the shared output array.
No host pixel readback occurs in this API; explicit `Prepared.readback` remains
available for testing. Geometry/color policy is supplied once for the request.

The preparation queue defaults to depth two, with a hard maximum of eight and a
separate actual imported-plane byte cap. When full, the producer waits for the
oldest command, releases its source import/command/intermediates, then continues
decode. One additional borrowed decoder output may exist while this wait runs.
All completed output buffers remain owned by the result under a checked total
output-byte cap. Queue depth and imported-plane byte high-water marks are
reported. Cancellation or allocation/consumer/GPU errors fence submitted work
before releasing results. Limits describe library-owned leases and admission
estimates; opaque OS decoder/texture-cache workspace and concurrent model
reservations are not included in a whole-process memory guarantee.

Qualification uses original closed/open-GOP fixtures with hashed FFprobe
receipts. Native range-backed sparse decoding produces exact baseline pixels:

| Closed-GOP input: 60 pictures; select packet indexes 12 and 59 | From start | Verified IDR |
| --- | ---: | ---: |
| Decoder submissions | 60 | 13 |
| Payload reads, excluding container indexing | 60 | 13 |
| Payload bytes, including qualification probes | 61,067 | 14,137 |

The open-GOP fixture requires all 60 packets. These are tested work/read counts,
not latency or end-to-end embedding benchmarks. Overlapping windows reuse two
of eight selections (six unique outputs), with CPU/Metal value comparisons,
depth-one/two bounds, source teardown, late cancellation/retry, sink failures and
allocation-failure campaigns. Portable planner tests execute on WASI; Linux
compiles the planner, window reuse and CPU preparation without Apple dependencies.
The expanded Linux H.264 subset below is implemented; remaining profile tools and CUDA/NVDEC remain stage 4 work. Model-specific tokens,
resident vision/backbone/pooling and retrieval parity still depend on PR #1014.

## Implemented portable MJPEG lane

[mjpeg.zig](src/mjpeg.zig) decodes complete JPEG samples from static MP4/MOV
`jpeg` visual entries using `lib/image.jpeg` in-process. `Track.codec` distinguishes
AVC from MJPEG; AVC-only planning/Apple decode fail explicitly for MJPEG. The
container reader indexes the same sample tables/timelines and distinguishes the
track handler from QuickTime's additional data handler. MJPEG entry version zero,
one picture per sample, progressive field metadata and stable geometry are
required. AVI, abbreviated external JPEG tables, paired/interlaced fields,
progressive/arithmetic JPEG and dynamic geometry are excluded from this lane.

`decodeFrame` reads just the selected packet, validates one complete baseline
8-bit, one-scan, one- or three-component JPEG with included tables and a direct
EOI after entropy, and returns owned RGBA plus packet index/PTS/duration/timebase.
The JPEG decoder supplies its existing display RGB policy and chroma upsampling;
this is not the NV12 matrix conversion or a general color-management promise.
No prior packets, platform decoder or FFmpeg runtime are required. Frames outlive
the reader/source. Pixel and compressed-packet caps apply before decoding; an
allocator wrapper enforces actual live decode allocation bytes, including output
RGBA, and records their high-water mark. Allocator exhaustion remains distinct
from resource denial. Packet leases use the source's separate read/retention caps.

`mjpeg.prepareWindows` reuses the portable timestamp-window union, then decodes
and prepares one unique picture at a time. `preparation.referenceRgba` applies
explicit rotation, the shared quantized Pillow bicubic resize and patch packing;
its SDR matrix field is unused for already-decoded RGB. Output is a contiguous
host float array with `frame(slot)` slices and window-to-slot mappings, owned PTS,
source/codec stamp and copied display/color metadata. Unique-frame and total
output-byte caps apply before output allocation. The result records decoded
packets, payload bytes and decode allocation high-water; `reusedSelections()`
counts duplicate window references. Decode RGBA, preparation scratch and final
outputs have separate limits, not a global concurrent-request reservation.

The checked-in original 4:4:4 two-second fixture contains eight 64×48 JPEG
pictures. Every decoded RGBA channel matches independent FFmpeg output within
three byte levels, with exact packet clocks/positions and hashed provenance.
This comparison qualifies the fixture and declared JPEG display policy; it does
not establish subsampled-chroma parity with every FFmpeg conversion policy.
Ten overlapping selections prepare eight unique outputs/read eight payloads;
a sparse selected picture reads one payload. Tests cover reader/source teardown,
malformed/interlaced/progressive inputs, memory/work limits, deterministic
allocation failure, cancellation during decode/preparation, and retry. The full
portable decode/preparation suite executes on WASI and compiles for Linux.

This CPU route supplies host patches for a later model consumer. The Metal route
below stages software-decoded pictures on the caller's device. Neither runs an
embedding model. Additional H.264 tools, NVDEC/CUDA and resident model execution
remain pending independently or behind
PR #1014 as described in the implementation plan.

## Implemented software decode to Metal

`preparation.Metal.submitRgba` accepts exactly packed RGBA8 plus dimensions,
preparation options and original control. It copies input once into owned shared
Metal storage before returning, so the producer may overwrite/free its buffer
immediately. RGB is already decoded: alpha and the NV12 matrix option are ignored,
as in `referenceRgba`. The new horizontal RGBA kernel applies explicit rotation
and the shared 22-bit bicubic coefficients; the existing vertical kernel clips,
normalizes/centers and packs patches. Resize intermediates stay on the GPU.

Geometry, pixels, resize scratch, coefficient storage and
`max_host_staging_bytes` are admitted before native submission. The shared image
control follows coefficient generation; native allocation/copy/commit remain
synchronous platform calls. `Prepared` owns the staging buffer, command and output.
`wait` checks control; `releaseSource` frees completed input/command/intermediates
while retaining output. Destruction fences pending work even after cancellation.
No host pixel readback occurs during submission. `buffer()` remains the borrowed
completed Metal-buffer handoff; a later inference consumer must retain/fence its
own use before releasing the result.

`Prepared.rgba_staging_bytes` and `coefficient_staging_bytes` count logical input
bytes supplied to `newBufferWithBytes`. The NV12 import route reports zero RGBA
staging. These counts exclude command parameters, allocator/driver overhead and
physical transfer behavior; shared-memory staging is not a PCIe/DMA measurement
or a zero-copy claim. `readback` remains an explicit test/debug operation.

[mjpeg_metal.zig](src/mjpeg_metal.zig) connects pure Zig MJPEG decode to this
preparer on the caller's existing Metal device. `prepareWindows` shares one
preparation policy across the request-local picture union. The decoder works on
one host frame at a time while prior Metal commands may run. When capacity is
full it fences the oldest command and releases its staging before submitting
another. Depth defaults to two with hard maximum eight; individual input staging,
aggregate in-flight staging, total staging work and retained output bytes have
separate caps. At most one decoded host frame exists while waiting for capacity.
Per-command scratch bounds plus queue depth bound the resize/coefficient work;
these are not whole-process or concurrent-model memory reservations.

The result owns completed unique-picture patch buffers, presentation-ordered
window mappings/PTS, codec/source stamp, copied display/color metadata, decode/
payload counts, decode allocation high-water, staging counts and queue/input
high-water marks. It outlives reader/source/preparer destruction. Errors after
partial submission join GPU work before releasing staging, outputs and leases.
Linux/WASI retain the CPU MJPEG path; Metal APIs fail closed there.

Native tests compare CPU/Metal values within `2e-6` across every rotation and
both centering modes, include nontrivial color/alpha patterns, destroy/overwrite
producer input immediately, and qualify queue depths one, two and eight. Byte
caps can force a depth-two queue to retain only one staged input. Late cancellation,
retry and deterministic allocation failure exercise partial-work cleanup.
For the original two-second fixture, preparing overlapping windows together gives:

| Work, excluding container indexing | Separate window jobs | Shared window job |
| --- | ---: | ---: |
| Decoded/prepared pictures | 10 | 8 |
| RGBA staging bytes | 122,880 | 98,304 |

Depth two retains at most 24,576 staged RGBA bytes; completed unique patch outputs
occupy 221,184 bytes. These are asserted logical work/resource counts, not latency
or embedding benchmarks. Resident vision/projector/backbone/pooling still await
PR #1014; PAFF field marking/standalone field packets and NVIDIA NVDEC/CUDA remain independent follow-ups.

## Implemented independent efficiency and portable H.264 work

`preparation.Metal` owns one resident coefficient geometry. The key is rotated
source width/height plus target width/height; color matrix, range and centering
remain command parameters because they do not change resize coefficients. A hit
reuses the uploaded tables without CPU coefficient regeneration or upload;
temporary CPU tables have already released.
A geometry change replaces the resident entry; submitted commands retain the old
buffers through completion. Cache hits still recheck the caller's scratch budget.
`clearCoefficients` explicitly evicts the entry. The default coefficient cap is
16 MiB; `Metal.admit(pool, cap)` reserves a persistent shared budget before use.
Destroying the preparer fences its queue before releasing that reservation.
`Prepared.coefficient_staging_bytes` now reports uploads for that submission:
zero on a hit. `Prepared.gpuSeconds` preserves completed command execution time
after input resources have released. No model or host readback is needed.

CPU, MJPEG/Metal and VideoToolbox/Metal window options accept a shared
`media.admission.Pool`. Outputs retain their reservations until result destruction;
transient decoder/preparation memory and command capacity return after completion.
Metal jobs require the preparer to have been admitted to the same pool. Reservations
cover configured conservative scratch/decode/queue bounds, source/cache/index
owners separately, and completed output bytes. They do not measure opaque driver
workspace or deduplicate borrowed host aliases. Size limits for the intended
geometry rather than relying on large defaults when configuring concurrency.
Admission denial occurs before packet decoding and can be retried by the caller.
Native worker tests qualify atomic contention; overlapping output lifetimes,
late cancellation, allocation failures and retry qualify release ordering.

`h264.decodeFrame` and `h264.decodeSelected` are pure Zig decoders available on
Linux, macOS and WASI. The declared static `avc1` subset accepts Baseline, Main,
High, High 10, High 4:2:2 and High 4:4:4 Predictive configurations. Sample depth is
8–14 bits with equal luma/chroma depth; profile-specific limits still apply
(High is 8-bit, High 10/High 4:2:2 are at most 10-bit). Chroma may be 4:2:0,
4:2:2 or 4:4:4. One SPS/PPS and stable geometry remain required.
Implemented tools include:

- Multiple I/P/B slices, with per-slice entropy, prediction availability and filter
  boundaries. Missing and overlapping primary macroblocks fail explicitly. Work
  includes redundant slices in the default 256-slice-per-packet limit.
- All seven progressive Baseline FMO maps, including dynamic map directions and
  explicit maps; ASO reconstructs by macroblock ownership instead of arrival order.
  Redundant copies are consumed when a primary picture exists; they do not replace
  it or provide recovery after a missing primary picture.
- CAVLC/CABAC, including field significance contexts, 4:2:2 DC codewords,
  independent 4:4:4 component contexts, CABAC I_PCM restart and bounded padding.
- Intra4x4/Intra8x8/Intra16x16 and chroma prediction, constrained intra prediction,
  4x4/8x8 transforms, custom SPS/PPS scaling matrices and both fallback rules,
  separate Cb/Cr QP offsets, lossless transform bypass and high-depth clipping.
- P/B partitions, skip, spatial/temporal direct, quarter-pel interpolation,
  explicit/implicit weighting, reference list reordering, sliding-window marking
  and bounded frame MMCO commands. POC types 0, 1 and 2 are supported.
- Mixed MBAFF frame/field macroblock pairs, with sample-accurate neighbours,
  field/frame motion scaling, parity-aware chroma motion, field scans, separate
  field POCs and mixed-boundary deblocking. Fields use views over woven storage.
- PAFF complementary field pairs carried together in an indexed packet. The
  first field is filtered and admitted as a reference before the second field;
  completion updates the same DPB allocation. Default field reference lists and
  field list reordering include previous and current-first-field references.
- Cropping, emulation prevention and full/video-range signaling. Output PTS and
  duration come from the indexed packet.

`decodeFrame` replays the dependency span from a verified IDR to the selected
packet and returns owned semiplanar Y + interleaved Cb/Cr with `Frame.host()`.
`Frame.nv12` retains its existing name: read `bit_depth` and `chroma_format` rather
than assuming NV12. Above 8 bits samples are right-aligned little-endian `u16`;
4:2:2 and 4:4:4 have their corresponding chroma dimensions. `Frame.host()` supplies
byte strides and native-depth metadata. `decodeSelected`
keeps one bounded decoded-picture buffer across the selected span, decodes each
packet once, and publishes borrowed native semiplanar pictures during a callback. Callback slots
retain the caller's selection order, including B-picture presentation order; the
callback must copy/prepare the picture before returning and must not destroy it.
`software.prepareWindows` uses this batch path to share reference reconstruction and
selected pictures across overlapping clips. It returns owned host patches independent
of Source/Reader/frame lifetime. Decode receipts count actual dependency packets and
payload bytes rather than only selected packets. The default dependency span is capped
at 256 packets and the SPS reference count at 16 pictures.

Geometry/packet/reference workspace bounds are checked before reconstruction;
`max_decode_bytes` also enforces the actual decode allocator high-water limit.
Reference samples, picture/slice metadata, FMO storage and both lists' motion metadata are included in the conservative
workspace estimate. Cancellation is checked during RBSP copying, macroblocks,
filtering and output rows. Shared output admission remains held until Frame destruction;
batch callback failure frees the output, decoded-picture buffer and packet leases.

This remains a declared tool subset. Standalone PAFF fields in separate indexed
packets, non-complementary pairs, PAFF adaptive/long-term marking, separate colour
planes, monochrome, unequal component depths, frame-number gaps, SP/SI, other
profiles, data partitions and dynamic parameter sets remain explicit rejections.
MBAFF frame MMCO remains available. No deinterlacing policy is applied: interlaced
output preserves woven samples. Capability `portable_h264_decode` is true with
`static-avc1-profile-subset-multislice-8to14bit-420-422-444-mbaff-paff-pairs`; callers
must preserve that qualification. No OpenH264, x264 or FFmpeg runtime is linked.

Native-depth host preparation validates plane geometry and converts into the
existing RGB8 processor policy before resizing/packing; decoded samples retain
full depth. Full-range conversion uses the actual sample maximum. Direct Metal
NV12 import and the Apple hardware configuration qualifier retain their existing
8-bit 4:2:0 contract. Additional Metal native-depth/chroma imports, CUDA/NVDEC and
model-specific execution remain separately qualified work.

Independent x264/FFmpeg vectors cover intra prediction, CAVLC/CABAC 8x8 transforms,
filtered/cropped/low-QP pictures, P/B pictures, spatial and temporal direct prediction,
fades with explicit P weights, implicit B weights, B-pyramid reference pictures and
frame-number wrap. Qualified pictures match FFmpeg NV12 byte-for-byte; oracle hashes,
encoder settings and presentation picture types are recorded in
[testdata/h264-tools-oracle.json](testdata/h264-tools-oracle.json). Weight rounding,
coincident-POC fallback, all six short/long-term marking operations, packet mutation,
dependency limits, callback cancellation and
exhaustive reference-allocation failures have portable tests. This is coding-tool
qualification on generated vectors, not a broad production-stream corpus. CAVLC and
CABAC constants come from pinned Cisco OpenH264 BSD-2-Clause tables; the revision and
license are in [third-party notices](THIRD_PARTY_NOTICES.md).

Current platform results are recorded in
[testdata/h264-advanced-validation.md](testdata/h264-advanced-validation.md).

The expanded fixture receipt is
[testdata/h264-advanced-oracle.json](testdata/h264-advanced-oracle.json). Independent
FFmpeg comparisons cover mixed MBAFF I/P/B pictures at 8/10 bits with CAVLC/CABAC,
filtering, multi-slice custom matrices, lossless 4:2:0/4:2:2/4:4:4 streams, paired
PAFF references/list reordering, spatial/temporal B prediction and compressed native 9/12/14-bit DC vectors.
Known-sample normative vectors cover FMO/ASO/redundancy and 11/13-bit output, which
FFmpeg's pixel formats do not represent. Qualification includes slice holes and
overlap, malformed scaling lists, both scaling fallback rules, separate chroma QP,
negative high-depth chroma QP, shared reservation resizing and exhaustive allocation
failure cleanup across fields, explicit maps and high-depth component buffers.
CABAC extension aliases and field offsets are regenerated from
[ITU-T H.264 (08/2021)](https://www.itu.int/rec/T-REC-H.264-202108-I/en); the generator
rejects duplicate/missing table rows before writing constants.


Codec work remains independent of EmbeddingGemma 2 tokens and model execution.

`zig build bench-video -Doptimize=ReleaseSafe` measures the checked-in MJPEG fixture.
`-Dbenchmark-input=/absolute/path/video.mp4` uses positional file reads and sampled
overlapping windows on an external static/fragmented MP4 or MOV. macOS selects
VideoToolbox/Metal for AVC or software JPEG/Metal for MJPEG; Linux selects the pure
Zig software route. One cold request and 20 warm requests report completed wall
latency, GPU execution sum, selected pictures, decode live allocation peak, logical
RGBA/coefficient uploads, device-wide allocated bytes and process peak RSS. The
summary reports warm p50/p95 and selected frames/s. GPU time excludes queue wait;
RSS is a process lifetime high-water mark, device allocated size includes other
device activity, and driver decoder workspace has no allocator-level live peak.
Wall timings include result cleanup and polling, so this is a decode/preparation
benchmark rather than an embedding or concurrent-serving throughput claim.
Dated raw receipts live under [testdata/benchmarks](testdata/benchmarks/README.md).

## Remaining module and API shape

Proposed modules are `src/mod.zig`, `decoder.zig`, `surface.zig`, `sampling.zig`,
`software/`, and optional `backends/videotoolbox.zig` and `backends/nvdec.zig`.
Backends compile only into supported targets; CPU-only builds must not require
Apple or NVIDIA frameworks. Bindings follow repository backend conventions,
with minimal platform shims only where the ABI cannot be represented directly.

| Concept | Contract |
| --- | --- |
| Capabilities | Codec, profile, bit depth, chroma, geometry, output formats, dynamic changes, and device interoperability supported by this backend. |
| Decode session | Owns codec state and reference surfaces; accepts packet leases, drains reordered output, flushes, resets, and closes. |
| Surface lease | Host planes or an opaque device resource with explicit retention, completion, and release. |
| Frame | Source PTS/timebase, duration when known, configuration generation, geometry, color/orientation metadata, and surface lease. |
| Sample plan | Ordered selected picture identities or timestamp targets plus the policy that generated them. |
| Decode plan | Valid random-access/preroll starts, dependency ranges, and selected output identities. |

Host planes carry format, strides, lengths, and plane geometry. Support planar
YUV and semiplanar NV12/P010 representations before forcing RGB. Device surfaces
carry backend/device identity, format, geometry, and synchronization information;
they must not expose an assumed host pointer. Color metadata includes matrix,
range, primaries, transfer function, and chroma siting. Rotation and pixel aspect
ratio participate in displayed geometry.

Decoded output is a lease, not a newly allocated RGB image per frame. Packet
leases live until the decoder finishes consuming them. Surface leases live
through the final asynchronous inference consumer, not merely until a submit
function returns. Codec references may retain a surface longer than the
application's output lease. Decoder close/cancellation must join callbacks and
drain or fence device work before freeing their context.

## Frame selection and dependency-aware decoding

Select pictures before pixel decoding whenever metadata permits. Sampling
policy is supplied by the caller: clip interval, FPS or selected indexes,
maximum output count, overflow strategy, and reference-compatible rounding.
Record the resulting actual picture identities and PTS for repeatability.

For model parity, reproduce the pinned upstream selection algorithm, including
its frame-index/FPS rounding; do not substitute a different timestamp algorithm
and call it equivalent. For native timestamp sampling, specify a separate
versioned policy for VFR, duplicate PTS, gaps, endpoints, and short clips.
Uniform overflow samples across the declared interval; it does not truncate to
the first portion. Unknown duration/index availability follows `lib/media`'s
explicit scan/spool boundary.

Map selected pictures to safe random-access starts. Merge overlapping dependency
ranges and process each range in decode order once. Codec-specific preroll and
reordering apply, including open GOP dependencies. A selected B-frame can require
decoding a reference picture with a later PTS. Sparse inference does not imply
sparse independent compressed-frame decoding. Skip conversion/vision execution
for unselected pictures, but reconstruct pictures needed as references.

For short clips or dense selections, a single sequential pass may be cheaper
than repeated seeks. For long sparse selections, a seek plan may save decode
work. Choose using a bounded cost estimate for packets, preroll, source ranges,
and session resets; expose the chosen route in telemetry. Adjacent indexing
windows should reuse a forward decoder and overlapping prepared frame tokens
when their identities and policies match.

## Backend strategy

### VideoToolbox → Metal

Start with qualified MP4/H.264 input on Apple. Configure a VideoToolbox
decompression session, validate actual decoder/output capabilities, and receive
CoreVideo pixel buffers. Import compatible YUV planes through
`CVMetalTextureCacheCreateTextureFromImage`; inference consumes those textures
without a full-resolution host RGB round trip. Retain buffers/textures until
Metal completion. Require hardware acceleration when the selected route promises
hardware decode; report a platform software route separately if permitted.

VideoToolbox is documented by [Apple](https://developer.apple.com/documentation/videotoolbox).
CoreVideo provides [Metal texture import](https://developer.apple.com/documentation/corevideo/cvmetaltexturecachecreatetexturefromimage(_:_:_:_:_:_:_:_:_:)).
Whether a particular format/device is copy-free must be measured, not inferred
from the existence of an import API.

### NVDEC → CUDA

Query codec/profile/geometry support and create the decoder in the inference
device's CUDA context. Consume decoded device surfaces directly in CUDA
preprocessing and avoid device-to-host pixel copies. Map/unmap lifetimes,
decoder reference/output pools, streams, and completion events are explicit.
Keep a separate mapping/consumption stage where blocking map calls would stall
decode. SDK-dependent opaque output is optional, not the baseline contract.
See the [NVDEC programming guide](https://docs.nvidia.com/video-technologies/video-codec-sdk/13.1/nvdec-video-decoder-api-prog-guide/index.html).

Device-resident decode does not establish EmbeddingGemma CUDA support. Inference
backend qualification is a separate prerequisite. A transfer to another device
or backend must be explicit, admitted, and counted.

### Pure Zig software

An MJPEG lane can first reuse `lib/image` JPEG decoding to exercise the complete
portable surface/inference path. It does not qualify H.264. Start H.264 with
progressive 8-bit 4:2:0 and an explicitly documented profile/tool subset; baseline
coverage alone does not cover CABAC/B-frame streams. The implemented Main/High
tool subset above adds those tools with independent vectors. Additional tools and
production-stream qualification remain incremental work.

The codec work includes NAL/configuration parsing, entropy decode, inverse
quantization/transforms, intra/inter prediction, motion compensation, in-loop
deblocking, reference-picture management, and output ordering. Use SIMD for
measured reconstruction hotspots and bounded persistent scratch. Full reference
pictures remain necessary even when the model needs a small resized output.
Reduced-resolution reference reconstruction cannot be treated as an exact
optimization. Reject unsupported interlace, chroma, bit depth, or tools before
advertising support. HEVC, VP9, and AV1 are independent later projects.

### Metal execution after either decoder

Metal preprocessing and inference are required in the first Apple rollout,
with their own [implementation track](../../../docs/plans/native-video-inference.md#metal-implementation-track).
VideoToolbox is the hardware decode backend; Metal is the preparation/model
execution backend. They are separate capabilities. Pure Zig software decode
can also feed Metal through bounded uploads of selected host YUV planes.

The resident route imports decoded planes, prepares reference-compatible patches
in Metal buffers, batches the vision encoder/projector, and passes device tokens
directly to joint backbone execution. Pooling and normalization stay on Metal;
only the final vector needs readback. Imported surface lifetimes extend through
the last consuming command completion. Inference owns the kernels, model weights,
scratch scheduling, and device tensor interface; this library owns the decoded
surface leases and import boundary. Qualify each decode → Metal combination
separately rather than treating hardware decode as proof of fast model execution.

## Preprocessing and numerical fidelity

The inference adapter should consume selected YUV/device surfaces and prepare
model-native patches directly. Avoid chains of full-resolution YUV → RGB →
resized RGB → CHW → patches. Fuse conversion, orientation, resize, rescale, and
patch packing only after proving fidelity to the declared reference path.

Conversion order, chroma reconstruction, range handling, 8-bit rounding,
antialiasing, and bicubic filter behavior all affect prepared pixels. A fused
floating-point operation is not automatically equivalent to reference conversion
through an 8-bit RGB image. Preserve reference semantics or expose and qualify a
distinct policy. Existing `lib/image` processing supplies reusable host routines;
device kernels belong beside inference backend execution, not inside container
parsers. HDR/tone-mapping behavior must be qualified explicitly or rejected.

## Scheduling, memory, and failure

Use bounded queues for packet input, decoded output, and prepared batches.
Overlap source reads, decode, preprocessing, and vision work with backpressure.
Reserve source buffers, parser/index memory, codec scratch, all reference/output
surfaces, tensor preparation, and model execution workspace before work begins.
Codec reference requirements set a minimum surface pool; a small output ring
alone cannot bound total decode memory. Shared memory aliases are charged once
by underlying allocation identity and retained until every owner releases them.

Limits cover source geometry, reference count, packet sizes, decoded pictures,
dependency/preroll work, total surface bytes, output count, and elapsed work.
Bound decoded dependency work separately from sampled output count. A 32-frame
request must not allow an unbounded full-video decode. Oversized or changing
streams require re-admission or an explicit rejection before new allocation.

Keep malformed input, unsupported capability, capacity denial, decode failure,
device failure, cancellation, and deadline expiry distinguishable. Capability
fallback occurs before producing results and must be observable; never silently
return fewer selected frames or a different quality policy. Retry from a known
random-access boundary under the original deadline, releasing failed-attempt
resources first. Never publish a partial joint embedding as a complete result.

## Qualification and telemetry

Use FFmpeg and codec reference decoders only as offline test oracles. Test
selected picture IDs/PTS, YUV geometry/pixels, displayed RGB, prepared patches,
and final embeddings separately so decode errors are distinguishable from
processor/model errors. Record backend-specific decode tolerance where output
rounding differs, and validate retrieval consequences rather than claiming
bit-exact hardware parity without evidence.

Cover B-frames, long/open GOPs, VFR, short clips, rotation, odd dimensions,
range/matrix differences, truncated packets, malformed metadata, configuration
changes, denied capacity, cancellation during callbacks/device work, and repeated
retry/session reuse. Track retained leases after failure and concurrent pressure.

Measure source bytes/ranges, packets and dependency pictures decoded, selected
frames, source/target pixels, backend route, surface high-water bytes, copies by
direction, preparation/vision/backbone time, p50/p95 latency, throughput, and
cancellation recovery. Compare short clips and long sparse/windowed inputs on
identical source/model bytes. Performance acceptance belongs to the
[implementation plan](../../../docs/plans/native-video-inference.md), and requires
end-to-end evidence rather than a demux-only microbenchmark.
