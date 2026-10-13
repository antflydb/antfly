# Native video decoding and sampled surfaces

Status: native container/decode/preparation/scheduling libraries, shared admission,
benchmarks and the qualified pure Zig H.264 subset are implemented. Latest main
(PR #1014) is merged as of 2026-10-09. EmbeddingGemma 2 ordered video groups now
use native decode/CPU preparation or VideoToolbox/Metal preparation and the merged
native/Metal vision and text encoders. Completed Metal patch buffers enter inference
directly; Metal video projection, token composition and backbone input remain on the device. Final pooling and vector normalization now execute on Metal before the response readback; pretrained CPU/Metal parity and reduced-dimension HTTP checks pass. Six pretrained
F32 video cases qualify native CPU and Metal numerical parity under the declared
RGB decoder policy. A real-clip reference-preservation campaign also executes on
macOS CPU/Metal and actual Linux ARM64; resident vision/backbone optimization
remains open. See the
[pretrained validation receipt](testdata/video-pretrained-validation.md).

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
The expanded Linux H.264 subset below is implemented. The model code from
PR #1014 is merged, and the integration/reference-preservation receipts below
qualify the delivered CPU/Metal subset. CUDA/NVDEC and the remaining residency
and codec gates are tracked in the implementation plan.

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

This CPU route supplies host patches to model consumers. The Metal route below
stages software-decoded pictures on the caller's device. The EmbeddingGemma 2
adapter described below consumes both routes; NVDEC/CUDA and further resident
model optimization remain follow-ups in the implementation plan.

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
or embedding benchmarks. The merged EmbeddingGemma 2 adapter is described below;
further resident model optimization and NVIDIA NVDEC/CUDA remain follow-ups.

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
  includes redundant slices in the default 256-slice-per-picture limit.
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
  and bounded frame/field MMCO commands. POC types 0, 1 and 2 are supported.
- Mixed MBAFF frame/field macroblock pairs, with sample-accurate neighbours,
  field/frame motion scaling, parity-aware chroma motion, field scans, separate
  field POCs and mixed-boundary deblocking. Fields use views over woven storage.
- PAFF complementary field pairs carried together in an indexed packet or as
  fields split across indexed packets, including bottom-first pairs. A bounded
  assembly extension also accepts pairs separated by unrelated coded pictures. The
  first field is filtered and admitted before second-field prediction; completion
  updates the same DPB identity and copies storage only when a pending prediction
  snapshot still retains it. Default field reference lists and reordering
  include previous and current-first-field references. Field MMCO 1–6 apply to
  individual fields, preserving an unaffected complement, resolving long-term
  index conflicts, enforcing the long-term index bound and resetting POC/frame
  state for MMCO 5. Direct prediction and weighting use the selected field's
  reference status.
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

Selecting any packet containing slices of a field produces the completed woven
picture. The decoder tracks actual packet membership rather than a packet-index
range, so intervening pictures cannot acquire another pair's callbacks or timing.
It reads ahead until selected pairs complete, charging every traversed packet,
including AUD/SEI/filler-only packets, to dependency and payload limits. Auxiliary
packets do not contribute presentation intervals and cannot themselves be selected
as pictures. PTS is the minimum member packet PTS; duration spans the member
presentation intervals. All requested member slots receive the same woven picture.
No incomplete field is published.

`max_pending_pictures` defaults to 16 and accepts 1–16. Reconstruction slots are
allocated lazily and reused; the slice bound applies across all fragments of both
fields. An unfinished field retains its prediction DPB snapshot, including marking
status and immutable samples, so intervening marking or eviction cannot alter its
remaining slices. Shared backing uses copy-on-write on complementary completion.
Workspaces, selection membership, retained DPB samples and snapshot state are charged
to shared admission and `max_decode_bytes`. Missing complements, overlapping primary
slices, ambiguous identities, cancellation and callback errors release all resources.
An IDR reset may discard unselected unfinished pairs, but fails if a requested pair
would be lost. Continuation IDR fragments are not seek points even if marked sync;
field reset packets must begin with macroblock zero.

H.264 defines complementary fields as consecutive coded access units. Buffering a
pair across unrelated coded pictures is an explicitly qualified assembly extension,
not a claim of conformant H.264 ordering. Pairing requires consistent frame number,
field/reference status, IDR identity and validated slice headers. Transport must
preserve unique pending picture identities. Repeated primary slices and conflicting headers fail
explicitly rather than being resolved heuristically.

Geometry/packet/reference workspace bounds are checked before reconstruction;
`max_decode_bytes` also enforces the actual decode allocator high-water limit.
Reference samples, picture/slice metadata, FMO storage and both lists' motion metadata are included in the conservative
workspace estimate. Cancellation is checked during RBSP copying, macroblocks,
filtering and output rows. Shared output admission remains held until Frame destruction;
batch callback failure frees the output, decoded-picture buffer and packet leases.

This remains a qualified tool subset. The portable route accepts avc1/avc3,
8–14-bit monochrome and 4:2:0/4:2:2/4:4:4, separate colour planes, declared
frame-number gaps, Extended-profile CAVLC data partitions and 8-bit 4:2:0 SP/SI.
Unequal component depths outside monochrome, additional profiles and unsupported
parameter transitions fail explicitly. Interlaced output preserves woven samples;
no deinterlacing policy is applied. Capability `portable_h264_subset` is
`qualified-avc1-avc3-8to14bit-mono-420-422-444-dynamic-epochs-separate-planes-sp-si-partitions-mbaff-paff`.
No OpenH264, x264, JM or FFmpeg runtime is linked.

`h264_dynamic.Session` scans a bounded parameter-set registry and records decode
epochs. Layout/POC-compatible non-IDR SPS changes preserve prediction references
while allowing crop, scaling, range and reference-capacity changes. Storage-incompatible
geometry/depth/chroma changes can start at a complete non-IDR intra picture with an
empty DPB. Missing references and parameter changes during pending picture assembly
fail explicitly. These are qualified extensions: H.264 keeps an SPS active for its
coded video sequence; this does not claim arbitrary non-IDR SPS conformance. PPS
changes can occur between complete pictures. Limits bound probe packets, retained
parameter bytes and configurations. Probe counters are separate from dependency
decode counters. A selected request may span epochs and retain duplicate/unordered
callback slots. Dynamic indexing supports avc3 sample entries; it does not silently
reinterpret a changed container track description. Static Apple hardware planning
continues to reject in-band reconfiguration.

Separate colour planes reconstruct through three independent monochrome DPB
passes, then combine owned native samples; receipts count all three passes and
payload reads. Frame gaps infer bounded non-existing references with POC and sliding
marking; attempting to predict from unavailable samples fails. B/C data partitions
may arrive in subsequent partition-only transport packets after category A; bounded
gathering stops at the next coded picture or configuration boundary. Auxiliary packets
may intervene. Association uses slice identity, not NAL ordering. Category A must arrive
first across packets; residuals preceding A and ambiguous picture boundaries fail. Missing, duplicate and orphan partitions fail.
SP/SI reconstruction and deblocking are checked against the official JM 19.0 decoder,
including switching SP. These qualifications do not promise every possible combination
of these tools. See [expansion validation](testdata/h264-expansion-validation.md).

Native-depth host preparation validates plane geometry and converts into the
existing RGB8 processor policy before resizing/packing; decoded samples retain
full depth. Monochrome uses neutral chroma in CPU preparation. `Metal.submitNative`
imports caller-owned R8/RG8Uint or R16/RG16Uint integer textures for 8–14-bit
4:2:0/4:2:2/4:4:4, retaining both until command/output destruction. Samples are
right-aligned, and chroma plane dimensions must match the declared subsampling.
`submitHost` stages owned native integer planes without a CPU RGB conversion;
`native_staging_bytes` and the host staging bound cover this copy. Both routes reuse
cached bicubic coefficients and match CPU preparation across range and rotation.
Monochrome native import/staging and uncompressed CoreVideo P010 imports are
implemented and qualified in the later "Monochrome and P010 Metal surfaces"
section. The VideoToolbox compressed decoder and its hardware qualifier retain
their 8-bit 4:2:0 output contract; importing P010 does not establish high-depth
hardware decoding.
CUDA/NVDEC and model execution remain separate work.

Independent x264/FFmpeg vectors cover intra prediction, CAVLC/CABAC 8x8 transforms,
filtered/cropped/low-QP pictures, P/B pictures, spatial and temporal direct prediction,
fades with explicit P weights, implicit B weights, B-pyramid reference pictures and
frame-number wrap. Qualified pictures match FFmpeg NV12 byte-for-byte; oracle hashes,
encoder settings and presentation picture types are recorded in
[testdata/h264-tools-oracle.json](testdata/h264-tools-oracle.json). Weight rounding,
coincident-POC fallback, all six short/long-term marking operations, packet mutation,
dependency limits, callback cancellation and
exhaustive reference-allocation failures have portable tests. The corpus also includes an original Sintel stream-copy excerpt and real-content
Baseline/Main/High10/High444 differential runs. This is bounded qualification,
not a guarantee of universal production-stream compatibility. CAVLC and
CABAC constants come from pinned Cisco OpenH264 BSD-2-Clause tables; the revision and
license are in [third-party notices](THIRD_PARTY_NOTICES.md).

Current platform results are recorded in
[testdata/h264-advanced-validation.md](testdata/h264-advanced-validation.md).

A further 30 PAFF packet vectors and their native sample hashes are recorded in
[testdata/h264-paff-packets-oracle.json](testdata/h264-paff-packets-oracle.json).
They cover standalone CAVLC/CABAC PCM, bottom-first pairs, field I/P/B selection,
long-term list reordering, IDR long-term fields and MMCO 1–6 at 8/10/14 bits.
FFmpeg matches independently known samples without frame-rate duplication.
Tests also cover signed timestamps, missing/mismatched complements, bounded
lookahead, duplicate callback slots, cancellation after the first field and
exhaustive standalone-reference allocation failures.

The buffered assembly qualification adds 15 generated vectors in
[testdata/h264-paff-assembly-oracle.json](testdata/h264-paff-assembly-oracle.json):
CAVLC/CABAC fragments with metadata gaps, bottom-first ordering, three interleaved
pairs and frozen prediction after reference eviction at 8-bit 4:2:0, 10-bit 4:2:2
and 14-bit 4:4:4. FFmpeg validates canonical consecutive field-pair streams against
independently known samples; native tests separately validate the reordered packet
extension, timestamps, slot membership, budgets and exhaustive allocation failures.

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
advertising support. HEVC remains a later project. The maintained VP9/AV1 routes are documented below.

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

### Corpus qualification and measured portable SIMD

`zig build qualify-video -Dqualify-input=/absolute/path/input.mp4` emits native
sample hashes and media/source clocks for every packet in a bounded MP4 corpus.
`scripts/qualify_h264.py INPUT NATIVE.jsonl` compares FFmpeg native-depth sample
hashes by media PTS with movie edits disabled, without range/depth conversion.
`scripts/qualify_h264_corpus.py` reproduces pinned real-content profile variants.
FFmpeg and JM are offline oracle tools only. `qualify-mode=mutate` runs deterministic
bounded container/packet mutations with cancellation and allocation limits.
`zig build fuzz-video --fuzz=2000` exercises pure Zig coverage-guided reconstruction
and parameter parsing without platform shims; `-Dvideo-fuzz-filter=SUBSTRING`
selects a target. Short campaigns supplement differential tests; they do not establish
complete decoder coverage or security qualification.

Native sample row copies and UV packing use Zig `@Vector`/`@shuffle` with LLVM;
tail rows and other compiler backends use scalar code. The measured 1080p packing
candidate improved throughput, while a measured 4×4 transform candidate was slower
and remains disabled. Differential/randomized tests cover signed overflow fallback,
unaligned byte output, crop strides and vector tails. Benchmark receipts and exact
compiler/backend limitations live in [benchmarks](testdata/benchmarks/README.md).
Use `-Dvideo-use-llvm=true`, `-Dbenchmark-backend=cpu`, `-Dbenchmark-iterations=N`
and `-Dbenchmark-size=384` to compare completed real-clip routes at realistic
preparation geometry. Output reports completed wall/GPU times and process/allocator
peaks; aggregate device allocation is not a per-request GPU peak.


## EmbeddingGemma 2 model adapter (2026-10-09)

Inference imports video alongside the exact audio/media/image module identities.
`pipelines/embedding_gemma2_video.zig` samples logical presentation order with the
pinned processor's 1 FPS / uniform 32-frame policy, maps selections back to decode
indexes, and projects one frame at a time. The video geometry budget is 140 pooled
16×16 / 3×3 soft tokens per frame, distinct from the image 280-token budget. RGB8
Torchvision antialiased bicubic preparation is centered for patch projection; the
coefficient cache key includes the resize policy. Video placeholders use ID 258884,
inside each frame's image opener/closer, without timestamps or separators between
frames. Original tokenizer sidecars need not contain the dynamically added video
token: validated image placeholder rows are rewritten before encoder execution.
Mixed text/image/audio/video order, task-prefix and joint mean-pooling semantics
follow the merged model pipeline, including the 8192-token total bound.

Native CPU uses portable MJPEG/H.264. Metal uses verified-IDR VideoToolbox for
eligible static H.264, direct CoreVideo preparation and a retained external MTLBuffer
handoff on the runtime's device. Portable H.264 and MJPEG may instead stage native
planes or RGBA into Metal. The model accepts the qualified progressive SDR
subset; broader monochrome/display sampling remains a library capability. A producer fence completes
before the imported CT enters inference. Producer ownership is released after CT
retention and before inference pooling/reuse; cancellation and failure release both.
The model admission covers request preprocessing and workspace before decode.
Video patch projection, learned-position addition, Q/K/V normalization, vision
RoPE, spatial pooling and active-token sequence mean pooling execute on Metal.
Projected video tokens are composed with text and other media using device
concatenation and typed row gathering, and enter the backbone directly. Final
vector normalization now executes on device before the response readback. The new normalization path is described below; CPU output validation and serialization remain host operations.

The HTTP grouped form accepts `media` or binary `attachment` with `video/mp4` or
`video/quicktime`; existing Go/Python/TypeScript media contracts carry these MIME bytes.
Flat video parts and WebM codecs are rejected. Encoded input is bounded to 64 MiB,
frames to 32, source pixels and decoder/preparation workspace retain library limits.
The current model display policy accepts square-pixel unrotated SDR, declared SPS VUI or nclx/nclc
BT.601/BT.709 matrices, and a documented BT.601 default when color metadata is absent.
HDR/unsupported display policies fail instead of being silently interpreted. Video
audio tracks are not implicitly embedded. Container init data is explicit for live
fMP4 ingestion; complete snapshots can be submitted through the same byte API.

Contract, processor identity, presentation sampling, mixed placeholder placement,
HTTP family scoping and Metal buffer destruction/linear-consumer tests qualify this
integration. The pinned official checkpoint and independent F32 video oracle now
qualify six video/mixed cases, including presentation reordering, repeated frames,
multiple clips and HTTP 128-dimensional output. JPEG reference RGB uses independent
Pillow/libjpeg decoding; FFmpeg default MJPEG IDCT differs on the colorful fixture.
See the [pretrained validation receipt](testdata/video-pretrained-validation.md) for
exact scope, errors and remaining application-level qualification.

See the [dated integration validation](testdata/video-model-integration-validation.md)
for executed checks, cross-platform compilation and retained qualification limits.

The model adapter now obtains completed logical AVC pictures for PAFF/MBAFF,
Extended-profile partition transport, separate planes and configuration epochs.
See the logical-picture and explicit-window policy below; transport members no
longer contribute separate frames to model sampling.


## Real-clip reference preservation and RGB recipe

The six-document Sintel corpus supplies independently decoded H.264/MJPEG clips
and four unlabelled text queries. The qualification harness compares every
embedding coordinate, cosine score, complete ranking and top-k set with pinned
PyTorch F32. This measures Antfly's preservation of the upstream model; relevance
and usefulness for a customer's database remain customer evaluation concerns.
The dated [retrieval and pipeline receipt](testdata/video-retrieval-validation.md)
records actual Linux execution, completed CPU/MPS/Metal timings, load/retry
behavior, and the exact scope of memory observations.

Real colored H.264 inputs exposed a one-byte conversion difference between the
old float/nearest RGB conversion and the pinned FFmpeg ARM Q13 conversion. It
changed two non-tied rankings. The model adapter now explicitly selects
`RgbRounding.q13_components`: floor each signed Q13 color component before final
rounding. CPU and Metal share independently implemented arithmetic; the generic
preparation API still defaults to `nearest`. Four independently generated
matrix/range golden cases qualify all byte values produced by the fixture.
Native decoder planes already matched FFmpeg; no native pixels were substituted
into the reference and no comparison tolerance was relaxed.

This changes the video recipe to `embeddinggemma2-f32-mean-v2-q13-video` and the
pretrained model identity to
`ba4732081640dc8db20c48eb87ca17da5bb5bc4fe21b32b584d126e83f61bb8f`.
Previously indexed video vectors need the normal identity-aware re-embedding
migration. Qualification covers the recorded widths, SDR metadata, FFmpeg version
and reference decode policy; it does not promise byte-identical output from every
platform's FFmpeg SIMD dispatch or arbitrary display/HDR inputs.

Static software AVC selections now use `h264_sparse.decodeSelected`, with
verified IDR runs bounded to the software decoder's dependency packet limit and
a separate aggregate packet-work bound. Callback slots preserve the original
presentation order and duplicate selections. An overlong GOP fails its bound;
container sync flags alone never authorize a seek. Dynamic parameter-set sessions
retain their existing route. The model uses at most 8192 submitted software
packets per clip, 32 selected pictures and the existing decoder allocation cap.

Codec implementations remain pure Zig. Stateful VP8 and the maintained VP9/AV1
source ports are documented below; no libvpx/dav1d runtime adapters are used.


## Portable VP8 keyframe entry point

`vp8.decodeFrame` reads a displayed keyframe from the existing WebM index and
returns owned padded 8-bit YUV 4:2:0 planes. It shares the pure Zig VP8 entropy,
intra prediction, transforms and loop filters with `lib/image`, through a raw
packet API; it requires no RIFF wrapping, RGB conversion or external decoder.
The coded frame tag is checked independently of the container sync hint.
Reference-dependent interframes fail with `UnsupportedVp8Interframe`; this is
not model WebM integration. Later sections add VP8 interframes and VP9/AV1
reconstruction.

Packet, pixel and live allocation limits apply before/during decoding. A shared
admission token reserves decoder workspace, then shrinks to the owned output
planes after scratch release. The frame keeps that token until destruction.
Tests compare three keyframes byte-for-byte against independent FFmpeg YUV,
inject allocation failures and deny concurrent capacity while a frame is held.
Fixture commands and hashes are in `testdata/vp8-keyframes.json`. Further codec
work follows the [VP8 decoding guide](https://www.rfc-editor.org/rfc/rfc6386.html).


### Monochrome and P010 Metal surfaces

`Metal.submitHost` and `submitNative` support monochrome at 8 through 14 bits.
A monochrome native surface supplies `uv = null`; shader conversion uses neutral
chroma without reading a fabricated image plane. A tiny bound resource satisfies
Metal validation. The command owns it until completion/release.

`apple.Surface` also imports uncompressed CoreVideo `x420`/`xf20` P010 buffers.
Their 10-bit samples occupy the most significant bits of 16-bit storage;
`Format.bitDepth`, `sampleShift` and `fullRange` preserve that contract in CPU
mapping, host staging and direct CoreVideo/Metal import. Native integer textures
may specify `NativeSurface.sample_shift` explicitly, with validated depth/storage
bounds. The high-depth color conversion retains existing SDR policies. Tests
compare both host and device paths to right-aligned CPU samples, cover rotations
and ranges, and release producer buffers/textures before GPU completion.
Compressed CoreVideo formats and HDR tone mapping remain unsupported. This does
not change VideoToolbox codec/profile qualification.


## Resident token composition and overlapped projection

The video projector exposes an owned tensor result. Metal captures retain one
projection per unique selected packet and compose repeated frames in presentation
order without soft-token readback. Ordered text/image/audio/video composition
validates marker counts and IDs, scales text on device, concatenates projected
media rows, and gathers the final sequence using bounded i32 row indices. Existing
host image/audio projections can upload into that table. The final device tensor
enters the joint language backbone; CPU and raster-batch APIs preserve their
existing host contracts. A real Metal test compares mixed composition with the
host reference and asserts zero additional host-mirror downloads.

The projector batches up to two prepared frames with identical geometry. Patch,
Q/K/V, FFN and projector linear operations share frame-major rows; attention has
an independent frame batch axis. Learned positions and 2-D RoPE reset at each
frame, and pooling preserves frame boundaries. Mixed geometry flushes single
frames. CPU retains one pending patch frame and flushes a final singleton; duplicate
samples reuse the owner projection. Metal batches resident inputs and slices the
result on device, preserving unique-packet ownership and presentation ordering.

A request-local Metal worker exclusively owns projection/backend operations while
the producer decodes and prepares subsequent frames. Two preparations can queue
behind two executing frames; one producer-held submission can wait for admission
to the queue. The model reserves 1920 MiB workspace before decoding: two 768 MiB
transformer ceilings and three additional 128 MiB preparation slots. Failure
closes the queue, wakes blocked producers, drains GPU leases and joins before
backend/allocator teardown. GPU lease destruction fences outstanding preparation
commands. CPU batching shares transformer dispatches but does not add a decode
worker. Backbone execution remains request-local and serialized by the session.

Actual-GPU tests compare batched positions, RoPE and pooling against independent
single-frame results without intermediate downloads. Pretrained, token-count,
retry and retrieval gates must qualify the rebuilt production binary separately.

`sampling.presentation` accepts logical picture IDs and timestamp durations,
selects the most recent active picture at each cadence, and exposes explicit
skip/hold-gap and collapse/preserve-repeat policies. It handles reordered metadata,
VFR and windows beginning inside a picture. Callers must supply logical pictures;
it does not itself derive them from PAFF/partition packets. The pinned model's
existing index/FPS sampling policy remains separate.

`display.layout` explicitly chooses coded pixels or presentation geometry.
Presentation mode handles orthogonal quarter-turn matrices, translations and
non-square pixel aspect ratios with bounded square-pixel dimensions. Perspective,
shear, reflection and arbitrary rotations fail. Preparation already handles these
rotations; this geometry API does not qualify HDR tone mapping or broaden the
model adapter's default unrotated square-pixel SDR policy.

Model-level VFR qualification retimes the pinned MJPEG fixture while preserving
compressed frames, frame count and total duration. The pinned HF average-FPS
index policy still selects `[0, 4]`, even when frame 4 presents at 0.25 seconds;
this establishes index-policy parity, not timestamp-policy parity. CPU and Metal
compare the complete embedding against the independent pretrained pixel oracle.
The same HTTP campaign verifies explicit rejection of rotation, non-square pixels
and PQ HDR. No model rotation/SAR rendering or HDR tone mapping is qualified.

Still open: final device normalization, logical field/partition model indexing,
qualified model rotation/SAR/HDR rendering, VP8 interframes and pure Zig VP9/AV1
decoding. Decoder transport/header parsing alone is not advertised as decoding.


The rebuilt production CPU and Metal routes pass all six pretrained cases,
128-dimensional output, admission retry and corrupted-frame cleanup/retry. Fresh
Metal short/long-clip campaigns preserve pinned PyTorch vectors and scores;
the short corpus preserves all four exact rankings/top-k sets. See the
[composition and scheduling receipts](testdata/video-retrieval-validation.md#device-composition-and-projection-scheduling-continuation).

## Stateful VP8 and presentation/HDR rendering continuation

`vp8.Session` decodes WebM VP8 packets in coded order, including hidden reference
pictures. `next()` returns independently owned planar Y/U/V frames; null denotes
an invisible picture or EOF, distinguishable through `next_index`. Frames can
outlive the session. The existing `decodeFrame` remains the random-access
keyframe-only entry point. Both paths are pure Zig and portable to Linux.

The shared VP8 reconstruction now handles ZERO/NEAREST/NEAR/NEW/SPLIT motion,
all four split layouts, intra blocks in interframes, six-tap/bilinear subpixel
prediction, residuals, normal/simple filtering, reference refresh/copy/sign bias,
persistent segmentation maps and entropy state. Versions 0–3 are accepted.
Reference updates commit transactionally after successful reconstruction and
retained allocations; malformed input, cancellation and allocation failures leave
state available for retry. Packet/pixel/workspace limits and shared admission
cover the session and separately owned outputs. The initial implementation keeps
immutable shared reference slots with independent returned output; further decoder optimization remains a performance
optimization, not a decoding dependency.

`testdata/vp8-interframes.json` pins seven independent FFmpeg/libvpx oracles:
174 displayed pictures and three hidden alt-reference pictures, multiple token
partitions, keyframe resets, versions 0–3, and a real Sintel excerpt. These are
qualification examples, not exhaustive codec conformance. Runtime reconstruction
links no codec libraries. WebM model ingestion is implemented with pretrained
CPU/Metal/Linux ARM64 reference-preservation receipts below. The
maintained VP9/AV1 routes below expose their qualified capabilities explicitly.

The EmbeddingGemma 2 model now honors orthogonal rotation and SAR metadata.
SAR controls model target geometry through one direct separable resize; it does
not materialize an intermediate square-pixel image. Unsupported transforms and
conflicting color declarations still fail. The recipe identity advances to
`embeddinggemma2-f32-mean-v3-display-hdr`, keeping different rendering behavior
out of the preceding recipe's embedding identity.

PQ/HLG YUV uses an explicit fixed policy: [BT.2100 transfer functions](https://www.itu.int/rec/R-REC-BT.2100),
D65 BT.2020 NCL or BT.709 primaries, 100-nit exposure, a global luminance
[Reinhard shoulder](https://www-old.cs.utah.edu/docs/techreports/2002/pdf/UUCS-02-001.pdf),
and sRGB/RGB8 output. HLG assumes a 1000-nit display, gamma 1.2 and zero black.
This reproducible inference policy ignores mastering/dynamic HDR metadata.
The model requires explicit, consistent gamut and transfer declarations and
rejects HDR-tagged MJPEG. Preparation's CPU path evaluates transfer/quantization
in f64; Metal caches 16,385-entry f64-generated transfer tables per policy, then
uses device interpolation, gamut/shoulder conversion and existing resize/packing.
Resize and color tables share the preparer's admitted coefficient budget.

Independent float64 fixtures cover ten-bit 4:2:0, both ranges, both gamuts and
both transfers. CPU and actual Metal preserve every RGB8 code; normalized Metal
patches allow `1e-6` for fused float32 arithmetic. Tests also cover table reuse,
admission denial and in-flight output surviving preparer destruction. Positive
pretrained qualification scripts and full PyTorch CPU/MPS vectors are in
`tools/embeddinggemma2/testdata/render`. They explicitly extend the pinned
processor's presentation policy. Qualification remains bounded to these inputs;
hardware HDR import, all depths/chroma formats and real-world HDR scenes require
additional evidence. Earlier dated receipts continue to qualify their own binaries.

## Maintained pure Zig VP9 and AV1 reconstruction

`video.codecs.Session` now decodes elementary VP9 and AV1 packets, and
`decodeWebmPacket` connects these decoders to the existing bounded WebM range
reader. This supersedes the earlier VP9/AV1 implementation gaps above. The cores
are checked-in, Antfly-maintained Zig source ports of pinned BSD reference code;
there is no libvpx/dav1d/FFmpeg runtime, codec C build, or build-time download.
Ownership, pinned origins and port-specific changes are recorded in
[src/codecs/PORTING.md](src/codecs/PORTING.md).

The qualified VP9 fixtures cover profiles 0–3, 8/10/12-bit 4:2:0, 4:2:2 and
4:4:4, interframes, superframes and odd geometry. AV1 now accepts profiles 0–2,
8/10/12-bit monochrome/4:2:0/4:2:2/4:4:4, including interframes,
hidden/show-existing updates and film grain. Independently decoded fixtures
qualify the additional formats, odd dimensions, Annex B and spatial/temporal
operating points; this is bounded coverage, not complete codec conformance.
See [the expanded AV1 receipts](testdata/av1-extended/README.md).

`Options.av1_framing` selects low-overhead OBU packets (default) or Annex B.
Annex B validates nested temporal/frame/OBU sizes, redundant payload sizes,
headers, delimiter placement and frame-unit structure before core entry. It
normalizes complete temporal units within the shared decode allocation budget;
`max_annex_b_frame_units` also bounds framing work. Framing failures remain
retryable. WebM requires low-overhead framing and rejects Annex B options.

`av1_operating_point` explicitly selects an index (default 0); unavailable
indices fail instead of silently falling back. `av1_all_layers` defaults to true,
retaining every visible spatial-layer output. False selects the highest spatial
layer within the chosen operating point. Supply complete temporal units for
that policy: separately drained spatial-frame packets do not establish a complete
temporal unit. Output timestamps remain caller-owned; selecting a temporal
operating point drops excluded layers without inventing timestamps.

Create one Session per sequential coded stream. `decode` returns an owned Batch
containing every visible picture; hidden updates retain references without
inventing an output. VP9 superframes are split and drained separately so earlier
visible pictures are retained. A packet producing multiple visible pictures
assigns its caller-provided PTS/duration to each; callers needing distinct
picture timestamps must supply distinct framed access units. WebM supplies its
stored packet timestamps, initializes AV1 configuration OBUs from CodecPrivate,
and checks both reader and session cancellation controls. It does not add a
model-level WebM ingestion or sampling policy.

Frames own right-aligned native-depth semiplanar samples and color metadata.
`Frame.host()` exposes the existing CPU/Metal preparation surface, with ceil
chroma dimensions for odd widths/heights. Callers must pass the frame's color
metadata into their preparation/display policy. Planar-to-semiplanar output
uses Zig portable vectors plus scalar tails; no SIMD throughput claim is made.
Apple import, resident model execution and CUDA coverage are separate routes.

Options bound packet bytes, pixels, core live memory, output bytes and picture
count. Stable session owners use the existing decode Budget and shared
admission; separately admitted outputs can outlive the session. Allocation,
cancellation or decode failure after core entry invalidates and reclaims the
session. Recovery requires a new session and replay from a synchronization
point. Preflight/admission denial before entry is retryable. Internal workers
are disabled; distinct sessions can run concurrently with atomic initialization.

Qualification fixtures and per-picture reference hashes are in
[testdata/vpx-av1](testdata/vpx-av1/README.md). They cover 144 visible pictures
across 12 streams, including AV1 grain and VP9 depth/chroma variants. Tests also
exercise every first-picture allocation failure, concurrent shared admission,
cancellation, output lifetime, truncation, header mutations and multiple visible
pictures in a VP9 superframe, plus actual CPU/Metal preparation parity on decoded
high-depth/chroma/odd-geometry frames. macOS execution and Linux x86_64/ARM64 compilation
are separate receipts; cross-compilation does not establish actual Linux runtime
qualification. The existing Linux qualification workflow runs these portable
tests. Broad real-world conformance/fuzzing and realistic performance remain
qualification work, rather than implied by the source port.


### WebM logical-picture replay for model input

`webm_pictures.index` reconstructs the bounded coded stream and retains only
visible-picture timing and geometry. Hidden VP8/VP9/AV1 reference pictures update
state without becoming sampling candidates. Multiple visible outputs from one
packet have independent logical IDs; they retain the packet timestamp rather
than receiving an invented frame clock. Presentation ordering is stable at equal
PTS. Sampling uses the pinned average-FPS policy with a 32-frame uniform cap.
The declared Segment Duration takes precedence over rounded packet PTS. Without
a declared duration, an absent final duration uses the last positive presentation
step; a stream with
no inferable rate uses capped index sampling.

`webm_pictures.walk` replays dependencies once up to the final selected logical
picture. Selected frames borrow independently owned native-depth surfaces for
the callback. The model sends these through the existing CPU preparation or
Metal preparation/projection queue, preserving frame batching and duplicate
projection reuse. Indexing and replay each limit coded work to 8192 packets and
visible metadata to 65536 pictures; no full decoded clip is retained. This
initial implementation decodes twice and is a target for measured seek/index
optimization.

The inference HTTP contract accepts `video/webm` for EmbeddingGemma 2 ordered
groups. MP4/MOV processing retains its prior path and sampling behavior. The
current WebM track parser has no display/SAR/Colour model metadata contract;
qualification below is for generated default-display SDR fixtures. Broader WebM
display metadata support and pretrained qualification remain separate gates.


The [WebM qualification receipt](testdata/webm-validation/README.md) records
pretrained CPU/Metal and actual Linux ARM64 parity over five clips and three
queries, including real VP9/AV1 Sintel derivatives. Eight vectors agree within
`1e-6`; all rankings/top-k sets match. Current full video gates pass 141 tests on
macOS with actual Metal (two skips) and 120 on Linux ARM64 (23 platform skips).
Bounded H.264/VP9/AV1 coverage-guided campaigns total over 608000 runs without
failure. This establishes the documented corpus/format subset and reference
preservation, rather than arbitrary codec conformance or end-user relevance.


## Logical AVC pictures and explicit model windows (2026-10-11)

`h264_pictures.index` observes completed native decoder assemblies, including
complementary PAFF fields, slices or data partitions spread across packets and
in-band configuration epochs. It records presentation timestamps, assembly spans,
geometry/depth/chroma and the first transport member, rather than retaining an
entire decoded clip. Separate-colour-plane passes are joined and require all
three matching planes. Replay selectors identify the completed assembly. Bounds
cover transport packets, logical pictures and existing decode workspace; control
checks and shared decode admission remain active. Static progressive AVC retains
its packet-index fast path.

Model `Sampling.reference` preserves the pinned processor's average-FPS/index
policy over logical presentation pictures. `Sampling.timestamps` is an explicit
native policy: nanoseconds relative to the first logical PTS, exclusive end,
earliest picture at or after each target, stable ties and omitted duplicate IDs.
All comparisons use rational integer timestamps. The default step is one second,
with at most 32 frames. `overflow=uniform` spans the requested interval;
`truncate` keeps its first targets. When end is omitted, picture durations define
the tail; an unknown final WebM block duration uses the preceding distinct PTS
step. A lone picture without a duration requires an explicit end. Empty windows
and malformed policies fail. This policy is distinct from HF index sampling.

HTTP ordered video parts accept an optional object:

```json
{"sampling":{"policy":"timestamps","start_ns":20000000000,"end_ns":40000000000,"step_ns":1000000000,"max_frames":16,"overflow":"uniform"}}
```

Omitting `sampling`, or supplying only `{"policy":"reference"}`, selects the
reference policy. Unknown keys, non-integer times, invalid bounds and sampling on
non-video content are rejected. Native callers use `Part.sampled_video`. Supply
multiple input groups containing distinct intervals to receive separate window
embeddings. A group still produces one joint embedding across its ordered media
parts; windows across groups do not yet share decoded/projected frame storage.
The video adapter applies this policy to MP4/MOV and WebM logical indexes, charges
selected geometry during HTTP preflight, and uses portable AVC for field/partition
transport on CPU and Metal. These inputs do not use the progressive Apple route.

The recipe advances to `embeddinggemma2-f32-mean-v4-logical-timestamps`. Consumers
must respect the changed identity when storing or comparing embeddings; existing
receipts retain the identity of the binary they qualified. Qualification inputs
and reproducible commands are in
[the sampling corpus](../../../tools/embeddinggemma2/testdata/sampling/README.md).
General mixed-tool conformance and broader display/Colour qualification remain
separate requirements.


## Device final pooling and normalization continuation (2026-10-11)

`ComputeBackend.maskedMeanNormalizedRows` combines masked mean pooling, requested
output-dimension truncation and device L2 normalization. Metal retains the output
in private storage and returns it as a CT. The first dispatch preserves the
existing masked-mean accumulation policy; the second rescales by the largest
finite component before reducing squared values, avoiding norm overflow and
underflow for the tested finite scales. A buffer barrier separates dispatches,
and one threadgroup completes all reads before its in-place writes. Invalid or
zero norms produce NaNs that final response validation rejects.

Ordered multimodal groups, resident text and raster cohorts use this path.
Flat text/media requests pass their requested EmbeddingGemma 2 dimension into
preparation so HTTP serialization does not perform a second truncation/normalize
step. Flat resident text uses the same bounded encoder and normalized pooling
primitive. The default remains 768 dimensions; 128/256/512 are trimmed before
normalization. Other model families retain their existing contract. Explicit
resident-text diagnostic disablement can still select its host route.

The returned vector is read back once for length, finite/nonzero validation and
serialization. No CPU centroid or L2 normalization follows this device output.
The ordinary `maskedMeanRows` slot remains available to unrelated consumers.
CPU pooling remains available for native execution. This is a numerical/layout
optimization, preserving the v4 sampling/rendering recipe rather than defining
a different processor policy.

Actual-Metal units pass long 8192-row masked pooling, skipped NaN padding,
retained row views after parent destruction, 128/256/512/768 outputs, finite
scales 1/1e-35/1e35, invalid norms and planned command scopes. The preceding
sampling receipt continues to qualify its frozen source/binary. Pretrained
requalification passes ten vectors and exact rankings/top-k on CPU/Metal,
twenty Metal dimension cases plus default-dimension reset, and the six original
mixed/video cases with admission and damaged-frame retries. See the new
[resident normalization receipt](../../../tools/embeddinggemma2/testdata/resident-normalization/README.md). Cross-request scheduling, cross-group frame
reuse and isolated performance measurements remain separate work.


## Inherited DASH addressing and native transport

The native media layer selects MP4 video representations from multiple
adaptations and inherits SegmentTemplate/SegmentList addressing across Period,
AdaptationSet and Representation. BaseURL chains resolve against the actual
manifest URL supplied by the live reader. Returned references, including escaped
signed query parameters, are playlist-owned. Authenticated HTTP still pins the
origin before fetching initialization or media bytes; changing overlapping
descriptors fails transactionally. Representation/configuration identity is
stable across reloads, and transport failures preserve the next sequence.

This extends the native streaming library rather than changing the pretrained
model's rendering recipe. Automatic multi-period playback and stable live
period cursors now preserve inferred timing, numbering resets, announced gaps
and pending references as earlier Periods disappear. Decoder epochs change at
Period boundaries even when the initialization URI is unchanged; held packet
leases remain valid. Returned segment metadata supplies exact rational
presentation mapping while packet timestamps retain the native track clock.
Broader HLS coverage remains implementation work.
See [MEDIA.md](../media/MEDIA.md#dash-template-and-live-ingestion) for selection
options, timing requirements and unsupported addressing features.


## Pipeline admission ownership

The model adapter now supplies a media admission sub-ledger backed by its
process-wide execution permit. MP4/WebM source/indexing and portable decoder
reservations share the prepaid host preprocessing bound. MJPEG reserves its
decode peak before allocating, then retains only the RGBA output charge until
frame destruction. Metal preparations reserve scratch/staged source storage and
a command slot before uploading coefficients or submitting driver work.
`releaseSource` shrinks that reservation to the retained output after completion;
`deinit` fences the command before releasing capacity.

Five preparations may coexist: two queued, two consumer jobs and one producer.
The original bridge used a 3344 MiB blanket video workspace bound. The
geometry-derived planning continuation below replaces it with 1536 MiB model
execution plus selected preparation/native-surface bounds and 16 MiB coefficient
storage. Its request-local ledger permits up to five command owners.
The parent shared controller gates concurrent requests before decode; HTTP
preflight has a separate shared host reservation. The first-video producer
continuation below prepares before the model gate/provider lease. Measured
scheduling and reservation tuning remain work. The current server passes the six original pretrained video/mixed cases on CPU
and Metal, including admission/decode retry and reduced dimensions. Eight WebM
vectors (five clips and three queries) preserve all reference rankings/top-k;
concurrent/sustained and disconnect/retry checks pass. These are correctness and
ownership checks on a shared host, not isolated throughput measurements.


### Shared preflight and geometry planning

Video inspection before model loading now acquires shared host capacity and
uses bounded/reclaiming allocation. Temporary metadata/decoder ownership is
released before execution admission. The model acquires host-only planning
capacity, then reserves device preparation bytes from selected geometry, unique
pictures (up to five live owners) and native decoder surface estimates.
Sequential video parts use a maximum transient share. The blanket 3344 MiB
Metal video estimate is superseded by 1536 MiB model workspace plus the planned
preprocessing share. CPU inspection does not require Apple surface capacity.
Shared denials retain existing leases and propagate through the request retry
contract rather than a per-item partial response. Allocation failures during
hardware probing propagate instead of choosing a different budget.

Current macOS CPU/Metal pretrained reports and source hashes are in
[preflight qualification](../../../tools/embeddinggemma2/testdata/preflight-admission/README.md).
Cross-request stage overlap, current Linux pretrained execution, measured tuning
and the other non-CUDA goal requirements remain active.


### Preparation before the model gate (2026-10-11)

Grouped inference starts a bounded producer for its first video after shared
execution admission and before the model gate/backend lease. A two-slot queue
hands owned CPU patches or Metal preparations to the model consumer. Later
video parts stay sequential. Producer work never touches model weights or
backend operations; cleanup closes, joins, drains and fences before returning
resource capacity. Calls without a runtime retain the synchronous path.

Cold Metal preparation borrows the process device through once initialization.
The producer retains its preparer until queued imports are consumed/drained;
CoreVideo imports independently retain the originating cache. Direct CPU and
actual-Metal checks cover blocked queues, cancellation, allocation failure and
imports that outlive their cache/preparer. Model execution still serializes;
measured scheduling/performance tuning and the broader non-CUDA scope remain
active. Current Metal and actual Linux ARM64 binaries preserve six original
cases, eight WebM vectors and ten timestamp/window vectors with exact rankings.
See the [source-bound execution receipt](../../../tools/embeddinggemma2/testdata/pipeline-overlap/README.md).


### Larger WebM streams, reference ownership and completed benchmarks

The new WebM benchmark decodes VP8/VP9/AV1 and optionally prepares every Nth
visible picture on CPU or Metal. It fences each GPU preparation and records
completed wall time, command execution time, first/warm setup, decoder allocation
high water, process RSS high water, device aggregate allocation and logical
upload/copy bytes. Codec reconstruction stays in maintained Zig. Benchmark-only
image-source substitution supports matched source-pinned experiments without
adding a production decoder policy or runtime dependency.

VP8 reference slots now alias immutable decoder-owned pictures instead of cloning
all three slots every frame. Refresh allocates one reference copy; unchanged/copy
commands retain existing owners. Obsolete owners are freed once, and failure or
cancellation before commit leaves references and entropy unchanged. Returned
planes remain independently owned and mutable. A broader real-stream campaign
also found and corrected the 16x16/chroma true-motion corner sentinel at frame
edges; the six-picture regression and independent hashes are in
`testdata/vp8-true-motion-border.json`.

The larger corpus contains full 52-second 854x480 Sintel derivatives and 12-second
1920x1080 upscaled excerpts in all three codecs. All 4623 visible picture hashes
match independent FFmpeg reconstruction, including SVT-AV1 encoder output.
Inputs and hashes reproduce byte-for-byte. The matched copy baseline includes
the same edge correction and also matches the VP8 goldens. Actual Linux ARM64 also matches all 4623 pictures and passes 137 portable tests
with 25 skips. The full macOS suite passes 160 with two skips; 24 model tests and
eight current CPU/Metal pretrained WebM vectors/rankings pass. Timings and exact
source/binary/input/oracle scope are in the
[WebM benchmark receipt](testdata/benchmarks/webm/README.md). These
measurements cover decode/preparation, rather than model execution or deployment
capacity; broader codec/display/streaming qualification and the full non-CUDA
scope remain active.


### Header-only WebM logical-picture indexing

Model planning now indexes VP8/VP9/AV1 displayed-picture headers without reconstructing pixel planes. Hidden pictures still update reference geometry; VP9 superframes and AV1 operating-point filtering preserve displayed output ordinals. AV1 uses the owned frame-header grammar with header-only reference slots and validates tile-group framing. Entropy and pixel correctness remain replay responsibilities. An explicit `strategy = .reconstruct` index provides the qualification oracle. Metadata allocations use shared admission and bounded capacity; the returned index retains its reservation until deinitialization. All 4,623 larger-corpus picture records and sampled IDs match reconstruction on macOS and actual Linux ARM64. The metadata fuzz campaign passes 101,937 runs, and seed acceptance checks exercise Smith serialization before parsing. Shared-host timings are diagnostic. Current CPU pretrained parity passes all eight vectors and three exact rankings/top-k sets; Linux pretrained and corruption/retry follow-ups remain in progress; this change does not establish general codec conformance. Receipts are in `testdata/header-index-validation`.


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

## WebM presentation through model preparation

WebM track presentation and Colour metadata now reaches model preparation.
Crop is applied in coded pixel coordinates before rotation/resampling, so odd
crop boundaries preserve the original chroma sample phase. CPU and Metal use
the same cropped extent. Metal stages/imports the original planes and applies
crop in the shader; it does not allocate an intermediate cropped raster.
Resize coefficients are cached by cropped geometry and preparation policy,
with crop origin remaining a command parameter. RGBA and P010/CoreVideo paths
use the same ordering and bounds.

Display dimensions supply the target aspect ratio in one resample. Rectangular
projection roll is counterclockwise; positive 90 degrees maps to the existing
clockwise-270 renderer. Unsupported stereo/projection/pose and chroma-siting
policies fail explicitly. Container Colour declarations are combined with
codec signals; incompatible matrix, transfer, primaries, depth, chroma and
range declarations fail. VP8's container declarations can supply its RGB
conversion matrix and full/limited range. The fixed HDR renderer still uses
its declared exposure policy rather than deriving exposure from mastering data.

The embedding recipe advances to
`embeddinggemma2-f32-mean-v6-webm-presentation`, since previously ignored metadata
can change an embedding. Independent RGB24 crop fixtures exercise odd/even
boundaries and all quarter turns. Metal tests additionally exercise RGBA staging,
coefficient reuse and P010 imports. The presentation fixture generator preserves
compressed Cluster payload hashes while changing only container declarations;
`tools/embeddinggemma2/video_webm_presentation_qualification.py` supplies a pinned
PyTorch oracle with explicit presentation processing, rather than assuming the
upstream loader applies container display metadata automatically. Qualification
receipts are recorded separately from the earlier v5 receipts. Broader real-world
HDR/display qualification and the full non-CUDA goal remain active.

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


## Native MPEG-TS AVC and HLS decoding

`transport_avc.Reader` converts the media demuxer's Annex B PES payloads into
length-prefixed AVC packets and an owned `media.mp4.Reader` view. It builds AVC
configuration from in-band SPS/PPS or a supplied continuation configuration,
retains PES timestamps in the 90 kHz domain, and feeds the existing native
H.264 logical-picture/configuration decoder. It does not serialize or remux an
MP4 file. The backing snapshot must outlive the reader; `Reader.deinit` releases
both in order. Normalization admission is derived from actual payload size,
including scratch/snapshot overlap, rather than the maximum stream allowance.

For HLS, keep one request-local adapter at a stable address:

```zig
var codec = video.transport_avc.Hls{ .allocator = allocator };
defer codec.deinit();
var reader = try media.hls_live.Reader(Http).init(
    allocator, &transport, playlist_url,
    .{ .transport_stream = codec.factory() },
);
defer reader.deinit();
```

The adapter commits continuity, configuration and ordinals only after successful
normalization. HLS discontinuities reset TS state and open a bounded epoch;
configuration changes without a discontinuity fail. Held segment readers remain
valid across reloads and epochs. Demuxing, normalization, configuration, leases
and retained packet arrays use the supplied shared pool. HTTP/AES behavior
remains in media. This path is portable and adds no runtime codec dependency.

Qualification compares every decoded NV12 picture in closed-GOP and B-frame
fixtures against independent FFmpeg hashes, including presentation timestamps.
Retry, discontinuity, held leases, allocation-failure cleanup and production
HTTP authentication are exercised. See the source/binary-bound receipts in
`../media/testdata/transport/validation`.

Per-segment PAT/PMT and recoverable access-unit clocks remain restrictions.
AUD-delimited NAL/access units can now span PES records, as described below. Durations use adjacent DTS deltas; an isolated
packet needs an explicit duration (HLS can supply EXTINF). Broader VFR terminal
policy, TS SAR/display qualification, external TS maps, NALs spanning PES and
multiple access units per PES remain open. Raw TS/model-input integration and
pretrained TS/HLS parity are not established by this decoder-library change.
The full non-CUDA goal remains active.

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

### H.264 progressive reference storage

Completed progressive reconstructions now share their planar samples, motion
arrays and macroblock metadata with the decoded-picture buffer. Snapshot and
workspace owners use the same per-decoder reference count. Reusing a workspace
allocates replacement storage before releasing its old share; a late slice
copies the payload before mutation. Allocation failure leaves the previous
reference and workspace intact. Ordinary next-picture reset does not copy the
old payload. Field-coded PAFF/MBAFF completion retains its existing copying
path. Independently retained packed output keeps its existing ownership.

`h264.Options.share_reference_storage = false` retains the copying oracle.
Optional `reference_storage_stats` counts progressive commit payload bytes
copied or retained, and late-slice copy-on-write bytes. These counters exclude
normal reset clearing, packed output conversion and field completion; they
are cumulative and saturating. Shared admission remains conservative and
includes each new ownership counter. Eliminating copy traffic does not by
itself reduce the reserved bound or establish a throughput improvement.

`video-benchmark <mp4> cpu|cpu-copy <iterations> <preparation-size>` compares
the two paths with completed CPU timing, peak RSS, reference counters and a
SHA-256 of the prepared CPU patch arrays. Hashing and job cleanup are included
in the completed timing. `install-video-benchmark` installs this executable.
The pinned trailer comparison and qualification gates are recorded in
[testdata/reference-storage-validation](testdata/reference-storage-validation/README.md).

### Request-local projected-frame reuse

The EmbeddingGemma 2 pipeline now reuses complete frame projections across
overlapping video parts and groups in one request. Entries are keyed by the
SHA-256 of the exact clip bytes and the model sampler's picture identity, and
bound to one loaded model session. CPU entries own F32 arrays; Metal entries
retain dense F32 device tensors across compute-wrapper teardown. The sampler
still opens and validates container/header metadata. Missing interframes still
replay required dependencies; cached targets skip preparation and projection,
and a fully cached selection skips reconstruction.

The cache holds up to 128 entries and 64 MiB of retained payload/allocation
spans. Each entry keeps a separate shared admission lease until eviction or
request cleanup. Device views charge the full underlying buffer span, so a
small view cannot hide a larger retained allocation. Cache capacity, allocation
or admission denial falls back to uncached execution. These bounds cover
retained payloads; fixed cache metadata and process RSS are separate.

Producers move entries into bounded queue tickets without manipulating Metal
buffer reference counts. Borrowed entries cannot be evicted; consumers import
them into the current backend and return the tickets. Queue closure and failed
consumers return outstanding tickets before cache cleanup. A process protection
guard remains active through cached-device cleanup. Both grouped HTTP and
direct grouped inference own the cache inside their model handle lifetime,
while releasing the model execution lock between groups.

Sampling order, duplicate frames, frame markers, task prefixes and final
normalization retain their existing contracts. Qualification receipts are in
`testdata/frame-cache-validation`; wider sustained scheduling, streaming-window
reuse and codec/display qualification remain in the active non-CUDA goal.

The final pretrained grouped/multipart preservation run passes on native CPU,
Metal and actual Linux ARM64. All 18 ordered groups and both multipart groups
match their pinned uncached binaries exactly; reference rankings/top-1/3/5
overlap are exact, with maximum cosine score drift below 3e-7. The frame-cache
receipt binds the source, binaries, checkpoint, oracle and runtime artifacts.

### Partial HLS ingestion boundary

The shared media layer now exposes partial fMP4 HLS units and explicit bounded
blocking reloads. Parent fallback verifies already consumed encoded samples
before returning only the new tail, preserving packet leases and retry cursors.
`Segment.hls_timing` carries sequence/part/discontinuity identities and an
independence hint. Consumers must retain decoder dependencies across parts;
the playlist hint is not a validated codec restart point. Preload hints never
become input until published as actual parts.

This closes a library ingestion gap. Model-level live window orchestration,
TS partials, date-range delta updates and broader live rendition/CBC-range qualification
remain in the active non-CUDA objective. See the media
[partial-HLS receipt](../media/testdata/hls/partial-validation/README.md).
VP9 and AV1 remain checked-in, Antfly-maintained Zig ports with no runtime
libvpx/dav1d/FFmpeg dependency; ownership and update rules are documented in
[src/codecs/PORTING.md](src/codecs/PORTING.md).

### Owned live packet windows and model reader input

[live_windows.zig](src/live_windows.zig) joins finite published HLS/DASH units in
decode order, preserving supplied preroll and future reorder context. It owns
sample bytes, configuration/colour metadata and packet indexes independently of
input segments. Mixed epochs/configurations/DASH periods and duplicate decode
clocks fail before model input. Staging, snapshots and indexes share admission;
allocation-failure and retained-output tests cover cleanup and ownership.

The model pipeline accepts borrowed immutable packet readers for preflight,
synchronous encoding and asynchronous preparation. Independent source counters,
controls and preparation leases leave caller read state untouched. Reader input
uses timestamp/logical-picture sampling and the existing CPU/Metal scheduling,
admission and cancellation paths. Projection caching is disabled for these
borrowed readers: a packet identity is not an immutable complete-clip cache key.
The caller retains source, packets and metadata until synchronous completion or
`Prefetch.deinit`; `embedLiveWindow` manages its joined owner across execution.

A caller supplies a presentation-complete window, including codec preroll and
reordered-output dependencies, and separates configuration epochs. Automatic
watermarks, live reload-to-window scheduling and pretrained streamed-window
parity are still required. See [the window qualification receipt](testdata/live/validation/README.md).

### Reload-to-window coordinator and exact source sampling

[live_coordinator.zig](src/live_coordinator.zig) retains request-local published
units under explicit byte/unit/packet/window bounds. Incoming units survive
capacity failures, epoch changes drain old windows before clocks reset, and
prepare/commit preserves the cursor across allocation, IO and cancellation
failures. Eviction keeps validated logical IDR preroll, including the first
member of field/partition assemblies, plus earlier-coded future presentation
samples. Delivered windows remain independently owned.

Automatic watermarks cover MJPEG and supported AVC using monotone
scheduled decode clocks with enforced `PTS >= DTS`; AVC validates completed
pictures through the native decoder and caps the frontier at open assemblies
for field-coded and in-band configuration streams. Late/unsupported timing fails before
ownership transfer. Source-time sampling uses exact rational comparisons and
retains its selected picture when preroll is evicted; existing clip-relative
sampling remains available. Empty intervals carry explicit gap events.

The typed `embedding_gemma2.embedNextLive` adapter pumps currently published
units, runs ordinary admitted native/Metal embedding and commits only after
success. Reload/polling remains explicit. Source intervals and HLS/DASH timing
are returned with each event. The adapter reserves conservative host capacity
for validation/staging; limits above 64 MiB or 8192 packets are rejected before
model admission. Current frontier validation decodes retained GOPs repeatedly;
measuring and reducing that extra work remain required. Broader field/dynamic
streamed pretrained parity and cross-window projection reuse also
remain in the full active goal. See
[testdata/live/coordinator-validation](testdata/live/coordinator-validation/README.md).

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

See [qualification receipts](../media/testdata/hls-delta-validation/README.md).


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

See [qualification receipts](testdata/live/metadata-reuse-validation/README.md).


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

See [qualification receipts](testdata/live/reader-selection-validation/README.md).


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

See [qualification receipts](../media/testdata/index-admission-validation/README.md).


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

See [qualification receipts](testdata/selected-output-validation/README.md).


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

See [qualification receipts](testdata/core-admission-validation/README.md).


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

See [qualification receipts](testdata/live/reader-cache-validation/README.md).


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

See [qualification receipts](testdata/live/advance-validation/README.md).


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

See [qualification receipts](testdata/live/idr-cache-validation/README.md).
