# Native video decoding and sampled surfaces

Status: phase 1 frame selection implemented, 2026-10-08. Video codec decoding,
surface ownership, inference integration, and Metal execution remain planned.

Related documents:

- [Shared media containers and timelines](../media/MEDIA.md)
- [Image codecs and preprocessing](../image/IMAGE.md)
- [Audio codecs and PCM](../audio/AUDIO.md)
- [Native video inference implementation plan](../../../docs/plans/native-video-inference.md)

## Goal and boundary

`lib/video` will decode container packets into timestamped, leased surfaces and
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
`zig/` with Zig 0.17. This phase does not decode pictures or return embeddings.

## Proposed module and API shape

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
coverage will not cover typical CABAC/B-frame streams. Add broader H.264 tools
only with independent vectors and real-stream qualification.

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
