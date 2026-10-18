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

Apple VP9/AV1 WebM also exposes a request-owned
[continuation](src/backends/apple_webm.zig). Forward windows submit only newly
required packets; backward windows use independent verified replay. A stable
native callback owner borrows each window's capture until a drain fence completes,
then clears that pointer. Consumer/decode failure destroys native references and
releases admission before retry. One caller may use an owner at a time; concurrent
calls fail explicitly, and callers must join before destroying it. Native
reference estimates belong to the request-lived pool, separate from selected
surfaces and temporary preparation buffers. Model integration also reserves shared
session preprocessing capacity for the retained CoreVideo backing.

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
Forward AVC windows retain bounded portable H.264 or VideoToolbox reference
state. Portable reuse includes separate colour planes and dynamic parameter-set
updates within compatible configuration epochs; geometry/representation epochs
construct fresh decoder owners. Portable VP8/VP9/AV1 WebM windows retain reference
and entropy state across forward selections. Backward selections and outputs
already consumed within a multi-output packet use verified independent replay.
Eligible Apple VP9/AV1 WebM windows also retain native reference state. Dependency bytes remain bounded
per window even when reconstruction reuses references. Native decoder callbacks
drain before window release; retained Apple surfaces keep their admission until
decoder destruction. Request-local projected reuse and bounded preparation overlap
apply on CPU/Metal.

Finite MP4/WebM requests build one augmented presentation index. Each timestamp
query preserves decode-order ties, reordered PTS, overlapping VFR holds, gaps and
unknown-duration semantics in O(log N), rather than scanning the whole clip.
MP4 also indexes logical packet intervals for field/partition closure and keeps a
request-owned nearest-verified-IDR cache. The cache stores indexes, not packet
leases, and requires immutable source identity, track configuration and metadata.
Finite advanced AVC uses transport header assembly for its logical picture index:
field parity/identity, independent colour planes, partition packet spans and
configuration epochs are indexed without reconstructing the clip's pixels.
Selected dependency decoding still validates entropy, coverage and references.
Live publication retains the reconstruction-based index so incomplete pictures
cannot become visible. Both indexes have differential fixture coverage.
All retained metadata participates in request admission. The next window's
selection, dependency planning and preparation run off the current backbone's
critical path after its decoder drains. Every exit joins that planning worker.
Busy model-provider queues yield retryable admission pressure. Unexecuted chunks
do not advance acknowledged coverage or become permanent per-chunk failures,
including when partial results are enabled.

Known text/video input shapes derive sequence, host and workspace reservations
from inspected geometry, preparation concurrency and conservative attention/
activation bounds. Host allocation enforces the advertised ceiling; rendered
sequence length is checked before backbone execution. Uninspected image/audio
inputs retain the conservative initial reservation. After rendering, every media
type reduces its execution permit using the actual sequence, retained host
owners and projected inputs before starting the next window. Prepared image and
audio cohorts also derive sequence/output and host allowances from their known
rows. Reductions reapply the session's workspace profile and cannot grow a lease.
The allocator enforces the reduced host ceiling. Bounds do not alter sampling policy.

CPU and Metal ordered media groups may share a prepared backbone cohort,
including video and mixed prompts. Towers retain independent source clocks and
part ordering; length buckets bound padding and restore result order. CPU
attention keeps grouped K/V compact and uses bounded dense vision tiles.
Audio cohorts share conformer projections/FFNs while resetting temporal kernels
at clip boundaries. Device media tokens compose without intermediate activation
readbacks. Preparation leases retain ownership while separately admitted encoder
supplements cover larger peaks; cancellation drains backend owners before both
leases release. Cohort pressure falls back only after complete cleanup.
Compatible mixed-group image/audio parts also share tower waves, with one
separately admitted CPU preparation wave overlapping current tower execution.
Scatter maps preserve source part order; errors retain their group identity.
Embedding output keeps token RMSNorm before masked pooling, projects once per
group and normalizes compact vectors. Metal retains the batch through one final
readback. Packed model-owned projections share existing bounded cache classes;
audio packing preserves independently declared clipping bounds.

Flat text arrays tokenize once and share length-sorted encoder calls, with at
most four items, 8192 padded rows and 25% padding overhead. Results scatter to
input order; admission pressure drains and splits a cohort before retrying.
Explicit alternate pooling/projection configurations retain their own contract.
Metal grouped GEMM tiles stop at disjoint item ranges and compute only each
tile's visible key envelope; score storage uses that envelope's stride. CPU
portable grouped attention shares packed K/V across bounded head workers, with
sequential inner GEMMs. BLAS retains its own worker scheduler.
Backbone head preparation reads packed projections directly and combines head
RMSNorm, split-half RoPE and row gathering/compact-KV expansion. Gather indices,
window ranges and aligned device visibility metadata are prepared once per
forward call and retained until completion/cancellation. Vision FFNs share
admitted packed gate/up weights and consume them directly on CPU/Metal without
slice buffers. Image/video position lookups preserve typed table storage and
gather the required rows instead of copying the full 60 MiB F32 position table.

Language QKV and gate/up projections use one canonical packed snapshot for all
admitted sequence lengths, including inputs above 512 rows. Native individual
projections borrow output-row slices; Metal applies output-row views directly
from the parent slot. Neither view creates a second persistent weight payload.
The model lease protects views, and cache pressure reclaims the parent as one
allocation. Packed QKV/head preparation and packed GELU avoid extraction copies;
activation storage remains bounded by the existing admitted row envelope.

The pure Zig CPU backend also prepares immutable F32 GEMM panels for reused
matrix weights. Panels use portable SIMD across output columns, 64-column
panels, 256-element K blocks and bounded row blocks. Tiny inputs and system
BLAS retain their existing kernels. Panels are admitted within the same model
and class ceilings as dense snapshots; allocation/admission failure falls back
to direct GEMM. Admission also protects space for unwarmed dense weights, so
optional early panels cannot force late layers to widen on every request.
A request reader pins both representations. Idle reclamation
releases each parent and its panels together, and unload releases all owners.

Mixed media cohorts use a bounded readiness scheduler. A group executes once
all its required projected parts are available, while the next CPU preparation
wave runs. A singleton may wait one preparation wave to coalesce a useful
backbone batch; ready work cannot be deferred indefinitely. Waves remain
homogeneous batches of at most four items but can
cross image/audio boundaries. Text-only groups can overlap the first media
preparation wave. Finished groups release projected tokens promptly; public
result order, per-group failures, cancellation and shared admission remain
unchanged. GPU tower and backbone work stay ordered on one leased backend.


Metal grouped attention folds query heads sharing a KV head into matrix rows,
then batches distinct KV heads for dense key envelopes. Partial envelopes
encode one QK/PV pair per KV head, avoiding padded RHS batch strides and K/V
expansion. Range/window masking stays exact. Query packing and score/output
storage are bounded by the current tile; padded tail rows are not multiplied.
Grouped K/V and resident visibility metadata now cover short queries too;
metadata is uploaded once per forward, rather than once per layer. Queries
below 64 rows with at most 64 keys, or visible envelopes through 32 keys,
use a head-width-specialized SIMD-group kernel with no threadgroup scratch.
Aligned 512-wide global heads with key
envelopes through 128 use fused 64-key F32 attention. Local envelopes through 1536 use GEMM,
even above 512 total rows. Longer global heads retain GEMM when its workspace
fits. The 1536-key bound covers a 512-query tile plus two 512-key windows.
When GEMM workspace cannot be admitted, global heads use fused 64-key tiles
on aligned shapes and a tail-safe 32-key variant otherwise. These kernels
retain online softmax and F32 matrix accumulation without materializing score
tiles, with 21,632 and 27,776 bytes of threadgroup storage respectively.
Device capability checks retain the original compact fallback. Explicit
strategy, query-tile and key-block overrides are reserved for qualification;
workspace limits and cancellation still apply.

Audio relative-position inputs, query scales, normalization ones and completed
per-layer relative-key projections are cached as immutable model constants.
Prepared constants have an 8 MiB/128-entry ceiling within the audio class and
shared memory admission. Device publication copies into fresh model-owned
storage and waits for completion; failed copies are not published. Cancellation
releases the consumer's handle; completed snapshots remain reusable on retry.
Warm device execution performs no scale readback. Audio pressure reclamation and model
unload release these constants alongside prepared weights.

Backbone post-normalization/residual epilogues use one backend operation,
including the final per-layer output scale. PLE GELU and multiplication share
one activation operation. Audio uses the same post-normalization/residual
primitive, with its residual weight applied before addition, and combines
scalar output clipping with SiLU. CPU uses portable Zig SIMD; Metal uses
precise F32 kernels. The operation contract preserves normalization, weight,
scale, addition and final-scale order. Unsupported backends retain the
separate-operation path. Model-owned scalar clamp metadata is bounded to 256
entries with keys of at most 192 bytes, owns no request tensors, and is released
on model teardown. Mutable compute contexts do not publish it.

Cold Metal packed projections populate the final owned F32 snapshot directly
from mixed BF16/F32 checkpoint segments. No combined host F32 matrix or
per-segment F32 staging is retained. Shared storage is used on unified-memory
devices; other Metal devices blit that same generated staging buffer directly
to private storage, avoiding a second shared upload allocation.
Publication remains atomic: failed preparation preserves any previous slot,
and the existing class/admission limits charge the final allocation.


Descriptors, model identity, gaps and terminal coverage travel with the vectors.
Overview sampling remains distinct from retrieval coverage; audio inclusion stays
opt-in. See the design for source references, admission bounds and qualification.

`pipelines/embedding_gemma2_video.zig` connects logical MP4/WebM/normalized TS
pictures to the pinned default 1 FPS, uniform 32-frame policy and explicit sampled
windows. Video uses 140 pooled 16x16/3x3 soft tokens per frame, distinct from image
geometry. Mixed text/image/audio/video preserves order and one embedding per
group; the total sequence bound is 8192 tokens. Video audio is not implicitly
embedded. Equal-geometry frames form cohorts of up to eight, bounded by the
vision scratch estimate (384 MiB native, 768 MiB Metal). Queue capacity also
respects admitted preparation slots and total selected pictures. Consumers take
available work without waiting to fill a cohort; incompatible geometry flushes
separately. A bounded process-local calibration table measures completed cost per
frame separately by backend and geometry. It explores 1/2/4/8-frame candidates,
then prefers smaller allocations unless a larger cohort improves measured
throughput by more than five percent. Paired minima exclude isolated cold setup
or scheduling stalls, and smoothed periodic probes adapt to sustained contention.
Calibration never overrides admission or waits for work. Metal concatenation uses
one destination and linear row copies; image geometry cohorts and audio projections
use the same linear assembly rule. Gemma 2 vision calibrates one- versus two-layer
Metal command frames separately for each geometry and frame-batch size, retaining
activation leases until completion/cancellation. Cold starts and timing ties retain
the qualified deeper grouping; shallower frames require a measured improvement
over five percent.
Scratch estimates include both layers; large image geometries retain single-layer
frames to preserve their existing memory envelope. External frame ownership is
preserved.

Metal retains vision/projector results, token composition, backbone inputs and
final pooling/normalization until response readback. EmbeddingGemma 2 selects
full-F32 tiled vision attention, preserving float inputs and softmax probabilities.
CPU validation and response serialization remain host operations.

The provider retains 114 eligible immutable vision/projector F32 matrices.
Vision/language each have a 640 MiB ceiling and share model/process admission and
a combined 1536-entry bound. Relevant pressure yields idle vision weights, then
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

## Window execution costs

Apple WebM continuations validate compatibility once for their immutable picture
and restart index. Decoder packet limits apply to the submitted dependency span,
including warm continuation or backward replay, rather than total clip length.
Changing an index requires a new owner. Backward replay preserves forward state.

MP4 chunk windows borrow immutable coded storage, including range sources. Dynamic
parameter sets use a first-packet bootstrap overlay. Request-bound selection keys
combine immutable source/track identity with absolute picture IDs, so overlapping
windows do not copy or rehash their coded dependencies. Standalone content-bound
selection still verifies payloads. Sources and index storage outlive all workers.

Resident frame projections assemble into one final tensor with device row copies,
including variable token counts and repeated sampled pictures. CPU fallback uses
one host destination. Preparation of one following window may overlap the current
language backbone, after projection consumption has joined the current decoder.
Lookahead has independent host/device admission, bounded queues, serial retry on
resource pressure, and joined cleanup on early stop or cancellation. It does not
share language KV state between independently contextualized embeddings.

Native unmasked vision attention uses tiled F32 streaming softmax, including
parallel head execution when an Io worker context is available. Empty masks mean
all keys are valid; they do not force a quadratic score-matrix allocation.

The chunk endpoint groups up to two already available, compatible video windows
for one backbone forward. The native batch API accepts up to four. Each window
keeps its own positions, key mask and normalized output; no contextual state is
shared. Cohorts exceeding 2048 estimated tokens per row or 25% padding execute
independently. Metal cohorts also preserve the singleton frame/PLE fusion regime
at the existing 128/512-row boundaries; crossing a faster kernel regime is not
assumed to improve throughput. Independent fallback retains between-window
preparation overlap and consumes a still-valid prefetched producer.
Projection/decode stays ordered for retained codec continuations.
One following window can prepare during the batched backbone. Batch admission
pressure drains consumed producers and retries individual windows, while output
ordinals advance only after each sink acknowledgement. Cancellation and sink
failure release computed but unpublished vectors and join the next worker.

Finite AVC planning reads bounded NAL prefixes, skips entropy by original NAL
length, and reads complete bounded SPS/PPS syntax. Configuration discovery caches
prefixes for picture assembly in a separate admitted metadata budget. Cache
pressure discards that optional cache and continues sparse reads; it does not
reduce the parameter-set budget or impose a new clip-length limit. Planning
releases cached prefixes afterward. Decoder submission always uses original
coded packets, never the synthetic prefix buffers.

## Verification

Use the repository Zig toolchain. From the repository root:

```sh
zig build --build-file zig/lib/video/build.zig test-video test-live-windows -Doptimize=ReleaseSafe
zig build --build-file zig/pkg/inference/build.zig test-video-model -Doptimize=ReleaseSafe
python3 -m unittest discover -s zig/pkg/inference/scripts/embeddinggemma2 -p 'test_*.py'
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

### Sampling benchmark

`zig build --build-file zig/lib/video/build.zig bench-sampling -Doptimize=ReleaseFast`
compares 256 windows over a 108,000-picture timeline with the linear oracle.
It reports index construction and lookup separately and verifies matching
checksums. This measures metadata selection, not decode or model throughput.

`zig build --build-file zig/lib/video/build.zig bench-picture-index -Doptimize=ReleaseFast`
alternates header and reconstruction indexing on the same advanced AVC fixtures,
checks complete picture metadata equality, and reports aggregate elapsed time for
eight runs per case. This isolates indexing cost; full-pipeline throughput still
requires the pretrained/load qualification tools.

Model execution uses named, model-owned packed vision QKV snapshots and fused
normalization/2-D RoPE. Frame/window batches can cross the former 512-row
backbone cutoff: packed per-layer input storage has a 64 MiB ceiling and larger
backbones use a rolling two-slot schedule, encoding a successor while its
predecessor runs. Activation views belong to their encoding frame and retire
when that frame completes, without draining the successor. Both groups
share the existing layer memory envelope; cancellation drains submitted work
before releasing owners. CPU per-layer projections use retained weight-row views
and bounded projection groups instead of copying each layer’s weight slice.
Independent masks, positions and pooling remain per window. Shared admission,
producer ownership and cancellation boundaries are unchanged. Cache admission
failure retains bounded preparation; cache reclamation cannot invalidate active
CPU borrowers or Metal commands. These mechanisms establish reuse and bounded
lifetimes; speed comparisons require the pinned per-case performance gate in
[the model guide](../../../docs/guides/embeddinggemma2.md#per-case-performance-gate).

### CUDA decoder cache accounting

Each cached NVDEC decoder owns its admission lease independently of the request
that created it. Sequence negotiation reserves both the context's physical device
budget and shared backend scratch admission before driver allocation. Compatible
clips reuse the same reservation; resize, failed creation, error cleanup, eviction
and context teardown release it after decoder destruction. Changing admission
controllers first evicts the old decoder under the session execution lock. Video
requests denied admission evict their idle decoder and retry admission once.

NVDEC does not report an exact allocation size. The policy reserves 64 MiB of
opaque driver headroom plus twice the aligned NV12 payload for decode surfaces,
one mapped output and one working surface. Width is aligned to 256 bytes and
height to 32 rows, with at most 4096 by 4096 pixels and 32 decode surfaces. This
is a conservative policy envelope, not measured driver memory. NVIDIA hardware
qualification must check that envelope against completed-driver memory deltas.
See NVIDIA's [NVDEC programming guide](https://docs.nvidia.com/video-technologies/video-codec-sdk/13.1/nvdec-video-decoder-api-prog-guide/index.html)
for decode/output surface allocation behavior.

### CUDA staged frame cohorts

The EmbeddingGemma 2 NVDEC consumer converts each selected coded picture to owned
RGB storage and records a CUDA event before resize/projection. Surface unmapping
waits for that event, so subsequent vision work does not hold the decoder surface
and CPU/NVDEC decoding can overlap queued vision execution. Consumer failures
still drain all queued reads before unmapping.

Repeated model samples of one coded picture share staging and vision projection;
the projected token rows and timestamps are repeated in their original logical
order. The default singleton route transfers its hidden tensor directly into the
tower. The image-batch qualification profile permits bounded cohorts of four
compatible pictures, flushes geometry changes and the final partial cohort, and
preserves the 32-frame/model-token limits. Cleanup drains failed work and owns all
partial staging slots. Batched cuDNN plans preserve independent attention per
picture. Profile identity and NVIDIA qualification are documented in
[CUDA_TUNING.md](../../pkg/inference/CUDA_TUNING.md#embeddinggemma-2-encoder-optimization-profiles).

The CUDA EmbeddingGemma2 resize path caches coefficient plans by source/target
geometry and pixel preparation policy. Model-owned plans and pinned copy staging
remove success-path per-picture stream drains; source slots wait for H2D events,
while NVDEC surface release retains its separate RGB-conversion fence. Retention
is bounded and shared-admitted, with a synchronous uncached fallback on pressure.
The `NATIVE_VISION` qualification profile runs batched native online-softmax
attention without score/probability arenas or cuDNN. Video admission drops the
completed media peak before the BF16 backbone, using the actual expanded length.
These changes do not establish NVIDIA speedups without completed-device timings.

The native CUDA vision tower now keeps attention inputs/output in BF16. Rotary
packing preserves the existing F32 arithmetic and conversion boundary, while the
native head-64 kernel stores BF16 output directly for projection. This removes
per-layer whole-tensor F32 bridges without changing the tiled attention reduction.
The opt-in native-vision identity is versioned for the typed BF16 implementation.


CUDA media cohorts pack composed BF16 rows directly for the text backbone.
Resident F32 video producers remain accepted and convert directly into the final
BF16 encoder arena, including zero padding. Optional graph replay supports up to
four items and 4096 padded rows, with shape-specific addresses, bounded retained
admission and synchronized eviction. Grouped immutable PLE projections use a
16-MiB output budget rather than retaining all 24 layers for long contexts.

CUDA packed projection consumers read QKV and gate/up GEMM rows directly, keeping
head RMS, text rotary and GELU BF16 rounding boundaries. Vision head fusion is
limited to attention routes that already consume BF16 rotary output. The fused
backbone profile also combines vision residual/norm passes. Optional BF16-output
GEMM tuning uses bounded completed-device trials and a context-owned bounded
cache; cache misses decline during graph capture. These profiles remain opt-in,
carry versioned model identities and require NVIDIA vector/ranking and timing
qualification. See [CUDA tuning](../../pkg/inference/CUDA_TUNING.md) for controls,
memory bounds and observed execution gates.
