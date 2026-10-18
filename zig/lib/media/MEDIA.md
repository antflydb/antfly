# Shared media containers and timelines

`lib/media` owns portable container parsing, packet indexes, source leases,
timelines, live transport and shared admission. [lib/video](../video/VIDEO.md)
owns video reconstruction/preparation; [lib/audio](../audio/AUDIO.md) owns codec
and PCM behavior. Model sampling, tokens and inference belong to model adapters.
The optional object-store adapter supplies authenticated transport without
introducing that dependency into portable container consumers.

[Model-aware chunking](../../../docs/plans/model-aware-media-chunking.md) shares
version-pinned source/index ownership across model windows and returns source
coordinates with optional embeddings through `/ai/v1/chunk`. MP4 borrows bounded
coded packet views over immutable source storage; WebM retains its logical index and portable reference decoder
or eligible Apple VP9/AV1 session across forward windows; WAV ranges decode selected samples. Retained decoder
leases belong to the request pool and outlive individual preparation/model
windows. Backward selections replay independently. Compressed audio visitors
retain codec state and feed bounded blocks into the model window queue, including
HE-AAC/SBR/PS and chained Ogg Vorbis/Opus/FLAC. Logical audio chains reset codec
state and normalize their clocks/channels without a whole-source PCM counting
pass. The first decoded block establishes the output clock; bounded tail
lookahead preserves minimum-duration final windows. EOF supplies the exact sample
count and terminal validation. Unknown-length resampling holds one pending output
frame; WebM FLAC holds only declared discard padding in a budgeted ring. A late
codec/transport failure remains a terminal stream error, even if earlier chunks
were delivered.
HTTP/S3 references enforce immutable versions and exact range responses through
configured scraping security/credentials. Media supplies leases, clocks and
admission; model adapters supply token budgets. Container packets remain distinct
from searchable chunks. Chunk range reads have separate fetched-byte, upstream
request and cache ceilings, independent of whole-object download policy. A partial
last cache fill uses remaining wire capacity, and cache hits do not spend upstream
budgets. Effective operational ceilings are configured through
`NodeConfig.chunking_limits` / `ANTFLY_CHUNK_<FIELD>` and published by model
discovery. Source/output allocation grows shared admission with actual ownership.
The design documents contiguous-decoder and read budgets.

See [qualification and reproduction](../../../docs/plans/native-video-noncuda-audit.md)
for measured results and platform limitations. Historical run logs and progress
reports are recoverable from Git history; generated results belong under ignored
`.benchmark-results/`.

## Sources, clocks and ownership

[source.zig](src/source.zig) exposes immutable borrowed bytes and known-length
positional providers with independently released packet leases. Source identity,
read/retention/total-byte/read-count limits, cancellation and deadlines are
explicit. Source/provider owners remain stable and outlive their borrowed uses.
Advancing a reader never invalidates an outstanding owned payload lease.

[remote.zig](src/remote.zig) supplies version-pinned ranges and bounded forward
read-ahead. Nearby header/packet reads can share one provider request; leases
survive cache eviction. A failed/cancelled fill invalidates its window. Logical
source bytes/reads remain distinct from wire bytes/provider requests. Sequential
sources use bounded spool/scan policy; they do not pretend to offer cheap random
access or silently replace uniform sampling with initial frames. Spool capacity
grows geometrically up to the declared maximum. A moving allocation reserves
old and new storage before copying, and EOF transfers the existing allocation
without a final full-input copy. Retained spare capacity remains admitted until
the snapshot is destroyed; a tight pool may deny the transient growth peak.

[timeline.zig](src/timeline.zig) uses checked signed rational clocks and explicit
rounding. Media DTS/PTS and source presentation time remain distinct. Preserve
negative composition offsets, decode preroll, leading edits, VFR, duplicate
timestamps and gaps; unknown duration is distinct from zero. Half-open windows
use source time. Container readers preserve display/color metadata rather than
silently rotating or converting samples. Audio adapters retain their existing
trim/priming ownership without subtracting delay twice.

[admission.Pool](src/admission.zig) provides atomic nonblocking host/device/command
reservations. Denial does not mutate counters or perform source reads. Move-only
reservations release after resources are destroyed; growth denial preserves the
old reservation. Attach one shared pool to concurrent source/cache/job owners.
Construction accounting includes moving-growth overlap where implemented. MP4
index admission charges actual construction and retained allocations rather than
reserving its default maximum per reader. Limits are explicit ownership budgets,
not physical RSS or opaque driver-memory guarantees.

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

The pure Zig CPU backend uses model-owned F32 GEMM panels as its canonical
prepared matrix representation. Panels are built directly from typed checkpoint
rows, without first allocating a dense F32 mirror. Individual language projections
and packed QKV/gate-up calls share one named panel owner; aligned output-row views
borrow its panels. Portable SIMD operates across output columns with 64-column
panels, 256-element K blocks and bounded row blocks. System BLAS retains its own
kernels. Canonical panels obey the existing shared host budget and class ceilings;
allocation or admission failure uses the typed/direct fallback. Request readers
pin panel owners until teardown, and idle reclamation/unload releases their exact
admission. Optional panels for pre-existing dense snapshots still reserve dense
weight headroom.

CPU row slices retain their source owner, including typed checkpoint mappings,
instead of copying matrix weights. CPU per-layer input projections are grouped
under a 16 MiB projected-output ceiling; long inputs keep the streamed path.
Metal backbones above the small-input frame threshold encode one bounded layer
group while its predecessor executes. MPS activation views transfer to their
encoding frame and retire on its completion. The schedule rolls forward without
draining the successor. The two live groups share the existing 128 KiB/row layer
envelope; scratch released by the successor cannot be recycled by predecessor completion.
Cancellation drops unsubmitted work and drains submitted work before releasing
owners. `ANTFLY_EMBEDDINGGEMMA2_DISABLE_PIPELINED_ENCODER=1` selects serial
submission for diagnostics.

Mixed media cohorts use a bounded readiness scheduler. A group executes once
all its required projected parts are available, while the next CPU preparation
wave runs. Completed CPU preparation buffers and their admission are released
before running ready backbones. A singleton may wait one preparation wave to
coalesce a useful backbone batch; ready work cannot be deferred indefinitely. Waves remain
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


## MP4/MOV and fragmented input

[mp4.Reader](src/mp4.zig) retains codec/configuration, geometry, display matrix,
pixel aspect and raw color declarations alongside decode-order packet metadata.
It supports qualified AVC and complete JPEG visual entries. Indexing reads bounded
metadata, including tail `moov`, while payloads are independently leased.
Container sync flags are hints; video validates actual IDR dependencies before
nonzero AVC starts.

Static indexing supports stsz/stz2, stco/co64, stsc, stts, signed/unsigned ctts,
stss and the qualified leading-empty/rate-1 edit arrangement. External references,
unsupported sample descriptions/edits and out-of-bounds offsets fail. Complete
baseline JPEG samples are independently decoded by video; AVI, abbreviated JPEG
tables and paired/interlaced JPEG fields are outside that route.

Fragment indexing accepts initialization metadata with mvex/trex and explicit
fragment timing/addressing. Selected traf requires tfdt and supported base-data
addressing; trun overrides, first-sample flags and signed composition offsets are
validated against mdat bounds. Static/fragment hybrids, implicit cross-track base
addressing, encrypted fragments and missing decode times fail under this contract.

[live_mp4](src/live_mp4.zig) supports bounded append/initialization configuration
epochs and automatic media framing. Framing parses box structure and lengths;
transport explicitly signals configuration/continuity boundaries where necessary.
Incomplete tails remain pending. Failed validation preserves committed state.
Changing geometry/configuration requires an explicit decoder epoch; it is never
silently reinterpreted as the old track.

## WebM

[webm.Reader](src/webm.zig) indexes VP8/VP9/AV1 tracks, preserving signed nanosecond
PTS, optional durations, invisible/discardable flags and payload offsets. No DTS
is invented. It supports Cues seeking, known/unknown-size Clusters, SimpleBlock
and BlockGroup; unknown Cluster boundaries are found by parsing element structure,
never searching codec payload signatures. A known source length can bound an
unknown-size Segment.

Track crop/display and Colour declarations remain available to video policy.
Laced video, content encodings, dynamic CodecState, interlaced WebM and non-unit
TrackTimestampScale are outside the qualified indexing contract. Codec support
is established by video capabilities rather than the container label. Shared
audio adapters delegate EBML/MP4 container work while retaining PCM APIs.

## Authenticated ranges and object storage

[objectstore.Adapter](src/objectstore.zig) optionally wraps the maintained
object-store clients: borrow an application client or create an owned S3/GCS
client. S3 uses SigV4/dynamic credentials; GCS uses configured bearer credentials
or the existing token manager. Borrowed client/configuration/identity strings
outlive readers. Owned adapters destroy their clients explicitly.

Ranges bind a strong ETag, non-null S3 version or canonical GCS generation.
Transport validates status 206, Content-Range, total/length and pinned version
before publishing bytes. Missing/weak/changed validators, unversioned S3 null,
malformed generations and full-object 200 responses fail. Redirects are disabled
for built-in object ranges; credentials are not replayed to redirected endpoints.
Cancellation retains its original classification. Source identity includes
object identity and immutable version.

## DASH

[dash](src/dash.zig) and hierarchy/cursor/live helpers support inherited templates,
BaseURL/addressing, selected representations, multiple periods and stable live
cursors. Representation/configuration changes form explicit epochs. Timeline and
availability calculations retain checked clocks, bounded expansion and declared
availability policy. Selection does not silently switch quality or codec.

Partial fMP4 resources can stream before HTTP EOF. The cursor remains on the
resource until accepted continuous native decode duration covers its advertised
interval. Clean short EOF leaves it pending; unchanged short body versions do
not duplicate delivery. Explicit inherited availability controls this behavior.
Unsupported manifest/addressing arrangements fail rather than guessing.

## HLS

[hls](src/hls.zig), master/live helpers and the HTTP reader support bounded master
selection, authenticated reload/ranges, discontinuities, fMP4 initialization,
whole-object AES-128 CBC and normalized AVC MPEG-TS. Encryption declarations and
key scopes are explicit; native decryption validates CBC completion/PKCS7 before
final commit. This is not general DRM or encrypted ISO-BMFF support.

Delta reloads preserve skipped maps, key scopes, byte ranges and discontinuities
in independently owned admitted snapshots. Explicit `_HLS_skip=YES` requires
advertised capability and a recent monotone-clock snapshot, preserving existing
authentication query bytes. Missing history triggers one full reload; failed
fetch/validation or overlapping descriptor mutation leaves the cursor unchanged.
Date-range metadata retains immutable deltas and bounded ownership.

Opt-in blocking reload and advertised fMP4 parts retain transactional cursors.
Accepted parts freeze expected prefixes. Parent reconciliation compares native
samples independently of moof boundaries, delivering only a new suffix. Fully
duplicate parents advance without duplicate callbacks. Active resources remain
pinned across retry/reload and compatible unfinished-part fallback. TS LL-HLS
parts remain outside the streamed TS contract.

## HTTP streaming and native MPEG-TS

[http_stream](src/http_stream.zig) parses growing resources under byte/body/window
limits and preserves authentication across supported requests. Initialization and
continuity changes are explicit. Retry validates every acknowledged fragment/AU
prefix before publishing new output; changed or shortened accepted prefixes fail.
HTTP EOF alone does not commit incomplete media or advance a partial resource.

[transport_stream](src/transport_stream.zig) extracts bounded AVC PES payloads
incrementally from arbitrary chunks. Native clocks, continuation flags and final
transactional state agree with buffered extraction. Fixed workspace and actual
buffer growth participate in shared admission. Video performs AVC lookahead,
access-unit normalization and logical-picture assembly; media does not infer a
picture from every PES packet.

The TS reader streams owned normalized units into the video coordinator before
HTTP EOF. It hashes acknowledged metadata/payload and output ordinals; retries
suppress verified accepted prefixes. Source continuity/configuration and manifest
cursor commit after validated completion. Cancellation/consumer failure releases
transient workspace while retaining the acknowledgment ledger. Buffered/streamed
reads share a lifetime received-byte budget, including failed attempts.

## Immutable model windows

Request-owned packet views retain original source offsets and copy only packet
metadata. Dynamic AVC bootstrap data overlays the first packet; other packets
remain demand-read from the original source. Source storage, provider context and
track metadata must outlive every window and preparation worker. Container
identity must name immutable content, such as a digest or pinned object version.
Owned live snapshots remain available when storage must outlive its producer.
Live joins and parameter-prefixed windows fill the final admitted snapshot backing
allocation directly. They retain no second full encoded-byte staging allocation;
packet indexes, identity and retained metadata leases are admitted separately.

Model chunk selection binds this immutable capability and absolute picture IDs,
avoiding coded payload copies and repeated payload hashing. Untrusted standalone
selection APIs retain full content validation. Speculative preparation is limited
to one next window, holds separate request/session host and device reservations,
and starts only after the current decoder has drained. Next-window metadata
planning runs in that wave as well, rather than delaying the current backbone.
The planning worker is joined before its slot, source or request is released. Admission pressure falls
back to serial execution; cancellation joins workers before releasing sources.

Finite AVC metadata scans fetch NAL lengths and bounded header prefixes instead
of entire compressed packets. Prefix buffers are planning metadata, not decodable
media. A bounded optional cache shares the configuration pass with picture
assembly; long clips fall back to sparse header reads when the cache fills.
Original source ranges remain authoritative for decoder validation and playback.

The inference consumer may retain two current immutable windows for a masked
backbone batch plus one next-window preparation wave. All readers and selections
stay in stable slots until completed execution and worker join. Independent
window identity, sample selection, output ordering and continuation semantics
remain unchanged by batching.

Model-owned language, vision and audio preparation caches have separate payload
ceilings under shared process admission. Cache capacity grants no extra memory.
Idle class reclamation happens before retrying an unchanged run reservation;
CPU compute owners pin borrowed immutable mirrors until request teardown, and
Metal refuses reclamation while a command group is active or submitted. Warm
model admission reuses the loaded generation's validated metadata; source
version, content policy and execution identity validation still apply.

## Verification

From the repository root, with its pinned Zig toolchain:

```sh
zig build --build-file zig/lib/media/build.zig test-media test-media-http -Doptimize=ReleaseSafe
zig build --build-file zig/lib/media/build.zig test-media-objectstore -Doptimize=ReleaseSafe
zig build --build-file zig/lib/video/build.zig test-live-windows test-transport-http -Doptimize=ReleaseSafe
```

[Fixture provenance](testdata/README.md), source-adjacent tests and generators
cover leases, malformed input, limits, allocation failures, authenticated loopback
HTTP, cancellation and retry. Portable tests and actual Linux execution are
separate from Apple device qualification. Cloud-client tests use local endpoints;
they do not establish arbitrary cloud/origin interoperability. Model-level
reference preservation and final source scopes are recorded in the
[non-CUDA qualification summary](../../../docs/plans/native-video-noncuda-audit.md).

Optional Apple decoding remains in `lib/video`; media sources and container
indexes stay portable. WebM header indexes now carry packet bindings, codec
configuration and colour metadata so VideoToolbox and Zig replay use the same
logical selections. Apple release artifacts declare a macOS 15 deployment target;
framework SDK availability does not imply a hardware decoder for every stream.

CUDA video decoder caches retain their own shared backend scratch lease across
request completion. Per-request media/model permits do not cover this retained
state. Allocation is admitted before decoder creation, and eviction/destruction
releases both the context and shared reservations. See
[VIDEO.md](../video/VIDEO.md#cuda-decoder-cache-accounting) for the bounded surface
envelope and NVIDIA hardware qualification requirement. Embedding identity pins
also distinguish CUDA BF16 execution from the existing CPU/Metal F32 recipe.

CUDA mixed-content preparation now shares the existing ordered/manual-marker
parser with the host frontend while retaining image/audio projections and
composing text/media rows on device. BF16 text lookup/scaling occurs before media
replacement; projected rows remain unscaled. Backend handles and execution locks
outlive prepared tensors. Packed projection and graph caches hold separate
retained admission, with bounded storage and synchronized ownership transitions.
See [CUDA_TUNING.md](../../pkg/inference/CUDA_TUNING.md#embeddinggemma-2-encoder-optimization-profiles)
for optional arithmetic profiles and qualification requirements.

CUDA ordered text/image/audio groups use the prepared-wave scheduler when the
session's `MEDIA_BATCH` qualification profile is enabled. CPU preparation overlaps
device execution; projected rows remain resident and the ordered composer keeps
manual placeholders, Pillow image policy and reference audio resampling intact.
Length buckets run the BF16 backbone in batches of at most four. Queue cancellation
continues to use live cohort members and original result indexes. The profile is
part of the embedding identity and defaults off pending NVIDIA qualification.
Singleton CUDA mixed requests retire preprocessing admission after composition;
image header geometry determines the initial bounds.

EmbeddingGemma2 Metal audio cohorts encode bounded groups of conformer layers
with at most one submitted group and one encoding group. The group size is
limited by a 384-MiB activation envelope inside the existing audio reservation;
large inputs retain synchronous single-layer execution. Matrix views detach to
their command frame before rotation. Cancellation drains submitted work before
layer inputs/weights leave scope, and external frame owners keep submission
control. Diagnostics and `ANTFLY_EMBEDDINGGEMMA2_DISABLE_PIPELINED_AUDIO=1` retain
the per-layer wait path for comparison.

CUDA audio preparation derives its sample limit from the same model config for
singleton and mixed cohorts. Header duration bounds include media wrappers;
unknown lengths use the full token ceiling without a PCM inspection pass.


Audio cohort projection packing is selected by a checked 64-MiB activation
budget, including clips beyond the former 512-row threshold. Metal and CUDA
attention and depthwise convolution process segmented cohorts directly into the
final output allocation. Shared row metadata resets chunk alignment and causal
taps at each clip boundary; signed validity tags distinguish invalid rows from
out-of-clip keys even with finite mask values. Backend declines retain isolated
per-clip execution. Metadata stays alive until all submitted groups complete.

CUDA packed projection consumers read QKV and gate/up GEMM rows directly, keeping
head RMS, text rotary and GELU BF16 rounding boundaries. Vision head fusion is
limited to attention routes that already consume BF16 rotary output. The fused
backbone profile also combines vision residual/norm passes. Optional BF16-output
GEMM tuning uses bounded completed-device trials and a context-owned bounded
cache; cache misses decline during graph capture. These profiles remain opt-in,
carry versioned model identities and require NVIDIA vector/ranking and timing
qualification. See [CUDA tuning](../../pkg/inference/CUDA_TUNING.md) for controls,
memory bounds and observed execution gates.

CUDA vision replay shares the encoder's four bounded arena slots. Grid axes and
batch identify retained position tables; workspace/weight changes invalidate
captured execution. BF16 towers capture after warming, while transient F32
attention keeps eager ownership. Short encoder graphs avoid the former 64-row
minimum and use completed warm latency to choose eager/replay submission.

Spatial projector preparation has direct CUDA/Metal kernels for ordered patch
merging and channel standardization, without host index tables or intermediate
gathers. RMS keeps the backend's established reduction. CUDA `SPATIAL_POOL`
remains a separately identified opt-in profile pending NVIDIA qualification.

CUDA deferred frame/image cohorts resize into final patch row spans and share a
single patch-projection GEMM. Bounds and per-item fallback preserve ordered,
bounded preparation without growing concatenations. Vision `a` scratch is
hidden-width, allowing four 480×480 frames to fit the unchanged graph arena cap.

Audio preparation shares fixed immutable frontend tables. CUDA device constants
have an 8 MiB / 128-entry admitted cache; warm query scales require no per-layer
readback. Snapshot borrowers retain memory/admission through eviction. Neither
cache stores clip data, and unusual frontend configurations retain private plans.


Accelerator audio preparation uploads PCM once and windows on device, with
fused magnitude and log/mask operations. Equal-length audio clips batch through
subsampling without temporal padding; masks and output order remain clip-local.
CUDA/Metal normalization and final flatten index batched NCHW storage directly,
removing five full transposes. The fixed 512 FFT uses shared immutable twiddles,
Zig SIMD across CPU frames and cooperative accelerator blocks. BLAS builds choose from
completed FFT/BLAS log-mel timings in a bounded eight-geometry policy; a forced
DFT reference remains available. CUDA implicit-GEMM convolution and FFT are
separately identified opt-in candidates pending NVIDIA qualification.
CUDA clamp metadata uses immutable scalar lookups before packed projection.
CUDA graph retention shares the existing memory ceiling across reuse-aware
vision/backbone descriptors and fences only evicted storage by completion event.

The fixed audio frontend can fuse windowing, FFT, sparse mel projection and
log/masking in one accelerator block, with one caller-owned or preparation-owned
Metal command frame. F32 SIMD-group matrix convolution reuses NCHW tiles on
compatible Metal devices, with tiled F32 and direct fallbacks. Batch admission
counts current live/retained buffers and the CUDA masking buffer; legacy backends
retain the transpose allowance under the same 128 MiB limit.

CUDA conformer plans are opt-in. Actual eager requests warm and learn bounded
buffer lifetimes before capture. Stable input/mask/segment/scale storage, immutable
prepared depthwise weights and generation-aware keys preserve replay ownership.
Audio shares vision/backbone graph admission and completion-fenced eviction;
capture/admission declines retain eager execution, and cancellation remains
retryable. Kernel and graph qualification require actual route counters.

Metal conformer scalar-clipped projections have an F32 SIMD-group matrix route
that applies input bounds during tile loads and output bounds/SiLU before the
final store. A 32-entry geometry/operation cache compares completed GPU time with
staged MPS, excludes three warmups, and requires a 5% win; denied or unsupported
qualification preserves staged execution. Cold tuning uses separately admitted
scratch of at most 32 MiB and never consumes an unsubmitted request activation.

Audio execution can retain private Metal scratch across frames in an optional
cache with 128 buffer descriptors and a 128 MiB ceiling. Page-rounded buffer
allocations are admitted individually. Each retired
buffer retains its producer command buffer; reuse and eviction require completion.
Cancellation publishes no storage, pressure reclamation drains the cache, and
model teardown returns the reservation. In-frame reuse remains the first choice.

Compatible 128–1024-wide Metal attention heads distribute context positions
across SIMD groups while preserving the existing dimension-reduction and softmax
order. Narrow heads keep the original kernel. Audio feature cohorts share one
preparation frame and preserve externally owned frames.

Compatible audio FFNs also fuse weighted RMS normalization with their scalar
input bounds. This removes a normalization/clamp intermediate and dispatch even
when staged MPS wins projection qualification. The established reduction order
and vector multiplication order are retained; unsupported bounds/layouts use
the reference route. `ANTFLY_EMBEDDINGGEMMA2_METAL_AUDIO_STATS=1` reports actual
fused-linear, norm/clamp, parallel-attention and retained-storage route counts.
F32 device-weight RMS normalization also shares this SIMD4 row reduction when
no input bounds are requested, preserving the reference fallback for other
layouts and arithmetic policies.

Metal audio retention charges page-rounded buffer allocations when they enter
retained storage, rather than reserving the entire ceiling per model. Borrowing
and eviction return their charges; releases reconcile at safe residency-lock
boundaries. Retention declines on admission or lock contention, preserving eager
execution. Completion fences, cancellation cleanup, and model pressure reclamation
remain mandatory.

Nearby Metal audio lengths can share subsampling convolutions with at most
12.5% geometry padding. Device-only NCHW padding zeros artificial rows after the
first normalization, preserving the shorter clip's second-convolution boundary.
Compaction uses the padded batch stride and returns each clip's original rows
and mask. Existing exact-length CUDA batching is preserved.

Metal audio schedules bind current inputs to prepared immutable model slots
and coefficient buffers. They retain no request tensors, allocators, or weight
pins. Sixteen admitted geometry/range entries cover singleton execution and all
twelve possible cohort frame groups; recurrent shapes receive priority over
one-off requests. Model pressure releases schedules before their model entries.
Capture uses request-local scratch; retained instruction/constant arrays are
sized to the actual capture. Small coefficients share a packed buffer (at most
4 MiB per plan). Non-escaping linear and nonlinear outputs share a lifetime-planned
private arena (at most 64 MiB per plan), with model-owned tensor descriptors reused
on replay. View ancestors extend lifetimes; returned storage remains independently
owned. Oversized geometries retain ordinary output allocation.
Host metadata and physical device allocations are admitted before retention,
including driver padding. Denied optional preparation preserves eager results
and retries with bounded backoff, so a transient denial cannot permanently lose
prepared bindings or the arena.

Replay checks current request control at every instruction and uses shared
compute encoders within each existing command frame. Cohorts keep their bounded
pipelined frame groups and cancellation fences. Prepared fused linear routes
are independent of the evictable tuning cache; unprepared routes can complete
with staged device operations if fusion declines. Five interleaved warm pairs
compare median encoding times, rejecting isolated outliers. A slower plan cools
down for eight visits before retrying; a winning plan resamples after 64 visits.
Commands are still encoded per execution; this is not a GPU command graph.
Transposed depthwise coefficients use the model preparation cache across weight
sources. `ANTFLY_EMBEDDINGGEMMA2_METAL_DISABLE_AUDIO_PLAN=1` selects eager scheduling.

The Metal backbone uses model-owned indexed coefficient bindings and projection
slots across text and media requests. Norm coefficients share one admitted
buffer; layer scalars remain validated host values. Model pressure drops these
bindings before releasing language projection slots. A two-entry LRU retains
full-mask attention ranges, positions and aligned device metadata, keyed by
batch, sequence length and window. Warm reuse allocates no request descriptors
or upload buffers; padded or sparse masks retain dynamic preparation. Geometry
storage shares model admission and pressure cleanup. Optional preparation can
retry after admission pressure clears. CPU audio waves prepare and wait outside
the execution gate; each ready wave acquires it for device execution and releases
it before waiting for the next wave. Invalid CPU inputs require no execution
lease. Preparation leases, cancellation checks and ordered output ownership
remain in force.


Metal F32 backbone execution also retains up to two admitted activation layouts,
keyed by row count and projection geometry. Six non-overlapping projection
lifetimes share one range; scratch norms, attention/FFN residuals and alternating
hidden rows have separate ranges. Normal and global-attention QKV shapes retain
separate descriptors over the shared projection range. Each layout is capped at
64 MiB; oversized or denied layouts keep the existing allocation path, and
transient admission denial retries after eight forward visits. Model pressure
releases layouts before language matrix slots. Request outputs remain independent.
Unsupported fused-kernel capabilities decline before encoding to the existing
device path. Reusable private outputs do not retain host readback snapshots;
planned writes invalidate any mirror populated by a host fallback. These are
reusable output views with ordinary command encoding.

Audio arena layout splits oversized expired ranges and coalesces adjacent expired
ranges before growing storage. This avoids fragmentation when packed producers
replace several smaller outputs. Seeded randomized lifetime checks verify that
simultaneously live outputs never overlap.

Audio attention can read packed QKV rows directly, applying each projection's
optional output clamps at load time. This removes dense Q/K/V slicing and clamp
intermediates while preserving segmented masks, query scaling and reduction
order. Backends without this capability keep the split path. Metal pooling and
final normalization process batch members in batched dispatches and write the
final rows directly, preserving truncation before normalization.

Single-media token templates use an eight-entry model-owned cache keyed by loaded
immutable tokenizer generation and instance, media kind, soft-token count and
token limit. Custom tokenizers without an immutable generation keep normal
encoding. Misses use the
original tokenizer; warm requests reuse its exact IDs and masks. Retention is
admitted and model pressure releases it. Metal composition looks up only wrapper,
text and padding positions, then merges those rows and projected media in one
indexed device dispatch. Unsupported kernels retain the row-copy path. Masked
padding retains its original token lookup values.

`ANTFLY_EMBEDDINGGEMMA2_METAL_DISABLE_FOLLOWUP=1` selects the previous execution
policy for qualification; this policy participates in audio schedule identity.
`ANTFLY_EMBEDDINGGEMMA2_BENCH_METAL_FOLLOWUP=1` adds the completed alternating-route
comparison to the ReleaseFast pretrained ordered-media oracle, retaining the
previous prepared bindings and audio arenas on both routes.
