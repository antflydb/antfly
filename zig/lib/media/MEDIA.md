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
