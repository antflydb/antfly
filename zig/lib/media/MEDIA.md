# Shared media containers and timelines

Status: shared audio demux, bounded sources/timelines, static and fragmented
MP4 video indexes, WebM video indexing, remote range adapters and shared admission
are implemented, 2026-10-08. The broader
reader and seek contracts below remain planned unless listed as implemented.
The initial design checkout is based on `origin/main` at
`cdf572a7467d581f6f1b39bcf514878488555f11`. A remote refresh was unavailable.

Related documents:

- [Video decoding and surfaces](../video/VIDEO.md)
- [Existing audio runtime](../audio/AUDIO.md)
- [Existing image runtime](../image/IMAGE.md)
- [Native video inference implementation plan](../../../docs/plans/native-video-inference.md)

## Purpose and ownership

`lib/media` owns pure Zig container parsing, track discovery, packet access,
seek indexes, and presentation timelines shared by audio and video. It will not
decode codecs, prepare model tensors, select a model's frame rate, or perform
network authentication. Those responsibilities belong to `lib/audio`,
`lib/video`, inference adapters, and source providers respectively.

The existing `lib/audio/src/mp4.zig` and `webm.zig` entry points now delegate
container work to `lib/media/src/mp4_audio.zig` and `webm_audio.zig`. Audio codec
decoding and the public PCM boundary stay in `lib/audio`. The migration preserves
qualified AAC/ALAC access units, edit/trim metadata, WebM timing, and lacing.

## Implemented phase 1 API

- [source.zig](src/source.zig): immutable borrowed bytes or known-length range
  providers, a positional file adapter, independent packet leases, cancellation
  and deadline callbacks, and read/retention/total-byte/read-count limits. Sources
  are single-consumer and must remain at a stable address until leases release.
- [isobmff.zig](src/isobmff.zig) and [ebml.zig](src/ebml.zig): shared checked
  container primitives; the audio adapters preserve their legacy error boundary.
- [timeline.zig](src/timeline.zig): checked signed rational rescaling with floor
  rounding and explicit media-to-source edit mapping.
- [mp4.zig](src/mp4.zig): first supported `avc1` or `jpeg` video track, explicit codec identity, retained AVC-only `avcC`,
  display matrix, pixel aspect and raw color metadata, decode-order packet index,
  independent payload reads, and container sync hints. Indexing skips `mdat`
  payloads, including when `moov` is at the end. Raw media clocks and mapped source
  DTS/PTS remain available; negative composition offsets carry decode preroll.

The MP4/MOV index qualifies non-fragmented, self-contained files with one stable
sample description (AVC or complete JPEG), `stsz`/`stz2`, `stco`/`co64`, `stsc`,
`stts`, signed/unsigned
`ctts`, `stss`, and leading empty edits followed by one rate-1 media edit. It
rejects external data references, unsupported display geometry and
edit arrangements, and resource-limit violations. `syncBefore` returns a
container hint. `lib/video.decode_plan` now qualifies static-avc1 IDR samples
before allowing nonzero starts, retains bounded probe leases, and groups selected
pictures into decode runs. The Apple decoder keeps packet-zero decoding as its
default/reference. Open-GOP recovery hints alone do not authorize seeking. See
[the scheduling contract](../video/VIDEO.md#implemented-independent-scheduling);
codec qualification belongs to video rather than the container reader.

The portable MJPEG lane accepts QuickTime `jpeg` version-zero visual entries
with one picture per sample and progressive field metadata. Each JPEG is
independent; `lib/video.mjpeg` reads only selected payloads and validates baseline
8-bit complete pictures before pure Zig decode. `Track.codec` is explicit; MJPEG
tracks have empty `avcc` and zero NAL length. A QuickTime data handler in `minf`
does not overwrite the video track handler in `mdia`. AVI, abbreviated tables
and paired/interlaced JPEG fields remain unsupported. `lib/video.mjpeg_metal`
now stages decoded RGBA into a bounded Metal preparation queue on macOS, preserving
the same owned window/PTS metadata. This remains video orchestration; media has
no device or model dependency.

The pure Zig H.264 lane now qualifies bounded multi-slice `avc1`/`avc3`, mixed
MBAFF, PAFF pairs carried together or as fields split across indexed packets,
custom scaling matrices, POC type 1, FMO/ASO/redundant copies and native 8–14-bit 4:2:0/4:2:2/4:4:4
samples under the profile/tool limits in [VIDEO.md](../video/VIDEO.md#implemented-independent-efficiency-and-portable-h264-work).
Container indexing preserves codec bytes and sample timing; it does not infer
support from an `avc1` label or assemble field pictures itself. The software decoder
verifies both complementary fields before publishing a woven picture and admits
the first field before second-field prediction. Output depth/chroma and plane
strides are explicit; high-depth samples remain little-endian `u16` through decode.
The decoder supports individual field MMCO 1–6; malformed marking and non-complementary pairs fail explicitly. Portable dynamic
parameter-set epochs, monochrome/separate planes, gaps, SP/SI and data partitions
are qualified with the bounds described in VIDEO.md. A bounded decoder
assembly extension buffers pairs across unrelated coded pictures, preserving
packet membership and frozen prediction references under shared admission. This
extension is qualified separately from H.264 consecutive-access-unit semantics;
container indexing continues to expose packets without assembling pictures. The Apple
hardware/Metal NV12 route retains its 8-bit 4:2:0 qualifier.

Shared admission reservations can now resize atomically. Configuration parsing
shrinks its temporary reservation to the live explicit FMO map, keeps it charged
through seek probing and reconstruction, and releases it after freeing that map.
Growth denial preserves the prior reservation. Slice, pixel, packet, dependency
and allocator high-water limits remain independent of shared admission limits.

WebM video indexing and version-pinned object-store transport adapters are now
implemented below, together with bounded sequential spooling, automatically framed live
fragment ingestion and Cues seeking. General encrypted/dynamic-description support
remains planned.

Tests use six original synthetic MP4 fixtures with hashed FFprobe receipts,
including B-frames, VFR, rotation, audio/video tracks, signed composition offsets,
and compact/co64 tables. They cover malformed inputs, bounded tail metadata
reads, cancellation/retry, independent leases, and allocation failures. See
[testdata/README.md](testdata/README.md) for provenance and regeneration.

From `zig/`, run `zig build test-media test-video`; from `zig/lib/media/`,
run `zig build check-media -Dtarget=wasm32-wasi`; `zig/lib/video/` has its own
`check-video` and `test-video` steps for decoder/preparation portability.
Use the repository's Zig 0.17 toolchain. Existing audio regression tests continue
through the inference package's audio test steps.

The intended dependency graph is:

```text
source provider → lib/media → lib/audio → PCM → audio inference adapter
                           → lib/video → surfaces → video inference adapter
lib/image → reusable image processing and MJPEG image decoding
```

`lib/media` must not depend on inference, model registries, device runtimes, or
the object-store client. Container code is portable and available to CPU-only
builds. A provider supplies file, object-store, or network reads through a small
source interface.

## Implemented independent container and source extensions

`mp4.Reader` accepts empty initialization tables with `mvex/trex` and subsequent
fragments in the same immutable, known-length source. The selected track retains
one static sample description. Each selected `traf` requires `tfdt` and either
`default-base-is-moof` or an explicit base offset; `trun` supplies the initial
data offset, per-sample overrides, first-sample flags and signed/unsigned
composition offsets. Durations, offsets, resource limits, monotonic decode
intervals and containment inside `mdat` are checked. Negative composition offsets
contribute to decode preroll. Hybrid static/fragment samples, implicit cross-track
base addressing, encrypted fragments and missing decode times are rejected.
Packet clocks, payload positions/sizes and sync hints match the independent
fragmented FFprobe receipts, including signed B-frame composition offsets.
The signed fixture records FFprobe's one-frame clock translation explicitly,
so raw media clocks and source decode-preroll clocks are compared separately. Fragment payloads are leased one at a time; aggregate
fragment metadata is bounded. This is file indexing, not a live segment service.
The qualification follows the [W3C ISO BMFF fragment structure](https://www.w3.org/TR/mse-byte-stream-format-isobmff/).

`webm.Reader` indexes the first supported VP8, VP9 or AV1 video track in a WebM
Segment. It retains Tracks metadata, reads bounded element/block prefixes and
skips full codec payloads during indexing. Packets retain signed nanosecond PTS,
optional default/BlockDuration, keyframe hints, invisible/discardable flags and
absolute codec-payload offsets. No decode timestamp is invented. Known-size
and unknown-size Clusters, SimpleBlock and BlockGroup are supported; the Segment
may have unknown size when the Source length is known. Unknown Cluster boundaries
are found by parsing element headers until the next Segment child, never by scanning
codec payload for signatures. Laced video, content encodings, dynamic CodecState,
interlacing and non-unit TrackTimestampScale are rejected. Index availability does not advertise a VP8/VP9/AV1 decoder.
The [Matroska element definitions](https://www.matroska.org/technical/elements.html)
are the container reference; the VP9 fixture independently matches FFprobe.

`remote.ObjectRange` remains the portable callback interface for range providers.
The native `objectstore.Adapter(@import("objectstore"))` now supplies authenticated
S3/GCS transport through the existing `lib/objectstore` clients, so applications
need credentials/configuration rather than an HTTP/authentication callback. The
optional adapter keeps HTTP/auth dependencies out of core media/portable consumers.
Use `Adapter.borrow` for an application-managed client, `Adapter.createS3` for an
owned S3 client, or `Adapter.createGcs` for an owned GCS JSON API client. S3 uses the
existing SigV4 signer and dynamic credential-provider support; GCS uses configured
bearer credentials or the existing cached Google-token manager. Existing objectstore
configuration helpers may supply environment/provider configuration. Owned adapters
are destroyed with `destroy`; borrowed clients and bucket/key/version strings must
outlive readers and caches. An adapter is single-consumer; share the application's
I/O runtime through client configuration and instantiate adapters per request.

Requests carry an explicit strong HTTP ETag, non-null S3 version ID, or canonical
GCS generation. The built-in clients issue Range with If-Match/versionId/generation,
request no metadata probe and cap response bodies to the requested size. Actual wire
status, parsed Content-Range and strong-validator metadata are preserved through
`GetResult`. The adapter verifies 206, start/length/total and the pinned version
before copying/publishing bytes. Weak/missing/changed ETags, wrong versions,
unversioned S3 `null`, malformed GCS generations and full-object 200 responses are
rejected. Both built-in transports disable redirects; credentials are not replayed
to a redirected endpoint. Cancellation/deadlines propagate through the object client,
and the original media cancellation error is preserved. Source.identity must also
include object identity and its immutable version.

`zig build test-media-objectstore` qualifies SigV4/bearer headers, version selectors,
range/body limits, malformed response metadata, strong ETag matching, owned-client
cleanup and actual localhost HTTP requests through both built-in clients. Cloud
credential discovery is inherited from `lib/objectstore`; these tests do not use live
cloud accounts. The optional integration is compiled separately from `test-media`.

`remote.ReadAhead` adds one fixed, forward read window (default 256 KiB), allowing
nearby packet/header reads to share a provider request. Large reads progress through
short reads without increasing cache capacity. Source leases copy independently
and survive cache eviction. A failed/cancelled fill invalidates the whole window.
Provider read count and wire bytes have separate limits from Source's logical
reads/bytes. ObjectRange records received bytes and conservatively charges failed
attempts; ReadAhead records provider bytes and cache hits. Prefetch may fetch nearby
payload bytes that the parser itself skips. Configure the window before use; do not
mutate provider identity/configuration while a reader is active.

`admission.Pool` provides atomic, nonblocking host/device/command reservations
across request-local owners. A denied reservation mutates no counters and performs
no source read. Tokens are move-only and release after resources are destroyed.
Source leases hold host reservations, ReadAhead holds its cache reservation, and
MP4/WebM readers reserve their configured maximum index bytes until destruction.
Limits are conservative upper bounds, rather than physical RSS accounting; pool
limits/configuration must remain fixed while concurrent users run. Attach the same
pool to every source/cache/job to cover shared pressure. Rejected work can be retried
by the application's scheduler under the original cancellation deadline.

## Sources and packet lifetime

The eventual generalized reader API extends the implemented source, timeline,
and MP4 modules with shared packet/seek types and a video WebM reader. Its public
types should express these concepts:

| Concept | Contract |
| --- | --- |
| Source | Borrowed immutable bytes or bounded positional reads; explicit seekability, optional length, and source identity. |
| Reader | Discovers tracks, exposes metadata, iterates packets in decode order, and provides supported seek operations. |
| Track | Stable track ID, kind, codec/configuration, timescale, geometry or audio format, color/orientation metadata, and timeline mapping. |
| Packet | Track ID, payload lease, DTS, PTS, optional duration, random-access/dependency information, and configuration generation. |
| Packet lease | Explicit release; remains valid until released even when the reader advances. |
| Seek plan | A valid decode start and the packets/ranges needed to reach requested presentation positions. |

Borrowed-byte readers may expose packet slices without copying while the source
is retained. Range-backed readers lease bounded read buffers; advancing a reader
must not invalidate payloads still held by asynchronous decoders. Small codec
configuration data may be owned by the reader rather than repeatedly copied.
Lease and source retention are accounted separately from packet metadata.

Positional reads carry deadlines and cancellation. Object-store providers
translate these into their existing bounded range operations. Cache keys include
an immutable object version or validated source digest, not just a URL. Avoid
downloading an entire long video to discover a tail-positioned MP4 `moov` box.
Bound tail probing, metadata reads, and read coalescing; expose the cost of
non-seekable sources rather than pretending they support cheap random access.

Sequential sources can decode forward, but global uniform sampling may require
known duration or an index. If the required metadata is unavailable, require an
explicit bounded scan/spool policy or return an unsupported operation. Do not
silently change sampling to the first few frames.

## Timeline contract

Keep timestamps as signed ticks with an explicit rational timebase. Compare and
rescale with checked arithmetic and documented rounding; avoid accumulated
floating-point timestamp drift. Unknown duration is distinct from zero duration.
Packets are consumed in decode order; decoded pictures are selected and emitted
in presentation order. DTS and PTS are separate values.

Track media time and source presentation time are separate domains. Preserve
edit-list mapping, negative composition offsets, leading gaps, and codec delay.
Normalize source time once at the container boundary and identify that domain in
every clip interval. Retain enough original timing to diagnose mappings. Audio
decoders continue handling priming/trim under one explicit ownership rule; shared
timeline mapping must not subtract delay twice.

Use half-open clip intervals `[start, end)`. Preserve variable frame rate,
duplicate timestamps, discontinuities, and gaps. A sampling policy must state
how it resolves these; the container reader must not fill visual gaps by
inventing pictures. Missing metadata and unsupported non-contiguous edits must
produce explicit outcomes. Rotation/display transforms are metadata, not an
implicit mutation of the decoded pixel plane.

Random-access flags are hints requiring codec-aware interpretation. An intra
picture is not necessarily a safe independent start, and open GOPs may require
preroll. The decoder supplies any additional dependency restrictions before a
seek plan is accepted.

## Initial container scope

### ISO BMFF / MP4 / QuickTime

Extract existing box parsing and sample table logic without widening advertised
audio support. Add video track sample entries and configuration records,
`stts` decode durations, versioned/signed `ctts` composition offsets, `stss`
sync samples, `stsc`, `stsz`/`stz2`, `stco`/`co64`, and supported `elst` mappings.
Validate offsets against the source and resolve sample-description changes.
H.264 packet framing and configuration are explicit; hardware backends receive
the framing they require without unnecessary whole-stream copies.

The qualified fragmented MP4 subset is implemented below through
`moof`/`traf`/`tfhd`/`tfdt`/`trun`. Addressing and timeline forms outside that subset
remain explicit rejections. Similarly, encrypted tracks, unsupported edit mappings, and
unsupported dynamic descriptions cannot masquerade as ordinary packets.

### Matroska / WebM

Extract EBML and block/lacing handling with the existing audio tests intact.
Expose video tracks, codec private data, cluster timestamps, block timestamps,
durations when available, and Cues-based seeking. Handle unknown-sized elements
under explicit nesting/byte limits. Codec-dependent references and invisible
frames remain visible to the decoder even when they produce no sampled output.
WebM video support is advertised only when a qualified decoder exists for the
selected track's codec.

## Bounded work and failure behavior

All readers accept limits for metadata bytes, tracks, sample/index entries,
nesting, packet bytes, read-buffer retention, total bytes read, and scan work.
Checked arithmetic precedes allocation and offset addition. Large indexes use
bounded/lazy representations where practical; metadata is not exempt from
admission because no pixels have been decoded yet.

Distinguish malformed input, unsupported container/codec/timeline features,
source I/O failure, resource denial, cancellation, and deadline expiry. Exact
error names are an implementation decision. Cancellation releases leases and
joins pending source work before reader/source destruction. Failed seeks leave
the reader in a documented recoverable state or require a reset.

## Extraction and validation

1. Capture current MP4/WebM audio fixture results, gaps, trim, and failures.
2. Extract shared parsing behind existing audio entry points and establish
   equivalent PCM/timing results before adding video-specific behavior.
3. Add packet iteration and timestamp/index tests for video tracks, then range
   readers and seek plans. Keep unsupported shapes explicit at each stage.
4. Qualify fragmented inputs and additional container features independently.

Use generated and licensed fixture corpora with expected track selections,
packet bytes, DTS/PTS, edit mappings, and seek starts. Cover B-frame reordering,
VFR, negative timestamps, tail `moov`, leading gaps, multi-track selection,
unknown duration, truncation, overflow, lacing, and malformed indexes. Compare
metadata and timelines to an independently pinned reference such as ffprobe;
FFmpeg is a test oracle, not a runtime dependency. Include allocation-failure,
cancelled read, stale-source, and retained-packet lifetime checks.

Completion requires unchanged qualified audio semantics, bounded metadata/read
memory, and packet/timeline parity for the declared container scope. These
contracts are not a claim that every MP4 or WebM stream is supported.

## Cues, sequential input and live fragments

`webm.Reader.cues` retains validated CueTime/track/Cluster positions bounded by
`max_cues`. `seek(pts)` uses the cue at or before the request, with a sync-packet
fallback when Cues are absent. Positions must resolve to the selected track's
indexed visible sync packet; malformed targets, duplicate times, unsupported
codec-state changes and budget excess fail. A container sync hint is not proof
of a codec dependency reset. Native video decoder consumers are documented below.

`stream.Sequential` adapts a non-seekable reader. `stream.collect` explicitly
spools to a bounded immutable `Snapshot`, handling short reads, final EOF, cancellation
and invalid provider counts. Zero bytes mean final EOF; temporary unavailability
must return an error. Byte/read/chunk limits prevent indefinite buffering; exact
byte-limit EOF is checked with a one-byte probe. Shared admission reserves the spool
bound and shrinks to owned storage. This is completed-input spooling, not an unbounded
live seek abstraction. Snapshot addresses stay stable; release Readers and packet
leases before snapshot destruction. Provider context and initial identity are borrowed.

`live_mp4.Ingest` owns an initialization snapshot and accepts arbitrary partial
arrival chunks. `nextSegment(false)` discovers complete moof + one-or-more mdat
segments at following moof/styp headers; drain it after each push. At final EOF,
`nextSegment(true)` closes the last segment and resolves size-zero boxes. Partial
headers/payloads wait until more input, then fail explicitly if EOF is premature.
Extended-size headers are supported and bounded. `finishSegment` remains an optional
caller-framed compatibility API. Only validated supported fragments are published. Emitted `Segment`
values own immutable backing, a Reader, and distinct ordinal source identities.
Earlier packet leases survive subsequent arrivals. Segment count, aggregate ingested
bytes, pending bytes, identity bytes and index limits bound retention. Pending-buffer
growth charges both old and new allocations during copying. DTS replay/regression
and changed track/configuration/geometry fail; failed validation retains pending
input for retry or `discardSegment`. The caller still supplies complete initialization bytes; automatic media box
framing does not implement HTTP/HLS/DASH transport,
indefinite sessions, encrypted samples or initialization reconfiguration. Consumers
must release retained Segments to keep shared budgets available.

MP4 `avc3` descriptions expose `Track.inband_parameter_sets`; packet offsets and
clock indexing are unchanged. Dynamic portable codec decoding uses the video
parameter registry rather than altering media parsing or Apple hardware policy.

## Pretrained video qualification (2026-10-09)

The EmbeddingGemma 2 adapter now compares six MP4/MOV video and ordered mixed
groups against independently generated pinned-checkpoint F32 embeddings. Native
CPU and Metal checks include presentation sampling, repeated frames, token counts,
normalization and HTTP dimensions. This qualifies the tested RGB decoder policies
and model integration; it does not extend container or codec scope. See the
[pretrained receipt](../video/testdata/video-pretrained-validation.md) and
[reproducible fixtures](../../../tools/embeddinggemma2/testdata/video/README.md).

## Live initialization epochs

`live_mp4.Ingest.reconfigure(identity, initialization)` accepts a new initialization
at an explicit transport discontinuity. Drain or discard pending media first;
otherwise it returns `PendingMediaSegment`. The replacement moov, track, AVC
configuration and fragment defaults are validated before publication. Failed
validation, cancellation, admission or allocation leaves the current initialization
and timeline in place. Existing segments and packet leases retain independent,
immutable snapshots.

A successful replacement increments `Segment.epoch` and resets decode timestamps
and codec identity together. Consumers must reset decoder state at the epoch
boundary. Segment ordinals, ingested bytes and shared admission remain cumulative;
`max_reconfigurations` bounds repeated initialization changes. Inline initialization
boxes inside pending media still require the transport to signal the boundary.
The native HTTP HLS reader below signals these boundaries using map and
discontinuity changes. DASH list/template snapshots reuse the same transport, including synchronized live availability filtering. Representation selection, inherited DASH addressing and multi-period/live cursor support are implemented below.

The media suite covers a real changed SPS/96×48 initialization, invalid replacement
rollback, replay after discontinuity, outstanding old leases and allocation failures.


## HLS fMP4 playlist parsing

`hls.parse` implements a bounded fMP4 media-playlist subset of
[RFC 8216](https://www.rfc-editor.org/rfc/rfc8216.html). Parsed strings borrow the
manifest; the owned segment array preserves media/discontinuity sequence numbers,
map changes, explicit and same-object implicit byte ranges, duration and EOF.
Input bytes, URI lengths and segment counts have independent limits. Allocation
failures release the parsed array. Master, I-frame, encrypted and low-latency
playlists fail explicitly; this does not expose MPEG-TS decoding.

`http_stream.Adapter(httpx)` performs native HTTP/HTTPS reads using a borrowed
`lib/httpx` client. Client default headers provide authentication. All resolved
references remain on the configured origin; redirects, ambient cookies and hidden
retries are disabled. Exact byte-range responses require HTTP 206 and matching
`Content-Range`. Request counts, body size, cumulative transport work, cancellation
and shared host-memory admission apply to initialization, playlists and media.
Failed requests retain their work charge. Response storage remains reserved until
its owned copy has replaced the HTTP response.

`hls_live.Reader(httpx)` owns bounded playlist snapshots, reloads explicitly and
returns immutable `live_mp4.Segment` objects. Changed maps or discontinuity sequence
numbers establish new configuration epochs, requiring the consumer to reset its
decoder. Outstanding packet leases survive these changes. Overlapping playlist
entries must stay immutable; a sliding window that skips unconsumed media fails
with `MediaStreamGap`. Retrying a failed media fetch preserves initialized state;
retrying a failed snapshot commit does not download or append that media twice.
`reloadAfterSeconds` supplies a polling delay to the caller's scheduler.

`zig build test-media-http` tests authenticated reads, exact ranges, redirect
refusal and a complete reload/configuration transition against a local HTTP
server using the production client. The portable suite additionally injects
allocation failures across both epochs. Encrypted, MPEG-TS, master-selection,
low-latency HLS and broader DASH support remain open.


## DASH template and live ingestion

`dash.parse` accepts bounded static XML MPDs and selects one MP4 video
representation per selected Period. `dash.parseAtWithSelection` and `dash_live.Options.selection`
select period/adaptation/representation IDs, a bandwidth ceiling and a BaseURL
alternative. Without selectors the first eligible video representation is used;
with a ceiling the highest eligible bandwidth is used. Audio adaptations are
skipped. `SegmentList` and `SegmentTemplate` attributes and child addressing
inherit from Period through AdaptationSet to Representation; nearer values
override inherited values. BaseURL resolution follows the MPD/Period/adaptation/
representation hierarchy. Relative BaseURLs require `selection.manifest_url`;
live readers supply the actual manifest URL and retain authenticated origin
pinning. Without `period_id`, selected video Periods are combined in presentation
order. Period starts follow the preceding known duration; a following explicit
start can establish the preceding duration. The final duration can come from
`mediaPresentationDuration`. Ambiguous timing, duplicate Period IDs and overlaps
fail explicitly. Explicitly announced gaps retain their period timing; missing
unconsumed periods/segments report `MediaStreamGap`. `max_segments` bounds the
combined available references; `max_periods` and `max_timeline_entries` separately
bound metadata, including future/expired Periods. `max_planning_bytes` bounds
total canonical planning work (16 MiB by default); each Period releases its
temporary arena after copying independently owned references.
Templates expand `$RepresentationID$`, `$Bandwidth$`, `$Number$`, `$Time$`,
`$$` escapes and bounded `%0Nd` decimal widths. `SegmentTimeline` supports
explicit/implicit starts, nonnegative repeats and `r=-1` bounded by the next
start, period end or live clock. Duration-based templates use the period duration
or live clock. Timescales are bounded to 32 bits; XML elapsed durations and UTC
`Z` timestamps accept millisecond precision. All returned URI references are
owned by the playlist, including decoded signed-query attributes. All allocations and
expanded URI bytes are bounded, including allocator-failure cleanup.

`dash.parseAt` and `dash_live.Options.clock` accept synchronized UTC Unix
milliseconds. Dynamic templates require `availabilityStartTime`; `Period@start`
and `presentationTimeOffset` establish sample/presentation clock mapping. Only
complete segments with zero availability offset are exposed. The time-shift
window drops expired references arithmetically, avoiding historical enumeration;
early snapshots can be empty. `minimumUpdatePeriod` is exposed by
`reloadAfterSeconds`; the caller schedules polling. Changes to availability,
period-start, timescale, offset or selected representation/configuration identity
fail during reload. Overlapping
segment descriptors remain immutable; missing unconsumed sequences report gaps.

`dash_live.Reader(httpx)` reuses authenticated origin-pinned HTTP, exact ranges,
shared admission, transactional snapshot reload and retry-safe fMP4 ingestion.
It returns owned segments and packet leases. Period-local segment numbers may
restart; cursors use stable Period identity and timing rather than current XML
indices or a renumbered snapshot. Unknown live end times can become known when
a successor appears. A consumed cursor retains that learned boundary after the
old Period disappears; changing published configuration or an announced successor
fails. Incomplete future segments wait; expired pending references fail even when
the refreshed available window is empty. Every Period transition reconfigures the
decoder epoch, including transitions with an unchanged initialization URI.

Returned `live_mp4.Segment.dash_timing` exposes the Period boundary and sample/
presentation mapping. `presentationStart()`/`presentationEnd()` return exact
rational milliseconds (`numerator`/`denominator`); packet timestamps retain the
native track clock. Consumers can apply Period clipping using `duration_ms`.
Parser, allocation-failure, clock
advancement, retry, gap and production authenticated HTTP tests cover these paths.
It is a library API; grouped model requests still accept owned MP4/MOV bytes.

This is a declared subset of [DASH addressing and timing](https://dashif.org/Guidelines-TimingModel/).
Nonzero availability offsets, incomplete low-latency segments, UTCTiming fetching, MPD Patch, DRM, EssentialProperty,
SegmentBase and unsupported attributes fail explicitly. A bounded native XML
tree supports namespace prefixes, comments, CDATA and predefined/numeric
entities; DTDs, external/custom entities and invalid namespace bindings fail.
The caller supplies clock synchronization. Period/representation selection is
fixed for a reader rather than adaptive switching.

### VP8 decoding and display/color metadata consumers

WebM packet indexing now feeds `antfly_video.vp8.Session` for pure Zig stateful
VP8 reconstruction, including invisible reference pictures. Packet ordering and
visibility remain transport metadata; the decoder independently checks coded
frame tags and updates references in coded order. WebM model ingestion uses
logical visible-picture indexing and bounded replay; the VP9/AV1 decoder
consumers are documented below.

MP4 orthogonal display matrices and pixel aspect ratios now reach the
EmbeddingGemma 2 presentation renderer. AVC VUI primaries are retained alongside
matrix/transfer declarations; model preflight combines VUI and container color
metadata before selecting the explicit PQ/HLG rendering policy. Color conflicts
and unsupported transforms fail. These model policies use a new embedding recipe
identity and have separate positive pretrained rendering oracles; media indexing
still performs no color conversion itself. See [VIDEO.md](../video/VIDEO.md#stateful-vp8-and-presentationhdr-rendering-continuation).

## VP9 and AV1 decoder consumers

The WebM VP9/AV1 index now has a native consumer in
`video.codecs.Session.decodeWebmPacket`; earlier statements that these codecs
have no reconstruction route are superseded by this implementation. Antfly
maintains checked-in Zig source ports, with no runtime codec-library dependency.
VP9 fixtures cover profiles 0–3 at 8/10/12-bit across 4:2:0/4:2:2/4:4:4. AV1's
qualified public route includes profiles 0–2, 8/10/12-bit and
monochrome/4:2:0/4:2:2/4:4:4, including film grain and reference updates.
Native elementary AV1 additionally accepts explicit Annex B framing and
operating-point selection; WebM itself continues to supply low-overhead OBUs.

The consumer leases packets through the existing source interface, applies
session/reader control checks, and handles AV1 configuration OBUs from WebM
CodecPrivate. Decode remains sequential; seek recovery must create a new decoder
and replay coded dependencies from a synchronization point. Output is a bounded,
independently owned picture batch. Multiple pictures in one container packet
retain that packet's timestamp. Model ingestion assigns stable logical IDs to
visible decoder outputs and applies its separately documented sampling policy. See [VIDEO.md](../video/VIDEO.md) and the
[maintained port contract](../video/src/codecs/PORTING.md) for support and failure
semantics.


WebM model consumers must distinguish coded packets from displayed pictures.
The native video module supplies `webm_pictures` to index decoder outputs and
replay selected logical pictures. Container invisibility and codec hidden
reference updates do not contribute to model frame counts; a single packet can
produce multiple pictures. PTS stays in nanoseconds and equal timestamps retain
stable decoder-output order. Container indexing alone is not evidence of
pretrained model parity.

WebM `Reader.duration_seconds` preserves the optional declared Segment Duration
scaled by TimestampScale. It is validated for finite positive values after the
entire Info element is read, so element ordering does not affect scaling.
Sampling consumers should prefer this value over rounded block timestamp tails.
See the [Matroska element specification](https://www.matroska.org/technical/elements.html).


## Logical AVC model timelines and explicit windows (2026-10-11)

MP4 packet identities remain transport identities. Video consumers now use
`video.h264_pictures` to obtain completed field/partition picture metadata and
map model selections back to transport selectors. The index handles native
configuration epochs and joins separate-colour-plane metadata; it does not
change MP4 parsing or claim one packet equals one displayed picture. WebM uses
its existing decoder-output logical index. HTTP video parts can independently
choose reference index/FPS sampling or explicit rational timestamp windows;
container duration and caller interval semantics remain distinct.
See [VIDEO.md](../video/VIDEO.md#logical-avc-pictures-and-explicit-model-windows-2026-10-11)
for bounds, endpoint behavior, recipe identity and window embedding semantics.


## Resident inference output continuation (2026-10-11)

The EmbeddingGemma 2 Metal consumer now pools, applies requested dimensions and
normalizes on device before final response readback. Container indexing and
source timestamp policies retain their existing contracts; media code does not
perform model normalization. Actual device ownership/planned-scope units and
pretrained CPU/Metal vector/dimension/retry campaigns qualify this consumer path.
See the [normalization receipt](../../../tools/embeddinggemma2/testdata/resident-normalization/README.md).
