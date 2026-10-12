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
allocation failures across both epochs. HLS master selection is now implemented as described below. AES-128 identity-key encryption is now implemented below. SAMPLE-AES/CBCS,
MPEG-TS, low-latency HLS and broader DASH support remain open.


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


### Model pipeline admission bridge

EmbeddingGemma 2 supplies a media Pool as a sub-ledger of its already-acquired
shared model execution permit. MP4/WebM sources, indexers and portable decoder
sessions use its host preprocessing share; Metal preparations and coefficient
storage use its device/command share. MJPEG output frames retain their reservation
after the reader is destroyed. Submitted Metal owners fence work before releasing
capacity; releasing completed source storage retains the output charge.

The same parent controller gates concurrent model requests before decoding.
HTTP video preflight has its own shared host reservation. The first-video
producer described below decodes/prepares before the model gate, permitting
preparation to overlap another request's execution. Library callers continue
to supply a shared Pool explicitly; the bridge does not silently introduce a
process-global singleton.


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


### Larger WebM reconstruction benchmarks

The video benchmark consumes the existing range-source/WebM packet contracts and
reconstructs visible VP8/VP9/AV1 pictures without changing container indexing or
transport semantics. Larger 480p/1080p Sintel-derived streams expose hidden coded
work separately from displayed/prepared counts. Native codec output matches all
4623 independent picture hashes; VP8 reference slots now share immutable owners,
while outputs retain independent storage. Detailed source/measurement scope is
recorded in the video design and benchmark receipts. Broader streaming/display
qualification, replay/index costs and the full non-CUDA goal remain active.


### WebM planning boundary

The model-facing video layer can derive logical displayed-picture timelines from VP8/VP9/AV1 headers rather than repeatedly reconstructing the clip during admission and sampling. Container packet PTS/duration remain attached to each displayed output, including multiple visible pictures from one packet. Header validation is a planning boundary; selected replay validates compressed entropy and pixels. Packet reads retain source controls and byte limits, while metadata capacity is admitted separately.


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

## WebM presentation and Colour metadata

The bounded track reader retains crop edges, display dimensions/units, stereo,
projection type/private data/pose, and Colour declarations. Known singleton
fields reject duplicates; nested metadata shares the element and cancellation
budgets. Mastering data remains borrowed from the reader's metadata lease;
finite chromaticity/luminance values are validated. These fields follow the
[Matroska EBML schema](https://raw.githubusercontent.com/ietf-wg-cellar/matroska-specification/master/ebml_matroska.xml).

Model consumers apply crop before rotation and resize, use display dimensions
as an aspect ratio, and reconcile container matrix/transfer/primaries with
codec declarations. Depth/chroma/range mismatches fail. VP8 container Range is
authoritative because its clamping bit specifies reconstruction clamping,
rather than full/limited-range conversion; see
[RFC 6386 section 9.2](https://www.rfc-editor.org/rfc/rfc6386.html#section-9.2).
The video consumer declares supported rendering policies separately: rectangular
quarter turns and SDR/PQ/HLG policies, with explicit failures for stereo,
spherical projections, nonorthogonal poses and explicit chroma siting that
requires another reconstruction filter. Media retains declarations without
performing pixel conversion.

## HLS master discovery and video rendition selection

`hls_master.parse` retains borrowed variant and audio/video/subtitle/caption
rendition metadata. `Master.select` chooses the highest advertised peak bandwidth
within optional URI, resolution, frame-rate and declared-codec constraints.
Codec matching respects the sample-type prefix boundary; audio codec entries
can accompany the selected video codec. Missing declarations do not satisfy an
explicit constraint. Video DEFAULT is honored unless a rendition name is
provided. Equal-bandwidth choices use lexical URI and descriptor ordering,
including duplicate video URIs with different audio groups. Separate audio and
subtitle metadata remains available to callers; the live reader ingests video.
These declarations follow [RFC 8216](https://www.rfc-editor.org/rfc/rfc8216.html).

`hls_live.Reader` accepts a master or direct media URL. Initial discovery fetches
the master and chosen media playlist. The selected URL commits only when the
media snapshot validates; failed transport, parsing, allocation or cancellation
retains the previous state. Later reloads poll the selected media URL, without
hidden adaptation or another master fetch. Maps and media resolve relative to
that URL while authentication remains pinned to the original transport origin.
Foreign-origin choices fail before an authenticated request. Existing epoch,
mutation/gap, retry and held-packet ownership rules remain in effect.

Master parsing/selection checks cancellation and bounds input, attributes,
variants, renditions, URIs and planning storage. Its arrays carry shared
host-memory admission through destruction, including growth/shrink overlap.
Portable tests cover deterministic selection, malformed groups/attributes,
allocation failures and cancellation; production HTTP tests exercise authenticated
alternate-camera discovery and an initialization change with an old packet held.
A pinned public Apple master configuration additionally tests real metadata;
its referenced MPEG-TS media is not part of this qualification. Use
`zig build test-media-hls-fuzz --fuzz=100000` for the bounded parser/selection
campaign. SAMPLE-AES/CBCS, MPEG-TS and low-latency ingestion and automatic separate-track
assembly remain within the active streaming scope.

Final HLS master gates pass 90 portable and six production HTTP tests on both
macOS and actual Linux ARM64. A fresh bounded parser/selection campaign passes
117,703 runs with four positively accepted Smith seeds, including the public
Apple master fixture. Source/binary/report receipts are in
`zig/lib/media/testdata/hls/master-validation`; these are protocol/ownership
results, not codec, model, performance or arbitrary HLS conformance claims.

## Native HLS AES-128 ingestion

The fMP4 HLS reader handles AES-128 whole-resource CBC with PKCS7 padding using
Zig's AES implementation. Identity key files must contain exactly 16 bytes.
Explicit IVs are 128-bit big-endian values; media without an explicit IV derives
one from its sequence number. Encrypted maps require an explicit IV and retain
the declaration active at the MAP tag independently of later media key changes.
METHOD=NONE clears media encryption. Unsupported key formats can coexist with
an identity declaration; unsupported-only media fails before ingestion. Supported
master SESSION-KEY hints validate without replacing the media key declaration.
These rules follow [RFC 8216 sections 4.3.2.4 and 5](https://www.rfc-editor.org/rfc/rfc8216.html).

Keys use the existing authenticated same-origin transport, request/byte budgets,
cancellation and host admission. One key URI is cached within the current
playlist snapshot; a successful reload invalidates its material so a reused URI
can supply a rotated key. Maps and media decrypt in their owned response buffers,
retain the full allocation for cleanup and ingest only the validated unpadded
prefix. Key-cache material, key bodies and decrypted response buffers are wiped
on release. Four-block AES decryption and portable byte-vector XOR preserve CBC
chaining without another plaintext allocation. Exact byte ranges describe
independently encrypted segment resources, with CBC restarted at each segment.

Independent OpenSSL fixtures cover padding/block boundaries, encrypted maps,
sequence IVs, rotated keys and reconfiguration. Portable failure tests retain
cursor/initialized retry state, deep-own map declarations, reject foreign keys
before initialization/media reads, and reclaim admission on every allocation
failure. Production HTTP tests authenticate keys and ranged media while keeping
old packet leases valid. OpenSSL is a qualification tool, never a runtime
codec/crypto dependency. SAMPLE-AES/CBCS, DRM processing, MPEG-TS and low-latency
HLS remain implementation work.


## Native MPEG-TS AVC extraction and HLS adapter

`transport_stream.demux` extracts owned Annex B AVC PES payloads and their
90 kHz PTS/DTS from bounded 188-byte MPEG-2 transport packets. It validates
PAT/PMT CRCs and complete numbered table sections, selects one AVC program,
checks payload continuity (including exact duplicates and counter rollover),
and unwraps 33-bit timestamps. Unsupported encryption, missing tables/clocks,
truncated sections/PES and continuity gaps fail before publication. Audio and
unselected streams are ignored. Input state is copied; only `next_state` from a
successful result should be committed. Shared admission covers extraction
scratch and retained output, with cancellation and allocation-failure cleanup.

An HLS media playlist without `EXT-X-MAP` uses the optional
`hls_live.Options.transport_stream` factory supplied by `lib/video`. This keeps
container/HTTP/AES code independent of pixel decoding. The factory runs after
native authenticated fetching and AES-128 decryption. Factory failure preserves
the playlist cursor. The returned `live_mp4.Segment.origin` is
`normalized_transport`: its backing contains codec configuration and indexed
packet bytes, **not a serialized MP4 file**. Consume its `.reader` packet API.

The AVC adapter now joins complete AUD-delimited access units across PES
boundaries, including split prefixes and untimed continuation records. Multiple
access units sharing one PES without recoverable clocks still fail with
`AmbiguousTransportPictureTiming`; they do not receive invented timestamps. Every segment requires complete PAT/PMT tables. External
TS initialization maps, cross-segment partial-unit buffering, clock inference, audio assembly,
other TS video codecs and low-latency partial resources remain work. These
restrictions are explicit and do not establish general MPEG-TS conformance.

Independent FFprobe fixtures cover closed-GOP and reordered B-frame clocks and
payloads. Production HTTP tests additionally exercise authenticated no-map HLS
through native AVC decoding. `test-media-transport-fuzz --fuzz=100000` uses two
serialized, positively accepted TS seeds and bounded shared-admission cleanup.
Qualification receipts are in `testdata/transport/validation`.

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

### Decoder reference ownership and admission

Progressive native H.264 workspaces now share immutable reconstruction buffers
with the decoded-picture buffer and its snapshots. Buffer mutation first
allocates a replacement owner transactionally; a normal reset needs no copy
of the old reference payload. Field-coded reconstructions retain the existing
copying completion path. Source ranges, independently retained packed frames
and prepared window outputs preserve their separate lifetime contracts.

Admission accounts for workspace/reference ownership counters and continues
to reserve the conservative decoder bound. Reference copy telemetry measures
progressive payload traffic, rather than reducing memory reservation based
on an unproven aliasing assumption. Qualification and the pinned trailer
copying/shared comparison are recorded in
[the reference-storage receipt](../video/testdata/reference-storage-validation/README.md).

### Request-scoped model projection cache

Grouped video inference owns a bounded projection cache for its request. Exact
clip-byte hashes and sampled picture identities distinguish entries; one model
session owns their lifetime. Native CPU entries own their arrays, while Metal
entries retain device buffers independently of temporary compute wrappers.
Container/header validation still runs, and uncached interframes still replay
required coded dependencies. A fully cached selection bypasses decoding/preparation.

Every retained entry holds an independent shared memory lease. Device views
charge their full buffer span. Queue tickets preserve ownership under
backpressure, cancellation and eviction; the model consumer performs device
imports and retains/releases, and cleanup joins producers before releasing
entries. The cache does not extend the model lock across groups or introduce a
new runtime dependency. Model handle ownership and process protection remain
active through device cache cleanup.

Fresh Linux ARM64 runs now also close the earlier reference-storage runtime
follow-up: 174 decoder tests pass with 27 platform skips, and 31 model/cache
tests pass with three skips. The current follow-up receipts are in
`../video/testdata/frame-cache-validation`. These results qualify their recorded
source and binary scope; broader conformance and sustained load remain separate
requirements of the full active goal.

The final pretrained grouped/multipart preservation run passes on native CPU,
Metal and actual Linux ARM64. All 18 ordered groups and both multipart groups
match their pinned uncached binaries exactly; reference rankings/top-1/3/5
overlap are exact, with maximum cosine score drift below 3e-7. The frame-cache
receipt binds the source, binaries, checkpoint, oracle and runtime artifacts.

### Partial fMP4 HLS and blocking reload

Native HLS ingestion now accepts published partial segments with parent media
sequence and part-index identities, bounded byte ranges, independent/gap hints,
PART-INF, SERVER-CONTROL and PRELOAD-HINT metadata. A completed parent is preferred
when no part has been consumed. Otherwise the reader continues at the next part,
or recovers the unlisted tail from the completed parent. It hashes ordered sample
DTS/PTS, duration, sync state, size and encoded bytes to verify the consumed prefix
before skipping duplicates. Changed prefixes fail with `SourceVersionChanged`;
HTTP, allocation and admission failures preserve the retry cursor. Retained packet
leases survive reload and parent acknowledgement. Normal fMP4 ingestion does not
pay for these HLS sample commitments.

`reloadBlocking` is explicit and bounded by the existing HTTP timeout/control.
It appends `_HLS_msn` and `_HLS_part` while preserving authentication query bytes,
rejects pre-existing delivery directives including percent-encoded aliases, and
keeps the canonical media URL for relative resource resolution. Polling exposes
PART-TARGET timing. Preload hints are metadata only: unpublished resources are
never fetched or emitted. Returned `Segment.hls_timing` identifies the parent,
part and discontinuity sequence; `independent_hint` does not prove random access.

This implementation follows selected semantics in the work-in-progress
[HLS second-edition draft 22](https://datatracker.ietf.org/doc/html/draft-pantos-hls-rfc8216bis-22),
not a full protocol-conformance claim. Independently encrypted AES-128 partial
objects and parent fallback are tested. TS partials, CBC byte-range chaining,
delta playlists, rendition coordination and model-level live orchestration remain
outside this qualification. No runtime codec or transport dependency was added.

Source, binary, fixture and report identities are recorded in
[testdata/hls/partial-validation](testdata/hls/partial-validation/README.md).
