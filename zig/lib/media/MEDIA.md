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

WebM video indexing and version-pinned object-store transport adapters are now
implemented below. Sequential unknown-length sources, live fragment ingestion,
Cues-based seeking and general encrypted/dynamic-description support remain planned.

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
Clusters, SimpleBlock and BlockGroup are supported; the Segment may have unknown
size when the Source length is known. Laced video, content encodings, dynamic
CodecState, interlacing, unknown-size Clusters and non-unit TrackTimestampScale
are rejected. Index availability does not advertise a VP8/VP9/AV1 decoder.
The [Matroska element definitions](https://www.matroska.org/technical/elements.html)
are the container reference; the VP9 fixture independently matches FFprobe.

`remote.ObjectRange` wraps a caller-owned authenticated transport. Requests carry
an explicit strong HTTP ETag, non-null S3 version ID, or canonical GCS generation.
The transport must issue Range with the matching If-Match/versionId/generation
condition and return independently parsed response metadata. The adapter requires
206, matching Content-Range start/length/total, and the pinned version before
bytes are published. It rejects weak ETags, unversioned S3 `null` and malformed
GCS generations. Transport/authentication, redirects, retries and credential
management stay with the application; no network client or cloud SDK is added to
media. Source.identity must also include object identity and its immutable version.

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
