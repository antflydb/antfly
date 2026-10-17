# Audio Support

The implemented shared MP4/WebM container extraction is documented in
[MEDIA.md](../media/MEDIA.md), with decoder/surface contracts in
[VIDEO.md](../video/VIDEO.md) and rollout in the
[native video inference plan](../../../docs/plans/native-video-inference.md).
These proposals do not change the audio support or timing described below.

Antfly inference routes native/server audio through `lib/audio`. The public boundary is
decoded PCM `f32` samples plus sample rate and channel count; encoded bytes are
handled as adapters above that boundary.

The browser path intentionally uses `AudioContext.decodeAudioData` for
container/codec decode, then passes PCM into Zig/WASM for feature generation.
That keeps browser codec support aligned with the platform and avoids shipping
large pure-Zig decoders to WASM before we need them.

## Public Surface

- `lib/audio.decode(...)` returns mono PCM and keeps the existing mono-first
  pipeline behavior.
- `lib/audio.decodeInterleaved(...)` returns interleaved PCM plus channel count
  for callers that need stereo-aware data.
- Transcription and audio embedding have PCM-first entry points; byte-based
  entry points are adapters.
- Format/MIME/filename helpers report only formats that are wired through the
  shared audio boundary.

All public interleaved decode output is normalized to finite `[-1, 1]` PCM.

## Supported Formats

| Format | Current Path | Notes |
| --- | --- | --- |
| WAV/RIFX/RF64/BW64 | Pure Zig | PCM8/16/24/32/64, float32/64, WAVE_FORMAT_EXTENSIBLE, G.711 `alaw`/`mulaw`, RF64/BW64 `ds64` sizing. |
| AU/SND | Pure Zig | Signed PCM, float32/64, G.711 `ulaw`/`alaw`. |
| AIFF/AIFC | Pure Zig for common PCM/float/G.711 | Covers checked-in AIFF PCM16 plus synthetic AIFC `NONE`/`twos`/`sowt`, `in24`/`in32`, `raw `, `fl32`/`fl64`, `ulaw`, and `alaw`; unsupported variants now fail closed. |
| CAF | Pure Zig for `lpcm`/G.711 and checked-in stereo ALAC | CAF packet-table priming/remainder trimming is applied at the shared decode boundary, including ALAC `data` chunks that run to end-of-file and the checked-in 16-bit and 24-bit stereo ALAC fixtures; unsupported variants now fail closed. |
| MP3 | Pure Zig | Pure-Zig coverage includes checked-in mono, free-format stereo downmix, and mode-switching stereo vectors, plus every MPEG-2 and MPEG-2.5 low sampling frequency (24/22.05/16/12/11.025/8 kHz) including their short blocks. Silent frames decode to silence and a final frame the stream cuts short decodes from the bit reservoir, so a clip keeps its length and its timeline; only a leading Xing/Info/VBRI frame is read as metadata instead of audio. Unsupported MP3 shapes still fail closed, and a sample rate without band tables is rejected when the frame header is read rather than reaching the tables. Short blocks share the long blocks' IMDCT normalization, so every checked-in MPEG-1, MPEG-2 and MPEG-2.5 vector now decodes sample-exact against the reference decoder. External fetched MP3 vector coverage is now being widened through the misc e2e lane. |
| AAC/ADTS | Pure Zig | AAC Main/LC core lanes and maintained HE-AAC/SBR/PS reconstruction; bounded qualification and profile limits below. |
| MP4/M4A/QuickTime audio | Pure Zig demux plus codec-specific pure-Zig decode for the owned AAC/ALAC lane | Demuxes checked-in AAC and ALAC access units/config, including the checked-in 16-bit and 24-bit stereo ALAC fixtures. Handles `stsz`/`stz2` sample sizes, direct or `wave`-wrapped codec config, QuickTime `qt  ` brand/`.mov`/`.qt` aliases, later supported audio tracks after earlier unsupported ones, and contiguous-media-edit plus leading/trailing-empty-edit `elst` timing for priming/remainder trimming; unsupported codec/container shapes fail closed. |
| FLAC | Pure Zig | Covers the checked-in native `.flac` 16-bit and 24-bit fixtures through the shared `lib/audio` boundary, plus the external `flac-test-files` sweep including the 32-bit `uncommon/05 - 32bps audio.flac` vector and the raw-frame-start/prefix-noise cases via raw-frame fallback. Only the intentionally malformed `faulty/` negatives and the intentionally unrepresentable mid-stream-format-change `uncommon/01-04` vectors remain expected unsupported. |
| Ogg/FLAC | Pure Zig for the narrow checked-in mapping | Reassembles Ogg pages/packets for the checked-in FLAC-in-Ogg mapping and feeds the result into the native FLAC decoder; unsupported Ogg/FLAC shapes now fail closed. |
| Ogg/Vorbis | Pure Zig, reference-exact | Shared Ogg page/packet parsing tracks BOS/EOS, packet boundaries, and the granule of the last packet *completed* on a page, and the Vorbis lane decodes headers, floor0/floor1, residues, inverse coupling, IMDCT/windowing, and overlap/add per Vorbis I. Every decodable file of the Xiph Vorbis vector set (including `singlemap-test`, `48k-mono`, `out-of-spec-blocksize`, `one-entry-codebook-test`, `unused-mode-test`, and the truncated `test-short2`) decodes at 107–120 dB SNR against ffmpeg/libvorbis with exact frame counts; chained streams decode every chain. |
| Ogg/Opus, Opus | Pure Zig, reference-exact | `opus.zig` owns `OpusHead`/TOC/frame packing, the range decoder, trim/gain, and the packet loop that mirrors `opus_decode_frame` (mode transitions, 5 ms redundancy frames, SILK-to-CELT mixing). `opus_celt.zig` is a port of the libopus float CELT decoder (band allocation, PVQ, anti-collapse, backward MDCT with TDAC, pitch postfilter, de-emphasis) and `opus_silk.zig` a port of the fixed-point SILK decoder (side info, shell coder, NLSF/LTP dequantisation, LPC/LTP synthesis, stereo MS-to-LR, 48 kHz resampler) with its tables generated from the libopus sources. All twelve RFC 8251 test vectors decode at 65 dB SNR or better against the reference `.dec` files for mono and stereo output, and ffmpeg-encoded SILK and CELT streams match libopus decodes sample for sample. Packet loss concealment and comfort noise are not implemented, which a file decoder does not need. |
| WebM/Matroska | Pure Zig demux into the owned Opus/Vorbis/FLAC lanes | Parses EBML with known and unknown-size Segments and Clusters, skips non-audio tracks, and reassembles all three lacing modes. Every block's presentation time is kept (cluster Timestamp plus the block's signed relative timecode, scaled by TimestampScale, less CodecDelay, which is the priming the decoder drops), and decoding puts the audio back on that timeline: a track that starts late, or a recording that was paused, keeps its silence instead of sliding earlier, so a transcript offset still matches where a player would seek. Gap insertion is exact for Opus (packet durations come from the TOC) and Vorbis (block sizes come from the packet headers); a FLAC track is placed by its start offset only, because frame lengths live in each FLAC frame header. Inserted silence is capped at ten minutes per file so a broken timestamp cannot materialise hours of it. |

## Pure-Zig Implementation Tracker

| Area | Status | Next Work |
| --- | --- | --- |
| Shared PCM boundary | Stable | Keep model-specific feature policy in `src/pipelines/audio.zig`. |
| WAV | Broad pure-Zig coverage | Performance tuning only if profiling warrants it. |
| AU/SND | Common pure-Zig coverage | Add real-world malformed/edge corpus cases as found. |
| AIFF/AIFC | Common pure-Zig coverage | Decide how far to go beyond PCM/float/G.711 before falling back. |
| CAF | `lpcm`/G.711 pure Zig, checked-in 16-bit/24-bit stereo ALAC pure Zig | Broaden ALAC compressed-frame coverage beyond the checked-in stereo fixtures. |
| MP4/M4A demux | Pure-Zig demux for checked-in AAC/ALAC, including 16 kHz stereo, 44.1 kHz mono, and 44.1 kHz short-window AAC fixtures, `stsz`/`stz2` sample tables, stable single-description `stsc` chunk maps even when the selected description is not entry 1, version-0/1/2 audio sample entries, direct or `wave`-wrapped config atoms, order-insensitive audio `mdia` handler/minf parsing, order-insensitive `moov` `mvhd`/`trak` scanning, skipping earlier unsupported audio tracks when a later one is supported, contiguous-media-edit trim metadata | Widen ISO BMFF/QuickTime layouts beyond discontiguous edit-list timing and broader multi-track/container shapes. |
| AAC Main/AAC-LC and HE-AAC/PS | Bounded pure-Zig qualification | Core fixtures cover ADTS/MP4 tools, 960-sample MP4 cores, edit-list and iTunSMPB priming. Five owned HE-AAC/PS fixtures compare complete trimmed PCM with independent FFmpeg output at a 50 dB SNR floor. See the AAC notes for profile limits. |
| MP3 | Narrow pure-Zig decode lane | Broaden Huffman/requantization/oracle coverage before calling it general. Unsupported MP3 shapes now fail closed. |
| FLAC | Narrow pure-Zig decode lane | Add malformed/real-world corpus coverage and more stream-shape coverage before calling it general. |
| Ogg container | Narrow pure-Zig owned codec mappings | Pure-Zig Ogg dispatch now owns the checked-in Vorbis, Opus, and FLAC-in-Ogg lanes and fails closed on unsupported codec/container shapes. Broaden packet/page coverage only if we intend to own more Ogg shapes. |
| Opus | Full RFC 6716 decoder, no PLC | CELT, SILK, and hybrid frames at every frame size, mono/stereo streams with mid-stream channel and mode switches, redundancy frames, and code 0-3 packets (including multi-byte padding) decode through the libopus ports in `opus_celt.zig` and `opus_silk.zig`; the Xiph sweep checks every RFC 8251 vector at 40 dB SNR minimum. Remaining gaps are packet loss concealment, DTX comfort noise, and multistream (`mapping_family` other than 0). |
| Vorbis | Pure-Zig decode, reference-exact | Validated stage by stage against stb_vorbis traces and end to end against ffmpeg and libvorbis on the Xiph vector set. The bugs that had kept it apart were all spec-reading ones: codewords must be assigned in entry order (Vorbis I 3.2.1, not DEFLATE canonical order), floor1 subclass books are stored +1, residue type 2 interleaves the submap's channels with its limits on the interleaved length, classword digits past the last partition are still consumed, the window flags are read from the mode's block flag (not from the block size, which breaks when both block sizes are equal), the dB table is the exact 256-entry one, the IMDCT is unnormalised, and audio starts at the first packet's centre. |

## AAC Notes

AAC Main/LC retains the existing long/short-window, mono/stereo, PNS, TNS,
M/S/intensity, prediction, PCE and narrow MP4 960-sample core lanes. Unsupported
object types and required tools fail explicitly. An SBR sync declaration without
an enhancement payload preserves the compatible LC core without inventing bands.

HE-AAC and HE-AAC v2 use the maintained Apache-2.0
[AAC enhancement core](src/aacplus/PORTING.md), with complex QMF synthesis and
parametric stereo. Runtime builds compile Zig source without a codec library.
The current contract is 1024-sample cores, 1x/2x output, one SBR element (stereo
CPE or mono SCE, including PS), one or two output channels, sample rates up to
192 kHz and access units up to 64 KiB. Separate-SCE-pair SBR, enhancement over
960-sample cores and arbitrary MPEG-4 Audio profiles are outside this contract.
Implicit ADTS SBR/PS discovers the actual output clock/channel layout. MP4 uses
its declared output layout and applies container priming/remainder trimming.

Five owned fixtures cover 44.1/48 kHz HE-AAC/PS, mono, distinct stereo channels,
high-frequency tones, chirps, noise, silence and transients. Independent FFmpeg
PCM16 references check the complete trimmed output with a 50 dB SNR floor.
Visitors emit at most 4096 interleaved samples per block using bounded codec
state. Batch/visitor comparisons check the shared PCM contract. Allocation-failure,
consumer cancellation and deterministic truncation/mutation tests exercise cleanup;
a failed decode or consumer retires its decoder state. Broader real-world and
malformed-input qualification remains necessary before claiming general conformance.
The former synthetic high-band/stereo approximation has been removed.

## MP3 Notes

The MP3 backend is now fully pure Zig in both runtime and test/build paths. It
is not yet a complete general MP3 replacement.

Implemented pieces include:

- Sync scan, frame walking, header parsing, side-info parsing, reservoir-aware
  granule payload extraction, and region planning.
- MPEG-1 and LSF scalefactor handling for covered lanes.
- Huffman coverage for the checked-in fixtures, including table-zero regions,
  codebook-24 family, smaller table families used by the corpus, and `count1`
  quad decode.
- Requantization, stereo processing, alias reduction, IMDCT, hybrid overlap-add,
  and synthesis filterbank for the covered paths.
- Free-format frame-length inference, padding-aware free-format handling, and
  mode-switching stereo output for checked-in minimp3 vectors.

Remaining MP3 work:

- Wider Huffman and requantization coverage from broader corpora.
- Wider full-stream checked-in and external corpus breadth beyond current
  vectors.
- Decision on whether any higher-level caller needs stereo end-to-end beyond
  the shared interleaved decode boundary.
- More conformance breadth before calling the current fail-closed boundary
  general-purpose.

## Performance Notes

### Local Benchmark Baseline

> **Relocated:** The dated single-run benchmark baseline that previously lived here (28 lines, 2026-04-14) is preserved verbatim in [docs/design/audio/history/benchmark-baseline-2026-04.md](../../../docs/design/audio/history/benchmark-baseline-2026-04.md).

Run with:
`zig build -Doptimize=fast bench-audio -- --bench all --warmup-iters 2 --measure-iters 20`

Implemented SIMD/algorithmic work:

- FFT-based STFT replaced the old naive DFT mel path.
- SIMD dot product for mel filterbank accumulation.
- SIMD max/clamp/scale for Whisper-style normalization.
- Stereo downmix fast path for interleaved two-channel PCM.
- Direct WAV decode/encode fast paths for common PCM and float formats.
- MP3 Huffman primary lookup tables plus faster bit-window reads.
- MP3 full-stream decode scratch reuse for granule pairs, quads,
  coefficients, hybrid blocks, subband samples, and PCM staging.
- Vorbis codebook primary lookup tables plus faster LSB bit-window reads.
- AAC spectral/scalefactor Huffman primary lookup tables.
- Vorbis decoded-floor scratch reuse and precomputed floor-1 x ordering.
- AAC sequence filterbank scratch reuse for long/eight-short window buffers.
- AAC dequantization lookup tables for common `x^(4/3)` magnitudes and
  scalefactor powers.
- Shared AAC/Vorbis FFT-based IMDCT with pruned `N/4` power-of-two transforms
  and Bluestein fallback for non-power-of-two windows.
- Fused AAC long-window windowing plus overlap output so the decoded long-block
  scratch is not walked once for windowing and again for overlap-add.
- Fused Vorbis packet-window multiplication into timeline overlap-add so packet
  block samples are not walked once for windowing and again for mixing.

Pending only if profiling shows value:

- SIMD for MP3 IMDCT/DCT32/synthesis hot loops.
- Additional SIMD in codec-specific transforms once correctness is stable.

## Conformance

Checked-in fixtures live under `lib/audio/testdata`.

Current test layers:

- `zig build test-audio` for shared public audio behavior.
- `zig build test-audio-internals` for selected stable module-level codec and
  container internals.
- `zig build audio-open-corpus -- <dir>` for a repo-native non-MP3 corpus
  runner that checks files represented in the shared passing table against the
  pure-Zig decode path and the owned checked-in reference case, without linking the
  FFmpeg oracle.
- `zig build audio-xiph-corpora-e2e-fetch` / `zig build audio-xiph-corpora-e2e-run`
  for an opt-in external Xiph-family sweep over upstream Vorbis, Opus,
  Vorbis-tools, Opus-tools, the RFC 8251 Opus packet vectors, the legacy Xiph
  Vorbis vector set, the official Opus example-file sets, the small secure
  stereo FLAC master set from `media.xiph.org`, and
  `flac-test-files`, without linking the FFmpeg oracle.
- `zig build audio-misc-corpora-e2e-fetch` / `zig build audio-misc-corpora-e2e-run`
  for an opt-in external MP3 and AAC/MP4 sweep over public minimp3 vectors
  plus fetched public `.aac`, `.m4a`, and `.mp4` samples, without vendoring
  media into the repo.
- The shared checked-in non-MP3 codec corpus currently covers all 58 encoded
  fixtures under `lib/audio/testdata/codec-corpus`, with decoded shape and,
  where useful, PCM closeness checked against owned WAV references with explicit
  thresholds.
- The upstream Xiph-family runner now cleanly distinguishes
  `expected_unsupported` corpus negatives from real decode regressions, so the
  diagnostic summary is about actual parity gaps rather than malformed-input
  vectors we intentionally fail closed on.
- The current Xiph-family external runner summary is `success=124`,
  `expected_unsupported=10`, `unsupported=0`, `decode_failed=0`, `crashed=0`.
- The current active external encoded-fixture coverage is `flac-test-files`,
  the RFC 8251 Opus packet vectors, the official Opus example-file sets, the
  secure stereo FLAC master set from `media.xiph.org`, and the fetched legacy
  Xiph Vorbis vector set. The fetched Vorbis/Vorbis-tools/Opus repos still do
  not currently contribute encoded `.ogg`/`.oga`/`.opus` fixtures to the
  recursive sweep.
- A shared test also verifies that every checked-in encoded corpus file is
  represented in the passing table, so fixture additions cannot silently bypass
  the corpus summaries.
- The test suite also verifies that the shared passing fixtures and
  `lib/audio/testdata/codec-corpus/README.md` stay aligned in both directions,
  so corpus provenance stays consistent with the checked-in files.
- The shared non-MP3 corpus now also documents its explicit passing,
  decode-but-not-claimed, and unsupported lane ownership in the same style as
  the MP3 corpus notes.
- Unsupported and fallback cases are tracked explicitly instead of silently
  broadening support claims.

Conformance rules:

- Keep expected failures explicit.
- Prefer small checked-in smoke vectors plus open corpus runners for broad
  coverage.
- Keep external-corpus runners diagnostic unless the upstream lane is stable
  enough to gate.
- Do not claim a pure-Zig lane unless it is wired end-to-end and covered by
  tests.
- Keep FFmpeg as the reference oracle until the pure-Zig backend for a codec has
  broad enough coverage to stand alone.

## Non-Goals

- No attempt to reimplement FFmpeg wholesale in Zig.
- No attempt to extend Vorbis past Vorbis I (no floor0-heavy legacy
  encoders beyond the vector set, no Vorbis II) without a reference to
  validate against.
- No browser-side encoded-codec decode in Zig before there is a concrete reason
  to replace `AudioContext`.


## Model-aware windows and cancellation

`DecodeOptions.control` supplies a borrowed cancellation/deadline check for
synchronous decoding. Packet/frame loops in MP3, AAC, ALAC, FLAC, Opus and Vorbis
check it between coded units; the scope restores the prior caller control on
success or failure. The callback must outlive the decode and must not suspend.
The model-aware runner also checks cancellation between model windows and source
reads. WAV inputs can decode exact sample ranges without retaining full PCM;
MP3, FLAC, AAC (including SBR/PS), ALAC and Opus/Vorbis visitors retain codec prediction/overlap
state while emitting bounded PCM blocks. A two-block queue overlaps decoding with
model work, retaining only the current PCM window plus overlap. MP4/CAF priming
and remainder trimming, Opus gain/pre-skip, channel clipping before downmixing and
supported WebM timeline gaps preserve the ordinary decode contract. Unsupported incremental profiles and single-threaded execution use the existing
bounded full-decode fallback. AIFF/AIFC, AU and PCM/G.711 CAF deliver at most 1024 frames per
block, including cancellation checks. Chained Ogg Vorbis, Opus and FLAC (including mixed-codec chains) reset codec
state per logical stream and normalize to the maximum sample rate/channel count
using a previous frame and one output block. Opus pre-skip/gain and each logical
stream's final granule remain local to that stream. WebM FLAC without declared
sample totals uses a PCM-free counting pass before streaming execution.
Compressed input and packet metadata are still contiguous bounded owners.
Metadata without a trustworthy duration uses a bounded PCM-free validation pass;
that trades additional decoding for stable final-tail and continuation semantics.
See [model-aware chunking](../../../docs/plans/model-aware-media-chunking.md) for
sample clocks, duration/token budgets, pause boundaries and coverage metadata.

The streaming regressions compare block output with batch PCM for container
trimming, gain, fractional chained-stream resampling and undeclared FLAC totals.
Two-minute AU/CAF inputs execute with 16 KiB of visitor scratch; consumer
cancellation propagates immediately. Run from the repository root:

```sh
zig build --build-file zig/pkg/inference/build.zig test-audio-streaming -Dmetal=false -Doptimize=ReleaseSafe
zig build --build-file zig/pkg/inference/build.zig test-audio-codecs -Dmetal=false -Doptimize=ReleaseSafe
```
