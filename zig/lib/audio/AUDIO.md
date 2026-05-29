# Audio Support

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
| MP3 | Pure Zig | Pure-Zig coverage includes checked-in mono, free-format stereo downmix, and mode-switching stereo vectors; unsupported MP3 shapes now fail closed. External fetched MP3 vector coverage is now being widened through the misc e2e lane. |
| AAC/ADTS | Pure Zig for a narrow AAC Main/AAC-LC lane | Supports checked-in long/short-window mono/stereo AAC-LC shapes, real mono PNS/TNS-gain fixtures, real stereo low-bitrate CPE/TNS fixtures, narrow MP4/M4A `frame_length_960` AAC-LC access-unit shapes, a narrow MP4/M4A AAC-LC core shape with SBR sync-extension signaling, payload-structure-aware sync-extension SBR/PS postprocess when a real `FIL` enhancement payload is present, narrow explicit HE-AAC/PS object-type long-window synthetic shapes via synthesized 2x highband enhancement, explicit PCE/layout validation, non-predictive AAC Main mono/stereo synthetic shapes, synthetic AAC Main long-window predictor-data coverage, and synthetic PS mono-core stereo output plus stereo intensity/TNS/gain CPE coverage. Unsupported AAC tools and broader enhancement-profile syntax now fail closed. |
| MP4/M4A/QuickTime audio | Pure Zig demux plus codec-specific pure-Zig decode for the owned AAC/ALAC lane | Demuxes checked-in AAC and ALAC access units/config, including the checked-in 16-bit and 24-bit stereo ALAC fixtures. Handles `stsz`/`stz2` sample sizes, direct or `wave`-wrapped codec config, QuickTime `qt  ` brand/`.mov`/`.qt` aliases, later supported audio tracks after earlier unsupported ones, and contiguous-media-edit plus leading/trailing-empty-edit `elst` timing for priming/remainder trimming; unsupported codec/container shapes fail closed. |
| FLAC | Pure Zig | Covers the checked-in native `.flac` 16-bit and 24-bit fixtures through the shared `lib/audio` boundary, plus the external `flac-test-files` sweep including the 32-bit `uncommon/05 - 32bps audio.flac` vector and the raw-frame-start/prefix-noise cases via raw-frame fallback. Only the intentionally malformed `faulty/` negatives and the intentionally unrepresentable mid-stream-format-change `uncommon/01-04` vectors remain expected unsupported. |
| Ogg/FLAC | Pure Zig for the narrow checked-in mapping | Reassembles Ogg pages/packets for the checked-in FLAC-in-Ogg mapping and feeds the result into the native FLAC decoder; unsupported Ogg/FLAC shapes now fail closed. |
| Ogg/Vorbis | Pure Zig for the checked-in stereo fixtures | Shared Ogg page/packet parsing now tracks BOS/EOS, packet boundaries, and page-end granule ownership, and the pure-Zig Vorbis lane now covers the checked-in stereo `.ogg`/`.oga` corpus through floor1, residue reconstruction, inverse coupling, IMDCT/windowing, and overlap/add; unsupported Vorbis shapes now fail closed. |
| Ogg/Opus, Opus | Pure Zig for the checked-in mono/stereo 48 kHz CELT fixtures plus narrow checked-in SILK/hybrid Ogg probes | Shared Ogg parsing, `OpusHead`/TOC/frame-packing parsing, trim/gain helpers, range decoding, raw tail-bit reads, CELT header parsing, coarse/fine energy reconstruction, narrow CELT PVQ/residual decode, and packet-to-packet overlap resynthesis now cover the checked-in mono `.opus` fixture plus the checked-in stereo `.opus`/Ogg alias pair. The real CELT corpus now also covers checked-in mono 5 ms restricted-low-delay packets, checked-in stereo 2.5 ms restricted-low-delay packets, checked-in stereo 40 ms and 60 ms multi-frame packets, and checked-in mono 120 ms multi-frame packets, while the older checked-in stereo corpus still explicitly exercises coupled and dual-stereo low bands below the intensity boundary plus intensity-shared stereo tail bands, with alias-plan parity checks between `.opus` and Ogg/Opus. On the SILK side, generated low-bitrate WB probe frames now exercise real packet-header parsing, side-info/pulse decode, WB parameter reconstruction, excitation synthesis, and narrow sample reconstruction, and those paths now also have both synthetic Ogg end-to-end runtime coverage and checked-in real Ogg fixture coverage for mono/stereo SILK and mono/stereo hybrid streams, including checked-in 10 ms SILK packet shapes, checked-in mono/stereo WB FEC/LBRR fixtures, checked-in mono/stereo 10 ms, 40 ms, and 60 ms WB FEC/LBRR fixtures, plus non-FEC 40 ms and 60 ms multi-internal-frame SILK packet shapes. The hybrid lane now also has checked-in 10 ms and 20 ms mono/stereo Ogg fixture coverage, including checked-in 10 ms and 20 ms mono/stereo hybrid FEC/LBRR fixtures. Hybrid probe frames also decode a real CELT highband residual after the SILK front and integrate that highband into mixed 48 kHz PCM. The external RFC 8251 Opus packet-vector sweep is now green under the pure-Zig raw packet-stream path, including mixed mono/stereo packet-topology streams, with unsupported CELT packets inside those vectors degraded to zero-filled packet-local output instead of failing the whole stream. Unsupported Opus shapes now fail closed. |

## Pure-Zig Implementation Tracker

| Area | Status | Next Work |
| --- | --- | --- |
| Shared PCM boundary | Stable | Keep model-specific feature policy in `src/pipelines/audio.zig`. |
| WAV | Broad pure-Zig coverage | Performance tuning only if profiling warrants it. |
| AU/SND | Common pure-Zig coverage | Add real-world malformed/edge corpus cases as found. |
| AIFF/AIFC | Common pure-Zig coverage | Decide how far to go beyond PCM/float/G.711 before falling back. |
| CAF | `lpcm`/G.711 pure Zig, checked-in 16-bit/24-bit stereo ALAC pure Zig | Broaden ALAC compressed-frame coverage beyond the checked-in stereo fixtures. |
| MP4/M4A demux | Pure-Zig demux for checked-in AAC/ALAC, including 16 kHz stereo, 44.1 kHz mono, and 44.1 kHz short-window AAC fixtures, `stsz`/`stz2` sample tables, stable single-description `stsc` chunk maps even when the selected description is not entry 1, version-0/1/2 audio sample entries, direct or `wave`-wrapped config atoms, order-insensitive audio `mdia` handler/minf parsing, order-insensitive `moov` `mvhd`/`trak` scanning, skipping earlier unsupported audio tracks when a later one is supported, contiguous-media-edit trim metadata | Widen ISO BMFF/QuickTime layouts beyond discontiguous edit-list timing and broader multi-track/container shapes. |
| AAC Main/AAC-LC | Narrow pure-Zig decode lane | Broaden conformance before claiming general AAC. Narrow MP4/M4A `frame_length_960` access-unit decode is now covered, sync-extension SBR now has a narrow payload-aware enhancement lane, and explicit HE-AAC/PS object-type configs now have a narrow synthesized highband/stereo-widening lane, but remaining hard cases still include wider real stereo CPE/tool combinations beyond the checked-in PNS/TNS/gain/short-window fixtures plus general enhancement-payload SBR/PS reconstruction beyond the current synthetic/object-type boundary. |
| MP3 | Narrow pure-Zig decode lane | Broaden Huffman/requantization/oracle coverage before calling it general. Unsupported MP3 shapes now fail closed. |
| FLAC | Narrow pure-Zig decode lane | Add malformed/real-world corpus coverage and more stream-shape coverage before calling it general. |
| Ogg container | Narrow pure-Zig owned codec mappings | Pure-Zig Ogg dispatch now owns the checked-in Vorbis, Opus, and FLAC-in-Ogg lanes and fails closed on unsupported codec/container shapes. Broaden packet/page coverage only if we intend to own more Ogg shapes. |
| Opus | Narrow pure-Zig CELT decode lane plus narrow SILK/hybrid probe decode lane | Checked-in `OpusHead`, TOC/frame-packing parsing, packet-duration/frame-count parsing, page-end granule checkpoint validation, output-gain scaling, range-decoder helpers, raw tail-bit reads, CELT header parsing, coarse/fine energy reconstruction, a narrowed CELT allocation/PVQ/residual path, and packet-to-packet overlap resynthesis now exist for the checked-in mono 48 kHz fixture and the checked-in stereo `.opus`/Ogg alias pair. The CELT lane now also has checked-in mono 5 ms restricted-low-delay coverage, checked-in stereo 2.5 ms restricted-low-delay coverage, checked-in stereo 40 ms and 60 ms multi-frame packet coverage, and checked-in mono 120 ms multi-frame packet coverage, while the older checked-in stereo alias pair is still explicitly asserted to keep residual-plan parity across container aliases and to exercise coupled or dual-stereo low bands below the intensity boundary plus intensity-shared tail bands. Generated low-bitrate SILK and hybrid probe frames now also exercise real packet-header parsing, side-info/pulse decode, WB NLSF/gain/LTP parameter reconstruction, excitation reconstruction, narrow SILK sample synthesis, actual hybrid highband decode/integration from the shared post-SILK range-decoder state, synthetic Ogg end-to-end runtime decode, and checked-in real Ogg fixture decode for mono/stereo SILK and mono/stereo hybrid streams. The SILK lane now also accepts checked-in LBRR/FEC-bearing mono and stereo WB packets, checked-in mono and stereo 10 ms, 20 ms, 40 ms, and 60 ms LBRR/FEC packet shapes, checked-in 10 ms packet shapes, and checked-in non-FEC 40 ms and 60 ms multi-internal-frame packet shapes instead of only 20 ms single-internal-frame packets. The hybrid lane now has checked-in 10 ms and 20 ms mono/stereo runtime coverage, including checked-in mono/stereo hybrid FEC/LBRR packet shapes at both durations. Unsupported Opus shapes now fail closed; remaining blockers are wider real-world Opus conformance outside the current checked-in CELT/SILK/hybrid corpus, not missing end-to-end runtime wiring for these narrow lanes. |
| Vorbis | Pure-Zig checked-in stereo Ogg decode | Checked-in identification/comment/setup parsing now feeds real floor1 decode, residue reconstruction, inverse coupling, IMDCT/windowing, and overlap/add for the checked-in stereo `.ogg`/`.oga` fixtures. Broader Vorbis coverage beyond that lane still needs more corpus and validation. |

## AAC Notes

The pure-Zig AAC path is capability-shaped rather than file-name-shaped. It
stays in Zig when the stream uses supported AAC Main/AAC-LC syntax and fails
closed when a required tool or container shape is not implemented.

Implemented AAC pieces include:

- `AudioSpecificConfig` and ADTS header parsing.
- Narrow AAC Main profile support, including synthetic long-window
  predictor-data coverage, where the bitstream shape otherwise matches the
  covered AAC-LC lane.
- Narrow MP4/M4A AAC-LC `frame_length_960` access-unit support for the
  supported mono/stereo long-window lane. ADTS remains on the 1024-sample
  framing path.
- Narrow MP4/M4A AAC-LC core compatibility when the `AudioSpecificConfig`
  carries an SBR sync extension but the packetized LC core remains directly
  decodable at the container sample rate.
- Narrow payload-structure-aware sync-extension SBR/PS support when access
  units carry a real trailing `FIL` enhancement payload marker; the current
  enhancement step now derives small synthetic envelope/noise/stereo hints from
  the carried payload bytes instead of treating all enhancement payloads the
  same, now also derives narrow tail-byte detail/phase hints rather than using
  those later payload bytes only as hash noise, applies that structure per
  access unit across multi-frame decode, and when one access unit carries
  multiple same-kind enhancement payloads the later payload now drives the
  narrow postprocess instead of being merged into a synthetic max/xor blend, can
  preserve older unrefreshed hint fields when a later same-kind payload is
  shorter than the earlier one instead of wiping them to zero, now also does
  the same for shorter real refresh payloads in later access units, can
  carry the last seen sync-extension enhancement profile across later access
  units that omit a fresh `FIL` marker, now also keeps the last plain-SBR
  shaping profile across later PS-only refresh access units instead of letting
  PS-marked bytes clobber it, can also carry the last seen
  PS-marked stereoization profile across later access units that only refresh
  the SBR-marked payload, tolerates leading non-SBR `FIL` metadata before the
  first channel element, scans independent stereo `SCE+SCE` access units for
  trailing enhancement payloads too, and only enables the narrow
  sync-extension PS mono-core stereo lane when the carried `FIL` payload is
  actually PS-marked rather than merely SBR-marked; pre-PS access units in a
  later-PS sequence now decode as dual-mono until the first PS-marked payload
  appears, and stale carried SBR/PS shaping now decays across repeated later
  access units that only refresh part of the enhancement state or omit it
  entirely.
- Narrow explicit HE-AAC/PS object-type support for synthetic long-window
  mono/stereo access-unit shapes via synthesized 2x highband enhancement to the
  extension sample rate, including explicit PS mono-core stereo output and the
  same carried enhancement-profile decay across later no-fill access units once
  a real trailing payload has appeared; in multi-access-unit explicit-object
  sequences, pre-payload access units stay on the conservative pre-enhancement
  lane until the first real trailing payload activates the narrow HE/PS
  postprocess, and explicit PS stereoization now stays off for SBR-only
  payloads until a real PS-marked payload appears.
- Long-window and checked-in short-window IMDCT/windowing/overlap-add.
- Mono `SCE` and stereo `CPE` PCM sequence reconstruction for supported
  checked-in AAC-LC shapes, including real mono PNS/TNS-gain fixtures and real
  low-bitrate stereo `CPE`/TNS ADTS fixtures.
- Synthetic stereo `CPE` coverage that combines gain-control, intensity, and
  TNS payloads with trailing SBR `FIL` enhancement payload markers.
- Pulse application, deterministic mono PNS injection, TNS coefficient
  mapping/filtering, structured gain-control payload parsing, MS stereo, and
  intensity-stereo reconstruction for the covered lanes.
- Explicit PCE mono/stereo layout inference and element-tag validation for ADTS
  and MP4/M4A access units.
- Leading PCE/DSE/FIL metadata skipping, metadata-only access-unit skipping,
  repeated-PCE consistency checks, mixed-sample-rate ADTS rejection, ID3v2/ID3v1
  metadata around ADTS frames, and CRC-protected ADTS frame handling.
- Real low-bitrate MP4/M4A SBR config detection so the pure-Zig path accepts
  only the checked core-compatible shape and still rejects broader
  enhancement-profile syntax rather than mis-decoding it.

Still intentionally not claimed:

- General AAC decode.
- General HE-AAC/SBR/PS enhancement-payload reconstruction beyond the current
  core-compatible SBR-signaled shape and narrow synthesized enhancement path.
- Arbitrary object types outside the current AAC Main/AAC-LC lane.
- Broad stereo CPE/tool combinations beyond the current checked-in corpus.

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

This is a local single-run baseline for future codec performance work. Compare
new runs on the same machine/toolchain before drawing conclusions.

- Date: 2026-04-14 15:23 PDT, updated after fused AAC/Vorbis
  window/overlap output.
- Commit: `dc9da56` plus current AAC/Vorbis working-tree performance changes.
- Host: Darwin 24.6.0 arm64.
- Zig: `0.16.0`.
- Command:
  `zig build -Doptimize=ReleaseFast bench-audio -- --bench all --warmup-iters 2 --measure-iters 20`

| Benchmark | Iterations | Total ms | ns/iter | MiB/s |
| --- | ---: | ---: | ---: | ---: |
| `mp3_decode` | 20 | 33.550 | 1,677,500 | 5.10 |
| `vorbis_decode` | 20 | 38.695 | 1,934,750 | 2.55 |
| `opus_decode` | 20 | 69.559 | 3,477,950 | 4.16 |
| `aac_adts_decode` | 20 | 45.755 | 2,287,750 | 4.54 |
| `aac_decode` | 20 | 45.749 | 2,287,450 | 4.86 |
| `mp3_synth` | 20 | 0.034 | 1,700 | 5,170.04 |

AAC per-iteration counters from the same run:

| Benchmark | Spectral parse ms | Spectral decode ms | Tools ms | Filterbank ms | IMDCT ms | Window ms | Overlap ms | Access units | Channel decodes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `aac_adts_decode` | 0.114 | 0.206 | 0.379 | 1.297 | 1.201 | 0.001 | 0.091 | 0 | 34 |
| `aac_decode` | 0.111 | 0.196 | 0.371 | 1.303 | 1.206 | 0.002 | 0.090 | 17 | 34 |

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
- No attempt to take on general Opus/Vorbis support casually; that is a real
  codec-surface expansion, not a small cleanup.
- No browser-side encoded-codec decode in Zig before there is a concrete reason
  to replace `AudioContext`.
