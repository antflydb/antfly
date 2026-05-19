# Audio E2E

This directory documents opt-in end-to-end harnesses for broader upstream audio
corpora. These are not part of the checked-in codec corpus contract under
`lib/audio/testdata/`; they are a wider pure-Zig regression sweep.

Current harness:

- [`../audio_xiph_corpora_e2e.zig`](../audio_xiph_corpora_e2e.zig)
  - clones or reuses upstream sources for:
    - `xiph/vorbis`
    - `xiph/opus`
    - `xiph/vorbis-tools`
    - `xiph/opus-tools`
    - `ietf-wg-cellar/flac-test-files`
    - RFC 8251 Opus packet vectors (`opus_testvectors-rfc8251.tar.gz`)
    - official Opus example files from `https://opus-codec.org/static/examples/`
      and `https://media.xiph.org/opus/samples/examples/`
    - stereo FLAC masters from `https://media.xiph.org/`
    - legacy Xiph Vorbis vectors from `https://people.xiph.org/~xiphmont/test-vectors/vorbis/`
  - walks `.ogg`, `.oga`, `.opus`, and `.flac` files recursively
    plus RFC Opus `.bit` packet streams
  - probes each file in a subprocess so decoder panics are isolated as
    `crashed`
  - exercises the pure-Zig shared audio path without linking the FFmpeg oracle
  - reports `success`, `expected_unsupported`, `unsupported`,
    `decode_failed`, and `crashed` counts
  - currently treats malformed FLAC negatives under
    `flac-test-files/faulty/`, the intentionally unrepresentable
    `flac-test-files/uncommon/` mid-stream-format-change vectors, and the
    current legacy Vorbis vector lane as `expected_unsupported`
  - currently exercises real external encoded fixtures from
    `flac-test-files`, the RFC Opus packet-vector set, the official Opus
    example-file set, a small stereo FLAC master set from `media.xiph.org`,
    and the fetched legacy Vorbis vector set; the fetched
    `vorbis`/`vorbis-tools`/`opus`/`opus-tools` repos still do not currently
    add checked external `.ogg`/`.oga`/`.opus` fixture files to the recursive
    sweep

- [`../audio_misc_corpora_e2e.zig`](../audio_misc_corpora_e2e.zig)
  - fetches or reuses external non-Xiph audio samples for:
    - `lieff/minimp3` public `.bit`/`.pcm` vector pairs
    - public AAC and MP4/M4A sample assets
  - walks `.mp3`, `.aac`, `.m4a`, and `.mp4` files recursively
    plus minimp3 `.bit` vectors with their sibling `.pcm` references
  - exercises the pure-Zig shared audio path without vendoring the media
  - reports `success`, `expected_unsupported`, `unsupported`,
    `decode_failed`, and `crashed` counts
  - is intended to become the fetched breadth lane for MP3 and AAC/MP4,
    analogous to the Xiph-family lane for Vorbis/Opus/FLAC

Suggested usage:

```sh
zig build lib-audio-conformance-fetch
zig build lib-audio-conformance-run
zig build lib-audio-conformance
```

Quick local status:

```sh
zig run lib/audio/audio_xiph_corpora_e2e.zig -- status /tmp/termite-audio-xiph-corpora
```

Fetch once explicitly if needed:

```sh
zig run lib/audio/audio_xiph_corpora_e2e.zig -- fetch /tmp/termite-audio-xiph-corpora
```

Run the broad sweep:

```sh
zig run lib/audio/audio_xiph_corpora_e2e.zig -- run /tmp/termite-audio-xiph-corpora
```

Current clean summary:

```text
Xiph-family: success=124 expected_unsupported=10 unsupported=0 decode_failed=0 crashed=0
Misc MP3/AAC/MP4: success=12 expected_unsupported=0 unsupported=0 decode_failed=0 crashed=0
```

Single-file probe:

```sh
zig run lib/audio/audio_xiph_corpora_e2e.zig -- probe-one /tmp/termite-audio-xiph-corpora vorbis/test/whatever.ogg
```
