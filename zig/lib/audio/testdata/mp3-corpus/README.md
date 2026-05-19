# MP3 Conformance Corpus

This directory is for checked-in MP3 regression and conformance vectors used by
the pure-Zig decoder harness.

Current status:

- The checked-in pure-Zig MP3 conformance metadata lives in
  `lib/audio/src/mp3/mp3.zig`.
- The MP3 layer now also has an interleaved stereo decode path, while the
  generic `lib/audio` facade still intentionally exposes mono PCM.
- The checked-in smoke fixture is still `lib/audio/testdata/tone.mp3`.
- The first checked-in public vectors are:
  - `l3-compl.bit`
  - `l3-si.bit`
  - `l3-si_huff.bit`
  - `l3-he_free.bit`
  - `l3-he_mode.bit`
When adding new vectors here:

- Prefer public-domain or permissively licensed MP3 conformance vectors.
- Record expected sample rate and comparison tolerances next to the vector
  metadata in code.
- Keep expected sample counts and any explicit fail-closed semantics per vector
  in the checked-in metadata instead of weakening global checks.

Planned sources:

- public `minimp3` vectors
- ISO/IEC 11172-4 vectors when access and licensing allow

Deferred for later:

- additional stereo-output and other vectors that still need explicit
  expected-fail or recovery semantics before they belong in the passing
  pure-Zig corpus
