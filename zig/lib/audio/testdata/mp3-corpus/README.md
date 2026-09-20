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
The low sampling frequency vectors are generated, not fetched:

- `lsf-short-22050.mp3`, `lsf-short-24000.mp3` (MPEG-2) and
  `lsf-short-12000.mp3`, `lsf-short-11025.mp3`, `lsf-short-8000.mp3`
  (MPEG-2.5) are one second of a 440 Hz tone interrupted by a click every
  250 ms, which is what makes the encoder emit the short blocks that reach the
  short scalefactor band tables.
- `lsf-tone-8000.mp3` is one second of an uninterrupted 440 Hz tone. Its
  energy has to stay at 440 Hz, which is what distinguishes the MPEG-2.5 8 kHz
  long bands from the MPEG-1 48 kHz table this rate used to fall back to.
- Each was produced with `ffmpeg`/`libmp3lame` from a formula, so they carry no
  third-party content:
  `ffmpeg -f lavfi -i "aevalsrc='if(lt(mod(t,0.25),0.002),0.9,0.5*sin(2*PI*440*t))':s=<rate>" -t 1 -ar <rate> -ac 1 -c:a libmp3lame -b:a 48k <name>.mp3`

When adding new vectors here:

- Prefer public-domain or permissively licensed MP3 conformance vectors.
- Record expected sample rate and comparison tolerances next to the vector
  metadata in code, including `expected_samples` where a reference decoder
  agrees on the exact length: a frame quietly dropped or duplicated changes
  that even when the audio that survives still sounds right.
- Keep expected sample counts and any explicit fail-closed semantics per vector
  in the checked-in metadata instead of weakening global checks.

Planned sources:

- public `minimp3` vectors
- ISO/IEC 11172-4 vectors when access and licensing allow

Deferred for later:

- additional stereo-output and other vectors that still need explicit
  expected-fail or recovery semantics before they belong in the passing
  pure-Zig corpus
