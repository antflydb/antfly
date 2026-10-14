# Independently sourced real-content decoder qualification

Big Buck Bunny: **(c) copyright 2008, Blender Foundation /
www.bigbuckbunny.org**, licensed under [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/).
These excerpts retain that license. The generator is Apache-2.0 source code;
that license does not replace the movie's license.

The source comes from the [official movie archive](https://download.blender.org/peach/bigbuckbunny_movies/),
with attribution and license documented by the [Blender project](https://peach.blender.org/about/).
`bbb/manifest.json` pins the downloaded archive, extracted MP4, generator,
encoder version, commands, coded stream hashes and independent decoded picture
hashes. The original movie is a local qualification asset, not a build input.

Three excerpts start at source seconds 48, 160 and 300. Each contains 36 visible
pictures at 24 FPS, encoded separately as VP8, VP9 and AV1: nine streams and 324
pictures. Changes from the original are extraction, removal of audio, SAR 1 and
8-bit 4:2:0 transcoding at the original 320x180 geometry. FFmpeg/libvpx/SVT-AV1
are offline fixture/oracle tools; the maintained runtime remains native Zig.

This adds an independent movie source to the Sintel corpus. It does not prove
arbitrary-stream conformance or native high-resolution detail. Qualification
compares every visible picture's geometry, native depth/chroma and complete
normalized-plane SHA-256, rather than checking only a sampled image or an
embedding. Platform-specific reports retain binary and tool identity.

Generate into a fresh directory:

```sh
python3 zig/lib/video/scripts/generate_independent_webm_corpus.py \
  --source .benchmark-assets/native-video/BigBuckBunny_320x180.mp4 \
  --output zig/lib/video/testdata/real-content/bbb \
  --ffmpeg /opt/homebrew/bin/ffmpeg
```

The source hash must match the pinned original. The generator bounds the number
of independent pictures and rejects partial output or oracle failure.

Both macOS ARM64 and actual offline Linux ARM64 match all 324 independent
pictures, including geometry, native depth/chroma and every normalized sample.
A fresh generation reproduces all nine coded streams and all decoded hashes
byte-for-byte. `qualification.json` records the source, binary, runner, tool and
artifact hashes; `macos/report.json` and `linux-arm64/report.json` retain each
stream's outcome and raw picture hashes. The Linux report's platform field is
the host Python runner; decoding executes inside the pinned Linux image with
network disabled and read-only mounts, as recorded separately in the receipt.
`linux-runner.sh.txt` retains the executed wrapper (replace the worktree prefix
when replaying elsewhere).
