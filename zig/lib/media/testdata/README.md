# MP4 reference fixtures

These small clips are original synthetic FFmpeg `testsrc2`/sine output, licensed
under Apache-2.0 with this repository. No downloaded video is included.
`mp4-oracle.json` records source hashes, FFmpeg/FFprobe versions, and independent
packet and metadata receipts. Tests consume checked-in bytes without FFmpeg.

Regenerate from the worktree root with:

```sh
python3 zig/lib/media/scripts/generate_fixtures.py
```

The generator requires FFmpeg/FFprobe and rewrites one tail-moov fixture to use
compact sample sizes and 64-bit chunk offsets. Inspect receipt changes when
upgrading the reference tools. The checked-in receipts use version 9.0.2.

## Fragmented MP4 and WebM video

`fragmented.mp4` and `fragmented-bframes.mp4` qualify explicit moof-relative
fragment addressing, trex defaults, decode timestamps and composition offsets.
Their receipts pin hashes and FFprobe packet metadata. For the signed B-frame
fixture FFprobe shifts presentation time by 1,024 ticks; the test normalizes that
shift and independently verifies native decode preroll. `video.webm` is a VP9
indexing fixture with known-size clusters and unlaced blocks; its receipt pins
FFprobe positions, timestamps, durations and keyframe flags. Codec payloads are
indexed without a VP9 decoder dependency.

```sh
python3 zig/lib/media/scripts/generate_video_fixtures.py
```

FFmpeg and FFprobe are offline fixture tools only. Original generated patterns
are covered by the repository's Apache-2.0 license.
