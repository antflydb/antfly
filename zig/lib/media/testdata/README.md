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
