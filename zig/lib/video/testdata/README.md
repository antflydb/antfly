# Frame-selection reference fixtures

`reference/hf_sample_frames.py` preserves the inspected Hugging Face Transformers
EmbeddingGemma 2 `sample_frames` method (Apache-2.0, Hugging Face copyright).
Its source URL and observation date are in the file. See the
[upstream license](https://github.com/huggingface/transformers/blob/main/LICENSE).
The snapshot SHA-256 pins the actual observed method; remote commit resolution
was unavailable. Runtime tests need neither Python nor Transformers.

`sampling-oracle.json` contains 13 boundary cases and 64 seeded cases verified
by executing that snapshot with NumPy 2.4.4. From the worktree root, regenerate
with an environment containing NumPy:

```sh
python3 zig/lib/video/scripts/generate_sampling_fixtures.py --numpy
```

The generator also checks an independent standard-library transcription against
the executed method. Running without `--numpy` records that upstream execution
was skipped; such a receipt does not satisfy the checked-in conformance test.
Review snapshot and receipt changes together when updating processor policy.

## Decoder and preparation fixtures

`decode-bframes.mp4` copies the original shared media fixture; its NV12 receipt
is an independent FFmpeg 9.0.2 decode in presentation order. Native tests compare
selected pictures by PTS with a maximum byte difference of two. `prepare-sdr.mp4`
is original 160×96 testsrc2 output used for resize/rotation tests.
`decode-hardware.mp4` is an original 640×360 four-picture B-frame clip used for
hardware-only route qualification. All are Apache-2.0 synthetic assets.
`decode-oracle.json` pins their hashes and tool version.

Regenerate from the worktree root (requires FFmpeg with libx264):

```sh
python3 zig/lib/video/scripts/generate_decode_fixtures.py
```

CPU/Metal comparisons use explicit SDR color/chroma conventions and shared
quantized bicubic coefficients. They qualify that preparation contract; model
geometry, official decoder color conversion, vision tokens, and embeddings still
need end-to-end processor/model qualification after PR #1014 is integrated.

## Scheduling fixtures

`schedule-closed.mp4` and `schedule-open.mp4` are original Apache-2.0 testsrc2
clips: 60 pictures at 10 FPS, fixed GOP 10, and two B pictures. Closed GOPs carry
IDR starts; open GOPs advertise non-IDR recovery pictures that require prior
dependencies. `scheduling-oracle.json` records hashes, tool versions and FFprobe
packet clocks/positions/flags. Tests compare optimized native output exactly
against decode-from-start and verify actual range read/submission counts.

```sh
python3 zig/lib/video/scripts/generate_scheduling_fixtures.py
```

For packet indexes 12 and 59 the closed-GOP fixture reads/submits 13 packets
(14,137 payload bytes) instead of 60 (61,067 bytes); the open-GOP fixture falls
back to the initial IDR. Container indexing is excluded from these counters.
No GPU latency or embedding speedup is inferred from packet savings.

## Portable MJPEG fixture

`mjpeg.mov` is original Apache-2.0 testsrc2 output: eight 64×48 baseline 4:4:4
JPEG pictures over two seconds. `mjpeg.rgba` is independent FFmpeg 9.0.2 output;
`mjpeg-oracle.json` pins hashes, byte counts and FFprobe packet clocks/positions.
Native and WASI tests compare every RGBA channel with maximum error three. This
qualifies the 4:4:4 fixture, not chroma-upsample parity for subsampled JPEGs.

```sh
python3 zig/lib/video/scripts/generate_mjpeg_fixtures.py
```

The generator requires FFmpeg/libavcodec only offline. Runtime decoding uses the
existing pure Zig JPEG implementation with complete per-sample tables.

The same fixture qualifies `mjpeg_metal.prepareWindows` against CPU patches.
Separate versus shared overlapping-window jobs prepare ten versus eight pictures
and stage 122,880 versus 98,304 RGBA bytes. Tests assert depths one/two/eight,
logical staging caps, buffer/source lifetimes, cancellation/retry and allocation
failure. Synthetic 160×96 RGBA patterns additionally compare every rotation and
both centering modes within `2e-6`; alpha is ignored consistently. These are
preparation/resource receipts, not model or physical bus-transfer benchmarks.
