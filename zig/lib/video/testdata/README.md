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

## Qualified software H.264 fixtures

`h264-intra*.mp4` use progressive 8-bit 4:2:0 Baseline IDR pictures, CAVLC,
Intra16x16 prediction and explicitly disabled deblocking. The independent NV12
oracles cover all luma/chroma prediction modes, low/high quantization, cropping
and full range. `h264-pcm.mp4` adds I_PCM and emulation-prevention bytes.
`h264-intra-filtered.mp4` qualifies active in-loop deblocking.
The receipt pins every fixture/output hash and records FFprobe packet metadata.
Tests compare every decoded byte, exercise cancellation, bounded mutation and
allocation failure, and verify overlapping windows decode each selection once.

```sh
python3 zig/lib/video/scripts/generate_h264_fixtures.py
```

Generation requires an offline x264 development installation, pkg-config, a C
compiler and FFmpeg. The helper disables Intra4x4 through x264's analysis API;
FFmpeg's `partitions=none` option alone does not disable that intra tool. No x264
or FFmpeg code is linked into runtime decoding. CAVLC lookup tables have their
own pinned generator and BSD notice in `../THIRD_PARTY_NOTICES.md`.

Completed timing receipts and their measurement limits are in
[benchmarks/README.md](benchmarks/README.md).

### Broader H.264 tools

`h264-intra4*.mp4`, `h264-main-*.mp4`, `h264-high-*.mp4`,
`h264-baseline-p.mp4` and `h264-cavlc-b.mp4` are generated from FFmpeg's
procedural `testsrc2`, with an additional procedural fade for explicit P weighting.
They qualify CAVLC/CABAC, intra4/intra8, filtering, cropping, inter partitions,
spatial/temporal direct, weighted prediction, B-pyramid reference marking and
frame-number wrap. The tool receipt records encoder settings, tool versions,
presentation-order picture types and hashes for every MP4 and FFmpeg-decoded NV12.
These generated assets are contributed under the repository Apache-2.0 license.

```sh
python3 zig/lib/video/scripts/generate_h264_tools_fixtures.py
```

The CABAC table generator accepts pinned `codec/common/src/common_tables.cpp`
from OpenH264 revision `1a0073f0322c8b74cbcb75ca1bb1c3d19d75538d`, verifies its
SHA-256 and retains its BSD-2-Clause license:

```sh
python3 zig/lib/video/scripts/generate_h264_cabac_tables.py /path/to/common_tables.cpp
```

### Multiple slices, fields and native sample formats

`h264-high-jvt`, `h264-high-custom`, `h264-baseline-slices`, `h264-high-slices`,
`h264-high-constrained` and `h264-high-slice-threads` extend the tool receipt with
custom/default matrices, constrained prediction and slice boundaries. The advanced
receipt records every encoded file and native sample output:
[h264-advanced-oracle.json](h264-advanced-oracle.json).

`h264-high8/high10-*` include 4:2:2/4:4:4, lossless transform bypass and actual mixed
MBAFF fields, with CAVLC/CABAC, I/P/B pictures, multi-slice JVT matrices and filtered
and unfiltered variants. FFmpeg decodes at native planar depth, then the generator
interleaves Cb/Cr without quantization. Tests assert that the MBAFF corpus actually
contains field macroblocks.

`h264-paff-*`, `h264-mbaff-pcm-*` and `h264-intra-dc-*` have independently known
samples. FFmpeg confirms 8/9/10/12/14-bit vectors, including first-field references,
PAFF list reordering, spatial/temporal B prediction and distinct Cb/Cr QPs. The 11/13-bit vectors use normative
known values because FFmpeg lacks corresponding native pixel formats. Field pairs
start with an IDR field and use a non-IDR complementary second field. Standalone
fields in separate indexed packets are outside the current video API qualifier.

All seven `h264-groups-*` maps use known PCM samples and descending first-macroblock
arrival order, including both dynamic-map directions. `h264-redundant*` checks
primary-picture preservation and the explicit missing-primary error. FFmpeg does
not implement FMO, so these vectors are qualified against normative maps and known
PCM sample placement instead of being presented as FFmpeg conformance evidence.
The standalone ISO BMFF muxer preserves configurations ordinary muxers reject.

The `.nv12` suffix is historical: consult each receipt's `bit_depth` and
`chroma_format`. Native-depth files contain tightly packed Y then interleaved
Cb/Cr, using right-aligned little-endian `u16` above 8 bits. Field output is woven;
there is no implicit deinterlacing.

```sh
python3 zig/lib/video/scripts/generate_h264_advanced_vectors.py
python3 zig/lib/video/scripts/generate_h264_cabac_extensions.py /path/to/h264-202108-layout.txt
```

The extension generator reads factual mappings from ITU-T H.264 (08/2021),
Tables 9-25–9-33 and 9-43, from `pdftotext -layout` output. It verifies every
extended initial state against the pinned base table and rejects duplicate/missing
field-context rows. Each aliased initial state gets independently evolving CABAC
storage at runtime. No reference encoder/decoder is linked into the library.
