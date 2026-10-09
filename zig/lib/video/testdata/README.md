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
start with an IDR field and use a non-IDR complementary second field. Complete
standalone fields in consecutive indexed packets are covered by the PAFF packet
receipt below.

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


`h264-paff-packets-oracle.json` adds 30 independently known native sample vectors
checked by FFmpeg with frame-rate passthrough. They cover CAVLC/CABAC standalone
fields, bottom-first pairs, I/P/B pictures, previous/current-field prediction,
long-term list reordering, IDR long-term fields and field MMCO 1–6, in 8-bit
4:2:0, 10-bit 4:2:2 and 14-bit 4:4:4. Paired-packet variants ensure the same field
marking behaves identically without a packet boundary. Each standalone packet
holds one complete field; the fixture mux preserves separate samples and signed
composition offsets. Known output has one woven picture per complementary pair.

Regenerate with `python3 scripts/generate_h264_paff_packets.py`. The shared
advanced-vector helpers can be imported without regenerating other fixtures.
Runtime validation includes field-specific reference identities, long PicNum 31,
callback slots for either field, actual dependency counts, signed presentation
intervals, resource limits, mismatched/missing complements and cancellation or
allocation failure after retaining a first field.


`h264-paff-assembly-oracle.json` adds 15 buffered assembly vectors. Regenerate with
`python3 scripts/generate_h264_paff_assembly.py`. CAVLC and bottom-first CABAC
fields are fragmented into individual slice packets with intervening AUD packets;
three pairs arrive in A-top/B-top/C-top/B-bottom/A-bottom/C-bottom order. Fragmented
variants test overlapping pending workspaces. A P-field begins before an unrelated
picture evicts its reference and finishes afterward, testing frozen prediction.
Each variant covers 8-bit 4:2:0, 10-bit 4:2:2 and 14-bit 4:4:4 native output.

The reordered packet streams are a qualified decoder assembly extension: H.264
complementary fields normally occupy consecutive coded access units. FFmpeg checks
canonical consecutive streams against independently known samples, not the
extension's packet order. Native tests check the modified transport separately.
Receipt member lists define exact packet membership; auxiliary packets use a distant
PTS to catch accidental inclusion in output timing. Tests validate every member
selection, callback slots, pending/slice/dependency limits, missing and duplicate
fragments, cancellation, reference copy-on-write and exhaustive allocation failures.

## Expanded codec qualification and asset attribution

`h264-sintel-original.mp4` is a stream-copy excerpt of the public Sintel trailer,
not an Apache-2.0 synthetic fixture. Sintel © Blender Foundation /
[durian.blender.org](https://durian.blender.org/about/), licensed
[CC BY 3.0](https://creativecommons.org/licenses/by/3.0/). Source:
[480p trailer](https://download.blender.org/durian/trailer/sintel_trailer-480p.mp4).
Source SHA-256: `b670602fa00934ca27c4351bb0efe7ea7a07fae57284e44226025eeed7c51254`.
The excerpt removes audio and copies video (`ffmpeg -ss 10 -i SOURCE -t 2 -an
-c:v copy OUTPUT`); keyframe preroll yields 66 pictures. Its oracle records exact
native sample hashes and media PTS, with movie edit lists disabled in FFmpeg.
The original source edit clock remains independently preserved by the native reader.

`h264-jm-*.mp4` are resized/re-encoded derivatives of four pictures beginning at
12 seconds in the same CC BY 3.0 source; retain the above attribution and license.
`scripts/generate_h264_jm.py` requires a locally built official
[JM 19.0](https://iphome.hhi.de/suehring/tml/download/jm19.0.zip) tree and the pinned
source. It invokes the encoder and independent decoder and hashes decoder output.
JM encoder reconstruction is explicitly not used as the sample oracle. No JM code
or binary is included or linked. Receipts pin both elementary and MP4 bytes,
component format/depth and sample hashes. Cases cover data partitions, separate
planes at 8/10/14 bits, primary SP, switching SP and SI.

The monochrome, dynamic epoch and frame-gap fixtures remain original Apache-2.0
normative synthetic assets. Their generators and receipts record construction and
independent FFmpeg checks where supported. These focused cases qualify bounded
paths rather than all possible combinations of syntax tools.

For additional real-content profiles, run `scripts/qualify_h264_corpus.py` with
`--source SOURCE --native zig-out/bin/video-qualification --output DIRECTORY`.
It records exact FFmpeg/libx264 commands, content hashes and full-frame differential
results for Baseline/Main/High10/High444. External corpus artifacts are not fetched
or generated during normal tests. See [expansion validation](h264-expansion-validation.md).

JM monochrome cases additionally qualify compressed CABAC 8-bit and CAVLC 10-bit I/P pictures. JM writes neutral 4:2:0 chroma for monochrome output; these receipts hash only each native Y plane without converting range/depth.

`generate_h264_transitions.py` adds original non-IDR compatible SPS/crop/reference
changes with predicted P-skip samples, plus non-IDR intra geometry epochs. Hashes
are checked against known samples and independent canonical IDR FFmpeg decodes;
the combined non-IDR transition streams are explicitly qualified extensions,
not claimed to be conforming SPS activation within one coded video sequence.
