# Larger WebM codec measurements and qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Sintel copyright Blender Foundation / durian.blender.org, licensed under
[CC BY 3.0](https://creativecommons.org/licenses/by/3.0/). The six pinned streams
are derivatives of the original 480p trailer; `corpus/manifest.json` records
source URL/hash, commands, FFmpeg version, coded hashes and independent NV12
picture hashes. Full 854x480 streams contain 1253 pictures over 52.209 seconds;
1080p streams contain 288 pictures from a 12-second excerpt beginning at source
second eight. The 1080p copies upscale the 480p source and do not supply native
1080p detail. AV1 is encoded with SVT-AV1, adding encoder diversity to the AOM
fixtures. All coded streams and 4623 picture hashes reproduce byte-for-byte.
Only qualification tooling uses FFmpeg; builds never download codec sources or
link runtime codec libraries.

The native benchmark has `decode`, `cpu`, `metal`, `hash` and `dump` modes. Decode
includes independently owned semiplanar output packing, not merely header parsing.
CPU/Metal prepare every 24th visible picture to 384x384 with explicit SDR matrix
selection and torchvision resize. There is no model execution or GPU output
readback. `dump` writes one selected visible picture for differential diagnosis;
its final argument is that picture's ordinal, whereas other modes use it as the
preparation stride. Hidden packets still reconstruct references.

From `zig/lib/video`:

```sh
zig build install-webm-benchmark -Doptimize=ReleaseSafe -Dvideo-use-llvm=true
zig-out/bin/webm-benchmark testdata/benchmarks/webm/corpus/sintel-vp8-480p.webm decode 7 384 24
zig-out/bin/webm-benchmark testdata/benchmarks/webm/corpus/sintel-av1-1080p.webm metal 3 384 24
python3 scripts/qualify_webm_benchmarks.py --binary zig-out/bin/webm-benchmark --corpus testdata/benchmarks/webm/corpus --output /tmp/webm-parity
python3 scripts/generate_webm_benchmarks.py --source /path/to/pinned-sintel-original.mp4 --output /tmp/reproduced-webm --ffmpeg /path/to/ffmpeg
```

Every iteration creates and destroys a decoder session. Metal setup/coefficient
owners persist across iterations; warm runs upload no new coefficients. Completed
wall time includes decode, output destruction and fenced preparation. Command GPU
intervals are measured separately. `first-pass` does not claim an evicted filesystem
cache. Container indexing and backend setup are separate fields. Decoder live peak
tracks the codec allocation ledger; process RSS is a process-wide high-water mark;
device allocated bytes are an aggregate observation, not a per-request GPU peak.
Upload/copy counts are logical bytes, not measured bus traffic. Optional VP8
reference fields are null for VP9/AV1 because those internal copy counts are not
instrumented. Software decoding dominates the measured VP9 route.

Timed owned processes run sequentially without owned encoding or compilation
work overlapping them. Background host activity remains uncontrolled. VP8 uses
one first pass and six warm samples per variant; CPU/Metal preparation uses one
first pass and two warm samples. Median/p95 are observations on this host/compiler,
not deployment capacity estimates or stable population statistics.

The source-pinned copy baseline includes the same true-motion edge correction
as the sharing variant. Both agree with every independent VP8 picture hash.
To reproduce it, copy the maintained image module to a temporary directory,
apply `copy-baseline.patch` with `git apply --unidiff-zero -p4` from that
image directory, and pass that directory's
`src/mod.zig` with `-Dwebm-benchmark-image-source=ABSOLUTE_PATH`. This substitution
is benchmark-only and adds no production decoder switch.

| VP8 input | Copies before / after | Decoder peak before / after | Warm median before / after |
|---|---|---|---|
| 480p, 1253 pictures | 2,338,398,720 / 779,466,240 bytes | 4,357,800 / 3,113,640 bytes | 11.2471 / 10.8618 s |
| 1080p, 288 pictures | 2,707,292,160 / 902,430,720 bytes | 21,950,400 / 15,683,520 bytes | 12.1491 / 11.6402 s |

Reference-copy volume falls by two thirds; decoder allocation peak falls by
28.6%. Warm medians improve about 3.4% and 4.2% in these runs. Returned output
remains independent; immutable reference slots share internal owners. The
transactional allocation/cancellation tests preserve prior state on failure.

Long-stream differential testing also found the 16x16/chroma true-motion corner
sentinel defect at frame edges. The fix follows the boundary behavior described
by [RFC 6386](https://www.rfc-editor.org/rfc/rfc6386), with a small six-picture
real-stream regression retained outside this larger corpus. FFmpeg and libvpx
independently agree on the first formerly failing picture.

Linux ARM64 executes the portable suite and compares all corpus pictures against
the same independent hashes. Current pretrained CPU/Metal reports and exact
source/binary/artifact hashes are recorded in `qualification.json`. The final
macOS gate passes 160 tests with two platform skips; actual Linux ARM64 passes
137 with 25 skips. Both platforms match all 4623 independent picture hashes.
Twenty-four model tests and all eight CPU/Metal pretrained WebM vectors and
three rankings/top-k sets pass, including concurrent and disconnect/retry checks. General codec conformance, continuing fuzzing/recovery, measured
SIMD, H.264 reference copies, index/replay costs, cross-group reuse and the other
non-CUDA requirements remain active.


Header-only planning can be measured separately with `webm-benchmark INPUT index 7`
or `index_reconstruct 7`. `index_compare 2` checks every picture's PTS, duration
and geometry, plus sampled IDs, against full reconstruction on each iteration.
`completed_ns` measures logical-picture indexing alone; selection/comparison is
outside that interval. `container_ns` records initial container parsing. In
comparison mode RSS includes reconstruction and is not a header-only memory
measurement. These modes perform no model inference or GPU preparation.


`replay_compare 2` selects 32 logical pictures from the final quarter, then
compares packet-zero and codec-key replay. Both paths hash selected native
samples and compare ordinals, PTS/duration, geometry, depth/chroma and colour
metadata. Timings include selected hashing and completed decode, and exclude
model projection/preparation. Packet counts and sequence bootstrap bytes are
reported separately; shared-host timing does not establish capacity.


`cpu` and `cpu_scalar` provide matched integer-RGB preparation runs. Both use
the model's row-alignment RGB policy and the same geometry, decoding, sample
selection and resize/patch code. The SIMD mode changes only RGB row arithmetic.
`preparation_ns` excludes SHA256 verification; `completed_ns` includes it.
`cpu_prepared_sha256` hashes every prepared F32 patch array in order, enabling
exact full-output checks across modes. Metal does not populate a pixel hash
(the empty hash does not imply GPU output verification).
