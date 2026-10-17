# Progressive H.264 reference-storage qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Completed progressive pictures share planar, motion and macroblock metadata
storage with the DPB and snapshots. Normal workspace reuse allocates without
copying old payloads; late-slice mutation first detaches with a copy. Allocation
failure leaves the old owner intact. PAFF/MBAFF field completion still copies.
The optional counters measure progressive reference payload traffic, excluding
output packing, reset clearing and field completion. Admission stays conservative.

The video suite passes 199 tests with two platform skips on macOS ARM64 in
ReleaseSafe. It includes transactional snapshot/detach ownership, exhaustive
allocation-failure checks and exact copying/shared output comparisons for
Baseline, Main/B-frame, High and High 10 4:4:4 fixtures. The model suite passes
27 tests. The Linux ARM64 ReleaseSafe suite cross-compiles successfully; fresh
Linux execution is unavailable because the local Docker daemon has no socket
and its supported restart timed out. Prior Linux receipts qualify prior source
states and do not substitute for this change's runtime check.

All 1,253 decoded pictures of the independently pinned Sintel trailer match
FFmpeg 9.0.2 NV12 SHA-256 values exactly after presentation-time ordering.
This tests an additional real 854×480 H.264 clip, not general conformance.

The trailer is from Blender Foundation's Sintel project, licensed CC BY 3.0:
<https://download.blender.org/durian/trailer/sintel_trailer-480p.mp4>. Its SHA-256
is `b670602fa00934ca27c4351bb0efe7ea7a07fae57284e44226025eeed7c51254`.
Original encoded media is not added to the repository. The recorded frame
hashes and benchmark output are qualification artifacts. FFmpeg is an external
validation tool and introduces no runtime product dependency.

Reproduce with the qualified Zig 0.17 development compiler:

```sh
zig build test-video install-video-tests install-video-benchmark -Doptimize=ReleaseSafe --prefix /tmp/reference-storage
zig build install -Doptimize=ReleaseSafe --prefix /tmp/reference-storage
/tmp/reference-storage/bin/video-benchmark sintel_trailer-480p.mp4 cpu-copy 3 384
/tmp/reference-storage/bin/video-benchmark sintel_trailer-480p.mp4 cpu 3 384
/tmp/reference-storage/bin/video-qualification hash sintel_trailer-480p.mp4
ffmpeg -i sintel_trailer-480p.mp4 -map 0:v:0 -an -pix_fmt nv12 -fps_mode passthrough -f framehash -hash sha256 sintel.framehash
zig build install-video-tests -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/reference-linux
```

Run `/tmp/reference-linux/bin/video-tests` on actual Linux to close the runtime
follow-up. Model gates run from `zig/pkg/inference` with
`zig build test-video-model -Doptimize=Debug`. Compare native hashes sorted by
`media_pts` against non-comment FFmpeg framehash rows in presentation order.
The manifest binds qualified source, tool/binary and artifact identities.

The benchmark prepares 38 unique 384×384 pictures from 1,251 dependency packets
of the 1,253-packet trailer. Completed CPU timing includes decoding, preparation,
CPU patch hashing and job cleanup; reader initialization precedes timing. The
first iteration is a fresh decode job, not a cold disk cache. There are only two
warm samples per path, with copying then sharing run sequentially on a shared
host; timings are diagnostic and do not establish a throughput improvement.
Process RSS is a process-wide high-water mark. The reported decoder bound is a
conservative logical reservation, not an allocator-observed physical peak.
Cross-group model reuse, field-reference sharing, sustained fuzz/conformance
qualification and wider full-pipeline performance remain in the active goal.

The final matched runs avoid 1,516,475,520 commit-copy bytes per iteration,
with zero late-slice copy bytes on this clip and identical CPU patch hashes.
Measured warm p50/p95 are 28.328/29.928 seconds for copying and 27.820/27.903
seconds for sharing. These small shared-host samples establish copy avoidance
and output preservation, not a statistically qualified speedup.

## Current Linux runtime follow-up

The [current Linux library gate](https://github.com/antflydb/antfly/blob/bf9d632d025a6b12837997f6aeb86e1a818ec831/zig/lib/video/testdata/current-linux-validation/README.md) now
executes all three shared-reference regressions on actual ARM64 Linux: transactional
detach, exhaustive allocation failure and copying/shared progressive output.
The complete current suite passes 252 cases with 29 platform skips and zero
failures. Its source-bound receipt closes the runtime follow-up described above;
the earlier Sintel benchmark and timing measurements retain their original scope.
