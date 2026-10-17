# Live AVC validation metadata reuse

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Successful append retains its native completed/pending picture walk. Prepare
clones admitted scalar metadata rather than decoding again; commit uses that
snapshot and performs the existing fallible IDR payload checks. Prefix eviction
rebases a validated suffix and retains pending/future preroll. Old snapshots own
independent arrays/admission and remain usable after coordinator mutation.
Indexes charge construction/reallocation bounds and retain only their owned
picture bytes at return. The 128 KiB headroom test proves prepare/commit no
longer require decoder workspace admission. Allocation/IO/cancellation failures
remain transactional. ArrayList planes are released before shrinking admission.

Final macOS ReleaseSafe gates pass 215 video tests (two skips), 14 live checks
and 40 CPU/Metal model checks. Actual Linux ARM64 passes 16 AVC tests, 14 live
checks and 36 model checks (four Metal skips). Pinned pretrained native CPU,
Metal and actual Linux live inference match complete-clip vectors exactly for
three real Sintel windows and 246 tokens per window, preserving pinned PyTorch
scores, rankings and top-1/top-3 sets. Checkpoint/oracle/source/binary hashes
bind these reports. Coordinator walk counts [3,5,6] and snapshot counts [1,2,3]
measure control validation only, not model decode/planning or GPU work.

The benchmark compares the prior 4dcfc473f4 source against current production
coordinator/index sources, using the same current benchmark/media/image/backend
code. Both temporary cores add one identical callback at indexMode entry; the
private Zig counter symbol is exported by the benchmark. Current native counters
must agree with instrumentation. No production library is instrumented. Exact
before/after patches are archived as text with hashes. The complete unchanged
video core comes from the pinned Git archive. This source substitution is a
benchmark-only facility, not a runtime alternate decoder.

Two serial before/after/after/before runs each collect one cold and ten warm
samples per small case. The 20 pooled warm samples show:

| Case | Windows | Walks before/after | Warm median before/after |
|---|---:|---:|---:|
| Real Sintel, 6 seconds, 128x72 | 3 | 9 / 6 | 218.6 / 132.1 ms |
| Generated interleaved PAFF, 10-bit 4:2:2 | 24 | 72 / 24 | 29.4 / 6.6 ms |

Field packets explicitly normalize clocks to packet ordinal with one-tick
duration; this measures assembly/control behavior, not original container-clock
or pretrained field parity. Each packet fixture owns a bounded 256 KiB parsed
index resized to one retained packet, plus its independent backing/metadata.
The 52-second, 480x320 real Sintel case remuxes the tracked long fixture with
FFmpeg copy mode and strips the trailing mfra; encoded video is not changed.
Sintel is © Blender Foundation, durian.blender.org, licensed under [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/). The parent was resized/re-encoded as described in [its provenance](../../../../../../zig/pkg/inference/scripts/embeddinggemma2/testdata/retrieval-long/README.md); this derivative changes only MP4 framing and removes the trailing index.
It delivers 27 windows with exact source interval/eligibility/ordered packet
clock+payload digests and 54 versus 27 validation walks. Single cold/warm
samples before are 41.2/40.5 seconds and after 20.4/24.4 seconds. These long
samples are diagnostic; concurrent host activity affects latency. The small
ABBA measurements are also shared-host diagnostics, not throughput guarantees.

Timing covers completed serial coordinator creation, validation, delivery,
commit and all owner destruction, including digest IO. File acquisition is
excluded. Admitted peaks are reservation accounting, not RSS: conservative new
metadata admission adds about 0.8 MiB to both small-case peaks. The allocator is
libc on both matched variants. Source, fixture and binary manifests identify the
experiments. Neither benchmark runs vision/projector/backbone execution.

Reproduce the production gates with Zig 0.17.0:

```sh
# zig/lib/video
zig build test-video test-live-windows -Doptimize=ReleaseSafe --summary all
zig build install-live-coordinator-benchmark -Doptimize=ReleaseSafe --prefix /tmp/live-control
/tmp/live-control/bin/live-coordinator-benchmark /path/to/suite.json /tmp/report.json 11
# zig/pkg/inference
zig build test-video-model build-embeddinggemma2-live -Doptimize=ReleaseSafe --prefix /tmp/live-model
```

For a matched historical comparison, extract `git archive 4dcfc473f4 zig/lib/video`
to an isolated directory, replace its coordinator/index with the archived
before/after text files respectively, and build with
`-Dlive-benchmark-video-source=/isolated/zig/lib/video/src/mod.zig`. The fixture
suite format is recorded in the JSON artifacts; replace absolute input paths
for the local checkout. The long-fixture manifest records the exact offline
FFmpeg remux command and fragment boundaries. Existing
`zig/pkg/inference/scripts/embeddinggemma2/video_live_qualification.py` verifies native/Metal against
the pinned oracle in the prior pretrained receipt. Linux uses the pinned,
offline/read-only ARM64 image with read-only fixture/model/binary mounts.

Append still reconstructs retained GOPs on each new unit, and field/dynamic
model planning still repeats logical walks. Incremental decoder state, broader
pretrained field/dynamic stream coverage, cross-window projection reuse,
full-pipeline sustained load and the full non-CUDA scope remain active.
