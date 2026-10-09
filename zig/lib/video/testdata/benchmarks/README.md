# Completed video timing receipts

These 2026-10-08 measurements use Zig 0.17.0 in ReleaseSafe. macOS ARM64
(27.0.1) runs VideoToolbox/Metal natively. Linux ARM64 runs statically linked
musl executables in a local Alpine 3.22 Docker VM on the same host. Linux results
qualify actual execution but are not bare-metal Linux performance estimates.

Each JSONL receipt contains one cold iteration, twenty completed warm iterations
and a nearest-rank median/p95 summary. Completion includes GPU fencing and job
output destruction. GPU execution time sums command intervals and is separate
from completed wall time; it does not represent end-to-end CPU decode time.
Setup records backend/preparer creation, excluding container indexing.

`decode_live_peak_bytes` tracks software decoder allocations. It is zero when
VideoToolbox's internal allocations cannot be observed. Process peak RSS is the
process-wide high-water mark; Metal allocated bytes cover the device, rather
than a single job. Both may include unrelated runtime allocations. Logical
upload counts are not measurements of physical bus traffic. No output readback
is used by the benchmark.

The default MJPEG fixture is eight 64×48 pictures. External H.264 inputs are
`decode-hardware.mp4` for macOS and `h264-intra.mp4` for Linux. These tiny synthetic
clips measure setup, reuse and instrumentation; they are not production video
throughput claims. Warm MJPEG Metal iterations upload zero resize-coefficient
bytes after the cold iteration populates the geometry cache.

Run from `zig/lib/video`:

```sh
zig build bench-video -Doptimize=ReleaseSafe
zig build bench-video -Doptimize=ReleaseSafe -Dbenchmark-input=/absolute/path/input.mp4
zig build check-video check-video-benchmark -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe
```

Linux receipts execute the cross-compiled `video-benchmark` binary in
`docker run --rm -v CACHE:/artifacts:ro -v TESTDATA:/fixtures:ro alpine:3.22`
with no argument for MJPEG, or `/fixtures/h264-intra.mp4` for H.264.

Fixture SHA-256:

- `mjpeg.mov`: `333c49a606751e093df1e379ad3046bdf85f0eb05d22ac1888ab199f34eb145f`
- `decode-hardware.mp4`: `56afd820084fc267cf15cc254d16c4b8fb23bd180e87e8e6f96e360e76c57a66`
- `h264-intra.mp4`: `a92193773dd430c70841649c2af90d17c083e8cfcef0c0a3588446161a929762`

Repeat measurements on deployment hardware and representative resolution, codec
and concurrency before sizing shared admission budgets.

The additional `linux-arm64-h264-high-2026-10-08.jsonl` receipt measures the expanded
pure Zig decoder against `h264-high-pyramid.mp4` (128×96, sixteen pictures), including
CABAC, B-picture prediction, weighted references, in-loop filtering and batch
preparation. Earlier H.264 receipts predate the expanded decoder and describe its
initial intra-only implementation. The newer receipt reports completed CPU work
and allocation peaks; its tiny synthetic input and Docker VM remain unsuitable for
production capacity estimates.

High-profile fixture SHA-256: `ff3779dc9357543752834e69f4bc3122efbc16f2a89a56efe2518423f88d2e99`.
