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

## Full-trailer and portable SIMD expansion

The Sintel receipts use the pinned 52.2-second original 854×480, 24fps trailer
(source/attribution in [fixture README](../README.md)), Zig 0.17.0 ReleaseSafe,
LLVM, macOS ARM64, and two overlapping sampled windows yielding 38 unique prepared
pictures. Dependency decoding covers intervening pictures; the reported frame rate
counts selected prepared pictures, not every decoded dependency. Preparation is
384×384 with BT.709. Each route has one cold and only two warm samples; p50/p95
are descriptive observations, not stable capacity estimates. Source container indexing
is excluded from setup, and no tensor GPU readback occurs in the Metal benchmark.

```sh
zig build bench-video -Doptimize=ReleaseSafe -Dvideo-use-llvm=true \
  -Dbenchmark-input=/absolute/path/sintel_trailer-480p.mp4 \
  -Dbenchmark-size=384 -Dbenchmark-iterations=3
# Add -Dbenchmark-backend=cpu for the pure Zig software/CPU route.
```

The Metal receipt includes completed command execution time, retained coefficient
reuse, process peak RSS and device aggregate allocation. VideoToolbox internal
allocation is unobservable (`decode_live_peak_bytes=0`); neither process RSS nor
aggregate Metal allocation is a measured per-request peak GPU allocation. CPU
receipts report the decoder allocator's actual high-water usage separately.

`macos-arm64-packing-llvm-2026-10-08.jsonl` measures 32×1920×1080 4:2:2 u16 plane
packing (132,710,400 output bytes) over ten samples. Portable Zig vector packing
measures roughly 6.5–8.6ms versus 17.8–20.8ms scalar. Explicit vectors are enabled
for LLVM, with scalar fallback for other Zig backends. This microbenchmark measures
packing alone, not total decode acceleration. Run `zig run -OReleaseSafe -fllvm
src/pixels_benchmark.zig` and qualify LLVM branches with
`zig build test-video -Dvideo-test-llvm=true -Dvideo-test-filter=SIMD`.

`macos-arm64-transform-selfhost-2026-10-08.jsonl` compares one million 4×4 inverse
blocks. The vector candidate is slower (~13.5ms versus ~8.2ms scalar), so it remains
disabled in decoding. Run `zig run -OReleaseSafe src/transform_benchmark.zig`.
Measurements apply to this compiler/backend and host; repeat on deployment hardware.

The full-trailer receipts separate selected `frames` from actual
`dependency_packets`: CPU reconstructs 1,251 packets for 38 selected pictures,
while verified-IDR hardware planning submits 933. Warm completion is about 28.1–28.3s
for software/CPU and 326–327ms for VideoToolbox/Metal on this host. This comparison
includes different dependency plans as well as different decoders/preparers. CPU
allocator high water is 12,140,140 bytes, while process RSS includes retained 384×384
patch tensors (~91MB total). These observations identify dependency-span reduction
and prediction/entropy work as future software performance targets; SIMD packing
alone does not make software match hardware throughput.
