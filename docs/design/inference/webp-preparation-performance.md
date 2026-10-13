# WebP preparation performance

Dated decisions and rejected trials: [media throughput history](history/gemma4/media-throughput-2026-10.md#jpeg-and-webp-preparation).

The shared CPU WebP decoder now borrows lossless prefix tables, uses an eight-bit
LSB-first lookup for short codes, and uses canonical length ranges for longer
codes and short input tails. The previous implementation copied a large table
per symbol and scanned the entire alphabet at every code length. CPU sampling
on the checked-in lossless fixture confirmed symbol lookup on the hot path.
Bit fields now read bounded byte fragments instead of extracting every bit
separately. Single-symbol trees still consume zero bits.

This accelerates VP8L images and compressed lossless alpha planes attached to
lossy VP8 images. Ordinary VP8 decoding, image transforms, color conversion,
alpha composition, cancellation checkpoints and model preprocessing semantics
remain unchanged. Prefix tables stay local to their owning group. The group-count
limit remains 1000; no decoded image cache or new runtime dependency is introduced.
The canonical representation replaces the old per-symbol code/length/symbol
entries with two-byte symbols, length metadata and a fixed 1024-byte short lookup.

## Measurements

On an Apple M4 Max with 36 GiB unified memory, the paired baseline/candidate then
candidate/baseline campaign runs all 12 checked-in valid WebP fixtures at one and
four workers. Each process uses two warmups and 20 measured waves. All 96 cases
retain exact old/new RGBA hashes and dimensions. Concurrent development activity
limits these observations; they are not an idle-machine qualification.

| Fixture | Workers/images | Baseline p50 | Candidate p50 | Paired speedup |
| --- | --- | --- | --- | --- |
| Upstream lossless, 150×100 | 1 | 25.04–28.21 ms | 0.56–0.61 ms | 41.2–50.2× |
| Upstream lossless, 150×100 | 4 | 33.83–36.61 ms | 1.19–1.25 ms | 27.0–30.7× |
| Upstream lossy + compressed alpha, 400×301 | 1 | 11.57–12.25 ms | 2.76–3.24 ms | 3.8–4.2× |
| Upstream lossy + compressed alpha, 400×301 | 4 | 16.86–17.96 ms | 3.37–3.78 ms | 4.7–5.0× |

Four-worker waves repeat the same encoded fixture, with independent outputs.
They measure decoder concurrency rather than a diverse image workload. Input
reading, SHA-256 verification and output release are outside the timed wave;
thread startup/join, allocations and decode-to-RGBA are included. Tiny fixture
measurements are dominated by worker startup and scheduling. Other lossy timings
vary with machine activity; no general VP8 speedup is claimed.

A separate leading-zero-count normalization trial for the VP8 boolean decoder
passed exact output checks but regressed some four-worker cases versus the
lossless-only candidate in both orders. That change is not retained. Its raw
comparison remains in `.benchmark-results/webp-lossy-normalization-2026-10-11/`.

The [results report](webp-preparation-performance.json) records binary/input/source
hashes, all paired observations, validation and limits. Raw samples, saved
binaries, profiler output and the campaign script are in
`.benchmark-results/webp-preparation-retained-2026-10-11/`.

## Reproduce and validate

Build with the repository's Zig 0.17 toolchain:

```sh
cd zig
zig build lib-image-webp-bench
zig build lib-image-webp-test -Doptimize=ReleaseSafe
zig build lib-image-test -Doptimize=ReleaseSafe
zig-out/bin/lib-image-webp-bench \
  testdata/image/webp/upstream/go_image/blue-purple-pink.lossless.webp 20 4 2
```

Benchmark arguments are input path, measured waves, concurrent images/workers
(1–4), and warmups. The executable emits one JSON report to stderr. Encoded input
is capped at 64 MiB, output at 16 million pixels/64 MiB per image, and concurrency
at four. These output/admission bounds are not total working-set or RSS limits.
Retain an old binary, run both variant orders with identical arguments, and check
input/pixel hashes and dimensions before comparing timings.

All 91 direct WebP tests pass in ReleaseSafe. The new prefix test covers all
65,536 sixteen-bit inputs against an independent linear decoder, including
15-bit codes with permuted symbols. Boundary tests cover short tails, truncated
long codes and zero-bit single-symbol trees. Bit-reader tests compare scalar
results at 49 offsets and every count from zero through 31. Direct WebP tests now
participate in the normal shared image suite; importing the image module alone
did not previously discover the codec's internal tests.

The CPU decode change applies to callers of the shared image library. Images
still decode on CPU before model-specific preparation/upload. These measurements
establish codec throughput gains on the tested inputs, not GPU-resident WebP
decoding or end-to-end CLIP/Gemma4/Qwen embedding throughput improvements.
