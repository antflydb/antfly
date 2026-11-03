# GIF image embedding latency on GB10, 2026-11-03

Investigates a reported production request: a roughly 1 MB animated GIF supplied
as a `data:image/gif` `image_url` returned a 768-dimensional vector in 1.9 seconds.
The original GIF, deployment version, backend, hardware, load, and warm/cold state
were unavailable. These measurements characterize the local implementation;
they do not reproduce that production request.

## Fixture and image semantics

The public fixture is Marvel's [Rotating earth (large).gif](https://commons.wikimedia.org/wiki/File:Rotating_earth_(large).gif),
licensed [CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/):

- 1,001,718 encoded bytes, 400 × 400 pixels, 44 frames.
- SHA-256: `b0acdc9a01f91f6123dbff38d4bee59d0d6e4b26071b5c83937b242b6b202378`.
- The controls are a PNG and a single-frame GIF made from its first frame.

The encoded-image path calls `pipelines/image.zig:decodeGif`, which calls
`lib/image/src/gif.zig:decodeFirstFrameAlloc`. The decoder requests one frame and
breaks immediately after rendering it, before processing any later pictures.
An animated GIF supplied as an image therefore represents its first frame.
Animation length does not multiply vision inference work. The chunking endpoint
has a separate GIF-animation path; that is outside this experiment.

An independent ReleaseFast build of the repository GIF decoder measured 100
first-frame decodes with file IO excluded: median **0.860 ms**, p95 **1.555 ms**.
One full-animation decode took **22.358 ms** for all 44 frames. The first-frame
measurement excludes RGB conversion, resize, inference, transport, and loading;
the embedding benchmark below includes image preparation and inference.

## Measurement method

Source base: `origin/main` at `87be75e64b`. Host: NVIDIA GB10, SM 12.1, driver
580.178.04, Linux aarch64. Checkpoint: `google/embeddinggemma-2` revision
`914f7f89142e33e77833254d9c9b90c3cef7303b`.

The existing synchronized image benchmark was extended to accept CUDA, retain
warmup samples, and pass the existing media timers through serial embedding
calls. `scripts/embeddinggemma2/gif_latency.py` runs each format in a fresh model
session with three warmups and twenty measured calls. It verifies 768 finite,
normalized components and compares each control vector with the animated GIF.

The timed boundary is encoded image bytes through a completed vector, including
CPU image decode, GPU resize, vision tower/projector, token packing, embedding
encoder, and final readback. File reads, session creation, tokenizer loading,
base64 decoding, JSON parsing, HTTP, network transfer, and HTTP/broker
scheduling are outside that boundary. Session and tokenizer setup are reported
separately. A first-use sample is the first call in that new session, with OS and
driver caches potentially already warm.

Stage timers measure host elapsed time. GPU stages enqueue asynchronously; CUDA
encoder time includes final readback and any earlier queued GPU work. The stage
values must not be described as isolated GPU kernel durations.

## Results

The animated GIF's warmed median was **102.75 ms**, versus **102.59 ms** for
its first-frame PNG and **102.41 ms** for its single-frame GIF. All three
returned **bit-identical final vectors** (maximum absolute difference zero).
The animation adds no meaningful inference cost in this comparison.

| Input | Encoded bytes | First inference in fresh session | Warm p50 | Warm p95 |
|---|---:|---:|---:|---:|
| Original animated GIF | 1,001,718 | 576.64 ms | 102.75 ms | 104.04 ms |
| First frame, PNG | 53,797 | 372.98 ms | 102.59 ms | 103.57 ms |
| First frame, GIF | 42,480 | 382.26 ms | 102.41 ms | 104.23 ms |

Each row has a separate fresh session, three warmups and twenty measured calls.
The animated-GIF warmups were 576.64, 106.45 and 101.66 ms. Its first call also
ran first in the campaign, so the first-use difference between formats is not
evidence of a GIF-specific cold penalty.

Offline session creation took 2.35–2.45 seconds, and tokenizer loading took
0.43–0.50 seconds; those costs are excluded from the inference columns. Reusing
a loaded model therefore matters considerably more than changing GIF encoding.
The production observation could include a different backend, initialization,
transport, queueing, or other serving overhead. The original 1.9-second report
cannot distinguish those explanations.

The 400 × 400 first frame is resized to 768 × 768, producing 2,304 vision patches,
256 projected tokens, and a 260-token encoder sequence. Image compute follows
this geometry rather than the animation's compressed byte count. Warm GIF CPU
preparation, including decode and RGB conversion, measured 1.25 ms median.
The resident route downloaded only 3,072 bytes in one readback per vector.

### CPU control

A small native CPU control used the same GIF, one warmup and three measured
calls. The measured calls took 3.964, 3.868 and 3.918 seconds (median 3.918 s).
Its first inference took 37.519 seconds; offline session creation and tokenizer
loading were 0.028 and 0.437 seconds. The output had 768 finite components and
unit norm. CPU and CUDA use different arithmetic identities, so this is a backend
latency comparison, without a cross-backend parity claim. This control reinforces
that backend and initialization state are essential context for the reported
1.9 seconds.

### GPU attribution and optional profiles

An Nsight Systems trace covered model setup and six embeddings (three warmup,
three measured). It recorded 604.06 ms of aggregate GPU kernel duration.
The largest entries were materialized attention softmax (27.4%), two BF16 GEMM
families (19.2% and 10.6%), another BF16 GEMM family (8.4%), and exact attention
(7.9%). This supports investigating attention/GEMM and startup rather than the
GIF codec. The host stage timer's roughly 100 ms in `encoder_ns` includes queued
vision work; it does not prove that the embedding encoder alone takes 100 ms.
cuDNN was unavailable on this host.

Existing opt-in profiles were measured independently with the same fixture and
control checks:

| Profile | Animated GIF warm p50 | Change from default | Cosine to default | Maximum component difference |
|---|---:|---:|---:|---:|
| Default | 102.75 ms | — | 1.0 | 0 |
| `NATIVE_VISION=1` | 96.52 ms | −6.1% | 0.99998448 | 0.00080591 |
| `EXTENDED_FLASH=1` | 98.48 ms | −4.2% | 0.99999170 | 0.00056744 |

The option names above use the prefix
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_`. Route counters confirmed native vision
execution for the first profile and extended attention with no scalar attention
fallbacks for the second. Each profile's PNG/GIF controls still returned identical
vectors within that profile. These profiles alter arithmetic and embedding
identity; this single-image experiment does not qualify retrieval compatibility
or justify enabling them for an existing index.

The recorded measurements, fixture hashes, binary/checkpoint identities, raw
latency samples, vector hashes, and top GPU kernels are in
[the evidence receipt](2026-11-03-gif-latency.json). Full vectors, logs and the
Nsight trace remain in `zig/pkg/inference/.benchmark-results/gif/` in this worktree.

## Production follow-up

Measure repeated requests against the same loaded model, with the response's
actual backend/model identity and input token count. Separate HTTP end-to-end
latency from model execution, and compare the GIF with its exact first-frame PNG.
If warmed execution is close to this local result while HTTP still takes 1.9 s,
inspect request upload, admission/broker waits, and model asset loading. If model
execution itself takes 1.9 s, profile that backend and device. The local results
do not support changing the GIF decoder to address that discrepancy.

## Reproduction

Run from `zig/pkg/inference` in a branch worktree. Keep build output and caches
inside that worktree. `MODEL` points to an existing verified checkpoint bundle.

```bash
zig build build-gemma4-image-bench -Doptimize=ReleaseFast -Dcuda=true
mkdir -p .benchmark-results/gif
curl -fL 'https://upload.wikimedia.org/wikipedia/commons/2/2c/Rotating_earth_%28large%29.gif' \
  -o .benchmark-results/gif/earth.gif
printf '%s  %s\n' \
  b0acdc9a01f91f6123dbff38d4bee59d0d6e4b26071b5c83937b242b6b202378 \
  .benchmark-results/gif/earth.gif | sha256sum --check
ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_STATS=1 \
  uv run scripts/embeddinggemma2/gif_latency.py \
  --binary zig-out/bin/antfly-gemma4-image-bench \
  --model "$MODEL" --gif .benchmark-results/gif/earth.gif \
  --output-dir .benchmark-results/gif/default \
  --warmups 3 --iterations 20
```

Use the default fatbin artifact. Portable CUDA 13.2 PTX is incompatible with this
host's R580 driver, as documented in `zig/pkg/inference/CUDA.md`.
