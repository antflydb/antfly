# CPU qualification on GB10, 2026-11-03

Native F32 EmbeddingGemma 2 text-query and image serving passed the following
reference-host checks. These measurements establish the stated workload and
resource envelope; they do not qualify x86, hard pod CPU/memory limits,
FP16/BF16 CPU variants, audio/video, mixed text/image latency or automatic routing.

## Host and configuration

- Linux aarch64 NVIDIA GB10, 20 ARM cores, affinity 0–19, approximately 128 GB RAM.
- CPU-only production HTTP server: `-Doptimize=ReleaseFast -Dstrip=true -Dcuda=false`,
  `ANTFLY_INFERENCE_REQUIRED_BACKEND=native`, one preloaded model.
- Checkpoint: `google/embeddinggemma-2` revision
  `914f7f89142e33e77833254d9c9b90c3cef7303b`.
- Independent reference: torch 2.10.0+cpu, Transformers commit
  `92cd495f2720c064bc78eb2d93e28704c5bce51f`; existing `reference.py` checks
  upstream source and checkpoint hashes.
- Text budgets: 6 GiB host, 1 GiB backend, 7 GiB combined, 2 GiB scratch,
  16 request slots and 64 work units. Image budgets: 12/1/13/6 GiB respectively,
  4 request slots and 64 work units. Both use the detected host memory envelope
  in an unlimited shared cgroup, not a hard container limit.

## Results and acceptance limits

| Check | Text queries | Images |
| --- | --- | --- |
| Independent numerical cases | 346 | 160 |
| Maximum component error | 3.28e-7 | 5.18e-5, on a JPEG |
| Successful load responses | 8,342 | 354 |
| Ten-minute soak | 4 clients, 11.53 queries/sec | 2 clients, 0.44 images/sec |
| Soak p95 / p99 | 386 / 401 ms | 4.75 / 4.82 s |
| Peak worker RSS | about 1.45 GiB | about 2.25 GiB |

All load responses passed vector/identity checks with zero errors and released
request admission and scratch. Image profiles also passed at one and four
clients: 4.32/6.34 s p95 and 0.26/0.67 images/sec respectively. The serving worker
remained unchanged through each load/recovery campaign and did not load CUDA.
The first image after text-model preload took 20.38 s; prewarm an image worker
before traffic that requires warm latency. Filesystem caches may already be warm.

Text component tolerance was 1e-5. Image tolerance was 1e-4 with cosine ≥0.99999
for lossless inputs, and 1e-3 with cosine ≥0.9999 for JPEG; unit-norm error was
at most 1e-4. Dimension projection/restoration passed at 128/256/512/768.
Image inputs covered 8-bit JPEG/PNG, grayscale/alpha/palette PNG, first-frame GIF,
lossless WebP, uncompressed BMP and ordered text/image and two-image groups.
The steady image load used one image per request. Exact 4 MiB data URLs,
2048×2048 RGBA, four simultaneous maximum-byte inputs and image-after-text
correctness passed separate boundary checks.

## Retrieval and recovery

- SciFact: all 300 test queries against 5,183 fixed CUDA document vectors.
  CPU/CUDA nDCG@10 was 0.85331/0.85333; recall@10 was 0.93467/0.93533.
  Maximum allowed losses were 0.005 and 0.01 respectively.
- Flickr8k: 128 of 1,000 test images selected by original JPEG SHA256, with all
  640 associated captions. This is a deterministic subset, not a full-dataset
  result. CPU matched upstream F32 retrieval metrics. With fixed CUDA captions,
  CPU/CUDA image-index text-to-image recall@1 was 94.69%/95.00%, and nDCG@10
  was 0.97940/0.98055. Recall@5 was 1.0 in both directions. Recall@1/5 loss had
  to stay within 0.01 and nDCG@10 loss within 0.005.
- Flat/group/data-URL forms, batch order, per-item corrupt-input errors, identity
  conflicts, deadlines, TCP reset, legal half-close, concurrent timeout isolation
  and correct retries passed. Overload bursts admitted 16 text / 4 image requests
  and rejected excess work with retryable HTTP 503 and Retry-After. All 46 text
  regression cases passed after image serving.

CPU and CUDA recipe identities remain distinct; retrieval compatibility does
not override an index identity pin. Qualification fixes preserve HTTP 504/408
for execution aborts and correct VP8L code-length repeats. The retained Zig
regressions include a 70-byte real-encoder WebP fixture and malformed bounds.

## GIF observation and evidence

The original 1,001,718-byte Earth GIF represents its first frame. Its CPU data-URL
embedding matched the PNG control and independent F32 output (about 3.1 s warm
in the separate boundary run). The earlier direct CUDA benchmark took about
103 ms warm and produced identical GIF/PNG vectors; first-frame decode took
0.86 ms. The reported production 1.9 s request was not reproduced.

Tested text/image executable SHA256 values were respectively
`161709f7cf06388986bc2c3c9fc8f06a253add25d3088b51e7ec4e1d7bc3433c` and
`9a2e5fea22063a32f39417fb6745275d16ebaa6f46de4a3d94a2077ace159622`.
Raw vectors, timings and receipts remain in ignored `.benchmark-results/`
artifacts. Existing `scripts/embeddinggemma2/reference.py`, `compare.py` and
`qualify.py` provide reusable reference, comparison and HTTP-contract checks;
host-specific campaign scaffolding and generated result dumps are not part of
the maintained source tree.
