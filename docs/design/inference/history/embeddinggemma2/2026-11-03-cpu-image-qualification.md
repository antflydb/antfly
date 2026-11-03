# Native F32 CPU image qualification on GB10

**Passed:** native F32 CPU image serving on this Linux aarch64 NVIDIA GB10,
within the tested image sizes, concurrency and application budgets below.
The [machine-readable receipt](2026-11-03-cpu-image-qualification.json) records
the exact executable, acceptance limits, source hashes and raw evidence.
CPU and GPU services retain distinct recipe identities.

## Scope and acceptance policy

The acceptance limits were declared before their corresponding measurements in
`scripts/embeddinggemma2/testdata/cpu-image/policy.json`. They cover native F32
CPU inference from the pinned upstream BF16 checkpoint, using the existing
stripped ReleaseFast CPU-only production executable. The server invocation and
binary hash are captured before the first image request.

The image service has 4 request slots, 64 work units, and application memory
budgets of 12 GiB host, 1 GiB backend, 13 GiB combined and 6 GiB scratch. The
host has 20 ARM cores and approximately 128 GB RAM, with affinity to cores 0–19.
It uses the detected host envelope in an unlimited shared cgroup. This does not
qualify a hard Kubernetes memory limit or a smaller CPU quota.

- Natural-image holdout: select 128 of the 1,000 Flickr8k test images by sorting
  SHA256 of original JPEG bytes; use all five associated captions per image.
  Selection is independent of embedding output. Preserve the publisher archive
  SHA256, dataset revision and individual image hashes.
- Numerical checks: 768 finite normalized components, maximum component error
  at most 1e-4 and cosine at least 0.99999 for lossless inputs; JPEG allows at
  most 1e-3 and cosine at least 0.9999 to accommodate codec implementation
  differences. Numerical comparisons use a separately executed, pinned upstream
  PyTorch F32 model and processor, with checkpoint/source hash verification.
- Retrieval: exact F64 cosine rankings in both text-to-image and image-to-text
  directions. Hold caption vectors fixed while comparing the CPU image index
  with the upstream F32 index; repeat with CUDA caption queries and the CUDA
  BF16 image index. Relative losses must be at most 0.01 in recall@1/5 and
  0.005 in nDCG@10. Recall is the fraction of queries with at least one relevant
  item among the first K results. Image-to-text nDCG uses all five captions.
- Serving: 60-second profiles at 1/2/4 clients and a 600-second soak at two
  clients. Zero request/vector errors, exact completion accounting, stable
  worker/supervisor, p95 at most 8 seconds per concurrent client, worker lifetime
  RSS below 10 GiB, and per-profile RSS growth below 256 MiB. The first image
  request after text-model preload must finish within 90 seconds.
- Contracts: flat media, ordered groups and data image URLs; batching/order;
  dimensions 128/256/512/768 and restoration; corrupt-image errors and per-item
  results; wrong identity rejection; GIF first-frame equivalence.
- Recovery: deadline during image execution, TCP reset, legal half-close,
  cleanup and correct retries. An 8-client burst against 4 request slots must
  reject excess work with retryable HTTP 503 and Retry-After, without replacing
  the worker or retaining admission/scratch resources.

The 156 numerical cases include the 128 original held-out JPEGs, eight decoded
PNG controls, synthetic RGB images from 1×1 to 2048×1536, portrait/landscape and
extreme aspect ratios, grayscale/RGBA/paletted PNG, baseline/progressive JPEG,
lossless WebP, BMP, animated GIF and its first-frame PNG, plus ordered text/image
and two-image groups. The steady-state load uses one image per request. Ordered
compositions are checked for numerical and serving correctness separately.

The source-image test envelope is at most 2048 pixels per side and 4 MiB encoded.
This is a tested qualification scope, not a new server validation setting. The
corpus is a deterministic test subset, not a full Flickr8k leaderboard result
or evidence for arbitrary application relevance. Audio/video, reduced-precision
CPU variants, x86, hard pod quotas, animated temporal embedding and a low-latency
text SLA sharing an image-loaded service remain outside this campaign.

## Results

All 156 primary image cases passed independent F32 reference checks, including
the previously rejected WebP. Four additional source-boundary cases passed,
bringing the independent image reference count to **160**. Maximum component
error was **5.1722e-5**, on a natural JPEG. The full serving contracts passed:
flat/group/data-URL forms, dimensions and restoration, batch order, per-item
errors, corrupt input, identity conflicts, and GIF first-frame equivalence.

The fixed-query retrieval results cover the same deterministic 128-image,
640-caption subset in every comparison:

| Query vectors / image index | Text→image R@1 | Text→image nDCG@10 | Image→text R@1 | Image→text nDCG@10 |
| --- | ---: | ---: | ---: | ---: |
| Upstream F32 / upstream F32 | 0.948438 | 0.979975 | 0.992188 | 0.981501 |
| Upstream F32 / CPU | 0.948438 | 0.979975 | 0.992188 | 0.981501 |
| CUDA / CUDA | 0.950000 | 0.980551 | 0.992188 | 0.981945 |
| CUDA / CPU | 0.946875 | 0.979398 | 0.992188 | 0.981437 |

Recall@5 was 1.0 in both directions for all comparisons. CPU matched upstream
F32 retrieval metrics exactly. With CUDA queries, replacing the CUDA image
index by CPU images reduced text-to-image recall@1 by 0.003125 and nDCG@10
by 0.001153; both passed the declared compatibility limits. This is semantic
compatibility on this subset and does not override CPU/GPU identity pins.

| Clients | Duration | Successful images | Images/sec | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 60 s | 16 | 0.258 | 3.81 s | 4.32 s | 4.32 s |
| 2 | 60 s | 28 | 0.445 | 4.50 s | 4.73 s | 4.73 s |
| 4 | 60 s | 43 | 0.665 | 5.90 s | 6.34 s | 6.42 s |
| 2 | 600 s | 267 | 0.443 | 4.50 s | 4.75 s | 4.82 s |

All **354** load responses were correct, with zero errors. The ten-minute soak
completed 267 image requests at two clients. Lifetime worker RSS peaked at
**2.25 GiB**; RSS decreased when moving from the four-client profile to the
two-client soak. Worker/supervisor identities remained stable throughout the
serving phase, and the CPU worker did not map a CUDA library.

The first image after text-model preload took **20.38 seconds** in the serving
campaign. This is a fresh process with potentially warm filesystem caches.
Preload alone does not establish warm-image latency: issue a representative
image warmup before routing latency-sensitive image traffic to a new worker.

All four slots were occupied during an eight-client overload burst: four
requests completed correctly and four received retryable HTTP 503 with
Retry-After. Deadlines during computation, TCP reset, legal half-close, flat
image deadlines and correct retries passed before and after the soak. A timed
out image did not cancel an admitted peer. Admission, work units and scratch
returned to zero after cleanup. All 46 independent text-regression cases also
passed after the image campaign; no mixed text/image latency SLO is claimed.

The numerical collection and serving phases used separate fresh workers of
the identical executable. Reuse requires complete successful vectors and
matching binary SHA256, suite, model identity, CPU affinity and configuration.

## Source-boundary and original-GIF checks

The original pinned 1,001,718-byte animated Earth GIF and its first-frame PNG
passed independent F32 checks through data image URLs and produced equivalent
CPU vectors. Warm requests took 3.09 s and 3.11 s in the separate boundary run.
These timings are distinct from the concurrent serving profiles above.

An exact 4 MiB PNG (1024×1024 RGB noise plus a valid ancillary chunk) and a
2048×2048 RGBA PNG matched independent references to within 2.3e-7 per
component. Warm requests took about 3.2 s. Four simultaneous 4 MiB data URLs
all completed correctly, with latencies from 4.96 to 9.47 s and released
resources. An image request after an intervening text query remained correct
and took 4.07 s. Source bounds are a tested scope, not new server rejection
settings for larger inputs.

## Decoder issue found during qualification

A valid 70-byte lossless WebP generated by Pillow was rejected as `INVALID_IMAGE`.
The VP8L normal Huffman decoder treated `max_symbol` as an expanded alphabet
position. The limit counts encoded code-length symbols, so a run-length symbol
can legitimately expand past that count while remaining within the alphabet.
The decoder now counts encoded symbols separately and retains strict expanded
alphabet bounds. It also uses the specified initial previous code length of 8
for an initial repeat code.

The change follows the [WebP lossless specification, section 6.2.1](https://developers.google.com/speed/webp/docs/webp_lossless_bitstream_specification)
and the [upstream libwebp decoder](https://github.com/webmproject/libwebp/blob/main/src/dec/vp8l_dec.c).
A generated real-encoder fixture checks every decoded RGBA pixel. Focused tests
cover limited repeat expansion, implicit trailing zeros, initial repeats and
malformed alphabet overflow. The complete 95-test WebP suite passes. The old
hand-written repeat-code test encoder and its obsolete initial-repeat rejection
expectation were corrected to match the specification.

The final qualification uses a rebuilt production executable and a fresh
collection of every image case; the earlier rejecting executable is retained
only as diagnostic history.

## Reproduction and evidence

Run from `zig/pkg/inference`. Build the CPU production server with
`zig build http-server -Doptimize=ReleaseFast -Dstrip=true -Dcuda=false`. Keep
build caches in the worktree. The pinned Python reference environment uses
Torch 2.10.0+cpu and Transformers commit
`92cd495f2720c064bc78eb2d93e28704c5bce51f`; `reference.py` verifies the upstream
implementation and every checkpoint artifact.

`cpu_image_reference.py prepare` verifies the immutable Flickr8k test Parquet
in the shared conformance fixture cache, writes deterministic image/codec cases
and caption suites, and preserves asset SHA256 values. Its `oracle` operation
runs the upstream F32 image and caption encoders independently.

`cpu_image_qualification.py collect` sends each case through the production
CPU server and preserves complete output vectors. Its `evaluate` operation
requires complete matching successful reference/candidate receipts and checks
numerical and cross-modal retrieval gates. `contracts`, `load`, `recovery` and
`overload` exercise the corresponding live serving gates. Every operation is
fail-closed and records the running executable and process identities.

Raw measurements, vectors and manifests are retained under
`.benchmark-results/cpu-image-qualification`. Corpus/checkpoint data is not
committed. The dataset is the pinned [Flickr8k test split](https://huggingface.co/datasets/jxie/flickr8k)
at revision `56f58c967835f7c508d684f36bd7897cca9d7634`; its 136,773,352-byte
Parquet has publisher SHA256
`f3bc4c3548ec1d9bb3c445c871cfcd3aca6003e3d4786e404fc9356b5215410d`.

Run the owned-server campaign after preparing the suite and reference:

```sh
python scripts/embeddinggemma2/cpu_image_campaign.py \
  --binary zig-out/bin/antfly-inference-http-server --model-dir "$MODEL" \
  --results .benchmark-results/cpu-image-qualification \
  --policy scripts/embeddinggemma2/testdata/cpu-image/policy.json \
  --text-oracle .benchmark-results/cpu-qualification/reference/query-oracle.json \
  --report .benchmark-results/cpu-image-qualification/final.json
```

The campaign requires matching `reference.json`, `cuda-captions.json` and
`cuda-images.json` in its results directory. Caption vectors use the managed
CUDA query benchmark; image vectors use the encoded-image CUDA benchmark.
`--reuse-collection` reuses a completed numerical collection only after the
identity/configuration/hash checks above; it reruns serving gates in a fresh
owned process. `cpu_image_bounds_qualification.py` separately generates and
checks the original GIF, its PNG control, exact byte/pixel bounds, four-client
maximum-byte traffic and image-after-text behavior. Its `--gif` input must match
the original investigation's pinned SHA256.

Validation passed: the CPU ReleaseFast production build, 95 complete WebP codec
tests, 21 Python evidence tests, Python lint/format, Zig formatting, image
contracts/load/recovery/overload, 160 image reference cases, and the 46-case text
regression. The qualification remains limited to this ARM host and the stated
workload. x86 and hard pod CPU/memory limits require separate execution.
