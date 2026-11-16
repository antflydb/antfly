# EmbeddingGemma2 automatic CUDA defaults

EmbeddingGemma2 selects the measured encoder profile without optimization
environment variables on a **NVIDIA L4** (SM89), when cuBLASLt and a bundled
BF16 WMMA image are available. The hardware fence uses the actual selected
device, rather than treating every SM89 GPU as qualified. Portable PTX,
unqualified hardware, and unsupported stage shapes keep the previous routes.

Automatic stages are singleton shared encoders of 1–8192 tokens. The profile
enables extended attention, packed projections, graph replay and fused backbone
operations. When a short tail would prevent tiled padding under graph replay's
12.5% work limit, automatic replay stays off and the existing 64-row padding
remains active. Explicit `GRAPH_REPLAY=1` retains its earlier behavior. This
shape rule avoids a measured image-only latency regression at 264/270 tokens.
The global packed-GEMM attention operator retains its narrower
512–8192-token shape gate, admission checks and library fallbacks. A retained
allocation generation invalidates graphs if a retired scratch address is reused.

Vision-tower profile flags, audio-tower experiments, generic packed audio
projections, projection autotuning, native/fused vision-attention experiments, spatial pooling, and
image/media batching remain explicit. Image and audio requests can use the automatic shared encoder while their
towers retain legacy behavior. Existing physical multi-row encoder calls
keep the previous execution path unless explicit overrides enable a profile;
automatic defaults do not broaden batching qualification.

## Overrides and identity

Set `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUTOMATIC=0` to turn off automatic
promotion. Explicit route settings still apply. With no other route flags this
restores the prior default recipe and embedding identity.

Each promoted flag accepts `0` to disable that automatic route, or `1` to retain
its existing explicit behavior:

- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_EXTENDED_FLASH`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_PACKED_PROJECTIONS`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GRAPH_REPLAY`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_BACKBONE`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GLOBAL_GEMM`

The immutable session recipe records potential automatic routes separately from
explicit arithmetic flags. Stage resolution uses device/shape information
without mutating the recipe observed by queues. GPU discovery is unnecessary
to hash the declared recipe, so planning and loaded-model identities agree.
The automatic dispatch rule and its override mask have a distinct CUDA identity
from both legacy and explicit profiles. CPU and Metal identities are unchanged.
Existing pinned CUDA embeddings must use a compatible recipe or be regenerated;
do not mix vectors across these identities.

## Validation and reproduction

The policy tests cover L4 boundaries, larger batches, A100/RTX4090/other compute
capabilities, missing library/artifact support, individual route disables,
explicit enables, and the master switch. Identity tests distinguish automatic,
legacy, explicit, and partially disabled recipes. The full CUDA inference
executable is type-checked, including all resolved-policy consumers.

`build-embeddinggemma2-media-bench` runs the production pipeline from text or
encoded image/audio files through synchronized vector readback and records its
immutable policy. The promotion driver rotates three fresh processes per round:
legacy (`AUTOMATIC=0`), automatic (no route flags), and the explicit five-flag
profile. It checks exact token counts, finite normalized vectors, global-route
counters, process-tree PSS/NVML memory and immutable input/binary hashes.
Automatic vectors must match the explicit reference at cosine ≥0.9999 and
maximum absolute difference ≤0.003. Every case must also stay within 5% of
legacy latency; these criteria were frozen before collecting the narrowed
profile. This promotion regression check does not replace the independent [BF16 media qualification limits](MEDIA.md).

```sh
python zig/lib/pjrt/tools/media_inputs.py --model "$EMBEDDING_MODEL_DIR" \
  --out evidence/automatic-promotion/fixtures --include-text
python zig/pkg/inference/scripts/embeddinggemma2/profiling/compare_automatic_model.py \
  --binary zig-out/bin/antfly-embeddinggemma2-media-bench \
  --model "$EMBEDDING_MODEL_DIR" --suite evidence/automatic-promotion/fixtures/suite.json \
  --out evidence/automatic-promotion/final-run --rounds 3 --iterations 8
```

The rejected encoder+vision campaign is retained under
`evidence/automatic-promotion/run/`, with its measured binary saved as
`vision-profile-bench`. The first encoder-only candidate uses `encoder-run/`;
it exposed the same short-tail padding regression. Final shape-aware results use `final-run/`.

The retained suite combines four exact-length repeated-word text cases with six
synthetic media fixtures. `media_inputs.py --include-text` reproduces all ten
rendered token sequences; the checkpoint and tokenizer are pinned by hash in the
campaign provenance. Text timing here includes rendering/tokenization; it is
not directly interchangeable with the older prepared-ID timings in `GLOBAL_ATTENTION.md`.
Raw vectors, logs and sampled memory remain in `evidence/automatic-promotion/`.

## Final L4 promotion results

Three rotating fresh-process rounds, three warmups and eight measured calls per
workload/arm/round. Each value pools 24 samples. All ten workloads pass the
frozen latency and numerical gates. The legacy arm disables automatic promotion;
the explicit arm forces all five flags, including its earlier graph padding and
vision-tower profile. Automatic mode sets no optimization flags.

| Workload | Legacy ms | Automatic ms | Explicit ms | Speedup over legacy |
|---|---:|---:|---:|---:|
| Text, 128 tokens | 7.22 | 5.28 | 5.29 | 1.37× |
| Text, 512 tokens | 16.09 | 13.86 | 13.86 | 1.16× |
| Text, 2,048 tokens | 225.08 | 60.72 | 60.53 | 3.71× |
| Text, 8,192 tokens | 2899.45 | 301.50 | 300.95 | 9.62× |
| PNG | 98.28 | 95.13 | 110.98 | 1.03× |
| JPEG | 125.90 | 121.75 | 140.49 | 1.03× |
| Caption + JPEG | 210.81 | 146.56 | 148.11 | 1.44× |
| Audio, 1s | 10.10 | 9.45 | 9.31 | 1.07× |
| Audio, 12s | 25.27 | 22.21 | 22.17 | 1.14× |
| Audio, 30s | 80.39 | 51.94 | 52.68 | 1.55× |

Median per-process campaign peak GPU memory: **2.885 GiB legacy,
3.354 GiB automatic, 3.740 GiB explicit**. Median
peak process-tree footprint (PSS plus NVML): 4.071,
4.556, and 4.940 GiB, respectively. These peaks
include model loading and all ten cases in one process. Automatic mode retains
about 0.469 GiB more GPU memory than legacy for
packed weights and reusable execution storage; it is a latency/memory tradeoff,
with existing admission accounting and fallbacks preserved.

All 720 measured vectors are finite, normalized and 768-dimensional. All 240
automatic vectors pass comparison to the explicit reference: minimum cosine
0.999992640, maximum
component error 0.000591271.
This does not resolve the separately documented strict CPU BF16 audio failures.

Validation: 15 focused Zig tests pass in both CUDA and CPU builds; the complete
CUDA inference type check and release benchmark build pass; 11 Python tests,
Ruff and whitespace/format checks pass. The watch process panics after reporting
successful tests with this Zig toolchain; a subsequent one-shot CUDA test/type
check completes successfully. See the [machine-readable results](automatic-defaults-l4.json)
for artifact hashes, cases and scope.
