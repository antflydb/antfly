# Reranking in antfly-inference-zig

antfly-inference-zig supports three reranking approaches: cross-encoder (BERT/RoBERTa native path), late-interaction text reranking (ColBERT), and multimodal late-interaction reranking (ColQwen).

---

## Cross-Encoder Reranking (Native BERT/RoBERTa)

The native cross-encoder path targets RoBERTa-family models such as `bge-reranker-base`.

Relevant code:
- `src/models/bert.zig`
- `src/architectures/bert.zig`
- `src/pipelines/reranking.zig`
- `src/ops/mlx_compute.zig`
- `src/backends/mlx.zig`

### Endpoint

`POST /api/rerank`

Standard cross-encoder inference: tokenize `[CLS] query [SEP] document [SEP]` pairs, run through the BERT/RoBERTa encoder session, extract classification logits, apply sigmoid (num_labels=1) or softmax.

### Distributed MLX Tensor Parallel

Distributed MLX configuration for the native reranker path:

```
ANTFLY_INFERENCE_MLX_DISTRIBUTED_ENABLE=1
ANTFLY_INFERENCE_MLX_DISTRIBUTED_MODE=tensor_parallel
ANTFLY_INFERENCE_MLX_DISTRIBUTED_BACKEND=ring
ANTFLY_INFERENCE_MLX_WORLD_SIZE=<n>
ANTFLY_INFERENCE_MLX_RANK=<rank>
ANTFLY_INFERENCE_MLX_LOCAL_RANK=<rank>
MLX_WORLD_SIZE=<n>
MLX_RANK=<rank>
MLX_HOSTFILE=<path>
ANTFLY_INFERENCE_MLX_ALLOW_CPU_STREAM_WITHOUT_METAL=1
```

The current verified local setup is 2-rank ring mode on one host.

What the TP path implements:
- Fixed TP linear math for sharded `[out, in]` weights
- MLX-native matmul for TP linears
- Per-rank row/column/bias shard cache keyed by tensor name
- Cached borrowed MLX arrays and transposes for those shards
- BERT/RoBERTa encoder attention and FFN projection seams routed through MLX tensor-parallel helpers when TP mode is enabled

### Build and Verify

Build the standalone probe:

```bash
ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache-termite-rerank-probe \
ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache-termite-rerank-probe \
zigup run master build probe-cross-encoder-rerank
```

Run a one-shot probe:

```bash
./zig-out/bin/probe-cross-encoder-rerank \
  /Users/tim/.cache/bge-reranker-base \
  "what is antfly inference zig" \
  "antfly inference is a zig inference server with native model runtimes" \
  --tokenizer-dir /Users/tim/.cache/bge-reranker-base \
  --backend native
```

Run the bounded BLAS-vs-MLX TP verifier:

```bash
bash ./scripts/verify_cross_encoder_rerank.sh
```

That script builds the standalone probe, runs a BLAS baseline, runs a 2-rank MLX tensor-parallel check, and compares scores within tolerance.

### Benchmarking

```bash
ANTFLY_INFERENCE_RERANK_BENCH_REPEAT=8 \
bash ./scripts/benchmark_cross_encoder_rerank.sh
```

Runs repeated reranks in-process on one loaded model session for BLAS and 2-rank MLX TP. The probe reports `last_ms`, `min_ms`, `max_ms`, `avg_ms`, `warm_avg_ms`. `warm_avg_ms` excludes the first run (most useful for the TP shard/transposed-weight cache).

Server-lifecycle benchmark:

```bash
ANTFLY_INFERENCE_BIN=/Users/tim/Documents/af/antfly-inference-zig/zig-out/bin/termite \
ANTFLY_INFERENCE_RERANK_SERVER_BENCH_REPEAT=4 \
bash ./scripts/benchmark_cross_encoder_rerank_server.sh
```

Knobs: `ANTFLY_INFERENCE_RERANK_SERVER_BENCH_REPEAT`, `ANTFLY_INFERENCE_RERANK_SERVER_REQUEST_TIMEOUT_SECS`, `ANTFLY_INFERENCE_RERANK_SERVER_STARTUP_SETTLE_MS`

### Performance Results

Measured on bounded local benchmark input (`bge-reranker-base`):

| Backend | avg_ms | warm_avg_ms |
|---------|--------|-------------|
| BLAS | 3061 | 3056 |
| 2-rank MLX TP | 440 | 297 |

Cold spike ~1.4s, then stable ~295ms warm.

Verifier parity:
- BLAS score: `0.506288`
- 2-rank MLX TP score: `0.506027`
- diff: `0.000261` (within tolerance `0.000500`)

Server path via `/api/rerank`:

| Backend | avg_ms | warm_avg_ms |
|---------|--------|-------------|
| Server BLAS | 3075.3 | 3075.4 |
| Server 2-rank MLX TP | 170.3 | 166.3 |

Server scores matched the standalone verifier exactly.

### Limitations

- Verification is currently on a bounded local 2-rank ring setup, not a production multi-host soak test
- The native TP path is tuned for the BERT/RoBERTa reranker path, not every server model family

### Entry Points

| Artifact | Path |
|----------|------|
| Probe source | `src/probe_cross_encoder_rerank.zig` |
| Verifier | `scripts/verify_cross_encoder_rerank.sh` |
| Benchmark | `scripts/benchmark_cross_encoder_rerank.sh` |
| Server benchmark | `scripts/benchmark_cross_encoder_rerank_server.sh` |

---

## Late-Interaction Text Reranking (ColBERT)

Late-interaction text rerankers such as ColBERT use the same `/api/rerank` endpoint as cross-encoders. The late-interaction scorer runs in Zig using token-level hidden states plus MaxSim scoring.

### Manifest Contract

Add a `model_manifest.json` alongside the model files:

```json
{
  "type": "reranker",
  "capabilities": ["colbert"]
}
```

Recognized capability values: `late_interaction`, `colbert`

When either is present, `LoadedModel.rerankingPipeline()` selects the native late-interaction scorer. Otherwise it uses the cross-encoder scorer.

### Encoding Behavior

Late-interaction single-text encoding is chosen from the model config:

- Encoder-style models: `tokenizer.encodeForModel()`
- Decoder-style models (e.g., `qwen2`): `tokenizer.encodeForGenerationConfigured()` respecting `add_bos_token`

This makes the reranker compatible with both BERT-style ColBERT checkpoints and decoder-style text models that expose token-level hidden states.

### Distributed MLX

The same distributed MLX env contract as the cross-encoder path applies to text late-interaction rerankers:

```
ANTFLY_INFERENCE_MLX_DISTRIBUTED_ENABLE=1
ANTFLY_INFERENCE_MLX_DISTRIBUTED_MODE=data_parallel   # or tensor_parallel
ANTFLY_INFERENCE_MLX_DISTRIBUTED_BACKEND=ring
ANTFLY_INFERENCE_MLX_WORLD_SIZE=<n>
ANTFLY_INFERENCE_MLX_RANK=<rank>
ANTFLY_INFERENCE_MLX_LOCAL_RANK=<rank>
```

The distributed MLX config is plumbed into the native reranker pipeline configuration used by `LoadedModel.rerankingPipeline()`.

Current state on the native BERT/RoBERTa cross-encoder path: distributed MLX TP is implemented and verified; bounded BLAS-vs-TP verification passes; repeated-request benchmarking shows warm TP behavior after the shard/transposed-weight cache is populated.

---

## Multimodal Reranking (ColQwen)

### Endpoint

`POST /rerank_multimodal`

Request fields:
- `model`: reranker name
- `query`: text query
- `documents`: array of multimodal documents

Each document carries `content` in the same format used for generation and embedding:
- plain string text
- array of `ContentPart` values: text parts, `image_url` parts using data URIs, or inline `media` parts with `image/*` mime types

### Current Behavior

- Text-only multimodal documents are reranked through the existing native text reranker path.
- Image-bearing requests are parsed, validated, resized, normalized, and grid-prepared natively in Zig.
- Models that do not advertise `colqwen` or `multimodal_late_interaction` are rejected for image-bearing requests.
- Image-bearing requests execute end to end when the model has a native GPT/Qwen text session plus either a `visual_model` export or native Qwen2-VL vision config.
  - If a `visual_model` export is present, Antfly inference uses it.
  - Otherwise it falls back to the native Qwen2-VL-style vision tower path.
- Visual-session input contract: `pixel_values` and, when requested by the export, `image_grid_thw`
- Visual-session output contract: `[1, tokens, hidden]` or `[tokens, hidden]`; `tokens` must match the prepared image token count.

The text side runs through Antfly inference's native GPT/Qwen compute backend and late-interaction scorer. The image side uses either the `visual_model` export or the native Qwen2-VL-style vision/projection fallback.

Antfly inference has a distributed-aware multimodal ColQwen wrapper:
- The probe reports `runtime.distributed.Config`, `uses_distributed_mlx`, and `uses_tensor_parallel_mlx`
- The served `/rerank_multimodal` path runs through the same wrapper (not free functions)

### Verification

```bash
bash ./scripts/verify_colqwen_rerank.sh
```

Defaults:
- model bundle: `/tmp/colqwen2-v1.0-hf`
- tokenizer bundle: `/tmp/colqwen2-v1.0`
- probe image: `/Users/tim/Documents/af/go-xla/docs/gomlx_stablehlo_gopher.png`

Override with:
```
ANTFLY_INFERENCE_COLQWEN_MODEL_DIR=<path>
ANTFLY_INFERENCE_COLQWEN_TOKENIZER_DIR=<path>
ANTFLY_INFERENCE_COLQWEN_IMAGE_PATH=<path>
ANTFLY_INFERENCE_COLQWEN_QUERY=<string>
```

The verification script rebuilds `probe-colqwen2-rerank`, runs the full native MLX ColQwen2 path, and asserts that the probe reaches `document_encode` and emits a final `score=...` line.

The probe emits:
- `distributed enabled=... mode=... backend=... rank=... world_size=... local_rank=...`
- `pipeline uses_distributed_mlx=... uses_tensor_parallel_mlx=...`

### Model Bundle Notes

The published `vidore/colqwen2-v1.0-hf` config contains the full `vlm_config.vision_config` needed by the native fallback. The lighter `vidore/colqwen2-v1.0-merged` config does not expose enough vision-layer detail on its own, so the native fallback should target the HF wrapper variant unless a separate visual export is present.

### Remaining Work

- Bounded BLAS-vs-MLX TP verification on a real local ColQwen2 bundle
- Request-level `/rerank_multimodal` smoke/regression surface
- Verify native Qwen2-VL vision behavior under distributed MLX on the larger machine
- Unified text and multimodal late-interaction reporting semantics
- Broader multimodal server-path regression coverage
```

---
