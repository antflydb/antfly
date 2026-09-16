# Reranking in antfly-inference-zig

antfly-inference-zig supports three reranking approaches: cross-encoder (BERT/RoBERTa native path), late-interaction text reranking (ColBERT), and multimodal late-interaction reranking (ColQwen).

---

## Cross-Encoder Reranking (Native BERT/RoBERTa)

The native cross-encoder path targets RoBERTa-family models such as `bge-reranker-base`.

Relevant code:
- `src/models/bert.zig`
- `src/architectures/bert.zig`
- `src/pipelines/reranking.zig`

### Endpoint

`POST /api/rerank`

Standard cross-encoder inference: tokenize `[CLS] query [SEP] document [SEP]` pairs, run through the BERT/RoBERTa encoder session, extract classification logits, apply sigmoid (num_labels=1) or softmax.

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
  ~/.cache/bge-reranker-base \
  "what is antfly inference zig" \
  "antfly inference is a zig inference server with native model runtimes" \
  --tokenizer-dir ~/.cache/bge-reranker-base \
  --backend native
```

### Benchmarking

```bash
ANTFLY_INFERENCE_RERANK_BENCH_REPEAT=8 \
bash ./scripts/benchmark_cross_encoder_rerank.sh
```

Runs repeated reranks in-process on one loaded model session for BLAS. The probe reports `last_ms`, `min_ms`, `max_ms`, `avg_ms`, `warm_avg_ms`. `warm_avg_ms` excludes the first run (most useful for the TP shard/transposed-weight cache).

Server-lifecycle benchmark:

```bash
ANTFLY_BIN=./zig-out/bin/antfly \
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

### Verification

```bash
bash ./scripts/verify_colqwen_rerank.sh
```

Defaults:
- model bundle: `/tmp/colqwen2-v1.0-hf`
- tokenizer bundle: `/tmp/colqwen2-v1.0`
- probe image: any small local PNG

Override with:
```
ANTFLY_INFERENCE_COLQWEN_MODEL_DIR=<path>
ANTFLY_INFERENCE_COLQWEN_TOKENIZER_DIR=<path>
ANTFLY_INFERENCE_COLQWEN_IMAGE_PATH=<path>
ANTFLY_INFERENCE_COLQWEN_QUERY=<string>
```

The verification script rebuilds `probe-colqwen2-rerank`, runs the full native ColQwen2 path, and asserts that the probe reaches `document_encode` and emits a final `score=...` line.

### Model Bundle Notes

The published `vidore/colqwen2-v1.0-hf` config contains the full `vlm_config.vision_config` needed by the native fallback. The lighter `vidore/colqwen2-v1.0-merged` config does not expose enough vision-layer detail on its own, so the native fallback should target the HF wrapper variant unless a separate visual export is present.

### Remaining Work

- Request-level `/rerank_multimodal` smoke/regression surface
- Unified text and multimodal late-interaction reporting semantics
- Broader multimodal server-path regression coverage
```

---
