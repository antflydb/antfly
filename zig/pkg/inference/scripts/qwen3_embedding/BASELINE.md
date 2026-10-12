# Qwen3-Embedding Baseline Measurement Protocol

Protocol for establishing and refreshing Qwen3-Embedding performance baselines on
the Antfly inference runtime. Fill in the results tables once model bundles are
available locally.

## Configurations

Measure the full cross product and record every cell:

| Dimension   | Values                                                        |
|-------------|---------------------------------------------------------------|
| Backend     | metal (resident), native                                      |
| Corpus      | short (~20 tok), passage (~256 tok), long (1k-8k tok), mixed  |
| Batch sizes | 1, 8, 32, 128                                                 |

Fixed protocol per cell: 3 warmup iterations, 20 measured iterations, quiet
machine (no other GPU work), report p50/p95 latency and embeddings/sec. For the
pretokenized bench also record real tok/s vs padded tok/s (padding waste).

Current reference results, qualification scope, and measured refutations are
summarized in [Qwen performance](../../models/qwen/PERFORMANCE.md#qwen3-embedding-reference-results).
The [M4 Q8_0 gap summary](reports/qwen3_embedding_metal_gap_m4_q8_summary.md)
records the latest qualification receipt.

> **Relocated:** Dated results (2026-10-01 short-query/capacity/soak, 2026-09-04 native admission smoke, and the 2026-09-02 interim, parity-plan, and gap-closure sessions) → [qwen3-embedding-baseline-results.md](../../../../../docs/design/inference/history/qwen/qwen3-embedding-baseline-results.md).

## Pretokenized encoder bench (no tokenizer, no HTTP)

```bash
cd zig/pkg/inference
zig build bench-qwen3-embedding-e2e -Doptimize=fast -- \
    --model-dir ~/.antfly/inference/models/<qwen3-embedding> \
    --backend metal --batch 32 --seq-len 256 --warmup 3 --iters 20

# Ragged mode (per-item token lengths, padded to batch max):
zig build bench-qwen3-embedding-e2e -Doptimize=fast -- \
    --model-dir ~/.antfly/inference/models/<qwen3-embedding> \
    --lengths 20,256,1024 --warmup 3 --iters 20

# First-vector dump for oracle cross-checks:
#   add --print-embedding and compare against transformers_embedding_oracle.py
```

The model dir may also come from `ANTFLY_INFERENCE_QWEN3_EMBEDDING_MODEL`.

## Endpoint bench (Antfly vs llama.cpp)

Start Antfly inference serving the model with one resident request at a time,
then start llama.cpp with the same physical batch/ubatch, last-token pooling,
flash attention, model-default EOS, and all prompt caches disabled:

```bash
llama-server -m qwen3-embedding-<size>-<quant>.gguf \
    --embeddings --pooling last -c 4096 -b 4096 -ub 4096 \
    --flash-attn on --cache-ram 0 --parallel 1 \
    --host 127.0.0.1 --port 8080
```

Run both through the same OpenAI-compatible API:

```bash
python scripts/qwen3_embedding/benchmark_qwen3_embedding_endpoint.py \
    --url http://127.0.0.1:18099/ai/v1/embeddings \
    --reference-url http://127.0.0.1:8080/v1/embeddings \
    --model '<served-antfly-model-id>' --reference-model '<llama-model-id>' \
    --fixture scripts/qwen3_embedding/fixtures/qwen3_embedding_0_6b_exact_tokens.json \
    --fixture-token-count 511 --antfly-reported-token-offset -1 \
    --reference-reported-token-offset 0 --batch-sizes 1 --iters 20 --warmup 2 \
    --require-comparable --cosine-threshold 0.995 --fail-below-ratio 0.90 \
    --antfly-model-file '<path-to-gguf>' --reference-model-file '<path-to-same-gguf>' \
    --antfly-build-id '<git-revision-and-dirty-state>' \
    --reference-build-id '<llama.cpp-revision-and-build>' \
    --antfly-build-file '<path-to-live-antfly-executable>' \
    --reference-build-file '<path-to-live-llama-server-executable>' \
    --antfly-server-pid '<live-antfly-pid>' --reference-server-pid '<live-llama-pid>' \
    --antfly-server-args '<shell-quoted argv after the Antfly executable>' \
    --reference-server-args '<shell-quoted argv after the llama-server executable>' \
    --output baseline_qwen3_embedding.json
```

Repeat with `--fixture-token-count 2551`. Strict mode hashes both model paths
and requires byte-identical files; resolves each live PID to the declared
executable, hashes those executable bytes, and exactly matches its live argv;
requires every response to report the requested model; rotates through distinct
equal-length prompts with globally distinct first tokens to defeat active-slot
longest-common-prefix reuse; validates every server-reported prompt count;
alternates AB/BA order; cross-checks every measured embedding pair; and reports
a deterministic paired-bootstrap 95% interval for the throughput ratio. The
checked-in compact fixture expands to exact token sequences that include the
model-required EOS in their counts.
Antfly's usage field excludes that injected EOS while llama.cpp's includes it,
hence the documented `-1` Antfly offset.

On a fanless host, add `--precondition-iters 8` to the long-context run. Those
iterations alternate AB/BA over two fixture batches reserved outside the timed
warmup/measurement set. They are excluded from all statistics and leave a
different-first-token prompt in each active slot before timing starts. Record
the count in the JSON report; do not compare a cold run on one backend with a
thermally steady run on the other.

Do not use repeated prompts: llama.cpp can reuse an active slot's prefix even
with `--cache-ram 0`. Do not override `tokenizer.ggml.add_eos_token=false` for
this model: last-token pooling then selects a different model token and destroys
output parity. `llama-bench -p N -n 0` is also not an endpoint-equivalent
reference unless embedding mode, flash attention, KV types, batch, and ubatch
are all made identical.

## Profiling environment variables

For Metal stage attribution when a cell regresses:

- `TERMITE_METAL_STAGE_TIMING=1` — per-stage GPU timing (attention/ffn/tail/etc.)
- `TERMITE_METAL_PREFILL_TRACE=1` — prefill dispatch trace
- `TERMITE_EMBED_RESIDENT_FAIL_CLOSED=1` — fail instead of silently falling back
  off the resident Qwen3 path (the pretokenized bench already fails closed on
  metal via `resident_projection_required`)

## Results

Machine: ____ (chip, RAM, macOS) — Date: ____ — Commit: ____ — Model/quant: ____

### Pretokenized (bench-qwen3-embedding-e2e, metal)

| Corpus  | Batch | p50 ms | p95 ms | emb/s | real tok/s | padded tok/s | waste |
|---------|-------|--------|--------|-------|------------|--------------|-------|
| short   | 1     |        |        |       |            |              |       |
| short   | 8     |        |        |       |            |              |       |
| short   | 32    |        |        |       |            |              |       |
| short   | 128   |        |        |       |            |              |       |
| passage | 1     |        |        |       |            |              |       |
| passage | 8     |        |        |       |            |              |       |
| passage | 32    |        |        |       |            |              |       |
| passage | 128   |        |        |       |            |              |       |
| long    | 1     |        |        |       |            |              |       |
| long    | 8     |        |        |       |            |              |       |
| mixed   | 32    |        |        |       |            |              |       |
| mixed   | 128   |        |        |       |            |              |       |

### Endpoint (antfly vs llama.cpp, corpus=mixed)

| Batch | antfly p50 ms | antfly emb/s | llama.cpp p50 ms | llama.cpp emb/s | min cosine |
|-------|---------------|--------------|------------------|-----------------|------------|
| 1     |               |              |                  |                 |            |
| 8     |               |              |                  |                 |            |
| 32    |               |              |                  |                 |            |
| 128   |               |              |                  |                 |            |
