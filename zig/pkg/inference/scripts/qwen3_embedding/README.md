# Qwen3-Embedding CUDA qualification

This directory contains the reproducible CUDA qualification lane for the
Qwen3-Embedding-0.6B production targets. Both the official Q8_0 GGUF bundle
and official BF16 safetensors bundle are deployable; the independent
Transformers oracle evaluates the pinned safetensors weights in FP32.

The source catalog pins every artifact by repository commit, size, and
SHA-256. Use the registry variant `q8-0-bundle-v1` for deployment and
`bf16-safetensors-bundle-v1` for BF16 CUDA deployment or oracle generation.

## Correctness

Install the isolated oracle dependencies and generate evidence without
network access after the pinned model is present:

```bash
python -m venv /tmp/qwen3-embedding-oracle-venv
/tmp/qwen3-embedding-oracle-venv/bin/pip install -r requirements-qwen3-embedding-oracle.txt
HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  /tmp/qwen3-embedding-oracle-venv/bin/python transformers_embedding_oracle.py \
  --model-dir /path/to/qwen3-embedding-0.6b-bf16 --output /tmp/qwen3-embedding-oracle.json
python qualify_qwen3_embedding_cuda.py \
  --oracle /tmp/qwen3-embedding-oracle.json --model MODEL_ID --tier q8_0
# Use --tier bf16 when MODEL_ID resolves to the safetensors bundle.
```

Run the Antfly server with CUDA as a required backend, not merely as a
preference or preload hint. The embeddings request schema does not select a
backend, so an ordinary auto-backend server may silently load a second CPU
session even when a CUDA variant was preloaded. Use an absolute models path so
preload and request-time discovery also resolve to the same cache key:

```bash
ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda antfly-inference run \
  --models-dir /absolute/path/to/models \
  --preload-model embedder:cuda:MODEL_ID
```

Wait for the server to report `selected backend cuda` and `listening` before
starting the qualifier. The required-backend policy is fail-closed: if CUDA is
unavailable or the artifact cannot use it, qualification requests fail instead
of falling back to a CPU backend.

The qualifier fails closed on vector width, finite values, unit norms,
per-case cosine, batch-versus-single equivalence, Matryoshka
truncate-and-renormalize equivalence, and retrieval top-1 agreement.

## Performance

Run the pretokenized E2E benchmark (model loading and tokenization are outside
the timed region) for rectangular and ragged batches:

```bash
zig build -Dcuda=true -Doptimize=ReleaseFast bench-qwen3-embedding-e2e -- \
  --backend cuda --model-dir /path/to/qwen3-embedding-q8 --batch 8 --seq-len 256
zig build -Dcuda=true -Doptimize=ReleaseFast bench-qwen3-embedding-e2e -- \
  --backend cuda --model-dir /path/to/qwen3-embedding-q8 --lengths 32,64,128,256
```

Promotion requires the lower bound of the 95% confidence interval to reach
at least 90% of the matched PyTorch or llama.cpp throughput, with p95 latency
and peak device memory no more than 1.10x the matched reference. Record the
exact GPU, driver, CUDA artifact SHA-256, model receipt, commands, raw samples,
and warmup policy with every result.

Unit tests for the stdlib-only evidence tools:

```bash
python -m unittest discover -s . -p 'test_*.py'
```
