# Qwen3-Embedding parity oracle and qualification

Scripts for qualifying the Antfly inference runtime against the fp32
Transformers reference for `Qwen/Qwen3-Embedding-0.6B` (28-layer
`Qwen3ForCausalLM`, last-token EOS pooling, L2 normalize, 1024-dim output with
Matryoshka truncation to 32-1024 dims).

## Files

- `transformers_embedding_oracle.py` — runs the pinned checkpoint (fp32, CPU,
  eager attention, left padding) over a fixed prompt set and emits the golden
  JSON (`antfly.qwen3_embedding.transformers_oracle.v1`): exact token ids
  (including the single tokenizer-appended trailing EOS `151643`), applied
  instruction per case, and embeddings at dims 1024/256/32 (reduced dims are
  truncate-then-renormalize of the 1024 vector).
- `qualify_qwen3_embedding_metal.py` — replays the oracle cases against a
  running Antfly server's `/ai/v1/embeddings` endpoint and gates cosine
  parity per precision tier, batch-vs-single equivalence, `dimensions`
  truncate+renormalize behavior, unit norms, and top-1 retrieval-rank
  agreement. Stdlib only.
- `requirements-qwen3-embedding-oracle.txt` — frozen oracle dependencies.
- `test_transformers_embedding_oracle.py`, `test_qualify_qwen3_embedding_metal.py`
  — offline unit tests (no network, no model downloads).
- `benchmark_qwen3_embedding_endpoint.py` — throughput/latency benchmark against
  `/v1/embeddings` (separate lane; see its docstring and `BASELINE.md`).

## Produce the oracle

```bash
python3 -m venv .venv-qwen3-embedding && source .venv-qwen3-embedding/bin/activate
pip install -r requirements-qwen3-embedding-oracle.txt
python3 transformers_embedding_oracle.py \
    --output /tmp/qwen3_embedding_oracle.json
# or fully offline from a local checkout of the pinned revision:
python3 transformers_embedding_oracle.py \
    --model-dir /path/to/Qwen3-Embedding-0.6B \
    --output /tmp/qwen3_embedding_oracle.json
```

The hub path pins revision `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3` by
default; the dependency set is verified fail-closed against the pins in the
requirements file.

## Qualify a local server

Pull one of the served variants first:

```bash
antfly inference pull qwen3-embedding-0.6b              # GGUF Q8_0
antfly inference pull qwen3-embedding-0.6b-f16          # GGUF F16
antfly inference pull qwen3-embedding-0.6b-safetensors  # safetensors
```

Then run the gate against the running server:

```bash
python3 qualify_qwen3_embedding_metal.py \
    --oracle /tmp/qwen3_embedding_oracle.json \
    --base-url http://127.0.0.1:8080 \
    --model qwen3-embedding-0.6b \
    --tier q8_0 \
    --report /tmp/qwen3_embedding_qualification.json
```

Exit code 0 means every gate passed; 1 prints a failure table; 2 is an
infrastructure/usage error.

## Gates per tier

| Tier | Min cosine vs fp32 oracle (1024-dim) |
| ---- | ------------------------------------ |
| bf16 | 0.999 |
| f16  | 0.999 |
| q8_0 | 0.995 |
| q4_k | 0.99  |

Tier-independent gates: batch-vs-single cosine >= 0.9999; `dimensions=256`
response vs host-side truncate+renormalize of the server's own 1024-dim vector
cosine >= 0.99999; L2 norm of every returned vector within 1e-3 of 1.0; top-1
retrieval agreement on the oracle query/document matrix; query-vs-document
embeddings of identical text must differ.

## Tests

```bash
python3 -m unittest discover -s . -p 'test_*.py' -v
```
