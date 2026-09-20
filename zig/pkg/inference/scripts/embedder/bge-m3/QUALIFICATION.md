# BGE-M3 format qualification

Measured on Apple M4 Max, 36 GiB memory, macOS 26.3.1, ReleaseFast. These are short-input measurements on a shared development machine, not an isolated load test or a long-context guarantee. Cold time includes the first model request; the OS file cache was not cleared between processes. Warm latency is the median of three requests after warmup.

All **36 cases passed**. Each of four English, Chinese, and French texts was checked separately, then in mixed-length batches of two and four. The production model manager selected the requested backend without fallback. All models were installed through the production managed pull path.

The independent oracle is ONNX Runtime 1.22.1 on CPU using the official export and tokenizers 0.21.4. Checks require identical token IDs, 1024 finite components, unit norm within 1e-4, oracle cosine >= 0.995, and CPU/Metal and single/batch cosine >= 0.9999. The oracle threshold accommodates the community Q8_0 conversion; this is a small functional qualification, not a retrieval-quality benchmark.

## Artifacts

| Format | Repository | Resolved commit |
|---|---|---|
| onnx | `BAAI/bge-m3` | `5617a9f61b028005a4858fdac845db406aefb181` |
| safetensors | `BAAI/bge-m3` | `84790c1a606f60d06c6932e4ecdd174b466d84ac` |
| gguf | `gpustack/bge-m3-GGUF` | `2d48f1737679ad900d5c26c5aad5410e9c70fdca` |

The official ONNX artifact was selected from `main`; safetensors used the explicit historical revision; GGUF used the explicitly requested `gguf:Q8_0` conversion. Receipts record these sources in the accompanying JSON.

## Accuracy and latency

| Format | Backend | Lowest oracle cosine | Lowest CPU/Metal cosine | Warm batch 1 (ms) | Warm batch 4 (ms) | Batch 4 embeddings/s |
|---|---|---:|---:|---:|---:|---:|
| onnx | native | 1.000000000 | — | 101.5 | 272.6 | 14.7 |
| onnx | metal | 1.000000000 | 1.000000000 | 365.5 | 1384.6 | 2.9 |
| safetensors | native | 1.000000000 | — | 74.5 | 107.3 | 37.3 |
| safetensors | metal | 1.000000000 | 1.000000000 | 58.3 | 67.5 | 59.3 |
| gguf | native | 0.995515936 | — | 48.3 | 260.4 | 15.4 |
| gguf | metal | 0.995516931 | 0.999999881 | 27.3 | 25.5 | 156.9 |

**Imported ONNX is currently slower on Metal than CPU.** It executes the general imported graph; safetensors and GGUF use the native BERT architecture path. Correctness parity does not imply performance parity. Further imported-graph fusion and residency optimization can be measured against this baseline. Quantized BERT encoders retain quantized weights but use f32 activations on CPU so matrix shape does not change embedding arithmetic.

The scope is dense normalized CLS embeddings. Sparse lexical output, ColBERT output, CUDA, large batches, and long-context throughput are not qualified here.

## Reproduce

Build `zig build inference-build-bge-m3-benchmark -Doptimize=ReleaseFast` from `zig/`, then run `qualify_formats.py` with the three installed model directories as documented in [Model Downloads](../../../../../../docs/guides/model-downloads.mdx). The script saves per-case logs and exits nonzero on a failed check. [Machine-readable results](qualification-m4-max.json) include every case, source receipt, cold latency, and single/batch cosine.
