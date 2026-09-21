# BGE-M3 format qualification

Measured on Apple M4 Max, 36 GiB memory, macOS 26.3.1, ReleaseFast. Measurements use a shared development machine, not an isolated load test. Cold time includes the first model request; the OS file cache was not cleared. The latency table uses the median of three warm requests.

All **111 real-model cases passed**:

- 48 format cases: ONNX, safetensors, and Q8_0 GGUF on native CPU and Metal; four multilingual texts individually and mixed-length batches of 2, 4, 8, and 16.
- 30 Unicode cases: four texts individually and in batches of four, across all formats and both backends. These cover short combining-mark graphemes, the Precompiled normalizer's six-byte cutoff, emoji sequences, Indic scripts, Arabic, Hebrew, Thai, and compatibility characters.
- 18 longer-input cases: 403- and 531-token texts individually and in batches of two, across all formats and both backends.
- 10 independent GGUF execution checks: ONNX Runtime executes the official graph with every initializer replaced by gguf-py's dequantized artifact weights.
- Five explicit `partitioned` ONNX/Metal cases, in addition to the default `compiled-preferred` strategy above.

The production model manager selected the requested backend. All models were installed through the managed pull path. The independent reference uses ONNX Runtime 1.22.1 on CPU and tokenizers 0.21.4. Checks require identical token IDs, 1024 finite components, unit norm within 1e-4, official-model cosine >= 0.995, and CPU/Metal plus single/batch cosine >= 0.9999. The separate dequantized-GGUF reference requires cosine >= 0.9999. No failed threshold is relaxed automatically.

## Artifacts

| Format | Repository | Resolved commit |
|---|---|---|
| onnx | `BAAI/bge-m3` | `5617a9f61b028005a4858fdac845db406aefb181` |
| safetensors | `BAAI/bge-m3` | `84790c1a606f60d06c6932e4ecdd174b466d84ac` |
| gguf | `gpustack/bge-m3-GGUF` | `2d48f1737679ad900d5c26c5aad5410e9c70fdca` |

The official ONNX artifact was selected from `main`; safetensors used an explicit historical revision; GGUF used the explicit `gguf:Q8_0` conversion. Receipts record the sources in the JSON reports.

## Accuracy and latency

The table summarizes the 48-case multilingual matrix. Batch-one latency is the first corpus text; minimum cosines cover all cases for that format/backend.

| Format | Backend | Lowest official-model cosine | Lowest CPU/Metal cosine | Warm batch 1 (ms) | Warm batch 4 (ms) | Batch 4 embeddings/s |
|---|---|---:|---:|---:|---:|---:|
| onnx | native | 1.000000000 | — | 110.4 | 286.8 | 13.9 |
| onnx | metal | 1.000000000 | 1.000000000 | 146.4 | 177.4 | 22.5 |
| safetensors | native | 1.000000000 | — | 74.8 | 100.7 | 39.7 |
| safetensors | metal | 1.000000000 | 1.000000000 | 60.3 | 71.2 | 56.2 |
| gguf | native | 0.999784147 | — | 49.0 | 251.9 | 15.9 |
| gguf | metal | 0.999784409 | 0.999999885 | 28.3 | 35.3 | 113.4 |

Imported ONNX batch-four Metal latency improved from the previous 1384.6 ms baseline to 177.4 ms, about **7.8× faster**, and is now faster than native CPU for this batch. The earlier 166 ms development measurement is within the variation seen between runs; the table uses the final recorded matrix. Safetensors and GGUF still benefit from the dedicated BERT architecture path. Quantized BERT retains quantized weights and uses f32 CPU activations to keep arithmetic stable across batch shapes.

The Unicode corpus exposed an additional GGUF correctness defect: [llama.cpp's RoBERTa converter](https://github.com/ggml-org/llama.cpp/blob/master/conversion/bert.py) removes the reserved leading position rows. Applying full-table indices to that cropped table produced cosine as low as 0.9333. Artifact-aware position offsets restore the correct indexing without copying the table; the fixed Unicode results exceed 0.99979 against the official model. Against independently dequantized GGUF weights, native cosine exceeds 0.99999999999 and Metal exceeds 0.99999988.

## ONNX GPU planning and residency

Dynamic inputs select a bounded four-entry LRU of concrete shape plans. Plans share the parent's resident initializer handles and immutable graph template; replacing a model file cannot mix old weights with newly read graph metadata. Static dependency analysis, typed constant folding, and GPU shape/index primitives keep the qualified graph on Metal.

The strict harness checks partition reports and actual execution statistics for every Metal ONNX case:

- No CPU fallback nodes or host-assisted nodes.
- No intermediate device transfers or host outputs; final public embedding readback remains necessary.
- One graph execution per requested batch, with the actual batch dimension preserved.
- No plan rebuild during warm requests.
- Reusable graph buffer slots and bounded Metal command frames are actually used.

For batch four, the graph has 2603 nodes in one Metal partition, 389 shared initializer handles, 12 reusable buffer slots (6,424,736 bytes), and 62 command-frame chunk boundaries. Chunking retains the existing cancellation checks rather than submitting one unbounded command buffer. Final outputs retain their shared backend ownership across shape-plan eviction.

## Reproduce

Build `zig build inference-build-bge-m3-benchmark -Doptimize=ReleaseFast` from `zig/`, then use the three installed directories with `qualify_formats.py`, as shown in [Model Downloads](../../../../../../docs/guides/model-downloads.mdx). The standard matrix uses `--batches 1,2,4,8,16 --require-resident-onnx`.

For additional runs:

- Unicode: `--texts-json zig/pkg/inference/scripts/embedder/bge-m3/unicode-corpus.json --batches 1,4 --repeats 2`.
- Longer inputs: `--texts-json zig/pkg/inference/scripts/embedder/bge-m3/long-corpus.json --batches 1,2 --repeats 2`.
- GGUF execution reference: select only the GGUF model, add `--oracle-gguf /path/to/bge-m3-Q8_0.gguf --minimum-cosine 0.9999`, and use the Unicode corpus.
- Explicit partitioning: select ONNX and Metal with `--graph-runtime partitioned --require-resident-onnx --batches 1,4 --repeats 2`.

Reports include source receipts, every case, cold/warm timings, parity values, and GPU execution statistics: [format matrix](qualification-m4-max.json), [Unicode](qualification-unicode-m4-max.json), [longer inputs](qualification-long-m4-max.json), [GGUF execution reference](qualification-gguf-reference-m4-max.json), and [explicit partitioning](qualification-partitioned-m4-max.json). Per-case raw logs are also emitted by the harness. Unicode grapheme segmentation separately passes all 1093 Unicode 16 conformance vectors.

The scope is dense normalized CLS embeddings. These checks do not establish retrieval quality, sparse/ColBERT support, CUDA parity, maximum-context performance, or sustained concurrent throughput.
