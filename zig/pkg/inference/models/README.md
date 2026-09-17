# Model Family Docs

Per-model-family design and status docs for the inference runtime. Backend
docs (CUDA, Metal, WASM, native, PJRT, ONNX, GGML) and cross-cutting runtime
docs live one level up in `zig/pkg/inference/`; fine-tuning lives in
`../finetuning/`. Dated qualification evidence and implementation ledgers for
each family are under `work-log/completed/inference/<family>/`.

| Family | Document | Covers |
|---|---|---|
| Gemma 4 | [gemma4/GEMMA4.md](gemma4/GEMMA4.md) | Generation support, MTP speculative decoding, chat REPL, current CUDA/Metal defaults, and the Metal performance plan |
| Qwen | [qwen/QWEN3VL.md](qwen/QWEN3VL.md) | Qwen3-VL generation and reranking runtime architecture, safety contracts, and qualification workflow |
| Qwen | [qwen/QWEN3_5.md](qwen/QWEN3_5.md) | Qwen3.5 / Chandra OCR 2 model shape, implemented foundation, and fine-tuning readiness |
| Qwen | [qwen/PERFORMANCE.md](qwen/PERFORMANCE.md) | Retained Qwen3 embedding and Qwen3-VL Metal optimizations, rollback env vars, and the promotion gate |
| GLiNER2 | [gliner2/CUDA.md](gliner2/CUDA.md) | CUDA encoder dispatch policy, qualified benchmark contract, and remaining work |
| GLiNER2 | [gliner2/FINETUNING.md](gliner2/FINETUNING.md) | Operator and release contract for GLiNER2 fine-tuning on Zig, Metal, and CUDA |
| GLiNER2.5 | [../scripts/gliner25/README.md](../scripts/gliner25/README.md) | Development tools, fixture policy, and the CPU ([BENCHMARK.md](../scripts/gliner25/BENCHMARK.md)) and Metal ([METAL_BENCHMARK.md](../scripts/gliner25/METAL_BENCHMARK.md)) comparison harnesses; no design doc yet, see the GLiNER2 docs above for the shared runtime |
| BitNet | [bitnet/BITNET.md](bitnet/BITNET.md) | BitNet-style GGUF support: tensor types, current state, and work items |
| LayoutDoc | [layoutdoc/LAYOUTDOC.md](layoutdoc/LAYOUTDOC.md) | Native LayoutDoc document-classification runtime, HTTP API, probe CLIs, and parity fixtures |

Family directory names follow `scripts/<family>/` and `src/architectures/`.
