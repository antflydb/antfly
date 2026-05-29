# antfly-inference-zig TODO

## Features (missing from Go inference)

- [x] **Grammar + speculative decode parity**: native generation now applies grammar masking and grammar advancement during target-side speculative verification, so constrained decoding and draft-model acceleration can coexist on the native backend.
- [x] **Constrained decoding backend policy**: constrained decoding is explicitly native-backend only for now. ONNX `/api/generate` requests remain unconstrained-only unless we add token-level masking support there.
- [ ] **Result caching**: Go antfly inference has per-endpoint TTL caches with singleflight dedup for: embedding, sparse embedding, chunking, reranking, NER, reading, transcription. ResultCache and singleflight primitives exist in `src/cache/` but aren't wired into any handler.
- [ ] **Dense binary serialization**: Go antfly inference defaults to `application/octet-stream` for `/api/embed` (LE float32 arrays). JSON is opt-in via Accept header. We always return JSON.
- [ ] **Sparse binary serialization**: Go antfly inference supports binary `SparseVectorsContentType` (`application/x-sparse-vectors`, Accept header-based). Lower priority — JSON format works.
- [x] **Tool-calling parity**: `/api/generate` now executes FunctionGemma-style tool use end to end, including prompt formatting, non-streaming parsing, and streamed `tool_calls` argument deltas.
- [ ] **Multimodal generation parity**: Zig has an ONNX `ortgenai` image-bearing generation path, but native multimodal generation is still missing for models like Gemma 3, and multimodal success coverage/streaming behavior still lag Go inference.
- [ ] **Native GLiNER parity validation**: GLiNER now has a native DeBERTa + span-head path and prefers native weights when available. The remaining work is proving parity with real GLiNER models across MLX/BLAS, adding backend-specific tests, and tightening any performance gaps in the native head.

## MLX Gemma Follow-Up

- [ ] **Budget coordination plan**: see [BUDGETS.md](./BUDGETS.md) for the canonical plan covering `LoadBudget`, `RunBudget`, global coordination, and the Hypura-like tiered runtime direction.
- [ ] **Clean up MLX Gemma debug scaffolding**: remove the temporary scheduler/KV/paged-attention debug env toggles and noisy MLX tied-logits logging added during the Gemma 3 decode investigation, while keeping the actual correctness fixes.
- [x] **Validate MLX Gemma before commit**:
  1. short multimodal MLX CLI prompt
  2. short server `/api/generate` multimodal check
  3. one longer text-only MLX sample before committing
