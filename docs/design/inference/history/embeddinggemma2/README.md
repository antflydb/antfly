# EmbeddingGemma 2 implementation history

Dated qualification evidence for the EmbeddingGemma 2 runtime. The current
usage guide is [EmbeddingGemma 2](../../../../guides/models/embeddinggemma2.mdx).

- [Model guide and qualification, as of 2026-10-22](2026-10-22-model-guide-and-qualification.md):
  numerical contract, ordered-group batching, decision thresholds,
  long-context and PyTorch comparisons, per-case performance gate, video and
  CUDA BF16 qualification. This was the full guide before it was split.
- [PyTorch comparison data, 2026-10-07](2026-10-07-comparison.json)
- [GIF latency on GB10, 2026-11-03](2026-11-03-gif-latency.md): animated GIF versus
  identical first-frame PNG/GIF controls, cold/warm timings, and CUDA profiling.
- [CPU text query qualification, 2026-11-03](2026-11-03-cpu-query-fallback.md): managed
  CPU/CUDA query latency, fixed-index retrieval, separate-pod qualification gaps,
  and precision recommendations.
- [CPU production qualification on GB10, 2026-11-03](2026-11-03-cpu-production-qualification.md):
  independent F32 reference, full SciFact retrieval, production HTTP contracts,
  sustained load, overload and cancellation/deadline recovery. Qualified scope
  is native F32 text queries on this ARM host.
- [CPU image production qualification on GB10, 2026-11-03](2026-11-03-cpu-image-qualification.md):
  independent image/caption references, CPU/GPU retrieval compatibility,
  image serving load/recovery/overload, byte/pixel boundaries, and the VP8L
  decoder correction found by the campaign.
