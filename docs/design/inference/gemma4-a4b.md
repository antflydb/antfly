# Gemma 4 26B-A4B serving

How Antfly loads and serves the Gemma 4 26B-A4B mixture-of-experts checkpoint.
The user-facing summary is in
[Local Model Compatibility](../../guides/supported-models.mdx#generation-families);
dated measurements are in [history/gemma4](history/gemma4/a4b-perf.md).

## Supported layout and placement

Gemma 4 26B-A4B support is intentionally narrower than dense Gemma 4. The GGUF must match the qualified 30-layer, 128-expert, top-8 Q4_0 layout. Metal supports bounded or resident placement. CUDA requires a 16 GiB resident A4B envelope and the loaded resident Q4_0 MoE primitives; device selection follows kernel availability, rather than the GPU used for benchmarking. Unsupported geometry, streamed CUDA, or unavailable kernels are rejected at load time rather than falling back to host MoE execution. Server deployments must also leave transient host/combined admission headroom for the encoded GGUF during CUDA upload; only the 16 GiB CUDA envelope remains leased after load.

## CUDA loading

CUDA uses a bounded multithreaded pinned-host load pipeline by default. Clean GGUF pages remain kernel-reclaimable and shared across rolling workers unless `--a4b-drop-host-cache-after-load` is passed to `antfly inference generate`. Production images can install a versioned `$MODEL/a4b-cuda-pack-v2` pre-sharded expert pack; use `--a4b-prepared-pack required` to fail closed if that artifact is absent or stale. Server preloads automatically choose the loader worker count and staging budget, use a valid installed pack when available, and warn and use the canonical GGUF when an optional pack is unusable. Preloads eagerly publish a reusable model session by default. This is a full-model residency contract, not an `ngl`-style layer split. Preload entries accept only the public model reference fields (`kind`, `name`, `backend`, `format`, `quantization`, `residency_mode`, `memory_budget_mb`); loader tuning is not configurable per preload. Cache retention is a deployment tool rather than a latency guarantee; hosts must have enough reclaimable memory for their checkpoint and loader working set, and should measure replacement-worker load time on the actual storage topology.
