# CUDA Generation Batching

CUDA generation batching is experimental and available only through explicit
server configuration:

```json
{
  "generation_batching": {
    "mode": "on",
    "max_step_items": 2,
    "max_step_query_tokens": 512,
    "max_decode_wait_us": 1000
  }
}
```

The experimental safety envelope is limited to two decode rows with matching
sequence lengths and KV positions. Prefill steps stay singleton. When more than
two requests are active, the scheduler preserves fairness but uses the optimized
singleton CUDA route.

The row-two path has repeated paged-KV growth coverage and is deterministic
within a batch. It is intentionally disabled by default: long generation can
choose a different greedy token stream than singleton execution because the row
kernels have different floating-point reduction order, and the optimized
singleton decoder runtime is currently faster.

For optimal singleton service, use the shared tuned server wrapper documented
in `CUDA_TUNING.md`. It deliberately forces batching off because its
request-scoped graph replay profile is not compatible with interleaved batched
requests.

Run the rollout gate with:

```sh
zig/pkg/inference/scripts/gemma4/benchmark_gemma4_cuda_batching.py \
  --tokens 256 --concurrency 1 2 4 --max-step-items 2 \
  --min-c2-speedup 1.50
```

The gate records latency, aggregate throughput, response fingerprints, and
scheduler metrics in `/tmp/antfly-gemma4-cuda-batching` by default. Manual
promotion requires at least 1.50x C2 aggregate speedup, exact C1/C2 response
fingerprints, a passing staggered isolation probe, positive row-two scheduler
hits, and the configured singleton p95 latency bound. Concurrency 4 and higher
remain diagnostic. Keep batching default-off and do not widen the row cap until
the target hardware passes this gate.
