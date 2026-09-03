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

## Pretokenized encoder bench (no tokenizer, no HTTP)

```bash
cd zig/pkg/inference
zig build bench-qwen3-embedding-e2e -Doptimize=ReleaseFast -- \
    --model-dir ~/.antfly/inference/models/<qwen3-embedding> \
    --backend metal --batch 32 --seq-len 256 --warmup 3 --iters 20

# Ragged mode (per-item token lengths, padded to batch max):
zig build bench-qwen3-embedding-e2e -Doptimize=ReleaseFast -- \
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
checked-in fixture includes the model-required EOS in its exact token counts.
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

## Interim results — 2026-09-02 perf session (batch-1 endpoint, graph phase)

Machine: Apple M4 (fanless), 16GB, Darwin 25.5.0 — branch qwenvl_metal (uncommitted
working tree over 4eee44472) — Qwen3-Embedding-0.6B GGUF Q8_0.

Timings are the `text.encoder.qwen3.graph` phase from `TERMITE_EMBED_TIMING=1`
on single-document requests; llama.cpp reference is `llama-bench -p N -n 0`
(build 660b1b4bd) on the identical GGUF.

**Historical only:** the llama.cpp column below used llama-bench's generation
graph defaults (`embeddings=false`, `ubatch=512`, flash attention off), so it is
not an apples-to-apples embedding endpoint comparison. Keep it only as the
optimization session's original reference point; use the strict endpoint
protocol above and the closure results below for parity claims.

| Tokens | naive baseline | +batched Q8_0 FFN | +SG flash (q8 tile) | +q16 tile | llama.cpp pp |
|--------|----------------|-------------------|---------------------|-----------|--------------|
| 511    | 3.4 s          | —                 | 0.51 s              | 0.46 s (1107 tok/s) | 2012 tok/s |
| 2551   | 52 s (34.5 s warm) | 34.5 s        | 5.2 s               | 4.12 s (620 tok/s)  | 1608 tok/s |
| 8192   | ~600 s (timeouts) | —              | 37 s                | —         | —            |

Fix 1: `tryDeviceQuantizedGatedFfnResidual` per-row loop → batched Q8_0 device
encode (dispatches per 2551-tok request: 214,761 → 477). Rollback:
`TERMITE_METAL_FORCE_Q8_0_GATED_FFN_ROWWISE=1`.

Fix 2: `termite_attention_f32_dense_causal_sg[_q16]` flash kernels replace the
naive/tiled f32 attention for full causal self-attention from position zero
(q_len >= 8, no bias/mask/window, head_dim % 32 == 0; q16 variant for
q_len >= 16 and head_dim <= 128). Rollbacks:
`TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION[_Q16]=1`.
Measured refutation: raising the tiled-kernel 2048 kv gate to 7936 (plan B1.d.1)
was 2.5x SLOWER than the naive kernel at 2551 tokens (barrier-bound) — do not
revisit without restructuring.

All 78 qualification gates re-pass after each fix (reports in session scratch;
regenerate via qualify_qwen3_embedding_metal.py). Remaining gap vs llama.cpp:
~1.8x short / ~2.6x long. Next levers: GQA head-pair K/V sharing in the flash
kernel, Q8_0 mm_sg linear throughput (~1.2 TFLOP/s observed), f16 activations
(B1.e), batching hygiene (B1.c). Full protocol tables above still to be filled
via bench-qwen3-embedding-e2e once the batch campaign runs.

## Parity-plan results — 2026-09-02 (batch-1 endpoint, graph phase, interleaved with llama-bench)

Same machine/model as above. All timings from a thermally-interleaved A/B run
(fanless M4: only interleaved comparisons are valid).

The llama.cpp values in this historical table have the same llama-bench protocol
mismatch noted above and overstate the fair gap. They are not the final parity
result.

| Tokens | before plan | final (f16-KV flash + zero-bias elision) | llama.cpp pp (same window) | gap |
|--------|-------------|------------------------------------------|----------------------------|-----|
| 511    | 0.46 s (1107 tok/s) | **0.42 s (1217 tok/s)** | 2060 tok/s | 1.69x |
| 2551   | 4.12 s (620 tok/s)  | **3.03 s (842 tok/s)**  | 1640 tok/s | 1.95x |

Landed (all uncommitted, kill switches in parens):
- f16 K/V flash attention: per-layer f32→f16 K/V convert into two persistent
  private buffers + `termite_attention_f32_dense_causal_sg_q16_f16kv` with
  direct device simdgroup loads, tail chunk staged
  (`TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION_F16KV`). 3.18 s @2551 alone.
- Zero-bias elision default-on — qwen3 GGUF slots carry synthesized zero
  biases; skipping the add is an identity
  (`TERMITE_METAL_DISABLE_ZERO_BIAS_ELISION`).

Measured refutations this round (do not revisit without new evidence):
- Preferring the split mm_sg route over the fused scalar
  `q8_0_pair_activation_multiply_mm` for gate+up at rows>=129: 33% SLOWER
  end-to-end — the fused kernel's per-row-group weight re-reads are
  cache-resident and the split pays extra dispatches + intermediate traffic.
- GQA head-pair attention kernel (2 q-heads per 256-thread TG sharing K/V
  reads, `..._sg_q16_f16kv_gqa2`): ~35% SLOWER — 23KB threadgroup footprint
  halves occupancy. Kernel kept opt-in via
  `TERMITE_METAL_ENABLE_DENSE_CAUSAL_SG_ATTENTION_GQA_PAIR`.

Exit state per plan: remaining ~2x sits in the Q8_0 mm-class linears
(~1.9 s of the 3.03 s @2551, ~1.2 TFLOP/s effective vs llama.cpp's ~2-3).
Closing it needs either a counter-guided mm_sg schedule campaign or end-to-end
f16 activations (new f16-input mm_sg + f16 norm/rope plumbing) — both out of
scope for the parity plan. Deferred small item: per-layer Q/K retainedCopy
removal (B1.f, ~10 ms). All 78 qualification gates pass on the final config.

## Gap-closure results — 2026-09-02 (strict embedding endpoint)

Machine: Apple M4 (fanless), 16 GB, Darwin 25.5.0. Antfly branch
`qwenvl_metal`. The server was built from the Metal source patch identified in
the raw reports as base `d90bd83a493ddca2f124621ed0648158633b1953` plus
SHA-256 `3a0e8f0efaa8e4395017bf791f122e050a7928f5f812fe0273ba720bbdfac786`;
those exact kernel sources are now captured by `d9753f524369509d9533a3f16e8436c01963e6cf`.
The final Antfly executable SHA-256 is
`06b0824cd0dfc10e3736d3f3f30af4df2c183728707b358e12a91efdd130afdb`;
the live llama.cpp build is `b8990-660b1b4bd`, executable SHA-256
`8eee1b1fa1c65d919c94116dd286134a4a9498059c21c1ee448c45fb87f2c590`.
Both endpoints opened the byte-identical Qwen3-Embedding-0.6B Q8_0 GGUF with SHA-256
`06507c7b42688469c4e7298b0a1e16deff06caf291cf0a5b278c308249c3e439`.

Strict cache-neutral endpoint results (batch 1, 2 warmups, 20 measured,
alternating AB/BA, distinct first token every timed request). The 2551-token
result used eight unmeasured alternating thermal-preconditioning pairs over
two additional reserved prompts:

| Model tokens | Antfly mean | llama.cpp mean | Antfly/llama estimate | paired 95% CI | min cosine | gate |
|--------------|--------------|----------------|------------------------|---------------|------------|------|
| 511 | 310.82 ms, 1644.0 tok/s | 293.17 ms, 1743.0 tok/s | **94.32%** | 94.17%-94.48% | 0.99999896 | PASS (lower CI >= 90%) |
| 2551 | 2303.85 ms, 1107.3 tok/s | 2120.87 ms, 1202.8 tok/s | **92.06%** | 90.20%-94.06% | 0.99996579 | PASS (lower CI >= 90%) |

The first preconditioned long run still crossed a thermal transition late in
the timed window and is retained as failed evidence: 90.72% estimate with an
89.11%-92.16% CI. An immediate steady-state replicate with the same protocol
passed. Report both when discussing fanless-host variance; use the passing
steady-state run above for the thermally stable comparison, not the faster
transition-state absolute tok/s.

The checked-in raw reports for this final executable are:

- `reports/qwen3_embedding_strict_511_pr.json`
- `reports/qwen3_embedding_strict_2551_pr.json` (thermal-transition failure)
- `reports/qwen3_embedding_strict_2551_pr_steady.json` (steady-state pass)
- `reports/qwen3_embedding_qualification_pr.json` (78/78 gates)

### Q8_0 matmul work that closed the gap

All winning routes are default-on and have startup-time rollback switches:

- Single-linear SG-v2: vectorized Q8_0 block dequantization, direct simdgroup
  matrix loads, a barrier-free 32-row bulk store, and a separate ragged tail.
  Roll back with `TERMITE_METAL_DISABLE_Q8_0_SG_V2=1`.
- Fused gate+up SG-v2: one activation tile feeds both Q8_0 projections and the
  SiLU/multiply epilogue, with f32 and f16-output variants. This removed the
  dominant duplicate activation traffic. Roll back with
  `TERMITE_METAL_DISABLE_Q8_PAIR_ACTIVATION_SG_V2=1`.
- F32-input M64 single-linear schedule: 64 rows by 64 output columns, eight
  simdgroups, K=32 panels, and separate bulk/tail stores. Roll back with
  `TERMITE_METAL_DISABLE_Q8_0_SG_M64=1`.

Optimization ladder from same-session cool focused runs:

| Configuration | 511 tokens | 2551 tokens |
|---------------|------------|-------------|
| previous parity-plan final | 1217 tok/s | 842 tok/s |
| + single-linear SG-v2 | 1218 tok/s | 854 tok/s |
| + fused gate+up SG-v2 | 1779.6 tok/s | 1218.6 tok/s |
| + f32 M64 schedule | 1882.8 tok/s | 1246.0 tok/s |

The freshly rebuilt no-enable-flags focused binary reached 1119.2 tok/s at
2551 tokens after the sustained endpoint run; disabling M64 produced the same
`-0.378788` checksum, and disabling fused gate+up selected its fallback with
checksum `-0.378625`. Treat those ordered rollback timings only as route proof,
not an A/B speed comparison, because the device temperature was changing.

Measured refutations retained opt-in for future hardware/schedule work:

- F16-input M64 (`TERMITE_METAL_ENABLE_Q8_0_SG_M64_F16=1`) was slower:
  1221.7 versus about 1246 tok/s at 2551 tokens. The f16-input SG-v2 M32 route
  remains the default.
- Sharing one activation tile across QKV's K/V projections
  (`TERMITE_METAL_ENABLE_Q8_KV_PAIR_SG=1`) was neutral within noise: 1237.6
  versus 1236.7 tok/s. It remains off.
- The earlier 23 KB GQA head-pair attention and split gate/up experiments
  remain refuted as documented above.

The refuted opt-in pipeline states are now created only when their enable flag
is present; promoted pipelines are not created when their rollback flag is
present.

### Correctness and build evidence

- Fresh serial package production build from `zig/pkg/inference`:
  `zig build -j1 -Doptimize=ReleaseFast -Dmetal=true -Donnx=false -Dpjrt=false`.
- Fresh server with no performance enable flags and
  `TERMITE_EMBED_RESIDENT_FAIL_CLOSED=1`: all 78 q8_0 gates passed, including
  oracle, MRL, batch equivalence, 8192-token truncation, retrieval, and role
  distinction. Lowest oracle cosine: 0.998499.
- Endpoint harness: 45 unit tests pass; scripts compile with `py_compile`; all
  48 checked-in fixture cases round-trip exactly through llama.cpp tokenization
  at their declared 511/2551 model-token lengths.
- Focused Metal hardware tests cover both promoted high-row paths: single Q8_0
  SG-v2/M64 linears match the host reference at 65, 96, and 128 rows, and the
  65-row fused gated-FFN route matches both the decomposed implementation and
  its rollback path.

The full repository test suite was not rerun locally. The monorepo production
build also stopped at its safety check before compiling: the storage-kernel
step declares an 18 GiB RSS bound on this 16 GiB host. The scoped inference
package production build and focused tests above pass; run the aggregate suite
in CI or on a host with adequate RAM and cache space.
