# Native KV compaction

Antfly's numerical KV compaction engine fits a smaller attention cache and
exports a portable binary checkpoint tied to the same model revision. It uses
native Zig/CUDA and the existing grammar engine. Structured text memory is a
separate context-management feature.

The bounded production qualification covers Gemma 4 E4B Q4_0 on NVIDIA L4
with f16 base KV and eager CUDA execution. FP16, FP8 disk encoding and rotated4
compressed CUDA checkpoints passed the scoped quality, serving, reliability
and latency gates below. This qualification applies to the recorded model
revision and runtime configuration; it is not a guarantee for arbitrary inputs.

The opt-in `ANTFLY_INFERENCE_CUDA_E4B_SHARED_KV_PREFILL=1` optimization
uses E4B's 24 KV-producing layers for every prompt row, then evaluates its 18
shared-KV layers only for the final output row. Intermediate KV-only chunks
omit that suffix. This follows the same model dependency used by
[vLLM's shared-KV fast prefill](https://github.com/vllm-project/vllm/blob/6d1909a1f23e62d38ad25c029dd43a71106a3990/vllm/model_executor/models/gemma4.py#L1229).
Checkpoint preparation/restoration, query capture, hidden-state consumers and
unsupported request shapes retain the full path. This optimization remains
default-off; the recorded full-replay comparison enables it.

## Preparation and restore

1. Ingest the exact prompt and preserve its tokens and next-token logits.
2. Clone the live KV prefix into an isolated CUDA owner. For long prefixes,
   build a 512-token self-study sequence from four historical 64-token windows
   ranked by token rarity, in their original order, followed by the latest 256
   tokens. Append this sequence to the clone in 256-token chunks, collecting
   320 post-RoPE queries per global consumer: 256 training and 64 held out.
   Capture every shared consumer's query heads. The authoritative history stays
   unchanged, future questions are excluded, and cached keys never substitute
   for queries.
3. Preserve local sliding-window donors, the first 128 global positions and
   the most recent 2,048 global positions exactly. Fit older global history in
   1,024-row chunks, targeting one retained row per four eligible rows. Shared
   donors are fitted once for their consumers.
4. Use residual key selection, nonnegative attention-mass fitting and
   regularized value fitting. Store mass adjustments as attention-logit biases.
   Validate complete heads after rounding to serving precision, requiring
   relative mass error and normalized output RMSE at most 0.05.
5. On numerical rejection, retry the affected head from its immutable source at
   3:1 and then 2:1 eligible-row retention. The error limits stay unchanged; use
   the first accepted fit and preserve the original head if all attempts fail.
   OOM, cancellation and invalid inputs never trigger a retention retry. Accept the complete
   candidate only if actual global device bytes, including metadata, improve
   by at least 1.5x. Otherwise export the original exact state with the explicit
   outcome `unchanged_insufficient_savings`. Cancellation, OOM and invalid
   inputs remain errors.
6. Export wire-version-3 state by default (legacy V2 remains available): model/tokenizer/template/layout identities,
   tokens, logical positions, donor mappings, exact and fitted KV, coverage,
   biases and continuation logits. Resume validates the complete transfer and
   exact rendered-token prefix, uploads independently owned state, and evaluates
   only appended input. An unchanged prompt uses saved next-token logits.

The V3 disk codec stores exact f16 K/V by default, retaining f32 biases/logits
and full integer metadata. Explicit `X-Antfly-KV-Encoding: fp8` requests try
E4M3FN with separate power-of-two key/value scales for each fitted historical
head range. The fitter tests dequantized values against the original whole head
on held-out consumer queries, with the same 5% mass/output limits. Only accepted
ranges replace the candidate values; failures retain the validated f16 fit.
Protected prefix/recent rows, local windows and insufficient-savings fallbacks
remain exact. This storage optimization does not change resident GPU precision
or global-device savings. Fresh-question answer-quality checks include actual
FP8 ranges in every category of the bounded suite. V1/V2 decoding remains supported;
`f32` export selects V2 for older clients. Decoding budgets expanded host state
independently of compressed wire bytes and publishes only after checksum and
semantic validation.

The checkpoint stays immutable and supports a fresh process with matching
identity. Physical compaction preserves logical positions; it does not enlarge
the 32,768-token context window. Appended prompt input is bounded to 8,192 tokens,
and the full prompt plus reserved output must fit the window.

The fitting planner and HTTP admission use the same geometry-derived allocation
bound: original state, worst-case candidate, task metadata and the largest
workspace for each admitted fit worker, including the largest 2:1 retry.
Up to four workers fit concurrently; the planner reduces this to three at
32K on the designated model. Fitting has a 3,840 MiB host ceiling,
with 128 MiB additional preparation allowance. Whole-head validation is limited
to one worker at a time. The reservation is recomputed from geometry, including per-head codec metadata;
this allocation bound is separate from measured RSS.

During upload, admission reserves bounded host memory before acquiring GPU
execution resources. Transfer ownership holds that lease through destruction.
Candidate publication checks owner revision, logical invariants, completed
device work and cancellation before replacing live state. Terminal responses
follow resource release. Ordinary generation can proceed while a partial
checkpoint upload is held; only one native checkpoint operation is admitted.

## Code and public interface

| Responsibility | Location under `zig/pkg/inference/src/` |
| --- | --- |
| Numerical reference and bounded fitting | `runtime/kv/attention_match.zig`, `attention_match_engine.zig` |
| Query collection, snapshot and identity | `runtime/kv/attention_match_queries.zig`, `attention_match_snapshot.zig`, `attention_match_identity.zig` |
| Portable state and transactional ownership | `runtime/kv/attention_match_checkpoint.zig`, `attention_match_resume.zig`, `attention_match_transaction.zig` |
| CUDA fitting, storage and fitted attention | `ops/cuda/attention_match_fit.zig`, `attention_match_storage.zig`, `attention_match_decode.zig` |
| Model integration and admission | `pipelines/generation.zig`, `server/server.zig`, `server/native_kv_wire.zig` |

Public endpoints are `POST /ai/v1/chat/context/checkpoint` and
`POST /ai/v1/chat/context/resume`. The Python SDK provides streaming file
helpers; JSON and SSE continuations use the same checkpoint contract.
See [API usage and supported configurations](../../../zig/pkg/inference/OPENAI.md#portable-native-kv-checkpoints)
and [SDK usage](../../../py/packages/sdk/README.md#native-kv-checkpoints).
The legacy compaction/selected-KV paths are separate and do not qualify this engine.

## Rotated4 resident encoding

Explicit `X-Antfly-KV-Encoding: rotated4` exports V4. Newly fitted old rows use
seeded signed Hadamard rotations, 4-bit keys and int8 values. The highest-risk
quarter of eligible rows remains FP16, selected using training queries. The
combined fitting and quantization error must still pass the original whole-head
held-out limits; rejected heads retain their validated FP16 fit. Publication
requires the FP16 fitted geometry to meet the savings gate before new codec
savings are counted, so quantization cannot bypass insufficient-savings fallback.
Protected prefix/recent rows and local windows stay exact.

CUDA scores compressed keys and accumulates decoded values directly, preserving
compressed history through append, clone, grow and rollback. This reduces both
wire and resident KV storage. FP8 and rotated4 are alternative encodings; FP16
remains the default. No experimental process switch is required for rotated4.
This is a TurboQuant-inspired MSE codec, not the paper's full Haar/QJL algorithm.
See [representation and CUDA ownership](../../../zig/pkg/inference/TURBOQUANT.md#attention-matched-caches-rotated4-checkpoints-and-cuda).

## Bounded qualification, 2026-10-23

The frozen suite contains synthetic multi-step facts, instruction priority,
code reasoning and typed tool calls. Sixteen critical cases were selected before
execution, four per category, at each context length. Checkpoint preparation
sees the archive only; the question is appended after export. Every restore must
attest saved-prefix reuse and equal prompt token accounting.

| Quality observation | 8K | 32K |
| --- | ---: | ---: |
| Cases per encoding | 16 | 16 |
| Correct with full KV / FP16 / FP8 / rotated4 | 13 / 13 / 13 / 13 | 13 / 13 / 13 / 13 |
| New compaction or codec failures | 0 | 0 |
| Actually compacted cases per encoding | 10 | 16 |
| Unchanged insufficient-savings fallbacks | 6 | 0 |
| Categories with actual FP8 and rotated4 coverage | 4/4 | 4/4 |

The shared model failures are `v2-code-01`, `v2-code-03` and
`v2-instruction-03`; they are not counted as compaction successes. Fallbacks
never count as quantization coverage. These synthetic cases establish the
approved bounded gate, not universal answer preservation or an external benchmark.
The 8K and 32K manifest SHA-256 values are respectively
`11b587964826b3e84a90881363b845964e5d857294ec148d2bf799a12dd8a14e` and
`8b52a176ebca0fc3ab49465ad03e6f747fb0e3d36eff605885e00bc4b21ba8f4`.

The final query-selection fix addresses a real 32K tool-argument regression:
two historical calibration windows had both selected tool declarations and
missed an identifier. Four rarity-ranked historical windows plus recent context
preserved the answer with real compaction. The total 512-token study budget,
1,024-row fit blocks, 5% numerical limits and savings gates were unchanged.
A smaller-fit-block alternative was rejected because it only preserved the
answer through unchanged-cache fallback and increased preparation time.

### Final public standalone measurements

These are the designated public fact fixtures, distinct from the quality-suite
latency case. Warm portable restore includes transfer, validation, upload and
a short continuation. Each encoding uses five warmups and 20 measured samples
in an independent warmed process; these are not interleaved pairs.

| Prefix | Encoding | File MiB | Global-KV savings | Warm restore p50 / p95 (s) | Initial prepare/export (s) | Peak host / GPU GiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 8,189 | f16 | 94.32 | 1.755x | 1.872 / 2.029 | 84.88 | 5.46 / 11.30 |
| 8,189 | fp8 | 83.06 | 1.755x | 1.924 / 1.970 | 85.93 | 5.46 / 11.74 |
| 8,189 | rotated4 | 87.50 | 1.937x | 1.972 / 1.999 | 85.32 | 5.46 / 11.74 |
| 32,752 | f16 | 226.74 | 2.494x | 4.255 / 4.514 | 134.46 | 7.45 / 12.46 |
| 32,752 | fp8 | 179.45 | 2.494x | 4.438 / 4.513 | 140.50 | 7.45 / 12.46 |
| 32,752 | rotated4 | 203.60 | 2.811x | 4.578 / 4.671 | 137.11 | 7.47 / 12.46 |

The 8K FP16 fixture with a 1,437-token appended suffix resumed in 6.91 s,
versus 2.87 s for warm full replay of that complete conversation. These are
single functional-test observations, not percentile qualification. Native
checkpoint append retains the full model path, whereas ordinary E4B replay
uses shared-KV fast prefill. Compaction therefore does not guarantee a latency
win on appended input; its memory savings remain a separate benefit.

Initial preparation includes prompt ingestion, cold model-identity work, fitting
and export; it is not warm restore latency. Historical 10 s/30 s preparation
targets were accepted as nonblocking. Global-KV savings do not describe total
GPU memory or whole-file savings. In the compacted quality cases, FP8 reduces
whole-file size versus FP16 by a median 8.87% at 8K and 15.27% at 32K;
protected/exact ranges and metadata prevent a blanket 50% saving.

Both codecs passed the relative restore limits against FP16: p50 at most 1.25x
and p95 at most 1.5x for total, first-content and remaining-decode latency.
Separately, the quality campaign's interleaved FP16/rotated4 measurements passed
those same six limits at both sizes. Final exact-token full replay also passed
the p50/p95 limits against llama.cpp at both sizes. Full replay and portable
restore are different operations; the restore table does not establish
llama.cpp speedup.

Public checks cover fresh-process restore, byte-identical repeat export,
JSON/SSE continuation, appended multi-chunk input, unchanged fallback,
checksum/prefix rejection, 32K context-limit rejection and recovery. FP8 and
rotated4 each passed physical OOM/recovery, cancellation during upload/prefill/
fitting/export, competing-upload admission, SSE abort and terminal cleanup.
The physical OOM check covers checkpoint restore on the same loaded worker;
it does not qualify live-cache replacement under physical GPU OOM.
Each also passed 100 mixed requests with 10 fresh actual compactions, the hard
8 GiB combined listener/worker RSS and 18 GiB GPU caps, and the 128 MiB
quiescent-growth diagnostic. Request overlap is bounded; GPU execution is serial.
The 20 successive real-model compaction cycles were explicitly deferred.

### Matched full replay

The same model and exact prompt tokens are replayed from scratch, with five
warmups and 20 samples per runtime. The designated answer has five content
tokens; the comparator reconciles Gemma turn-token accounting explicitly.

| Prefix tokens | Antfly full replay p50 / p95 (s) | llama.cpp p50 / p95 (s) | Antfly / llama.cpp p50 |
| --- | ---: | ---: | ---: |
| 8,189 | 2.125 / 2.177 | 1.975 / 1.999 | 1.076x |
| 32,752 | 11.248 / 11.494 | 10.586 / 10.625 | 1.063x |

### Provenance and scope

The loaded-model weights identity is
`fd904d45d8efe0c9c71bc74586f43b6f48334c65c164bdcc0b4963b4a6707987`
(the native domain-separated identity, not the raw GGUF file hash).

The final standalone binary SHA-256 is
`be249c4fbebcd50a33b4a60b4302283eab7e12ae4007a4f67b5faa8fd65f5402`;
its frozen source snapshot is
`c409206ce0b2fc4f5d63de729ecb5c76e50f057e21fd32c9b89a8bd2575394bc`.
The full numerical quality suites ran on focused inference-server binary
`632d4e4ed1f340f7e915aeb6a29cb9cb5bdc9a14589ad98f012de5b4cc9c6306`.
The final public candidate retains all 409 recorded numerical/runtime source
hashes; the intervening changes expose the qualified codec through the public
API/SDK and regenerate their schemas. The final standalone then ran the public,
reliability and latency matrix. Documentation-only qualification updates are
recorded separately from the tested runtime snapshot.

Final candidate SDK checks passed: Python 363 tests, Go SDK tests, TypeScript
493 tests with one skip plus typecheck, and checkpoint harness 70 tests with
450 subtests. Generated OpenAPI and Python SDK consistency checks passed.
Kernel correctness and CUDA memcheck/racecheck evidence covers the unchanged
fitted-attention implementation.

Evidence is retained locally under
`.cache/turboquant-selective/e4b-compaction-readiness/`:
`final-qualification.json` hashes all contributing artifacts;
`study-balanced-{quality,fp8-quality}-{8k,32k}.json` record quality;
`final-http-*`, `final-restore-*`, `final-stress-*` and
`final-replay-comparison-*` record standalone behavior and timing.
Earlier rejected fits, fallback-only pilots and publication-policy regressions
remain historical evidence under `.cache/turboquant-selective/` and
`.cache/fp8-readiness/`; they are not passing release evidence.

Use the [serving configuration](../../../zig/pkg/inference/OPENAI.md#portable-native-kv-checkpoints)
including its tuning flags and memory budgets. Other model revisions, devices,
graph replay, continuous batching, automatic mid-generation compaction and
legacy `polar4`/`turbo3` require separate qualification. Extended 64-case quality,
100-sample latency and 1,000-request stress profiles remain available; this
bounded record does not claim those larger campaigns passed.

## Reproducing checks

Run from the repository worktree root. Keep generated suites, model data,
checkpoints, logs and build outputs out of Git, under `.cache/`, `.models/` and
`zig/zig-out/`. Use fresh evidence paths and run hardware campaigns serially on
an idle GPU after compilation finishes.

```sh
py/packages/sdk/.venv/bin/python -m unittest discover \
  -s zig/pkg/inference/scripts/gemma4 -p 'test_*quality*.py'
bash zig/pkg/inference/scripts/regen-attention-match-fit.sh --check
bash zig/pkg/inference/scripts/regen-attention-match-decode.sh --check

python3 zig/pkg/inference/scripts/gemma4/prepare_native_quality_suite.py \
  --output .cache/native-compaction/native-quality-suite-v2.jsonl
py/packages/sdk/.venv/bin/python \
  zig/pkg/inference/scripts/gemma4/qualify_native_checkpoint_quality.py \
  --server zig/zig-out/bin/antfly --server-kind standalone --models-dir .models \
  --suite .cache/native-compaction/native-quality-suite-v2.jsonl \
  --quality-profile bounded --size 8k --encoding fp8 \
  --cases v2-fact-00 v2-instruction-00 v2-code-00 v2-tool_call-00 \
          v2-fact-01 v2-instruction-01 v2-code-01 v2-tool_call-01 \
          v2-fact-02 v2-instruction-02 v2-code-02 v2-tool_call-02 \
          v2-fact-03 v2-instruction-03 v2-code-03 v2-tool_call-03 \
  --output zig/zig-out/qualification/context-cuda/quality.json \
  --checkpoint .cache/native-compaction/quality-current.kv
```

For the 32K quality suite, generate with `--lines 2900` and qualify with
`--size 32k`. The designated public 32,752-token latency fixture instead uses
`--padding-lines 2975`. Rotated4 whole-system qualification uses
`qualify_rotated4_checkpoint.py --quality-profile bounded --require-full-kv-preservation`
with the same cases, and `--samples 20 --warmups 5` for paired latency.
Do not treat unchanged fallbacks as actual codec coverage.

Retained entry points under `zig/pkg/inference/scripts/gemma4/`:

| Purpose | Runner |
| --- | --- |
| Full-model rotated4 quality and paired latency | `qualify_rotated4_checkpoint.py` |
| Public export, SDK roundtrip and fresh-process restore | `qualify_native_checkpoint_http.py` |
| Prepare-phase sampling; terminal ownership and resume sampling | `qualify_native_prepare_sampling.py`; `qualify_completion.py` |
| Cancellation, physical OOM, upload overlap, SSE abort | `qualify_native_cancellation.py`, `qualify_native_checkpoint_oom.py`, `qualify_native_concurrency.py`, `qualify_native_sse_abort.py` |
| Sustained native requests | `qualify_native_stress.py` |
| Matched exact-token replay measurements and comparison | `qualify_matched_antfly.py`, `qualify_matched_llama.py`, `compare_matched_latency.py` |
| Private query, fit, restore and repeated-cycle diagnostics | `qualify_attention_match_queries.py` |
| Structured-memory/window API checks | `qualify_context_management.py`, `stress_context_cuda.py` |

Use each runner's `--help` for its required reference/checkpoint inputs.
Private diagnostics do not qualify the public API. Kernel-level CUDA oracles
are retained for the production fitter, fitted attention, repeat attention and
staged Q4 projection (`qualify_attention_match_fit.{cu,py}`,
`qualify_attention_match_decode.cu`, `qualify_repeat_attention{,_kernel}.cu`,
`qualify_q4_bf16_staged.cu`). The repeat `_kernel` file supplies the independent
chronological trace oracle; it is not a spare prototype. CUDA regeneration
scripts and shipped artifacts remain required.
