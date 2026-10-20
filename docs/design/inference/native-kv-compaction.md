# Native KV compaction

Antfly's numerical KV compaction engine fits a smaller attention cache and
exports a portable binary checkpoint tied to the same model revision. It uses
native Zig/CUDA and the existing grammar engine. Structured text memory is a
separate context-management feature.

The current scope is Gemma 4 E4B Q4 on NVIDIA L4 with f16 KV and eager CUDA
execution. Broader answer quality and complete production qualification remain
open. Numerical validation alone does not establish answer quality.

## Preparation and restore

1. Ingest the exact prompt and preserve its tokens and next-token logits.
2. Clone the live KV prefix into an isolated CUDA owner. Replay its last 512
   tokens in 256-token chunks, collecting 320 post-RoPE queries per global
   consumer: 256 training and 64 held out. Capture every shared consumer's
   query heads; never use cached keys as proxy queries.
3. Preserve local sliding-window donors, the first 128 global positions and
   the most recent 2,048 global positions exactly. Fit older global history in
   1,024-row chunks, targeting one retained row per four eligible rows. Shared
   donors are fitted once for their consumers.
4. Use residual key selection, nonnegative attention-mass fitting and
   regularized value fitting. Store mass adjustments as attention-logit biases.
   Validate complete heads after rounding to serving precision, requiring
   relative mass error and normalized output RMSE at most 0.05.
5. Preserve heads whose numerical fits are rejected. Accept the complete
   candidate only if actual global device bytes, including metadata, improve
   by at least 1.5x. Otherwise export the original exact state with the explicit
   outcome `unchanged_insufficient_savings`. Cancellation, OOM and invalid
   inputs remain errors.
6. Export wire-version-2 state: model/tokenizer/template/layout identities,
   tokens, logical positions, donor mappings, exact and fitted KV, coverage,
   biases and continuation logits. Resume validates the complete transfer and
   exact rendered-token prefix, uploads independently owned state, and evaluates
   only appended input. An unchanged prompt uses saved next-token logits.

The checkpoint stays immutable and supports a fresh process with matching
identity. Physical compaction preserves logical positions; it does not enlarge
the 32,768-token context window. Appended prompt input is bounded to 8,192 tokens,
and the full prompt plus reserved output must fit the window.

The fitting planner and HTTP admission use the same geometry-derived allocation
bound: original state, worst-case candidate, task metadata and the largest
workspace for each of four fit workers. Fitting has a 3,840 MiB host ceiling,
with 128 MiB additional preparation allowance. Whole-head validation is limited
to one worker at a time. For the designated 32K fixture, the total reservation
is 4,049,665,584 bytes; this is an allocation bound, not measured RSS.

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
See [API usage and supported configurations](../../../zig/pkg/inference/OPENAI.md#portable-native-kv-checkpoints-under-qualification)
and [SDK usage](../../../py/packages/sdk/README.md#native-kv-checkpoints-under-qualification).
The legacy compaction/selected-KV paths are separate and do not qualify this engine.

## Recorded evidence, 2026-10-20

The pre-merge standalone ReleaseFast candidate
`9ee66204fb1b4df5557f68bbac3ce66c6039e941104cf9a57b8c84febd751deb`
passed public 8K/32K export, SDK repeat preparation, byte-identical re-export,
fresh-process restore, wrong-prefix/checksum rejection and recovery, and
context-limit rejection.

| Public-path observation | 8K | 32K |
| --- | ---: | ---: |
| Logical prefix tokens | 8,189 | 32,752 |
| Actual global-KV savings | 1.686994x | 2.085466x |
| Validated / preserved global heads | 6 / 2 | 6 / 2 |
| Checkpoint bytes | 201,728,532 | 555,662,840 |
| Warm saved-prefix resume | 2.411 s | 6.265 s |
| Complete initial prepare/export request | 102.611 s | 403.763 s |
| Fresh-process restore, including model load/identity hashing | 70.082 s | 74.347 s |
| Peak exporter listener + worker RSS | 5.422 GiB | 7.541 GiB |
| Peak exporter GPU memory | 11.658 GiB | 12.424 GiB |

These are individual observations, not preparation or general serving
percentiles. Global-KV savings do not describe total GPU memory or checkpoint
file compression. The 8K checkpoint plus a 1,437-token suffix resumes in
9.214 s versus 40.746 s for full replay of the same conversation.
A separate saved-prefix completion campaign with five warmups and 100 samples
measures 2.385 s p50, 2.478 s p95 and 2.731 s maximum, for a five-token answer.

The same candidate passed cancellation during upload, prefill, fitting and
export; physical restore OOM/recovery; SSE abort/recovery; and terminal cleanup.
Ordinary generation completed during a held partial upload, competing native
operations failed promptly, and reset released the guard in 11.797 ms.
This qualifies bounded request overlap, not simultaneous GPU model execution.

After merging main at `e0fa87c69f`, 130 native CPU tests passed, with one
CUDA-only skip covered by the 18/18 L4 CUDA checks. Go SDK tests passed;
Python SDK had 38 passes, TypeScript 493 passes/one skip plus typecheck,
OpenAPI 10 passes, and isolated grammar/tokenizer 37 passes.
These are recorded checks, not a substitute for final-candidate qualification.

The post-merge focused inference server
`6a43b29c70a0ad1eee752d223dd1117311dbbce41398e3088ddfe17538607d8d`
reproduces the 8K reference checkpoint byte for byte and restores its correct
answer. Its digest is
`5b09989a64944c79c14990b9d7bea167dc45509323527452ce038825b7a63c8a`.
The complete standalone campaign has not been repeated on the merged candidate.

## Answer quality

`prepare_native_quality_suite.py` generates the frozen V2 suite: 64 synthetic
cases, 16 per category, crossing four task families with archive positions at
10%, 25%, 45% and 60%. Sixteen cases are designated critical before execution.
The archive is checkpointed before the final question; restore appends the
question and must attest saved-prefix reuse and equal full-prompt token counts.
The suite digest remains
`11b587964826b3e84a90881363b845964e5d857294ec148d2bf799a12dd8a14e`.

All 16 critical cases were exercised in two diagnostic runs:

| Category | Full-KV correct | Restored correct | Actually compacted |
| --- | ---: | ---: | ---: |
| Multi-step facts | 4/4 | 4/4 | 0/4 |
| Instructions | 3/4 | 3/4 | 0/4 |
| Code reasoning | 2/4 | 2/4 | 0/4 |
| Typed tool calls | 4/4 | 4/4 | 0/4 |

All used `unchanged_insufficient_savings`. Equal 13/16 scores establish no added
failures on exact fallback; they do **not** establish quality under compaction.
The full 64-case campaign has not passed. The failures are `v2-code-01`
(filtered sum 17 instead of 25), `v2-code-03` (JSON count 16 instead of 19),
and `v2-instruction-03` (`ready.` instead of `ready`, without credential exposure).
The code answer keys agree with Python execution. On identical native tokens,
llama.cpp b11669 reproduces the first arithmetic failure at short and long
context; that is a scoped diagnostic, not broad runtime parity.

Scoring rejects malformed/duplicate-key/nonfinite JSON, wrong argument types,
extra fields/calls, truncated answers and changed/reordered requests. It
separates baseline failures, regressions and improvements. Unchanged fallbacks
cannot conceal failures on the actually compacted subset; interrupted runs
retain completed pairs without passing full-suite gates. Older evidence remains
readable, but new runs require an explicit frozen suite path.

A private fit-only diagnostic on the 8,253-token code archive found:

| Calibration replay | Validated / preserved heads | Candidate savings | Query collection |
| --- | ---: | ---: | ---: |
| Last 512 tokens | 3 / 5 | 1.248935x | 0.941 s |
| Full prefix | 2 / 6 | 1.148928x | 16.840 s |

Neither passes the public 1.5x gate. No candidate was installed or production
threshold changed. Including the final question before checkpointing produces
1.513153x savings on that code prompt, but does not validate unseen questions.

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
  --output zig/zig-out/qualification/context-cuda/quality.json \
  --checkpoint .cache/native-compaction/quality-current.kv
```

Retained entry points under `zig/pkg/inference/scripts/gemma4/`:

| Purpose | Runner |
| --- | --- |
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

## Release gates and next work

| Area | Required evidence |
| --- | --- |
| Quality | At most 2 percentage points aggregate drop and 5 per category versus full KV; all designated critical cases pass; actual compaction in every category with the same limits on the compacted subset |
| Compression | Nominal 4x eligible-old-row retention and at least 2x actual global-KV savings on the designated 32K workload; public candidate minimum remains 1.5x |
| Memory | At most 8 GiB combined listener/worker RSS and 18 GiB GPU memory, including temporary fitting/replacement allocations |
| Latency | Matched full-replay p50 at most 1.25x and p95 at most 1.5x llama.cpp; at least five warmups and 100 samples per designated workload |
| Reliability | 1,000 mixed requests, 100 compactions and 20 successive real-model compaction cycles; no corrupt state, leaked admission or unbounded growth |

First measure per-head rejection causes and evaluate bounded fit/retention
alternatives for archive-only prefixes. Full-prefix replay alone did not help.
Keep baseline reasoning/format failures separate from compaction regressions.
Freeze a successor before the remaining 48 quality cases and complete matrix,
then broaden histories and 32K coverage. Final-candidate sustained testing,
live-replacement failure checks, matched latency and merged standalone
requalification remain open. Historical 10 s/30 s compaction targets were
accepted as nonblocking by the user; continue recording phase timings separately
from ingestion. Automatic mid-generation compaction and additional models or
backends need separate qualification.

Evidence lives locally under `zig/zig-out/qualification/context-cuda/`:
`public-checkpoint-deferred-upload-admission-{8k,32k,cancellation,oom,concurrency,sse-abort,completion}.json`,
`quality-v2-critical-summary.json` (with both source fingerprints),
`quality-v2-merged-reference-diagnostic.json`, `quality-v2-code-token-capture.json`,
`quality-v2-llama-code-diagnostic.json`, and `quality-v2-calibration-diagnostic.json`.
The first quality run stopped intentionally after eight complete cases; the
second finished with `passed=false` and no infrastructure error. Historical
experiment chronology is available in Git rather than duplicated here.
