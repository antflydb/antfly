# OpenAI Compatibility in antfly-inference-zig

antfly-inference-zig can be used as a local OpenAI-compatible inference server for chat-style generation. The goal is not to copy the entire OpenAI platform, but to make common OpenAI client integrations work with antfly-inference-zig using a familiar API shape.

## What Works

### Chat completions

Use the OpenAI-style endpoint:

```text
/api/chat/completions
```

antfly-inference-zig also keeps its native generator endpoint:

```text
/api/generate
```

Both endpoints route to the same generation logic. If you are configuring an OpenAI SDK, point `base_url` at:

```text
http://host:port/api
```

and use normal chat completions calls.

### Response format

Non-streaming chat responses follow the OpenAI chat completion shape, including:

- `id`
- `object`
- `created`
- `model`
- `choices`
- `usage.prompt_tokens`
- `usage.completion_tokens`
- `usage.total_tokens`

### Streaming

Streaming responses use Server-Sent Events and emit OpenAI-style chat completion chunks:

- `object: "chat.completion.chunk"`
- stable `id` across the stream
- stable `created` timestamp across the stream
- an initial `delta.role = "assistant"` event
- incremental `delta.content` updates
- final `finish_reason`
- terminating `data: [DONE]`

Streaming works for both ONNX-backed generation and native generation paths.

### Models listing

`/api/models` includes an OpenAI-friendly top-level model list:

```json
{
  "object": "list",
  "data": [
    {
      "id": "model-name",
      "object": "model",
      "created": 1234567890,
      "owned_by": "termite"
    }
  ]
}
```

antfly-inference-zig still keeps its richer nested task-based model metadata in the same response so existing antfly inference clients continue to work.

### Health endpoints

Operational endpoints are available outside `/api`:

- `/healthz`
- `/readyz`

`/healthz` reports basic liveness.  
`/readyz` reports whether antfly-inference-zig can discover usable models and includes per-task counts. The server initializes this inventory before publishing its listener and refreshes it periodically; probes return the last successful snapshot without scanning model storage on the request path.

## Compatibility Notes

### Token accounting

antfly-inference-zig reports:

- prompt tokens when the server can determine them directly
- completion tokens from the generated output
- total tokens as the sum of the two

For some ONNX generation paths, prompt token accounting may be approximate or unavailable at the backend layer. In those cases antfly-inference-zig still returns a valid OpenAI-compatible response shape.

### Tool calling

Tool-call parsing and streaming are supported through antfly-inference-zig's existing tool parser integration. When a model emits tool calls, antfly-inference-zig maps them into OpenAI-style `tool_calls` structures in both full responses and streaming deltas.

### Portable native KV checkpoints

Native KV checkpoints preserve numerical attention state for Gemma 4 E4B Q4_0.
The bounded production qualification covers NVIDIA L4, eager CUDA, f16 base KV,
and FP16/FP8/rotated4 checkpoint encodings at 8K and 32K. See the
[qualification record](../../../docs/design/inference/native-kv-compaction.md#bounded-qualification-2026-10-23)
for the exact model revision, measurements and limits. The public endpoints are:

```text
POST /ai/v1/generate/context/checkpoint
POST /ai/v1/generate/context/resume
```

Preparation accepts a generation request with the full messages, tool definitions
and template options. It emits no answer and requires `stream: false`. The
response contains metadata and one binary checkpoint in an `AFATT001` attachment
envelope. Use the [Python SDK streaming helpers](../../../py/packages/sdk/README.md#native-kv-checkpoints)
to validate and save the checkpoint without buffering the whole response.

Checkpoint export defaults to V3 `f16` storage: K/V values occupy two bytes on
disk and restore exactly to the supported f16 serving cache. Positions, coverage,
biases and continuation logits retain their original precision. Set the
`X-Antfly-KV-Encoding` request header to select:

| Encoding | Wire version | Behavior |
| --- | --- | --- |
| `f16` (default) | 3 | Lossless storage of the supported f16 cache; halves K/V payload bytes versus V2. |
| `fp8` (opt-in) | 3 | E4M3FN for fitted historical ranges that pass whole-head validation after dequantization; rejected attempts keep the validated f16 fit. |
| `rotated4` (opt-in) | 4 | Rotated 4-bit keys and int8 values for accepted fitted history; compressed rows remain resident on CUDA. |
| `f32` | 2 | Legacy export for older readers. |

FP8 uses independent power-of-two K/V scales per accepted head. Protected prefix
and recent rows and all local windows remain exact f16; attention biases remain
f32. `fp8_heads` and `fp8_fallback_heads` report accepted and rejected FP8 attempts
on a successfully compacted checkpoint. An insufficient-savings fallback stays
entirely exact, even when FP8 was requested. Restore automatically detects the
wire encoding and expands to f16 serving state; GPU memory savings are unchanged.
Older SDKs/readers need V2 export or an upgrade. File-size savings exclude unchanged
metadata and scales. The bounded quality gate includes fresh-question
continuations with actual FP8 coverage in every task category; qualification
does not establish answer preservation for arbitrary workloads.

Rotated4 first validates the FP16 fit, then validates the combined fitting and
quantization error against the original cache. Rows with the highest estimated
attention error remain FP16; rejected heads retain their validated FP16 fit. Protected prefix/recent rows and
local windows stay exact. Accepted rotated4 rows reduce both checkpoint and
resident KV storage. The metadata reports accepted and fallback head counts plus eligible,
compressed, and retained-exact row counts. Unchanged fallbacks never count as
quantization coverage. FP8 and rotated4 are alternative export encodings.
Rotated4 requires no experimental process switch; the default remains f16.
See the [codec and qualification record](TURBOQUANT.md#attention-matched-caches-rotated4-checkpoints-and-cuda)
for the V4 representation and tested scope.

Resume sends that checkpoint with the complete messages and the same tool/template
options. Their rendered tokens must extend the saved token prefix exactly. The
server evaluates only the appended suffix; an unchanged prompt resumes from the
saved next-token logits. Resume supports ordinary JSON and SSE responses. The
checkpoint stays immutable and can be reused after a server restart with matching
model weights, tokenizer, chat template and KV layout.

Read the preparation metadata before treating the operation as compaction:

| Outcome | Meaning |
| --- | --- |
| `compacted` | Validated fitting met the minimum 1.5x actual global-cache savings. Heads that failed validation remain exact. |
| `unchanged_insufficient_savings` | The checkpoint contains the original exact state because fitting could not meet the savings threshold. It remains resumable; count it as an unchanged fallback. |

The native window is 32,768 logical tokens. Preparation reserves one continuation
position. Resume allows at most 8,192 newly appended prompt tokens, and the full
prompt plus reserved output must fit the native window. Reducing physical KV rows
does not reset logical positions or increase this limit. Metadata is limited to
2 MiB and checkpoint files to 2 GiB. Consume and validate the complete transfer
before publishing a file; a partial HTTP 200 response is not a usable checkpoint.

The qualified configuration uses eager CUDA execution, disabled decode
graph replay and disabled continuous batching. When using the Gemma tuning wrapper,
set `ANTFLY_SERVER_DECODE_GRAPH_REPLAY=off`,
`ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING=1` and
`ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS=0`. Native checkpoints do not support
media, draft models, prompt-cache keys, compiled/graph execution, or combining the
request with `context_management`, `kv_cache` or `cache_compaction_ratio`.

The recorded L4 campaign additionally uses these backend and tuning settings:

```sh
export ANTFLY_INFERENCE_PREFERRED_BACKEND=cuda
export ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda
export ANTFLY_SERVER_DECODE_GRAPH_REPLAY=off
export ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING=1
export ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS=0
export ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=flash-f16-sm89
export ANTFLY_INFERENCE_CUDA_Q4_0_CUBLAS_PREFILL=1
export ANTFLY_INFERENCE_CUDA_FUSED_GATE_UP_BF16=1
export ANTFLY_INFERENCE_CUDA_E4B_F16_WARP_SPLIT=1
export ANTFLY_INFERENCE_CUDA_Q4_0_PLE_GATE_FUSION=0
export ANTFLY_INFERENCE_CUDA_E4B_SHARED_KV_PREFILL=1
```

Shared-KV fast prefill applies to eligible ordinary generation. Native checkpoint
preparation and append retain the full model path. Portable restore latency
excludes initial preparation, and appended-input restore can be slower than full
replay; the qualification record reports both observations.

Preparation reserves host scratch from model geometry and prompt length, bounded
by a 3,840 MiB fitting ceiling plus 128 MiB auxiliary allowance. The default
768 MiB scratch budget is insufficient for the designated 8K/32K workloads.
The standalone qualification uses these inference budget overrides:

```text
--inference-host-budget-mb 8192
--inference-backend-budget-mb 18432
--inference-combined-budget-mb 26624
--inference-kv-budget-mb 4096
--inference-scratch-budget-mb 6144
```

These are admission settings, not measurements of memory consumption. The
concurrency qualifier reserves 48 weighted admission units for two designated
8K requests costing 24 units each. It checks the combined listener/worker RSS
against 8 GiB and worker GPU memory against 18 GiB. Live host admission can still
reject a request when available memory is
insufficient. Keep client timeouts large enough for prompt ingestion, cold model
loading/identity hashing, fitting and transfer; warm resume timings exclude those
preparation costs.

| HTTP status | Client action |
| --- | --- |
| `400` | Inspect the error for an invalid frame, changed model/prefix, unsupported runtime or exceeded logical context window. Correct the request before retrying. |
| `413` | Reduce a transfer that exceeds the metadata/checkpoint limits. |
| `503` | Honor the returned transient-capacity retry information. Only one native checkpoint operation is admitted at a time. |
| `507` | Inspect the configured inference memory budgets and available resources. Repeating the same request does not resolve an insufficient configured budget. |

The [implementation and release-gate record](../../../docs/design/inference/native-kv-compaction.md)
records the passing 16-case quality suites, 20-sample warm latency checks and
100-request/10-compaction reliability run per codec. The same three full-KV
model failures remain separate from the zero added compaction/codec failures.
Automatic mid-generation compaction, graph execution, extended stress profiles
and other models/backends require separate qualification.

### Context preparation and bounded chat

Context preparation is an Antfly extension rather than an OpenAI API field. Use the canonical inference endpoints:

```text
POST /ai/v1/generate/context/prepare
POST /ai/v1/generate
```

`/generate/context/prepare` accepts the model, messages, tools, `tool_choice`, `chat_template_kwargs`, an output-token reservation, and one of these modes:

- `inspect` reports exact rendered input counts without changing the transcript.
- `window` removes eligible old turns while preserving system/developer instructions, recent complete turns, and tool-call/result units.
- `compact` creates a versioned structured-memory checkpoint. Compaction must be requested explicitly; generation does not summarize implicitly.

The latest user turn and its entire tool-call/result sequence are always protected,
even with `keep_recent_turns: 0`. If that mandatory turn cannot fit, windowing
returns `CONTEXT_LIMIT_EXCEEDED` rather than splitting the turn. Agent tool loops
must then reduce their inputs or rebuild context at the agent layer.

Generation's `context_management.mode` accepts `reject` or `window`; preparation
accepts `inspect`, `window`, or `compact`. `ContextDiagnostics.policy` reports the
requested action. Its booleans report actual changes: `applied` means the effective
input changed, `windowed` means messages were windowed away, `compacted` means
checkpoint memory is present, and `checkpoint_created` identifies a newly created
checkpoint. Applying existing memory can therefore report `compacted: true` under
`window` without running a summarizer.

Context planning always emits `context.tool_results`, including `[]` when there
are no identified tool-result outcomes. This applies to `/generate/context/prepare`
and generation with `context_management`. The schema remains optional for older
servers and responses without context planning; a missing field is not an empty
outcome list.

The array identifies each supplied tool result by `tool_call_id` and original `message_index`, with disposition `retained`,
`summarized`, or `dropped`. Summarized results are represented by attributed
checkpoint evidence, possibly verbatim. Evidence from an earlier checkpoint has
no `message_index`; a reused call ID can have separate archived and current
outcomes. Tool messages without IDs are omitted. These outcomes describe raw retention or attributed
evidence, not whether the model uses it correctly. During `compact`, a
result folded only into the conversation summary, without matching attributed
`tool_evidence`, is conservatively reported as `dropped`, not `summarized`.

Per-result token counts are out of scope. Context planning drops whole messages
and never truncates within one. The agent layer must compute delivered tokens
itself; an outcome describes retention or attribution, not a token budget.

Structured checkpoints require matching model, tokenizer, and template revisions.
HTTP 409 `CONTEXT_CHECKPOINT_STALE_REVISION` identifies a revision change;
`CONTEXT_CHECKPOINT_MISMATCH` identifies digest, history, or format errors. On a
revision change (including generator fallback), rebuild from the original
transcript for the selected model instead of retrying the same checkpoint.

Internal compaction summarizes prior memory together with completed exchanges,
retaining fact labels and tool attribution while omitting repetitive dialogue.
Its schema grammar permits at most one formatting-whitespace character at each
separator so indentation cannot consume the summary budget; whitespace inside
strings is unaffected. A truncated or invalid summary fails the request before
any checkpoint is issued. After validating tool attribution, tool results of at
most 256 bytes are retained verbatim when the existing tool-evidence byte budget
allows it. Recompaction preserves prior small results; a newly completed result
supersedes prior evidence. Larger results remain model-summarized. The final
checkpoint must still fit the exact rendered token budget. Conversation facts
and larger tool results require model-backed quality validation.

Send the returned messages to generation. When a checkpoint is present, those messages contain the retained instructions and active suffix; do not replay the archived prefix. Supply the checkpoint through `context_management.checkpoint`:

```json
{
  "model": "your-gemma4-e4b-model",
  "messages": [
    {"role": "system", "content": "The original instruction returned by prepare."},
    {"role": "developer", "content": "The original developer instruction returned by prepare."},
    {"role": "user", "content": "Continue from the checkpoint."}
  ],
  "max_tokens": 256,
  "context_management": {
    "mode": "reject",
    "checkpoint": {"version": "v1", "...": "fields returned by prepare"}
  },
  "kv_cache": {"mode": "exact_hybrid", "version": "v1"}
}
```

Managed responses include `context` diagnostics with original and effective input counts, the target and reserved output counts, retained/dropped message counts, checkpoint state, and the effective KV policy. Managed SSE generation emits an empty-choices metadata chunk containing these diagnostics before token chunks. Clients should fail closed if the diagnostics do not attest the requested mode, checkpoint, and KV policy.

Structured-memory digest mismatches and stale model/tokenizer/template revisions are rejected when a checkpoint is consumed. The covered-prefix digest records provenance for explicit recompaction; generation with an independent active suffix has no archived prefix to compare against it. These digests detect accidental changes and unrecomputed edits; checkpoints are unsigned request data, so a party able to rewrite the checkpoint can also recompute its unkeyed digests. Treat checkpoints as portable prompt data rather than tamper-proof authorization credentials. Applications remain responsible for access control, trusted storage, and transcript ownership.

Current limits:

- `select_v1` selection and storage code exists, but the public policy remains rejected by default until a backend is qualified with transactional selected-KV storage.
- The current GPU selection scorer is a correctness-first scalar implementation; performance qualification and optimized scoring remain pending.
- `exact_hybrid` preserves model-native attention semantics. Eligible Metal greedy routes use bounded local-attention rings with global KV history; other routes retain full history. It does not enable lossy selected-KV retention.
- Sampling and grammar-constrained generation retain full KV history by default. A direct eager ring route exists behind an internal qualification switch; it rejects paths that could materialize evicted history. It still requires hardware qualification.
- The deprecated `cache_compaction_ratio` field is unsupported by the resident server.
- Speculative draft-model preparation is unsupported; preparation budgets the target model.
- Compaction is lossy structured memory. A successful response does not promise semantic equivalence with the archived transcript.
- Exact limits and quality depend on the model revision, tokenizer, chat template, backend, and memory budgets used by the server.

### Scope

This compatibility layer is aimed at practical SDK interoperability:

- Python OpenAI SDK chat completions
- streamed chat completions
- model listing
- health checks for local and Kubernetes-style deployments

It should be treated as API compatibility for common client flows, not as a promise that every OpenAI endpoint or field is implemented.

## Example

Python OpenAI SDK:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/api",
    api_key="unused",
)

resp = client.chat.completions.create(
    model="your-local-model",
    messages=[
        {"role": "user", "content": "Say hello briefly."},
    ],
)

print(resp.choices[0].message.content)
```

## Verification

Build:

```text
zig build
```

Run e2e tests:

```text
cd e2e
uv sync
ANTFLY_INFERENCE_BIN=../zig-out/bin/antfly inference .venv/bin/pytest -q
```

Useful manual checks:

```text
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/api/models
```

Gemma 4 context-management qualification requires a running hardware-backed server and a concrete model/backend combination. The standard-library runner writes a JSON evidence artifact containing revisions, actual token counts, policy diagnostics, checkpoint lineage, and every asserted case:

```text
python3 scripts/gemma4/qualify_context_management.py \
  --base-url http://127.0.0.1:8080 \
  --model your-gemma4-e4b-model \
  --backend cuda \
  --server-pid SERVER_PID \
  --server-binary zig-out/bin/antfly-inference \
  --target-input-tokens 8192 \
  --max-tokens 64 \
  --runs 3 --qualification-mode \
  --max-p95-latency-ms APPROVED_LIMIT \
  --max-latency-cv APPROVED_LIMIT \
  --max-gpu-memory-bytes APPROVED_LIMIT \
  --max-host-rss-bytes APPROVED_LIMIT \
  --output zig-out/qualification/gemma4-e4b-context-cuda.json
```

Run its dependency-free validator tests with:

```text
python3 -m unittest scripts/gemma4/test_qualify_context_management.py
```

The CUDA stress runner also exercises near-8k greedy and sampled generation,
repeated checkpoint consumption, and buffered/streamed JSON-schema output:

```text
python3 scripts/gemma4/stress_context_cuda.py \
  --base-url http://127.0.0.1:8090 --model your-gemma4-e4b-model \
  --output ../../zig-out/qualification/context-cuda/stress.json
python3 -m unittest discover -s scripts/gemma4 -p 'test_*context*.py'
```

Both runners require the response to attest `exact_hybrid/v1`, the checkpoint
state, and the input/output reservations. Streaming evidence requires completed
text as well as metadata and `[DONE]`; the stress run reconstructs the streamed
JSON, checks its schema, and verifies the remembered fixture fact. A failed stress
run writes `evidence_passed: false` and its error before exiting unsuccessfully.
Use an optimized serving binary for latency measurements. Debug builds are useful
for functional checks, but their CPU tokenization, planning, and dispatch costs
are not representative performance evidence.

For native-checkpoint workload comparisons, use `qualify_matched_antfly.py`,
`qualify_matched_llama.py` and `compare_matched_latency.py` under
`scripts/gemma4/`. These consume a passing public checkpoint reference, compare
exact token IDs and generation settings, and keep full replay separate from
portable restore and resident reuse. Run them serially on an idle device.
The llama runner defaults to `--input-mode chat`; formatting and tokenization
are included on both servers. The comparator rejects pretokenized or unlabeled
historical runs as end-to-end evidence. An untimed template/tokenizer preflight
requires identical token IDs, and every timed response must attest zero prefix
reuse and matching output accounting. `--input-mode token_ids` remains available
only as a separate inference diagnostic. Use `--reference-binary` on the Antfly
runner to measure a saved qualified executable with an updated harness: the
binary hash must still match the passing reference, and its original source
provenance is recorded separately from the current harness checkout.
The [validation guide](../../../docs/design/inference/native-kv-compaction.md#reproducing-checks)
lists the maintained runners. Matching prompt counts alone is insufficient,
and measured latency does not substitute for answer-quality qualification.

Choose competitor baselines by the operation being measured:

- [llama.cpp's server](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md)
  exposes context shifting and KV-cache controls. It is the first CUDA baseline
  here because it can load the same GGUF artifact.
- [MLX LM](https://github.com/ml-explore/mlx-lm) documents rotating KV caches and
  prompt caching for Apple-silicon inference. Measure that baseline on the Metal
  test host and record any weight conversion.
- [Transformers cache strategies](https://huggingface.co/docs/transformers/kv_cache)
  cover dynamic/static caches, sliding-window layers, offloading, and quantization.
  A PyTorch comparison must specify the model implementation, cache strategy,
  attention implementation, and weight precision.

Evaluate structured summarization separately with fact retention, tool attribution,
checkpoint integrity, and total compaction cost. Cache eviction or context shifting
alone does not establish those properties. A competitive release needs both quality
evidence and matched-work latency/memory measurements; this runner does not assign
an unmeasured performance threshold.

Record the resolved CUDA prefill profile and chunk size. The shared Gemma tuning
wrapper defaults to `required-fast`; preserving the runtime's automatic selector
requires `ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT=1`. The promoted SM89 Flash route
currently requires one KV head and its qualified query/page/window geometry.
Gemma 4 E4B's two-KV-head workload is outside that contract, so E2B Flash promotion
evidence does not establish E4B long-context performance.

The 2026-10-17 L4 baseline used the same E4B Q4_0 GGUF, f16 KV, 8,177 prompt
tokens, 64 generated tokens, one warmup, and three measured alternating pairs.
The optimized Antfly `required-fast`/256-row path measured 38.78 s median HTTP
latency versus 4.86 s for llama.cpp b11669 with Flash attention (7.94x median
paired ratio). These measurements predate the compaction grammar changes and
cover prepared-history generation only. They establish a prefill performance
gap, not release p95 or compaction quality. Local artifacts are
`zig-out/qualification/context-cuda/comparison.json` and
`comparison-provenance.json`, including the exact binary/model hashes.

That baseline passed near-8k windowing and prepare/generate token-count parity,
but compaction rejected a truncated summary after 469 s. Subsequent diagnostics
exposed whitespace-only generation and invalid structured summaries. The branch
now has regression coverage for reusable grammar scratch buffers, bounded
compaction formatting, rejection of raw JSON string control bytes, and
position-aware token decoding. Gemma metaspace tokens must retain their leading
space inside an output: decoding each token as a new sequence had let the grammar
accept `" tool_evidence"` where the schema required `"tool_evidence"`. Both token
masking and grammar advancement now use the same initial/continuation byte table.
The regression also compares whole-output decoding for WordPiece and byte-level
BPE; SentencePiece byte-fallback contributions retain their raw bytes.

The corrected debug CUDA request returned a checkpoint (1,671 input tokens
reduced to 427), and checkpoint-only generation recovered the codename. Tampered
memory was rejected with HTTP 409. That initial diagnostic still failed quality:
tool facts copied call arguments instead of the tool result, and the 64-token
checkpoint SSE request ended with `length`. These failures remain recorded in
`compact-tokenizer-debug.json` and `checkpoint-tokenizer-debug.json`.

Two subsequent fixes retain small attributed tool results within the existing
budget and inherit GGUF stop-token IDs when a minimal HF sidecar omits them.
An explicitly configured HF stop policy remains authoritative. With the EOS fix,
the debug checkpoint response stops after nine tokens in both buffered and SSE
paths (`checkpoint-eos-debug.json`). The current native context/grammar suite
selected 128 tests: 120 passed and 8 skipped. Python context harnesses passed 26
tests. Debug CUDA measurements overlap compilation and must not be used as
release latency evidence. The final optimized candidate campaign passed
windowing, rendered-token parity, both fact checks, checkpoint-only recall,
digest-mismatch rejection, recompaction lineage, and streamed recall from the
child checkpoint (`candidate-final-cuda.json`). It reduced 1,671 input tokens
to 436, then recompacted to 399. Compaction/recompaction took 41.90/40.54 s;
checkpoint recall took approximately 1.2 s. The single campaign, including
model loading and with prefill profiling enabled, sampled peaks of 8.40 GiB
GPU allocation and 5.27 GiB host RSS. It is candidate evidence, not repeated-run
or competitive-performance qualification.

The larger 192-turn stress fixture passed greedy and sampled generation at
8,164 prepared input tokens, but failed conversation-fact retention after six
compaction batches. Its 13,825-token history became a 438-token active context
in 255.20 s. The tool result remained exact; the conversation summary lost the
original user fact and described the filler dialogue instead. This is a release
blocker, preserved with the failed checkpoint in `stress-final-cuda.json` and
memory/binary provenance in `stress-final-cuda-provenance.json`. Recall from the
tool result does not establish that conversation facts survived. A separate
sampled-recall diagnostic returned the wrong codename (`antfly`), recorded in
`stress-checkpoint-tail-final-cuda.json`. With the smaller checkpoint that did
pass fact retention, two sampled recalls plus buffered and streamed JSON-schema
answers passed (`checkpoint-schema-final-cuda.json`).

An optimized build with the tokenizer fix, before the EOS and tool-retention
changes, reproduced the baseline's exact 64-token output at 38.32 s warm HTTP
latency. A streaming CUDA profile attributed 25.22 s to attention and 3.86 s to
Q4 linear kernels (`prefill-profile-q4.json`,
`prefill-stream-profile-q4.json`). The latter recorded one successful request and
an immediate follow-up HTTP 503, so it is not repeat-run stability evidence.
The optional BF16 prefill-weight mirror experiment was rejected at admission:
its estimated backend peak was 30.93 GB, above both the configured budget and
the L4's physical memory. This is an estimate, not measured VRAM consumption
(`prefill-profile-bf16.json`). Public `select_v1` and bounded sampled/grammar KV
paths remain unqualified.

At `--max-concurrent-requests 1`, an immediate request after a completed SSE
response can receive a retryable HTTP 503 while the prior handler finishes
cleanup. The recorded response identifies `inference_admission` and advertises
`retry_after_ms: 1000`; three successive child-checkpoint streams passed with
that interval (`checkpoint-child-debug-repeated.json`,
`checkpoint-child-debug-idle.json`). This does not establish zero-delay stream
handoff or concurrent-request stability.

On the final optimized binary, repeated SSE requests with a one-second interval
preserved the baseline's exact output and used 8,177 input/64 output tokens;
the warm `required-fast` measurement was 39.54 s. The opt-in
`ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=mma` route measured 24.98 s warm on
the same token-count workload, but produced a different greedy continuation.
The active routes were MMA-M32 for HD256 and MMA for HD512. This one measured
pair suggests a performance opportunity, not numerical or model-quality parity,
and does not justify changing defaults. Artifacts are
`prefill-stream-profile-q4-final.json` and
`prefill-stream-profile-mma-final.json`, with token usage, output hashes, binary
hashes, and sampled process memory. Neither route is competitive with the
recorded llama.cpp baseline yet.

On an Apple Silicon development host, run the selected-storage kernel test from `zig/pkg/inference`:

```text
zig build test -Dmetal=true -Dcuda=false \
  -Dtest-filter="selected Metal GPU scorer" --prefix ../../zig-out
```

This test checks GPU scores against the CPU oracle across supported KV formats, transactional row repacking, and causal attention output over noncontiguous absolute positions. It must pass on Metal; a skipped host test is not qualification evidence. The internal `select_v1_qualification_override` and `exact_hybrid_sampled_ring_qualification_override` switches are for controlled qualification harnesses and are disabled by default.

For CUDA, the runner queries `nvidia-smi` for the device UUID, name, compute capability, driver, and total memory. It samples only `--server-pid`, using `nvidia-smi` for that process's GPU allocation and `/proc` for host RSS. Qualification requires explicit `--max-gpu-memory-bytes` and `--max-host-rss-bytes` limits; the example deliberately leaves their values to the release owner. The intended first campaign is Gemma 4 E4B with an 8192-token input target and at most 64 decoded tokens on the recorded NVIDIA L4. This describes the test scope and does not claim that the campaign has passed.

Check model admission before collecting timings. Automatic resource budgets can
be lower than the memory available on a discrete GPU. If admission reports
`MODEL_RESOURCE_LIMIT`, inspect the logged peak estimates and limits, then set
the serving `--host-budget-mb`, `--backend-budget-mb`, and `--combined-budget-mb`
for the test machine. Record those settings alongside the evidence; admission
budgets and measured peak-memory release limits serve different purposes.

Metal remains supported. A Metal qualification instead supplies `--hardware-evidence`, `--memory-samples`, and `--max-peak-phys-footprint-bytes`. The hardware JSON uses schema `antfly.macos_hardware_evidence.v1`, a measured `device.name`, and the serving `server_pid`. Run `scripts/sample_macos_process_memory.py` against that PID before the campaign and leave it running through completion. Both backends require `--server-binary`, at least three runs, and explicit `--max-p95-latency-ms` and `--max-latency-cv` limits. The p95 includes every request. The stability limit applies separately to matching model-executing compaction, recompaction, buffered generation, checkpoint generation, and checkpoint streaming groups; fast bookkeeping and the expected 409 rejection cannot dominate the CV. The runner discards no warm-up observations. Run a separate non-qualification candidate campaign first when the release procedure requires warming caches or kernels.

Successful managed responses expose `X-Antfly-Inference-Backend`, `X-Antfly-Inference-Route`, and `X-Antfly-Inference-Pid`. The runner uses these server observations, rather than the requested backend, to bind the HTTP evidence to the sampled process. It records the tested binary hash and the source snapshot, including uncommitted changes. Missing evidence fails qualification mode.

Any passing `qualified` field is scoped to the recorded context API campaign with `exact_hybrid/v1`, model revisions, binary, and device. CUDA currently retains full exact KV history for this path. Public `select_v1` remains unavailable, and bounded sampled or grammar ring execution remains unqualified and disabled by default. The artifact does not qualify those paths, unexercised kernels, E2B, or any model other than the recorded E4B revision. Those release gates require separate long-context, kernel parity, performance, and failure-recovery runs.

Failed campaigns retain the partial run, checkpoint, and request timings in the artifact. A failed fact-retention assertion remains a failed functional gate even when compaction returned HTTP 200.

A passing artifact is candidate evidence for the recorded model revisions and backend. It does not by itself declare broad production qualification. Release qualification also requires review of the artifact on the intended hardware, repeated-run stability, latency and memory limits, and the repository's other Gemma 4 quality and backend gates.

## Files Involved

- `src/pipelines/generation.zig`
- `src/server/server.zig`
- `../../../specs/openapi/inference/api.yaml`
- `../../e2e/inference/test_generate.py`
- `../../e2e/inference/test_models.py`
- `../../e2e/inference/test_chat.py`
- `../../e2e/inference/test_health.py`
- `../../e2e/inference/test_openai_compat.py`
