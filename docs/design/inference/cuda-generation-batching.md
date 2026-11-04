# Multi-request CUDA generation batches

The cooperative CUDA scheduler can combine two ready decode requests into one
model forward. Each request contributes one token and retains its own position,
KV storage, admission lease, cancellation lifetime and output stream. Prefill
chunks retain singleton turns, avoiding padding a decoder token to a long
prefill's query length.

Enable both controls explicitly:

```json
{
  "inference": {
    "cooperative_execution": {
      "mode": "on",
      "max_active_requests": 2
    },
    "generation_batching": {
      "mode": "on",
      "max_step_items": 2,
      "max_decode_wait_us": 1000,
      "max_idle_prefill_chunk_size": 512
    }
  }
}
```

Both controls remain off by default. Cooperative execution with row batching
off continues to interleave one request per forward. Row batching alone does
not opt a model into cooperative sharing. The current envelope remains two
active requests and two decode rows. Graph, prompt-cache, speculative,
selected-KV and multimodal requests retain exclusive ownership.

The backend declares row eligibility from its eager resident weight inventory
and available kernels. The current implementation targets dense Gemma 4 E4B QAT
Q4_0 with its Q6_K output head; the declaration honors tied token embeddings.
An unsupported checkpoint or missing quantized output-head kernel retains a
one-row capability. Requests exceeding that declaration receive
`UNQUALIFIED_COOPERATIVE_BATCH`.

## Cohort ownership and numerical policy

The execution scope owns the lane, mutex, borrowed aggregate budget, decode
regime and final GPU drain. The cohort budget includes both requests' admitted
KV and scratch, counts shared model weights once, preserves the tightest owner
limits, and limits arena growth to admitted scratch. It does not mutate either
request's budget or replace the process-wide physical allocation guard.

A claimed cohort drains before any row publishes ready, including on failure.
Cancellation is checked for each row after the shared GPU work drains. A
cancelled request can retire while its healthy peer consumes the peer's own
result. A stream that cannot drain poisons the worker and requires restart;
releasing borrowed KV pointers after an unconfirmed drain is unsafe. Cohort
retirement invalidates remembered per-request backend ownership, so the next
request re-enters through the normal state transition. Ordinary consecutive
singleton acquisitions retain their existing same-owner reuse.

The scope distinguishes decode cohorts from prefill matrices. A two-row decode
must not select BF16 prefill mirrors or prefill tensor-core arithmetic solely
because its matrix has multiple rows. The quantized gate/up and output-head
paths retain decode arithmetic. Cooperative CUDA decoding uses the same
full-logits path while alone and shared, including at temperature zero. A peer's
arrival or retirement must not switch
between a greedy shortcut and the canonical sampled projection. Cooperative
requests therefore continue through the scheduler even when only one owner is
active; batching can change the number of rows without changing this policy.
The output head uses the singleton quantized Q6_K projection for each head row; the backbone
remains one batched forward. Other checkpoints do not acquire this
numerical qualification through matching architecture dimensions alone.

These contracts extend the lane, admission and failed-free recovery invariants
in [cooperative execution ownership](cooperative-execution.md).

## Reproducing the checks

The focused test target covers scheduler packing and fairness, budget ownership,
execution-scope retirement, per-row cancellation, numerical routing and backend
capability fences without loading a model or requiring a GPU:

```bash
cd zig/pkg/inference
zig build test-execution-batch -Dcuda=false -Dmetal=false --summary all
zig build test-execution-batch -Dcuda=true -Dmetal=false --summary all
```

The target also runs in the default inference test graph. The measured focused
build peaked at approximately 1 GiB; its build-scheduler reservation is 2 GiB.

Use `qualify_cooperative_execution.py --mode cooperative --rows 1` for the
control and `--rows 2` for the candidate. Supply `--binary`, `--config`,
`--cgroup` and an output directory. Run both modes on the same executable,
model inventory, GPU and runtime environment, changing only the row-batching
mode and row limit. Supply the first run's `references.json` to the second.
Compare the directories with:

```bash
python3 zig/pkg/inference/scripts/qualify_cooperative_execution.py \
  --compare-batching ROW1_OUTPUT ROW2_OUTPUT --output COMPARISON_OUTPUT
```

The harness verifies real two-row steps, independent output references in both
response modes, queue turnover, exclusive transitions, healthy-peer output
after streaming disconnect, idle KV recovery and complete memory evidence.
Comparison requires at least 10% more aggregate output throughput, no more than
25% regression in either request's completion or short-request first content,
and embedding/Decide p95 within twice the control and below one second.
These are experiment gates, not production SLOs.

## Qualification record on GB10

The optimized production HTTP server was built with
`zig build http-server -Dcuda=true -Dmetal=false -Doptimize=ReleaseFast
-Dstrip=true -Dcpu=baseline+sha2`. Its SHA-256 is
`0faa7f347073e372f32161df925afff5e52ab9a73bf2e4a743872f564cf83206`.
Both modes use that exact executable and configuration apart from the batching
switch and row limit. Thinking is disabled and temperature is zero. Independent
warehouse and garden prompts have 526 and 408 input tokens, with different
verification codes and fixed 512/256-token output limits.

Both modes pass exact independent output references in streaming and buffered
responses, two rounds of seven queued/shared/exclusive requests, idle recovery,
and stable resident model IDs. The two-row campaign additionally disconnects a
stream after observing an executed two-row step; its healthy peer finishes with
its exact reference and both active work and transient KV return to zero.

| Client-observed metric | One row | Two rows |
| --- | ---: | ---: |
| Streaming aggregate output tokens/s | 31.84 | 34.00 |
| Buffered aggregate output tokens/s | 31.85 | 33.59 |
| Short first content | 1.68 s | 1.70 s |
| Short streaming completion | 14.21 s | 15.78 s |
| Short buffered completion | 14.21 s | 15.88 s |
| Long streaming completion | 24.01 s | 22.35 s |
| Long buffered completion | 24.00 s | 22.64 s |
| Embedding p95 | 23.84 ms | 50.23 ms |
| Decide p95 | 70.22 ms | 72.92 ms |

**Performance promotion fails.** Throughput improves 6.8% / 5.4%, below the
predefined 10% gain gate. Embedding p95 rises 2.11 times, exceeding the two-times
relative gate despite remaining below 51 ms. Short completion rises 11–12%,
within its 25% gate. All absolute smaller-model latency gates pass. Batching
therefore remains an explicit experiment, and the selected read profile keeps
one-row cooperative execution. Functional qualification is not reported as
production performance qualification.

The one-row campaign has 614 valid memory samples and the two-row campaign has
646, with no sampling failures and complete evidence in every workload phase.
Two-row managed CUDA peak is 11,229,959,440 bytes and service NVML peak is
11,582,685,184 bytes; these overlap and must not be added. Both remain below the
38 GiB managed-device guard. This campaign does not exercise exhaustion at that
ceiling; the separate scaled guard/recovery test remains the pressure evidence.

The separate two-row cancellation campaign also passes: two active requests,
twenty queued probes, six retryable 503 responses and fourteen retryable 504
deadlines. Waiting depth peaks at 14 while active KV stays exactly
1,535,115,264 bytes. Active outputs match their references, and exclusive-cache,
normal-generation and post-disconnect requests recover successfully. Its raw
report is `production-cancellation/` under the same evidence directory.

The final binary also passes the original long-context regression in one-row
mode: a 31,332-token prompt alongside a 1,956-token prompt, with exact independent
streaming and buffered references. Long/short completion is 87.13/36.58 seconds
streaming and 87.60/29.77 seconds buffered. Memory evidence is complete in every
phase, with no sampling failures. The report is `read-long-final/`.

The final binary also passes the write-pool regression with all four CUDA
models resident: EmbeddingGemma 2, GLiNER 2.5, Whisper small and LightOnOCR-3
0.8B. All 102 expected-success requests pass across steady traffic, bursts,
windowed audio, eight-page OCR and recovery; limit/deadline checks also pass.
All 138 memory samples are valid, cover every workload phase and include
service-specific NVML evidence. Resident model IDs remain stable. The report
is `write-final/`; its synthetic fixtures establish functional regression
coverage, not broad ASR or OCR accuracy.

Raw reports are `.benchmark-results/request-batching/production-row{1,2}/` in
the qualification worktree. The failed promotion decision is preserved in
`production-performance-gates/batching-gates.json`. Preliminary and debug
candidates are recorded separately and are not used for these performance
claims.

The focused CPU and CUDA-enabled builds each pass 82 tests. Eight telemetry
unit tests, license headers and the Apache product-boundary check pass.
All serving measurements use the shared GB10 machine; they do not establish
A100 performance or production SLOs. Remote PR CI still requires its normal
maintainer approval.
