# Multi-request CUDA generation batches

The cooperative CUDA scheduler can combine two ready decode requests into one
model forward. Each request contributes one token and retains its own position,
KV storage, admission lease, cancellation lifetime and output stream. Prefill
chunks retain singleton turns, avoiding padding a decoder token to a long
prefill's query length.

The shipped read-pool configuration enables both controls. Equivalent settings:

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
      "max_decode_wait_us": 0,
      "max_idle_prefill_chunk_size": 512
    }
  }
}
```

The read service batches eligible requests automatically; callers need no
batching flag. A zero batch-fill wait combines work already queued without
delaying a ready decoder for a peer. The general runtime defaults outside this
deployment profile remain off. Cooperative execution with row batching off
continues to interleave one request per forward. Row batching alone does
not opt a model into cooperative sharing. The current envelope remains two
active requests and two decode rows. Qualified F16 prefix-cache requests in
`simple` or `block_hash` mode can share these rows, including streaming requests.
CUDA radix caching remains disabled. Other KV dtypes, graph,
speculative, selected-KV and multimodal requests retain exclusive ownership.

The backend declares row eligibility from its eager resident weight inventory
and available kernels. The current implementation targets dense Gemma 4 E4B QAT
Q4_0 with its Q6_K output head; the declaration honors tied token embeddings.
An unsupported checkpoint or missing quantized output-head kernel retains a
one-row capability. Requests exceeding that declaration receive
`UNQUALIFIED_COOPERATIVE_BATCH`.

The numerical performance receipts below predate mandatory model-artifact
hashes. They are historical measurements, not reproducible controlled evidence
under the updated harness. Fresh campaigns now record and compare manifest,
configuration, tokenizer and weight hashes, and reject legacy reports without
them. This does not establish that the earlier model files differed. The
ready-only two-row deployment remains an explicit latency/throughput tradeoff;
its reported 2.2–2.8% throughput gain does **not** pass the 10% promotion gate.

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
The two-row Q6_K/Q8_1 output head reads each weight tile once for both rows
while preserving each singleton's integer products, scaling and reduction
order. Its standalone PTX targets CUDA 13.0 and compute capability 8.0 or later;
unsupported layouts/drivers retain the per-row projection. The hardware test
compares every logit bit for bit against independent singleton kernels.
`ANTFLY_INFERENCE_CUDA_E4B_PAIRED_HEAD=0` restores those kernels for controlled
comparisons. The backbone remains one batched forward. Other checkpoints do not acquire this
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
remained an explicit experiment after this campaign. The read deployment now
selects ready-only batching, as requested, without changing these historical
gate results. Functional qualification is not reported as production performance
qualification.

There are several limits on this initial speedup. Only the overlapping decode
portion can share a forward; the 512-token request continues alone after the
256-token peer finishes, and prefill stays singleton. The output head still
projects each row separately to preserve the canonical quantized result. Each
cohort drains and invalidates backend ownership: the streaming pair records
763 owner resets versus 297 in the one-row control (buffered: 773 versus 264).
Those counters identify extra state transitions, not their GPU time cost.
The original campaign also allowed a 1 ms batch-fill delay. The current read
configuration removes that deliberate delay, while retaining the drain and
ownership checks. A shared GPU and these counters do not isolate how much each
factor contributes to the measured embedding tail.

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

## Read deployment with no batch-fill wait

The read deployment now enables cooperative execution and two decode rows in
`configs/inference-read-pool-40g.json`. `max_decode_wait_us: 0` combines ready
requests without holding a decoder to fill a batch. Callers need no request
flag. Global defaults for other configurations are unchanged; disabling row
batching remains an explicit rollback to one-row cooperative execution.

The controlled repeat uses the same `0faa7f34...` executable and 512/256-token
workload above. Both controls have zero batch-fill wait and differ only in row
mode and limit. Exact streaming/buffered references, two seven-request queue
turnover rounds, stable model IDs, cancellation and healthy-peer recovery pass.
Mandatory managed-memory and service-NVML evidence is complete in every phase,
with no sampling failures. Two-row managed peak is 11,233,105,168 bytes and NVML
peak is 11,586,879,488 bytes, below the unchanged 38 GiB guard.

| Client-observed metric | One row | Ready-only two rows |
| --- | ---: | ---: |
| Streaming aggregate output tokens/s | 31.84 | 32.54 |
| Buffered aggregate output tokens/s | 31.81 | 32.70 |
| Short first content | 1.68 s | 1.69 s |
| Short streaming completion | 14.22 s | 13.58 s |
| Short buffered completion | 14.23 s | 13.93 s |
| Embedding p95 | 26.53 ms | 44.32 ms |
| Decide p95 | 72.08 ms | 73.16 ms |

Both throughput gates still fail: gains are 2.2% / 2.8%, below 10%. All
request-latency and smaller-model gates pass, including the embedding relative
gate at 1.67 times. The deployment selects this latency-oriented batching policy
without claiming that throughput promotion passed. Embedding median is nearly
unchanged (11.65 / 11.89 ms); its higher tail occurs while sharing the GPU with
Gemma. That is consistent with contention from wider generation steps, but is
not an isolated measurement of GPU scheduling or memory-bandwidth effects.

Reports are `ready-control/`, `ready-candidate/` and
`ready-performance-gates/batching-gates.json` in the same evidence directory.

The ready-only profile also passes a streaming decode-overlap regression with
31,332-token and 1,956-token prompts, using the existing independent references.
The short request arrives after the long request starts decoding. Twelve real
two-row steps execute during that phase; both outputs match their references.
Long completion is 88.13 seconds, short completion 11.13 seconds after arrival,
and short first content 4.21 seconds. Disconnect/healthy-peer recovery and
complete per-phase memory evidence pass. This report is `ready-long-overlap/`;
it is a functional long-context check, not a paired performance comparison.

Ordinary streaming and buffered HTTP requests also pass exact independent
references with no backend, cache-dtype, prompt-cache or batching flags. They
execute 56 real two-row steps and return active work and KV usage to zero.
The receipt is `ready-api-checks.json`.

## GLiNER batching is a separate path

The authenticated FP32 and Q8 DeBERTa GLiNER 2.5-Decide artifacts support native CUDA
classification batches of up to eight compatible inputs through
`POST /ai/v1/extract` with `schema_version: 2`. The span executor packs those
inputs into a padded encoder/classifier forward, subject to the checkpoint's
512-token per-sequence and aggregate resource limits. It is independent of
Gemma generation batching and needs no generation-batching flag.

Eligible concurrent CUDA classification calls now also enter the Node-owned
executor broker automatically, through both `/extract` and `/decisions`.
The typed `/decisions` API accepts either one `input` or an `inputs` array;
results retain caller order and each caller's schema and presentation options.
The read-pool model directory selects the authenticated Q8 split bundle.
EmbeddingGemma 2 CUDA text calls use the same broker with a separate
four-row, 8,192-padded-token capability and bounded length buckets. See [HTTP batching ownership and qualification](read-pool-http-batching.md)
for the exact eligibility limits and numerical contract. Other GLiNER
architectures, artifacts and backends retain their own batching contracts;
the boundary extractor's discovery does not declare Decide's classification contract.

The live read service also passes an eight-input classification request with
distinct texts, preserving order and exactly matching singleton results
(maximum numeric difference zero). A ninth input receives HTTP 413 before
execution, consistent with this read profile's eight-item request limit. The
same `ready-api-checks.json` receipt records this existing GLiNER capability.
