# Automatic HTTP batching for the read pool

Ordinary CUDA text requests for EmbeddingGemma 2 and the authenticated DeBERTa
GLiNER 2.5-Decide artifacts enter the existing Node-owned executor broker.
Callers need no batching flag or array input. Gemma 4 generation continues to
use its separate cooperative token scheduler. All three share the process
allocation guard; batching does not change the 38 GiB read-profile ceiling.

## Compatibility and ownership

`executor_microbatch.zig` owns the shared broker, while `tensor_microbatch.zig`
combines compatible tensor forwards. `gliner_microbatch.zig` handles GLiNER
classification budgets and results, boundary compatibility keys, and per-request
replies. `output_owner.zig` retains shared output admission until the last consumer
releases it. Read/write serving profiles remain deployment configuration.

Embedding text groups have at most four rows and 8,192 bucket-padded tokens in
aggregate. Each input may use the full 8,192-token model context, including task
prefixes and special tokens. The resident CUDA backend declares this capability
only for its default arithmetic policy. Nearby lengths share explicit attention
masks in quarter-step length classes; input padding introduced by grouping is at
most 25%. Kernel tile rounding is covered by existing workspace admission.

| Bucket width | Maximum rows in one cohort |
| --- | ---: |
| Up to 2,048 tokens | 4 |
| 2,560 tokens | 3 |
| 4,096 tokens | 2 |
| Above 4,096 through 8,192 tokens | 1 |

The aggregate token ceiling bounds a model turn alongside the other read models;
actual scratch admission can reduce the physical cohort further. Only a resource
refusal before GPU dispatch authorizes subdivision. Kernel, physical allocation,
cancellation and deadline failures are propagated without replay. The encoder
arithmetic is unchanged.

Groups require the same loaded model generation, task prefix/instruction and
backend. Per-request limits are checked against actual token counts; bucket
padding is charged to the cohort rather than reported as input usage. Output
vectors retain the full model dimension until each caller applies its requested
MRL dimension and normalization. Strict token counting includes model special
tokens; usage cannot silently fall back to an estimate. Mixed media, ordered
content groups, tracing and per-item-error routes retain their existing paths.
The generic multimodal capability remains conservative; this does not advertise
native batching for every content group. Arrays retain bounded four-ticket
submission waves and restore the caller's original output order.

GLiNER classification groups have at most eight prepared rows and compatible
length buckets, authenticated model generation, encoder configuration and budget
limits. Both the FP32 and Q8 DeBERTa artifacts retain their existing exact
qualification gates. This machine's read-model directory selects the qualified
Q8 split bundle. Other architectures, unqualified artifacts, extraction tasks
and split/windowed routes retain their existing execution contracts.

Each GLiNER caller owns its tokenizer plan, schema, presentation options and
response. A shared padded encoder/classifier forward produces separately owned
raw logits; presentation runs back on each caller after GPU work drains. Different
schemas and label counts can therefore share model execution without borrowing
another caller's interpretation. `/extract` and `/decisions`, including their
existing multi-input forms, use this boundary.

Both adapters reuse the broker's bounded collection window (200 microseconds
by default), per-item cancellation and deadlines, and synchronous borrowed-input
lifetime. No new thread pool or cross-pod service is introduced. Queued callers
retain host preparation, not a duplicate GPU workspace. Embedding cohorts use
the existing fair model lane. GLiNER direct and broker executions share a fair
lane before workspace admission and the model mutex.

A GLiNER cohort charges every padded row's conservative workspace estimate.
Capacity failures subdivide the cohort before any forward; a failed GPU forward
is never replayed. After execution, its peak admission reduces to an independently
owned host-output lease. The last broker consumer releases that lease after
copying its results. A canceled caller cannot release a healthy peer's output.
The process-wide DeviceBuffer guard remains the physical allocation backstop.

## Evidence and reproduction

`test-executor-microbatch` covers broker compatibility, bounded waves,
queued/claimed cancellation, deadlines, caller-local presentation, complete
cohort budgets and output retention through allocation failures. It runs in the
default inference unit graph and requires neither a model nor a GPU:

```sh
cd zig/pkg/inference
zig build test-executor-microbatch -Dcuda=false -Dmetal=false --summary all
zig build test-executor-microbatch -Dcuda=true -Dmetal=false --summary all
```

The HTTP campaign independently observes native batches for embeddings,
classification extraction and typed decisions. Singleton references preserve
response identity, schema, output ordering, dimensions and usage. Embedding
checks require finite unit vectors, norm error at most 0.0001, cosine similarity
at least 0.9999 and maximum coordinate error at most 0.005. Response metadata,
usage and ordering must match exactly. Classification numeric tolerance is 0.005.

The initial 0.002 coordinate probe failed: BF16 batch shapes produced differences
around 0.0024. The existing explicit native batch path also varies from singleton
execution. A projection-policy experiment did not remove this variation and was
removed; this change preserves existing CUDA arithmetic. The final campaign adds
a separate control captured from the previous executable: equal-length four-row
native batches must agree within 0.00001. A four-topic retrieval fixture must
preserve all four top results with score drift at most 0.005. The failed strict
probe receipts remain part of the local evidence; they are not passing results.

```sh
python3 zig/pkg/inference/scripts/qualify_read_pool_microbatch.py \
  --capture-native-references --binary CONTROL_BINARY \
  --config LOCAL_READ_CONFIG --cgroup SERVICE_CGROUP \
  --output .benchmark-results/read-pool-microbatch/native-control

# Restart the owned service with SERVING_BINARY and the same configuration.
python3 zig/pkg/inference/scripts/qualify_read_pool_microbatch.py \
  --native-references .benchmark-results/read-pool-microbatch/native-control/native-references.json \
  --binary SERVING_BINARY --config LOCAL_READ_CONFIG --cgroup SERVICE_CGROUP \
  --output .benchmark-results/read-pool-microbatch/qualification
```

The campaign also exercises smaller-model requests alongside two Gemma
requests, deadlines, client disconnects and recovery. It requires complete
managed-memory and service-specific NVML evidence in every phase. Missing
telemetry produces incomplete qualification. Use `--baseline` with a pinned
control executable to record the same workload without requiring new native
batch counters. Model-specific phases keep Gemma's batches from standing in for
evidence that embeddings or GLiNER actually share forwards.

`antfly_inference_executor_microbatch_native_batches_total` counts executor-
reported native groups, which may contain bounded physical sub-batches;
`native_items_total` counts their completed rows. `groups_total` also includes
singleton work and is not evidence of native batching. `canceled_items_total`
counts broker items removed before execution. These counters are distinct from
Gemma's generation scheduler metrics.

Native controls and candidate campaigns now include SHA-256 identities for all
loaded model artifacts, checked again at completion. The controls must be newly
captured with this harness; legacy receipts without hashes cannot be reused.

## DGX Spark measurements

The historical native-arithmetic campaign below predates mandatory model-artifact
hashes and cannot be re-compared by the updated harness. Its executable has SHA-256
`24fc2e85d16ff98ee707c89811648401cbe3fee150569f0034ea1bfeb9bb11aa`.
Its control is the previous serving executable, SHA-256
`0faa7f347073e372f32161df925afff5e52ab9a73bf2e4a743872f564cf83206`.
Both use the same GB10 GPU, pinned model files, configuration and runtime
environment. The read configuration enables two Gemma owners and ready-only
two-row decode, and enforces a 38 GiB managed allocation guard.

Three isolated 16-request bursts per route produced these median p95 values.
Wall time is the median time to complete each complete burst, including HTTP
and metric collection overhead. This is a descriptive comparison on shared
Spark hardware, not a production throughput or universal tail-latency guarantee.

| Route | Control p95 | Broker p95 | Control burst wall | Broker burst wall |
| --- | ---: | ---: | ---: | ---: |
| EmbeddingGemma 2 `/embed` | 67.93 ms | 54.77 ms | 73.24 ms | 59.99 ms |
| GLiNER Decide `/extract` | 393.59 ms | 156.73 ms | 398.64 ms | 162.54 ms |
| GLiNER Decide `/decisions` | 570.67 ms | 315.61 ms | 574.57 ms | 321.73 ms |

Every candidate burst observed native groups of at least two rows. The campaign
also passed multi-input ordering, task and dimension options, long embedding
broker admission, request ceilings, mixed Gemma traffic, deadlines, disconnected clients,
recovery to zero active requests/KV reservations, and unchanged model identities.
Every workload phase had managed and service-specific NVML samples, with no
sampling failures. Service NVML peaked around 9.85 GiB; this campaign does not
exercise physical exhaustion near 38 GiB.

Equal-length four-row embedding controls matched the old native path exactly
(maximum coordinate error zero, tested at 128 and 256 dimensions). The retrieval
fixture preserved all four top results with maximum score drift 0.00232.
This small fixture is a regression check, not a broad retrieval-quality evaluation.

Mixed-load tails remain a limitation: with two Gemma generations active, typed
`/decisions` reached 1.80 s versus 1.15 s in the control. Embedding and extraction
maxima improved from 36.81 to 20.05 ms and 464.37 to 192.80 ms respectively;
the two-generation completion maximum was 9.27 s versus 9.19 s. This campaign
does not pass or claim a universal one-second typed-decision latency gate.

Local receipts in the qualification worktree are under
`.benchmark-results/read-pool-microbatch/`: `baseline/`, `native-control/`,
`final/`, `final-evidence/`, and `burst-comparison.json`. The repeated
`final-evidence/` campaign also retains 81 per-response embedding agreement
measurements: minimum cosine 0.999950 and maximum coordinate error 0.002065. Failed strict probes remain in
`first-campaign/`, `qualified-v3/`, and `qualified-v4/`; the last is the removed
projection-policy experiment. Only the final executable described above is
qualified by the passing final campaign. No A100 hardware claim follows from
these GB10 measurements.

The same executable passed the long-context Gemma regression at 31,332 versus
1,956 input tokens and 64 versus 32 output tokens. Streaming and buffered results
matched independent references, and each short request completed before its long
peer while the long owner progressed. Streaming short first content was 5.27 s;
short completion was 35.38 s versus 88.79 s for the long request. Buffered short
completion was 40.99 s versus 90.74 s. Peer-disconnect recovery and mandatory
memory evidence passed. Receipts are in `final-long-mixed/`.

The focused broker/ownership target passed 32 tests in both CPU and CUDA builds;
the Gemma execution-batch target passed 82 CUDA tests. Zig formatting, Python
syntax, license headers and the Apache dependency boundary also passed.

The same binary also passed the write-pool regression: 102 successful requests
across EmbeddingGemma 2, boundary GLiNER 2.5, Whisper small and LightOnOCR-3 0.8B,
including mixed bursts, explicit batches, sparse windowed audio, eight-page OCR,
request limits, silence, OCR deadlines and recovery. All eight phases had valid
managed-memory and service NVML evidence with no sampling failures. This excludes
the previously failing repeated-sentence audio stress fixture, as does the
existing default write qualification. Receipts are in `final-write/`.

After qualification, the owned read service was restored with the final binary,
the 38 GiB guard, cooperative execution and ready-only two-row Gemma batching;
the owned write service was stopped. `make gc` completed, conservatively skipping
caches reported as active by the repository cleanup script.

## Artifact-aware harness validation

The updated harness was exercised against the existing `24fc2e85...` serving
executable, with newly captured native controls from `0faa7f34...`. Both receipts
record matching content identities for all 27 files across the three loaded
models and revalidate them at completion. The full read campaign passed native
batch observations, numerical/native controls, retrieval, request limits, mixed
Gemma traffic, cancellation/disconnect recovery and mandatory memory evidence.
Receipts are `review-artifacts-control/` and `review-artifacts/` under the same
local evidence root. This validates the new evidence capture on that existing
serving binary; the CUDA lifecycle fix is independently exercised through the
new production engine-host executable documented in
[cooperative execution](cooperative-execution.md#physical-cap-and-recovery-evidence-on-gb10).
No new serialized-versus-cooperative or one-row-versus-two-row performance
promotion is claimed. Eleven artifact-identity tests and eight memory-evidence
tests pass, including rejection of changed weights at the same path/size/mtime,
changed manifests/tokenizers, missing hashes, linked-shard cycles, files changing
during hashing, and model reloads during capture.

## Document-sized CUDA cohorts

The widened path removes the earlier short-query eligibility fence and exact-length
requirement. It was compared with the previous serving executable
`24fc2e85d16ff98ee707c89811648401cbe3fee150569f0034ea1bfeb9bb11aa` on the same
GB10, configuration, environment and immutable 27-file model inventory. The
candidate executable is
`808cf602544d619ad3b5723f3c962e441f95c9eca6ed007d32623c03c83cf42d`.
The control predates the latest-main merge; this is a shared-machine serving
comparison, not an isolated kernel benchmark.

Each length class has eight distinct documents of slightly unequal lengths,
repeated across three concurrent bursts. Values below are median burst p95
latencies, including queueing for all eight requests. They are not single-request
service times or peak GPU rates.

| Input class | Control p95 | Candidate p95 | Aggregate throughput ratio |
| --- | ---: | ---: | ---: |
| 128 tokens | 77.36 ms | 50.09 ms | 1.43x |
| 512 tokens | 252.37 ms | 202.16 ms | 1.25x |
| 1,024 tokens | 556.27 ms | 495.26 ms | 1.12x |
| 2,048 tokens | 1,463.71 ms | 1,478.59 ms | 0.99x |
| 4,096 tokens | 4,713.30 ms | 4,778.86 ms | 0.99x |
| 8,192 tokens | 17,539.34 ms | 17,960.60 ms | 0.98x |

Every 128–4,096-token burst observed native batches. The 8,192-token class stayed
singleton under the aggregate token ceiling. A separate three-row document case
also passed. All predefined document gates pass: median p95 at most 1.25 times
the control plus a 10 ms noise allowance, and throughput at least 0.90 times the
control. Longer cohorts are supported without claiming a throughput gain where
none was observed.

The campaign validates 256 embedding comparisons, with minimum cosine 0.999964
and maximum coordinate difference 0.001855. MRL dimensions, multi-input ordering,
usage, exactly-at-limit inputs and over-limit rejection pass. Four-topic retrieval
uses the actual vectors recorded from concurrent document bursts, not singleton
document replays: top results remain unchanged at all lengths, with maximum
score drift 0.001345. This synthetic fixture is a regression check rather than a
broad retrieval-quality benchmark.

With two Gemma generations and GLiNER traffic active, document p95 was 3.34 s
versus 3.56 s in the control, typed-decision p95 was 1.70 s versus 0.95 s, and
Gemma completion p95 was 7.83 s versus 7.47 s. These pass the predefined mixed
limits (2x plus 50 ms for smaller models, 1.25x plus 50 ms for generation).
They do not establish a universal sub-second decision SLO. In a separate
interactive-query turnover fixture, eight short queries completed before the
2K-document backlog drained, at 744 ms p95 against a 1,500 ms fixture gate.

Deadlines, disconnected clients, healthy-peer recovery, zero remaining active
requests/KV reservations and unchanged loaded-model identities pass. All workload
phases have valid managed and service NVML evidence: 537 samples, no sampling
errors. Managed CUDA peaks at 10.25 GiB and service NVML at 10.60 GiB; these
measurements overlap and must not be added. The 38 GiB managed ceiling is unchanged,
and this campaign does not exercise physical exhaustion near that ceiling.

The complete short read-pool regression passes with the widened path, including
both GLiNER APIs and native embedding controls. The same candidate passes the
102-request write-pool regression, with valid evidence in all eight phases.

Raw receipts are under `.benchmark-results/embedding-document-batching/` in the
qualification worktree: `control/`, `final/`, `short-read-regression/`, and
`write-regression/`. `final-batched-retrieval.json` derives the retrieval check
from the preserved `final/requests.jsonl` vectors and records that file's hash.
The updated harness performs this burst-vector check directly in subsequent runs.
The initial `candidate-v1/` campaign also passed; its performance numbers are
separate observations and are not substituted for the final table above.

```sh
# Run against the previous executable first, then restart with the candidate.
python3 zig/pkg/inference/scripts/qualify_embedding_document_batching.py \
  --binary CONTROL_BINARY --config LOCAL_READ_CONFIG --cgroup SERVICE_CGROUP \
  --output .benchmark-results/embedding-document-batching/control
python3 zig/pkg/inference/scripts/qualify_embedding_document_batching.py \
  --binary SERVING_BINARY --config LOCAL_READ_CONFIG --cgroup SERVICE_CGROUP \
  --control .benchmark-results/embedding-document-batching/control \
  --output .benchmark-results/embedding-document-batching/final
```

The focused read-model suite passes 42 tests in CPU and CUDA builds, including
real admission refusal with an in-memory controller, safe cohort subdivision,
non-replay of backend failures, padding bounds, legacy fallback compatibility,
and heterogeneous-array ordering. Its cold build peaks around 1 GiB. The
82-test generation batching suite and seven allocation-lifetime tests also pass
in both configurations. Nineteen shared artifact/telemetry tests and four
recorded-retrieval tests pass, together with formatting, license headers and the
Apache boundary check. Runtime changes preserve existing CUDA arithmetic; later
qualification edits only added tests and evidence handling.
