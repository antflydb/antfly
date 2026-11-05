# Automatic HTTP batching for the read pool

Ordinary CUDA text requests for EmbeddingGemma 2 and the authenticated DeBERTa
GLiNER 2.5-Decide artifacts enter the existing Node-owned executor broker.
Callers need no batching flag or array input. Gemma 4 generation continues to
use its separate cooperative token scheduler. All three share the process
allocation guard; batching does not change the 38 GiB read-profile ceiling.

## Compatibility and ownership

Embedding text groups have at most four rows, each at most 64 tokens including
the task prefix and model special tokens. The resident CUDA backend declares
this capability only for its default arithmetic policy. Longer text retains the
existing per-request route. Groups require the same loaded model
generation, task prefix/instruction, backend and exact token length. Output
vectors retain the full model dimension until each caller applies its requested
MRL dimension and normalization. Strict token counting includes model special
tokens; usage cannot silently fall back to an estimate. Mixed media, ordered
content groups, tracing and per-item-error routes retain their existing paths.
The generic multimodal capability remains conservative; this does not advertise
native batching for every content group.

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

`test-read-pool-microbatch` covers broker compatibility, bounded waves,
queued/claimed cancellation, deadlines, caller-local presentation, complete
cohort budgets and output retention through allocation failures. It runs in the
default inference unit graph and requires neither a model nor a GPU:

```sh
cd zig/pkg/inference
zig build test-read-pool-microbatch -Dcuda=false -Dmetal=false --summary all
zig build test-read-pool-microbatch -Dcuda=true -Dmetal=false --summary all
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

## DGX Spark measurements

The final native-arithmetic executable has SHA-256
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
fallback, request ceilings, mixed Gemma traffic, deadlines, disconnected clients,
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
