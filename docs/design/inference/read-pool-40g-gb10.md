# Read-pool 40 GiB memory experiment on GB10

This experiment runs Gemma 4 E4B QAT Q4_0, the qualified GLiNER2.5-Decide Q8
bundle and BF16 EmbeddingGemma2 together. It checks co-residency, mixed text
traffic, growing generation KV, retained prefix caching and resource recovery.
It does not qualify A100 numerical behavior or predict A100 throughput.

The source baseline for the latest retest is `ab58a77e24`, with the process-wide
managed-CUDA guard and scheduled-generation deadline fixes in this worktree.
The earlier `e6144685f8` measurements are retained separately.

## Isolation and budgets

The local account cannot access Docker's socket or use passwordless sudo, and
there is no Kubernetes client. The server therefore runs as a user-owned systemd
service in its own cgroup, bound to `127.0.0.1:18840`. Existing services are left
running. This is a serving-process experiment, not a running Kubernetes pod.

[The canonical configuration](../../../configs/inference-read-pool-40g.json)
pins all three models to CUDA, prevents idle unloading, and separates the budgets:

| Domain | Limit | Meaning |
|---|---:|---|
| Managed CUDA allocations | 38 GiB | Shared physical allocation guard across all model contexts; leaves approximately 2 GiB of the nominal 40 GiB target for other device users |
| Host admission | 32 GiB | Separate from the device budget |
| Combined admission | 70 GiB | May be narrowed by the runtime's unified-memory/process policy on Spark |
| KV admission | 20 GiB | Sublimit across memory tiers, not a preallocation |
| Scratch admission | 12 GiB | Sublimit across memory tiers, not a preallocation |
| Process/cgroup memory | 72 GiB | Host-process envelope; not a substitute for a CUDA device cap |
| Retained prompt cache | 8 GiB logical | Prefix-cache accounting; not additional GPU memory to add to the other counters |

Weights, active KV, cached buffers and scratch share the same 38 GiB allocation
guard. The KV and scratch sublimits do not partition the GPU into independent
20 and 12 GiB reservations. Driver/module storage and vendor-private allocations
are outside this guard; NVML process memory is sampled independently.

The three admission weight/KV/scratch gauges are conservative reservations.
`antfly_inference_cuda_managed_live_bytes` measures outstanding managed CUDA
allocations, including cached scratch and KV buffers. Its peak includes pending
allocation reservations. NVML process memory includes additional driver-managed
storage. These measurements overlap and must not be added together.

## Runtime changes used by the experiment

Common `Node.init` now connects the explicit device budget to a process-wide
`DeviceBuffer` allocation guard. Per-model quotas alone do not enforce the sum
of three resident contexts. The added metrics expose live, peak and limit bytes.
A mock-driver regression verifies cross-context rejection and restoration of
reservations after allocation failure.

The pressure run also exposed a generation deadline that could remain blocked
in the scheduler. Scheduled prefill/decode now checks execution control while
waiting and drains claimed work safely. Deadline errors return a retryable
HTTP 504 instead of a generic generation failure. Eleven selected allocation,
scheduler, cancellation and response-mapping tests passed on the latest build.
These changes support the experiment's memory/deadline guarantees; they are
separate from the existing A100 kernel compatibility work.

## Reproduction

Install the three pinned models under the configured models directory. The local
experiment uses private directories with hardlinked immutable weight files.
Raw requests, replies, memory samples, model hashes, binary hashes and configs
are retained under `.benchmark-results/read-pool-40g/` in the worktree.
`environment-latest.json` identifies the source revision, runtime patch, serving
binary, model weights, local config and profile used for the latest retest.

Build the constrained serving executable with CUDA fatbins and start it with
`--config` pointing to the canonical config with local model/ML paths substituted.
Set `ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda`.

Latest main's E4B Flash-prefill path is an explicit Spark/L4 profile:
`ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=required-flash-f16-sm89`.
Despite its historical name, the E4B geometry also supports GB10. Its prefill
contract ends at 32K tokens. Do not source the complete short-context GB10 tuning
file for this test: that file fixes CUDA graph capacity at 2,048 tokens.
The generic main default and this explicit Flash profile are separate cases.
The Flash profile is not admitted on A100; retain the separate conservative
A100 profile until hardware-specific qualification is available.

The canonical config has a 32,768-token input ceiling and enables both
`cooperative_execution.mode: on` and `generation_batching.mode: on`, with
`max_step_items: 2` and `max_decode_wait_us: 0`. It combines ready decoders
without waiting to fill a batch. The earlier one-row comparisons explicitly
disable row batching with `max_step_items: 1`. The historical campaigns below used the
former combined batching switch. See [cooperative execution ownership](cooperative-execution.md)
for the current independent controls, recovery contract and validation gates.
Do not interpret high GPU utilization as proof that decode batches formed or
that latency is suitable for a production SLO.

With the owned server running, the mixed completed-generation sweep is:

```sh
python3 zig/pkg/inference/scripts/qualify_read_pool_40g.py \
  --records 64 256 512 1024 --output-tokens 128 --probes 12 \
  --generation-timeout 300 --output .benchmark-results/read-pool-40g/latest
```

`--pressure` instead tests real long-context KV reservations with five-second
caller deadlines, retryable saturation, lease cleanup and subsequent short
requests. Those deliberately interrupted requests do not qualify long-context
generation output. This stress mode needs a separate local config with the input
ceiling raised to 131,072 tokens; the canonical serving config remains bounded.
Both modes verify that the same resident model instances
survive. Supplying `--cgroup` adds host-memory and service-specific NVML samples.

The read-pool work precedes the separate write-pool experiment. Whisper,
GLiNER2.5 extraction and LightOnOCR are outside these measurements.

## Latest-main results

The short reference request produced identical greedy output on the generic
default and explicit Flash-prefill profile: 1,911 input tokens, 128 output
tokens, 10.97 seconds versus 6.23 seconds. This checks one reference request,
not general numerical equivalence between kernels.

The experimental batching sweep submitted two identical generation requests
together and continuously probed both smaller models. Times include queueing.

| Prompt tokens | Generation outcomes | Embedding p95 | Decide p95 | Peak service NVML |
|---:|---|---:|---:|---:|
| Idle baseline | No generation | 7.8 ms | 33.2 ms | 9.76 GiB |
| 1,911 | Both 200: 6.46 s / 16.91 s | 383.0 ms | 215.0 ms | 10.38 GiB |
| 7,761 | Both 200: 34.87 s / 95.85 s | 86.5 ms | 89.4 ms | 10.71 GiB |
| 15,615 | Both 200: 64.33 s / 184.30 s | 82.4 ms | 94.7 ms | 11.87 GiB |
| 31,289 | 200 at 114.42 s; peer 504 at 300.54 s | 66.5 ms | 224.9 ms | 13.65 GiB |

All embedding and Decide probes in this sweep returned HTTP 200; embedding
vectors matched their baseline exactly and typed decision answers/confidences
were unchanged. Each completed pair through 15,615 tokens produced identical
generation output. The 31K pair **did not pass** the completed-generation
campaign: its second request exceeded the five-minute deadline. The raw report
retains that failure. This is a latency limitation, with no observed OOM.
The failed sweep did not reach its final instance-ID/recovery assertions.

GPU utilization averaged 93–95% in the generation phases, measured for the
whole device. The separate PDF service was idle. Scheduler counters reported
singleton batches and zero two-row batches. Admission assigned the second long
request a 32-token shared prefill plan, compared with 512 tokens for its peer;
that small plan remained in use after the peer finished.

Across this sweep, conservative backend weight reservations stayed at 15.03 GiB.
Peak active backend KV reservations reached 5.75 GiB, scratch reservations
1.48 GiB, managed allocations 13.33 GiB and service NVML 13.65 GiB. Retained
prefix-cache logical bytes reached 8.00 GiB. These are overlapping accounting
views, not independent physical allocations to sum. The experiment stayed far
below the physical guard; it did not fill a physical 40 GiB GPU to exhaustion.

The separate `latest-pressure` campaign passed: pairs of 7,761- and
31,289-token requests deliberately expired, KV admission returned to zero,
and subsequent reads succeeded. At 122,270 input tokens, a concurrent request
received retryable HTTP 503 `MODEL_RESOURCE_BUSY` from backend admission;
the admitted peer expired and recovered. These were reservations interrupted
before full prefill, not completed 122K-context generations. Five-second
deadlines returned in 5.00–9.79 seconds while claimed work drained. All three
model instance IDs survived unchanged, and retained-cache reads recovered.

## Bounded serving configuration left running

The final `latest-supplement` campaign passed using the canonical config's
default batching policy (`off`), the Spark-only Flash-prefill environment
override, and a 32,768-token input limit. E4B retrieved the exact code
`cobalt-lantern-7319` from the middle of a 31,334-token prompt in 62.30 seconds,
stopping after nine output tokens. This is a retrieval sanity check, not a
long-context quality benchmark.

During that generation, 208 embedding probes and 208 Decide probes all returned
HTTP 200 with exact baseline vectors and unchanged decision answers/confidences.
Embedding p50/p95 was 46.0/99.5 ms and Decide p50/p95 was 224.9/300.4 ms. After
the stress and cache fill, p95 recovered to 6.8 ms and 34.1 ms respectively.
GPU utilization during retrieval averaged 94.3%.

Eight short cached generations forced eviction: the reported eviction counter
rose from zero to 172, while logical retained cache stayed at or below 8 GiB.
Peak service NVML memory was 11.42 GiB and managed allocations peaked at
11.05 GiB. All three model instance IDs were unchanged. The final snapshot has
zero active KV and scratch admission leases, while 10.87 GiB of managed buffers
remain allocated for weights and retained/cached storage. Those remaining
buffers are still charged against the 38 GiB guard.

The owned `antfly-read-pool-40g.service` remains bound to
`http://127.0.0.1:18840`, with its exact local config in
`.benchmark-results/read-pool-40g/local-serving-config.json`. It is a systemd
service, not a container/pod. The final snapshot and `summary-latest.json`
identify the tested service and the separate passing/failing campaigns.

The evidence supports memory fit and functional co-residency for this text
workload. Start latency-sensitive deployment validation with one active long
generation; the generic request admission limit does not enforce that
per-model recommendation. Concurrent long-prompt latency and A100-specific
performance still need qualification. Do not carry the Spark Flash override
onto A100; use the conservative A100 launch wrapper/profile there.

## Concurrent-generation follow-up

Tracing the ordinary `/generate` route showed that CUDA and Metal use
`shared_serial` execution and hold the model mutex for the whole request.
The generation batching switch does not release that mutex on this route.
The scheduler still sees leases for callers waiting on the mutex, so its
shared-execution heuristic selected 32 rows for a waiting long request. Memory
admission then sealed that small plan for its entire prefill.

The follow-up change selects the bounded idle ceiling for whole-request
execution before reserving memory. Isolated pipelines continue to use the
scheduler's fairness ceiling. Explicit request caps remain in force, and memory
admission can reduce the chunk size before creating the immutable plan. This
avoids resizing an admitted plan or using scratch that was never reserved.

The regression uses two actual coordinator leases for 31K prompts: the queued
lease recommends 32 rows, whole-request admission selects 512, isolated
admission retains 32, and a tighter real E4B scratch budget reduces 512 to 256.
Six selected planning, memory-backoff, immutable-plan and deadline tests passed.

The live follow-up passed with the same model files, Flash profile, default
reasoning policy, F16 KV and five-minute request deadline:

| Paired 31,289-token generation | Earlier result | After fix |
|---|---:|---:|
| First response, 128 output tokens | 114.42 s, HTTP 200 | 111.52 s, HTTP 200 |
| Second response, 128 output tokens | 300.54 s, HTTP 504 | 223.41 s, HTTP 200 |

Both requests now admit 62 chunks with a 512-token ceiling. Previously the
second admitted 978 chunks with a 32-token ceiling. Both completed outputs
match each other and the earlier successful 31K response exactly. All 1,155
embedding probes and 1,155 Decide probes returned HTTP 200 with exact baseline
vectors and unchanged typed decisions. All three instance IDs survived.

Embedding p50/p95 was 8.3/78.4 ms; Decide was 37.3/264.7 ms. The idle p95 values
were 7.3 ms and 34.0 ms. Peak service NVML memory was 11.43 GiB, with a lifetime
managed-allocation peak of 11.08 GiB, below the 38 GiB guard. The earlier sweep
included retained-cache stages and the focused follow-up skips them, so these
absolute memory peaks are not a controlled before/after memory comparison.
The earlier batching-on flag and follow-up batching-off config both use the
same whole-request CUDA gate on ordinary `/generate`.

The first request's internal timing was 56.40 seconds in prefill and 54.97
seconds in decode. Its peer reached the pipeline after 111.52 seconds, mostly
waiting on the model gate. Removing tiny chunks improves queue efficiency;
whole-request serialization and long-context kernel time remain substantial.

The follow-up deadline campaign also passed: the two intentionally interrupted
31K requests returned canonical HTTP 504 in 5.00 and 6.78 seconds, KV leases
returned to zero, and cached generation and smaller-model reads recovered.
The three model instance IDs were unchanged.

### Streaming request policy

The harness can measure first visible content with `--stream-generations`.
Default reasoning-capable Gemma requests currently defer public content until
generation completes to preserve reasoning/content order. Explicit thinking-off
requests can stream incrementally:

```json
{
  "stream": true,
  "stream_options": { "include_usage": true },
  "chat_template_kwargs": { "enable_thinking": false }
}
```

The separate `--records 256 --stream-generations --disable-thinking
--skip-retained` candidate passed: paired 7,761-token prompts produced first
visible content at 13.27 and 45.14 seconds and completed at 31.63 and 63.48
seconds. Both outputs matched each other, and all 337 embedding and 337 Decide
probes remained correct. The candidate's peak service NVML memory was 11.64 GiB.
Thinking-off changes request semantics and was measured separately; the
production/default reasoning policy was not changed.

Raw follow-up reports, the exact executable, source patch, configuration and
timing logs are in `.benchmark-results/read-pool-40g/concurrency/`. At that stage the service
used that directory's `read-pool-server` executable and the
same bounded local serving config, with diagnostic generation timing enabled.

The focused concurrency reproduction skips the separate retained-cache sweep:

```sh
python3 zig/pkg/inference/scripts/qualify_read_pool_40g.py \
  --records 1024 --output-tokens 128 --generation-timeout 300 \
  --probes 12 --skip-retained \
  --output .benchmark-results/read-pool-40g/concurrency/after-512
```

The harness now requires identical output from the two greedy requests. The
bounded serving config can also check deadline cleanup without raising its
input ceiling:

```sh
python3 zig/pkg/inference/scripts/qualify_read_pool_40g.py \
  --pressure --pressure-records 1024 --probes 12 \
  --output .benchmark-results/read-pool-40g/concurrency/deadlines
```

The original follow-up acceptance checks were:

1. Protect embedding and Decide tail latency with representative text lengths
   and batches, comparing bounded prefill chunk sizes under mixed traffic.
2. Queue whole-request generation before reserving active KV/scratch. Bound
   the queue, honor waiting deadlines, and verify that cancelled queued work
   never allocates GPU state or leaves a lease behind.
3. Expose model queue wait, time to first token, prefill time and decode time
   separately. High device utilization does not identify which requests wait
   or whether useful batches formed.
4. Qualify shared prefill/decode execution before enabling it for ordinary
   CUDA generation, including unequal context lengths, streaming and
   cancellation. Preserve greedy output equivalence and the physical guard.
5. Measure the conservative SM80 serving path on A100, then repeat the
   residency, activation-burst and recovery experiment for the write pool.

The subsequent cooperative execution, bounded queue, profiling and write-pool
experiments are recorded below and in [the write-pool report](write-pool-40g-gb10.md).

## Cooperative execution and bounded admission follow-up

The follow-up adds an opt-in, geometry-qualified dense E4B CUDA path. Ordinary
uncached text requests can share two active generation slots. Each claimed
step acquires the model execution lane/mutex, installs its own run budget and
resets request-specific backend metadata. Sampling and request-owned KV remain
separate. Cleanup reacquires the same lane; retiring a handle cannot clear a
peer's budget. Graph, selected-KV, draft, prefix-cache and media routes remain
exclusive. Default generation batching remains off.

A bounded FIFO model queue precedes scheduler leases and KV/scratch admission.
Waiters honor caller deadlines; queue rejection is retryable HTTP 503 and
expiry is retryable HTTP 504. The queue, first public content, prefill and decode
metrics are exposed separately. Pipeline phase times include scheduler waits;
first-content time excludes network transit and respects the existing default
reasoning/content ordering. None is a synchronized kernel timer.

The first one-row interleaving campaign passed. A 31,289-token request and a
1,911-token request each generated 128 tokens and matched their saved standalone
outputs. They took 120.01 and 104.15 seconds, respectively, with the short
request completing first. All 616 embedding and 616 Decide probes were correct;
observed p95 values were 92.2 and 255.7 ms. Peak service NVML was 12.05 GiB.
Functional interleaving alone therefore did not solve the short-generation tail.

The same campaign admitted two 8K requests while twenty additional callers
exercised bounded admission and 1.5-second deadlines. Six returned retryable
503 and fourteen returned retryable 504. Waiting callers did not increase
active KV admission: both the pre-queue and queued peak were 1,535,115,264 bytes.
The two admitted outputs matched, an exclusive cached request recovered, and a
client disconnect after first streamed content released active KV. All three
model IDs survived; active generation, queue depth, KV and transient scratch
returned to zero. Raw evidence is `next/step-gate-full/`.

The subsequent short-context policy prefers the cheaper pending context for
up to eight consecutive turns, then grants the next round-robin owner. It uses
the full prompt length for prefill and current sequence length for decode;
equal-cost work retains round robin. The preference is enabled only by the
cooperative E4B policy. Default/native scheduling remains unchanged. All 98 selected scheduler/runtime tests passed, including 63 native scheduler
tests and the admission, cancellation, ownership, planning and histogram
regressions. The short-context cases cover bounded bursts, equal-cost fairness
and a manual final prefill competing with continuously queued long chunks.

### Profile candidates kept separate

- Two-row execution formed real batches and preserved both saved outputs, but
  both mixed-context requests took about 265 seconds and the short request
  completed second. It failed the latency gate; the selected experiment uses
  one row per GPU step.
- The `tiled64` score-prework consumer produced an equivalent 31K output in
  111.93 seconds, without a meaningful improvement over the 111.52-second
  control. It was not selected.
- The required split-K-online profile rejected E4B's two-KV-head geometry. That
  specialization requires one KV head and is qualified only through 4,096
  total tokens. Its guard was preserved.
- Disabling score prework preserved the short output but regressed the short
  128-token generation to 25.62 seconds. The long run was interrupted after that
  regression; no completed long-context time is claimed.
- BF16 prefill mirrors initially failed co-resident load admission because a
  format-agnostic 6x artifact estimate was retained as steady weight residency.
  Construction still reserves that peak. After eager dense Gemma upload, the
  runtime now retains its measured immutable weight inventory plus headroom,
  excluding scratch/KV. The measured 15,358,964,904 weight bytes retained
  16,318,900,210 bytes rather than the 30,929,647,680-byte construction estimate.
  Lazy, expert, training and non-mirrored models keep their existing estimate.
  All three models then loaded below the unchanged physical guard.

The mirrored-profile run changed the free-text greedy outputs relative to the
original precision policy, so it is not an output-equivalent promotion. With
CUDA event profiling enabled, its 31K request spent 27.06 seconds in prefill and
108.38 seconds in decode. Decode attention accounted for about 103.73 seconds
of measured GPU event time, substantially more than projections/FFNs. The
whole request took 135.59 seconds. This identifies an attention bottleneck; it
does not establish a timing win over the unprofiled control.

These follow-up timings were collected with another GPU worker active; some
runs also overlapped CPU builds. The harness records all NVML process IDs and
separately isolates the owned service's memory footprint by cgroup. Whole-device
utilization cannot be attributed solely to this pool. No actual A100 timing or
numerical claim is made.

To reproduce the mixed-context and queue checks, use the bounded read config
with `cooperative_execution.mode: on`, `generation_batching.mode: off`,
`max_step_items: 1`, the Spark Flash
prefill override, and both generic decode-graph replay settings off:

```sh
python3 zig/pkg/inference/scripts/qualify_generation_concurrency.py \
  --output .benchmark-results/read-pool-40g/next/final \
  --references .benchmark-results/read-pool-40g/next/step-gate-full/requests.jsonl \
  --disconnect \
  --cgroup /sys/fs/cgroup/user.slice/user-1002.slice/user@1002.service/app.slice/antfly-read-pool-40g.service
```

`--embedding-batch-size 8 --probe-context-sentences 8` repeats the mixed
experiment with longer smaller-model inputs and batched embeddings. Keep that
report separate from the single-query baseline. The final qualification does
not enable BF16 mirrors, split-K-online, tiled64 or the score-prework rollback.
The [write-pool report](write-pool-40g-gb10.md) records the subsequent four-model
experiment and the additional long-audio accuracy limit.

### Manual final-prefill correction

The first shorter-context implementation exposed another scheduling boundary:
CUDA's final prefill/logit chunk takes a manual turn instead of posting a
`PendingPrefill`. Prioritizing only pending chunks could leave that short final
chunk behind the long request's remaining queued prefill. The opt-in selector
now compares every prefill owner, including manual turns, while retaining the
bounded round-robin fallback. A dedicated regression keeps a long chunk queued
and requires the short manual tail to acquire its turn, then verifies that long
prefill resumes after the short owner enters decode.

The preceding `next/before-manual-tail/` campaign passed correctness, queue
memory, cancellation and recovery but took 249.27 seconds for the long request
and 223.64 seconds for the short one under shared GPU load. This remains a
separate result; it is not evidence that short-tail latency was solved.

### Final live checks on the manual-tail build

`next/final/` passed saved-output equivalence for the 31,289/1,911-token pair,
queue-memory invariance, retryable saturation/deadlines, client-disconnect
cleanup, exclusive-cache recovery and unchanged model IDs. Both requests
produced 128 tokens. The observed long/short completion times were 249.07 and
204.85 seconds. The short request's pipeline prefill was 8.96 seconds, versus
about 129 seconds in the preceding contended run; decode elapsed time was
192.99 seconds, including waits. The change removes manual-prefill starvation;
**it does not solve the default response-completion tail**.

All 881 embedding and 881 Decide probes during that mixed phase passed their
baseline checks. Their observed p95 values were 175.7 and 564.2 ms, compared
with the phase's idle p95 values of 60.7 and 69.9 ms. These are contended shared
GPU observations, not isolated performance or an established production SLO.
Peak service NVML was 11.54 GiB; managed-allocation peak was 11.18 GiB. Twenty
queued probes again left the admitted KV peak unchanged, fourteen expired and
six were rejected as expected, and disconnect/recovery checks passed.

`next/larger-probes/` also passed. It runs a 7,761-token generation and a
1,911-token generation with 32 output tokens, while embedding batches contain
eight longer inputs and Decide receives longer background context. Every
embedding vector is checked against its corresponding warmed batch reference.
The 115 embedding batches and 115 Decide requests were correct; their p95
request latencies were 812.1 and 428.3 ms. These larger inputs are a separate
workload and should not be compared as if they were single short queries.

`next/distinct-markers/` passed two independent concurrent retrieval checks:
`cobalt-lantern-7319` from a 31,334-token prompt and `tulip-harbor-9462` from a
shorter prompt. Each response had to equal its own marker exactly. The short
request explicitly disabled thinking and streamed; its first public content
arrived at 9.14 seconds and it completed at 50.60 seconds. The default-reasoning
long request completed correctly at 135.68 seconds. Neither changed the default
request policy. Peak service NVML across this check reached 12.24 GiB, with a
managed-allocation peak of 11.91 GiB, below the same 38 GiB guard.

A separate small-query chunk comparison used the same 8K/2K generation pair
and 32-token outputs. Both 512- and 256-token configurations preserved the saved
outputs and instance IDs:

| Prefill ceiling | Long / short completion | Embedding p95 | Decide p95 |
|---:|---:|---:|---:|
| 512 | 54.10 / 47.49 s | 284.2 ms | 420.0 ms |
| 256 | 49.07 / 42.47 s | 129.2 ms | 410.2 ms |

The candidate halves admitted per-request prefill scratch from 957,349,888 to
479,199,232 bytes in this geometry. The observed embedding tail improved, while
Decide's tail changed little. The 256-token campaign starts a fresh service;
absolute lifetime memory peaks and warm-cache state are therefore not a
controlled before/after comparison. This is preliminary tuning evidence on a
shared GPU, and the completed 32K campaign remains the 512-token configuration.
This historical follow-up explicitly uses one-row cooperative execution.
The current read profile enables ready-only two-row batching; see the
[batching record](cuda-generation-batching.md) for its separate measurements.

Source snapshots include untracked implementation files, and final environment
receipts pin executable/model/configuration hashes and filtered CUDA feature
settings. `make gc`, Zig formatting, Python syntax checks, and the 98 selected
regression tests passed. The final running service after the sequential
qualification is the write pool on port 18841; read launch receipts/configs are
preserved for restarting it.


## Explicit cooperative ownership follow-up

The current implementation and final controlled GB10 results are documented in
[cooperative execution ownership](cooperative-execution.md). This supersedes the
earlier combined batching switch and running-service note: interleaving now has
its own opt-in, one-row limit, backend capability declaration and execution
scope. Common runtime initialization owns the shared allocation cap, and failed
frees retain durable ownership through confirmed recovery. The controlled
streaming short request improves from 86.69 to 36.48 seconds; a short request
arriving during long decoding improves from 31.44 to 6.91 seconds. Both measured
comparisons pass all predefined gates with valid memory evidence. The final
cooperative read service runs on port 18840. A100 remains unmeasured.
