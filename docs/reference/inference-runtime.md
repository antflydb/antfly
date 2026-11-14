# Inference runtime configuration

Operator reference for the inference runtime: the configuration file, CLI precedence,
admission and memory policy, request ceilings, and the model-instance API. The
user guide is [Antfly Inference](../guides/inference.mdx).

## Configuration file

`antfly inference run --config inference.json` reads admission settings and the
model settings `models_dir`, `ml_dir`, `max_loaded_models`, `preload`, `keep_alive`,
`prompt_cache`, and `kernel_jit`. `keep_alive` is the same duration string standalone
uses (default `5m`; `"0"` disables idle unloading). `generation_batching` uses
the same execution settings in both inference executables and embedded mode. `prompt_cache`
takes the same fields as standalone's (`enabled`, `mode`, `max_bytes_mb`, `min_tokens`,
`ttl_ms`); the prompt KV cache stays on by default when `prompt_cache` is omitted entirely,
matching prior behavior. `kernel_jit` takes the same fields as standalone's
(`mode`, `cache_dir`, `max_cache_bytes_mb`, `preload_budget_ms`); the
`--kernel-jit-*` flags and `ANTFLY_INFERENCE_KERNEL_JIT_MODE` still override it:

```json
{
  "inference": {
    "models_dir": "/models",
    "ml_dir": "/ml",
    "max_loaded_models": 3,
    "preload": [
      {"kind": "embedder", "name": "BAAI/bge-small-en-v1.5", "backend": "native"}
    ]
  }
}
```

For native LightOn CUDA readers, `inference.generation_batching.mode: "on"`
also enables the continuous OCR scheduler. `max_step_items` bounds physical
reader rows (up to 16 without an experimental wide profile); its active window
is bounded to twice that value. Generic text generation retains its own
qualified two-row ceiling. `off` and `auto` do not opt into the new reader path.
Diagnostic `ANTFLY_LIGHTONOCR3_CUDA_BATCH=0` overrides canonical activation;
`ANTFLY_LIGHTONOCR3_BATCH_ROWS` and `ANTFLY_LIGHTONOCR3_BATCH_WINDOW` override
their respective defaults, bounded by physical capacity. Setting both to `1`
provides a singleton rollback. Without an explicit window, canonical mode uses
twice the resolved row count. Admission, execution, and discovery use the same
resolved settings.
`resources.inference.admission.max_concurrent_requests` must allow overlapping
requests to reach the scheduler. Public `max_items_per_request: 1` does not
prevent batching independent callers.

On Ampere-or-newer CUDA devices, LightOn's automatic kernel policy uses split
paged decode attention and tensor-core prefill for the two supported checkpoint
geometries. Unsupported layouts retain the existing CUDA path. Explicit
`ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION=0` and
`ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off` disable the respective selections.
These choices do not raise memory or request ceilings. Decoder row batching is
independent of vision batching: the canonical reader profile reserves one page's
vision workspace, so a single full-size page does not require an eight-image
scratch reservation. Wider vision batches remain diagnostic overrides requiring
their own memory and output qualification.

```json
{
  "resources": {"inference": {
    "admission": {"max_concurrent_requests": 4, "max_work_units": 64},
    "request_limits": {"max_items_per_request": 1}
  }},
  "inference": {
    "keep_alive": "0",
    "generation_batching": {"mode": "on", "max_step_items": 2}
  }
}
```

Start at two reader rows and qualify higher widths against representative pages,
request deadlines and peak memory. A larger active window does not guarantee
that all pages fit memory simultaneously. For a pool that scales to zero,
`keep_alive: "0"` avoids unloading an idle model while the pod remains running;
model-count and memory-pressure eviction still apply. Preload the primary model
before routing to the newly activated pod. Set `inference.preload[].pin: false`
to warm the evictable cache rather than permanently reserving the model slot;
the default remains `true` for intentionally resident preloads. Explicit
per-model backend, GLiNER CUDA precision, and A4B load settings survive eviction
and apply to subsequent automatic cold loads. Pinning only controls residency;
explicit request placement overrides only placement. Specifying the same
backend on `/generate` preserves the configured residency, memory, loading and
precision settings on both warm and cold paths. Explicit load-policy APIs can
override the corresponding policy, and those variants retain distinct cache and
in-flight identities.

Reader performance qualification must distinguish native executor attribution
from observed batching. `execution.max_native_step_items` reports the largest
successful decoder step that a returned page participated in, including pages
from other requests. Zero or absent means unobserved; one means singleton.
`scripts/qualify_reader_page_latency.py --require-native-batch` requires at
least two observed rows for every request; `--min-native-step-items` can raise
that target up to the selected concurrency. Exact reference parity and complete
output checks remain independent requirements. Native item/epoch counts or
CUDA graph replay counts alone do not establish multirow execution.


CUDA model loads log host preparation, device initialization, weight upload and
total elapsed time. Host preparation may map rather than read all weight bytes:
subsequent page faults and conversions can appear in the upload phase. These
are wall-clock timings rather than synchronized GPU profiling spans.
The optional `ANTFLY_CUDA_LIGHTON_PRECISION_PROFILE=bf16_vision` admits
Ampere-or-newer devices with a CUDA 13-or-newer driver through generic vendor
kernels (the embedded vision artifact uses PTX 9.0); F32 remains the default.
Architecture-specific fused kernels retain their own guards. Qualify output
correctness and memory on the target GPU before promotion.

Pull the referenced models into that directory before starting the process.
Client-only `inference.api_url` and `inference.api_key` may stay in a shared
configuration file: both inference serving executables accept and ignore them.
The database uses those fields when it acts as a client.

Both inference executables read model settings under `inference` and resource
policy under `resources`. Flat settings and `keep_alive_ms` are rejected.
For example, migrate this older config:

```json
{"models_dir":"/models","keep_alive_ms":60000,"admission":{"inference":{"max_concurrent_requests":8}}}
```

to:

```json
{
  "resources": {"inference": {"admission": {"max_concurrent_requests": 8}}},
  "inference": {"models_dir": "/models", "keep_alive": "60s"}
}
```

Memory policy now uses bytes under `resources.memory` and
`resources.inference.memory`; convert older MiB values by multiplying by
1,048,576. Request ceilings live under `resources.inference.request_limits`.
Startup errors identify removed fields and their replacement paths. No `api_url`
is required to start the server; configure its listener with `--host` and `--port`.

Explicit CLI options override config regardless of argument order. Supplying
one or more `--preload-model` flags replaces the config preload list rather than
appending to it. For an exact match of kind, model reference (including artifact
variant), and backend, config-only `residency_mode`, `memory_budget_mb`, and `pin`
policies are retained; unrelated config models are not loaded. Duplicate config
entries matching the same CLI selection are rejected as ambiguous.
A model-count limit of zero disables that limit. When paths are
absent from both CLI and config, the usual environment/home defaults apply.
Preload entries retain their backend, artifact format/quantization, and optional
residency/memory-budget choices.
An unqualified preload name with `format`/`quantization` is expanded to an
explicit artifact reference (for example, `owner/model:gguf:Q4_K`) before
resolution. Conflicts between preferences and already-qualified references are
rejected rather than silently selecting another installed artifact.

The runtime reads this config directly. Colony's operator emits the same
canonical configuration for every InferencePool, including pools with no config
or resource overrides. Model CLI arguments and volume mounts read that same
nested section. Upgrade all pools and the operator together to a compatible
runtime; there is no legacy format selector or flat-config fallback.

For operator-generated eager preloads (the default loading strategy), specify a
recognized task such as `tasks: [embed]` or `tasks: [generate]` on each model.
The warm CLI needs a model kind; an omitted or unrecognized task hint
does not imply a generator. The operator reports a configuration validation
failure before creating or updating the workload when that kind is ambiguous.
Alternatively, supply `spec.config.preload` with an explicit `kind`, or use
lazy/bounded loading to discover the model task at request time. Per-model
eager strategy overrides have the same requirement.

Empty or null optional `backend`, `format`, and `quantization` fields in operator
preload configuration are treated as unspecified in both JSON and CLI output.

## Capacity, admission, and memory

- **Request admission.** `resources.inference.admission.max_concurrent_requests` (default 32; `--max-concurrent-requests` on `antfly inference run`; 0 disables the limit) bounds concurrent inference requests. `resources.inference.admission.max_work_units` independently sizes weighted work/media capacity; when omitted it derives from request concurrency. A few heavy requests can exhaust work capacity before the request count is reached. Standalone shares one budget across direct enrichment and query calls, `/ai/v1`, and `/ml/v1`. Requests are never queued. When capacity is exhausted, HTTP callers get `503` with `Retry-After: 1` and a JSON body carrying `retryable: true`.
- **Model residency.** At most `max_loaded_models` models stay loaded (default 10; 0 means unlimited), and the least recently used idle model is evicted to make room. Idle models unload after `inference.keep_alive` (default `5m`; `"0"` disables idle unloading). Concurrent requests for the same unloaded model share one load.
- **Memory.** Model loads and requests reserve memory before they allocate. The reservation comes from one process envelope shared with storage and caches. Set the envelope with `--process-memory-budget-mb` or `ANTFLY_PROCESS_MEMORY_BUDGET_MB` on `antfly standalone` or `antfly inference run`. Otherwise the cgroup limit is used, and host memory without a cgroup limit. In Kubernetes Burstable pods, set it below the container limit. A request that can never fit returns `MODEL_RESOURCE_LIMIT`. Temporary pressure returns a retryable `MODEL_RESOURCE_BUSY` once idle models have been evicted.
- **Query priority.** Background enrichment yields to query-time work in the same database process. Embedding backfill pauses (up to 5 s) while a query-time embed is running, and background generation pauses while an interactive generate or chat request is in flight. Backfill that receives `503` retries with backoff.

## Resource policy and request ceilings

Resource policy lives under `resources.inference`: `memory` contains byte-based
host/device/combined/KV/scratch ceilings, `admission` controls concurrent requests
and weighted work, and `request_limits` contains `max_items_per_request`,
`max_input_tokens_per_item`, `max_output_tokens_per_item`, and
`max_decoded_pixels_per_batch`. Per-request item ceilings preserve native batching
across independent callers. Model loading, prompt caching, and generation batching
remain under `inference`. The DB and standalone inference use the same policy;
the previous resource config locations are rejected.

The standalone inference worker also applies an explicit
`resources.inference.memory.max_device_bytes` (or `--backend-budget-mb` override)
to the sum of its managed CUDA `DeviceBuffer` allocations across model contexts.
Cached scratch and deferred frees remain charged; allocation failure restores
the reservation, and a failed free cannot authorize additional allocations.
This physical allocation guard supplements the shared logical admission ledger.
It excludes driver/module storage, NVDEC surfaces and vendor-library-private
allocations, so leave device headroom for those users. An omitted device override
keeps the existing automatic admission policy and an unlimited physical guard.

`/ml/v1/metrics` reports `antfly_inference_cuda_managed_live_bytes`,
`antfly_inference_cuda_managed_peak_bytes`, and
`antfly_inference_cuda_managed_limit_bytes`. These cover managed allocations,
including cached buffers; the peak includes pending allocation reservations.
The existing `antfly_inference_admission_{host,backend}_{weight,kv,scratch}_bytes`
gauges describe conservative admission reservations. Prompt-prefix cache logical
bytes and OS/NVML process memory are separate measurements; adding these counters
would double-count overlapping storage.

Scheduled generation polls the request execution control while waiting for
prefill/decode work, including dedicated-executor event waits. A caller deadline
returns HTTP 504 `INFERENCE_TIMEOUT`; explicit cancellation returns HTTP 408
`INFERENCE_CANCELLED`. Cancellation removes idle queued work immediately, but a
claimed GPU step must drain before its borrowed work pointer and KV owner can be
retired. These are cooperative boundaries, not GPU-instruction preemption.

Default CUDA and Metal generation holds the model gate for the whole request.
Prefill admission therefore uses the bounded idle chunk ceiling even when other
callers already hold scheduler leases while waiting for that gate. Isolated
pipelines retain the scheduler's shared-execution ceiling. In both cases,
explicit request limits and scratch/KV admission constrain the final immutable
plan before execution; queued callers do not force a serialized GPU request to
retain a tiny shared-execution plan.

With `inference.cooperative_execution.mode: on`, eligible dense Gemma 4 E4B
CUDA text requests share two active slots and interleave scheduler steps.
Enabling `inference.generation_batching.mode: on` additionally combines two
ready decode requests into one forward when the backend declares support.
Each step binds its owners' memory budget while holding the model mutex.
Graph, draft, prefix-cache, selected-KV and multimodal routes retain exclusive
ownership. The shipped `configs/inference-read-pool-40g.json` enables both
controls, with `max_step_items: 2` and `max_decode_wait_us: 0`: it batches ready
requests without waiting to fill a batch. No request flag is needed. Setting
`generation_batching.mode: off` restores one-row cooperative execution;
disabling cooperative execution restores exclusive generation. Other runtime
configurations retain their existing defaults. See the
[measurements and tradeoffs](../design/inference/cuda-generation-batching.md).
Compatible EmbeddingGemma 2 CUDA text requests also combine automatically under
the default arithmetic policy, with at most four rows and 8,192 bucket-padded
tokens per cohort. Nearby lengths share attention masks with at most 25%
grouping padding. Individual inputs retain the full 8,192-token model limit;
inputs above 4,096 tokens naturally occupy singleton cohorts.
Authenticated CUDA GLiNER 2.5-Decide classification calls combine up to eight
compatible rows through `/extract` and `/decisions`. These use the Node-owned
executor broker independently of the generation controls. Other execution profiles retain their existing paths. See the
[HTTP batching contract](../design/inference/read-pool-http-batching.md).

The write profile also collects independent CUDA GLiNER boundary requests with
matching schemas/options into cohorts of up to eight rows and 4,096 padded
tokens, with at most 512 prepared tokens per row. Length buckets bound grouping
padding to 25% when the encoder declares attention masks. Windowed documents,
larger rows and regex validation retain their existing exclusive execution.
Short-row preparation is retained through qualification, cohort repacking and
serial fallback; repacking repeats no tokenizer work.
Cohorts that cannot admit their additional workspace return to the callers'
existing serial paths before any model execution.
Each caller retains its IDs, unpadded usage, output limits and cancellation.
Responses are decoded and serialized with independent scalar, string and
response-byte budgets. A row-local output or semantic rejection fails only
its caller; driver failures and backing OOM fail the shared invocation. Extraction phases, decoded counts and solver diagnostics remain
caller-local. Published responses retain a separate memory owner until all
consumers rejoin. Pre-forward planning and admission refusals subdivide the
cohort or return to serial execution; failures after dispatch are never replayed.
The configured public item ceiling also applies before schema-v2 extraction
dispatch, independently of the internal cross-request cohort size.

Whisper small's CUDA encoder automatically collects up to four compatible audio
rows with a bounded 5 ms collection window. Decoder caches retain exclusive
per-request ownership. Full-context encoder position IDs repeat independently
for each row; batch cardinality does not reduce an audio item's context limit.
These encoder/boundary brokers work independently of `generation_batching`.
The shipped write profile enables LightOnOCR's separate continuous scheduler
with two decoder rows and a four-page active window. Internal Node callers can
override the executor collection delay, including zero; omission uses the
backend stage's default.

The cooperative policy prefers shorter contexts for up to eight consecutive turns, then grants
the next round-robin owner. Manual final-prefill turns participate in that
selection, so queued long chunks cannot hide a short request's final chunk.
Other scheduling policies retain strict round robin.

Generation waits in a bounded model queue before scheduler/KV/scratch admission.
Queue saturation returns retryable HTTP 503 `MODEL_RESOURCE_BUSY`; a waiting
deadline returns retryable HTTP 504. Queued cancellation removes its waiter
without reserving device state. `/ml/v1/metrics` exposes generation active/queued
gauges, queue rejection/cancellation counters and `queue_wait_seconds`,
`first_content_seconds`, `prefill_seconds` and `decode_seconds` histograms under
the `antfly_inference_generation_` prefix. Pipeline phase times include scheduler
waits. First-content time is server-observed availability, includes queueing and
excludes network transit; default reasoning-capable requests may defer public
content until completion. These are not synchronized GPU-kernel timers.

For eagerly uploaded dense CUDA Gemma 4 with BF16 weight mirrors, construction
keeps its conservative format-based load reservation. After upload, the retained
weight lease uses the measured immutable weight inventory plus bounded headroom.
Scratch and KV remain separate owners; lazy, expert, training and non-mirrored
models retain their existing estimate. OCR temporary/deferred-free cache leases
remain scratch-classified even while idle. Recovery should compare those leases
with the warmed baseline rather than requiring every scratch gauge to be zero.

Grouped embedding requests share one aggregate pixel ceiling across all groups,
including when execution splits them into separate microbatches. Transcription
shares one output-token budget across the audio item's windows and temperature
retries; generated timestamp and EOS tokens count, while forced prompt tokens do
not. Exhaustion returns `OUTPUT_TOKEN_LIMIT_EXCEEDED` rather than a successful
partial transcript. Session append calls each receive a request budget; a single
streaming upload shares its budget for the lifetime of that request. A failed
session processing pass preserves its buffered audio, conditioning, sequence
numbers, and previously delivered events. Retry with a fresh request budget by
committing the buffered audio; do not re-upload audio already appended. Completed
processing passes from earlier streaming chunks remain committed.

## Model instances

Antfly Inference owns the shared model cache. Antfarm and other clients use the
same model-instance API:

- `GET /ai/v1/models/instances` lists loading and loaded instances without
  extending their idle deadlines. Each incarnation appears once, with all its
  supported tasks.
- `POST /ai/v1/models/instances` with `{ "model": "owner/model", "task": "embedder" }`
  prepares the same instance used by that task's inference requests. Existing
  compatible instances are reused. Normal eviction still applies. Readers that
  own sessions per request have no persistent instance and return an unsupported
  instance error; their ordinary inference requests remain available.
- `DELETE /ai/v1/models/instances/{instance_id}` unloads one exact incarnation.
  An absent or stale ID succeeds without affecting a replacement. Busy, loading,
  and startup-managed instances return a conflict.

A model is an artifact; a model instance is one loading or loaded incarnation
inside a server; a backend is its selected execution implementation. Loaded
instances may stream weights and do not promise full GPU residency.
`active_handles` counts internal owners and load waiters, rather than HTTP
requests. `unloadable` and `unload_blocked_reason` describe eviction eligibility
at snapshot time; concurrent work can change it before an unload arrives.

Startup backend overrides for split transcription models also select the backend
for subsequent default requests. The pinned instance is reused across aliases
and canonical paths; explicit backend policies and newly published artifact
generations retain separate cache identities.

Relation models may advertise `untyped_entities` in their model capabilities.
These models emit generic `ENTITY` nodes and accept relation names without typed
endpoints. Antfarm uses this capability for REBEL artifacts, including renamed
installations, while preserving typed schemas for other relation models.

The CUDA BF16 source-audio route uses NVIDIA cuDNN 9 alongside cuBLAS and
cuBLASLt. Automatic library selection keeps the owned full-F32 audio route when
this vendor stack is unavailable; required-library mode reports the missing
dependency during preparation. The choice is recorded before source cache keys,
rounding or numerical work. Convolution and attention use the normal system loader lookup
for `libcudnn.so.9`. `ANTFLY_INFERENCE_CUDA_CUDNN_LIBRARY` selects an explicit
cuDNN library path; its dependencies must also be resolvable by the process
loader. An explicit path is authoritative and failure does not fall back to a
different installation. Antfly does not search another model runtime's Python
packages. An active source-precision operation does not initialize an SDK or
replay different arithmetic after work has started.

Making cuDNN available can also enable an existing optional attention route.
When validating convolution alone against a previously unavailable cuDNN
attention runtime, pin the existing
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_CUDNN=0` attention policy explicitly and
record the actual dispatch witnesses. SDK availability alone does not establish
attention numerical equivalence.

`ANTFLY_INFERENCE_CUDA_BF16_PREFIX_ATTENTION=1` is an experimental opt-in for
source vision's generic BF16 BSHD prefix-key attention program (HD64,
16-query/128-key tiles, K8 accumulation). It retains all physical query rows,
uses an explicit post-dot F32 scale, and keeps unnormalized BF16 probabilities
with an F32 denominator. The default attention schedule is unchanged. This
candidate needs the regenerated CUDA bundle and independent full-model quality
and hardware performance qualification before deployment; selecting it with a
missing kernel or unsupported device fails instead of changing schedules.

`ANTFLY_INFERENCE_CUDA_SOURCE_BIAS_WORKSPACE_1MIB=1` is an experimental
source BF16 biased-projection policy. It limits the cuBLASLt preference to at
most 1 MiB of the existing funded workspace, preserving its allocation and
lifetime. Vendor plans and audio capture identities distinguish this policy
from the default full funded capacity. Numerical and performance qualification
is required before deployment; no particular vendor algorithm is forced.
