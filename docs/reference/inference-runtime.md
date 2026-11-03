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
