# EmbeddingGemma 2 embeddings and similarity decisions

Antfly runs the open Hugging Face `google/embeddinggemma-2` safetensors checkpoint
on native CPU, Metal and CUDA. Text, images, audio, video and ordered combinations
share the model embedding space. CPU/Metal retain the qualified media and
chunking paths described below. CUDA requires an Ampere or newer GPU and uses
BF16 inference; its video route requires Linux NVDEC and a standalone progressive
SDR H.264 MP4 clip of at most 60 seconds and 256 MiB. CUDA video does not accept
mixed content, titles or explicit sampling policies. ONNX, PJRT and WebGPU are
excluded. The original EmbeddingGemma, GLiNER, Laya and Jev routes retain their
existing contracts.

## Checkpoint and numerical contract

The initial supported checkpoint is revision
`914f7f89142e33e77833254d9c9b90c3cef7303b`. Acquire its original weights and sidecars:

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/reference.py acquire models/embedders/embeddinggemma2
```

The acquisition tool verifies the resolved revision and LFS weight digest, keeps
the upstream license/model card, publishes files atomically, and records SHA256
hashes in `embeddinggemma2_receipt.json`. The weights retain their upstream license.
A managed registry reference is
`hf:google/embeddinggemma-2:safetensors@914f7f89142e33e77833254d9c9b90c3cef7303b`.

The runtime validates the text, vision and audio geometry and weight shapes,
tokenizer BOS/EOS and media tokens, and the pinned image/audio/video processor. It
accepts BF16 or F32 weights. Native CPU and Metal convert BF16 values exactly to
F32 and use F32 activations and accumulation. CUDA uses its model-specific BF16
encoder contract. Quantized and implicit F16 execution are rejected for this
family. CPU/Metal F32 qualification results below do not establish CUDA parity.

The text encoder is bidirectional, including its alternating global and sliding
attention. Mean pooling includes every active token: BOS, EOS, task prefixes and
expanded media tokens. Padding is excluded. The result is L2 normalized. Reduced
sizes 512, 256 and 128 truncate the 768-vector and then normalize again. The
8192-token limit includes all expanded media tokens and wrappers; overflow fails
instead of silently truncating input. Native CPU/Metal audio parts are limited to 30 seconds. CUDA singleton and
cohort audio share the model sample ceiling and the expanded 8192-token limit,
subject to the admitted decode and preparation memory budgets.

Task types are `RETRIEVAL_DOCUMENT` (default), `RETRIEVAL_QUERY`,
`QUESTION_ANSWERING`, `FACT_VERIFICATION`, `CODE_RETRIEVAL_QUERY`,
`SEMANTIC_SIMILARITY`, `CLASSIFICATION` and `CLUSTERING`. Query and document roles
must remain consistent between indexing and retrieval.

## HTTP and ordered groups

Existing flat inputs return one vector per text or media part. A group returns
one vector for all its ordered content. Each request must use one form throughout.
Ordered mixed/video groups use bounded prepared cohorts on CPU and Metal.
Compatible image/audio parts share towers across groups while preserving each
group's order; compatible completed inputs share length-bucketed backbone calls, with at most four groups and 8192 padded rows.
The broker also admits CPU image/audio cohorts; callbacks and caller-owned
profiling/preparation hooks keep their dedicated execution path. Queueing does
not reserve model scratch. A fitting singleton remains available when cohort
admission fails. Flat encoded image rows use bounded vision and embedding-encoder cohorts. Local PDF
page embeddings borrow the renderer's RGBA pixels, including padded row strides,
without PNG encoding or decoding. They share the encoded-image resize and
projector, batching compatible patch grids before executing the embedding encoder
in cohorts of up to four pages. Cohorts
preserve page order, cap token padding at 25%, and occupy at most 2048 padded
token slots. Masks isolate attention and mean pooling by page. Projections are
released after each cohort, and observed execution reports native batching only
when every cohort contains multiple pages. Model identity pins, reduced
dimensions, deadlines and cancellation also apply to this raster path.
On qualified Metal/CUDA row-operation paths, image-only cohorts retain visual
soft tokens, prompt embeddings and encoder activations on the backend through
masked pooling. Only final vectors are downloaded. CPU codec decoding and
model-exact resizing still precede upload; Native keeps the host execution path.
The [real-PDF benchmark](../../zig/pkg/inference/BATCHING.md#gemma4-pdf-measurement-and-resident-embedding-handoff) compares
host/resident handoffs and an experimental GPU painter with the exact CPU renderer.
Production PDF rendering uses the CPU renderer.
For example, post this body to `/ai/v1/embed`:

```json
{
  "model": "embeddinggemma2",
  "task_type": "RETRIEVAL_DOCUMENT",
  "dimensions": 256,
  "input": [
    {
      "title": "Account recovery",
      "content": [
        {"type": "text", "text": "How to reset a forgotten password."},
        {"type": "image_url", "image_url": {"url": "https://example.com/reset.png"}}
      ]
    }
  ]
}
```

A title is valid only for `RETRIEVAL_DOCUMENT` groups containing text. The task
prefix/title is applied once per group. Pure media receives no text task prefix.
Group content follows its supplied order and uses newline separators. Groups
support the existing bounded inline media, URL and framed attachment mechanisms.
The native endpoint allows up to 64 groups, with 1–64 parts each. Existing executor and
aggregate media budgets can impose lower limits. `error_policy: "per_item"`
returns valid groups with their original indexes and errors for invalid groups.

The embedding response includes `model_identity`, a SHA256 identity for the exact
config, weights, tokenizer, processor, SentenceTransformers sidecars and execution
recipe. CPU and Metal preserve the existing
`embeddinggemma2-f32-mean-v6-webm-presentation` identity. CUDA derives a separate
identity from that artifact identity and `embeddinggemma2-cuda-bf16-mean-v1`.
A pinned F32 identity therefore rejects CUDA BF16 execution, including backend
fallbacks that cross precision recipes. Supply the returned value as request
`model_identity` to reject a mismatched generation before encoding. Loaded
artifacts are checked for changes before and after execution. Identity separation
is not evidence of numerical parity; CUDA still requires hardware qualification.

The linked provider exposes `embedDenseGroupsDirectWithControl` and uses the same
validation, media policy, model pinning and admission. CLI examples:

```sh
antfly-inference embed models/embedders/embeddinggemma2 --backend metal \
  --group --title 'Account recovery' --dimensions 256 \
  --text 'Reset a forgotten password.' --image reset.png
antfly-inference embed models/embedders/embeddinggemma2 --backend native \
  --task-type RETRIEVAL_QUERY --text 'How do I regain access?'
antfly-inference decisions models --request decision.json --backend metal
antfly-inference decisions models --request multi-choice.json --backend native
```

## Choice decisions

`/ai/v1/decisions` accepts text `input` and named question arrays. EmbeddingGemma 2
supports `choice` and `multi_choice`, with `decision_method: "embedding_similarity"`
and `similarity_metric: "cosine"`. Answers contain raw cosine similarities, an
acceptance margin and a status. These scores do not represent probabilities or
confidence. Use trained deciders for `score` and `predicate` questions.

```json
{
  "model": "embeddinggemma2",
  "embedding_options": {
    "dimensions": 256
  },
  "questions": [
    {
      "name": "route",
      "type": "choice",
      "instructions": "Route the request to the responsible support team.",
      "choices": [
        {
          "value": "account",
          "examples": [
            "Reset my password",
            "I cannot log in"
          ]
        },
        {
          "value": "billing",
          "description": "Payments, invoices and unexpected charges"
        }
      ],
      "embedding_options": {
        "min_similarity": 0.35,
        "min_margin": 0.08
      }
    }
  ],
  "input": "I forgot my password."
}
```

`CLUSTERING` is the decision default; `CLASSIFICATION` is also supported. Renderer
`instruction-category-v1` renders the state and each example as
`Instruction: {instructions}\nInput: {text}`, and description prototypes as
`Instruction: {instructions}\nCategory: {description}`. The chosen task prefix
is then applied. A label with examples uses the normalized centroid of its
individually normalized reduced vectors; its description is excluded.

Ties within `1e-6` abstain. Optional `min_similarity` in [-1,1] and `min_margin`
in [0,2] cause abstention below their values. Defaults are uncalibrated selection
with tie abstention. Manual threshold examples above are illustrative and must
be evaluated for the application. Responses expose `renderer_version`,
`model_identity` and `prototype_set_hash` for reproducible evaluation.

Each loaded model has an admitted, bounded prototype cache (256 entries, at most
1 MiB). Identity, renderer, task, dimensions and rendered categories determine
reuse. Concurrent identical requests share one fill. Cancellation of a waiting
request preserves the owner's fill; failed owners release the reservation.
Request handles keep the owning model generation alive.

## Multi-choice decisions and batches

Use `type: "multi_choice"` with the same `choices` definitions. Each question
requires `similarity_thresholds`: a raw cosine threshold or a complete value-to-
threshold map, or its own qualified `embedding_options.calibration_id`. The
answer includes `choices`, all `similarities`, effective `similarity_thresholds`,
`margin` and `status`. `selected` means at least one value was accepted; `empty`
means every score was below its threshold and is a valid result; `abstained`
means the requested boundary margin failed and the selected set is withheld.
For multi-choice, `min_margin` measures the nearest distance to any threshold.

Request-wide `embedding_options` supplies only `task_type` and `dimensions`.
Acceptance options belong on each question, so routing and tagging can use
independent calibration artifacts. The endpoint fixes cosine as its metric;
changing metrics would invalidate existing fitted thresholds and margins.

For a batch, replace `input` with `inputs: [{"id": "a", "input": "..."}, ...]`.
The shared questions apply to every input. The response has `data` rows with
`input_index`, optional `id` and named `answers`, plus aggregate `usage`.
EmbeddingGemma 2 standalone decisions are served exclusively by `/decisions`.
Ordinary extraction classification remains available for compatible extractors.
See [the decisions guide](decisions.md) for the complete contract.

For SQL, configure the decider with `provider: "antfly"`,
`decision_method: "embedding_similarity"`, the model name, and optional
`embedding_options` and `model_identity`. `ai_choice` returns SQL NULL on
abstention; `ai_decide` returns the full decision JSON. `ai_score` and
`ai_probability` reject this decider before provider I/O. Materialization
provenance includes the method, model pin, renderer options and criteria.

For a remote decider, set `url` to the inference API base including `/ai/v1`.
Linked embedded deciders use their registered local provider instead.

When indexing, set the Antfly embedder configuration's `model_identity` to the
observed identity and preserve the matching task profile and dimensions. The pin
is included in producer metadata and query cache identity; both linked and HTTP
providers enforce it. Rebuild an index when switching from EmbeddingGemma 1,
changing task/dimension/processor/weights, or changing identity. Equal vector
sizes do not make different generations compatible. Unpinned configurations do
not provide this generation guarantee.

## Fitted raw thresholds and qualification

Each question's `embedding_options.calibration_id` loads
`<model-dir>/calibrations/<id>.json`. It is mutually exclusive with manual
thresholds, and binds to the exact asset identity, renderer, task, dimensions,
mode, ordered labels and prototype set. Runtime validation rejects unqualified,
missing, stale and mismatched artifacts.

Collect real application scores and truth labels with disjoint `fit`,
`validation` and sealed `holdout` samples. The fitting tool consumes:

```json
{
  "binding": {
    "model_identity": "<64 lowercase hex characters>",
    "renderer_version": "instruction-category-v1",
    "task_type": "CLUSTERING",
    "dimensions": 256,
    "prototype_set_hash": "<hash returned for this task>",
    "labels": ["account", "billing"],
    "mode": "single"
  },
  "samples": [
    {"state_sha256": "<SHA256 of input state>", "split": "fit", "scores": [0.7, 0.1], "labels": ["account"]}
  ]
}
```

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/calibrate.py scores.json \
  models/embedders/embeddinggemma2/calibrations/support_v1.json \
  --precision 0.9 --minimum-coverage 0.2
```

Holdout samples are excluded from threshold fitting. Qualification requires at
least 30 fit, 30 validation and 100 holdout samples. Single-label qualification
uses the Wilson 95% lower bound for selected accuracy plus minimum coverage;
multi-label qualification uses per-label precision bounds, minimum F1, and at
least 30 positive and 30 negative holdout examples per label. Thresholds remain
raw cosines. Artifacts are trusted deployment files, not a remotely attested
proof of the dataset or its labeling quality.

The reference tool's `oracle`, `long-oracle` and `media-oracle` commands require
Transformers commit `92cd495f2720c064bc78eb2d93e28704c5bce51f`, PyTorch 2.10.0 and
torchvision 0.25.0. It verifies the upstream implementation hashes and checkpoint
receipt before generating results. Native tests use
`ANTFLY_EMBEDDINGGEMMA2_MODEL`, `ANTFLY_EMBEDDINGGEMMA2_ORACLE`, optional
`ANTFLY_EMBEDDINGGEMMA2_MEDIA_ORACLE`, and `ANTFLY_EMBEDDINGGEMMA2_METAL=1`.
`ANTFLY_EMBEDDINGGEMMA2_SOAK=64` repeats the live managed decision test and checks
that prototype fills remain stable. Retain logs, oracle hashes, binary hashes,
hardware and concurrent-load conditions with each qualification run.

Numerical parity and contract tests do not establish application routing quality.
Release qualification also needs multilingual/domain retrieval and classification
holdouts, task/dimension comparisons, calibrated coverage/accuracy and multi-label
F1, clean-host latency/memory measurements, cancellation/concurrency soak and
index/retrieval migration validation. No production-qualified deployment
calibration artifact is supplied by this change.

Run the socket-level contract campaign against the current supervised server:

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/qualify.py \
  --url http://127.0.0.1:8090/ai/v1 --model embeddinggemma2 \
  --oracle artifacts/oracle.json --media-oracle artifacts/media-oracle.json \
  --iterations 32 --concurrency 4 --output artifacts/live-qualification.json
```

The recorded local qualification covers implementation correctness and serving
contracts; application and release gates remain explicitly open. Raw qualification
and performance reports are retained locally in the ignored
`.benchmark-results/embeddinggemma2/` directory, with a SHA-256 manifest. They
preserve checkpoint, reference, source and binary hashes, numerical errors,
timing samples, logs and resource conditions. This guide retains the results and
reproduction commands; generated reports are excluded from version control.

For the long-context encoder comparison, set
`ANTFLY_EMBEDDINGGEMMA2_BOUNDED=1` to enforce a 512 MiB live host-workspace
allocation ceiling and record its peak. Resident weights and backend allocations
have separate admission accounting. The ceiling measures the request allocator;
it excludes temporary C-heap BF16-to-F32 staging during Metal weight preparation.
Process RSS and VM observations are recorded separately. The CPU media path releases converted HF
weights after each completed layer, while Metal retains its identity-based slots.

## Qualified long-context comparison

The local long-context campaign records the implementation snapshot, pinned
checkpoint, source and binary hashes, all measured samples, rejected attempts,
parity checks and same-binary serving results. Its raw report is archived locally
as `.benchmark-results/embeddinggemma2/embeddinggemma2-long-context-performance.json`.
Measurements use an M4 Pro with 24 GiB RAM, Zig 0.17.0 ReleaseFast, official
PyTorch 2.10 MPS execution and four CPU threads.

The agreed gate is synchronized encoder p50 no slower than 1.10 times PyTorch
MPS at both 512 and 8192 tokens in each of three process orders. Each process
warms both cases twice, then measures 20 requests at 512 tokens and five at 8K.
The orders are MPS/baseline/candidate, baseline/candidate/MPS and
candidate/MPS/baseline. All three passed:

| Prepared encoder | Archived Metal baseline | Current Metal | PyTorch MPS | Current / MPS |
| --- | ---: | ---: | ---: | ---: |
| 512 tokens | 88.18–88.33 ms | 59.10–59.19 ms | 78.74–78.80 ms | 0.750–0.752 |
| 8192 tokens | 4.461–4.466 s | 2.023–2.035 s | 3.417–3.531 s | 0.576–0.592 |

These intervals span the three process-order medians. Both implementations time
embedding lookup, encoder execution, active-token pooling, reduced dimensions,
normalization and synchronized vector readback. Tokenization, receipt validation,
model loading and HTTP are outside this boundary. PyTorch prepares IDs and masks
on MPS before the timer; Antfly stages host IDs inside its lookup timing.
The comparison preserves lossless BF16-to-F32 weights and F32 inference.

Only measured intervals with zero new system pageouts, swapins and swapouts
qualify. One MPS attempt recorded a pageout; the driver retained the entire
attempt and repeated that process order. The older baseline lacks sample-window
and GPU-memory telemetry, so its whole process must be paging-free; the current
candidate must supply GPU drain telemetry. An initial driver metadata error is
also retained with its correction. Existing allocated swap on this workstation
remains documented in the report.

The current text path keeps lookup and scaling, encoder activations and masked
mean pooling on Metal, truncates on the device and reads back the requested
vector. Existing finite checks and F64 centroid normalization complete on CPU.
Grouped attention preserves the original two sliding or one global KV heads,
skips identity gathers and explicitly uses the model's score scale of 1.0.
Dense attention through 512 rows and long global layers use packed F32 MPS
QK, masked softmax and PV operations. The query tile is 512 through 512 rows,
clamped to the actual row count, and 256 for longer rows. This follows the
[official PyTorch MPS attention implementation](https://github.com/pytorch/pytorch/blob/v2.10.0/aten/src/ATen/native/mps/operations/Attention.mm).
Long sliding layers use 16-query/16-key blocks with 256-dimensional staging.
Unsupported grouped dispatch retains the existing device attention path.

The packed 8K global workspace is 130 MiB, reused across query tiles. Its hard
limit is 256 MiB per operation. Metal request admission reserves 768 MiB of
workspace, including the additional 256 MiB envelope; the live host allocator
retains its 512 MiB ceiling. The encoder helper's 8K host peak decreased from
40.75 MiB to 2.35 MiB, with zero host allocations after release. Separate warm
diagnostics sampled `MTLDevice.currentAllocatedSize` at every layer boundary:
additional allocated GPU bytes were 1.5 MiB at 512 tokens and 17.03 MiB at 8K.
Raw frame-retained ledger bytes include multiple references to reused or aliased
buffers and should not be interpreted as unique allocations. Boundary samples
also leave transient allocations inside opaque MPS operations unobserved.
Request completion drains frame-retained ownership and pending scratch slots.
Conditional PLE projection blocking remains deferred because both performance
targets pass; longer inputs retain one frame per layer.

Validation includes independent F64 attention references for both head sizes,
original KV counts, explicit score scales, three visibility ranges, empty rows,
mask holes and workspace denial. A separate full-8K attention check samples
eleven query rows across tile and batch boundaries; maximum error is `4.38e-6`.
Pooling checks cover masked NaNs and all four dimensions. Mid-tile cancellation
and timeout preserve their original error, drain before buffer release and allow
retry. A queued output-wrapping allocation failure and in-frame workspace reuse
also pass. Real-checkpoint tests cover padded 512/514-row batches and cancellation.
All eleven 511/512/513/8191/8192-token boundary and dimension cases pass within
`8.2e-7` against MPS on both CPU and Metal.

The fresh fifteen-case HTTP comparison passes within `1e-5` on CPU and Metal,
preserving all three complete retrieval rankings and the selected choice.
Current Metal HTTP medians are 71.76 ms at 512 tokens and 2.058 s at 8K. The
short-request baseline/candidate/candidate/baseline control records essentially
unchanged short-query latency, cached choice +0.47%, and 128-token latency +4.49%;
all pass the 5% regression gate. Each backend passes the 36-request HTTP contract
campaign and eight requests each at 512 and 8192 tokens from four clients.
Every concurrent vector is bit-identical to that backend's serial result.
Metal required zero capacity retries. The CPU 8K burst required 17 retries under
the unchanged 6 GiB host budget: responses were explicit `MODEL_RESOURCE_BUSY`,
`retryable: true`, `retry_after_ms: 1000` and `Retry-After: 1`. Bounded client
retries complete all requests. This checks overload behavior and recovery;
sustained throughput and deployment resource targets remain open.

The source is locally qualified for the agreed text/routing performance scope.
The report keeps `production_qualified: false`: application retrieval/routing
holdouts, deployment load and clean-host measurements, other target hardware
and index migration still require qualification. Media passes numerical and
serving checks; image/audio latency remains about 6.1/11.6 times MPS and is
outside this performance target.

Reproduce the prepared-text campaign after archiving the baseline executable
and a source manifest mapping repository-relative files to SHA-256 digests:

```sh
cd zig/pkg/inference
zig build build-embeddinggemma2-bench -Dmetal=true -Doptimize=ReleaseFast -j1
cd ../../..
python3 zig/pkg/inference/scripts/embeddinggemma2/encoder_campaign.py \
  --model-dir <model-dir> --suite suite.json \
  --baseline <archived-encoder-binary> \
  --candidate zig/pkg/inference/zig-out/bin/antfly-embeddinggemma2-bench \
  --source-manifest <frozen-source-manifest.json> --output-dir <campaign-dir>
cd zig/pkg/inference
zig build check-embeddinggemma2-attention -Dmetal=true -Doptimize=ReleaseFast -j1 \
  -- <model-dir> <attention-check.json>
```

The manifest requires a `files` object and the candidate's `binary_sha256`.
Keep the source and executable immutable during the campaign, and set
`PYTHONPATH` to the pinned reference dependencies. Diagnostic rollback controls:

- `ANTFLY_EMBEDDINGGEMMA2_DISABLE_RESIDENT_TEXT=1`
- `ANTFLY_EMBEDDINGGEMMA2_DISABLE_GROUPED_ATTENTION=1`
- `TERMITE_METAL_EMBEDDINGGEMMA2_DISABLE_GEMM_ATTENTION=1`
- `TERMITE_METAL_EMBEDDINGGEMMA2_GEMM_QUERY_TILE=128|256|512`
- `TERMITE_METAL_EMBEDDINGGEMMA2_SLIDING_QUERY_BLOCK=16|32`
- `TERMITE_METAL_EMBEDDINGGEMMA2_SLIDING_KEY_BLOCK=8|16`

Use `ANTFLY_EMBEDDINGGEMMA2_PROFILE_ENCODER=1` with
`TERMITE_METAL_STAGE_TIMING=1` for separate attribution and memory diagnostics.
These diagnostic timings do not set the performance gate.

## Compact PyTorch comparison

`zig/pkg/inference/scripts/embeddinggemma2/compare.py` compares the same pinned F32 checkpoint with
official PyTorch CPU and MPS execution. Eight edge cases cover a short document,
a short retrieval query, exact 128/512/8192-token documents, image, audio and an
ordered text/audio/image group. Four additional documents and three queries
check complete retrieval rankings and cosine scores. A three-category choice request compares the
rendered token IDs, raw cosine scores, margin and selected category. It requires
no evaluation dataset.

Prepare the suite once, then run each backend in a separate process:

```sh
export PYTHONPATH=<pinned-python-dependencies>
export VECLIB_MAXIMUM_THREADS=4 OMP_NUM_THREADS=4
python3 zig/pkg/inference/scripts/embeddinggemma2/compare.py --backend prepare \
  --model-dir <model-dir> --artifacts-dir <oracle-and-media-dir> --output suite.json
python3 zig/pkg/inference/scripts/embeddinggemma2/compare.py --backend mps \
  --model-dir <model-dir> --suite suite.json --output pytorch-mps.json
python3 zig/pkg/inference/scripts/embeddinggemma2/compare.py --backend metal \
  --model-dir <model-dir> --suite suite.json --reference pytorch-mps.json \
  --binary zig/pkg/inference/zig-out/bin/antfly-inference --output metal.json
```

Use `native` and `cpu` for the CPU pair. Compile the native CLI with
`-Doptimize=ReleaseFast -Dmetal=true`; exclude compiler and other inference work
from the measurement interval. GPU timings synchronize completion and include
the final vector's transfer to CPU. Native timings include HTTP, serialization,
tokenization and media preparation. PyTorch records both encoder timings and
preprocessing plus inference timings; it has no HTTP server in this comparison.

The default is two warmups and ten measured short-text requests, three measured
requests per media case, and one measured 8192-token request after warming the
encoder. Long-context results are single observations, not latency percentiles.
Choice routing measures ten requests with cached category prototypes, followed
by 64 requests to observe retained process memory. Reports retain exact samples,
vectors, binary/suite hashes, hardware, thread settings and VM counters.
Supplying `--reference` gates vectors, full retrieval rankings, raw scores,
choice and margin against that completed runtime result. It also rejects
different suite, checkpoint receipt or precision identities. The small corpus
checks numerical agreement; it does not estimate application retrieval quality.

Sliding layers restrict their actual attention ranges while preserving original
positions, padding holes and batch isolation. Metal groups the encoder into one
owned frame for at most 512 total rows, including padding and batch. Larger
inputs use workspace-bounded layer groups, releasing completed activation views
between groups; independent rows can batch across the former 512-row cutoff. Frames
complete or cancel before their transient projection slots are released. Cached
MPS activation views are released at frame completion and request teardown;
only admitted model weights and multiplication plans persist across requests.
Full-attention layers retain their global range. The grouped Metal hook and
bounded GEMM schedule are described above. The online device fallback uses
16-query tiles for 256-dimensional heads and 32-query tiles for 512-dimensional
heads, subject to device limits. Short inputs retain the existing attention
route. All paths preserve F32 arithmetic and exact visibility.
Native segment attention uses bounded F32 BLAS tiles. The default is
128 queries and 512 keys. EmbeddingGemma 2 uses 512-query tiles for 256–512
queries and for longer inputs with 512-dimensional global heads, with 1024-key
tiles when there are more than 512 keys. Short inputs and long sliding layers
retain the default tiles.
Exact windowed key ranges preserve positions, padding holes and batch isolation,
avoiding repeated window checks on CPU and Metal. CPU masks gaps between up to
three sorted key ranges in contiguous spans; generic finite-window callers
retain their per-key position checks. Rotary
frequencies are computed once per operation and trigonometric values reused
across heads at the same position. The shared per-layer projection is acquired
once per encoder pass. On Metal, one packed projection and RMSNorm produce
all layers' per-layer inputs when the packed output fits a 64 MiB ceiling.
Individual layers slice their columns on the device. Longer Metal contexts
pack only the current owned layer group rather than allocating the complete
all-layer output. CPU retains layer-at-a-time projection. Packed outputs and
live layer groups remain inside the existing admitted execution workspace.
CPU execution reuses uniquely owned RMSNorm and GELU inputs at their last use
and retires replaced projection/reshape handles. Aliased inputs and unsupported
consume operations retain independent output storage. Errors preserve source
ownership for cleanup or retry. Independent activation references, alias
preservation, allocation failures and retry behavior have focused coverage.
BF16 widening uses lossless vector operations, and Metal
linear preparation uses owned shared snapshots on unified-memory devices.
Immutable language, vision and audio weights have independent model-owned
payload ceilings: 640 MiB each for language/vision and 1536 MiB for the larger
audio tower. All payloads require resident host or backend admission before
allocation, with at most 1536 entries across the classes. CPU retains lossless
BF16 mirrors; Metal retains prepared F32 projection slots. Packed vision QKV
snapshots replace the three individual projection snapshots. Warm named QKV
execution acquires the packed snapshot before loading any original weights.
Vocabulary, mutable tensors and transient views are excluded. Cache denial
keeps bounded request-scoped preparation. Admission pressure can reclaim idle
classes; native compute contexts pin borrowed mirrors until teardown, and Metal
reclamation refuses active/submitted command groups. Unloading releases payloads
and their admission ownership; request workspace remains bounded independently.

Vision normalization and two-axis RoPE execute together without intermediate
transposes. Native dense vision attention uses bounded GEMM tiles, SIMD row
maxima and normalization, and no segmented-range sorting or masking. BLAS
execution holds one packed head and a 1 MiB score tile; bounded row workers
run only after each GEMM has completed. Without BLAS, a portable SIMD kernel
packs head-major inputs once and runs independent heads around sequential
register-tiled GEMM, avoiding nested worker pools.
BLAS tiles calibrate over ordinary completed calls per geometry and batch size:
each candidate gets one discarded warm sample and three measured samples, then
the lower median wins. A fixed 64-entry metadata table bounds exploration and
stores no tensors; failed calls release their tickets without training. Disable
exploration with `ANTFLY_EMBEDDINGGEMMA2_DISABLE_ATTENTION_CALIBRATION=1`.
Legacy layouts retain their existing path. Native backbone grouped
attention preserves compact K/V heads and reuses their packed rows across query
heads, with explicit score scaling and the same masks/windows as Metal. Compatible FFNs use fused GELU/product execution; CPU streams through
SIMD tiles and parallelizes large independent output ranges. Native audio local
attention intersects legal key ranges once per query and uses SIMD reductions
with at most four workers, retaining relative-position terms and logit capping.
Cancellation drains all workers before borrowed inputs or scratch are released.

Audio conformer cohorts concatenate independently subsampled clips. Shared
projections and FFNs run across the cohort, while temporal attention and causal
convolution reset at each clip boundary. Metal completes each owned conformer
layer before releasing its activation views. Native preparation omits unused
device DFT packing. Mixed image/audio/video prompts retain available device
tokens through composition, and strict audio preparation validates duration
using the decode consumed by the tower.

Prepared mixed cohorts preserve source/model identity and per-item failures.
Length buckets cap padding at 25%; original positions restore delivery order.
Initial preparation and retained inputs stay admitted while an independently
checked supplemental lease funds any larger encoder peak. The base lease never
grows or loses ownership, and both leases outlive backend teardown. Resource
pressure unwinds a cohort before retrying independent calls.

Mixed cohorts gather image/raster and encoded/PCM audio parts into bounded tower
waves. One CPU-only future wave may prepare while the current tower executes;
its independent shared lease and allocator retire after join. Scatter maps keep
part/group identity through geometry and audio-length differences. Invalid
preparation marks the original group, and pressure drains all producers before
independent execution retries.

Model-owned packed linears combine compatible backbone QKV, gate/up and audio
QKV weights under the existing cache class limits. Audio QKV packing requires
equal input clamps and preserves each output clamp. Packed FFNs consume
interleaved gate/up rows directly with portable SIMD workers or a Metal kernel. Dense
native GEMM output skips redundant zero initialization when beta is zero.

Flat text arrays tokenize once and share length-sorted encoder calls, with at
most four items, 8192 padded rows and 25% padding overhead. Results scatter to
input order; admission pressure drains and splits a cohort before retrying.
Explicit alternate pooling/projection configurations retain their own contract.
Metal grouped GEMM tiles stop at disjoint item ranges and compute only each
tile's visible key envelope; score storage uses that envelope's stride. CPU
portable grouped attention shares packed K/V across bounded head workers, with
sequential inner GEMMs. BLAS retains its own worker scheduler.
Backbone head preparation reads packed projections directly and combines head
RMSNorm, split-half RoPE and row gathering/compact-KV expansion. Gather indices,
window ranges and aligned device visibility metadata are prepared once per
forward call and retained until completion/cancellation. Vision FFNs share
admitted packed gate/up weights and consume them directly on CPU/Metal without
slice buffers. Image/video position lookups preserve typed table storage and
gather the required rows instead of copying the full 60 MiB F32 position table.

Language QKV and gate/up projections use one canonical packed snapshot for all
admitted sequence lengths, including inputs above 512 rows. Native individual
projections borrow output-row slices; Metal applies output-row views directly
from the parent slot. Neither view creates a second persistent weight payload.
The model lease protects views, and cache pressure reclaims the parent as one
allocation. Packed QKV/head preparation and packed GELU avoid extraction copies;
activation storage remains bounded by the existing admitted row envelope.

The pure Zig CPU backend uses model-owned F32 GEMM panels as its canonical
prepared matrix representation. Panels are built directly from typed checkpoint
rows, without first allocating a dense F32 mirror. Individual language projections
and packed QKV/gate-up calls share one named panel owner; aligned output-row views
borrow its panels. Portable SIMD operates across output columns with 64-column
panels, 256-element K blocks and bounded row blocks. System BLAS retains its own
kernels. Canonical panels obey the existing shared host budget and class ceilings;
allocation or admission failure uses the typed/direct fallback. Request readers
pin panel owners until teardown, and idle reclamation/unload releases their exact
admission. Optional panels for pre-existing dense snapshots still reserve dense
weight headroom.

CPU row slices retain their source owner, including typed checkpoint mappings,
instead of copying matrix weights. CPU per-layer input projections are grouped
under a 16 MiB projected-output ceiling; long inputs keep the streamed path.
Metal backbones above the small-input frame threshold encode one bounded layer
group while its predecessor executes. MPS activation views transfer to their
encoding frame and retire on its completion. The schedule rolls forward without
draining the successor. The two live groups share the existing 128 KiB/row layer
envelope; scratch released by the successor cannot be recycled by predecessor completion.
Cancellation drops unsubmitted work and drains submitted work before releasing
owners. `ANTFLY_EMBEDDINGGEMMA2_DISABLE_PIPELINED_ENCODER=1` selects serial
submission for diagnostics.

Mixed media cohorts use a bounded readiness scheduler. A group executes once
all its required projected parts are available, while the next CPU preparation
wave runs. Completed CPU preparation buffers and their admission are released
before running ready backbones. A singleton may wait one preparation wave to
coalesce a useful backbone batch; ready work cannot be deferred indefinitely. Waves remain
homogeneous batches of at most four items but can
cross image/audio boundaries. Text-only groups can overlap the first media
preparation wave. Finished groups release projected tokens promptly; public
result order, per-group failures, cancellation and shared admission remain
unchanged. GPU tower and backbone work stay ordered on one leased backend.


Metal grouped attention folds query heads sharing a KV head into matrix rows,
then batches distinct KV heads for dense key envelopes. Partial envelopes
encode one QK/PV pair per KV head, avoiding padded RHS batch strides and K/V
expansion. Range/window masking stays exact. Query packing and score/output
storage are bounded by the current tile; padded tail rows are not multiplied.
Grouped K/V and resident visibility metadata now cover short queries too;
metadata is uploaded once per forward, rather than once per layer. Queries
below 64 rows with at most 64 keys, or visible envelopes through 32 keys,
use a head-width-specialized SIMD-group kernel with no threadgroup scratch.
Aligned 512-wide global heads with key
envelopes through 128 use fused 64-key F32 attention. Local envelopes through 1536 use GEMM,
even above 512 total rows. Longer global heads retain GEMM when its workspace
fits. The 1536-key bound covers a 512-query tile plus two 512-key windows.
When GEMM workspace cannot be admitted, global heads use fused 64-key tiles
on aligned shapes and a tail-safe 32-key variant otherwise. These kernels
retain online softmax and F32 matrix accumulation without materializing score
tiles, with 21,632 and 27,776 bytes of threadgroup storage respectively.
Device capability checks retain the original compact fallback. Explicit
strategy, query-tile and key-block overrides are reserved for qualification;
workspace limits and cancellation still apply.

The attention qualification executable also compares Flash against GEMM tiles
of 128, 256 and 512 queries, plus automatic dispatch, at 128/512/2048/8192
total rows, for both head
geometries and single/four-item visibility. It reuses resident metadata,
restricts local visibility before dispatch, checks independent F64 outputs,
interleaves candidates, and records ten completed GPU/host samples plus paging
counters. Explicit qualification overrides obey the workspace ceiling. Timing
receipts qualify attention only; they do not establish end-to-end latency or
PyTorch parity. Run on an otherwise idle device and retain the binary/source
identity with the receipt:

```sh
cd zig/pkg/inference
zig build build-embeddinggemma2-attention-check -Doptimize=ReleaseSafe
zig-out/bin/antfly-embeddinggemma2-attention-check /path/to/model \
  /path/to/attention-timings.json --benchmark
```


Audio relative-position inputs, query scales, normalization ones and completed
per-layer relative-key projections are cached as immutable model constants.
Prepared constants have an 8 MiB/128-entry ceiling within the audio class and
shared memory admission. Device publication copies into fresh model-owned
storage and waits for completion; failed copies are not published. Cancellation
releases the consumer's handle; completed snapshots remain reusable on retry.
Warm device execution performs no scale readback. Audio pressure reclamation and model
unload release these constants alongside prepared weights.

Native audio local attention scales each query once per head in worker-local
SIMD scratch, reusing it across keys. Softmax skips masked entries when their
exponentials are guaranteed to round to zero in F32; other finite masks keep
the general normalization path. Clip boundaries and cancellation remain local
to each row.

Backbone post-normalization/residual epilogues use one backend operation,
including the final per-layer output scale. PLE GELU and multiplication share
one activation operation. Audio uses the same post-normalization/residual
primitive, with its residual weight applied before addition, and combines
scalar output clipping with SiLU. CPU uses portable Zig SIMD; Metal uses
precise F32 kernels. The operation contract preserves normalization, weight,
scale, addition and final-scale order. Unsupported backends retain the
separate-operation path. Model-owned scalar clamp metadata is bounded to 256
entries with keys of at most 192 bytes, owns no request tensors, and is released
on model teardown. Mutable compute contexts do not publish it.

Cold Metal packed projections populate the final owned F32 snapshot directly
from mixed BF16/F32 checkpoint segments. No combined host F32 matrix or
per-segment F32 staging is retained. Shared storage is used on unified-memory
devices; other Metal devices blit that same generated staging buffer directly
to private storage, avoiding a second shared upload allocation.
Publication remains atomic: failed preparation preserves any previous slot,
and the existing class/admission limits charge the final allocation.


Embedding-only calls apply the final token RMSNorm, masked-mean pool hidden
rows, then project once per item. Token-output callers retain the original
per-token projection. The final bias-free linear commutes with masked mean;
token RMSNorm does not. Metal pools, projects, truncates and normalizes the
whole cohort inside its owned command frame and returns one compact readback.
Admission accounts compact output vectors; the backbone workspace remains
bounded independently. Dimension truncation stays after projection and before
final L2 normalization.

Warm embedding admission clones the loaded generation's validated manifest,
including processor and capability policy, while briefly holding a model pin.
Identity verification remains in place. Retirement/reload publishes a new
snapshot; cold loads retain ordinary filesystem validation.

For independent-versus-cohort replay, including unequal audio clips, mixed
ordering, repeated media spanning preparation waves, captioned video, partial
failures and concurrent callers:

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/cohort_qualify.py \
  --binary zig/pkg/inference/zig-out/bin/antfly-inference-bench-server \
  --model-dir /path/to/model --backend metal \
  --output .benchmark-results/cohort-metal.json
```

Repeat with `--backend native`. The receipt pins the binary, corpus and model
identity and requires observed native batching; these correctness/scheduling
checks do not qualify the recorded latency against PyTorch.

For a paired CPU kernel diagnostic, including portable SIMD attention:

```sh
cd zig/pkg/inference
ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache" \
  ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
  ANTFLY_BENCH_DENSE_VISION_TILES=1 VECLIB_MAXIMUM_THREADS=4 \
  zig build test-embeddinggemma2 -Doptimize=ReleaseSafe \
  -Dtest-filter="embeddinggemma2 dense vision tile benchmark"
```

This compares actual attention outputs and completed kernel times at several
patch grids. It is a kernel diagnostic, not an end-to-end throughput gate.

The prepared CPU matrix diagnostic reports cold panel setup, retained bytes,
paired direct/prepared timings, and output agreement for each geometry, including
1-, 4- and 15-row projections as well as larger encoder batches:

```sh
zig build bench-linalg-prepared -Doptimize=ReleaseFast
```

`build-linalg-prepared` and `install-linalg-tests` build artifacts for execution
on another host. Qualify Linux x86 with both
`ANTFLY_INFERENCE_X86_KERNEL=portable` and `avx2`; the latter requires CPU and
OS AVX2 support. Translation can check correctness but does not establish
native hardware throughput.

The Metal attention diagnostic (`build-embeddinggemma2-attention-check`, then
`antfly-embeddinggemma2-attention-check MODEL OUTPUT --benchmark`) includes
short and long queries, aligned boundaries, isolated items, three GEMM query
tiles, specialized short and fused global kernels, and automatic dispatch.
It checks independent F64 outputs, completed GPU time, paging counters and
frame retirement. Identical or unsupported kernel variants are omitted.
These measurements do not replace completed whole-request comparisons.

### Per-case performance gate

Use `performance_gate.py` with completed `compare.py` or `video_compare.py`
receipts. It rejects mismatched weights, precision, suite/corpus, backend pairs,
missing binary identities and mixed timing boundaries, and recomputes latency
from raw samples. Every case must preserve F32 vector parity and meet the median
ratio independently; the default ceiling is 1.10 and minimum sample count is 10.
Insufficient samples, unobserved paging counters, new pageouts/swap traffic
and slow modalities fail rather than disappearing into an aggregate. Prepared
and request measurements record warm-sample VM counters outside their timers;
older reports fall back conservatively to whole-process paging observations. A gate failure is retained in the output file with a nonzero exit.

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/performance_gate.py \
  --candidate native.json --reference pytorch-cpu.json --output cpu-gate.json
python3 zig/pkg/inference/scripts/embeddinggemma2/performance_gate.py \
  --candidate metal-encoder.json --reference pytorch-mps-encoder.json \
  --boundary encoder --output metal-encoder-gate.json
```

Prepared text encoder timing excludes HTTP and tokenization on both runtimes.
Request comparison includes Antfly HTTP versus reference preprocessing and
inference without HTTP. Cold setup, p95, memory, concurrency and cancellation
remain separately reported boundaries. Images, PDF pages and GIF frames share
the vision path, but source decoding/extraction must also be measured for each
complete service workload. Passing isolated model cases does not certify those
wrapper costs or universal PyTorch performance parity.


### Earlier local measurements

The following measurements and development notes precede the long-context
implementation above. Their original binary/source scopes are preserved in the
locally archived reports; statements about the current binary in these notes
refer to that earlier snapshot.

The earlier performance comparison uses an M4 Pro Mac with 24 GiB RAM. All
fifteen cases agree with PyTorch on CPU and Metal:
vector errors stay below `1e-5`, all three complete retrieval rankings match,
and routing scores differ by less than `1.8e-7`, with the same selected category.
Observed local latency is:

| Case | Native CPU | PyTorch CPU | Metal | PyTorch MPS |
| --- | ---: | ---: | ---: | ---: |
| Short retrieval query | 32.1 ms | 21.6 ms | 26.6 ms | 16.1 ms |
| 512-token document | 208 ms | 145 ms | 99.3 ms | 79.4 ms |
| 8192-token document | 6.25 s | 6.58 s | 4.48 s | 3.43 s |
| Cached choice routing | 30.3 ms | 23.4 ms | 18.3 ms | 17.0 ms |

Values are medians except the single 8192-token observation. PyTorch values
include preprocessing and inference and exclude HTTP. CPU BLAS thread settings
and PyTorch use four threads. EmbeddingGemma2 CPU attention uses independent
heads for 128–512 queries, at most 512 keys and two to four heads when BLAS
and runtime Io are available. All scratch is allocated on the caller before
launching jobs; completion or cancellation drains every job before release.
Short, long, portable and no-Io paths retain sequential attention.
GELU buffers with at least 262144 elements use at most four independent chunks
when an EmbeddingGemma 2 native session has runtime Io. Chunks preserve the
existing SIMD groups and scalar tail, borrow disjoint input slices, and allocate
no activation scratch. Cancellation drains all chunks before returning;
failed copied outputs are released and consumed inputs retain caller ownership.
Short buffers, other model families and no-Io sessions keep the serial path.
These measurements ran sequentially on a working desktop with existing swap,
using fresh PyTorch baselines. Antfly CPU and Metal recorded no pageouts,
swapins or swapouts. PyTorch MPS recorded 30 system pageouts; these system
counters include desktop activity. The 64-request routing soak recorded about
0.73 MiB net growth in owned-process CPU RSS and no net growth on Metal.
The native macOS request allocator now uses libc under the same 512 MiB live
workspace limit. Live allocation accounting and allocator-retained physical
memory are measured separately; these short observations do not establish a
long-duration memory limit.

The PR performance target is text search/retrieval and choice routing with
comparable Metal execution, documented HTTP/validation overhead, and a tested
CPU fallback. Media numerical and serving support is tested, while comparable
media latency is outside that target. The same run measured:

| Media case | Native CPU | PyTorch CPU | Metal | PyTorch MPS |
| --- | ---: | ---: | ---: | ---: |
| Image | 5.97 s | 928 ms | 3.34 s | 554 ms |
| Audio | 90.0 ms | 209 ms | 396 ms | 35.0 ms |
| Ordered text/audio/image | 6.04 s | 1.12 s | 3.69 s | 577 ms |

These paths preserve numerical parity, but Metal image and audio latency remains
about 6.0x and 11.3x PyTorch MPS for these inputs. Media performance needs further
work for a deployment that relies on it.

EmbeddingGemma 2 now enables parallel Metal RMSNorm on its model-owned runtime.
Compatible 256- and 512-dimensional rows use the existing SIMD kernel: 128
threads per row below 256 rows, and 32 threads per row from 256 through 32768
rows. Incompatible shapes, unsupported devices and other model owners retain
existing dispatch selection. The bounded complete-encoder frame now admits
up to 512 total rows; larger inputs still complete one frame per layer.

Eight runs on the preceding qualified binary compared both Metal changes
independently in forward and reverse order, using eleven existing text cases
and cached routing. Each
variant passed vector, ranking and routing-score parity. The two observations
per variant were:

| Metal policy | Short query | 512-token document | Cached choice |
| --- | ---: | ---: | ---: |
| Previous normalization and 128-row frame | 40.1–40.3 ms | 148.2–148.4 ms | 30.2–30.3 ms |
| Parallel normalization only | 29.0 ms | 117.9–118.4 ms | 18.6–18.7 ms |
| 512-row frame only | 40.2–40.3 ms | 130.0–130.1 ms | 30.0–30.4 ms |
| Both, current default | 28.8 ms | 101.3–101.5 ms | 18.6–18.7 ms |

The independent controls confirm gains from both changes. The full fifteen-case
run includes media and 8K inputs, and passed checkpoint, padded-batch,
cancellation/retry, bounded workspace and live HTTP checks. Diagnostic traces
confirm the SIMD pipeline is reached and that the complete 512-row encoder
uses one frame instead of 25. Dispatch tracing changes encoder policy, so its
timings are attribution evidence rather than production latency baselines.

The Metal policies and CPU GELU scheduling have independent rollback flags,
read before starting a worker:

```sh
export TERMITE_METAL_DISABLE_EMBEDDINGGEMMA2_PARALLEL_RMS_NORM=1
export ANTFLY_EMBEDDINGGEMMA2_DISABLE_MEDIUM_METAL_FRAME=1
export ANTFLY_EMBEDDINGGEMMA2_DISABLE_PARALLEL_CPU_GELU=1
```

The default needs none of these flags. The earlier process-wide A4B normalization
option is retained for its existing uses; EmbeddingGemma 2 qualification now
uses the model-owned policy and does not require that process-wide option.

Retaining the CPU per-layer projection schedule reduced measured 512-token
host workspace from about 74 MiB to 26 MiB before parallel head scratch. The
current 512-token CPU peak is about 41 MiB; latency improved with parallel
heads and the macOS allocation change. Metal
retains its packed projection schedule. The packed Metal schedule and frame boundary are
numerically checked at 512 total padded rows and the bounded per-layer schedule
at 514 rows; 8K host workspace is about 388 MiB on CPU and 49 MiB on Metal.
Earlier larger attention tiles improved the observed Metal 8K time from
9.35 s to about 4.51 s (2.07x).

Two additional normalization fusion trials preserved numerical parity but
regressed short retrieval and routing latency. The combined head/RoPE and
residual trial measured 46.1 ms and 36.2 ms, respectively; residual fusion alone
measured 43.4 ms and 34.2 ms. Both were removed. The report retains their source,
binary, parity and timing evidence.

A shared RMS slot preparation experiment reduced isolated upload time, but
produced no meaningful whole-model gain. The encoder uses direct device
weight tensors and bypasses those slots; the report records this distinction
and the same-binary control. Replacing redundant attention window checks and
masking range gaps reduced CPU 512-token median latency from 286 ms to 272 ms
and the single 8K observation from 9.09 s to 7.89 s, with identical isolated
attention outputs and passing real-model parity. Metal timings were broadly
stable. Independent tests cover unordered, overlapping, reversed and empty
ranges, tile clipping, exceptional lane bits, and dense masked-softmax results.

An interleaved CPU tile experiment then selected wider query tiles for moderate
inputs and global heads, while retaining narrower tiles for long sliding layers.
The subsequent real-model run reduced the single CPU 8K observation from 7.89 s
to 6.77 s, within 3.4% of the fresh PyTorch observation. CPU 512-token median
latency moved from 272 ms to 267 ms; short inputs and Metal were broadly stable.
The larger tiles add about 1.75 MiB to peak 8K request workspace. An independent
portable attention reference checks both key tile sizes, irregular masks,
finite windows and fully empty rows; real checkpoint, padded-batch and live
serving checks passed on the final binary.

Reusing CPU activation storage subsequently reduced the observed 512-token
median from 267 ms to 248 ms and the single 8K observation from 6.77 s to
6.48 s. All fifteen CPU vectors were bit-identical to the previous qualified
binary; request-workspace peaks were unchanged. Short-query latency was
broadly stable. The report retains the prior measurements and a current worker
profile, with fresh PyTorch baselines and passing same-binary serving checks.

Parallel CPU heads and libc workspace allocation subsequently reduced the
512-token median from 248 ms to 224 ms, about 10%. All fifteen CPU and Metal
vectors were bit-identical to the previous qualified binary, with unchanged
retrieval rankings and routing decisions. CPU 8K timing stayed near 6.48 s,
within 1% of the fresh PyTorch observation. Independent F64 mask checks,
exhaustive allocation failures, cancellation while heads are running, and
retry tests passed. The native output wrapper now frees attention data if
its metadata allocation fails.

The latest qualified build adds parallel CPU GELU and a fused conservative tokenizer
scan. Four current-binary CPU runs in serial/parallel/parallel/serial order
measured 512-token latency at 225.1–225.5 ms with serial GELU and 208.1–208.4 ms
with parallel GELU, about an 8% reduction. Short-query and cached-routing
latency stayed broadly unchanged. All eleven control-case vectors were
bit-identical across all four runs. The full fifteen-case CPU and Metal vectors
were also bit-identical to the preceding qualified binary. Independent F64 and
exceptional-value checks, SIMD-tail parity, cancellation during borrowed chunk
execution, ownership, alias fallback and retry passed. The current focused
suite passed 78 tests with five unconfigured checkpoint skips; separate
configured CPU/Metal checkpoint and long-context campaigns passed.

Two current-binary HTTP campaigns passed with 36 requests and four clients
each, on native CPU and Metal. Each backend also completed eight concurrent
requests using the existing 512-token document. Every response was bit-identical
to its backend's serial result and matched PyTorch within `2.4e-7`. The CPU burst
completed in 0.84 s and the Metal burst in 0.73 s. These local checks exercise
concurrent correctness; they do not establish sustained throughput targets.
The report records source and binary hashes, checkpoint results, worker
profiling and resource observations. Real-model workspace checks explicitly
verify zero live request bytes after output release, including ReleaseFast
builds. Padded batches exercise cancellation and retry at 512 total rows.

An extended retention check then warmed a fresh CPU process and repeated the
existing routing input 256 times, with 512-token and 8K document cycles.
Additional RSS was about 5.6 MiB on the preceding qualified binary versus 58.9 MiB in the
identical check on the archived previous binary. Late routing samples on both
still showed small growth, so this comparison establishes a local observation
rather than a sustained deployment memory bound. All document vectors stayed
bit-identical to the serial parity campaign. The report retains both runs and
resource conditions.

An isolated Metal RMSNorm experiment compared the existing serial, tree and
SIMD kernels across eleven row/dimension shapes, including packed per-layer
projection and long inputs. All matched an independent F64 reference within
`5.1e-7`. Independent GPU tests additionally exercise the actual model-owned
dispatch policy, unsupported-shape fallback and isolation between two runtimes.
Whole-model qualification and the same-binary controls above establish the
measured local gain.

Isolated projection experiments compared the existing MPSMatrix API with the
upstream-style MPSNDArray API, including matched batch rank, strides, aliasing
views and shared encoders. All nine synthetic shapes produced bit-identical
F32 outputs. MPSNDArray reduced CPU command encoding cost, while GPU execution
times were essentially unchanged. These experiments did not change the
production projection API or establish a whole-model improvement.

Manifest probing reads tokenizer bytes afresh and conservatively scans literal
markers and relevant Unicode escapes in one SIMD pass. Wrapper, unknown-model,
fresh-file, file-error and metadata behavior is preserved, without a tokenizer
content cache. On the pinned 32 MB tokenizer, the isolated scan median improved
from 3.96 ms to 2.83 ms; this experiment excludes file-read cost. Boundary,
truncated-escape and 10000 seeded randomized comparisons agree with the retained
legacy scanner.

Three warm current-binary short-query traces show 15.6–16.7 ms in the Metal
encoder and 6.8–7.6 ms resolving the manifest, with traced totals of 25.2–27.3 ms.
These traces exclude the separate cold model load and provide attribution;
the untraced ten-sample HTTP median is 26.6 ms. PyTorch MPS preprocessing and
inference takes 16.1 ms without HTTP. Encoder execution is therefore close for
this query, while fresh validation and HTTP account for substantial service
overhead. HTTP-inclusive short retrieval is about 1.48x PyTorch latency on CPU
and 1.66x on Metal. Metal 512-token latency is about 1.25x PyTorch MPS, cached
choice routing about 1.08x, and the single Metal 8K observation about 1.31x.
The local evidence meets the agreed text/routing PR performance target with a
tested CPU fallback. Deployment latency, sustained concurrency, clean-host
resources and application quality remain separate release gates; this report
does not mark a deployment production qualified.

Earlier focused Debug qualification also verifies that config-probe allocation failures
preserve GLiNER's model-stage HTTP error and metric classification, release
admission, and allow a subsequent request to recover. The comparison report
records the initial diagnostic failure and its final passing regression.


## Video groups

Submit MP4, MOV or WebM as a `media` part in an ordered group, for example:

```json
{"model":"embeddinggemma2","input":[{"content":[{"type":"text","text":"Describe the action"},{"type":"media","data":"<base64 MP4>","mime_type":"video/mp4"}]}]}
```

The existing SDK media types accept these MIME bytes. Binary attachment envelopes
can carry `video/mp4`, `video/quicktime` or `video/webm` without base64 expansion. A group returns
one joint embedding for its ordered text and selected frames. Audio must be an
explicit separate part. Frames sample at 1 FPS with a uniform 32-frame cap over the
whole clip, up to 140 soft tokens each; total wrappers, media and text must fit 8192.
Video-only groups do not add a text task prefix. Flat video inputs are unsupported.
Native CPU uses pure Zig H.264/MJPEG and VP8/VP9/AV1; Metal prefers VideoToolbox for
eligible static H.264 and consumes completed Metal prepared patch buffers directly.
Video preparation overlaps batched resident Metal vision/projector execution.
Orthogonal MP4 rotation and SAR use direct resizing; explicit supported PQ/HLG
signals use the qualified display/HDR recipe. Declared VUI/container matrix or
range conflicts fail explicitly. WebM currently qualifies default-display SDR
fixtures; broader WebM display/Colour metadata remains an open gate.
Six pinned pretrained F32 video cases qualify native CPU and Metal numerical
parity, including ordered mixed groups, repeated frames and HTTP dimensions.
JPEG RGB uses the independent Pillow/libjpeg reference policy; default FFmpeg
MJPEG IDCT can differ. See the [pretrained receipt](../../zig/lib/video/testdata/video-pretrained-validation.md)
and [VIDEO.md](../../zig/lib/video/VIDEO.md#embeddinggemma-2-model-adapter-2026-10-09).

Model FPS sampling currently requires progressive one-picture-per-sample AVC
(Baseline/Main/High-family) or MJPEG. PAFF/MBAFF and Extended-profile partition
transport remain library capabilities; the model adapter rejects them until a
logical-picture index qualifies their frame-count/FPS semantics.


WebM model sampling uses visible decoder outputs rather than packet count. Hidden
reference frames remain dependencies, and VP9 superframe outputs have individual
logical IDs. Indexing stores metadata, then replay decodes once up to the last
selected picture and reuses the existing CPU/Metal projection batching. Each
pass allows at most 8192 coded packets and 65536 visible picture entries; the
model selects at most 32 pictures. This bounds memory without retaining a whole
decoded clip. Equal PTS retain output order; no per-picture timestamps are invented.


[Pretrained WebM qualification](../../zig/lib/video/testdata/webm-validation/README.md)
compares eight vectors and three retrieval rankings against pinned PyTorch.
Native CPU, Metal and actually executed Linux ARM64 preserve all rankings and
top-k sets, with maximum coordinate error below `1e-6`. This includes two
attributed real VP9/AV1 clips. WebM duration comes from the declared Segment
Duration when present, preserving pinned FPS sampling at rounded PTS boundaries.

### CUDA BF16 qualification

Use `zig/pkg/inference/scripts/embeddinggemma2/compare.py` with the same pinned
checkpoint and prepared suite for both runs. `--backend torch-cuda` loads BF16
weights/activations, casts floating media inputs to BF16 and pools/normalizes in
F32. `--backend cuda --binary <cuda-binary>` runs the native HTTP pipeline and
requires CUDA rather than allowing fallback. Both reports declare `bfloat16`;
CPU/Metal reports remain `float32`, and comparisons reject precision mismatches.
For example, from `zig/pkg/inference` in the pinned reference environment:

```sh
python scripts/embeddinggemma2/compare.py --backend torch-cuda \
  --model-dir "$MODEL" --suite "$SUITE" --output torch-cuda.json
python scripts/embeddinggemma2/compare.py --backend cuda --binary "$BINARY" \
  --model-dir "$MODEL" --suite "$SUITE" --reference torch-cuda.json --output cuda.json
```

The CUDA reference synchronizes completed device work and records device/runtime,
allocated/reserved memory and their peaks. HTTP timing includes serialization and
preprocessing, so compare its scope with the reference's preprocessing-inclusive
measurement rather than treating encoder time as end-to-end time. The existing
strict vector, ranking and routing gates remain in force. CUDA encoder-only
benchmark mode is rejected until its measurement contract is implemented.
Reports remain `production_qualified: false`; executing this harness on NVIDIA
hardware and retaining its matched results is required before asserting parity.

Optional CUDA attention, projection, vision and graph profiles carry distinct
identity pins and remain subject to matched BF16 qualification. See the repository
`zig/pkg/inference/CUDA_TUNING.md` for profile controls, retained memory limits and
route-evidence gates. Mixed CUDA inputs compose their ordered text/media rows on
device; repeated video samples reuse projection while preserving logical slots.

CUDA additionally offers `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_NATIVE_VISION=1`
for native batched tiled vision attention and
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_MEDIA_BATCH=1` for ordered mixed-media/audio
cohorts with preparation overlap and batched BF16 backbone execution. Both are
session-frozen, change `model_identity`, and remain opt-in until NVIDIA vector,
ranking and performance qualification. The comparison harness can require observed
execution with `--require-route native_vision` and `--require-route media_batch`.

`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_BACKBONE=1` enables fused
normalization/residual/scaling kernels with explicit BF16 rounding boundaries.
It changes `model_identity` and remains opt-in pending NVIDIA qualification.
The BF16 native vision implementation also versions its existing opt-in identity;
reload and qualify models before using the resulting pin.


CUDA `PACKED_PROJECTIONS`, `GRAPH_REPLAY` and `EXTENDED_FLASH` opt-in profiles
carry updated identities for bounded grouped PLE, cohort replay and portable
WMMA tail padding. Reload and qualify the new pins before use. Graph replay is
bounded to four items, 4096 padded rows and four 128-MiB arenas; grouped PLE uses
at most 16 MiB of output scratch. BF16 composition avoids F32 widening for hard
tokens and batched input storage. These implementation changes do not establish
NVIDIA speedups without hardware measurements.

`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_TUNED_GEMM=1` enables bounded first-use
cuBLASLt tuning for BF16-output projections. Selected plans cache by geometry,
workspace and pointer alignment; tuning declines retry ordinary cuBLASLt
heuristics. Warm actual shapes before graph capture. Require observed selection
with `--require-route tuned_gemm`; a heuristic-only run does not qualify tuning.

Packed projections consume QKV and gate/up outputs directly, preserving BF16
rounding boundaries. Fused backbone execution also combines vision residual/norm
passes. These opt-in profiles have new identity versions; reload and qualify
pins with NVIDIA differential tests, pretrained vectors/rankings and completed
GPU timing before production use. The packed vision rotary consumer applies only
to existing BF16 attention routes. CPU and Metal profiles are unaffected.

CUDA graph replay now shares four bounded arena slots between encoder and vision
execution. Vision geometry tables persist per grid/batch; BF16 towers can replay
once warm. Short encoder queries retain exact row counts, larger buckets add at
most 12.5% rows, and completed warm measurements choose eager or replay execution.
The graph profile (`media-graphs-v3`) and extended attention profile
(`extended-dispatch-v3`) require reloaded, newly qualified model pins.

`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_SPATIAL_POOL=1` enables direct spatial
merge/standardization, with a distinct CUDA identity. Metal uses the precise
resident implementation. RMS normalization retains its existing backend
reduction. CUDA qualification can require `--require-route spatial_pool`,
`--require-route vision_graph` and `--require-route short_attention`; use repeated
images/video frames for vision replay and 16–63-token inputs for short attention.
These gates attest execution, and still require pinned vector/ranking parity and
completed timing on NVIDIA hardware before promotion.

CUDA audio caches immutable relative-position/query-scale/RMS/frontend constants
within an independently admitted 8 MiB / 128-entry budget. Warm query-scale
preparation avoids all twelve per-layer weight readbacks. Borrowers retain their
snapshots and admission across cache eviction. The fixed audio frontend recipe
also reuses thread-safe process tables. BLAS-enabled CPU preparation measures completed
FFT and BLAS preparations per frame count in a bounded cache, excludes cold
calls, and retains BLAS unless FFT wins by at least 5%. Optimized BLAS remains
competitive on large batches.

CUDA image cohorts write patches directly into one bounded batch destination
before a single projection GEMM. This versions the `IMAGE_BATCH` and `MEDIA_BATCH`
pins with `batched-patches-v2`; reload and run NVIDIA qualification before use.
The comparison harness can require `audio_constants` and `patch_into` execution.
Vision scratch now fits four 480×480 frames with packed BF16 preparation into the
existing 128 MiB graph arena, without raising the cache ceiling.


Audio acceleration also batches compatible subsampling lengths and keeps window,
magnitude, log/mask, channel normalization and flatten preparation on device.
Batched normalization and flattening read NCHW directly, avoiding five full
transposes. A planned 512 FFT fuses magnitude on Metal and CUDA; the CPU FFT
uses Zig SIMD across eight frames. Set `ANTFLY_EMBEDDINGGEMMA2_AUDIO_DFT=1`
to force reference preparation. CUDA FFT and tiled F32/BF16 implicit-GEMM audio
convolutions remain opt-in with distinct identities; see CUDA_TUNING.md for
flags and route gates. No full im2col buffer is required.
CUDA QKV packing reuses immutable clamp metadata. The CUDA frontend recipe is
now version 2; existing CUDA qualification pins must be regenerated. Graph
retention uses completion-event eviction under the unchanged total memory cap.
These routes require NVIDIA vector/ranking parity and completed-device timing
before performance promotion.

The optional `ANTFLY_EMBEDDINGGEMMA2_BENCH_FFT=1` test named `completed
spectrum benchmark` compares warm, completed spectrum preparation. On an Apple
M4 Max (Debug host harness, three warmups and five timed iterations), the precise
Metal route gave:

| Audio frames | DFT GPU µs | FFT GPU µs | DFT completed wall µs | FFT completed wall µs |
|---|---:|---:|---:|---:|
| 40 | 66.3 | 23.7 | 1204.8 | 455.6 |
| 200 | 95.8 | 42.6 | 1063.6 | 276.4 |
| 1000 | 261.4 | 152.3 | 1366.2 | 498.6 |

These are spectrum microbenchmarks against Antfly's DFT route, excluding mel
projection and model execution. They do not establish end-to-end or PyTorch
speed. The independent ReleaseFast CPU spectrum benchmark found Accelerate
faster at 200/1000 frames, supporting measured CPU selection rather than a
universal FFT default.

Audio preparation additionally offers a fused window/FFT/sparse-mel/log-mask
kernel and precise-F32 Metal matrix convolution, with tiled and direct
fallbacks. Warm feature preparation owns
one Metal frame unless the caller supplies one. The direct batch estimate
recovers three-clip Metal or two-clip CUDA cohorts at 1,000 frames within the same
128 MiB envelope. Diagnostics can force the staged frontend with
`ANTFLY_EMBEDDINGGEMMA2_AUDIO_UNFUSED=1` and direct Metal convolution with
`ANTFLY_EMBEDDINGGEMMA2_METAL_DIRECT_CONV=1`.

CUDA `AUDIO_FRONTEND_FUSED` and `AUDIO_GRAPH` flags use the
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_` prefix and remain separately identified
qualification candidates. The latter learns real conformer buffer lifetimes,
then captures stable addresses under the existing shared graph budget. Use
`--require-route audio_frontend_fused`, `audio_arena` and `audio_graph` to attest
execution before comparing pretrained vectors/rankings and completed latency.
The `ANTFLY_EMBEDDINGGEMMA2_BENCH_AUDIO=1` test named `completed fused frontend
and tiled Metal convolution benchmark` measures warm framed frontend and
convolution routes separately with completed GPU and wall times.

On an Apple M4 Max with the Debug host harness, three warmups and five timed
iterations, resident-input preparation and two-clip convolution measured the
following completed GPU times. Convolution uses 128 input channels, 32 output
channels, width 64, and input height `ceil(audio_frames / 2)`.

| Audio frames | Staged FFT frontend µs | Fused frontend µs | Direct conv µs | Tiled conv µs | Matrix conv µs |
|---|---:|---:|---:|---:|---:|
| 40 | 81.2 | 27.2 | 889.7 | 484.7 | 275.8 |
| 200 | 145.2 | 48.9 | 2888.7 | 1773.5 | 588.4 |
| 1000 | 261.9 | 166.6 | 3621.4 | 2554.5 | 836.1 |

These measurements exclude uploads, decoding and model execution. The harness
also reports completed wall time; these kernel comparisons do not establish
end-to-end or PyTorch speed. The retained-peak regression measured 103.1 MiB of
additional device allocation for the three-clip, 1,000-frame Metal cohort while
its command frame remained open, below the unchanged 128 MiB envelope.

The pretrained Metal ordered-media suite passes against the pinned official
PyTorch F32 oracle after these changes, including audio cohorts. Audio cosine
similarity was 0.999999998 with maximum absolute error 0.000008523; image,
text/image orderings and mixed inputs also passed. This qualifies the tested
fixtures; CUDA arithmetic and graph candidates still need NVIDIA qualification.
