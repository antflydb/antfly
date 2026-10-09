# EmbeddingGemma 2 embeddings and similarity decisions

Antfly runs the open Hugging Face `google/embeddinggemma-2` safetensors checkpoint
on native CPU and Metal. Text, images, audio, video and ordered combinations share the
same embedding space. Video accepts qualified MP4/MOV through ordered groups.
CUDA, ONNX, PJRT and WebGPU are excluded from this implementation. The original EmbeddingGemma, GLiNER, Laya and Jev routes retain
their existing contracts.

## Checkpoint and numerical contract

The initial supported checkpoint is revision
`914f7f89142e33e77833254d9c9b90c3cef7303b`. Acquire its original weights and sidecars:

```sh
python3 tools/embeddinggemma2/reference.py acquire models/embedders/embeddinggemma2
```

The acquisition tool verifies the resolved revision and LFS weight digest, keeps
the upstream license/model card, publishes files atomically, and records SHA256
hashes in `embeddinggemma2_receipt.json`. The weights retain their upstream license.
A managed registry reference is
`hf:google/embeddinggemma-2:safetensors@914f7f89142e33e77833254d9c9b90c3cef7303b`.

The runtime validates the text, vision and audio geometry and weight shapes,
tokenizer BOS/EOS and media tokens, and the pinned image/audio/video processor. It
accepts BF16 or F32 weights, converts BF16 values exactly to F32, and uses F32
activations and accumulation on both supported backends. Quantized and implicit
F16 execution are rejected for this family.

The text encoder is bidirectional, including its alternating global and sliding
attention. Mean pooling includes every active token: BOS, EOS, task prefixes and
expanded media tokens. Padding is excluded. The result is L2 normalized. Reduced
sizes 512, 256 and 128 truncate the 768-vector and then normalize again. The
8192-token limit includes all expanded media tokens and wrappers; overflow fails
instead of silently truncating input. Audio is limited to 30 seconds per part.

Task types are `RETRIEVAL_DOCUMENT` (default), `RETRIEVAL_QUERY`,
`QUESTION_ANSWERING`, `FACT_VERIFICATION`, `CODE_RETRIEVAL_QUERY`,
`SEMANTIC_SIMILARITY`, `CLASSIFICATION` and `CLUSTERING`. Query and document roles
must remain consistent between indexing and retrieval.

## HTTP and ordered groups

Existing flat inputs return one vector per text or media part. A group returns
one vector for all its ordered content. Each request must use one form throughout.
Encoded rows/groups execute serially within the request workspace. Local PDF
page embeddings borrow the renderer's RGBA pixels, including padded row strides,
without PNG encoding or decoding. They share the encoded-image resize and
projector, then execute the encoder in cohorts of up to four pages. Cohorts
preserve page order, cap token padding at 25%, and occupy at most 2048 padded
token slots. Masks isolate attention and mean pooling by page. Projections are
released after each cohort, and observed execution reports native batching only
when every cohort contains multiple pages. Model identity pins, reduced
dimensions, deadlines and cancellation also apply to this raster path.
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

The embedding response includes `model_identity`, a backend-independent SHA256
identity for the exact config, weights, tokenizer, processor, SentenceTransformers
sidecars and `embeddinggemma2-f32-mean-v1` recipe. Supply that value as request
`model_identity` to reject a mismatched generation before encoding. Loaded
artifacts are checked for changes before and after execution.

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
python3 tools/embeddinggemma2/calibrate.py scores.json \
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
python3 tools/embeddinggemma2/qualify.py \
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
python3 tools/embeddinggemma2/encoder_campaign.py \
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

`tools/embeddinggemma2/compare.py` compares the same pinned F32 checkpoint with
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
python3 tools/embeddinggemma2/compare.py --backend prepare \
  --model-dir <model-dir> --artifacts-dir <oracle-and-media-dir> --output suite.json
python3 tools/embeddinggemma2/compare.py --backend mps \
  --model-dir <model-dir> --suite suite.json --output pytorch-mps.json
python3 tools/embeddinggemma2/compare.py --backend metal \
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
owned frame for at most 512 total rows, including padding and batch. Longer
inputs complete one frame per layer to bound retained intermediates. Frames
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
once per encoder pass. On Metal, at up to 512 total rows, one packed projection
and RMSNorm produce all layers' per-layer inputs, matching the upstream
implementation. Individual layers slice their columns on the device. CPU and
larger Metal inputs compute one layer's projection at a time to retain the
host-workspace bound.
CPU execution reuses uniquely owned RMSNorm and GELU inputs at their last use
and retires replaced projection/reshape handles. Aliased inputs and unsupported
consume operations retain independent output storage. Errors preserve source
ownership for cleanup or retry. Independent activation references, alias
preservation, allocation failures and retry behavior have focused coverage.
BF16 widening uses lossless vector operations, and Metal
linear preparation uses owned shared snapshots on unified-memory devices.
Immutable language weights also have model-owned reuse: CPU retains losslessly
widened BF16 mirrors, and Metal retains prepared F32 projection slots across
requests. Each cache has at most 512 entries and 640 MiB of payload, charged to
its resident host or backend tier before allocation. Vocabulary and media
weights, mutable tensors and transient views are excluded. Admission denial
keeps the request-scoped preparation path. Unloading a drained model releases
both payloads and their admission ownership; the request workspace stays bounded
independently.


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

Submit MP4 or MOV as a `media` part in an ordered group, for example:

```json
{"model":"embeddinggemma2","input":[{"content":[{"type":"text","text":"Describe the action"},{"type":"media","data":"<base64 MP4>","mime_type":"video/mp4"}]}]}
```

The existing SDK media types accept these MIME bytes. Binary attachment envelopes
can carry `video/mp4` or `video/quicktime` without base64 expansion. A group returns
one joint embedding for its ordered text and selected frames. Audio must be an
explicit separate part. Frames sample at 1 FPS with a uniform 32-frame cap over the
whole clip, up to 140 soft tokens each; total wrappers, media and text must fit 8192.
Video-only groups do not add a text task prefix. Flat video and WebM decoding are
unsupported. Native CPU uses pure Zig H.264/MJPEG; Metal prefers VideoToolbox for
eligible static H.264 and consumes completed Metal prepared patch buffers directly.
Existing vision/projector host boundaries remain. The current model display policy
requires square pixels, no rotation and supported SDR color metadata (BT.601 default
when absent); declared VUI/container matrix or range conflicts fail explicitly.
Six pinned pretrained F32 video cases qualify native CPU and Metal numerical
parity, including ordered mixed groups, repeated frames and HTTP dimensions.
JPEG RGB uses the independent Pillow/libjpeg reference policy; default FFmpeg
MJPEG IDCT can differ. See the [pretrained receipt](../../zig/lib/video/testdata/video-pretrained-validation.md)
and [VIDEO.md](../../zig/lib/video/VIDEO.md#embeddinggemma-2-model-adapter-2026-10-09).

Model FPS sampling currently requires progressive one-picture-per-sample AVC
(Baseline/Main/High-family) or MJPEG. PAFF/MBAFF and Extended-profile partition
transport remain library capabilities; the model adapter rejects them until a
logical-picture index qualifies their frame-count/FPS semantics.
