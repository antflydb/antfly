# Native PJRT CUDA model experiment

This builds an Antfly Zig executable that loads GPU-resident BF16 parameters,
compiles prepared HLO through the PJRT C API, and executes Gemma 4 cached
generation or EmbeddingGemma2 text embedding. Python/Transformers/Torchax are
used **offline** to prepare graphs and numerical oracles. They do not execute
in the timed native inference loop.

This is an experimental artifact runner. It does not enable either model in the
HTTP server's existing XLA backend. It accepts trusted local artifacts, one CUDA
device, batch one and fixed prompt/cache shapes. Embedding supports the text
path only, with 768-dimensional normalized mean-pooled output. Gemma uses greedy
fixed-length generation, including tokens after EOS, and GPU-resident BF16 KV.

## Build and environment

Run from the repository worktree root with Zig 0.17:

```sh
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build \
  --build-file zig/lib/pjrt/build.zig model-runner -Doptimize=ReleaseFast \
  --cache-dir "$PWD/.zig-cache/pjrt" --prefix "$PWD/zig-out" -j1
```

The native executable needs a CUDA PJRT plugin plus its CUDA/cuDNN libraries,
with compatible `ptxas` and libdevice. The L4 experiment used JAX CUDA 12 plugin
0.10.2, CUDA runtime 12.8, cuDNN 9.8, and driver 580.159.03. Set
`LD_LIBRARY_PATH` and `PATH` for that installation. Keep `XLA_FLAGS` **unset**
for native execution: Antfly's bounded allocator rejects ambient XLA overrides.
The manifest can retain explicit compiler options instead.

`requirements.txt` describes the existing offline export environment. Install
PyTorch's CPU build separately to avoid an unnecessary second GPU framework.
For GPU oracles also install the matching JAX CUDA plugin. Oracle construction
and native measurements must run in separate processes without overlap.

The manifest's `capacity_bytes` and `device_bytes` set the PJRT BFC bound.
Defaults are specific to the measured L4: 21,000 MiB of a 23,034 MiB device,
without eager preallocation. Set both explicitly for another GPU. No other GPU
architecture was measured.

## Prepare artifacts

The exporters can run on CPU; they trace parameterized shapes rather than
embedding model weights or input token values in the graph:

The text-only `prepare_gemma4.py` command below produces version 1 artifacts for
`model-runner`. For text, image, audio and video with version 2 artifacts, use
`prepare_gemma4_multimodal.py` and `gemma4-runner` as described in
[the multimodal Gemma 4 guide](GEMMA4.md). The manifest formats and workers differ.

```sh
JAX_PLATFORMS=cpu JAX_ENABLE_X64=1 python zig/lib/pjrt/tools/prepare_gemma4.py \
  --config zig/lib/pjrt/tools/gemma4_config.json \
  --cases zig/lib/pjrt/tools/gemma4_cases.json --out evidence/gemma4

JAX_PLATFORMS=cpu JAX_ENABLE_X64=1 python zig/lib/pjrt/tools/prepare_embedding.py \
  --model-dir "$EMBEDDING_MODEL_DIR" --out evidence/embedding
```

Gemma's retained fixture contains only the three text prompts, exact input token
IDs and output budgets. The configuration retains only the Gemma 4 E4B text
backbone settings consumed by the exporter. Generated reference tokens and logit
oracles stay in ignored evidence directories. Stream the exact benchmark Q4 GGUF; the native
runner receives its **dequantized BF16** parameters, not Q4 device weights.
The source file SHA256 is
`676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee`.

Embedding preparation selects only `language_model.*` tensors from the original
BF16 `model.safetensors`. Inputs repeat the word `river`, rendered with the
reference `title: none | text: ` document prefix at exactly 128, 512, 2048 and
8192 tokens. This isolates sequence scaling; it is not a retrieval quality set.
The encoder uses bidirectional attention with the reference inclusive sliding
window, followed by the model's 768 projection, F32 mean pooling and L2 norm.

Optional GPU reference generation (separate from timings):

```sh
JAX_PLATFORMS=cuda JAX_ENABLE_X64=1 XLA_PYTHON_CLIENT_PREALLOCATE=false \
  python zig/lib/pjrt/tools/embedding_oracle.py \
  --model-dir "$EMBEDDING_MODEL_DIR" --manifest evidence/embedding/model.json

JAX_PLATFORMS=cuda JAX_ENABLE_X64=1 XLA_PYTHON_CLIENT_PREALLOCATE=false \
  python zig/lib/pjrt/tools/gemma4_oracle.py --weights "$GEMMA_GGUF" \
  --config zig/lib/pjrt/tools/gemma4_config.json \
  --manifest evidence/gemma4/model.json --steps 216
```

These add vector oracles to the embedding manifest, or create
`validation.json`, logit HLO and a 216 MiB teacher-forced logit oracle beside the
Gemma manifest. `compile_options.py --manifest ... --cuda-data-dir ...` can
retain the JAX compiler options and chosen CUDA tool directory for a validation
campaign. The numerical report retains every step even when a diagnostic policy
fails. Its exploratory BF16 policy is cosine >= 0.9999, relative L2 <= 0.02,
and argmax reference-score gap <= two BF16 ULPs. Inspect actual errors and token
choices; this is not a general model-quality qualification.

`require_exact_tokens: true` on a case enforces its token oracle. Otherwise
reference token differences are diagnostic and full generated IDs remain in the
report. The CPU-oracle tiny model can require exact matching; floating-point
compiler differences need not preserve a full real-model generation sequence.

## Measure native execution

Use a fresh output directory. The feeder streams weights once in manifest order;
the native process retains device parameters and cache buffers across calls.

```sh
python zig/lib/pjrt/tools/benchmark.py \
  --runner zig-out/bin/antfly-pjrt-cuda-model --plugin "$PJRT_CUDA_PLUGIN" \
  --manifest evidence/gemma4/model.json --weights "$GEMMA_GGUF" \
  --format gguf --out evidence/gemma4-native

python zig/lib/pjrt/tools/benchmark.py \
  --runner zig-out/bin/antfly-pjrt-cuda-model --plugin "$PJRT_CUDA_PLUGIN" \
  --manifest evidence/embedding/model.json \
  --weights "$EMBEDDING_MODEL_DIR/model.safetensors" \
  --format safetensors --out evidence/embedding-native
```

The paired embedding CUDA server harness accepts the same cases:

```sh
python zig/lib/pjrt/tools/benchmark_embedding_cuda.py \
  --binary "$ANTFLY_SERVER" --models-dir "$MODELS_DIR" \
  --model embeddinggemma-2 --manifest evidence/embedding/model.json \
  --out evidence/embedding-cuda
```

Use `--full-cli` when the binary expects `antfly inference run`. For the opt-in
comparison, set the five CUDA environment flags listed in [RESULTS.md](RESULTS.md)
and choose a fresh output directory. This harness retains those policy flags,
every vector, exact token usage, model identity and kernel-route counters.

The harness retains input/binary/plugin/HLO hashes, native and feeder logs,
per-call results, and sampled process-tree PSS/NVML peaks. Memory sampling
covers the native process tree, including loading/compilation, but excludes
the separate offline feeder. Warm timing excludes loading, compilation and
warmups, and includes synchronized device execution and output readback.
Native TTFT ends at prefill completion, before the token readback. No HTTP,
tokenization, streaming or request scheduling is included.

When comparing with native CUDA server measurements, label those entrypoint
differences. Also label Gemma's Q4-vs-dense-BF16 storage and KV precision, and
Embedding's whole-multimodal-vs-text-only resident inventories. Those memory
comparisons do not isolate allocator efficiency. Report compilation costs
separately; do not present warm timings as first-request latency.

## Checks

```sh
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build \
  --build-file zig/lib/pjrt/build.zig test \
  --cache-dir "$PWD/.zig-cache/pjrt" --prefix "$PWD/zig-out" -j1 --summary all
python -m compileall -q zig/lib/pjrt/tools
```

CPU tests cover BF16 shape validation, upload completion/event ownership and
failure cleanup, plus row-major I64 readback without an F32 conversion. Tests
that need an installed PJRT plugin can skip independently; actual CUDA model
execution and numerical checks are separate GPU campaigns.

## Full EmbeddingGemma2 image/audio pipeline

The separate `media-runner` target builds `antfly-pjrt-cuda-media`. Unlike the
text artifact runner above, `benchmark_media_pjrt.py` runs the official CPU
processor on the encoded file **on every call**, then sends its tensors to the
native PJRT worker. HLO contains the actual vision/audio tower, projections,
shared encoder, mean pooling and normalization. The timer includes file decode,
preprocessing, tokenization, IPC, device execution and synchronized readback.
It excludes model loading, compilation, warmups and request cleanup. This is a
direct pipeline comparison; neither arm includes HTTP.

The native CUDA comparator is `build-embeddinggemma2-media-bench`. It invokes
the production media pipeline in process, including encoded file reads. Native
CUDA has no reference-worker IPC overhead. Its stage observations include
asynchronous enqueue time; do not add them as GPU execution times. The reference
driver separately records CPU preprocessing, worker receive/upload and worker
execute/readback time; the latter includes synchronization, not just kernels.

The media tools additionally need Pillow and a torchvision CPU wheel compatible
with the installed torch version. The L4 run used torchvision `0.25.0+cpu` in a
worktree-local dependency directory. Transformers `5.23.0` was checked against
its wheel RECORD; the older text-experiment source hashes are not its identity.

```sh
python zig/lib/pjrt/tools/media_inputs.py --model "$EMBEDDING_MODEL_DIR" \
  --out evidence/media/fixtures
python zig/lib/pjrt/tools/validate_media_graph.py --model "$EMBEDDING_MODEL_DIR" \
  --suite evidence/media/fixtures/suite.json --out evidence/media/oracle.json
JAX_PLATFORMS=cpu JAX_ENABLE_X64=1 python zig/lib/pjrt/tools/prepare_media.py \
  --model "$EMBEDDING_MODEL_DIR" --suite evidence/media/fixtures/suite.json \
  --out evidence/media/programs
python zig/lib/pjrt/tools/media_campaign.py \
  --binary zig-out/bin/antfly-embeddinggemma2-media-bench \
  --runner zig-out/bin/antfly-pjrt-cuda-media --plugin "$PJRT_CUDA_PLUGIN" \
  --model "$EMBEDDING_MODEL_DIR" --manifest evidence/media/programs/model.json \
  --suite evidence/media/fixtures/suite.json --oracle evidence/media/oracle.json \
  --acceptance zig/pkg/inference/scripts/embeddinggemma2/profiling/media-acceptance.json \
  --out evidence/media/campaign
```

The campaign rotates control CUDA, candidate CUDA and PJRT across three fresh
process rounds, with three warmups and eight measured calls per case per round.
Only the candidate enables `GLOBAL_GEMM`; the other text CUDA opt-ins are held
constant. The PJRT executable cache is created fresh inside the campaign output,
compiled in the first reference process and deserialized in later processes.
Use it only on that device/plugin. All rounds upload weights afresh and retain
every output vector. Compilation/deserialization is excluded from warm latency
and included in sampled startup memory. Cache bytes are hashed between rounds.

The fixtures are deterministic synthetic PNG/JPEG patterns, caption plus JPEG,
and nonzero 1/12/30-second PCM16 WAV signals. They cover pipeline geometry and
numerical behavior, not retrieval quality or real-world corpus distributions.
The shared encoder sees 264/270/1157/29/304/754 tokens, respectively. The global
attention candidate qualifies for the caption and 30-second cases only.

Numerical gates are frozen separately from performance. Native towers use FP32
arithmetic with BF16 checkpoint values; PJRT uses BF16 tower arithmetic with the
official FP32 boundaries. A CPU FP32 oracle is an additional precision diagnostic,
and never overrides a failed CPU BF16 gate. The wrapper replaces shape-dependent
padding/placeholder discovery with processor metadata; its RMSNorm rewrite only
expresses upstream `type_as` as `to(dtype=...)`. CPU upstream-forward comparisons
check exact output agreement before exporting. Any failed case makes the campaign
exit nonzero **after** retaining its complete summary and per-case status.

CPU oracle rows are validated before publication. Both the campaign and FP32
report reject nonfinite error/cosine metadata, missing or duplicate cases,
invalid vectors, and wrapper/upstream vector differences above tolerance.
The error is recomputed from the saved vectors, so a success-looking summary
cannot certify a failed wrapper check.

See the [L4 media qualification report](../../../pkg/inference/scripts/embeddinggemma2/profiling/MEDIA.md)
for measured results, precision limitations and the original acceptance criteria.
