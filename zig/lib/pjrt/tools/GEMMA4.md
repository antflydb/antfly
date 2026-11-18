# Gemma 4 native CUDA / native PJRT CUDA qualification

These are offline artifact runners, not a PJRT HTTP serving backend. All model
operations in the PJRT arm run through `gemma4_runner.zig` and the plugin C API.
Python exports HLO on CPU, streams weights, and runs the official CPU media
processor. The native arm uses the production generation pipeline and request
boundary, with fresh paged F16 KV state per call.

The two arms use identical QAT GGUF source files. Native retains Q4 weights and
F16 KV; PJRT expands mapped weights and KV to BF16. This is a runtime/representation
comparison, not an equal-precision kernel comparison. Both ignore EOS and return
the complete fixed output-token budget, synchronizing generated token readback.

## Build and prepare

From the worktree root, using the repository's pinned Zig:

```sh
export ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global"
zig build --build-file zig/pkg/inference/build.zig bench-gemma4-core-build \
  -Doptimize=ReleaseFast -Dstrip=true -Dcuda=true -Dcuda-artifacts=sm89 \
  --cache-dir .zig-cache/native --prefix zig-out -j1
zig build --build-file zig/lib/pjrt/build.zig gemma4-runner \
  -Doptimize=ReleaseFast --cache-dir .zig-cache/pjrt --prefix zig-out -j1
```

Keep the global Zig cache inside the worktree as well. Preparation used
Torch 2.10.0+cpu, Transformers 5.23.0, torchax 0.0.13, JAX/JAXlib 0.10.2,
NumPy 2.4.6, PyAV 17.1.0, and torchvision 0.25.0+cpu. The CUDA PJRT plugin
comes from the JAX CUDA 12 plugin distribution; the measured plugin and workers
are bound by SHA256 in campaign provenance and executable-cache identity.

Supply the Gemma 4 E4B QAT text GGUF, its matching mmproj, the upstream E4B
processor/config/chat template/tokenizer, and a suite JSON. Each case contains
`id`, `kind` (`text`, `image`, `audio`, `video`), `text`, `max_tokens`, and
`expected_prompt_tokens`; media cases add a path relative to the suite. The suite
also declares fixture hashes/sizes, `fixture_scope`, and `context_tokens: 8192`.
`gemma4_inputs.make_fixtures` creates deterministic synthetic media from a list
of text cases carrying `text`, `id`, `max_tokens`, and prepared `prompt_ids`.
Synthetic media qualify geometry and implementation, not task quality.

```sh
JAX_PLATFORMS=cpu python zig/lib/pjrt/tools/prepare_gemma4_multimodal.py \
  --config "$CONFIG" --processor "$PROCESSOR" --suite "$SUITE" \
  --gguf "$GGUF" --mmproj "$MMPROJ" --out evidence/gemma4-artifacts
```

The retained three text prompts and expected prepared token IDs are in
`gemma4_cases.json`. To generate and prepare the eight-case fixture suite
before export (set `PROCESSOR` to the sidecar directory):

```sh
PYTHONPATH=zig/lib/pjrt/tools python - <<'PYCODE'
import json, os
from pathlib import Path
from gemma4_inputs import make_fixtures, prepare, processor
out = Path("evidence/gemma4-fixtures")
texts = json.loads(Path("zig/lib/pjrt/tools/gemma4_cases.json").read_text())
suite = make_fixtures(out, texts)
proc = processor(Path(os.environ["PROCESSOR"]))
expected = {case["id"]: case["prompt_ids"] for case in texts}
for case in suite["cases"]:
    inputs, metadata = prepare(proc, case, out)
    ids = inputs["input_ids"][0].tolist()
    if case["id"] in expected:
        assert ids == expected[case["id"]]
    case["expected_prompt_tokens"] = len(ids)
    case["processor_metadata"] = metadata
    case["input_shapes"] = {key: list(value.shape) for key, value in inputs.items()}
(out / "suite.json").write_text(json.dumps(suite, indent=2) + "\n")
PYCODE
```

Video renders one space after its marker to match the native standalone
formatter; frame sampling and soft-token positions are unchanged. Encoded media
must be retained and compared by hash: codec/build differences can change bytes
when fixtures are regenerated elsewhere.

The exporter rejects unknown/missing weights and unqualified physical layouts.
It retains vision/audio QAT clipping, reorders the vision patch convolution,
accounts for the reversed GGUF audio norm names, and handles the already-softplus
GGUF audio query scales exactly once. The functional sliding KV ring is checked
against the independent official CPU cache, including wraparound and shared KV.

## Measure and validate

Pass the same arguments to `benchmark_gemma4.py` for preparation and warm runs:

```sh
python zig/lib/pjrt/tools/benchmark_gemma4.py \
  --native zig-out/bin/antfly-gemma4-core \
  --runner zig-out/bin/antfly-pjrt-gemma4 --plugin "$PJRT_PLUGIN" \
  --gguf "$GGUF" --mmproj "$MMPROJ" --tokenizer "$TOKENIZER" \
  --template "$TEMPLATE" --processor "$PROCESSOR" --suite "$SUITE" \
  --manifest evidence/gemma4-artifacts/model.json \
  --cache evidence/gemma4-pjrt-cache --out evidence/prepare --prepare-only
```

Use a fresh output directory without `--prepare-only` for three alternating
fresh-process rounds, one warmup and three measured calls per case per arm.
Native additionally primes every requested modality before timing any case.
PJRT executable caches bind the manifest, worker, plugin, GPU/driver, model
sources, and each HLO. Warm runs refuse missing executables. No other GPU process
may be active. Finish builds and CPU oracles before timing. Memory is sampled
process-tree PSS plus NVML every 0.2 s, including startup and inference.

Time covers encoded file read, decode/preprocessing, towers, text decoder,
fixed-budget autoregressive generation, and synchronized token readback; excludes
HTTP, model loading, and cold compilation. PJRT includes Python processing and
worker IPC. Do not present a `--validate` run as performance data.

Run `gemma4_cpu.py` with the exporter's model/processor/suite arguments and a fresh
`--out` to generate independent official CPU FP32 first-step logits. It streams
layers and gathers token/PLE rows, so the complete dense model need not fit in
host RAM. `--only CASE` can partition the checks. This CPU reference uses the
official full forward, not the functional exported KV/media forward.
`gemma4_reference.py` verifies the installed modeling source against the wheel's
RECORD and rejects replaced forward methods. It does not import the export
adapter. GGUF's folded audio query scale is inverted at weight loading so the
unmodified official attention applies softplus once. References record the
Transformers version and modeling-source hash in `reference.json`.
`--capture-ple-input` additionally observes the merged text/media embeddings
through an official-module pre-hook for stage comparisons.

Then run the benchmark with `--validate --rounds 1 --iterations 1` into a fresh
campaign directory and compare it using:

```sh
python zig/lib/pjrt/tools/validate_gemma4.py --campaign "$VALIDATION" \
  --cpu "$CPU_REFERENCE" --out evidence/validation.json
python -m unittest discover -s zig/lib/pjrt/tools
```

The comparator reports cosine, relative L2, maximum absolute error, exact argmax,
and its FP32-reference gap. Its declared logit gate is cosine >= 0.9999 and
relative L2 <= 0.02. Argmax agreement is reported separately; qualification
requires both gates. The comparator saves all diagnostics and exits nonzero on
either failure. It rejects missing rounds, arms or vectors. Validation requires
`--iterations 1` so the native worker retains every measured vector. First-step
checks do not prove whole-generation parity. Preserve failures rather than
widening the gate. Invalid memory sampling also fails the benchmark campaign.
Large artifacts, media, vectors, HLO, executable caches and full logs belong under
ignored `evidence/`; keep only compact results/provenance in the branch.

For a later native decision, run the CPU tool with a completed native result:

```sh
python zig/lib/pjrt/tools/gemma4_cpu.py --config "$CONFIG" \
  --processor "$PROCESSOR" --suite "$SUITE" --gguf "$GGUF" --mmproj "$MMPROJ" \
  --native-tokens "$PERFORMANCE/r0-native/result.json" --out evidence/generated-end
```

It checks the final decision through the first configured EOS or fixed budget,
teacher-forcing the preceding native tokens and excluding the predicted label.
It extends text attention/type masks while retaining the original media inputs,
rejects inconsistent repeats, binds the native result by hash, saves
`generation-check.json` and exits nonzero on an argmax mismatch. This checks one
later decision per case, not every intermediate token or free-running parity.
Reference-position metadata prevents these vectors from validating prefill.

Add `--all-native-steps` to check every decision through the first EOS or fixed
budget. This runs the unmodified causal forward with the native prefix and
selects all prediction positions; it retains decision IDs, disagreement positions
and reference margins instead of large per-step logit arrays. A regression
compares this selection with separate official causal prefixes, including shared
KV layers. This mode rejects bidirectional text attention and exits nonzero if
any decision differs. Matching first and final decisions is insufficient.

## Native PLE normalization correction

The comparison exposed a native PLE context-projection discrepancy. The official
model applies `1/sqrt(hidden_size)` before RMSNorm. Folding away that multiply
requires multiplying RMSNorm epsilon by `hidden_size`; epsilon prevents exact
scale invariance otherwise. The correction updates direct, token-tensor/fused,
and prepared decoder paths without adding a tensor, dispatch, or scratch buffer.
The regression checks actual PLE outputs against the literal scaled-projection
formula at zero and nonzero epsilon, including low-variance projection rows.
CUDA qualification is measured here; live Metal validation is not available on
this Linux host.

The benchmark worker accepts `legacy_ple_normalization: true` to restore the old
epsilon in the same binary. Both arms use the same wrapper and kernel code;
`ple_control_calls` must be nonzero. This is a benchmark control, not a serving
option. Run a paired campaign against the independent reference directories:

```sh
python zig/lib/pjrt/tools/qualify_native_gemma4.py \
  --native zig-out/bin/antfly-gemma4-core --gguf "$GGUF" --mmproj "$MMPROJ" \
  --tokenizer "$TOKENIZER" --template "$TEMPLATE" --suite "$SUITE" \
  --cpu "$CPU_REFERENCE" --out evidence/paired-ple --measure
```

Repeat `--cpu` for partitioned references. The tool freezes accuracy and latency
criteria before execution, retains raw samples, and separates improvement from
the absolute reference gate. An improved result that still fails the absolute
gate exits nonzero. Timings require an idle GPU and no concurrent compiler or
CPU-oracle jobs. Diagnostic suite fields `kv_dtype: "f32"` and per-case
`ple_input_path` support precision/stage bisection; defaults remain F16 KV and no
stage capture. Stage capture requires one-token logit validation.

`--control video-resize` compares bilinear and bicubic video with corrected PLE
in both arms. `--control branch` restores both old behaviors in the legacy arm
and enables both corrections in the candidate. These are controls in one binary,
not a separately built main binary. Every declared fixture is hash-checked before
execution. The recorded arm environment identifies the resize override.

`--control image-resize` compares the CUDA GGUF image path's bilinear and
upstream bicubic preprocessing. `ANTFLY_INFERENCE_CUDA_GEMMA4_IMAGE_BICUBIC=1`
opts into bicubic; bilinear remains the default because the paired candidate
failed its non-increasing first-step-error criterion. The new image continuation
matches all 64 oracle decisions, so this remains available as an accuracy
experiment. This correction applies to standard 16-pixel
patches; other backends and geometries retain their existing behavior. The
`branch` control above covers PLE and video only, with the same image policy
in both arms. Use an image-only suite and corresponding first-step reference
directory to qualify image preprocessing separately under production defaults.

## Native video resizing and accuracy profile

The upstream processor specifies bicubic video resizing. On SM80+ with the
standard 16-pixel patches and `[0,1]` normalization, native CUDA now converts the
resident frame to RGB8 and reuses the existing resident bicubic resize. It
converts the result to GGUF's planar patch order before projection. Pixels stay
on the GPU. `ANTFLY_INFERENCE_CUDA_GEMMA4_VIDEO_BICUBIC=0` restores bilinear
preprocessing for paired checks. Other normalization/patch geometries retain
their existing route. The focused GPU regression checks normalization and patch
layout across 12 matrix/range/size combinations without a pixel readback.

The original four precision overrides pass all eight retained **first-step**
numerical and argmax gates against unmodified Transformers 5.23.0. The image
experiment adds a fifth override and fixes the intermediate image disagreement
in the seven-case comparison. The RGB policy adds a sixth override and closes
the video disagreement: together they match all 585 checked decisions through
EOS or budget on the eight retained fixtures. This profile retains Q4 text
weights and F16 KV; it is not an all-F32 decoder:

```sh
export ANTFLY_INFERENCE_CUDA_GEMMA4_PROJECTOR_COMPACT=0
export ANTFLY_INFERENCE_CUDA_GEMMA4_VISION_F16=0
export ANTFLY_INFERENCE_CUDA_Q4_0_PLE_GATE_FUSION=0
export ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1=0
export ANTFLY_INFERENCE_CUDA_GEMMA4_IMAGE_BICUBIC=1
export ANTFLY_INFERENCE_CUDA_GEMMA4_VIDEO_SWSCALE_RGB=1
```

This uses F32 media weights/vision attention, the ordinary PLE gate path and
exact LM-head multiplication. Apply the same profile to validation and timing;
campaign provenance records every override. These are explicit qualification
settings, not new serving defaults. The full profile also matches all 585
retained native decisions through EOS or budget; this does not establish broad
task-quality parity. The default fast profile
still has numerical failures on this corpus.

`ANTFLY_INFERENCE_CUDA_GEMMA4_VIDEO_SWSCALE_RGB=1` separately opts into the
upstream RGB8 integer-rounding boundary for eight-bit, limited-range BT.601
video (matrix 2/5/6), without tone mapping. BT.709, full-range, high-depth and HDR
retain the existing conversion. This is a Gemma4-only experiment; the ordinary
resident RGB converter and its existing PTX bodies are unchanged. It remains
off by default. `--control video-rgb` isolates this policy in the same binary.

The GPU pixel test uses a small independent PyAV-generated fixture, checks
matrix aliases and all rotations, and checks unsupported-format fallbacks.
For PTX regeneration, build `zig/pkg/inference/tools/nvrtc_ptx_compile.c` against `libdl`, then use
`input.cu output.ptx compute_75 --precise` with NVRTC 11.8. Precise mode disables
FMA contraction and fast math; the tool's existing default is unchanged. Prefix
the output with `// source-sha256: <SHA256 of input.cu>` and retain a single final
newline. The source-integrity test requires PTX 7.8 and CUDA 11.8; do not replace
it with PTX from a newer toolkit unsupported by the deployment driver.

The legacy bilinear fallback clamps half-pixel source coordinates before forming
interpolation weights. This fixes negative weights at upscaled raster edges and
matches the CPU image path. Its existing up/downscale regression retains its
original tolerance; the RGB kernel bodies are unchanged by this correction.

Build and run the focused native CPU regression with a compile-time filter:

```sh
zig build --build-file zig/pkg/inference/build.zig test-bin \
  -Dtest-filter="Gemma4 PLE context normalization" -Doptimize=ReleaseFast \
  --cache-dir .zig-cache/native --prefix zig-out -j1
zig-out/bin/antfly-inference-tests --test-filter "Gemma4 PLE context normalization"
```

Passing only the filter after `--` selects runtime execution and still compiles
the complete test binary.

## PJRT residual precision experiment

[PR #1404](https://github.com/antflydb/antfly/pull/1404) separates F32 residual
accumulation from BF16 branch/matrix and KV publication. The multimodal exporter
can test that design with `--precision-policy f32-residual-media`: dense E4B
decoder residual sums/scaling use F32, text matrices and KV remain BF16, and
media parameters use F32. The default remains `bf16`. This additional F32 media
policy is an experiment, not an exact reproduction of the PR's runtime.
The manifest records the policy and actual parameter dtypes, and the executable
cache identity changes with it. Export and validate into separate directories.
The adapter rejects MoE models; the independent oracle is unchanged.

The PR's posted timings use GB10 and a different generation harness. That
harness defaults to eight output tokens and allows EOS stopping; this suite
forces 64–256 output tokens. Exact commands and output counts are needed before
comparing the posted totals with L4 results. This runner already retains donor
KV buffers on the GPU; it does not recompute the full prefix per decode step.

## Optional llama.cpp oracle

`llama_gemma4_reference.cpp` uses the unmodified llama/libmtmd C APIs. Build it
against matching llama.cpp headers and libraries (the review used b12387):

```sh
c++ -std=c++17 -O2 -I"$LLAMA_HEADERS" zig/lib/pjrt/tools/llama_gemma4_reference.cpp \
  -L"$LLAMA_LIB" -lllama -lmtmd -lggml-base -lggml -o llama-gemma4-reference
GGML_BACKEND_PATH="$LLAMA_LIB" ./llama-gemma4-reference \
  "$GGUF" "$MMPROJ" "$REFERENCE_INPUTS" "$REFERENCE_OUTPUTS"
```

Each input-directory `cases.tsv` row contains whitespace-separated case ID,
kind (`text`, `image`, `audio`), absolute media path (`-` for text), expected prompt tokens and output-token
budget. `<id>.prompt` contains the exact rendered prompt, with
`[ANTFLY_REFERENCE_MEDIA]` at the media location. The wrapper checks prompt
geometry, saves full first-step logits and fixed-budget greedy tokens, and
records prepared prompt IDs and audio embeddings. Its vision budget is 256–280
soft tokens, context 8192, F16 KV and flash attention. Its default mode does not
measure performance. Video uses the Transformers oracle. Treat frontend disagreements as investigation results; do not
switch oracle per case to obtain a passing gate.
Use libraries built for the host's libc; the retained b12387 binaries required an
existing compatible runtime loader on this host.

The wrapper also accepts `--measure WARMUPS REPEATS`. This mode uses a greedy
backend sampler, scalar token readback and synchronized fixed-budget decoding.
It excludes model load, KV reset, prompt-template rendering and output-text
decoding. Media file reads, preprocessing and towers remain inside timing.
Validation uses full-logit CPU argmax and marks its timings unavailable.
Only the synchronized total is used for latency comparisons; the phase fields
use submission boundaries and are not independent synchronized GPU timings.
Use matching modern headers/libraries supporting `llama_process` and extended
batches; the fresh comparison uses official CUDA release b12521. The comparison
requires the measured GPU-sampled sequence to equal the full-logit oracle's
entire fixed-budget sequence.

`compare_gemma4.py` runs three alternating fresh-process rounds over common
text/image/audio cases, with one warmup and two measured calls per case by
default. It accepts the benchmark's model/runner/plugin/suite/cache arguments,
plus `--llama-runner`, `--llama-library`, `--llama-inputs`, repeated `--cpu`
reference directories and a fresh `--out`. For binaries requiring a newer libc,
both `--llama-loader` and `--llama-runtime` select an existing compatible runtime.
Prepare PJRT executables with the exact worker before running it.

This comparison uses the six native accuracy overrides above, checks independent
first-step logits before timing and reports competitor failures. It verifies
prepared llama.cpp prompt IDs, complete output budgets and stable greedy tokens
within/across rounds. It records the llama.cpp timing boundary difference rather
than claiming identical serving boundaries. Median improvement of at least 5%
with no p95 regression is the latency-win criterion; first-step accuracy and
latency wins do not certify whole-generation correctness.

For raw video boundary diagnosis, run the opt-in test
`gemma4 real video exports decoded RGB parity fixture` with
`ANTFLY_GEMMA4_VIDEO_PARITY_INPUT` set to the MP4 and
`ANTFLY_GEMMA4_VIDEO_PARITY_OUTPUT` to an existing directory. The test deliberately
downloads selected NV12 surfaces and RGB8 frames. Compare them with:

```sh
python zig/lib/pjrt/tools/gemma4_video_pixels.py --input "$VIDEO" \
  --capture "$CAPTURE" --out "$CAPTURE/summary.json"
```

This diagnostic compares timestamps, raw eight-bit SDR decoder surfaces and RGB
conversion before resize. It records PyAV/FFmpeg versions and hashes. It does
not widen the model gate or change the production color policy.

The manifest records export-time source hashes; campaign provenance separately
records the timed preparer and workers. Token IDs are variable program inputs.
The video spacing alignment preserves input shapes, frame metadata and static
soft-token positions, so it does not require recompiling the prepared HLO.
