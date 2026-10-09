# GLiNER2.5 development tools

This directory contains the retained native/Python inference comparisons and
trained-artifact checks. Historical campaigns and one-time fixture generators
belong in external evidence rather than the product tree.

## Fixtures and source identity

From the repository root:

```sh
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-fixtures
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-references
```

The fixture policy is documented in
[`../../testdata/gliner25/README.md`](../../testdata/gliner25/README.md).

Family captures keep numeric arrays compact while leaving object structure
reviewable. After regenerating a family capture, run:

```sh
python3 zig/pkg/inference/scripts/gliner25/format_family_captures.py
```

Then update every byte-level SHA-256 and size pin that names the changed
capture. Formatting must preserve the captured values and the historical
generator, request, contract, and source provenance stored inside the capture.

## Inference performance

The canonical direct-core comparisons are:

- [`BENCHMARK.md`](BENCHMARK.md): native CPU versus pinned Fastino CPU.
- [`METAL_BENCHMARK.md`](METAL_BENCHMARK.md): native Metal versus pinned
  Fastino MPS and CPU.
- [`CUDA.md`](CUDA.md): CUDA kernels and training, required-GPU tests, and
  pinned Fastino CUDA eager, compiled, mixed-precision and FlashDeBERTa candidates.

All comparisons verify model, source, token, and output identity before timing.
They write reports outside the repository and do not qualify HTTP serving or
release-tail performance.

## Training and artifact checks

The retained checkers each expose `--help` and write evidence to a caller-owned
output directory:

- `check_bundles.py` verifies converted model bundles and tensor identities.
- `check_trained_execution.py` runs bounded CPU or Metal inference against a
  trained artifact.
- `check_training_export.py` verifies portable full, head-only, LoRA, and DoRA
  exports and can compare them with the pinned Python runtime.
- `check_training_merge.py` verifies adapter materialization and merged output.
- `training_export_runtime.py` is the isolated Python execution worker used by
  the export checker.

Keep virtual environments, downloaded checkpoints, executables, reports, and
captured evidence outside Git. Use `.benchmark-results/gliner25/` or an external
artifact store for local evidence.

## ModernBERT encoder reference

`modernbert_reference.py` builds a tiny ModernBERT `BoundaryExtractor` with the
published head settings on the pinned upstream (it reuses `oracle.py`'s
runtime checks), saves it as a boundary checkpoint, and captures one padded
batch: token ids and routes, the routed encoder states, and every encoder
gradient for fixed cotangents. Its output is deterministic.

```sh
PYTHONDONTWRITEBYTECODE=1 <oracle venv>/bin/python zig/pkg/inference/scripts/gliner25/modernbert_reference.py \
  --upstream <GLiNER2 checkout at the pinned commit> --output <dir outside Git>
```

The tokenizer and `processor.json` are checked in under
`testdata/gliner25/modernbert_tokenizer` and pin the per-word tokenization
(each word is tokenized alone, as upstream does). The checkpoint and
`reference.safetensors` stay outside Git; set
`ANTFLY_GLINER25_MODERNBERT_REFERENCE=<dir>` to run the encoder parity test
(`ANTFLY_GLINER25_MODERNBERT_BACKEND=metal` for Metal) and the resident Metal
training-job test in `src/finetune/gliner/boundary_modernbert_test.zig`.

### Decide-1B production allocator comparison

Build the standalone benchmark separately from inference, from the repository root:

```sh
python3 zig/pkg/inference/scripts/gliner25/build_decide_benchmark.py --output .benchmark-results/decide-build.json
```

This calls `tools/run_bounded_zig_build.py` with one job and records the binary
and unchanged source snapshot. Measurement refuses a mismatched build receipt.

`benchmark_decide_service.py` runs the `decide-bench` command in that executable,
using `platform.processAllocator(smp_allocator)` and production IO. It keeps the
10 GiB process/generation envelope, one admitted request, two CPU threads, live
artifact qualification, and the 198-token serving ceiling. It hard-links pinned
files into a contained model directory; source and output must be on the same
filesystem. Unlike test-only symlinks, these pass production artifact containment.
The original testing-allocator qualification fixtures remain available for leak
and failure-path checks.

Each of six cases has one validation preflight, three warmups and twenty measured
samples. Two original cases are supplemented by four frozen CPU FP32 oracle
holdouts (40, 79, 93 and 174 tokens) in `decide_1b_short_holdout.json`. The generator
pins the upstream checkout, runtime, checkpoint and RoPE configuration and refuses
to overwrite a capture. Changing the fixture requires reviewing its new digest in
both benchmark implementations.

Four boundaries are reported separately: prepared encoder/head with completed
readback, loaded pipeline including schema compilation and tokenization, full
`Node.decideDirectJsonWithControl`, and in-process HTTP handler dispatch. The last
two include admission and serialization; HTTP excludes sockets. Pipeline samples
also check exact token IDs, ordered raw logits (2e-3), and presented probabilities
(5e-4). Service samples check outputs, token counts, model identity and idle
resource ownership outside the clock. Warm service phase means are printed to the
process log; they use existing scalar metrics and add no GPU synchronization.

Run one exploratory paired block, or omit `--blocks` for six paired blocks
(twelve serial processes) in AB/BA order (three cycles):

```sh
python3 zig/pkg/inference/scripts/gliner25/benchmark_decide_pairs.py \
  --binary zig/zig-out/bin/antfly-inference-bench-server \
  --build-receipt .benchmark-results/decide-build.json \
  --backend metal --blocks 1 \
  --model-dir /absolute/path/to/decide-1b \
  --upstream /absolute/path/to/pinned/GLiNER2 \
  --runtime-dir /absolute/path/to/pinned/python-runtime \
  --python /absolute/path/to/python3.12 \
  --output .benchmark-results/decide-paired-metal
```

Use `--backend native` for CPU after Metal. Every process runs serially; no builds
run inside the campaign. Keep the source and binary unchanged from the completed
build through the campaign. Receipts retain the source patch, untracked source
hashes, binary hash, command, host power/thermal/swap snapshots, raw samples and
whole-process resource usage. A failed child stops the campaign and remains in
its original output directory.

`compare_decide_performance.py` compares Antfly's complete HTTP handler against
the pinned Python loaded pipeline. Acceptance requires both original cases to
win in all six blocks with the paired-block bootstrap confidence interval below
one, plus no greater than 5% pooled p95 or held-out median regression. Missing
cases, failed runs, diagnostic runs, changed power sources and whole-process
swap growth fail acceptance. Whole-process
swap gating is conservative: it includes model loading, not just timed calls.
An exploratory one-block comparison always reports incomplete qualification;
serial p95 is descriptive and does not establish concurrent serving performance.

The Decide service now passes a validated internal extraction envelope and typed
classifier results across the span route. Name containment is checked once;
loaded identity, backend/geometry qualification, schema limits, request heap
admission, execution locking and cancellation remain in the existing executor.
Listing projects only `added_tokens` and `added_tokens_decoder` from tokenizer
JSON, avoiding a vocabulary/merge allocation on each request. Skipped subtrees
still undergo JSON syntax checking; full tokenizer semantics and exact consumed
artifact qualification remain load-time gates. No request/schema/result cache
or precision change is involved.

ModernBERT's existing packed exact-GeGLU hook is implemented by Metal and native
compute, removing intermediate gate/value tensors without changing the erf
activation. Native M-RoPE shares each token's phase across its heads with the
same FP32 arithmetic. On macOS with Accelerate, a single contiguous segment of
at most 198 tokens and 64-wide heads uses bounded dense attention; segmented,
noncontiguous and longer requests retain the general tiled path. The score
matrix is bounded to 156,816 bytes. The two-layer Metal cancellation cadence,
resident-weight ownership and admission limits are unchanged.

`TERMITE_METAL_DISABLE_PACKED_GEGLU=1` and
`ANTFLY_INFERENCE_DISABLE_SHORT_SEGMENT_ATTENTION=1` support diagnostic controls.
The campaign clears experimental inference flags; runs with injected profiling
or alternate dispatch must be marked `diagnostic_only` and cannot qualify.

The Metal encoder also has optional packed-QKV/split-half-RoPE and centered
residual-add/LayerNorm hooks. The former writes Q, K and V into one allocation
with independently retained device views and reuses one request-owned position
vector uploaded once for all encoder layers.
The latter preserves centered FP32 variance and returns both the normalized
activation and residual stream for ModernBERT; multilingual boundary attention
uses its norm-only form. Other backends and unsupported layouts retain the
existing operations. Neither hook changes the two-layer cancellation cadence.

For same-binary controls, pass `--diagnostic-disable qkv-rope` and/or
`--diagnostic-disable add-norm` to `benchmark_decide_service.py`. The runner
records these controls and marks their receipts ineligible for performance
acceptance. They map to `TERMITE_METAL_DISABLE_GLINER_QKV_ROPE=1` and
`TERMITE_METAL_DISABLE_GLINER_ADD_NORM=1`; normal paired campaigns clear them.
