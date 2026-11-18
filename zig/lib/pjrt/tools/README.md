# Native GLiNER2.5 CUDA / PJRT comparison

These offline tools export the upstream GLiNER2.5 computation to HLO and execute
it through Antfly's native Zig PJRT C API. Python prepares the graph and feeds
parameters; it does not execute timed inference. This experimental worker is
separate from the HTTP server's XLA backend.

The optimized FP32 L4 comparison is in [OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md).
The earlier explicit BF16 experiment is tracked separately in [BF16_RESULTS.md](BF16_RESULTS.md).
The subsequent Python CUDA BF16 qualification uses the separate policy below, with current results in
[BF16_QUALIFICATION.md](BF16_QUALIFICATION.md).
The original baseline is in [RESULTS.md](RESULTS.md), with compact
machine-readable provenance and metrics in [gliner25-l4.json](gliner25-l4.json).

The comparison covers `fastino/gliner2.5-base-v1` (DeBERTa base with the sparse
boundary head) and `fastino/GLiNER2.5-Decide` (DeBERTa large with the label-marker
classifier). Classifier cases use the same 14-label hotel intent/topic/urgency
schema for both checkpoints. Extraction cases exercise the base checkpoint's
five-label entity boundary head, including candidate proposal and pair scoring.

Both arms start with identical prepared tensors and end with synchronized F32
head outputs. Rendering, tokenization, final result decoding, HTTP, model load,
and post-readback cleanup are outside warm timing. Forward temporary allocations
remain inside warm timing. The native boundary path uses its
reference-v1 CUDA execution policy. This is a core comparison, not serving or
retrieval-quality qualification. Text is synthetic, with exact 128/512-token
geometries. Default physical batches are one and eight, with identical rows in
each batch. Optional Decide batch 32 uses four native forwards of eight versus
a single PJRT forward of 32; the base benchmark currently supports at most eight.

All checkpoint parameters stay resident in both workers, including unused heads.
FP32 uses highest JAX matmul precision. The initial BF16 characterization
compared PJRT BF16 against native FP32. The explicit `--native-precision bf16`
experiment now uses BF16 encoder and classifier/boundary projection weights,
direct BF16 attention projections, attention scores/probabilities, and FFN
intermediates. Embedding, normalization, bias storage and residuals remain FP32;
attention projection bias vectors are rounded to BF16 in a bounded immutable cache. Head
outputs and synchronized readback remain FP32. This hybrid precision is reported
separately from PJRT's BF16 graph and from the default native FP32 route.
Explicit precision selection remains gated. The newer automatic Decide BF16
profile has an independent source/hardware/runtime gate and Python BF16
qualification below. Numerical failures from the earlier experiment remain
failures; these measurements do not promote a PJRT serving backend. The native optimization preserves FP32 and automatically dispatches
only on its qualified SM89 geometry: up to eight rows, up to 512 tokens and head
dimension 64. It reuses a workspace capped at 32 MiB, including cuBLAS scratch
and pointer arrays; existing attention remains the fallback. Base and original
Decide FP32 also project compact relative-position rows. Eligible square and
expanding projections fuse their FP32 bias epilogues. CUDA entity-only requests
skip unrequested record-head work.

The diagnostic variables `ANTFLY_INFERENCE_CUDA_DISABLE_GLINER_FP32_ATTENTION`,
`ANTFLY_INFERENCE_CUDA_DISABLE_GLINER_POINTER_BATCHES`, and
`ANTFLY_INFERENCE_CUDA_DISABLE_GLINER_FP32_BIAS` disable the corresponding routes
for investigation. None is needed to enable the optimized default.

Experimental BF16 attention and projection operations share at most 64 MiB of
workspace, including conversion staging, vendor scratch and pointer arrays.
The FFN additionally uses at most 88 MiB of temporary storage at its largest
qualified geometry. Model projection matrices are uploaded directly as BF16;
there is no second resident FP32 copy. Bounded cuBLASLt algorithm tuning happens
at first use during excluded warmup, and cached plans are reused during timing.
Immutable BF16 relative projections have a separate model-owned cache capped at
96 MiB in total and 96 pairs. Projection-bias snapshots additionally have a
256 KiB and 128-vector sublimit within that total. Keys include all seven
checkpoint source identities, the
resident-weight generation, sequence/row geometry, layout, position buckets and
normalization epsilon. Entries own completed snapshots and retain backend-weight
admission when a controller is supplied. A denied reservation falls back to
ordinary projection computation. Bias keys bind source, generation and width;
conversion uses the same RNE kernel as the uncached route. A mutable normalized-
activation slot has an 8 MiB sublimit within the same 96 MiB total. It emits the
F32 result and its RNE BF16 activation in one kernel, retains shared admission,
and binds borrows to a generation plus exact source pointer/extent. New writes,
resizes and in-place mutations invalidate retired values and sibling views;
partial writes also invalidate the slot when their device ranges overlap.
Content Q/K/V tensors
are never cached. The explicit BF16 experiment also parallelizes boundary-head
attention over its 128-word band, with F32 score reductions and roughly 2 KiB
of CTA shared storage. Pool selection keeps exactly the requested sorted prefix
with the original score/index tie breaks, avoiding unused full sorts. Total
comparators permit PDQ sorting with identical ordering, and retained pool
compatibilities reuse the previously computed scalar-order dot products. Valid
Cartesian pairs have unique keys, so quotas update their best score directly
without sorting duplicate proposal entries. Scores at or below the padding
sentinel retain the full duplicate path to preserve their output positions.
These changes are checked against the full stable pool reference bit for bit. The
explicit BF16 head also uses warp-parallel query dots and bounded 512-thread
prefix scans at 256–512 positions. These reductions retain F32 outputs and
masked/padded semantics, but change addition order; separate serial GPU and F64
oracles check the kernels without weakening the full-model numerical gates.
These experiments target the same L4, batch-at-most-eight, 128/512-token
geometries as the comparison. Unsupported shapes retain ordinary operations.


## Build and export

Use a CPU PyTorch environment with `gliner2==2.1.0`, `torchax==0.0.13`,
`transformers==5.23.0`, JAX/JAXlib `0.10.2`, NumPy, ml_dtypes, safetensors,
psutil and nvidia-ml-py. The native worker needs the matching CUDA PJRT plugin
and CUDA/cuDNN shared libraries. Leave `XLA_FLAGS` unset. Every export records
checkpoint/sidecar hashes, dependency versions and the GLiNER2 source digest.

From the worktree root:

```sh
export ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global"
zig build --build-file zig/lib/pjrt/build.zig test gliner25-runner \
  -Doptimize=ReleaseFast --cache-dir "$PWD/.zig-cache/pjrt" \
  --prefix "$PWD/zig-out" -j1
zig build --build-file zig/pkg/inference/build.zig bench-gliner25-core-build \
  -Doptimize=ReleaseFast -Dcuda=true -Dcuda-artifacts=sm89 \
  --cache-dir "$PWD/.zig-cache/native" --prefix "$PWD/zig-out" -j1

python zig/lib/pjrt/tools/prepare_gliner25.py \
  --model "$MODEL_DIR" --out evidence/graphs --precision fp32
# For base extraction, use --kind entities --batches 1.
# For BF16 classification or extraction, use --precision bf16 and a separate output directory.
```

## Measure and validate

Use an idle L4. The worker's memory policy is bounded for the measured 23,034 MiB
device; other hardware is not qualified by these results. The driver checks for
other GPU compute processes before each arm. Cache identity binds the GPU/driver,
plugin, worker and manifest. Executable/HLO/checkpoint hashes are retained.

```sh
common="zig/lib/pjrt/tools/benchmark_gliner25.py"
python "$common" --native zig-out/bin/antfly-gliner25-core \
  --runner zig-out/bin/antfly-pjrt-gliner25 --plugin "$PJRT_CUDA_PLUGIN" \
  --model "$MODEL_DIR" --manifest evidence/graphs/model.json \
  --out evidence/preparation --cache evidence/executables --prepare-only \
  --iterations 1
python "$common" --native zig-out/bin/antfly-gliner25-core \
  --runner zig-out/bin/antfly-pjrt-gliner25 --plugin "$PJRT_CUDA_PLUGIN" \
  --model "$MODEL_DIR" --manifest evidence/graphs/model.json \
  --out evidence/comparison --cache evidence/executables \
  --rounds 3 --iterations 8 --native-precision fp32
```

To measure a native optimization against a frozen control, copy the control
binary before rebuilding and add `--control /path/to/control` to the comparison
command. Three arms then rotate through a Latin order over three rounds, using
fresh processes for every arm. The summary reports native speedups over both the
control and PJRT, with numerical gates applied across all three. When comparing
a BF16 native candidate to a frozen FP32 control, also pass
`--native-precision bf16 --control-precision fp32`. The summary separately
retains each arm's raw error and decisions/candidate routing against the
candidate, so the FP32/BF16 drift remains visible.

Preparation and compilation are separate from warm qualification. Fresh processes
alternate native/PJRT for three rounds, with three warmups and eight measured
forwards per case/round. Reported warm medians pool 24 samples. Memory polling
covers the whole common Python harness tree, including the parameter feeder and
worker, during loading/deserialization and inference. Cold cache preparation has
its own memory report and must not be presented as the warm memory measurement.

FP32 requires maximum component error at most 0.002; BF16 uses 0.02. These imply
sigmoid error bounds of 0.0005 and 0.005, respectively. Classification decisions
must agree; extraction candidate indices and validity masks must agree exactly.
The shortest singleton case also has an independent CPU reference. Failures
remain failures: the tools retain complete results and exit nonzero, rather than
relaxing a gate. Raw models, HLO, executables, tensors and traces stay under
ignored local directories. Only compact aggregate results belong in the branch.

```sh
python -m unittest discover -s zig/lib/pjrt/tools -p 'test_*.py'
```

## Automatic Decide BF16 qualification

The newer `bf16_full` profile is separate from the earlier hybrid `bf16`
experiment above. Automatic selection is restricted to the original dense
Decide artifact, NVIDIA L4 SM89, driver API >=13000 and cuBLAS version 130400 or 130401.
Base retains its accurate FP32 default. The production automatic constructor
ignores diagnostic semantic flags; `ANTFLY_INFERENCE_CUDA_DISABLE_GLINER_BF16=1`
opts out before upload. Explicit `fp32` remains an additional diagnostic control.

The reference is upstream eager Python CUDA BF16, with inference mode, no
autocast and the stock BF16 reduced precision reduction setting. The fixed
confidence tolerance is 0.02. With `--allow-boundary-changes`, multilabel changes
require a reference threshold distance <=0.02; argmax changes require a reference
probability gap <=0.04 and per-label drift <=0.02. All other decisions must match.
Entity selection still requires exact matches. This does not redefine the older
CPU FP32 raw-output gate or retrospectively pass its failed BF16 experiments.

Prepare 89 deterministic classification cases on CPU, including heterogeneous
batch 32, short/empty/Unicode/negated text, five custom schemas and thresholds:

```sh
python zig/lib/pjrt/tools/prepare_gliner25_bf16_validation.py \
  --model "$MODEL" --out "$EVIDENCE/model.json"
flock /tmp/antfly-l4-bench.lock python zig/lib/pjrt/tools/benchmark_gliner25_torch.py \
  --model "$MODEL" --manifest "$EVIDENCE/model.json" \
  --out "$EVIDENCE/torch.json" --warmups 0 --iterations 2
flock /tmp/antfly-l4-bench.lock "$NATIVE" "$MODEL" \
  "$EVIDENCE/model.json" "$EVIDENCE/native.json" 0 2 auto
python zig/lib/pjrt/tools/qualify_gliner25_bf16.py \
  --manifest "$EVIDENCE/model.json" --native "$EVIDENCE/native.json" \
  --reference "$EVIDENCE/torch.json" --config "$MODEL/config.json" \
  --out "$EVIDENCE/qualification.json" --allow-boundary-changes
```

Use fresh output names for each independent process. The prepared benchmark
records the actual encoder weight dtype, not just the requested precision.
The loaded-model benchmark (`bench-gliner25-decide-build`) also defaults to
`auto`, reports effective precision and derives observed physical forward counts
from the pinned Decide checkpoint's 24 FFN layers, enabling a direct batch-32
four-forward check. It measures the public pipeline including rendering,
tokenization and decoding; the prepared benchmark excludes those stages.
Neither scope includes HTTP. Keep GPU campaigns serialized with the same lock.
