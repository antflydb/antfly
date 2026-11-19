# GLiNER2.5 CUDA comparison and qualification

These offline tools compare GLiNER2.5 Base and the original Decide checkpoint
through native CUDA, Antfly's native Zig/PJRT worker and upstream Python.
[Current results and the latest published CUDA comparison](GLINER25_RESULTS.md)
are retained in one report, with compact provenance in
[gliner25-l4.json](gliner25-l4.json). Models, raw vectors, HLO, executables and
measurement timelines belong in ignored `evidence/` directories.

Base's automatic route retains FP32 weights/state and uses compensated Tensor
Core projections. Decide additionally selects its qualified BF16 profile on
NVIDIA L4 SM89, driver API >=13000 and cuBLAS 130400 or 130401. Other artifacts,
GPUs/runtimes and training retain existing routes. Full serving policy and
workspace/admission limits are documented in the
[model guide](../../../pkg/inference/models/gliner2/GLINER25.md#qualified-decide-bf16-default-on-nvidia-l4).

The PJRT worker executes trusted local HLO artifacts; it is separate from the
HTTP server's XLA backend. Python exports graphs and feeds parameters, while
measured PJRT inference runs through the Zig C API. All parameters remain
resident, including unused heads. The exporter records checkpoint/configuration
hashes, dependency versions and source identities.

## Build and prepared-input comparison

Export uses CPU PyTorch with `gliner2==2.1.0`, `torchax==0.0.13`,
`transformers==5.23.0`, JAX/JAXlib `0.10.2`, NumPy, ml_dtypes and safetensors.
The harness also requires psutil and nvidia-ml-py. PJRT needs a matching CUDA
plugin and shared libraries. The Python CUDA BF16 reference runs in a separate
CUDA-enabled PyTorch environment; measured reference versions are recorded in
`gliner25-l4.json`. Leave `XLA_FLAGS` unset.

From the worktree root, set `MODEL_DIR` to the retained checkpoint directory,
`PJRT_CUDA_PLUGIN` to its plugin library and `EVIDENCE` to a fresh evidence root:

```sh
export ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global"
zig build --build-file zig/lib/pjrt/build.zig test gliner25-runner \
  -Doptimize=ReleaseFast --cache-dir "$PWD/.zig-cache/native" \
  --prefix "$PWD/zig-out" -j1
zig build --build-file zig/pkg/inference/build.zig bench-gliner25-core-build \
  -Doptimize=ReleaseFast -Dcuda=true -Dcuda-artifacts=sm89 \
  --cache-dir "$PWD/.zig-cache/native" --prefix "$PWD/zig-out" -j1
python zig/lib/pjrt/tools/prepare_gliner25.py \
  --model "$MODEL_DIR" --out "$EVIDENCE/graphs" --precision fp32
```

The exporter creates exact 128/512-token classification shapes at batches 1/8.
For Base extraction use `--kind entities --batches 1`. Use `--precision bf16`
and a separate output directory for BF16 graphs. Optional Decide batch 32 runs
as four native forwards of eight; the Base core runner supports at most eight.

```sh
python zig/lib/pjrt/tools/benchmark_gliner25.py \
  --native zig-out/bin/antfly-gliner25-core \
  --runner zig-out/bin/antfly-pjrt-gliner25 --plugin "$PJRT_CUDA_PLUGIN" \
  --model "$MODEL_DIR" --manifest "$EVIDENCE/graphs/model.json" \
  --out "$EVIDENCE/preparation" --cache "$EVIDENCE/executables" \
  --prepare-only --iterations 1
flock /tmp/antfly-l4-bench.lock python zig/lib/pjrt/tools/benchmark_gliner25.py \
  --native zig-out/bin/antfly-gliner25-core \
  --runner zig-out/bin/antfly-pjrt-gliner25 --plugin "$PJRT_CUDA_PLUGIN" \
  --model "$MODEL_DIR" --manifest "$EVIDENCE/graphs/model.json" \
  --out "$EVIDENCE/comparison" --cache "$EVIDENCE/executables" \
  --rounds 3 --iterations 8 --native-precision fp32
```

A frozen `--control` executable adds a third rotating arm; `--control-precision`
sets its precision. Preparation/compilation are excluded from warm latency.
Timing includes input upload, forward allocations, encoder/requested heads and
synchronized F32 readback; excludes tokenization, final decoding and HTTP.
Memory includes startup and the coordinator/parameter feeder in both arms,
with simultaneous process-tree PSS+NVML sampled every 0.2 seconds. Cache identity
binds GPU/driver, plugin, worker and manifest. Use an idle GPU and the shared lock.

The generic comparison retains strict raw-output/decision checks and complete
failure records. Its BF16 characterization gate is distinct from the independent
Python CUDA BF16 confidence qualification below; a peer comparison does not
qualify a serving precision.

## Independent numerical qualification

`qualify_gliner25.py prepare` accepts a JSON list of texts and captures upstream
CPU FP32 outputs for classification or Base entities. Its `run` command compares
native/control outputs against that oracle with a fixed 0.002 raw-output limit
and exact classifications/canonical entity routing. Use
`ANTFLY_INFERENCE_CUDA_DISABLE_GLINER_BF16=1` when qualifying Decide's accurate
FP32 fallback with `--native-precision auto`.

For Decide BF16, prepare 89 deterministic cases covering empty/Unicode/negated
text, five custom schemas/thresholds, heterogeneous batches and batch 32:

```sh
python zig/lib/pjrt/tools/prepare_gliner25_bf16_validation.py \
  --model "$MODEL_DIR" --out "$EVIDENCE/validation.json"
flock /tmp/antfly-l4-bench.lock python zig/lib/pjrt/tools/benchmark_gliner25_torch.py \
  --model "$MODEL_DIR" --manifest "$EVIDENCE/validation.json" \
  --out "$EVIDENCE/torch.json" --warmups 0 --iterations 2
flock /tmp/antfly-l4-bench.lock zig-out/bin/antfly-gliner25-core "$MODEL_DIR" \
  "$EVIDENCE/validation.json" "$EVIDENCE/native.json" 0 2 auto
python zig/lib/pjrt/tools/qualify_gliner25_bf16.py \
  --manifest "$EVIDENCE/validation.json" --native "$EVIDENCE/native.json" \
  --reference "$EVIDENCE/torch.json" --config "$MODEL_DIR/config.json" \
  --out "$EVIDENCE/qualification.json" --allow-boundary-changes
```

Use fresh outputs for each independent process. The reference is stock Python
CUDA BF16 with inference mode, no autocast and its default reduced precision
reduction setting. Confidence drift is capped at 0.02. Threshold changes require
a reference distance <=0.02; argmax changes require a reference probability gap
<=0.04 and per-label drift <=0.02. Decisions outside those bands must match.
Entity selection still requires exact matches. Base BF16 remains unqualified.

The prepared benchmark records actual encoder dtype and physical forward counts.
The loaded-model benchmark (`bench-gliner25-decide-build`) also defaults to
`auto`, reports effective precision and derives observed forwards from the pinned
checkpoint's 24 FFN layers, enabling a batch-32/four-forward check. Its public
pipeline includes rendering, tokenization and decoding, and excludes HTTP.
Automatic production selection ignores diagnostic semantic flags.

To expose first-seen request costs, run a variable-length public-cases file with
`--all-cases 1 --warmup 0 --reps 3`. Its format is
`{"format_version":1,"cases":[{"id":"short","texts":["Room 12 needs towels"],"tasks":{"intent":["maintenance","other"]}}]}`;
add different text lengths, schemas and batch sizes to the cases array. This is
the public request format, separate from the prepared-tensor manifest above.
`first_forward_ms` records the first invocation before sorting samples, including
the first warmup when warmup is enabled. `cuda_transfers.plan_cache_misses` and
`matmul_tuning_searches` report per-case deltas across warmup and measured calls.
GLiNER serving uses heuristic plans: new shapes can miss the cache, but must
record zero timed tuning searches. Retain cold latency separately from warm
medians; model loading and HTTP remain outside this benchmark.

```sh
python -m unittest discover -s zig/lib/pjrt/tools -p 'test_*.py'
```
