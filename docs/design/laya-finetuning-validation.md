# Laya native finetuning validation

Initial implementation and smoke evidence measured September 21, 2026, following
inference PR #815. Current full-size quality, numerical, lifecycle, and resource
results are in [the production qualification report](laya-production-qualification.md). This change adds
`antfly inference finetune train laya <job.json>` and the `train-laya` build
target. See [the training guide](../guides/laya.md#native-finetuning) for data
and job configuration.

The implementation trains the full ModernBERT encoder and Laya decision heads
with AdamW, separate encoder/head learning rates, cosine scheduling, gradient
clipping, and accumulation. It supports the notebook's RLCD-plus-soft-CE
objective and soft CE alone. Checkpoints preserve optimizer moments, unfinished
accumulation, and deterministic data/dropout/noise order. Serving exports retain
the upstream tensor layout, preserve the unsupervised action head, and reset or
refit calibration using an independent calibration split.

The graph preserves fused QKV and GeGLU weights, exact GELU, split-half RoPE,
layer-zero identity attention norm, padding, and alternating global/local
attention. It does not use the older simplified ModernBERT training graph.
The native graph and objective remain shared between CPU and Metal. Metal's
materialized interpreter can produce host-backed gradients, so gradients are
explicitly staged into provider-owned tensors before resident AdamW updates.
Linear layers and normalization use primitive operations in both the forward
and backward graphs. This avoids inference caches holding snapshots of mutable
weights, including normalization cache collisions for device-only parameters.

## Reference and lifecycle evidence

The reference uses upstream `common.py` at
`6a5819129eb220570792e417e49723d697efd76f`, SHA-256
`f948ee606abe2ed2463f830c051f1a60dccc1b9f5ca1fdc15635cfcbe0cff7b2`.
The inspected [notebook](https://github.com/NandhaKishorM/laya/blob/main/notebooks/laya_finetune_typed_decisions_2xT4_kaggle.ipynb)
snapshot has SHA-256
`4c43f0e2c0e271836b43d20d684adc4b5555cd74ebae2cfea3b738a1261a10a3`.
Python is PyTorch 2.10.0 / Transformers 5.1.0; Zig is pinned 0.16.0.

The deterministic miniature fixture has two encoder layers and two head layers.
It covers all three decision types, soft targets, different option counts,
padding, raw logits, the sampled RLCD objective, and every trained parameter.
The PyTorch and Zig objective consume identical Gaussian samples; dropout is
disabled for this numerical comparison.

| Check | Result |
| --- | --- |
| CPU parameter-gradient parity | 45 tensors; max absolute error `1.0281801e-6` |
| Metal parameter-gradient parity | 45 tensors; max absolute error `8.940697e-7` |
| CPU resume with unfinished accumulation and dropout | Byte-identical exported weights to uninterrupted run |
| Metal resume with unfinished accumulation and dropout | Byte-identical exported weights to uninterrupted run |
| Released Metal-trained export, CPU and Metal serving | Exact tokenization; maximum decision-probability error `2.7567148e-7` on CPU and `6.556511e-7` on Metal; action probabilities pass `5e-5` |
| Data conversion | Six Python tests pass, including label order and malformed distributions |
| Unified CLI | `finetune train laya --help` resolves the native command |

The CPU Laya suite also includes inference regressions, data overlap rejection,
configuration rejection, and calibration minimum-count behavior. Optional
released-checkpoint and exported-checkpoint tests require their reference paths;
an absent reference is reported as a skip. An explicitly requested unavailable
Metal device fails instead of selecting CPU.
The final CPU suite selected 16 checks: 14 passed and two optional references
were skipped. The focused Metal training suite passed all eight checks. The
exported-checkpoint check was separately exercised on both backends.

The full finetuned gradient fixture (weights SHA-256
`5a831bd55c29164f4b25825e317ebcfdddb405beb522d8937cdeb9d411a98a9e`)
also passes the raw decision-logit and RLCD
loss/cotangent checks at `2e-4` on both backends. **The initial FP32-reference comparison failed:** at the unchanged per-tensor maximum-error gate
`5e-5 + 0.002 * max(abs(reference))`, CPU passes 197/201 tensors and Metal
passes 198/201. The largest relative L2 error among the failing tensors is
`0.003758` on CPU and `0.003802` on Metal. Maximum absolute errors over all
gradients are `0.009080887` and `0.005592346`, respectively.

Layer traces show one CPU and two Metal sign crossings in the first head's
ReLU inputs compared with PyTorch; small forward-rounding differences can
change these nonsmooth derivatives. This observation does not waive the
gradient gate or establish that every discrepancy has that cause. The original FP32 compatibility fixture still reports these failures. Subsequent
PyTorch FP32-versus-FP64 experiments reproduced all four outliers, and both native
backends passed all 201 gradients against the higher-precision reference at the
unchanged tolerance. See the current qualification report for that attribution
and fresh released-source and trained-artifact checks.

## Released-checkpoint smoke

The source is `convaiinnovations/laya` revision
`c5d78730f3493e4fe16d61507ef4b78eef7318cf`, the same revision used in the
[inference qualification](laya-qualification.md). Source weights SHA-256:
`891102d372688fc2a094dac56a384bc537b87c63f21f9f3dac0be2b7cbc8d86c`.

A ReleaseFast CPU run completed three optimizer steps using three synthetic
training decisions and three disjoint synthetic evaluation decisions. Settings:
one epoch, batch one, accumulation one, soft CE, no head dropout, encoder LR
`2.5e-5`, head LR `1e-4`, weight decay `0.01`, gradient clipping `1.0`, seed 42.
The training fixture generator writes the exact JSONL inputs.

| Metric | Before | After |
| --- | ---: | ---: |
| Held-out soft CE | 2.4338261 | 2.9679364 |
| Held-out argmax accuracy | 0 / 3 | 0 / 3 |
| Ordinal expected-value error | 0.4870228 | 0.1129786 |

Tracked host-allocation peak was `20,541,449,360` bytes (about 19.1 GiB). This
does not include every process/driver allocation and is not a no-paging or peak
RSS qualification. The exported FP32 weights have SHA-256
`c287d3caf4a5af1eeec1a3f4c0cb2ca3b705733f5585e07d44a016bdf9a21eab`.

**This is a mechanical smoke test, not a quality result.** The synthetic targets
are unrelated to the base model's learned task distributions. Held-out CE
worsened; this run establishes updates, durable checkpoints, and complete
export, not useful domain adaptation. This initial smoke did not include a typed-decisions or application-specific
accuracy campaign; the later pinned offline campaign is reported separately.

A separate released-model Metal run completed one optimizer update, durable
checkpointing, and serving export with one synthetic choice training example
and one disjoint evaluation example. Held-out soft CE changed from `2.7496723`
to `2.0140076`; accuracy remained 0/1. Tracked host-allocation peak was
`9,656,785,084` bytes (about 9.0 GiB), excluding GPU/driver allocations. This
small smoke does not qualify sustained training memory use or throughput.
Exported weights SHA-256:
`a18f634e0428f0840e44b9cfd085acab456cc62d2566f2fad93550b9556fccda`.
Independent PyTorch serving parity for this export passed on CPU and Metal.

Local diagnostic artifacts:

- `/tmp/antfly-laya-training-reference`: miniature source, PyTorch logits,
  gradients, shared noise, and train/eval JSONL.
- `/tmp/antfly-laya-tiny-cpu-run`: standalone CPU CLI report and export;
  all 45 trained tensors changed and all action-head tensors stayed identical.
- `/tmp/antfly-laya-released-cpu-run-v2`: released-model job, metrics, optimizer
  checkpoint, report, and exported model.
- `/tmp/antfly-laya-released-metal-run`: full-size Metal update and export.
- `/tmp/antfly-laya-metal-export-reference`: independent PyTorch
  tokenization and serving probabilities for the Metal-trained checkpoint.
- `/tmp/antfly-laya-released-export-reference`: full-checkpoint gradient and
  activation oracle; the retained original FP32 gradient compatibility failure detailed above.

These `/tmp` artifacts are transient diagnostics, not release evidence.

## Reproduce

Follow the miniature fixture and native training checks in the guide. To
validate an actual exported model against independent PyTorch inference:

```sh
python3 scripts/laya_export_reference.py \
  --model /absolute/run/model --common /tmp/laya-common.py --output /tmp/laya-export-oracle
cd zig
ANTFLY_LAYA_EXPORT_REFERENCE=/tmp/laya-export-oracle \
  python3 tools/run_bounded_zig_build.py --zig /path/to/zig-0.16.0 \
  build inference-test -Dmetal=false -Dcuda=false -Donnx=false \
  -- --test-filter 'laya finetuned export'
```

Use `ANTFLY_LAYA_METAL=1` and `-Dmetal=true` for Metal. This compares exact token
IDs and option markers, decision probabilities, and action probabilities at
the released-model tolerance of `5e-5`. The miniature raw-logit regression
retains its original `2e-4` absolute tolerance; released action logits can be
thousands in magnitude and are checked through their served probabilities.

For full-checkpoint gradient comparison, run `laya_training_reference.py` on
the exported-model oracle directory, then point `ANTFLY_LAYA_REFERENCE` to it
and select `laya training forward objective`. Use `--precision float64` for the
qualified higher-precision encoder/head oracle. This emits full FP32 gradient
artifacts and requires substantially more RAM and disk than the tiny fixture.
Use `-Doptimize=ReleaseFast` for this check; scalar debug kernels are slow at
released-model dimensions. Set `ANTFLY_LAYA_TRACE=1` to compare encoder/head
activations from `activations.safetensors` in the forward and backward graphs.

Sustained full-size Metal training memory/performance, CUDA/DDP, mixed precision, LoRA,
encoder dropout, activation recomputation, action-head supervision, and
representative training quality remain outside the evidence recorded here.
