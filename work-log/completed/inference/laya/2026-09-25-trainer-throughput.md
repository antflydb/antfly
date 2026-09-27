# Laya trainer throughput and state-cache memory: 2026-09-25

Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../zig/pkg/inference/models/laya/LAYA.md)
(State cache, Trainer throughput).

Host: Apple M4 Max, 36 GiB, macOS 15 (Darwin 24.6.0), Zig 0.16.0, ReleaseFast.
Data and job: `scripts/laya/prepare_laya_training_data.sh .tmp/laya`, then
`antfly-inference finetune train laya .tmp/laya/td/prof.json` (packed question
mode, batch size 1, 14 microbatches of the step-0 train subset, released
checkpoint `c5d78730f3493e4fe16d61507ef4b78eef7318cf`). Frozen runs add
`"freeze_layers": N` to the same job.

## Per-step wall time and loss

Each step is `batch seconds loss`.

```
unframed (ANTFLY_LAYA_TRAIN_UNFRAMED=1), median 5.34 s
1 6.7 1.6273;2 5.55 1.4148;3 5.6 1.9196;4 5.31 1.1551;5 4.45 0.7116;6 5.23 0.6766;7 5.14 0.2296;8 5.55 1.1614;9 4.5 0.9236;10 5.38 0.5738;11 4.48 0.6766;12 5.38 1.4978;13 5.51 1.8385;14 5.43 0.8051
framed (default), median 2.02 s
1 2.61 1.6273;2 2.03 1.4148;3 2.09 1.9196;4 1.9 1.1551;5 1.56 0.7116;6 2.08 0.6766;7 2.0 0.2296;8 2.23 1.1614;9 1.7 0.9236;10 2.04 0.5738;11 1.59 0.6766;12 1.91 1.4978;13 2.21 1.8385;14 2.03 0.8051
framed, freeze_layers 11, median 1.41 s
1 1.61 1.6273;2 1.38 0.9854;3 1.53 1.893;4 1.35 1.0983;5 1.11 0.6937;6 1.48 0.6993;7 1.34 0.1582;8 1.63 1.2782;9 1.11 0.926;10 1.47 0.6645;11 1.1 0.7686;12 1.34 0.8299;13 1.63 1.832;14 1.48 0.8211
framed, freeze_layers 18, median 1.17 s
1 1.11 1.6273;2 1.1 0.8034;3 1.23 1.9789;4 1.11 1.1772;5 0.89 0.864;6 1.22 0.6721;7 1.1 0.301;8 1.34 1.5027;9 0.89 1.0155;10 1.22 0.7304;11 0.89 1.0149;12 1.1 0.8149;13 1.34 1.8038;14 1.22 0.9628
framed + one command batch per optimizer transaction, median 1.68 s
1 2.66 1.6273;2 1.81 1.4148;3 1.74 1.9196;4 1.63 1.1551;5 1.35 0.7116;6 1.77 0.6766;7 1.58 0.2296;8 1.94 1.1614;9 1.25 0.9236;10 1.73 0.5738;11 1.24 0.6766;12 1.6 1.4978;13 1.96 1.8385;14 1.73 0.8051
+ runtime inputs uploaded once, zero-copy gradient hand-off, median 1.38 s
1 1.96 1.6273;2 1.46 1.4148;3 1.5 1.9196;4 1.3 1.1551;5 1.08 0.7116;6 1.46 0.6766;7 1.31 0.2296;8 1.61 1.1614;9 1.08 0.9236;10 1.46 0.5738;11 1.08 0.6766;12 1.32 1.4978;13 1.58 1.8385;14 1.43 0.8051
```

Losses are identical to the unframed trainer at every step in both runs.

## Profiles (`sample`, 20 s of a steady-state run, main thread)

At 2.02 s per step: optimizer update ~42% (every snapshot, elementwise op
and zero fill submitted and waited alone), backward ~33% (GPU ~0.36 s, host
encoding ~0.13 s), forward ~15%, inputs ~5%.

After batching the optimizer (1.68 s): the update fell to ~0.23 s per step.
Host encoding (~0.19 s) turned out to be ~95% `add`/`multiply` uploading
host-backed runtime inputs (attention biases, RoPE tables, masks) through a
fresh staging buffer at every use. The gradient hand-off's per-gradient copy
added ~0.06 s.

After uploading inputs once and dropping that copy (1.38 s): GPU forward
~0.13 s and backward ~0.34 s, optimizer ~0.21 s (its snapshot copies and
full-state finiteness reads), inputs ~0.07 s (dropout random numbers on the
host), encoding ~0.05 s.

Earlier reference points on the same job: 8.55 s median with the step-0
trainer, 6.43 s after device strided slices, 2.65 s framed with a frame flush
after every `neg` (the workaround before the pool fix below).

## In-frame buffer reuse bug

Framed training failed with `NonFiniteTrainingUpdate` on
`laya training interrupted accumulation resumes to identical serving weights`
while the single-step gradient parity test passed. The investigation:

1. Flushing the frame after any of several unrelated ops (`add`, `mul`,
   `transpose`, `broadcast_in_dim`, `neg`) hid the failure, so it was a hazard
   across a window rather than one bad kernel.
2. `TERMITE_METAL_BUFFER_REUSE=0` fixed it, which pointed at the frame reuse
   pool.
3. Zero-filling buffers served from the pool fixed it, and filling them with
   0xFF did not. Restricting the fill by op isolated `scatter_add`, and
   restricting by size isolated its small allocations.
4. Downloading the grouped-scatter inputs showed that one `order` index array
   (128 entries, for a 3-row table) held a single repeated f32 value
   (`0xb7474ae0`, about -1.19e-5) instead of the uploaded indices.
5. A write history per buffer handle showed that the buffer's last recorded
   writes were two uploads, and the `order` upload never reached the private
   blit path. The buffer was shared storage: the pool had taken a shared
   buffer released earlier in the frame and returned it for a private
   request. The upload was an immediate `memcpy`, and the previous owner's
   queued GPU write ran afterwards when the frame was submitted.

Fix: `termite_metal_decode_runtime_release_buffer` pools only
`MTLStorageModePrivate` buffers. Regression test: "metal in-frame buffer reuse
never hands a host-writable buffer to a private request" (fails without the
fix). After the fix, framed training needs no op flushes, and every Laya test
passes on CPU and Metal.

## State cache in f16

`pipelines/laya_packed_test.zig`, cached vs full row, released-format fixture:

```
question f32: max error 1.67e-6, hits=2 misses=1 bytes=46080
question f16: max error 5.46e-4, hits=2 misses=1 bytes=23040
candidate f32: max error 1.67e-6, hits=2 misses=1 bytes=46080
candidate f16: max error 5.32e-4, hits=2 misses=1 bytes=23040
```

## Seed variance, frozen layers and the device-slice question

Step-0 recipe (packed question mode, `td/s0-*`), serving-evaluator accuracy on
760 decisions. "Step-0 trainer" means `TERMITE_METAL_DISABLE_DEVICE_STRIDED_SLICE=1`,
which is bit-identical to commit 0efff7fb83 for 80 steps (losses and gradient
norms equal).

```
trainer           seed 42  seed 43  seed 44
step-0 (host)     0.5737   0.3711   0.5566
current           0.4342   0.4605   0.4553
current fl=11     0.5184   0.5447   0.4724
current fl=18     0.4645   0.4908   0.4645
```

Training CE, mean of each 100-step block (dropout 0.1):

```
current 42  [1.209, 1.176, 1.146, 1.127]
current 43  [1.218, 1.123, 1.145, 1.175]
current 44  [1.19, 1.176, 1.141, 1.143]
step-0 42   [1.207, 1.08, 1.06, 1.039]
step-0 43   [1.236, 1.198, 1.216, 1.223]
step-0 44   [1.189, 1.119, 1.092, 1.048]
```

Without head dropout, 200 steps, CE per 50-step block:

```
current 42  [1.226, 1.209, 1.156, 1.152]
current 44  [1.196, 1.147, 1.113, 1.127]
step-0 42   [1.235, 1.179, 1.142, 1.119]
step-0 43   [1.243, 1.214, 1.132, 1.13]
step-0 44   [1.225, 1.158, 1.1, 1.133]
```

Gradient accuracy on the released model against float64 PyTorch
(`laya_training_reference.py --precision float64` on three ~330-token real
states, no dropout; the Zig parity test pointed at that fixture): worst
per-layer relative L2 error 0.4-0.6% with device slices, 0.7-1.2% without.
**Correction (2026-09-26):** these figures were read from a partial printout
of the mismatches. The worst relative L2 over `encoder.layers.*` weights is
4.0% (CPU) and 4.1% (Metal), largest in the norm weights, on both paths.
The step-1 gradient-norm difference between the two paths is 1e-4 relative
without dropout and 6e-4 with it. A single example repeated six times at
learning rate 1e-30 gives bit-identical gradients every step on both paths,
so no state is corrupted across steps.

Along the way the investigation found a real, unrelated bug: Metal dynamic
linear slots cached a private copy of a weight keyed by its buffer address,
and optimizer-replaced weights reuse addresses. Fixed in 88e2fadd2c with a
regression test (fails without the fix: -1.75 where 6.5 is expected). The
Laya trainer did not take that path; seed 42 is bit-identical before and
after.

## Weight quantization (released `laya`, unpacked, 760 decisions)

```
dense Metal  acc 0.3868 soft_ce 1.30796 footprint 7.94 GB  61 s
q8_0  Metal  acc 0.3842 soft_ce 1.30668 footprint 5.09 GB  67 s
dense CPU    acc 0.3868 soft_ce 1.30796 footprint 2.94 GB  602 s
q8_0  CPU    acc 0.3855 soft_ce 1.30745 footprint 3.47 GB  1587 s (overlapped training)
```

## Packed benchmark, device scoring (Metal, ReleaseFast)

`ANTFLY_LAYA_BACKEND=metal ANTFLY_LAYA_PACKED_BENCH=<laya> zig build test
-Doptimize=ReleaseFast -- --test-filter "laya packed benchmark"`, before
(6511538e46) and after (5583da02ff). Milliseconds, median of five warm requests.

```
state q   tokens unpacked  packed before->after  cached before->after
(1,16)    446    168.0     105.4 -> 103.6        106.0 -> 103.1
(1,64)    1694   513.1     259.6 -> 249.9        259.8 -> 249.3
(4,16)    542    411.3     131.7 -> 128.4        128.2 -> 125.8
(4,64)    1790   1490.2    305.5 -> 293.6        306.7 -> 297.7
(12,1)    408    196.9     129.4 -> 129.2        73.7 -> 74.7
(12,16)   798    1490.4    197.6 -> 193.7        145.9 -> 142.8
(12,64)   2046   5528.6    406.8 -> 399.5        363.1 -> 355.2
```

## Environment notes

Each fine-tune run writes ~8 GB (optimizer checkpoint plus exported model).
About twenty runs filled the disk (3.6 GB free), which failed one checkpoint
write and made memory pressure worse. Run scripts now delete checkpoints after
evaluation. Two trainers running at once nearly exhausted memory; run one at
a time.

## Banking77, candidate mode (2026-09-26)

`scripts/laya/prepare_laya_banking77.sh`, 1,540 train / 385 calibration /
400 eval records (77-way choice). Serving-evaluator results on the 400:

```
released laya, unpacked, upstream code (laya_upstream_baseline.py)  acc 0.3475
candidate fine-tune, rlcd, seed 42      acc 0.015  soft_ce 4.335 (diverged)
candidate fine-tune, soft_ce, seed 42   acc 0.8275 soft_ce 0.8217 ece 0.083  train 1786 s
candidate fine-tune, soft_ce, seed 43   acc 0.8100 soft_ce 0.8505 ece 0.109
```

RLCD CE per 200 steps: 3.612, 3.996, 5.002, 4.37, 4.345, 4.34, 4.346, 4.337
(grad norms 1,255-2,064 early). 300-step diagnostics, CE per 50 steps:
RLCD at a quarter of the learning rate 3.373, 3.124, 3.352, 2.617, 3.748,
2.975 (grad norms 979-3,348); soft CE 3.851, 2.786, 2.392, 2.524, 2.126,
2.291 (grad norms 93-291).

## LoRA (2026-09-26)

Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../zig/pkg/inference/models/laya/LAYA.md#lora).
Implementation: `finetune/laya/graph.zig` (`Lora`/`Targets`, `isLoraWeight`,
`isLoraFrozen`, `loraPrefix`, `loraDelta`), `finetune/laya/training.zig`
(`frozen` widened with an `?architecture.Lora` arg, `parameters` inits
`.lora_A` Kaiming-uniform / `.lora_B` zero), `finetune/laya/job.zig`
(`Config.lora`, `mergeLora`, `exportedValue`).

Verification this session was CPU-only, on the synthetic fixture
(`ANTFLY_LAYA_REFERENCE`), via `zig build test -- --test-filter lora` and
`--test-filter laya`:

```
finetune.laya.graph.test.laya lora adds rank-shaped adapters to every targeted linear and both groups get gradients ... OK
finetune.laya.graph.test.laya lora target and prefix helpers identify exactly the six adapted linears ... OK
finetune.laya.training_test.test.laya lora freezes only its targeted linear weights and biases, on top of frozen layers ... OK
finetune.laya.training_test.test.laya training with lora adapts only its targets, merges them at export, and resumes exactly ... OK
finetune.laya.job.test.laya lora merge computes base plus scale times B times A exactly ... OK
finetune.laya.job.test.laya lora job validation resolves targets and rejects malformed settings ... OK
```

Full `--test-filter laya` with the reference fixture: 64 selected, 56 passed,
8 skipped (CUDA, packed benchmark, and the export-reference test, none of
which this track touches), 0 failed — including the existing 45-gradient
PyTorch parity test and the frozen-layers test, both unaffected by widening
`training.frozen`'s signature.

**Update, later the same session (2026-09-26): Metal and released-model
measurements.** The lock policy changed (`with-lock gpu` no longer also
holds the build slot; a build only waits on a live GPU job when memory is
tight), which let the remaining old-policy chain drain and freed the queue.

Metal: `--test-filter laya` with `ANTFLY_LAYA_METAL=1 ANTFLY_LAYA_BACKEND=metal`
and the reference fixtures: 64 selected, 56 passed, 0 failed, 8 skipped
(CUDA/benchmark/export-reference, unrelated), including the LoRA end-to-end
job test (`"backend":"metal"`) and the 45-tensor PyTorch gradient parity
test (max absolute error 1.9e-6, unchanged from before this track).

Released-model numbers (Apple M4 Max, ReleaseFast, Metal, own worktree
build, `laya-released` checkpoint, jobs under `.tmp/laya/` with `lora:
{"rank":16,"alpha":32,"targets":["encoder","head"]}`):

Step time and peak memory, `td/prof.json`-style job (packed question mode,
batch 1, 14 microbatches), median step after two warm-up steps, `/usr/bin/time -l`:

```
full fine-tune (remeasured)  median 1.31 s  peak 25.88 GB
lora rank16                  median 1.23 s  peak 14.43 GB
```

Raw step_ms, full fine-tune (14 steps): 2373.861, 1342.65, 1409.957,
1249.336, 1027.417, 1373.559, 1254.247, 1497.145, 1014.922, 1389.317,
1021.039, 1252.919, 1511.434, 1372.526. Peak footprint (`/usr/bin/time -l`
"peak memory footprint"): 25882518024 bytes.

Raw step_ms, LoRA rank 16 (14 steps): 1206.345, 1115.658, 1305.337,
1162.286, 921.686, 1295.197, 1166.301, 1420.939, 927.203, 1297.53, 921.02,
1160.47, 1436.552, 1304.365. Peak footprint: 14431817760 bytes.

Step-0 packed accuracy, rank 16, same recipe/data/evaluator as the step-0
and run-to-run-variance sections above (`s0-train.jsonl`, 400 cases;
`s0-calibration.jsonl`; scored on the full 760-decision `s0-eval.jsonl`
through the serving evaluator, `--backend metal`):

```
seed 42: accuracy 0.48026  soft_ce 1.11312  ece 0.03261  (93.1 s eval, 760 decisions)
seed 43: accuracy 0.47763  soft_ce 1.11338  ece 0.02309  (92.1 s eval)
seed 44: accuracy 0.45395  soft_ce 1.11733  ece 0.04169  (94.3 s eval)
mean 0.4706  sd 0.0145 (vs current-trainer full fine-tune mean 0.450 sd 0.014)
```

Rank 64 was not measured (time). `*.safetensors` were deleted from every run
directory immediately after its eval.

## Long states: fused segment training attention (step 2c, 2026-09-25)

Added a fused training-attention op, `fused_segment_training_attention_v1`
(and its hand-written backward), next to the existing
`fused_deberta_training_attention_v1`/`fused_boundary_training_attention_v1`
in `ml/src/graph/node.zig`. It generalizes Laya's global, sliding-window
local, and tree-packed layers under one contract: `ranges` (up to three
ancestor-segment extents per query, `laya_tree` style) plus a graph-time
`window`, instead of a materialized `[batch*heads, S, S]` additive bias.

Kernel: `lib/linalg/src/attention.zig`
(`segmentTrainingAttentionForwardHost`/`BackwardHost`), beside
`segmentAttentionHost`. Forward tiles `BLOCK_Q`x`BLOCK_KV` with an online
softmax and saves only per-query `(row_max, row_sum)`. Backward recomputes
the forward (for `O` and the delta term) and makes one more tiled sweep that
recomputes `P` per tile for `dQ`/`dK`/`dV` -- no `[tokens, tokens]` tensor in
either sweep. Dropout is a seeded counter mix keyed by `(batch, head, query,
key)`, replayed identically in both sweeps.

Wired end to end: `node.zig` (attrs/opcodes) -> `builder.zig`
(`segmentTrainingAttentionV1`) -> `autodiff.zig` (VJP emits the backward op
and lets the existing `concat` VJP split the packed gradient into dQ/dK/dV)
-> `ops/segment_training_attention.zig` (control decode, admission) ->
`ops/native_compute.zig` (CPU dispatch) -> `graph/interpreter.zig`. CPU only:
the `ComputeBackend` vtable slots are unset on Metal, so
`finetune/laya/job.zig` only sets `use_fused_attention` when
`backend == .cpu`; a Metal job keeps the original dense-bias graph and its
`batch*L^2*heads <= 64M` bound unchanged. `finetune/laya/graph.zig` gained
`buildWithAttention`/`fusedAttention`, and `training.zig`'s `inputs` now
builds the one `segment_control` runtime tensor (from `Example.position`/
`packed_row` via `laya_tree.ranges`) instead of the three dense bias tensors
when fused attention is selected. `graph.validate` drops the quadratic
admission bound in that case and checks `seq_len <= 8192` instead
(ModernBERT's pretraining length, already `laya.max_len`'s ceiling).

Tests added: `lib/linalg/src/attention.zig` (forward vs. dense masked
softmax for global/local-window/tree-segment ranges; backward vs. finite
differences, with and without dropout), `ops/segment_training_attention.zig`
(admission, control decoding), `ml/src/graph/segment_training_attention_test.zig`
(VJP wiring, mirroring `deberta_training_attention_test.zig`), and
`finetune/laya/fused_attention_test.zig` (the real training graph, through
`training.inputs` and the CPU backend, comparing the fused and dense builds'
logits on an unpacked example and a synthetic tree-packed row with identical
random weights). `finetune/laya/job.zig`'s admission tests gained a case
showing the fused path admits a batch the dense bound rejects.

## Metal: host-bridged execution (2026-09-25, same day)

Added `MetalCompute.segmentTrainingAttentionV1Op`/`BackwardV1Op`
(`ops/metal_compute.zig`), wired into the Metal `ComputeBackend` vtable.
These do not add an on-device kernel; they reuse the `HostFallbackNative`
bridge that `hostFallbackSdpa` and `hostFallbackDisentangledRelativeAttention`
already use for their own device-kernel-missing cases: download `qkv`/
`control`/`dOut` to a `NativeCompute` instance, run the exact same CPU
kernel, upload the result back. `MetalTensor.toHostSlice` already flushes
any active command frame before reading (`metal_tensor.zig`:
"a runtime frame may still be queuing GPU writes to this buffer... The
flush is a cheap no-op when no frame is active"), so this is safe inside
`training.executeFramed`'s framed forward/backward without further
changes -- no separate unframed-training carve-out was needed.
`finetune/laya/job.zig` originally set `use_fused_attention = true`
unconditionally (both backends); a Metal job that took the fused path
this way runs attention host-bridged, adding a download/upload round trip
to *every* attention call in the main trainer even when the layout was
small enough for the on-device dense path. Changed: `job.zig` now selects
fused attention only when a split's layout actually exceeds the dense
path's own `batch*L^2*heads` admission bound
(`exceedsDenseAttentionBound`, re-checking `architecture.validate` with
`use_fused_attention=false`) or `Config.force_fused_attention` asks for it
explicitly. Ordinary Metal jobs (anything that fit before this track) keep
running the on-device dense path unchanged; only long-state jobs beyond
the dense bound (the actual point of step 2c) pay the host-bridge cost.
Metal jobs that do take it get the same `seq_len <= 8192` admission bound
as CPU, without GPU parallelism for this op.

Added a Metal variant of the fused-vs-dense parity test
(`fused_attention_test.zig`, `SkipZigTest` without a Metal device) and a
fused-attention variant of the released-model gradient-parity test
(`training_test.zig`, gated by `ANTFLY_LAYA_REFERENCE`) that reuses the
relfix/ref oracle but with a looser tolerance (2e-3 absolute / 2% relative
vs. the dense path's 5e-5 / 0.2%) since the tiled kernel sums in a
different order than the dense generic matmul/softmax path.

A true on-device Metal kernel remains open. Sketched design: a forward
kernel extending `termite_sdpa_f32_segments` with a batch dimension,
dropout and saved `(row_max, row_sum)`, plus a backward that avoids
cross-threadgroup atomics by computing `dQ` query-major (reusing the saved
stats and an in-kernel `delta_i = dot(dOut_i, O_i)`) and `dK`/`dV` key-major
via a *range-symmetry* argument: for unpacked rows every token's `ranges`
entry is the same `[0, seq_len)` (or window) on both the query and key
side, so a key's threadgroup can reuse `ranges[key]` to find the queries
that see it. That symmetry does not hold for tree-packed rows (a trunk key
is visible from many branches, but the trunk's own range does not list
them), so this design does not extend to Metal training on packed rows
without either atomics or a real reverse-range structure.

## Open items and verification status

No CUDA kernel.

## Verification (2026-09-26, after the build lock's gpu-holds-build policy was fixed)

`zig build test-linalg -- --test-filter segmentTraining`: passes.
`zig build test -- --test-filter laya` on CPU, then Metal
(`ANTFLY_LAYA_METAL=1 ANTFLY_LAYA_BACKEND=metal`), both with
`ANTFLY_LAYA_REFERENCE` pointed at `ref`: both pass clean (63 selected, 55
passed, 8 skipped, 0 failed, 0 leaked on CPU; equivalent on Metal), after
fixing what the first real build found:

- A `///` doc comment directly before a `test` block is a compile error in
  this Zig version; changed to `//` (`training_test.zig`).
- `segment_training_attention.zig`: `result` in `forward()` is read-only,
  not mutated -- `const`, not `var`.
- `fused_attention_test.zig`: `ComputeBackend.fromFloat32Shape` returns
  `!CT` directly, not `!?CT` (dropped a stray `orelse`); the synthetic
  tree-packed row needs two valid option markers per question
  (`laya_tree.validate` requires at least two) and token ids under the
  test config's `vocab_size`; the shared test config needs `packing`
  enabled so a packed row's `l.questions` passes `graph.validate`;
  `training.inputs` is written for a scratch/arena allocator (its real
  callers, `training.step`/`predict`, pass one) and does not free its own
  host scratch arrays itself, so under `std.testing.allocator` directly it
  leaks -- gave it an arena, and freed `bindRandomWeights`'s returned
  slice too.

Relfix (`ANTFLY_LAYA_REFERENCE` on `relfix`, `--test-filter "every
parameter gradient match"`) does not cleanly pass its own hard gate for
**either** attention path on this fixture -- see LAYA.md's "Relfix on the
current fixture" for the full numbers and reasoning; short version: dense
(byte-for-byte unchanged code) fails the same gate with the same tolerance,
so this predates the track. Reading the diagnostic per-tensor
`relative_l2` values instead: CPU dense 52 mismatches / 4.0% worst, CPU
fused 6 mismatches / 4.3% worst; Metal dense 42/2.3%, Metal fused 4/4.1%.
Fused is not a regression relative to dense on this fixture (fewer
mismatches, comparable worst tensor), but neither is within the
previously-recorded 0.4-0.6% band; that band likely needs
re-establishing against a freshly regenerated fixture independent of this
track.

`training_packed_test.zig`'s "converts an unpacked checkpoint into a
served packed model" test compares the *trainer's own* eval predictions
(the training graph at its final weights) against *serving the exported
model* -- the same weights on both sides, so a gap here is a bug, not
training drift. It went from 6.0e-8 to 2.9e-3 max probability error once
`job.zig` defaulted to fused attention unconditionally.

First hypothesis (wrong): tile-order reordering noise compounding through
training into real weight drift. Ruled out because this test compares one
set of weights against itself through two forward paths, not two different
trained models -- drift between separately-trained models can't explain a
same-weights comparison.

Actual root cause: the dense path's attention dropout is a *runtime* mask
(`graph.zig`'s `drop()`, bound to all-ones by `training.inputs` when
`training=false`, i.e. at `predict`/eval), but the fused op's
`dropout_probability` is a *graph-time* attribute baked into
`SegmentTrainingAttentionAttrs` when `job.zig`'s `Cache` builds its
`Program` -- and that `Program` is reused, unrebuilt, for every training
step and every `predict` call. With no runtime toggle, the fused op kept
applying its seeded in-kernel dropout during eval (`head_dropout` defaults
to 0.1), so the trainer's own eval differed from a dropout-free serving
forward by roughly that scale -- corrupting eval predictions and
calibration on every real long-state job, not just this test.

Fix: `control`'s layout gained a runtime `apply_dropout` word (index 6,
`SegmentTrainingAttentionAttrs.layout`'s `control_elements` now `7 +
batch_tokens + ranges`, up from `6 + ...`), set from the same `training:
bool` `training.inputs` already threads to the dense path's mask;
`segment_training_attention.zig`'s `forward`/`backward` substitute `0` for
`attrs.dropout_probability` when it's unset. Metal inherits this for free
(the host bridge forwards the same control tensor to the same CPU decode).
`training_packed_test.zig` gained a second case, "(forced fused
attention)", running this exact job with `force_fused_attention=true`;
both it and the default case now measure ~3e-8, and the test's original
`worst < 5e-5` bound needed no widening.

Separately, and *not* related to the bug above (it compares two
differently-trained models, not one model against itself): diffing
checkpoints from training the same fixture/config twice
(`force_fused_attention=false` vs `=true`) gives a worst relative L2 over
`encoder.layers.*` weight tensors of 2.43% on CPU (dropout-corrected).
Both runs are individually correct; the tiled online-softmax's summation
order differs from the dense masked softmax's, and on this tiny
six-example, three-epoch fixture that ~1e-7-per-call difference compounds
through Adam into measurable (if small) weight drift. This is a
characterized, expected property of switching attention implementations on
a job that could equally have trained with either, not something that
needs hiding -- long-state jobs beyond the dense bound have no dense
baseline to compare against in the first place.

`job.zig` also now selects fused attention only when a split's layout
actually exceeds the dense path's own admission bound (or
`Config.force_fused_attention` asks for it explicitly) --
`exceedsDenseAttentionBound` re-checks `architecture.validate` with
`use_fused_attention=false` first. This `ref` fixture (`seq_len=128`,
`batch=1`) never needed fused attention on its own, so by default it now
trains and evaluates on the dense path exactly as before this track.

## Long-state smoke test (2026-09-26)

Recipe in `.tmp/longstate/` (gitignored): 2k/4k/8k-token synthetic states
built by concatenating distinct `td/train.jsonl` records (word-count
estimate for the target length, actual tokenization unverified),
`laya.max_len` raised to 8192 in a copy of the released checkpoint's
`config.json`/`rl_agent_config.json`, job JSON per size (CPU backend,
`stop_after_microbatches: 3`, reduced from an initial attempt at 10 that
demonstrated the same per-step cost more slowly), release binary via
`zig build -Doptimize=ReleaseFast --prefix .tmp/longstate/rel`, then
`with-lock gpu -- /usr/bin/time -l .tmp/longstate/rel/bin/antfly-inference
finetune train laya job_2k.json`.

Result: the 2k run ran 748 s wall, reaching 15.3 GB peak memory footprint
(21.6 GB max RSS), then failed with `error.OutOfMemory` on an ordinary MLP
`dot_general` (`[6656, 5248]`) before completing one full step (no
`"step"` event logged). `vm_stat` at the time showed under 70 MB free
system-wide -- this session's nine concurrent agent tracks, each running
released-model work, had exhausted the shared 36 GB machine's memory
independent of this run. This is not an attention-specific bug (the
failing op is a dense linear layer, not `fused_segment_training_attention`),
but it means no clean per-step timing was captured, and 4k/8k were not
attempted: they need strictly more memory than a 2k run that already used
15-22 GB with near-zero system headroom, and stopping near 20 GB was the
explicit guidance. The recipe and reduced-step job configs are ready to
rerun once the machine has headroom; `vm_stat` is worth checking
immediately before doing so.

## Packed vs unpacked at equal budget (2026-09-26)

Current trainer, RLCD, 1 epoch, batch 1, unpacked with gradient accumulation
5. Serving-evaluator accuracy / soft CE / ECE.

```
step-0 (400 cases, 760 eval)   packed    42 0.434  43 0.461  44 0.455   mean 0.450 sd 0.014  soft_ce 1.129 ece 0.049
step-0 (400 cases, 760 eval)   unpacked  42 0.599  43 0.628  44 0.637   mean 0.621 sd 0.020  soft_ce 0.983 ece 0.088
larger (915 cases, 1840 eval)  packed    42 0.471  soft_ce 1.097
larger (915 cases, 1840 eval)  unpacked  42 0.671  soft_ce 0.946 ece 0.105
unpacked train time 1426-1750 s, packed larger 1194 s
```

## Distilling packed question mode from an unpacked teacher (2026-09-26)

```
teacher: unpacked fine-tune, s0-train, seed 42, gradient_accumulation 5   acc 0.599 soft_ce 0.988 ece 0.065  1393 s
labels:  prepare_laya_packed_distillation.py --gold-weight 0.5, 2000/2000 records distilled
students (packed question, RLCD, 1 epoch, s0-train-distilled):
  seed 42 acc 0.464   seed 43 acc 0.433   seed 44 acc 0.455   mean 0.451  soft_ce 1.135  ece 0.033
  per type (42/43/44): choice 0.368/0.364/0.360  score 0.424/0.362/0.408  noul 0.614/0.596/0.614
reference: packed on gold 0.450 (0.434/0.461/0.455); unpacked on gold 0.621 (0.599/0.628/0.637)
```

## Question-aware trunk (2026-09-26)

`"packing":"question","trunk_sees_questions":true`, step-0 recipe, RLCD, 1 epoch, Metal:

```
seed 42 acc 0.516 train 709 s   seed 43 acc 0.579 train 545 s   seed 44 acc 0.566 train 569 s
mean 0.554 sd 0.033 soft_ce 1.031 ece 0.060   peak footprint 26.0 GB each; device estimate 31527 MiB
```

## Teacher throughput (2026-09-26)

M4 Max 36 GB, MLX, `scripts/laya/benchmark_laya_teacher.py`.

- Qwen3-14B-4bit, 12 cases / 60 decisions of `td/s0-train.jsonl`:
  `{"per_question_s_per_decision": 0.957, "per_question_tokens": 17807, "shared_s_per_decision": 0.518, "shared_tokens": 7915, "speedup": 1.85, "mean_shared_prefix_tokens": 206, "prefill_tok_per_s": 310, "max_prob_diff": 0.0622, "split_max_prob_diff": 0.0622, "fork_vs_split_max_prob_diff": 0.0, "argmax_agree": "59/60", "prefill_tok_per_s_by_batch_len256": {"1": 325, "4": 283, "8": 271, "16": 322}}`
  Two earlier runs: speedup 1.51 and ~1.9.
- Qwen3-4B-4bit, same: per-question 0.325 s/decision, shared 0.197, 912 tok/s, batching flat (885-941).
- `td/s0-eval.jsonl`, 760 decisions, shared prefill: 4B 0.167 s/decision, accuracy 0.514, soft CE 1.117 (T 18.2);
  14B 0.534 s/decision, accuracy 0.645, soft CE 1.032 (T 14.6); uniform soft CE 1.212; argmax agreement 0.672.
- Cascade 4B → 14B by 4B calibrated top probability: threshold 0.5 escalates 27%, accuracy 0.582; 0.6: 53%, 0.617;
  0.7: 70%, 0.630; 0.8: 90%, 0.637; 0.9: 99%, 0.645.
- `prepare_laya_longcontext_teacher.py --score-all` on `td/s0-eval.jsonl` (760 decisions, no calibration):
  `--prefill shared` 0.650 s/decision, 109,893 tokens, accuracy 0.6461, soft CE 5.755, ECE 0.321;
  `--prefill per-question` 1.304 s/decision, 266,593 tokens, accuracy 0.6447, soft CE 5.759, ECE 0.318.

## Open-Jev scale run (2026-09-27)

`td/oj-mix.jsonl` = `openjev-train.jsonl` (64,450; converter drops: excluded source 4,206, state does not fit 10,460)
+ `s0-train.jsonl` (2,000). Config `td/oj42.json`: rlcd, seed 42, epochs 1, batch 1, `max_packed_len` 704.
Probe estimates: 2,048 → 137,698 MiB; 1,024 → 47,846; 768 → 34,343; 704 → 31,527; 640 → 28,936.
Train 23,897 s, 20,990 steps, peak footprint 27.4 GB.
`s0-eval`: overall 0.375, choice 0.329, score 0.273, noul 0.557, soft CE 1.270, ECE 0.062.
Mean CE per 2,000 steps: 1.492 1.283 1.408 1.601 1.652 1.852 1.816 1.502 1.596 1.454 1.522 (uniform 1.078).
Grad norm p50/p95 per 2,000 steps: 31.5/498, 20.5/871, 18.1/541, 17.0/569, 12.3/545, 15.3/758, 14.0/385,
13.0/666, 13.2/1419, 13.7/3114, 17.9/4419. Weights deleted after eval before an Open-Jev eval could run.
