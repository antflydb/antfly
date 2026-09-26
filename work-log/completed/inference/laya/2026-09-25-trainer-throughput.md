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
