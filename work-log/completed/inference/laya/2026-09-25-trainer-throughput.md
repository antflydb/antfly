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
```

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
