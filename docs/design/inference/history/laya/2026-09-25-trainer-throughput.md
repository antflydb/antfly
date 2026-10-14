# Laya trainer throughput and state-cache memory: 2026-09-25

> Summary of a verbatim log. Full text: [docs/design/inference/history/laya/2026-09-25-trainer-throughput.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/laya/2026-09-25-trainer-throughput.md) (per-step losses, raw step times, full per-run tables). Design and summary: [`LAYA.md`](../../../../../zig/pkg/inference/models/laya/LAYA.md) ([Trainer throughput](../../../../../zig/pkg/inference/models/laya/LAYA.md#trainer-throughput), [State cache](../../../../../zig/pkg/inference/models/laya/LAYA.md#state-cache), [LoRA](../../../../../zig/pkg/inference/models/laya/LAYA.md#lora)); per-change tables in [2026-09-25-trainer-measurements.md](2026-09-25-trainer-measurements.md).

Work log for the Laya fine-tuning trainer, 2026-09-25 to 2026-09-27, on an Apple M4 Max, 36 GiB, macOS 15 (Darwin 24.6.0), Zig 0.16.0, ReleaseFast. Profiling job: `antfly-inference finetune train laya .tmp/laya/td/prof.json` (packed question mode, batch size 1, 14 microbatches of the step-0 train subset, released checkpoint `c5d78730f3493e4fe16d61507ef4b78eef7318cf`). Covers framed-trainer throughput, an in-frame buffer reuse bug, f16 state cache, seed variance, quantization, LoRA, fused segment training attention for long states, and accuracy experiments (packed vs unpacked, distillation, layout variants, Open-Jev scale).

## Decisions
- **Framed training is the default**; one command batch per optimizer transaction, runtime inputs uploaded once, zero-copy gradient hand-off. Losses identical to the unframed trainer at every step.
- **Frame reuse pool holds only `MTLStorageModePrivate` buffers** (`termite_metal_decode_runtime_release_buffer`) — fixes `NonFiniteTrainingUpdate` in framed training (see Defects). Afterwards framed training needs no op flushes.
- **Fused segment training attention (step 2c)** `fused_segment_training_attention_v1` with hand-written backward: tiled online softmax, saves only `(row_max, row_sum)`, no `[tokens, tokens]` tensor; dropout is a seeded counter mix replayed in both sweeps. Admission becomes `seq_len <= 8192` instead of the quadratic dense bound. Metal runs it host-bridged via `HostFallbackNative` (no on-device kernel).
- **`job.zig` selects fused attention only when a split exceeds the dense path's `batch*L^2*heads` admission bound** (`exceedsDenseAttentionBound`) or `Config.force_fused_attention` — selecting it unconditionally made ordinary Metal jobs pay a host round trip per attention call.
- **Fused op dropout is a runtime toggle**: `control` gained an `apply_dropout` word (index 6; `control_elements` now `7 + batch_tokens + ranges`), because a graph-time dropout attribute in a reused `Program` kept applying dropout during eval.
- Run one trainer at a time and delete checkpoints after evaluation (each run writes ~8 GB; about twenty runs filled the disk).

## Rejected approaches
- **Frame flush after `neg`** (2.65 s/step) — workaround before the pool fix.
- **RLCD on Banking77 candidate mode** — diverged (acc 0.015, soft CE 4.335); soft CE objective reached 0.8275 (seed 42) / 0.8100 (seed 43).
- **Distilling packed question mode from an unpacked teacher** (`--gold-weight 0.5`) — students mean 0.451 vs packed-on-gold 0.450; no gain.
- **Pointer decision head** (`sptr16d`) — s0-eval 0.334, Open-Jev val 0.546, below packed. Earlier pointer runs discarded (unnormalized inputs; resident Metal gather of a LayerNorm output returned row 0).
- **Per-question upper layers** (`fuse_layers` 10 / 30) and **question-first** — s0-eval 0.457 / 0.476 / 0.476, Open-Jev val 0.601 / 0.605 / 0.621, none closing the gap to unpacked.
- **First relfix hypothesis for the 2.9e-3 serving gap** (tile-order noise compounding into weight drift) — ruled out because the test compares one set of weights against itself.

## Key measurements

Trainer median step on the profiling job:

| Configuration | Median step |
|---|---:|
| step-0 trainer | 8.55 s |
| device strided slices | 6.43 s |
| unframed (`ANTFLY_LAYA_TRAIN_UNFRAMED=1`) | 5.34 s |
| framed | 2.02 s |
| + one command batch per optimizer transaction | 1.68 s |
| + runtime inputs uploaded once, zero-copy gradient hand-off | 1.38 s |
| framed, `freeze_layers` 11 / 18 | 1.41 s / 1.17 s |
| full fine-tune (remeasured, 2026-09-26), peak 25.88 GB | 1.31 s |
| LoRA rank 16 (`targets` encoder+head), peak 14.43 GB | 1.23 s |

- Profile at 2.02 s: optimizer ~42% (each op submitted and waited alone), backward ~33%, forward ~15%. At 1.68 s, host encoding was ~95% `add`/`multiply` re-uploading runtime inputs through fresh staging buffers. At 1.38 s: GPU forward ~0.13 s, backward ~0.34 s, optimizer ~0.21 s.
- **f16 state cache**: max error 5.46e-4 (question) / 5.32e-4 (candidate) vs 1.67e-6 for f32, at half the bytes (23040 vs 46080).
- **Weight quantization** (released, unpacked, 760 decisions): dense Metal acc 0.3868, 7.94 GB; q8_0 Metal 0.3842, 5.09 GB; dense CPU 0.3868, 2.94 GB; q8_0 CPU 0.3855, 3.47 GB.
- **Seed variance** (step-0 recipe, 760 decisions, seeds 42/43/44): step-0 host trainer 0.5737 / 0.3711 / 0.5566; current 0.4342 / 0.4605 / 0.4553. LoRA rank 16 mean 0.4706 sd 0.0145 (vs current full fine-tune mean 0.450 sd 0.014).
- **Packed vs unpacked at equal budget** (2026-09-26): step-0 packed mean 0.450 sd 0.014 vs unpacked mean 0.621 sd 0.020; larger set (915 cases) packed 0.471 vs unpacked 0.671.
- **Question-aware trunk** (`trunk_sees_questions`): mean 0.554 sd 0.033, peak 26.0 GB.
- **Open-Jev scale (2026-09-27)**, 64,450 Open-Jev + 2,000 s0-train records, `max_packed_len` 704: RLCD s0-eval 0.375; soft CE s0-eval 0.464, Open-Jev val 0.664 (label-prior baseline 0.600). Layout control on 16,009 records: packed s0-eval 0.463 / Open-Jev val 0.607 vs unpacked 0.607 / 0.701.
- **Teacher throughput** (MLX): Qwen3-14B-4bit shared-prefix prefill speedup 1.85 (earlier runs 1.51, ~1.9); on s0-eval 4B accuracy 0.514 at 0.167 s/decision, 14B 0.645 at 0.534 s/decision. 4B → 14B cascade at threshold 0.7 escalates 70% for 0.630.
- Packed benchmark device scoring (6511538e46 → 5583da02ff), Metal (12,64): packed 406.8 → 399.5 ms, cached 363.1 → 355.2 ms.

## Defects found
- **In-frame buffer reuse**: the pool handed a shared buffer released earlier in the frame to a private request; the upload was an immediate `memcpy` and the previous owner's queued GPU write ran afterwards at submit, corrupting a `scatter_add` `order` index array. Regression test: "metal in-frame buffer reuse never hands a host-writable buffer to a private request".
- **Metal dynamic linear slots cached a weight copy keyed by buffer address**, and optimizer-replaced weights reuse addresses. Fixed in 88e2fadd2c (the Laya trainer did not take that path).
- **Fused-attention eval dropout** (above): trainer-vs-serving max probability error went from 6.0e-8 to 2.9e-3 when fused attention became the default; ~3e-8 after the fix.
- **Gradient accuracy correction (2026-09-26)**: the earlier 0.4–0.6% / 0.7–1.2% per-layer relative L2 figures came from a partial printout; the worst relative L2 over `encoder.layers.*` is 4.0% (CPU) and 4.1% (Metal). Relfix diagnostic: CPU dense 52 mismatches / 4.0% worst, CPU fused 6 / 4.3%; Metal dense 42 / 2.3%, Metal fused 4 / 4.1% — fused is not a regression, and the dense failure predates the track.
- Fused vs dense training of the same fixture gives 2.43% worst relative L2 weight drift on CPU — expected from summation-order differences compounding through Adam.

## Open items (as recorded in the log)
- No CUDA kernel for fused segment training attention; a true on-device Metal kernel remains open. The sketched range-symmetry backward does not extend to tree-packed rows without atomics or a reverse-range structure.
- Long-state smoke (2k tokens, CPU) hit `error.OutOfMemory` after 748 s on a dense MLP `dot_general` while the shared machine was exhausted by concurrent agents; no clean per-step timing, 4k/8k not attempted. Recipe in `.tmp/longstate/` is ready to rerun.
- The 0.4–0.6% gradient band likely needs re-establishing against a freshly regenerated fixture. LoRA rank 64 was not measured.
