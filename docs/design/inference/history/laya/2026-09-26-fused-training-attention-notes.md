# Laya fused training attention: fixture, eval-dropout and long-state notes (2026-09-26)

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 502–603 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

**Relfix on the current fixture.** `training_test.zig`'s "every parameter
gradient match PyTorch" test's own hard gate (zero per-tensor mismatches
against a `5e-5 + 0.2%*magnitude` bound) does not pass against the current
`relfix` fixture for **either** attention path -- dense fails it too, with
the identical unmodified tolerance, so this is a pre-existing fixture/dense
gap, not something this track introduced. Reading the printed worst
per-tensor `relative_l2` values instead (LAYA.md's original 0.4-0.6%
number is this same metric):

| Backend | Dense: mismatches / worst relative_l2 | Fused: mismatches / worst relative_l2 |
| --- | --- | --- |
| CPU | 52 / 4.0% | 6 / 4.3% |
| Metal | 42 / 2.3% | 4 / 4.1% |

Fused attention has *fewer* tensors over the strict per-tensor threshold
than dense in both backends, but its single worst tensor is somewhat higher
(4.1-4.3% vs 2.3-4.0%). A dense-only re-measurement on the merged branch
(2026-09-26) gives the same picture: worst over `encoder.layers.*` weights
4.0% (CPU) and 4.1% (Metal), concentrated in the norm weights. The fixture
has not drifted: an earlier "0.4-0.6%" figure in this document was misread
from a partial printout of the mismatches and is withdrawn. The per-tensor
gate in this test is tuned for the tiny synthetic fixture and does not fit
the 28-layer model; use the worst relative L2 over `encoder.layers.*` as the
statistic instead.

**Packed job training-vs-serving, and the eval dropout leak.**
`training_packed_test.zig`'s "converts an unpacked checkpoint into a served
packed model" test compares the *trainer's own* `eval_predictions.json`
(the training graph, evaluated at the run's final weights) against
*serving the exported model* (a real `pipeline.execute` session) -- the
same weights on both sides, so any gap here is a bug, not training drift.
Forcing fused attention unconditionally (the first cut of this track) rose
this from 6.0e-8 to 2.9e-3 max probability error on the 2-layer hidden-64
`ref` fixture (`head_dropout` defaults to 0.1).

Root cause: the dense path's attention dropout is a *runtime* mask
(`graph.zig`'s `drop()` binds an external `__laya_dropout_N` parameter that
`training.inputs` fills with all-ones when `training=false`, i.e. at
`predict`/eval), but the fused op's `dropout_probability` is a *graph-time*
attribute baked into `SegmentTrainingAttentionAttrs` when the `Program` is
built -- and `job.zig`'s `Cache` builds that `Program` once and reuses it
for every training step and every `predict` call. With no runtime toggle,
the fused op kept applying its seeded in-kernel dropout during eval, so the
trainer's own eval of its final weights differed from a dropout-free
serving forward by roughly `head_dropout`'s scale -- and would have
corrupted eval predictions and calibration on every real long-state job,
not just this test.

Fix: `control`'s layout gained one word, `apply_dropout` (word index 6,
before the positions/ranges that follow it -- see the "Op contract" bullet
above and `ops.segment_training_attention.ControlView`).
`finetune/laya/training.zig`'s `inputs` sets it from the same `training:
bool` parameter the dense path already uses for its all-ones mask;
`segment_training_attention.zig`'s `forward`/`backward` pass `0` instead of
`attrs.dropout_probability` into the kernel whenever it is unset. Metal
inherits this automatically: with the flag off the call takes the device
kernels, which have no dropout, and with it on the host bridge forwards the
same control tensor to the same CPU decode. Verified: `training_packed_test.zig` now has
a second case, "... (forced fused attention)", that runs this exact job
with `force_fused_attention=true` and asserts the original `worst < 5e-5`
bound; both it and the default (dense, admission-gated) case measure
~3e-8.

Separately (and *not* the explanation for the gap above, since it compares
different weights, not the same ones): training the identical fixture/config
twice, once with `force_fused_attention=false` and once `=true`, and
diffing the two exported checkpoints, shows a worst relative L2 over
`encoder.layers.*` weight tensors of 2.43% on CPU (dropout-corrected). Both
runs are individually correct -- the tiled online-softmax's summation order
differs from the dense masked softmax's, and on this tiny six-example,
three-epoch fixture that ~1e-7-per-call difference compounds through Adam
into measurable (if small) weight drift. This has no dense baseline to
compare against once a job's layout actually exceeds the dense bound (the
dense path cannot run at that length at all), so it is noted here as a
characterized, expected property of switching attention implementations,
not something the admission gate needs to hide.

**Long-state smoke test (2026-09-26, CPU, released 421M checkpoint).** Data
and job recipe in `.tmp/longstate/` (gitignored): states synthesized by
concatenating distinct `td/train.jsonl` records to ~2k/4k/8k tokens
(word-count estimate; actual tokenization untested), `laya.max_len` raised
to 8192 in a copy of the released checkpoint's `config.json` and
`rl_agent_config.json`, `zig build -Doptimize=fast --prefix <dir>`
once, then `/usr/bin/time -l <dir>/bin/antfly-inference finetune train laya
job_2k.json` under the gpu lock.

The 2k run reached 15.3 GB peak memory footprint (21.6 GB max RSS) over
748 s wall before `error.OutOfMemory` on a `dot_general` (a `[6656, 5248]`
MLP intermediate), before finishing even one full step or logging a
`"step"` event -- not a bug in the fused op specifically (nothing here is
attention-shaped; it is the encoder's ordinary MLP matmul), but this
machine's system-wide memory was critically low at the time (`vm_stat`:
under 70 MB free, all nine agents' tracks running full released-model
work concurrently). 4k and 8k were not attempted: they need strictly more
memory than a 2k run that already used 15-22 GB on a shared 36 GB
machine with near-zero headroom, and the task's own guidance is to stop
near 20 GB. Rerun `.tmp/longstate/job_{2k,4k,8k}.json` (currently
`stop_after_microbatches: 3`, reduced from a first attempt at 10 that made
the same throughput point more slowly) once the machine has real headroom,
and watch `vm_stat`/`/usr/bin/time -l` peak footprint, not just this
session's numbers.
