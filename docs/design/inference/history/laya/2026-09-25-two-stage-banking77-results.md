# Laya two-stage choice: Banking77 results

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 1388–1446 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

**Measured on Banking77** (400 eval messages, `packing.two_stage.top_k = 8`,
starting from the same released checkpoint and recipe as the candidate
baseline: one epoch, soft CE, `b77/train.jsonl` plus its synthetic stage-2
rows, calibration on `b77/calibration.jsonl`):

| Model | Accuracy | Soft CE | ECE | Top-8 recall | ms/decision | Tokens/decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Candidate-only baseline (prior work, mean of 2 seeds) | 0.819 | — | — | — | — | — |
| Two-stage checkpoint, seed 42, **stage 1 alone** (`two_stage` unset) | 0.8525 | 0.632 | 0.057 | 0.995 | 403 | 369 |
| **Two-stage checkpoint, seed 42, stage 1 + stage 2** | 0.8325 | 0.686 | 0.067 | — | 633 | 435 |
| Two-stage checkpoint, seed 43, stage 1 alone | 0.8425 | 0.640 | 0.063 | 0.985 | 374 | 369 |
| **Two-stage checkpoint, seed 43, stage 1 + stage 2** | 0.8375 | 0.785 | 0.071 | — | 625 | 435 |
| Mean, stage 1 alone (2 seeds) | 0.8475 | 0.636 | 0.060 | 0.990 | 389 | 369 |
| **Mean, stage 1 + stage 2 (2 seeds)** | **0.8350** | **0.735** | **0.069** | — | 629 | 435 |

- **Stage 2 makes accuracy worse, not better, on this recipe, and it does so
  on both seeds.** Same checkpoint, same eval set, only the decoding path
  differs: stage 1 alone scores 0.8525 / 0.8425 (seeds 42 / 43); blending in
  stage 2 drops both to 0.8325 / 0.8375, a consistent ~1.25-point mean loss,
  and soft CE and ECE get worse on both seeds too. This is the two-stage
  design's headline result and it is negative.
- **It is not a shortlist-recall problem.** Stage 1's top-8 shortlist
  contains the gold label 99.5% (seed 42) / 98.5% (seed 43) of the time — the
  99.9th-percentile case for a bound, not the bottleneck. Stage 2's own
  joint-branch distribution must be miscalibrated or simply wrong often
  enough, on the finalists it does see, to outweigh the cases where it
  corrects stage 1.
- **Also worth noting: stage 1 alone, on this checkpoint, beats the original
  candidate-only baseline** (0.8475 mean vs 0.819 mean). The two-stage
  training recipe trains on the candidate rows *and* a same-size batch of
  synthetic joint rows, which amounts to roughly double the gradient steps
  over related data; this is not a controlled ablation of stage 2's value,
  only a by-product of the extra training the mixed recipe happens to do. It
  means comparing the full two-stage number against the old baseline (0.835
  vs 0.819) would *understate* how much stage 2 alone costs; the fair,
  controlled comparison is stage 1 alone vs. stage 1 + stage 2 on the same
  weights (0.8475 vs 0.8350), which is unambiguous and still negative.
- **Cost.** Stage 2 adds one joint-branch forward per `choice` decision over
  `top_k` options (all 400 Banking77 eval messages have 77 > 8 options, so
  every decision pays it): +66 tokens per decision exactly (369 → 435,
  reproducible across seeds and runs) and roughly +240 ms per decision on
  this shared, loaded machine (389 → 629 ms mean; per-run wall time ranged
  374–693 ms depending on contention from other agents' jobs, so treat the ms
  figures as indicative only, unlike the token counts).
- **Hypotheses for the regression, untested here:** (a) the joint branch is
  trained on far fewer effective examples per class than the candidate branch
  (one sampled shortlist per record vs. all 77 options), so it may simply be
  undertrained; (b) uniform random negatives are an easy shortlist most of
  the time (top-8 recall is 99.5%), so stage 2 rarely has to do real
  discrimination work in training and may not have learned to firmly prefer
  the right answer among *hard* look-alikes; (c) the calibration gap is
  suspect too — the trainer fits one temperature for the whole `choice` kind
  (`job.zig`'s `calibrate`), mixing 77-way stage-1 rows and 8-way stage-2
  rows in one calibration set, so neither branch's temperature is fit for
  its own distribution shape. (c) is the cheapest to try next: fit
  `temperature_by_options` per count bucket instead of one flat value.
- **Not yet tried:** hard-negative shortlists (from a stage-1 checkpoint's
  own confusions) instead of random ones, a larger `top_k`, and a
  `mass_cutoff` shortlist instead of a fixed size.
