# Laya candidate mode on Banking77 (step 0b): 2026-09-26

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 1282–1343 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

### Candidate mode on Banking77 (step 0b)

Measured 2026-09-26. `scripts/laya/prepare_laya_banking77.sh` downloads
Banking77 (PolyAI, pinned commit) and writes one 77-way `choice` question per
message: 20 train and 5 calibration examples per intent (1,540 and 385), and
400 eval messages from the test split, all disjoint by normalized text.
Antfly's unpacked pipeline caps choice questions at 20 options, so only a
candidate-packed model can serve these. The unpacked reference is the
released checkpoint scored with upstream's own code
(`scripts/laya/laya_upstream_baseline.py`), which squeezes all 77 options into
the fixed head budget.

| Model | Training | Accuracy | Soft CE | ECE |
| --- | --- | ---: | ---: | ---: |
| Released `laya`, unpacked (upstream code) | zero-shot | 0.348 | — | — |
| Upstream reported, released / `laya-typed-decisions` | zero-shot, their 400 cases | 0.425 / 0.492 | — | — |
| Jev (published) | zero-shot | 0.870 | — | — |
| Candidate-packed fine-tune, RLCD, seed 42 | 1 epoch | 0.015 (diverged) | 4.335 | 0.002 |
| **Candidate-packed fine-tune, soft CE, seed 42** | 1 epoch | **0.828** | 0.822 | 0.083 |
| Candidate-packed fine-tune, soft CE, seed 43 | 1 epoch | 0.810 | 0.851 | 0.109 |

- **Candidate mode makes 77-way choice learnable.** Upstream attributes its
  0.425 ceiling to the unpacked layout: 77 options share one fixed budget of
  about 4 tokens each. With a branch per option the fine-tune reaches
  0.819 mean over two seeds (0.828, 0.810).
- **Not a like-for-like comparison.** The fine-tune saw 20 in-domain
  examples per intent; the reference numbers, including Jev's, are
  zero-shot. An unpacked fine-tune at equal budget is not possible here,
  because the unpacked layout has no room for 77 options.
- **RLCD diverges with 77 options.** Cross-entropy rose from 3.6 to 5.0 with
  gradient norms in the thousands, then collapsed to uniform (ln 77 = 4.34).
  A quarter of the learning rate did not help. The policy term's Gaussian
  exploration over all 77 logits is too noisy at batch size 1. Soft CE trains
  cleanly (gradient norms ~100). Use `"objective": "soft_ce"` for
  many-option questions.
- **Cost:** 1,540 training steps took ~30 minutes on Metal (~1.2 s per step
  at ~370 tokens per row). Evaluating 400 decisions processed 147,764 tokens
  in ~140 s.

**Training throughput (as measured for step 0).** Steady state on this
machine: ~5–6 s per unpacked decision, and ~6–9 s per packed case of five
decisions. See [Trainer throughput](../../../../../zig/pkg/inference/models/laya/LAYA.md#trainer-throughput) for what changed
since. Several things turned up along the way:

- **Bucketing:** sequences are bucketed to 64 tokens and options to 4 so
  compiled programs are reused. Parity is unchanged, but it did not shorten
  steps, so graph construction is not the bottleneck.
- **Batch size:** `batch_size` 4 raised throughput only from 0.88 to 1.16
  decisions/s, and 8 gave 1.10. Cost grows about linearly with tokens.
- **Where the time goes:** a profile of the Metal trainer showed ~61% of the
  main thread waiting on a GPU synchronization after each op, ~11% slicing on
  the host, and ~13% downloading gradients and uploading them again for the
  optimizer.
- **Environment:** peak memory is ~27.5 GB. A concurrent 10 GB Zig build got
  the trainer SIGKILLed. macOS also throttles a trainer launched from a
  background shell (nice 5, background scheduling policy, display sleep) to a
  few steps per hour; run it in the foreground or clear the policy
  (`taskpolicy -B -p <pid>`) and keep the display awake. The raw logs are in
  the work log. See [Training memory](../../../../../zig/pkg/inference/models/laya/LAYA.md#training-memory) for a breakdown of
  this footprint and the admission gate added to refuse a job that will not
  fit before it starts, instead of relying on the OS to kill a concurrent one.
