# Laya long-context teacher evaluation and throughput (step 2a): 2026-09-26

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 2104–2110, 2179–2282, 2305–2377 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

## Long-context teacher (step 2a)

Measured 2026-09-26 on an Apple M4 Max (36 GiB, shared with other agents).
Step 2a needs soft labels for typed-decisions states Laya's 512-token budget
cannot see, so a later long-context Laya (step 2c) has something to distil
from besides gold.


### How much of the dataset is actually invisible to Laya

`prepare_laya_finetune.py` drops any case whose state exceeds 316 *raw* tokens
from every split, a global bound sized for the worst case (a question that
uses the full 192-token head budget, leaving `512 - 192 - 3 ≈ 317` tokens for
state). Most >316-token states belong to questions with fewer labels, which
leave more of the 512-token budget for state and so still fit. Re-checking
with the exact `state_fits` budget math, only a fifth of the >316-token
records are truly invisible to Laya:

| Split | >316-token candidates | Actually invisible to Laya |
| --- | ---: | ---: |
| `eval` | 160 | 34 (21%) |
| `train` | 425 | 69 (16%) |

Only the "actually invisible" column got a teacher target; the roughly 80% of
"long-looking" records Laya can in fact see keep passing through the existing
`prepare_laya_packed_distillation.py` path. Calibration used the full,
1,000-record `td/calibration.jsonl` split regardless of this eligibility split
(300 `choice`, 400 `score`, 300 `noul`; every kind fell into a single
option-count bucket — 3-5, 3-5, and 2 respectively — so the dataset does not
exercise per-bucket temperatures beyond a single bucket per type). **This is
the main limitation of the underlying dataset**: LocalLLaMA/typed-decisions'
states top out at 549 tokens and its option counts don't vary within a
question type, so genuinely long-context and high-cardinality cases are both
rare here. Step 2c's 8k-token states, once available, will produce more of
the former; Banking77-style candidate mode already exercises option-count
variation for a different question.

### Agreement with gold

760 short-state decisions (`td/s0-eval.jsonl`, sampled 300, seed 20260925 —
both teachers can see the full state) and 103 long-state decisions (the
"actually invisible" rows above, `eval` + `train` combined — only the
long-context teacher sees the full state; Laya scores upstream's own
truncated sequence, exactly as serving would today), scored with the teacher's
final calibration (raw-sum scoring, temperatures fit on the full 1,000-record
`calibration.jsonl`). `laya_teacher` here reproduces the released, zero-shot,
unpacked numbers in [Accuracy (step 0)](../../../../../zig/pkg/inference/models/laya/LAYA.md#accuracy-step-0) closely
(0.393/1.311/0.152 on this short-state sample vs. the reported
0.387/1.308/0.158 on the full 760).

| States | Teacher | Accuracy | Soft CE | ECE | Decisions |
| --- | --- | ---: | ---: | ---: | ---: |
| Short (Laya sees them too) | Long-context teacher, calibrated | **0.640** | **1.018** | **0.104** | 300 |
| Short | Laya, zero-shot | 0.393 | 1.311 | 0.152 | 300 |
| Long (Laya cannot see them) | Long-context teacher, calibrated | **0.767** | **0.980** | 0.233 | 103 |
| Long | Laya, zero-shot, truncated state | 0.291 | 1.424 | **0.200** | 103 |

Per question type, long states:

| Type | Teacher accuracy | Laya accuracy | Teacher soft CE | Laya soft CE | Teacher ECE | Laya ECE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| choice | 0.824 | 0.196 | 1.058 | 1.630 | 0.323 | 0.166 |
| score | 0.632 | 0.184 | 1.046 | 1.446 | 0.222 | 0.255 |
| noul | 0.929 | 0.929 | 0.518 | 0.616 | 0.225 | 0.342 |

Fitting on the full calibration split (versus the initial run, which fit only
on the ~31 long-state calibration examples eligibility happened to select)
cut the teacher's soft CE by 3-4x on both short and long states and turned an
ECE loss into a near-wash: the teacher now wins accuracy and soft CE outright
in every row above, and ECE in 2 of 3 long-state question types (`choice`
alone still trails Laya's tuned calibration there). This is the expected
result: the earlier run's poor soft CE was a calibration-sample-size artifact,
not evidence the teacher's underlying scores were unreliable.

Timing: 1.6-1.9 s/decision end to end (prompt forward plus branched label
continuations), 21,745-104,890 prompt tokens per run. Peak resident set
(`/usr/bin/time -l`, 300-decision run): 8.94 GB, unchanged from the first
measurement and well under the 18 GB bound. A 1,000-record calibration run
plus 34-decision scoring pass took under 20 minutes of GPU compute (separate
from time spent queued behind other agents' jobs).

### Adopt/reject

**Decision: adopt**, on both short and long states, using the teacher's
calibrated distribution directly (still blended `w = 0.5` with gold per the
existing recipe, for consistency with `prepare_laya_packed_distillation.py`,
not because the blend is needed to offset overconfidence anymore):

- **Accuracy.** Decisive win everywhere: 0.640 vs. 0.393 on short states,
  0.767 vs. 0.291 on the long states this step exists for. Laya's
  truncated-state accuracy on long `choice`/`score` questions (0.196, 0.184)
  is barely above chance.
- **Soft CE.** Also a clear win once temperatures are fit on enough data:
  1.018 vs. 1.311 (short), 0.980 vs. 1.424 (long) — reversed from the
  under-calibrated first pass, where the teacher's soft CE was 3-6x worse
  than Laya's.
- **ECE.** A win on short states (0.104 vs. 0.152) and a near-tie on long
  states (0.233 vs. 0.200), with `choice` the one question type where Laya's
  own tuned calibration still edges out the teacher's single coarse
  temperature. Not a reason to withhold the teacher's targets: `score` and
  `noul` ECE both favor the teacher on long states, and the size of the
  remaining gap is small next to the accuracy and soft-CE swings.

This reverses the initial (pre-recalibration) mixed verdict, which had fit
temperatures only on the ~31 long-state calibration examples eligibility
happened to select and left `score`/`noul` at an unfit `1.0`. The lesson: a
teacher's own calibration is a property of its scoring convention and prompt,
not of which records happen to be labeled-target-eligible, so it should be
fit on the largest gold-labeled pool available, not a subset chosen for a
different reason. Revisit again once step 2c's 8k-token states give a
calibration split that exercises more than one option-count bucket per type.


### Teacher throughput

The question: how far can the teacher's labelling rate be pushed? The scaled
packed experiment needs about 10^5 decisions, like the Jev reproductions.

**Setup.**
- Machine: Apple M4 Max, 36 GB, MLX (mlx-lm 0.28 or later).
- Benchmark: `scripts/laya/benchmark_laya_teacher.py`.
- Throughput: 12 cases / 60 decisions from `td/s0-train.jsonl`.
- Quality: all 760 decisions of `td/s0-eval.jsonl` against gold.
- The benchmark does not include `prepare_laya_longcontext_teacher.py`'s
  bookkeeping or its `--compare-laya` pass.

**Where the time goes.**
- The teacher is compute-bound on prefill: about 280-320 tok/s for
  Qwen3-14B-4bit.
- Batching does nothing. Measured at length 256, rates were flat from batch 1
  to batch 16: 325, 283, 271 and 322 tok/s.
- Label continuations are negligible. The prompt forward is essentially the
  whole cost.
- The per-question prompt averages 297 tokens, and the state is 206 of them.
  The teacher re-reads the state once per question.

| Lever | Speed | Quality | Verdict |
| --- | ---: | --- | --- |
| Shared-state prefill: prefill the case's common prefix (the state) once, then fork the KV cache per question | **1.5-1.9×** (0.96-1.07 → 0.52-0.70 s/decision); 2.25× fewer tokens | Exact. The forked cache matches a two-chunk prefill of the same prompt bit for bit (difference 0.0). Both differ from a one-chunk prefill by up to 0.062 in label probability (1 argmax flip in 60, a near tie): bf16/4-bit chunking numerics, not the fork | **adopt** |
| Batching several prompts per forward | none on Metal | — | reject here |
| Smaller teacher (Qwen3-4B-4bit) | 3.3× (0.167 vs 0.534 s/decision, shared prefill) | Accuracy 0.514 vs 0.645 (14B); soft CE 1.117 vs 1.032 (uniform 1.212); agrees with the 14B on 67% of argmaxes. Below the unpacked student it would teach (0.621) | **reject** |
| 4B → 14B cascade (escalate when the 4B's calibrated top probability is below a threshold) | 1.7× at threshold 0.5, 1.2× at 0.6 (the 4B pass runs on every decision) | 0.582 (27% escalated), 0.617 (53%), 0.630 (70%) vs 0.645 | reject: every saving costs accuracy |
| Teacher only where Laya cannot see the state; Laya teacher elsewhere | ~5× fewer teacher calls (16-21% of decisions are invisible to Laya, [above](#how-much-of-the-dataset-is-actually-invisible-to-laya)) | Short states then get Laya-teacher targets, and distilling those did not help (0.451 vs 0.450) | only if quality allows |
| Datacenter GPU with prefix caching (for example vLLM with automatic prefix caching) | estimated ≥30× over this laptop | Same teacher, same targets | **the order-of-magnitude lever** |

The GPU row is an estimate, not a measurement. Prefill on a laptop GPU is
compute-bound, and a datacenter accelerator has well over 10× the
bf16/int8 matmul throughput. Serving stacks with automatic prefix caching get
the shared-state saving for free, and they batch across cases, which pays off
once there is compute headroom.

**Estimated wall-clock for 200k decisions:**
- About 2.5 days per-question on this laptop.
- About 1.5 days with shared-state prefill (0.65 s/decision, measured in the script).
- Hours on one datacenter GPU.

**Recommendation.**
1. Adopt shared-state prefill in `prepare_laya_longcontext_teacher.py`. Done:
   `--prefill shared` is now the default, and `--prefill per-question`
   reproduces earlier runs. Over all 760 decisions of `td/s0-eval.jsonl`
   (`--score-all`, uncalibrated), the script took 0.650 vs 1.304 s/decision
   (**2.0×**) and computed 109,893 vs 266,593 tokens. Accuracy was 0.646 vs
   0.645, and soft CE 5.755 vs 5.759.
2. Keep the 14B teacher.
3. For the 10^5-decision run, move labelling to a rented GPU running the same
   model behind a prefix-caching server. This is the only lever that reaches
   an order of magnitude without giving up label quality.

Stacking the lossy levers (the 4B cascade, Laya-only short states) would reach
about 8-10× on this laptop. The table shows what that costs in accuracy.

The 14B teacher is only 0.024 above the unpacked student on short states
(0.645 vs 0.621), so for short states the teacher's value is calibration and
scale, not accuracy. It is decisively better on long states (0.767 vs 0.291).

Reproduce:

```bash
uv run --script scripts/laya/benchmark_laya_teacher.py .tmp/laya/qwen3-14b-4bit td/s0-train.jsonl --cases 12
uv run --script scripts/laya/benchmark_laya_teacher.py .tmp/laya/qwen3-14b-4bit td/s0-eval.jsonl --score-output tacc-14b.jsonl
```

Accuracy is the argmax of the raw label scores against the argmax of `target`.
Soft CE uses one temperature fit on even rows and evaluated on odd rows
(4B: T = 18.2; 14B: T = 14.6).
