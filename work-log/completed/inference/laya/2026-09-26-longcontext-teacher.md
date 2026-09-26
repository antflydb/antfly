# Long-context teacher (step 2a) — raw run log

2026-09-26, Apple M4 Max (36 GiB, shared with ~8 other agents). Supports
[LAYA.md, "Long-context teacher (step 2a)"](../../../../zig/pkg/inference/models/laya/LAYA.md#long-context-teacher-step-2a).
Branch `laya/long-context-teacher`, base commit `9d9627ec3e`.

## Environment

- Teacher: `mlx-community/Qwen3-14B-4bit`, downloaded via `huggingface_hub.snapshot_download`
  (7.8 GB) to `.tmp/laya/qwen3-14b-4bit`. No local build needed (`mlx-lm` ships
  prebuilt wheels for macOS/arm64).
- Ran under the shared GPU lock (`with-lock gpu`), one bounded batch per
  invocation. Actual queueing observed: several runs waited 5-25 minutes for
  other agents' training/eval jobs to release the lock before starting; this
  is expected under ~9 concurrent agents and is not teacher cost.
- Script: `scripts/laya/prepare_laya_longcontext_teacher.py` (new).

## Eligibility check (which records Laya truly cannot see)

`prepare_laya_finetune.py`'s 316-raw-token cutoff is a global worst case;
re-checking against the exact `state_fits` budget (which depends on how many
labels a specific question has) found far fewer truly-invisible records than
the cutoff's superset:

```
eval:        160 candidates (>316 raw tokens) -> 34 truly invisible (21%)
train:       425 candidates                   -> 69 truly invisible (16%)
calibration: 145 candidates                   -> ~31 truly invisible (21%, inferred:
                                                   all fell in the "choice" bucket;
                                                   0 for score/noul)
```

Qwen tokenizer length check on the 425 `train` long-candidates: min/median/p90/max
= 320 / 375 / 473 / 549 tokens. All comfortably inside Qwen3-14B's 40,960-token
native context.

## Memory footprint

```
/usr/bin/time -l uv run --script scripts/laya/prepare_laya_longcontext_teacher.py \
    td/s0-eval.jsonl --score-all --compare-laya --limit 300 --seed 20260925 ...
        439.15 real       137.33 user        87.16 sys
     8935194624  maximum resident set size   (8.94 GB)
       10502528  peak memory footprint (mach task_info; smaller metric, not RSS)
```

8.94 GB RSS is well under the ~18 GB bound, with room to spare on the shared
36 GB machine.

## Timing

| Run | Decisions scored | Candidates | seconds/decision | Prompt tokens |
| --- | ---: | ---: | ---: | ---: |
| `eval-long` (+ calibration fit) | 34 | 160 | 1.826 | 21,745 |
| `train-long` | 69 | 425 | 1.929 | 44,050 |
| `short-state` (s0-eval sample) | 300 | 300 (`--score-all`) | 1.127 | 104,890 |

Each run held the GPU lock under 8 minutes of active compute (excluding queue
wait for other agents). Per-label cost after the first (prompt) forward per
record was 30-60 ms via the branched KV cache, against ~2 s for a full
prompt-length forward — confirms the cache branch (`KVCache.state` snapshot
and restore) does not force a re-encode.

## Full metrics JSON

Preserved from the actual runs (paths are under the producing worktree's `.tmp/laya/`,
not committed):

- `eval-long-metrics.json` — 34 long decisions, `--compare-laya`, calibration
  fit on `calibration-long.jsonl` (145 candidates -> temperatures `{"choice":
  3.9939209720620426, "score": 1.0, "noul": 1.0}`).
- `train-long-metrics.json` — 69 long decisions, `--compare-laya`, reused the
  eval-long temperatures via `--temperatures`.
- `short-state-metrics.json` — 300 short decisions (`s0-eval.jsonl` sample,
  seed 20260925), `--score-all --compare-laya`, reused the same temperatures.

Combined long-state metrics (34 + 69 = 103 decisions, decision-weighted mean):

| Metric | teacher_uncalibrated | teacher_calibrated | laya_teacher |
| --- | ---: | ---: | ---: |
| accuracy | 0.7670 | 0.7670 | 0.2913 |
| soft_ce | 4.6452 | 3.4912 | 1.4243 |
| ece | 0.2341 | 0.2481 | 0.1998 |

`laya_teacher` on the 300-decision short-state sample: accuracy 0.3933,
soft_ce 1.3114, ece 0.1517 — matches the released zero-shot unpacked numbers
in LAYA.md's Accuracy (step 0) table (0.387 / 1.308 / 0.158 on the full 760),
a useful cross-check that this script's Laya-baseline path (upstream
`build_sequence` + `DecisionModel`, reused from
`prepare_laya_packed_distillation.py`) reproduces the documented reference.

## Commands run

```bash
# Sanity check: KV-cache branching leaves the prompt's cache untouched and is fast
uv run --with mlx-lm python3 - <<'EOF'
# see zig/pkg/inference/models/laya/LAYA.md for the summary result:
# prompt forward 700 tok: 1.96 s; branched continuations: 0.06 s, 0.04 s;
# cache offset/shape unchanged after branching.
EOF

# Calibrate + score eval-long, compare against Laya's own (truncated) scoring
with-lock gpu -- uv run --script scripts/laya/prepare_laya_longcontext_teacher.py \
    .tmp/laya/eval-long.jsonl \
    --teacher-model .tmp/laya/qwen3-14b-4bit \
    --laya-model .tmp/laya/laya-released --common .tmp/laya/common.py \
    --calibration .tmp/laya/calibration-long.jsonl --compare-laya \
    --output .tmp/laya/distilled-eval-long.jsonl \
    --metrics-output .tmp/laya/eval-long-metrics.json

# Score train-long, reusing the fitted temperatures
with-lock gpu -- uv run --script scripts/laya/prepare_laya_longcontext_teacher.py \
    .tmp/laya/train-long.jsonl \
    --teacher-model .tmp/laya/qwen3-14b-4bit \
    --laya-model .tmp/laya/laya-released --common .tmp/laya/common.py \
    --temperatures .tmp/laya/distilled-eval-long.jsonl.json --compare-laya \
    --output .tmp/laya/distilled-train-long.jsonl \
    --metrics-output .tmp/laya/train-long-metrics.json

# Short-state comparison sample (both teachers can see the state)
with-lock gpu -- /usr/bin/time -l uv run --script scripts/laya/prepare_laya_longcontext_teacher.py \
    .tmp/laya/td/s0-eval.jsonl \
    --teacher-model .tmp/laya/qwen3-14b-4bit \
    --laya-model .tmp/laya/laya-released --common .tmp/laya/common.py \
    --temperatures .tmp/laya/distilled-eval-long.jsonl.json \
    --score-all --compare-laya --limit 300 --seed 20260925 \
    --metrics-output .tmp/laya/short-state-metrics.json
```

## Open items

- `score`/`noul` calibration had zero truly-invisible examples in the
  `calibration.jsonl` split, so their temperatures are unfit (default 1.0).
  Needs a bigger long-state calibration pool — step 2c's 8k-token states
  should supply one.
- Length-normalized log-likelihood (average log-prob/token) was used to
  compare labels of different token counts; not verified against an
  un-normalized (raw sum) alternative.
- No option-count bucketing of temperature (Laya's own config buckets by
  `type:count`); a single per-type temperature was used given the small
  calibration pool.
