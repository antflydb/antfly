# GLiNER2.5-Decide on Metal

The FP32 benchmark compares the loaded Antfly typed batch API with Fastino's
`batch_classify_text` on the same FP32 checkpoint and inputs. Tokenization,
encoder execution, classification and decoding are timed. HTTP transport is
outside this measurement.

The frozen fixtures cover 64-, 128- and 384-token inputs, mixed lengths, and
synthetic hotel reviews, each with 1, 8 and 32 inputs per call. They measure
performance and implementation parity, not model accuracy on a labeled holdout.

## Requirements

- macOS with a physical Metal/MPS device and Zig 0.17.
- An Antfly-installed `GLiNER2.5-Decide` extractor directory, including
  `model_manifest.json`, the tokenizer sidecars and the original FP32 weights.
  The checkpoint SHA-256 is
  `40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997`.
- A Python environment with Fastino GLiNER2 at commit
  `c379245ce9fe0fee03a530a320ddb1885847c4d6`. The retained results record the
  exact Python, Torch and supporting-library versions.

Run commands from a repository worktree. Keep the dependency checkout and
generated reports in the ignored `.benchmark-upstream` and `.benchmark-results`
directories. Build with the worktree's `zig/.zig-cache` and `zig/zig-out`.

```sh
git clone https://github.com/fastino-ai/GLiNER2.git .benchmark-upstream/GLiNER2
git -C .benchmark-upstream/GLiNER2 checkout c379245ce9fe0fee03a530a320ddb1885847c4d6
make -C zig watch ZIG=/path/to/zig-0.17 \
  WATCH_STEP=inference-bench-gliner25-decide-build \
  ZIG_BUILD_FLAGS='-Doptimize=fast -Dmetal=true -Dcuda=false -j1'
```

Stop the build watcher before measuring GPU performance.

## Default Python batching

Fastino defaults to eight inputs per internal batch. Apply that same limit to
both the untimed raw-score capture and the timed public API; otherwise a large
capture can inflate the default-batch memory measurement.

```sh
DECIDE_PYTHON=.benchmark-venv/bin/python
DECIDE_MODEL=/path/to/installed/GLiNER2.5-Decide
DECIDE_CASES=zig/pkg/inference/scripts/gliner25/fixtures/decide_metal_workloads.json

$DECIDE_PYTHON zig/pkg/inference/scripts/gliner25/benchmark_decide_metal_matrix.py campaign \
  --upstream .benchmark-upstream/GLiNER2 --model-dir "$DECIDE_MODEL" \
  --binary zig/zig-out/bin/antfly-inference-gliner25-decide-bench \
  --cases "$DECIDE_CASES" --python-microbatch 8 --antfly-controlled \
  --blocks 3 --warmup 3 --reps 20 --output .benchmark-results/decide-default8

$DECIDE_PYTHON zig/pkg/inference/scripts/gliner25/evaluate_decide_metal_matrix.py \
  --campaign .benchmark-results/decide-default8 --fixture "$DECIDE_CASES" \
  --output .benchmark-results/decide-default8-evaluation.json
```

For Python's full-batch variant, derive the five 32-input cases from the same
frozen fixture and use `--python-microbatch 0` in a new output directory:

```sh
python3 - <<'PY'
import json
from pathlib import Path
cases = json.loads(Path("zig/pkg/inference/scripts/gliner25/fixtures/decide_metal_workloads.json").read_text())
cases["cases"] = [case for case in cases["cases"] if len(case["texts"]) == 32]
Path(".benchmark-results/decide-full32-cases.json").write_text(json.dumps(cases, indent=2) + "\n")
PY
```

Inputs of size 1 and 8 already have the same effective batch shape under both
modes, so this variant adds only five cells. Keep the Antfly policy unchanged.

## Acceptance and evidence

Each cell uses three fresh-process pairs in AB/BA/AB order, three warmups and
twenty timed calls. The checker requires lower median latency, peak footprint
and peak RSS in every pair, plus lower pooled p95 latency. It also requires:

- Exact prepared token IDs and selected labels, maximum absolute logit error
  of 0.002, and probability error of 0.0005, including decoded confidence.
- The authenticated automatic Metal policy, FP32 mirrors, packed QKV execution
  without fallbacks, and no tuning environment overrides.
- Complete fixtures and sample counts, unchanged source/binary/harness,
  no swap growth, unchanged power source and available thermal reports.

RSS and macOS `phys_footprint` are sampled separately every 200 ms over the
whole process, including loading. Footprint excludes clean file-backed mappings;
neither metric is dedicated GPU VRAM. Reported GB are decimal.

The exact-artifact policy uses private FP32 mirrors, at most eight inputs and
1,000 padded tokens per physical forward, a 512 MiB QKV packing cap and 32 MiB
attention scratch cap. Nearby lengths reuse padding only within the token
budget and at most 25% extra padding. Public request limits are unchanged.

## Qualified M4 Pro result

The 2026-11-13 campaign used an M4 Pro with 16 GPU cores and 24 GiB memory,
macOS 27.2, Python 3.14.3, GLiNER2 2.1.0 at the pinned revision, Torch 2.15.0
and Transformers 5.23.0. The same automatic Antfly policy was used throughout.

| Python mode | Cells | Median speedup | Lower peak footprint |
| --- | ---: | ---: | ---: |
| Default internal batch of 8 | 15 | 1.25-2.76x | 9-46% |
| Full internal batch of 32 | 5 | 1.14-3.26x | 28-60% |

All 60 paired blocks passed the median, pooled-p95, footprint, RSS, numerical
and host-condition checks. Raw per-call samples, logits, process-memory traces,
evaluation JSON and executable snapshots remain local in `.benchmark-results`.
These results cover the loaded typed API; serving lifecycle tests are separate.

Run the benchmark provenance regression with:

```sh
python3 zig/pkg/inference/scripts/gliner25/test_benchmark_decide_metal_matrix.py -v
```

## Q8_0 split bundle

The published [Antfly Q8_0 bundle](https://huggingface.co/antflydb/GLiNER2.5-Decide/tree/358ae4b1263cedb79faf9e8b7984c3b4e5586874)
contains a quantized encoder and an FP32 auxiliary head. Both exact GGUF hashes,
the tokenizer and configuration must match before the Metal policy is installed.
It uses FP32 encoder mirrors decoded from the Q8 weights, the same bounded
physical batches, and independent serving admission reservations. Relative
position tables decode on the GPU when the bucket layout changes, avoiding a
2 MiB host download/upload round trip while retaining the bounded cache.

Quantization changes the learned scores. Compare runtime correctness with an
independent Python decode of these same GGUF bytes at the original strict
tolerances, and report drift from the original FP32 checkpoint separately. Keep
reference processes separate from both timed runtimes and their memory samples.

```sh
DECIDE_Q8=/path/to/installed/GLiNER2.5-Decide-Q8_0
DECIDE_Q8_REFERENCE=.benchmark-results/models/decide-q8-reference

$DECIDE_PYTHON zig/pkg/inference/scripts/gliner25/prepare_decide_metal_q8_reference.py \
  --bundle "$DECIDE_Q8" --source "$DECIDE_MODEL" --output "$DECIDE_Q8_REFERENCE"

$DECIDE_PYTHON zig/pkg/inference/scripts/gliner25/benchmark_decide_metal_matrix.py campaign \
  --upstream .benchmark-upstream/GLiNER2 --model-dir "$DECIDE_Q8" \
  --python-model-dir "$DECIDE_MODEL" --reference-model-dir "$DECIDE_Q8_REFERENCE" \
  --antfly-quantization q8_0 \
  --binary zig/zig-out/bin/antfly-inference-gliner25-decide-bench \
  --cases "$DECIDE_CASES" --python-microbatch 8 --antfly-controlled \
  --blocks 3 --warmup 3 --reps 20 --output .benchmark-results/decide-q8-default8

$DECIDE_PYTHON zig/pkg/inference/scripts/gliner25/evaluate_decide_metal_matrix.py \
  --campaign .benchmark-results/decide-q8-default8 --fixture "$DECIDE_CASES" \
  --output .benchmark-results/decide-q8-default8-evaluation.json
```

The timed Python baseline remains the original Fastino FP32 checkpoint. The
decoded-Q8 checkpoint supplies correctness outputs only. Repeat the five
32-input cases with `--python-microbatch 0` to check full Python batching.

The Q8 campaign on the same M4 Pro passed all 60 paired blocks and 2,400 timed
performance calls, including every paired median, peak RSS and footprint check,
pooled p95, exact token IDs and decoded labels against the decoded-Q8 reference.

| Original FP32 Python baseline | Cells | Median speedup | Lower sampled peak footprint |
| --- | ---: | ---: | ---: |
| Default internal batch of 8 | 15 | 1.25-3.23x | 5-41% |
| Full internal batch of 32 | 5 | 1.14-3.74x | 28-60% |

Peak RSS was 71-83% lower in the default-batch matrix. The maximum error against
the decoded-Q8 reference was 0.000032 for logits and 0.0000057 for probabilities.
Labels also matched the original FP32 model on all frozen performance inputs;
the maximum probability difference from FP32 was 0.0088. These synthetic inputs
do not establish an accuracy guarantee.

All 58 focused tests passed without skips or leaks, including real Q8 and FP32
serving batches through 128 inputs, concurrent admission, cancellation, budget
pressure and retry recovery. The CPU GGUF decoder, finite-value guard and source
provenance regressions also passed.

A separate 512-token stress probe with eight distinct inputs stayed within the
numeric tolerances (maximum logit error 0.000143, probability error 0.000032).
Two decisions selected different labels with reference top-score gaps of zero
and 0.00000048. Exact label identity therefore failed for that near-tie probe;
its raw failure and batch-shape diagnostic remain retained, and it is excluded
from the qualified performance matrix. Original numeric tolerances and exact-label
requirements remain in force for the performance matrix.

Raw Q8 evidence, the immutable executable/source snapshot and the aggregate
report remain in `.benchmark-results/q8-confirmation-*`,
`.benchmark-results/q8-device-policy-snapshot` and
`.benchmark-results/q8-final-report.json`.
