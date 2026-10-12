This two-document corpus exercises the extraction comparison command. It is a
smoke fixture, not a representative evaluation set.

Run from the repository root with the FP32 base bundle installed:

```sh
PYTHONPATH=py/packages/embedded/src python scripts/eval_extract.py \
  --models-dir "$ANTFLY_INFERENCE_MODELS_DIR" \
  --corpus scripts/testdata/eval_extract/corpus.jsonl \
  --config scripts/testdata/eval_extract/plain.json \
  --config scripts/testdata/eval_extract/descriptions.json \
  --output comparison.json
```

Both configs use window mode, a 256-word window, 32-word overlap and threshold
0.5. The documents fit in one window. The CLI forces Unicode-code-point spans
and confidence output so exact-span evaluation is unambiguous.

A local Metal run on 2026-10-12 using the FP32 base files pinned in
`zig/pkg/inference/testdata/gliner25/pipeline_cases_base.json` produced:

| Config | person mentions | location mentions | TP | FP | FN | precision | recall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| plain | 1 | 2 | 2 | 1 | 0 | 0.667 | 1 |
| descriptions | 1 | 1 | 2 | 0 | 0 | 1 | 1 |

Both found `Ada Lovelace` at [0,12) and `London` at [23,29). The plain config
also labelled `weather` at [4,11) as a location in the negative example;
the description config produced no entity there. This demonstrates that the
public comparison path measures changed predictions, without claiming a
population-level precision improvement or selecting a production threshold.

The first config includes cold model loading while the second reuses the
loaded model, so their elapsed times are not a throughput comparison. Use a
held-out labelled corpus and separately controlled warm-up for deployment
choices. The command's report retains complete outputs and effective configs.

The larger `regression.jsonl` set has 23 manually labelled documents (six
negatives), with repeated mentions, accented/Arabic/Indic names and a long-window
case. `scripts/ci/run_lite_extraction_eval.py` runs both configs through the real
public inference API, requires precision/recall >= 0.5 and correct spans for both
labels, and preserves the full report as a CI artifact. CI uses the SHA-pinned
FP32 model prepared by `prepare_lite_graph_model.py`; an unavailable runtime
fails this required gate. The broad 0.5 floors detect unusable extraction without
pretending this small curated set is a representative benchmark. Do not tune a
production threshold using these examples alone.

A local run on 2026-10-12 with the same pinned FP32 bundle produced identical
exact-span counts on CPU (`native`) and Metal, including the multi-window case:

| Config | TP | FP | FN | precision | recall | F1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| plain | 38 | 30 | 1 | 0.559 | 0.974 | 0.710 |
| descriptions | 38 | 3 | 1 | 0.927 | 0.974 | 0.950 |

These are regression results on a curated corpus, not held-out benchmark scores.
