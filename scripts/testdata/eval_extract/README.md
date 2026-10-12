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
