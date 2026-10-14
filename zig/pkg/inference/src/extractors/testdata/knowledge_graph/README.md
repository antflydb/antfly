# Knowledge graph inference evidence

These independent CPU PyTorch captures use upstream GLiNER2 revision
`55656fbfa01d3d4a77485e1a1eeeaf682990ccdf`. The complete Python source tree
hash is recorded in each fixture and checked before capture. Model file
sizes/hashes and runtime versions are embedded in each fixture. The capture
scripts verify all model files before loading. Fixture bytes are pinned by
`gliner_graph_qualification_fixtures.zig`.

Regenerate with Python 3.12 and the versions recorded in `runtime`:

```
python scripts/gliner25/capture_window_extraction.py --variant base --upstream UPSTREAM --model-dir MODEL --output OUTPUT
python scripts/gliner25/capture_window_extraction.py --variant multi_decide --upstream UPSTREAM --model-dir MODEL --output OUTPUT
python scripts/gliner25/capture_window_extraction.py --variant decide_1b --upstream UPSTREAM --model-dir MODEL --output OUTPUT
python scripts/gliner25/capture_span_extraction.py --upstream UPSTREAM --model-dir MODEL --output OUTPUT
```

Window fixtures cover one-character inputs, short claims, overlapping windows,
entity descriptions, relations, classification, and complete prompts of exactly
1024 tokens. Local outputs and raw classification logits come from upstream;
source rebasing, owned-word logit aggregation, interval selection and semantic
relation deduplication are independently reproduced in Python.

Decide-1B returns zero entity/relation counts on these probes upstream. To avoid
vacuous qualification, the span fixture also captures all 20 count logits,
three learned GRU slots, every valid span score and a span representation prefix.
Those forced diagnostic slots do not bypass the count gate in serving. Model
parity establishes implementation correctness, not extraction recall.

Run `inference-test` with filter `ModernBERT span extraction pinned` and
`ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR` for the 1B. For window tests, use filter
`GLiNER bounded window composition pinned`, `ANTFLY_GLINER_WINDOW_MODEL_DIR`
and `ANTFLY_GLINER_WINDOW_VARIANT=base|multi_decide|decide_1b`. Native and Metal
compare offsets exactly and probabilities within 5e-4. Large raw span dot
products use 2e-3 absolute plus 1e-5 relative tolerance, while their sigmoid
probabilities must independently meet 5e-4. Counts and GRU projections use
2e-3 absolute tolerance.

The bounded window policy composes a verified per-window encoder/head contract
with checkpoint-independent planner/merge contracts. It admits one document,
up to 64 windows, 1 MiB of UTF-8 and 131072 source words, with at most 1024
complete prompt tokens per new-profile window. Resource/work limits may reject
requests earlier. These document bounds do not imply a larger encoder context.
Existing base-only long-corpus and converted-precision profiles stay separate.
