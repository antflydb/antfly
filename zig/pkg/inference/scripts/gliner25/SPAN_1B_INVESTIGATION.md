# Decide-1B extraction investigation

The pinned `fastino/GLiNER2.5-Decide-1B` checkpoint has numerical span-head
parity, but no non-empty extraction quality evaluation. Do not advertise NER,
relations or generic span extraction capabilities for it. Its typed decision
and classification capabilities remain available. The published limits expose
`span_extraction_status: "parity_only_no_nonempty_eval"`.

The checked-in `testdata/gliner25/decide_1b_extraction_investigation.json` records
untouched upstream runs at commit `55656fbfa01d3d4a77485e1a1eeeaf682990ccdf`, with
all checkpoint files verified against the existing pinned hashes. It compares
Transformers 4.55.4 with the checkpoint-declared 5.17.0. Both produce the same
empty NER/relation outputs and a correct `refund` classification control.

The probes use `AutoExtractor.extract_entities` directly, including the upstream
README's Apple/Tim Cook example, descriptive labels, CoNLL labels, a single
person label, and a combined entity/relation schema. A threshold of 0.01 does
not recover entities. The count head predicts one instance, so the zero-count
gate is not responsible. Raw span logits remain thousands below zero (the
README example's maximum is approximately -3188.69); FP32 sigmoid underflows
to zero before decoding. We did not alter weights, force counts, or change
probabilities to manufacture a non-empty result.

This evidence rules out those label formats, thresholds and the tested
Transformers version mismatch. It does not establish why the checkpoint's
span heads behave this way or prove that every possible input is empty. The
model card describes this release as a classification specialist; the presence
of trained-looking span-head tensors alone does not qualify extraction quality.
A future checkpoint or upstream fix needs a real non-empty extraction evaluation
before these capabilities can be restored. Explicit span execution remains
available for diagnostics and compatibility testing.

Reproduce with the oracle environment and pinned source/checkpoint:

```sh
python investigate_span_extraction.py --upstream <pinned-GLiNER2> \
  --model-dir <pinned-Decide-1B> --output <receipt.json>
```

To compare runtime versions, use separate environments with the same PyTorch
and checkpoint bytes. The script records versions, raw logits, schema tokens,
count predictions, head-weight summaries and untouched public API outputs.
