# Retained GLiNER2.5 CUDA evidence

This is a curated diagnostic snapshot, with `qualification: false` throughout.
It contains only:

- Three benchmark reports and compressed paired/tail samples supporting the
  Python comparisons in [CUDA.md](../../../../CUDA.md): Multi FP32 eager,
  Multi-Decide FP16 resident-weight capacity, and Decide-1B FP16 resident-weight
  capacity. The Multi-Decide report retains its three failed cells.
- Five compressed holdout oracles and report summaries: classification for all
  three checkpoints, plus Multi entity and structured extraction. These are
  inputs to `check_family_reference_holdout.py`, `score_family_classification.py`
  and `recheck_family_holdout.py`. Successful per-document report rows are
  omitted; complete native/reference outputs and token IDs remain in the captures.
- `summary.json`, preserving fixture checks, per-language quality results,
  rejected precision candidates, source-span compatibility results, gold accuracy,
  and HTTP/window correctness checks. Historical source report hashes identify
  the original artifacts; the summary does not replace a replay capture.

`../manifest.json` pins every retained file. Runtime test inputs, configuration
sidecars and FP32 oracles elsewhere in `family/` remain intact.

Generate new campaigns into `.benchmark-results/gliner25/`, `/tmp`, or an external
artifact store. Do not check in intermediate optimization reports, duplicated
prediction arrays, build logs, or additional campaign histories. A retained
snapshot change needs an updated manifest and review of its scope and failures.
