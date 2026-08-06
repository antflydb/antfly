# GLiNER2 test fixtures

These fixtures are synthetic and intentionally tiny. They exercise parsing,
training-objective coverage, and cross-runtime parity; they are not release or
quality-evaluation data.

- Core objective fixtures: `ner_smoke.jsonl`, `classification_smoke.jsonl`,
  `json_smoke.jsonl`, and `relation_smoke.jsonl`.
- Cross-objective fixtures: `full_task_smoke.jsonl`, `mixed_task_smoke.jsonl`,
  `multicount_smoke.jsonl`, and `described_smoke.jsonl`.
- Validation and edge cases: the remaining `*_smoke.jsonl` files.

The release-data validator rejects overlap with every checked-in fixture in
this directory.
