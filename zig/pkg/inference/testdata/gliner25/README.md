# GLiNER2.5 regression fixtures

This directory contains inputs and expected results consumed by reusable tests,
generators and validation tools. Keep these files reproducible and bounded:

- `reference_manifest.json`, `requests.json`, model metadata and the three
  reference captures pin the upstream source, dependencies and artifact bytes.
- Tiny encoder/head, task, decoder, training and optimizer fixtures provide
  expected values. Their source generators and contracts define permissible
  updates; numerical tolerances must not be adjusted to fit a new result.
- `training_job_small_v1` supplies the fixed dataset used by job tests.
- `joint_optimizer_source_v1` retains its authoritative source-only capture,
  contract, generator and tests.
- Compact CrossNER/MASSIVE ledgers retain provenance and, where used by tests,
  evidence-identity regressions. They are not release-qualification flags.

Do not check in campaign directories containing build logs, executables,
object files, copied source trees, temporary environments, model/adapter output
copies or every intermediate attempt. Store new runs under the repository's
ignored `.benchmark-results/gliner25/` directory or an external artifact store.
Use a fresh directory for each attempt; preserve failures and exact source,
model, executable and report hashes with the run.

Large optional source captures and published model files remain external.
Tests select them explicitly and skip when absent. A skip does not establish
correctness or qualification. The reusable Python scripts live in
[`scripts/gliner25`](../../scripts/gliner25/); the shared process supervisor
lives there rather than inside a historical campaign archive.

See [oracle reproduction](../../../../../docs/GLINER25_ORACLE.md),
[implementation limits](../../../../../docs/GLINER25_IMPLEMENTATION.md), and
[training contracts](../../../../../docs/GLINER25_TRAINING.md).
