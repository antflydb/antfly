# GLiNER2.5 development tools

This directory contains the retained native/Python inference comparisons and
trained-artifact checks. Historical campaigns and one-time fixture generators
belong in external evidence rather than the product tree.

## Fixtures and source identity

From the repository root:

```sh
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-fixtures
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-references
```

The fixture policy is documented in
[`../../testdata/gliner25/README.md`](../../testdata/gliner25/README.md).

## Inference performance

The canonical direct-core comparisons are:

- [`BENCHMARK.md`](BENCHMARK.md): native CPU versus pinned Fastino CPU.
- [`METAL_BENCHMARK.md`](METAL_BENCHMARK.md): native Metal versus pinned
  Fastino MPS and CPU.

Both harnesses verify model, source, token, and output identity before timing.
They write reports outside the repository and do not qualify HTTP serving or
release-tail performance.

## Training and artifact checks

The retained checkers each expose `--help` and write evidence to a caller-owned
output directory:

- `check_bundles.py` verifies converted model bundles and tensor identities.
- `check_trained_execution.py` runs bounded CPU or Metal inference against a
  trained artifact.
- `check_training_export.py` verifies portable full, head-only, LoRA, and DoRA
  exports and can compare them with the pinned Python runtime.
- `check_training_merge.py` verifies adapter materialization and merged output.
- `training_export_runtime.py` is the isolated Python execution worker used by
  the export checker.

Keep virtual environments, downloaded checkpoints, executables, reports, and
captured evidence outside Git. Use `.benchmark-results/gliner25/` or an external
artifact store for local evidence.
