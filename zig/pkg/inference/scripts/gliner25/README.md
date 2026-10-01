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
- [`CUDA.md`](CUDA.md): CUDA kernels and training, required-GPU tests, and
  pinned Fastino CUDA eager, compiled, mixed-precision and FlashDeBERTa candidates.

All comparisons verify model, source, token, and output identity before timing.
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

## ModernBERT encoder reference

`modernbert_reference.py` builds a tiny ModernBERT `BoundaryExtractor` with the
published head settings on the pinned upstream (it reuses `oracle.py`'s
runtime checks), saves it as a boundary checkpoint, and captures one padded
batch: token ids and routes, the routed encoder states, and every encoder
gradient for fixed cotangents. Its output is deterministic.

```sh
PYTHONDONTWRITEBYTECODE=1 <oracle venv>/bin/python zig/pkg/inference/scripts/gliner25/modernbert_reference.py \
  --upstream <GLiNER2 checkout at the pinned commit> --output <dir outside Git>
```

The tokenizer and `processor.json` are checked in under
`testdata/gliner25/modernbert_tokenizer` and pin the per-word tokenization
(each word is tokenized alone, as upstream does). The checkpoint and
`reference.safetensors` stay outside Git; set
`ANTFLY_GLINER25_MODERNBERT_REFERENCE=<dir>` to run the encoder parity test
(`ANTFLY_GLINER25_MODERNBERT_BACKEND=metal` for Metal) and the resident Metal
training-job test in `src/finetune/gliner/boundary_modernbert_test.zig`.
