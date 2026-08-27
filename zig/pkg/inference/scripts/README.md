# Inference scripts

Model-specific diagnostics and release tooling live in family directories;
shared inference, code-generation, and kernel utilities remain at this level.
The model documentation owns the detailed commands and acceptance criteria.

## Model families

- `gemma4/` contains Gemma4 Metal/CUDA benchmarks, qualification gates,
  speculative decoding checks, LoRA workflows, tests, and fixtures.
- `gliner2/` contains GLiNER2 training, parity, release-readiness, hardware
  qualification, tests, and fixtures.
- `mxbai/`, `florence2/`, `clipclap/`, and `layoutdoc/` contain the focused
  verification scripts for those model families.

## GLiNER2 fine-tuning

Primary entry points:

- `gliner2/run_gliner2_lora_production_readiness.sh` runs the complete release
  gate.
- `gliner2/run_gliner2_lora_perf_gate.sh` runs repeated accelerator/performance
  parity.
- `gliner2/compare_gliner2_lora_python_zig.py` runs one native, Metal, or CUDA parity
  comparison against the pinned Python oracle.
- `gliner2/qualify_gliner2_cuda_hardware.py` records a CUDA hardware lane, and
  `gliner2/summarize_gliner2_cuda_hardware.py` verifies the
  multi-architecture matrix.
- `gliner2/prepare_gliner2_smoke_diagnostic.py` materializes explicitly non-release
  synthetic data from `../testdata/gliner2/`.

Supporting modules are grouped by role in their names:

- `evaluate_*`, `validate_*`, and `finalize_*` enforce release contracts.
- `summarize_*` materializes retained convergence or hardware evidence.
- `gliner2_*` contains shared data and contract helpers.
- `test_*gliner2*` and the adjacent named tests cover these script contracts.

See `../docs/finetuning/GLINER2.md` for the authoritative workflow. The
generated Unicode tables must be checked with
`python3.12 gliner2/generate_gliner2_unicode_tables.py --check`.

## Other model families

Prefer the matching documentation before running hardware gates; many scripts
intentionally require local model artifacts or a specific accelerator. Shared
CUDA artifact generation, paired benchmarking, Metal debugging, and OpenAPI or
descriptor regeneration remain in this directory because multiple model
families consume them.
