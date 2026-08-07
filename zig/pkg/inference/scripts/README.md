# Inference scripts

Most files here are model-specific diagnostics or release tooling, not CI entry
points. The model documentation owns the detailed commands and acceptance
criteria. This index identifies the small set of scripts an operator normally
starts with.

## GLiNER2 fine-tuning

Primary entry points:

- `run_gliner2_lora_production_readiness.sh` runs the complete release gate.
- `run_gliner2_lora_perf_gate.sh` runs repeated accelerator/performance parity.
- `compare_gliner2_lora_python_zig.py` runs one native, Metal, or CUDA parity
  comparison against the pinned Python oracle.
- `qualify_gliner2_cuda_hardware.py` records a CUDA hardware lane, and
  `summarize_gliner2_cuda_hardware.py` verifies the multi-architecture matrix.
- `prepare_gliner2_smoke_diagnostic.py` materializes explicitly non-release
  synthetic data from `../testdata/gliner2/`.

Supporting modules are grouped by role in their names:

- `evaluate_*`, `validate_*`, and `finalize_*` enforce release contracts.
- `summarize_*` materializes retained convergence or hardware evidence.
- `gliner2_*` contains shared data and contract helpers.
- `test_*gliner2*` and the adjacent named tests cover these script contracts.

See `../docs/finetuning/GLINER2.md` for the authoritative workflow. The
generated Unicode tables must be checked with
`python3.12 generate_gliner2_unicode_tables.py --check`.

## Other model families

Gemma4, Florence2, ClipClap, reranker, and quant-kernel scripts use the model or
backend name in the filename. Prefer the matching documentation before running
hardware gates; many scripts intentionally require local model artifacts or a
specific accelerator.
