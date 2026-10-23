# Inference scripts

Model-specific diagnostics and release tooling live in family directories;
shared inference, code-generation, and kernel utilities remain at this level.
The model documentation owns the detailed commands and acceptance criteria.

## Model families

- `embedder/` contains shared embedding endpoint/sparse benchmarks and
  model-specific reference, parity, and qualification tooling.
- `embeddinggemma2/` contains pinned reference acquisition, text/image/audio/video
  parity and performance qualification, corpus tooling, tests, and fixtures.
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

See `../models/gliner2/FINETUNING.md` for the authoritative workflow. The
generated Unicode tables must be checked with
`python3.12 gliner2/generate_gliner2_unicode_tables.py --check`.

## Other model families

Prefer the matching documentation before running hardware gates; many scripts
intentionally require local model artifacts or a specific accelerator. Shared
CUDA artifact generation, paired benchmarking, Metal debugging, and OpenAPI or
descriptor regeneration remain in this directory because multiple model
families consume them.

## LightOnOCR-3 Metal throughput

The native reader has an opt-in preset for qualified Apple M4 devices:

```sh
export ANTFLY_LIGHTONOCR3_THROUGHPUT=1
```

Set it in the inference server's environment. It selects sixteen physical decode
rows and a sixteen-page request window, prepared decode workspaces, an event-driven
executor, bounded activation reuse, batched paged attention and compact per-page KV.
One shared execution epoch can serve up to 256 page joins before retiring. Every
live page still acquires its own admission lease; the existing memory limits and
live-memory reserve apply.

The preset retains the reader's default precision. The separate optional setting
`TERMITE_METAL_LIGHTONOCR3_BF16_PREFILL_INPUTS=1` rounds prefill projection inputs to
BF16; benchmark it separately when selecting a 4B configuration. The 4B preset also
reserves its bounded shared immutable side-weight cache.

Individual settings override the preset, including explicit false values. For
example, `ANTFLY_LIGHTONOCR3_BATCH_ROWS=8` limits physical cohorts to eight,
`ANTFLY_LIGHTONOCR3_BATCH_WINDOW=8` limits active pages, and
`ANTFLY_LIGHTONOCR3_GATE_EVENTS=0` restores polling for the backend gate. Setting
`ANTFLY_LIGHTONOCR3_THROUGHPUT=0` restores the pre-preset defaults while preserving
explicit individual settings. Other hardware keeps its existing defaults.

The [execution report](lightonocr3/results/throughput_engine_m4_max/README.md)
records measured configurations, correctness checks, cache-capacity validation,
source/binary hashes and failed experiments. These synthetic fixture measurements
check consistency and throughput; they do not establish broad OCR accuracy parity.
