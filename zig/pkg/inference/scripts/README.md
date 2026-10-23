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

## Shared kernel recipes

Run from `zig/pkg/inference` with the repository-pinned Zig:

```sh
zig build quant-recipe-cuda-host-conformance
zig build quant-kernel-metal-runtime-conformance -Dmetal=true
zig build kernel-recipe-cuda-source
```

The host check compiles the generated expressions as C++ and checks packed
values, Q6 integer/scale components, activation policies and shuffle trees.
The Metal check runs the existing 50 device cases. The CUDA source target only
generates a model-free device test; it can run on a machine without CUDA.

On an NVIDIA host with `nvcc` in `PATH`:

```sh
zig build kernel-recipe-cuda-conformance
```

That target compiles for the installed GPU (`-arch=native`) and runs real
packed-value, activation and full-warp reduction kernels. Its scalar arithmetic
conformance disables contraction and flushing subnormals. This is a numerical
check; it does not qualify production matmul scheduling, CUDA capture or model
latency. The target always recompiles for the current GPU and reruns the device
checks, even if earlier build outputs are cached.

The shared descriptor fixture also runs through each target's existing tensor
setup and checks strict transfer admission, legality queries, output reuse and
input preservation. For example, on a CUDA host:

```sh
zig build test-inference-unit -Dcuda=true -Dcuda-artifacts=fatbin \
  -Dtest-filter='CUDA shared descriptors'
```

The matching CPU and Metal filters are `CPU shared descriptors` and
`Metal shared descriptors`; enable Metal with `-Dmetal=true` on a Mac.

After changing production CUDA source, rebuild its artifacts with the pinned
CUDA toolkit contract in `regen-cuda-artifacts.sh`, then run the affected device,
capture and model performance gates on the same host for baseline and candidate.
Host conformance and portable product compilation do not replace those gates.
