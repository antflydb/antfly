# GLiNER2 PII and GLiGuard regression tools

The [model guide](../../../models/gliner2/SPECIALTY.md) documents the API,
schemas, limits and measured GB10 results. Run these commands from the worktree
root. All weights, full numerical captures, caches and run receipts belong in
ignored `.specialty-artifacts/`, not in `testdata/` or the PR.

The small checked-in [manifest](../../../testdata/gliner25/specialty/manifest.json)
pins the original FP32 artifacts, tokenizer markers, upstream source, Python
runtime and external capture digests. Config sidecars exercise artifact routing
without weights; `decoder_oracle.json` covers model-independent decoder edges.
The larger numerical captures are optional inputs to the native/GPU tests.

## Prepare the reference inputs

Use the existing dependency lock and a worktree-local environment:

```bash
mkdir -p .specialty-artifacts
uv venv .specialty-artifacts/oracle-venv --python 3.12
UV_CACHE_DIR="$PWD/.specialty-artifacts/uv-cache" uv pip install \
  --python .specialty-artifacts/oracle-venv/bin/python \
  --requirement zig/pkg/inference/scripts/gliner25/requirements.txt
export SPECIALTY_PYTHON="$PWD/.specialty-artifacts/oracle-venv/bin/python"
export PYTHONDONTWRITEBYTECODE=1
export HF_HOME="$PWD/.specialty-artifacts/huggingface"
export CUDA_CACHE_PATH="$PWD/zig/pkg/inference/.zig-cache/cuda-driver"
mkdir -p "$CUDA_CACHE_PATH"
```

Download only the files named in the pinned manifest, preserving upstream bytes:

```bash
"$SPECIALTY_PYTHON" - <<'PY'
import json
from pathlib import Path
from huggingface_hub import snapshot_download
manifest = json.loads(Path('zig/pkg/inference/testdata/gliner25/specialty/manifest.json').read_text())
for profile, pin in manifest['models'].items():
    snapshot_download(pin['repository'], revision=pin['revision'],
        local_dir=f'.specialty-artifacts/{profile}', allow_patterns=list(pin['files']))
PY
curl --fail --location \
  https://codeload.github.com/fastino-ai/GLiNER2/zip/3c913c7369301133d3b7699252074c4303ada50e \
  --output .specialty-artifacts/upstream-frozen.zip
python3 -m zipfile --extract .specialty-artifacts/upstream-frozen.zip .specialty-artifacts
export SPECIALTY_SOURCE="$PWD/.specialty-artifacts/GLiNER2-3c913c7369301133d3b7699252074c4303ada50e"
```

Restore the reviewed full captures into `.specialty-artifacts/{pii,gliguard}/oracle.json`,
or reproduce them in the pinned CPU reference environment:

```bash
for profile in pii gliguard; do
  HF_HUB_OFFLINE=1 TOKENIZERS_PARALLELISM=false "$SPECIALTY_PYTHON" \
    zig/pkg/inference/scripts/gliner25/specialty/capture_oracle.py \
    --profile "$profile" --source-root "$SPECIALTY_SOURCE" \
    --model-dir ".specialty-artifacts/$profile" \
    --output ".specialty-artifacts/$profile/oracle.json"
done
export ANTFLY_GLINER_PII_MODEL_DIR="$PWD/.specialty-artifacts/pii"
export ANTFLY_GLINER_PII_CAPTURE="$ANTFLY_GLINER_PII_MODEL_DIR/oracle.json"
export ANTFLY_GLIGUARD_MODEL_DIR="$PWD/.specialty-artifacts/gliguard"
export ANTFLY_GLIGUARD_CAPTURE="$ANTFLY_GLIGUARD_MODEL_DIR/oracle.json"
```

The tests reject any capture whose size or SHA-256 differs from the manifest.
Regeneration on a different numerical reference environment can differ; review
such differences rather than updating pins to bypass a failure. Captures retain
tokens, marker positions, intermediate head values, logits and final outputs.
Python coordinates use Unicode codepoints. `capture_decoder_oracle.py` regenerates
the small decoder fixture separately from model inference.

## Build and run regressions

```bash
cd zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build \
  -Dcuda=true -Doptimize=ReleaseFast -Dstrip=true -Dinference-compile-memory-mb=24576
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build test-bin \
  -Dcuda=true -Doptimize=ReleaseFast -Dinference-compile-memory-mb=24576 \
  -Dtest-filter=specialty -Dtest-filter='GLiNER GRU residual' -Dtest-filter='gliner span v2'
zig-out/bin/antfly-inference-tests --test-filter specialty --test-filter 'GLiNER GRU residual'
cd ../../..
python3 -m unittest discover -s zig/pkg/inference/scripts/gliner25/specialty -p 'test_*.py'
```

Model-independent tests require only the small checked-in fixtures. Full oracle
and serving tests are opt-in through the four environment variables above;
Metal tests additionally require a Metal-enabled build and host.

## HTTP, artifact and memory checks

Use the normal ReleaseFast CLI explicitly. Run hardware checks sequentially.

```bash
export SPECIALTY_CLI="$PWD/zig/pkg/inference/zig-out/bin/antfly-inference-dev"
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_http.py \
  --server "$SPECIALTY_CLI" --warmups 10 --iterations 50 \
  --output .specialty-artifacts/http-b1.json
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_http.py \
  --server "$SPECIALTY_CLI" --batch-size 8 --warmups 10 --iterations 50 \
  --output .specialty-artifacts/http-b8.json
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_managed_install.py \
  --cli "$SPECIALTY_CLI" --output .specialty-artifacts/managed-install.json
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_artifact_drift.py \
  --server "$SPECIALTY_CLI" --output .specialty-artifacts/artifact-drift.json
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_soak.py \
  --server "$SPECIALTY_CLI" --models-root .specialty-artifacts/production-managed \
  --duration 300 --concurrency 8 --max-pss-growth-mb 64 \
  --output .specialty-artifacts/soak.json
```

The HTTP gate validates all 23 frozen cases, regardless of timing selectors.
GLiGuard uses physical batches up to eight; PII remains serial within requests.
The drift gate creates private negative fixtures and removes them afterward.
Do not run it alongside a live model manager using the hard-linked originals.
The soak checks all offset units, versionless/ragged requests, public limits,
rejection recovery, idle admission, reload and host PSS plateau.

## Sanitizers and matched PyTorch benchmarks

```bash
export SPECIALTY_TESTS="$PWD/zig/pkg/inference/zig-out/bin/antfly-inference-tests"
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_cuda_sanitizers.py \
  --test-binary "$SPECIALTY_TESTS" --candidate-server "$SPECIALTY_CLI" \
  --tool memcheck --tool initcheck --output .specialty-artifacts/sanitizers-memory.json
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_cuda_sanitizers.py \
  --test-binary "$SPECIALTY_TESTS" --candidate-server "$SPECIALTY_CLI" \
  --tool racecheck --tool synccheck --kernel-regex '^termite_.*' \
  --output .specialty-artifacts/sanitizers-sync.json
for batch in 1 8; do
  for profile in pii gliguard; do
    PYTORCH_JIT=0 CUBLAS_WORKSPACE_CONFIG=:4096:8 HF_HUB_OFFLINE=1 \
      "$SPECIALTY_PYTHON" zig/pkg/inference/scripts/gliner25/specialty/benchmark_oracle.py \
      --profile "$profile" --source-root "$SPECIALTY_SOURCE" \
      --model-dir ".specialty-artifacts/$profile" --device cuda --threads 4 \
      --batch-size "$batch" --warmups 10 --iterations 50 --profile-counters \
      --output ".specialty-artifacts/pytorch-$profile-b$batch.json"
  done
done
```

Match case ID, prepared tokens, request cardinality and physical batch size when
comparing results. Both paths use FP32 with TF32 disabled. Antfly timings include
HTTP; PyTorch timings cover its in-process public API. Allocation profiling is a
separate execution; missing CUPTI transfer data is unavailable, not zero.
Sanitizer receipts distinguish unfiltered memory checks from explicitly scoped
project-kernel synchronization checks. Preserve generated receipts with the run
artifacts rather than checking machine-specific snapshots into the repository.
