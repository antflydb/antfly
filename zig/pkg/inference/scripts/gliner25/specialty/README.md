# Original PII and GLiGuard qualification tools

The public API contract and complete task schemas are in
[GLiNER2 specialty models](../../../models/gliner2/SPECIALTY.md).
Run the commands below from the repository worktree root. Keep model artifacts,
Python environments, and caches inside that worktree.

## Immutable inputs

[`testdata/gliner25/specialty/manifest.json`](../../../testdata/gliner25/specialty/manifest.json)
pins both original FP32 model revisions, every consumed sidecar, the tensor
inventory, tokenizer-derived marker IDs, the frozen upstream source, and the
captured numerical fixtures. The model revisions are:

- PII: `fastino/gliner2-privacy-filter-PII-multi@1cb4166094dc58fa8d836429f060d6c95f62b495`.
- GLiGuard: `fastino/gliguard-LLMGuardrails-300M@7776816b54581f37b90dd2de12dbe13123229a9b`.

This download example preserves upstream bytes and checks size and SHA-256
before publishing each file into the local model directory. It reuses an
already verified file. No tokenizer or config conversion is required.

```bash
python3 - <<'PY'
import hashlib
import json
from pathlib import Path
import urllib.request

root = Path('.specialty-artifacts')
manifest = json.loads(Path('zig/pkg/inference/testdata/gliner25/specialty/manifest.json').read_text())

def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 << 20), b''):
            digest.update(block)
    return digest.hexdigest()

for profile, model in manifest['models'].items():
    for filename, pin in model['files'].items():
        target = root / profile / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and target.stat().st_size == pin['size_bytes'] and sha256(target) == pin['sha256']:
            continue
        pending = target.with_name(target.name + '.pending')
        url = f"https://huggingface.co/{model['repository']}/resolve/{model['revision']}/{filename}"
        with urllib.request.urlopen(url) as source, pending.open('wb') as output:
            for block in iter(lambda: source.read(8 << 20), b''):
                output.write(block)
        assert pending.stat().st_size == pin['size_bytes'], filename
        assert sha256(pending) == pin['sha256'], filename
        pending.replace(target)
PY
```

Fetch the frozen GLiNER2 2.0.0 source into the worktree and install the existing
oracle dependency lock. The capture script also verifies the complete upstream
Python source tree hash, import location, package version, and dependency
versions. A changed source checkout cannot regenerate a qualified capture.

```bash
mkdir -p .specialty-artifacts
curl --fail --location https://codeload.github.com/fastino-ai/GLiNER2/zip/3c913c7369301133d3b7699252074c4303ada50e \
  --output .specialty-artifacts/upstream-frozen.zip
python3 -m zipfile --extract .specialty-artifacts/upstream-frozen.zip .specialty-artifacts
uv venv .specialty-artifacts/oracle-venv --python 3.12
UV_CACHE_DIR="$PWD/.specialty-artifacts/uv-cache" uv pip install \
  --python .specialty-artifacts/oracle-venv/bin/python \
  --requirement zig/pkg/inference/scripts/gliner25/requirements.txt
```

## Capture and audit

Capture CPU FP32 reference outputs with the frozen dependency environment.
Both profiles use at most 512 prepared tokens, including schema tokens. The
fixtures contain model-facing tokens, marker positions, intermediate head
values, logits, and selected outputs. Python offsets use Unicode codepoints.
The seven PII languages, all 42 PII labels, all six GLiGuard tasks, composed
tasks, repeated Unicode text, overlap, empty output, and fallback cases are
included. Synthetic decoder and GRU tests cover threshold equality, ties,
count-zero gating, and cancellation-sensitive residual arithmetic separately.

```bash
for profile in pii gliguard; do
  PYTHONDONTWRITEBYTECODE=1 HF_HUB_OFFLINE=1 TOKENIZERS_PARALLELISM=false \
    .specialty-artifacts/oracle-venv/bin/python \
    zig/pkg/inference/scripts/gliner25/specialty/capture_oracle.py \
    --profile "$profile" \
    --source-root .specialty-artifacts/GLiNER2-3c913c7369301133d3b7699252074c4303ada50e \
    --model-dir ".specialty-artifacts/$profile" \
    --output ".specialty-artifacts/$profile/oracle.json"
done
```

`capture_decoder_oracle.py --source-root SOURCE --output OUTPUT` records
additional model-independent edge cases by executing the pinned upstream
runtime: omitted greedy versus explicit weighted overlap, stable confidence
ties, inclusive threshold equality, Unicode/repeated coordinates, cross-label
overlap, count-zero suppression, and unchanged mention counts for positive
count predictions of one, two, and nineteen. Its frozen output is
`testdata/gliner25/specialty/decoder_oracle.json`, bound by the same manifest.

Treat a regenerated fixture as new evidence: review numerical changes and
record its size and SHA-256 in the manifest and Zig consumer before using it
for qualification. The checked-in fixtures and their pins remain the frozen
acceptance reference.

The separate current-upstream audit compares preprocessing in isolated Python
processes. Download an exact current commit into a separate source directory;
pass its full commit SHA rather than an unversioned `main` name. This audit
never replaces the frozen numerical reference.

```bash
.specialty-artifacts/oracle-venv/bin/python \
  zig/pkg/inference/scripts/gliner25/specialty/audit_preprocessing.py \
  --profile pii --model-dir .specialty-artifacts/pii \
  --frozen-source .specialty-artifacts/GLiNER2-3c913c7369301133d3b7699252074c4303ada50e \
  --current-source .specialty-artifacts/GLiNER2-CURRENT_FULL_COMMIT \
  --current-commit CURRENT_FULL_COMMIT \
  --output .specialty-artifacts/pii/preprocessing-audit.json
```

Repeat with `--profile gliguard` and its model directory. Checked-in audit
receipts identify the exact current commit used for the initial comparison.

## Native and GPU gates

The real-model tests use these environment variables:

```bash
export ANTFLY_GLINER_PII_MODEL_DIR="$PWD/.specialty-artifacts/pii"
export ANTFLY_GLIGUARD_MODEL_DIR="$PWD/.specialty-artifacts/gliguard"
export ANTFLY_GLINER_PII_CAPTURE="$PWD/zig/pkg/inference/testdata/gliner25/specialty/pii/oracle.json"
export ANTFLY_GLIGUARD_CAPTURE="$PWD/zig/pkg/inference/testdata/gliner25/specialty/gliguard/oracle.json"
```

Run the focused inference gate from `zig/pkg/inference` with a worktree-local
cache. `-Dtest-filter=specialty` selects tests during compilation; a filter
passed only after `--` still compiles the entire suite. Use one hardware gate
at a time.

```bash
cd zig/pkg/inference
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
  zig build test-inference-unit -Doptimize=ReleaseFast -Dcuda=true -Dmetal=false \
  -Dtest-filter=specialty \
  > ../../../.specialty-artifacts/specialty-cuda-tests.log 2>&1
```

On Apple hardware, select `-Dcuda=false -Dmetal=true`. For a CPU
run, disable both GPUs. GPU tests require the actual backend; their execution
receipts must distinguish skipped tests from hardware passes. Public HTTP
qualification additionally records binary identity, backend configuration,
responses, GPU dispatch, transfer counts, allocation counts, and memory
reservation/peak receipts.

## Production HTTP gate

Build the standalone server from `zig/pkg/inference` with the same CUDA
artifact mode as the numerical gate:

```bash
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
  zig build http-server -Dcuda=true -Doptimize=ReleaseFast -Dstrip=true
```

Return to the worktree root and run the production gate. It owns a loopback
server, requires both loaded instances to use CUDA, validates all frozen cases,
records separate warm HTTP timings, and shuts the server down. The model
directories must also contain their frozen `oracle.json` captures.

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_http.py \
  --output .specialty-artifacts/production-http.json
```

Use `--batch-size 8 --case prompt_safety --case composed_all_six` for matched
GLiGuard batch timings. Validation always covers all 23 cases. The report
includes the executable SHA-256, model and CUDA artifact pins, backend settings,
responses and confidence errors. Public HTTP does not expose device transfer
or allocation counters; the separate Zig service receipts provide those.

Run this after the numerical/service gates and without other GPU work. The
companion server log remains next to the report. Use `--server` and
`--models-root` to select other local executable and artifact paths.

## Matched upstream benchmarks

`benchmark_oracle.py` loads a fresh model and installs no capture hooks. It
uses the same case IDs, text, schemas, and thresholds as the frozen fixtures.
Warmups and repeated timing trials exclude model load. Allocation and transfer
profiling uses a separate request, so profiler overhead does not enter the
reported latency. CPU allocation counters describe PyTorch allocator events;
Python heap allocations are outside that counter. Cumulative allocated bytes
are distinct from peak live memory.

```bash
.specialty-artifacts/oracle-venv/bin/python \
  zig/pkg/inference/scripts/gliner25/specialty/benchmark_oracle.py \
  --profile pii --model-dir .specialty-artifacts/pii \
  --source-root .specialty-artifacts/GLiNER2-3c913c7369301133d3b7699252074c4303ada50e \
  --device cpu --threads 4 --warmups 2 --iterations 10 --batch-size 1 \
  --case language_en --case all_42_labels --profile-counters \
  --output .specialty-artifacts/pii/pytorch-cpu-benchmark.json
```

For GLiGuard, use cases `prompt_safety` and `composed_all_six`, then repeat with
`--batch-size 8` for physical batching. PII always uses physical batches of
one, including inside a larger public request. Run `--device cuda` or
`--device mps` only after the corresponding hardware correctness gate and
without concurrent GPU qualification jobs. Compare matching case IDs and
request item counts against the public HTTP measurements.

If a hardware-compatible PyTorch build differs from the frozen CPU oracle
lock, the benchmark records every package version and
`matches_frozen_oracle_runtime: false`. Such performance evidence must be
reported separately; it does not replace pinned numerical qualification.
Missing CUDA profiler transfer events are reported as unavailable rather
than as measured zero transfers.

On GB10, the pinned PyTorch 2.9.1 CUDA 12.8 runtime needs `PYTORCH_JIT=0` for
this benchmark: its NVRTC cannot compile TorchScript fusion for SM121. This
uses the original Python functions in eager mode and preserves the frozen
package versions. The benchmark records this setting and explicitly disables
TF32. CUPTI from that runtime cannot profile GB10 transfers; those counters
remain unavailable while CUDA allocator counts and peaks are recorded.

Run model-independent fixture and counter checks with:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s zig/pkg/inference/scripts/gliner25/specialty -p 'test_*.py' -v
```

## Reproduce the GB10 production campaign

Run these commands from the worktree root after preparing the pinned artifacts
and oracle captures above. Freeze the executable and source inputs before the
hardware campaign, and run GPU gates sequentially on an otherwise idle device.
Each command writes its own JSON receipt and companion logs; the commands do
not imply that a particular checkout has passed qualification.

Build the normal CLI, the HTTP-only entry point, and an installed test binary
with runtime filters. The normal CLI supports both official `pull` and the
production `run` command; its installed name is `antfly-inference-dev`.
All targets use the same checked-in fatbin and worktree-local caches.

```bash
mkdir -p .specialty-artifacts
(
  set -e
  cd zig/pkg/inference
  ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
    zig build -Dcuda=true -Dmetal=false -Dcuda-artifacts=fatbin \
    -Doptimize=ReleaseFast -Dstrip=true -Dinference-compile-memory-mb=24576 \
    > ../../../.specialty-artifacts/build-cli.log 2>&1
  ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
    zig build http-server -Dcuda=true -Dmetal=false -Dcuda-artifacts=fatbin \
    -Doptimize=ReleaseFast -Dstrip=true \
    > ../../../.specialty-artifacts/build-http.log 2>&1
  ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" \
    zig build test-bin -Dcuda=true -Dmetal=false -Dcuda-artifacts=fatbin \
    -Doptimize=ReleaseFast \
    -Dtest-filter=specialty -Dtest-filter='GLiNER GRU residual' \
    -Dtest-filter='gliner span v2' -Dtest-filter='gliner boundary' \
    -Dtest-filter='GLiNER contextual' -Dtest-filter='GLiNER complete batched head' \
    -Dtest-filter='GLiNER default revisions' -Dtest-filter='GLiNER span backend' \
    -Dtest-filter='extraction v2' \
    > ../../../.specialty-artifacts/build-tests.log 2>&1
)
```

The recipes below exercise the normal CLI as the serving executable. To repeat
against the narrower HTTP-only entry point, replace its `--server` path with
`zig/pkg/inference/zig-out/bin/antfly-inference-http-server` and retain separate
receipts, since these are distinct binaries.

### Final HTTP benchmark receipts

Use the normal CLI for both final reports. Each run validates all 23 frozen
cases; the second run times the two matched GLiGuard workloads at batch eight.

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_http.py \
  --server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --output .specialty-artifacts/production-final-http.json
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_http.py \
  --server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --batch-size 8 --case prompt_safety --case composed_all_six \
  --output .specialty-artifacts/production-final-http-batch8.json
```

### Official managed installation and artifact rejection

The managed-install gate invokes the official CLI against canonical Hub names.
It seeds only verified upstream cache bytes, then checks immutable revision
resolution, generated capabilities and limits, transactional publication,
repeat pulls, and canonical listing. It uses no GPU. Hub metadata access is
required even when every large payload is already cached.

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_managed_install.py \
  --cli zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --artifacts-root .specialty-artifacts \
  --models-root .specialty-artifacts/production-managed \
  --repetitions 2 \
  --output .specialty-artifacts/production-final-managed-install.json \
  > .specialty-artifacts/managed-install.log 2>&1
```

The artifact-drift gate uses native CPU serving and disposable clones. It
checks explicit rejection through instance loading and cold extraction, valid
control recovery, and preservation of the original files. Its clone directory
must not exist beforehand and is removed on success or failure; place the
report outside that directory.

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_artifact_drift.py \
  --server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --artifacts-root .specialty-artifacts \
  --models-root .specialty-artifacts/production-drift-models \
  --output .specialty-artifacts/production-final-artifact-drift.json \
  > .specialty-artifacts/artifact-drift.log 2>&1
```

### Managed serving soak and lifecycle

The soak uses the canonical managed installation from the previous step. It
loads both CUDA instances on demand, validates every frozen case in all three
public coordinate units, exercises chunked and maximum requests, checks
unsupported tasks and oversized/long-document rejection, then unloads and
reloads each instance. Readiness describes the installed model inventory and
is checked independently from the loaded-runtime list.

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_soak.py \
  --server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --models-root .specialty-artifacts/production-managed \
  --artifacts-root .specialty-artifacts \
  --duration 300 --concurrency 8 --max-pss-growth-mb 64 \
  --output .specialty-artifacts/production-final-soak.json \
  > .specialty-artifacts/production-final-soak.log 2>&1
```

The 64 MiB bound applies to growth between two late-window median Linux
process-group PSS measurements after shape warmup. It is not an absolute RAM
limit or a GPU-private memory measurement. Public admission ledgers must return
to their idle values. CUDA owned-buffer peaks and reservations are checked
separately by the Zig service gates.

### Conflicting attention diagnostics

These four runs intentionally request global diagnostic modes that specialty
profiles must ignore. Every run validates all frozen outputs. The production
log must identify both profiles with `weights=f32 attention=f32` and
`diagnostic_overrides=ignored` before extraction and again at completion.
The report records requested diagnostics separately from the observed profile
policy, including inherited CUDA visibility and arithmetic controls.

```bash
(
  set -e
  for mode in auto streaming-f16 materialized-f16 generated-tc; do
    python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_http.py \
    --server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
    --models-root .specialty-artifacts \
    --attention-mode "$mode" --generated-tc-auto 1 \
    --warmups 2 --iterations 5 \
    --case language_en --case composed_all_six \
    --output ".specialty-artifacts/production-final-policy-$mode.json" \
    > ".specialty-artifacts/production-final-policy-$mode.log" 2>&1
  done
)
```

The ordinary run defaults to `--attention-mode f32 --generated-tc-auto 0`.
Policy markers are required for ordinary runs too. The HTTP gate also binds
the candidate SHA to the live Linux server leader through `/proc/PID/exe`
before and after requests. `--case` limits timing
workloads only; it never reduces the 23-case HTTP validation phase.

### CUDA sanitizer lanes

The sanitizer runner consumes the installed runtime-filtering test executable
and performs no build. Its default is all four tools—memcheck, initcheck,
racecheck, and synccheck—against both GRU residual modes and all 12 PII plus
11 GLiGuard head fixtures. Each selected test must execute and pass with zero
skips. Tool summaries, full memcheck leak summaries, and explicit head case
receipts are required; an exit code of zero alone is insufficient.

Keep memcheck and initcheck unfiltered in their own invocation:

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_cuda_sanitizers.py \
  --test-binary zig/pkg/inference/zig-out/bin/antfly-inference-tests \
  --candidate-server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --models-root .specialty-artifacts \
  --sanitizer /usr/local/cuda/bin/compute-sanitizer \
  --tool memcheck --tool initcheck --timeout 1800 \
  --output .specialty-artifacts/production-final-sanitizers-memory.json \
  > .specialty-artifacts/production-final-sanitizers-memory.log 2>&1
```

An explicit project-kernel scope can be used for racecheck and synccheck when
instrumenting vendor library kernels exceeds the campaign's resource envelope.
This still executes every head fixture and both GRU residual modes. The
`^termite_.*` filter covers the named project kernels on these serving paths;
unmatched cuBLAS/vendor kernels are excluded from these two tools.

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/qualify_cuda_sanitizers.py \
  --test-binary zig/pkg/inference/zig-out/bin/antfly-inference-tests \
  --candidate-server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --models-root .specialty-artifacts \
  --sanitizer /usr/local/cuda/bin/compute-sanitizer \
  --tool racecheck --tool synccheck --kernel-regex '^termite_.*' --timeout 1800 \
  --output .specialty-artifacts/production-final-sanitizers-sync.json \
  > .specialty-artifacts/production-final-sanitizers-sync.log 2>&1
```

Kernel filtering is never enabled implicitly. The runner emits the installed
tool's `--kernel-name regex=PATTERN` syntax and rejects invalid patterns or
patterns that exclude required project entry points for any selected suite.
It verifies those names in the pinned PTX entry inventory and the test binary,
then still requires CUDA initialization, successful CUDA-only tests, and exact
head case receipts. Explicit checked/instrumented-kernel messages are retained
when the tool emits them; a missing runtime kernel listing is reported as
unavailable, not as zero checks.

Filtered reports identify their regex and vendor exclusion explicitly, and
never claim all-kernel coverage or a complete unfiltered sanitizer campaign.
The two commands above provide separate tool/scope receipts; retain that
distinction when reporting results.

To include the complete public loopback HTTP service tests, including maximum
admitted request geometry, repeat each invocation with `--include-http` and a
separate output path, retaining its explicit tool and kernel scope. Those
instrumented requests can take substantially longer. Alternatively, stage just
the HTTP lanes with both `--filter pii-http` and `--filter gliguard-http`. Omitting
`--tool` requests all four tools; omitting `--kernel-regex` keeps all kernels
eligible for instrumentation.

`--tool` and `--filter` are repeatable for staged diagnostics. `--case` narrows
only head fixtures and records the exact selected IDs; HTTP coverage is
unaffected. Reduced runs remain explicitly partial in their receipts.
`--candidate-server` fingerprints the associated production executable but
does not execute it under the sanitizer. The selected test binary exercises
the shared production implementations; use the production HTTP and soak gates
to qualify the normal CLI or HTTP-only executable itself.

Sanitizer receipts bind the observed source fingerprint, test binary,
associated candidate, original model/oracle pins, CUDA artifacts, tool version,
and injection libraries. Source and executable hashes are checked again at
completion. This identifies the observed inputs; it does not attest a
reproducible compilation. Keep sources and binaries unchanged while a receipt
is being collected.

The GPU runners own a Linux process group shared by the server supervisor and
workers. Cleanup waits for all live members, not just the initial process.
If SIGKILL is required, the runner drains the group and fails qualification;
a surviving live PID is reported explicitly. Zombie entries have already
released CUDA contexts and mappings and are outside the live-process/PSS scope.

## Assemble the production receipt

After collecting the final receipts, assemble a compact checked-in report. The
assembler rechecks their binary, source, model/oracle and CUDA artifact bindings,
coverage, numeric gates, memory limits and raw sanitizer logs. Missing or failed
gates fail assembly. Build, CUDA-freshness, and frozen-source companion receipts
also retain each command, exit status, flags and log hashes; their schemas and
values are preserved in the checked-in production report. Collect these alongside
the individual runtime gates. Use the actual compiled test executable path in
`--test-binary`.

```bash
python3 zig/pkg/inference/scripts/gliner25/specialty/summarize_qualification.py \
  --artifacts-root .specialty-artifacts \
  --test-binary zig/pkg/inference/zig-out/bin/antfly-inference-tests \
  --server zig/pkg/inference/zig-out/bin/antfly-inference-dev \
  --regression-log zig/pkg/inference/.specialty-production-final-regressions.log \
  --output zig/pkg/inference/testdata/gliner25/specialty/gb10-production-qualification.json
```

The default regression gate requires zero failures. This GB10 campaign also
records the existing, documented default-native-Q8 Decide failure. Reproducing
that exact broader suite requires
`--baseline-exception .specialty-artifacts/production-final-native-q8-baseline.json`.
The assembler verifies its historical log, unchanged documentation and native
implementation, exact test identity and numerical error, and pinned Q8 model.
It permits only that one out-of-scope baseline; leaks, additional failures or
missing FP32 specialty coverage remain failures. The report preserves the raw
failed regression result and explicitly marks native Q8 unqualified.
