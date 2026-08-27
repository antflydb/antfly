#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/gliner2/run_gliner2_lora_production_readiness.sh [options]

Runs the strict GLiNER2 LoRA batch-32/seq-128 cross-runtime release-parity gate,
a deterministic held-out full-task quality gate through the frozen upstream
decoder, a native full-heldout GLiNER2 quality gate, and an opt-in head-MLP
fusion guard. A separately generated five-seed convergence summary is required
so independent-training equality is judged statistically rather than tensor by
tensor.

Options:
  --runs N                 Production gate repeated runs (default: 5)
  --head-runs N            Opt-in head fusion guard runs (default: 1)
  --out-dir DIR            Output directory (default: /private/tmp/termite-gliner2-production-readiness)
  --model-dir DIR          Base model for parity and release-adapter quality (required)
  --release-adapter-dir DIR
                           Fully trained Zig PEFT adapter scored on heldout data (required)
  --python-model PATH      Python model path/id forwarded to the perf gate
  --train-data FILE        Representative training JSONL (required)
  --eval-data FILE         Disjoint full-task eval JSONL (required and scored)
  --python-bin FILE        Python executable forwarded to the perf gate
  --upstream-source DIR    Clean upstream GLiNER2 checkout at the pinned commit (required)
  --zig-backend metal|cuda Accelerator under test (default: metal)
  --zig-cuda-artifacts NAME
                           portable, sm89, or fatbin (default: fatbin)
  --convergence-summary FILE
                           Passing output from summarize_gliner2_convergence.py (required)
  --cuda-hardware-qualification FILE
                           Current passing L4/A100/H100 FP32 qualification matrix (required for CUDA)
  --heldout-min KEY=FLOAT  Required held-out floor; repeat for every metric listed below
  --heldout-threshold N    Upstream extraction threshold (default: 0.5)
  --heldout-batch-size N   Upstream CPU evaluation batch size (default: 8)
  --heldout-max-length N   Optional maximum word-token length
  --compare-steps N        Steps per comparison run forwarded to the perf gate
  --warm-production-ready  Use strict warm-step production target defaults
  --loop-profile           Enable executor loop-profile timing summaries
  --hazard-profile         Enable planned-access hazard timing summaries
  --skip-head-opt-in       Skip the experimental head-MLP fusion guard
  --require-head-opt-in    Fail if the experimental head-MLP fusion guard fails
  --help                   Show this help

Required --heldout-min keys:
  entities.micro_f1, entities.exact_match,
  classifications.micro_f1, classifications.exact_match,
  json_structures.micro_f1, json_structures.exact_match,
  relations.micro_f1, relations.exact_match, count.accuracy
EOF
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

runs=5
head_runs=1
out_dir="/private/tmp/termite-gliner2-production-readiness"
skip_head_opt_in=0
require_head_opt_in=0
train_data_set=0
eval_data_set=0
train_data=""
eval_data=""
model_dir=""
release_adapter_dir=""
python_bin="/private/tmp/gliner2-parity-venv/bin/python"
upstream_source=""
zig_backend="metal"
zig_cuda_artifacts="fatbin"
convergence_summary=""
cuda_hardware_qualification=""
heldout_threshold="0.5"
heldout_batch_size="8"
heldout_max_length=""
heldout_minima=()
gate_args=()
extra_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      runs="${2:?missing value for --runs}"
      shift 2
      ;;
    --head-runs)
      head_runs="${2:?missing value for --head-runs}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:?missing value for --out-dir}"
      shift 2
      ;;
    --model-dir)
      model_dir="${2:?missing value for --model-dir}"
      gate_args+=("$1" "${model_dir}")
      shift 2
      ;;
    --release-adapter-dir)
      release_adapter_dir="${2:?missing value for --release-adapter-dir}"
      shift 2
      ;;
    --python-model)
      gate_args+=("$1" "${2:?missing value for $1}")
      shift 2
      ;;
    --python-bin)
      python_bin="${2:?missing value for --python-bin}"
      gate_args+=("$1" "${python_bin}")
      shift 2
      ;;
    --train-data)
      train_data_set=1
      train_data="${2:?missing value for --train-data}"
      gate_args+=("$1" "${train_data}")
      shift 2
      ;;
    --eval-data)
      eval_data_set=1
      eval_data="${2:?missing value for --eval-data}"
      gate_args+=("$1" "${eval_data}")
      shift 2
      ;;
    --upstream-source)
      upstream_source="${2:?missing value for --upstream-source}"
      gate_args+=("$1" "${upstream_source}")
      shift 2
      ;;
    --zig-backend)
      zig_backend="${2:?missing value for --zig-backend}"
      if [[ "${zig_backend}" != "metal" && "${zig_backend}" != "cuda" ]]; then
        echo "--zig-backend must be metal or cuda" >&2
        exit 2
      fi
      gate_args+=("--zig-backend" "${zig_backend}")
      shift 2
      ;;
    --zig-cuda-artifacts)
      zig_cuda_artifacts="${2:?missing value for --zig-cuda-artifacts}"
      if [[ "${zig_cuda_artifacts}" != "portable" && "${zig_cuda_artifacts}" != "sm89" && "${zig_cuda_artifacts}" != "fatbin" ]]; then
        echo "--zig-cuda-artifacts must be portable, sm89, or fatbin" >&2
        exit 2
      fi
      gate_args+=("--zig-cuda-artifacts" "${zig_cuda_artifacts}")
      shift 2
      ;;
    --convergence-summary)
      convergence_summary="${2:?missing value for --convergence-summary}"
      shift 2
      ;;
    --cuda-hardware-qualification)
      cuda_hardware_qualification="${2:?missing value for --cuda-hardware-qualification}"
      shift 2
      ;;
    --heldout-min)
      heldout_minima+=("${2:?missing KEY=FLOAT for --heldout-min}")
      shift 2
      ;;
    --heldout-threshold)
      heldout_threshold="${2:?missing value for --heldout-threshold}"
      shift 2
      ;;
    --heldout-batch-size)
      heldout_batch_size="${2:?missing value for --heldout-batch-size}"
      shift 2
      ;;
    --heldout-max-length)
      heldout_max_length="${2:?missing value for --heldout-max-length}"
      shift 2
      ;;
    --compare-steps)
      gate_args+=("$1" "${2:?missing value for $1}")
      shift 2
      ;;
    --warm-production-ready)
      gate_args+=("$1")
      shift
      ;;
    --loop-profile | --hazard-profile)
      gate_args+=("$1")
      shift
      ;;
    --skip-head-opt-in)
      skip_head_opt_in=1
      shift
      ;;
    --require-head-opt-in)
      require_head_opt_in=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    --)
      shift
      extra_args+=("$@")
      break
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

default_dir="${out_dir}/default"
head_dir="${out_dir}/head-mlp-opt-in"
quality_report="${out_dir}/heldout_quality.json"
native_release_report="${out_dir}/native_full_task_quality.json"
readiness_report="${out_dir}/readiness_summary.json"
mkdir -p "${out_dir}"
rm -f \
  "${readiness_report}" \
  "${quality_report}" \
  "${native_release_report}" \
  "${default_dir}/perf_summary.json" \
  "${head_dir}/perf_summary.json"

if ((${#extra_args[@]})); then
  echo "release readiness rejects comparison arguments after --; profile and tolerance overrides are not allowed" >&2
  exit 2
fi
if (( skip_head_opt_in && require_head_opt_in )); then
  echo "--skip-head-opt-in and --require-head-opt-in are mutually exclusive" >&2
  exit 2
fi
if [[ "${zig_backend}" == "cuda" ]]; then
  if (( require_head_opt_in )); then
    echo "--require-head-opt-in is Metal-only and cannot be used with --zig-backend cuda" >&2
    exit 2
  fi
  skip_head_opt_in=1
  if [[ -z "${cuda_hardware_qualification}" || ! -f "${cuda_hardware_qualification}" ]]; then
    echo "--cuda-hardware-qualification must name a current L4/A100/H100 matrix for CUDA readiness" >&2
    exit 2
  fi
fi
if [[ ! "${runs}" =~ ^[0-9]+$ ]] || (( runs < 5 )); then
  echo "production readiness requires at least five performance runs" >&2
  exit 2
fi
if [[ ! "${head_runs}" =~ ^[0-9]+$ ]] || (( head_runs < 1 )); then
  echo "--head-runs must be positive" >&2
  exit 2
fi

if (( ! train_data_set || ! eval_data_set )); then
  echo "--train-data and --eval-data are required for release-parity validation" >&2
  exit 2
fi
if [[ -z "${upstream_source}" ]]; then
  echo "--upstream-source is required for frozen upstream held-out evaluation" >&2
  exit 2
fi
if [[ -z "${convergence_summary}" || ! -f "${convergence_summary}" ]]; then
  echo "--convergence-summary must name a current five-seed convergence report" >&2
  exit 2
fi
if [[ "${zig_backend}" == "cuda" ]]; then
  PYTHONPATH="${script_dir}" "${python_bin}" - "${cuda_hardware_qualification}" "${script_dir}/../.." <<'PY'
import json
import sys
from pathlib import Path
from summarize_gliner2_cuda_hardware import verify_summary

path = Path(sys.argv[1]).resolve()
report = json.loads(path.read_text(encoding="utf-8"))
errors = verify_summary(report, Path(sys.argv[2]).resolve())
if errors:
    raise SystemExit("; ".join(errors))
PY
fi
if [[ -z "${model_dir}" || -z "${release_adapter_dir}" ]]; then
  echo "--model-dir and --release-adapter-dir are required for release quality validation" >&2
  exit 2
fi
if [[ ! -d "${model_dir}" || ! -f "${release_adapter_dir}/adapter_config.json" || ! -f "${release_adapter_dir}/adapter_model.safetensors" || ! -f "${release_adapter_dir}/task_head.safetensors" || ! -f "${release_adapter_dir}/training_manifest.json" ]]; then
  echo "release model, standard PEFT adapter, or Zig training manifest is missing" >&2
  exit 2
fi
"${python_bin}" "${script_dir}/validate_gliner2_release_data.py" \
  --train "${train_data}" --eval "${eval_data}" --require-full-task \
  --model-dir "${model_dir}" --release-adapter-dir "${release_adapter_dir}" \
  --backend "${zig_backend}"
heldout_preflight=("${upstream_source}" "${heldout_threshold}" "${heldout_batch_size}" "${heldout_max_length}")
if ((${#heldout_minima[@]})); then
  heldout_preflight+=("${heldout_minima[@]}")
fi
PYTHONPATH="${script_dir}" "${python_bin}" - "${heldout_preflight[@]}" <<'PY'
import sys
from pathlib import Path
from evaluate_gliner2_full_task import UPSTREAM_COMMIT, parse_minima, verify_upstream

try:
    verify_upstream(Path(sys.argv[1]).resolve(), UPSTREAM_COMMIT)
    threshold = float(sys.argv[2])
    batch_size = int(sys.argv[3])
    max_length = int(sys.argv[4]) if sys.argv[4] else None
    if not 0 < threshold <= 1 or not 1 <= batch_size <= 256 or (max_length is not None and max_length < 1):
        raise ValueError("invalid held-out threshold, batch size, or max length")
    parse_minima(sys.argv[5:])
except ValueError as exc:
    raise SystemExit(str(exc))
PY
PYTHONPATH="${script_dir}" "${python_bin}" - "${convergence_summary}" "${model_dir}" "${train_data}" "${eval_data}" "${upstream_source}" "${zig_backend}" <<'PY'
import json
import sys
from pathlib import Path

from gliner2_release_contract import (
    CANONICAL_GLINER2_VERSION,
    CANONICAL_NORMALIZATION,
    CANONICAL_ORACLE_PACKAGE_VERSIONS,
    CANONICAL_UNICODE_VERSION,
    UPSTREAM_COMMIT,
    path_fingerprint,
)
from summarize_gliner2_convergence import (
    EVIDENCE_CONTRACT,
    OUTPUT_CONTRACT,
    STOCK_STOCHASTIC_TRAINING_POLICY,
    verify_summary_evidence,
)
from validate_gliner2_release_data import base_model_fingerprint

report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
expected_fingerprints = {
    "base_model": base_model_fingerprint(Path(sys.argv[2])),
    "train_data": path_fingerprint(Path(sys.argv[3])),
    "eval_data": path_fingerprint(Path(sys.argv[4])),
}
if (
    not isinstance(report, dict)
    or report.get("contract") != OUTPUT_CONTRACT
    or report.get("evidence_contract") != EVIDENCE_CONTRACT
    or report.get("evidence_bound") is not True
    or report.get("pass") is not True
    or report.get("seed_count") != 5
    or report.get("oracle", {}).get("commit") != UPSTREAM_COMMIT
    or report.get("oracle", {}).get("checkout") != str(Path(sys.argv[5]).resolve())
    or report.get("oracle", {}).get("gliner2_version") != CANONICAL_GLINER2_VERSION
    or report.get("oracle", {}).get("package_versions") != CANONICAL_ORACLE_PACKAGE_VERSIONS
    or report.get("normalization") != CANONICAL_NORMALIZATION
    or report.get("unicode_version") != CANONICAL_UNICODE_VERSION
    or report.get("training_policy") != STOCK_STOCHASTIC_TRAINING_POLICY
    or report.get("accelerator_backend", "metal") != sys.argv[6]
    or report.get("fingerprints") != expected_fingerprints
):
    raise SystemExit("convergence summary is not a passing five-seed report for the selected model/train/eval artifacts")
evidence_errors = verify_summary_evidence(report)
if evidence_errors:
    raise SystemExit("convergence evidence is stale or invalid: " + "; ".join(evidence_errors))
PY

native_min_f1=""
native_min_exact_match=""
if ((${#heldout_minima[@]})); then
  for minimum in "${heldout_minima[@]}"; do
    case "${minimum}" in
      entities.micro_f1=*) native_min_f1="${minimum#*=}" ;;
      entities.exact_match=*) native_min_exact_match="${minimum#*=}" ;;
    esac
  done
fi
if [[ -z "${native_min_f1}" || -z "${native_min_exact_match}" ]]; then
  echo "native quality requires entities.micro_f1 and entities.exact_match minima" >&2
  exit 2
fi

default_rc=0
head_rc=0
quality_rc=1
native_release_rc=1

default_cmd=(
  "${script_dir}/run_gliner2_lora_perf_gate.sh"
  "--warm-production-ready"
  "--runs" "${runs}"
  "--out-dir" "${default_dir}"
  "--max-zig-python-warm-step-ratio-median" "1.0"
  "--max-zig-python-warm-step-ratio-any-run" "1.10"
)
head_cmd=(
  "${script_dir}/run_gliner2_lora_perf_gate.sh"
  "--production-batch32"
  "--op-stats"
  "--runs" "${head_runs}"
  "--out-dir" "${head_dir}"
  "--max-zig-metal-peak-live-bytes-median" "3221225472"
  "--max-zig-metal-planned-barriers-median" "40"
  "--max-zig-metal-planned-scopes-median" "55"
  "--max-command-dispatch-median" "6250"
  "--max-fallback-median" "0"
  "--max-true-host-output-median" "0"
  "--min-zig-head-mlp-forward-region-median" "1"
)
if ((${#gate_args[@]})); then
  default_cmd+=("${gate_args[@]}")
  head_cmd+=("${gate_args[@]}")
fi
set +e
"${default_cmd[@]}"
default_rc=$?

if (( default_rc == 0 )); then
  quality_cmd=(
    "${python_bin}" "${script_dir}/evaluate_gliner2_full_task.py"
    "--model-dir" "${model_dir}"
    "--adapter-dir" "${release_adapter_dir}"
    "--eval-data" "${eval_data}"
    "--upstream-source" "${upstream_source}"
    "--output" "${quality_report}"
    "--threshold" "${heldout_threshold}"
    "--batch-size" "${heldout_batch_size}"
  )
  if ((${#heldout_minima[@]})); then
    for minimum in "${heldout_minima[@]}"; do
      quality_cmd+=("--min-metric" "${minimum}")
    done
  fi
  if [[ -n "${heldout_max_length}" ]]; then
    quality_cmd+=("--max-length" "${heldout_max_length}")
  fi
  "${quality_cmd[@]}"
  quality_rc=$?
fi

if (( quality_rc == 0 )); then
  native_quality_cmd=(
    "${python_bin}" "${script_dir}/evaluate_gliner2_native_release_smoke.py"
    --model-dir "${model_dir}" \
    --adapter-dir "${release_adapter_dir}" \
    --eval-data "${eval_data}" \
    --threshold "${heldout_threshold}" \
    --min-f1 "${native_min_f1}" \
    --min-exact-match "${native_min_exact_match}" \
    --output "${native_release_report}"
  )
  if ((${#heldout_minima[@]})); then
    for minimum in "${heldout_minima[@]}"; do
      case "${minimum}" in
        entities.micro_f1=* | entities.exact_match=*) ;;
        *) native_quality_cmd+=(--min-task-metric "${minimum}") ;;
      esac
    done
  fi
  "${native_quality_cmd[@]}"
  native_release_rc=$?
fi

if (( skip_head_opt_in )); then
  head_rc=0
else
  TERMITE_METAL_ENABLE_HEAD_MLP_FORWARD_RUNTIME_REGION=1 "${head_cmd[@]}"
  head_rc=$?
fi
set -e

finalize_cmd=(
  "${python_bin}" "${script_dir}/finalize_gliner2_readiness.py"
  --out-dir "${out_dir}"
  --convergence-summary "${convergence_summary}"
  --release-adapter-dir "${release_adapter_dir}"
  --backend "${zig_backend}"
  --default-rc "${default_rc}"
  --head-rc "${head_rc}"
  --quality-rc "${quality_rc}"
  --native-rc "${native_release_rc}"
  --skip-head-opt-in "${skip_head_opt_in}"
  --require-head-opt-in "${require_head_opt_in}"
)
if [[ "${zig_backend}" == "cuda" ]]; then
  finalize_cmd+=(--hardware-qualification "${cuda_hardware_qualification}")
fi
"${finalize_cmd[@]}"

if (( default_rc != 0 )); then
  exit "${default_rc}"
fi
if (( quality_rc != 0 )); then
  exit "${quality_rc}"
fi
if (( native_release_rc != 0 )); then
  exit "${native_release_rc}"
fi
if (( require_head_opt_in && head_rc != 0 )); then
  exit "${head_rc}"
fi
if "${python_bin}" - "${readiness_report}" <<'PY'
import json
import sys
from pathlib import Path

summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if summary.get("production_ready") is not True:
    for blocker in summary.get("production_readiness_blockers", []):
        print(f"readiness blocker: {blocker}", file=sys.stderr)
    raise SystemExit(1)
PY
then
  echo "GLiNER2 Zig ${zig_backend^^} production readiness: PASS"
  exit 0
fi
exit 3
