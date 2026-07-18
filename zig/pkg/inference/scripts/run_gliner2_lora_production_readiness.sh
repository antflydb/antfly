#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run_gliner2_lora_production_readiness.sh [options]

Runs the strict GLiNER2 LoRA batch-32/seq-128 cross-runtime release-parity gate,
a deterministic held-out full-task quality gate through the frozen upstream
decoder, a native full-heldout GLiNER2 quality gate, and an opt-in head-MLP
fusion guard.

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
  --heldout-min KEY=FLOAT  Required held-out floor; repeat for every metric listed below
  --heldout-threshold N    Upstream extraction threshold (default: 0.5)
  --heldout-batch-size N   Upstream CPU evaluation batch size (default: 8)
  --heldout-max-length N   Optional maximum word-token length
  --compare-steps N        Steps per comparison run forwarded to the perf gate
  --warm-production-ready  Use strict warm-step production target defaults
  --loop-profile           Enable executor loop-profile timing summaries
  --hazard-profile         Enable planned-access hazard timing summaries
  --max-zig-python-warm-step-ratio-median N
                           Forward warm median Zig/Python ratio limit
  --max-zig-python-warm-step-ratio-any-run N
                           Forward warm any-run Zig/Python ratio limit
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
runs_explicit=0
head_runs=1
out_dir="/private/tmp/termite-gliner2-production-readiness"
skip_head_opt_in=0
require_head_opt_in=0
warm_production_ready=0
train_data_set=0
eval_data_set=0
train_data=""
eval_data=""
model_dir=""
release_adapter_dir=""
python_bin="/private/tmp/gliner2-parity-venv/bin/python"
upstream_source=""
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
      runs_explicit=1
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
    --compare-steps | --max-zig-python-warm-step-ratio-median | --max-zig-python-warm-step-ratio-any-run)
      gate_args+=("$1" "${2:?missing value for $1}")
      shift 2
      ;;
    --warm-production-ready)
      warm_production_ready=1
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

if (( ! train_data_set || ! eval_data_set )); then
  echo "--train-data and --eval-data are required for release-parity validation" >&2
  exit 2
fi
if [[ -z "${upstream_source}" ]]; then
  echo "--upstream-source is required for frozen upstream held-out evaluation" >&2
  exit 2
fi
if [[ -z "${model_dir}" || -z "${release_adapter_dir}" ]]; then
  echo "--model-dir and --release-adapter-dir are required for release quality validation" >&2
  exit 2
fi
if [[ ! -d "${model_dir}" || ! -f "${release_adapter_dir}/adapter_config.json" || ! -f "${release_adapter_dir}/adapter_model.safetensors" || ! -f "${release_adapter_dir}/task_head.safetensors" || ! -f "${release_adapter_dir}/training_manifest.json" ]]; then
  echo "release model, standard PEFT adapter, or Zig training manifest is missing" >&2
  exit 2
fi
python3 "${script_dir}/validate_gliner2_release_data.py" \
  --train "${train_data}" --eval "${eval_data}" --require-full-task \
  --model-dir "${model_dir}" --release-adapter-dir "${release_adapter_dir}"
heldout_preflight=("${upstream_source}" "${heldout_threshold}" "${heldout_batch_size}" "${heldout_max_length}")
if ((${#heldout_minima[@]})); then
  heldout_preflight+=("${heldout_minima[@]}")
fi
PYTHONPATH="${script_dir}" python3 - "${heldout_preflight[@]}" <<'PY'
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

native_min_f1=""
native_min_exact_match=""
for minimum in "${heldout_minima[@]}"; do
  case "${minimum}" in
    entities.micro_f1=*) native_min_f1="${minimum#*=}" ;;
    entities.exact_match=*) native_min_exact_match="${minimum#*=}" ;;
  esac
done
if [[ -z "${native_min_f1}" || -z "${native_min_exact_match}" ]]; then
  echo "native quality requires entities.micro_f1 and entities.exact_match minima" >&2
  exit 2
fi

if (( warm_production_ready && ! runs_explicit )); then
  runs=3
fi

default_rc=0
head_rc=0
quality_rc=1
native_release_rc=1

default_cmd=(
  "${script_dir}/run_gliner2_lora_perf_gate.sh"
  "--production-ready"
  "--runs" "${runs}"
  "--out-dir" "${default_dir}"
)
head_cmd=(
  "${script_dir}/run_gliner2_lora_perf_gate.sh"
  "--production-batch32"
  "--op-stats"
  "--runs" "${head_runs}"
  "--out-dir" "${head_dir}"
  "--max-zig-metal-peak-live-bytes-median" "1717986918"
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
  for minimum in "${heldout_minima[@]}"; do
    quality_cmd+=("--min-metric" "${minimum}")
  done
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
  for minimum in "${heldout_minima[@]}"; do
    case "${minimum}" in
      entities.micro_f1=* | entities.exact_match=*) ;;
      *) native_quality_cmd+=(--min-task-metric "${minimum}") ;;
    esac
  done
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

python3 - "${out_dir}" "${default_rc}" "${head_rc}" "${quality_rc}" "${native_release_rc}" "${skip_head_opt_in}" "${require_head_opt_in}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
default_rc = int(sys.argv[2])
head_rc = int(sys.argv[3])
quality_rc = int(sys.argv[4])
native_release_rc = int(sys.argv[5])
skip_head_opt_in = bool(int(sys.argv[6]))
require_head_opt_in = bool(int(sys.argv[7]))


def load_summary(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        return {"error": f"invalid json: {exc}"}
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else None


default_summary_path = out_dir / "default" / "perf_summary.json"
head_summary_path = out_dir / "head-mlp-opt-in" / "perf_summary.json"
quality_path = out_dir / "heldout_quality.json"
native_release_path = out_dir / "native_full_task_quality.json"
quality_report = None
if quality_rc == 0:
    try:
        quality_report = json.loads(quality_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        pass
native_release_report = None
if native_release_rc == 0:
    try:
        native_release_report = json.loads(native_release_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        pass
if not isinstance(quality_report, dict):
    quality_report = None
if not isinstance(native_release_report, dict):
    native_release_report = None
default_summary = load_summary(default_summary_path) if default_rc == 0 else None
head_summary = load_summary(head_summary_path) if not skip_head_opt_in and head_rc == 0 else None
default_ready = default_rc == 0 and default_summary is not None and default_summary.get("pass") is True
quality_ready = quality_rc == 0 and quality_report is not None and quality_report.get("pass") is True
native_ready = (
    native_release_rc == 0
    and native_release_report is not None
    and native_release_report.get("pass") is True
)
head_ready = None if skip_head_opt_in else head_rc == 0 and head_summary is not None and head_summary.get("pass") is True
head_required_ready = not require_head_opt_in or head_ready is True
integrity_errors = []
if default_rc == 0 and not default_ready:
    integrity_errors.append("cross-runtime parity command returned success without a current passing summary")
if quality_rc == 0 and not quality_ready:
    integrity_errors.append("held-out quality command returned success without a current passing report")
if native_release_rc == 0 and not native_ready:
    integrity_errors.append("native full-task quality command returned success without a current passing report")
if not skip_head_opt_in and head_rc == 0 and head_ready is not True:
    integrity_errors.append("head opt-in command returned success without a current passing summary")
blockers = []
if not default_ready:
    blockers.append("cross-runtime parity gate failed")
if not quality_ready:
    blockers.append("held-out full-task quality gate failed or did not run")
if not native_ready:
    blockers.append("native held-out full-task quality gate failed or did not run")
blockers.extend(integrity_errors)
blockers.extend(
    [
        "independently trained upstream/Zig held-out convergence parity is not evaluated",
        "stock upstream SamplingConfig augmentation, shuffle, and stochastic dropout result parity are not implemented",
        "U+0130 lowercase expansion and normalization-changing Unicode remain unsupported and fail closed",
    ]
)
if not head_required_ready:
    blockers.append("required head opt-in gate failed")
summary = {
    "cross_runtime_parity_ready": default_ready,
    "heldout_quality_evaluated": quality_report is not None,
    "heldout_quality_ready": quality_ready,
    "native_heldout_full_task_quality_ready": native_ready,
    "native_full_task_quality_evaluated": native_release_report is not None,
    "upstream_peft_release_gate_ready": False,
    "configured_upstream_peft_gate_ready": default_ready and quality_ready and head_required_ready,
    "quality_policy": "caller_supplied_complete_minima",
    "deployment_scope": "upstream_peft_and_antfly_native_full_heldout_gliner2_quality",
    "production_ready": False,
    "production_readiness_blockers": blockers,
    "head_opt_in_ready": head_ready,
    "default_rc": default_rc,
    "head_opt_in_rc": None if skip_head_opt_in else head_rc,
    "default_summary_path": str(default_summary_path),
    "head_opt_in_summary_path": None if skip_head_opt_in else str(head_summary_path),
    "heldout_quality_path": str(quality_path),
    "heldout_quality": quality_report,
    "native_full_task_quality_path": str(native_release_path),
    "native_full_task_quality": native_release_report,
    "default_summary": default_summary,
    "head_opt_in_summary": head_summary,
}
out_path = out_dir / "readiness_summary.json"
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(f"readiness summary: {out_path}")
if integrity_errors:
    raise SystemExit(4)
PY

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
echo "production readiness remains blocked by held-out convergence parity, stock stochastic trace parity, and the fail-closed Unicode subset" >&2
exit 3
