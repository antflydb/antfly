#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

set -euo pipefail
if [[ "${ANTFLY_CI_TRACE:-0}" == "1" ]]; then
  set -x
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

if [[ -z "${HOME:-}" || ! -w "${HOME:-/}" ]]; then
  export HOME=/tmp/antfly-ci-home
fi
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/antfly-ci-uv-cache}"
export UV_LINK_MODE="${UV_LINK_MODE:-copy}"
antfly_venv="${ANTFLY_E2E_VENV:-/tmp/antfly-ci-venv/antfly}"
inference_venv="${ANTFLY_INFERENCE_E2E_VENV:-/tmp/antfly-ci-venv/inference}"
export HF_HOME="${HF_HOME:-/tmp/antfly-ci-hf}"
export ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-$repo_root/zig/.zig-cache}"
export ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-/tmp/antfly-ci-zig-global}"
mkdir -p "$HOME" "$UV_CACHE_DIR" "$(dirname "$antfly_venv")" "$(dirname "$inference_venv")" "$HF_HOME" "$ZIG_LOCAL_CACHE_DIR" "$ZIG_GLOBAL_CACHE_DIR"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  "$script_dir/zig-build-e2e-binaries.sh"
fi

cd "$repo_root/zig"
if [[ ! -x ./zig-out/bin/antfly ]]; then
  echo "missing ./zig-out/bin/antfly; rerun without SKIP_BUILD=1 or run 'zig build antfly' first" >&2
  exit 1
fi
chmod +x ./zig-out/bin/antfly

uname -a
uv --version
if ! ./zig-out/bin/antfly --version; then
  echo "zig-out/bin/antfly is not executable on this host; rerun without SKIP_BUILD=1" >&2
  exit 1
fi

export ANTFLY_BIN="${ANTFLY_BIN:-./zig-out/bin/antfly}"
export ANTFLY_LSM_OPEN_DEBUG="${ANTFLY_LSM_OPEN_DEBUG:-1}"
e2e_suite="${ANTFLY_E2E_SUITE:-all}"
case "$e2e_suite" in
  all|antfly|inference) ;;
  *)
    echo "ANTFLY_E2E_SUITE must be all, antfly, or inference; got: $e2e_suite" >&2
    exit 2
    ;;
esac
# ANTFLY_FS_PATH_DEBUG is intentionally not defaulted on: it logs one line per
# path component on every filesystem op (~8k lines/s per node), slowing the
# servers and flooding the per-node log tails captured on failure until they
# carry no diagnostic signal. Export it explicitly when chasing an
# fs-path-specific bug.

if [[ "$#" -gt 0 ]]; then
  antfly_args=("$@")
  run_inference="${RUN_INFERENCE_E2E:-0}"
else
  antfly_args=(
    -m "not objectstore_integration and not standalone_integration and not real_model and not postgres_integration and not slow"
    e2e/antfly
  )
  run_inference="${RUN_INFERENCE_E2E:-1}"
fi

antfly_status=0
if [[ "$e2e_suite" != "inference" ]]; then
  UV_PROJECT_ENVIRONMENT="$antfly_venv" "$script_dir/zig-antfly-e2e-pytest.sh" "${antfly_args[@]}" || antfly_status=$?
fi

inference_status=0
if [[ "$e2e_suite" == "inference" ]] || [[ "$e2e_suite" == "all" && "$run_inference" == "1" ]]; then
  UV_PROJECT_ENVIRONMENT="$inference_venv" uv run --project e2e/inference pytest -q --continue-on-collection-errors \
    -m "not slow and not multimodal and not model_integration and not browser_integration" \
    e2e/inference || inference_status=$?
fi

overall_status=0
if [[ "$antfly_status" -ne 0 ]]; then
  echo "Antfly E2E suite failed with exit code $antfly_status" >&2
  overall_status=$antfly_status
fi
if [[ "$inference_status" -ne 0 ]]; then
  echo "Inference E2E suite failed with exit code $inference_status" >&2
  if [[ "$overall_status" -eq 0 ]]; then
    overall_status=$inference_status
  fi
fi
exit "$overall_status"
