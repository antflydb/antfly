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
  echo "missing ./zig-out/bin/antfly; rerun without SKIP_BUILD=1 or build install -Dedition=full first" >&2
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
export ANTFLY_FS_PATH_DEBUG="${ANTFLY_FS_PATH_DEBUG:-1}"

if [[ "$#" -gt 0 ]]; then
  antfly_args=("$@")
  run_inference="${RUN_INFERENCE_E2E:-0}"
else
  antfly_args=(
    -m "not objectstore_integration and not swarm_integration and not real_model and not postgres_integration and not slow"
    e2e/antfly
  )
  run_inference="${RUN_INFERENCE_E2E:-1}"
fi

UV_PROJECT_ENVIRONMENT="$antfly_venv" uv run --project e2e/antfly pytest -q -x "${antfly_args[@]}"

if [[ "$run_inference" == "1" ]]; then
  UV_PROJECT_ENVIRONMENT="$inference_venv" uv run --project e2e/inference pytest -q -x \
    -m "not slow and not multimodal and not model_integration and not browser_integration" \
    e2e/inference
fi
