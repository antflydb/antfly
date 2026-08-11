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
export ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-$repo_root/zig/.zig-cache}"
export ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-/tmp/antfly-ci-zig-global}"
mkdir -p "$HOME" "$ZIG_LOCAL_CACHE_DIR" "$ZIG_GLOBAL_CACHE_DIR"

target="${ANTFLY_CI_ZIG_TARGET:-x86_64-linux-gnu}"
cpu="${ANTFLY_CI_ZIG_CPU:-baseline}"
optimize="${ANTFLY_CI_ZIG_OPTIMIZE:-Debug}"
strip="${ANTFLY_CI_ZIG_STRIP:-false}"
build_capi="${ANTFLY_CI_BUILD_CAPI:-false}"

case "$strip" in
  true|false) ;;
  *)
    echo "ANTFLY_CI_ZIG_STRIP must be true or false, got: $strip" >&2
    exit 2
    ;;
esac
case "$build_capi" in
  true|false) ;;
  *)
    echo "ANTFLY_CI_BUILD_CAPI must be true or false, got: $build_capi" >&2
    exit 2
    ;;
esac

cd "$repo_root/zig"

uname -a
zig version

build_steps=(antfly)
if [[ "$build_capi" == "true" ]]; then
  build_steps+=(capi capi-smoke)
fi

python3 tools/run_bounded_zig_build.py --zig zig -- build \
  -Dtarget="$target" \
  -Dcpu="$cpu" \
  -Doptimize="$optimize" \
  -Dstrip="$strip" \
  "${build_steps[@]}"

chmod +x zig-out/bin/antfly

if [[ "$build_capi" == "true" ]]; then
  "$script_dir/zig-production-runtime-smoke.sh" zig-out/bin/antfly
fi
