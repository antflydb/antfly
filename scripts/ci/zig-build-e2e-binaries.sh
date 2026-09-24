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

cpu="${ANTFLY_CI_ZIG_CPU:-baseline}"
optimize="${ANTFLY_CI_ZIG_OPTIMIZE:-Debug}"
build_args=(build)
if [[ -n "${ANTFLY_CI_ZIG_TARGET:-}" ]]; then
  build_args+=("-Dtarget=$ANTFLY_CI_ZIG_TARGET")
fi
strip="${ANTFLY_CI_ZIG_STRIP:-false}"
build_capi="${ANTFLY_CI_BUILD_CAPI:-false}"
enable_cuda="${ANTFLY_CI_ZIG_CUDA:-false}"
cuda_artifacts="${ANTFLY_CI_ZIG_CUDA_ARTIFACTS:-fatbin}"

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
case "$enable_cuda" in
  true|false) ;;
  *)
    echo "ANTFLY_CI_ZIG_CUDA must be true or false, got: $enable_cuda" >&2
    exit 2
    ;;
esac
case "$cuda_artifacts" in
  fatbin|portable|sm89) ;;
  *)
    echo "ANTFLY_CI_ZIG_CUDA_ARTIFACTS must be fatbin, portable, or sm89, got: $cuda_artifacts" >&2
    exit 2
    ;;
esac

cd "$repo_root/zig"

uname -a
if command -v lscpu >/dev/null 2>&1; then
  lscpu
fi
zig version

build_steps=(antfly)
if [[ "$build_capi" == "true" ]]; then
  build_steps+=(capi capi-smoke)
fi

build_args+=(
  "-Dcpu=$cpu"
  "-Doptimize=$optimize"
  "-Dstrip=$strip"
  "-Dcuda=$enable_cuda"
  "-Dcuda-artifacts=$cuda_artifacts"
  --summary all
  "${build_steps[@]}"
)

if [[ "$enable_cuda" == "true" ]]; then
  # This script comes from the candidate checkout even when approved PR CI
  # uses main's workflow. Retain progress before an outer timeout can kill Zig
  # without its per-archive build summary. RSS is in KiB; cgroup memory is bytes.
  report_build_resources() {
    date -u '+CUDA build resources: %Y-%m-%dT%H:%M:%SZ'
    ps -C zig -o pid,ppid,etime,time,pcpu,rss,stat,wchan,comm || true
    ps -C zig -o pid=,args= | awk '{
      for (i = 2; i < NF; i++)
        if ($i == "--name") print "compile pid=" $1 " name=" $(i + 1)
    }' || true
    free -m || true
    for metric in memory.max memory.current memory.peak memory.events cpu.max cpu.stat; do
      if [[ -r "/sys/fs/cgroup/$metric" ]]; then
        echo "cgroup $metric:"
        cat "/sys/fs/cgroup/$metric" || true
      fi
    done
  }
  echo "CUDA build scheduler budget (bytes):"
  python3 tools/run_bounded_zig_build.py --print-max-rss
  report_build_resources
  (
    sleep_pid=''
    trap '[[ -z "$sleep_pid" ]] || kill "$sleep_pid" 2>/dev/null || true; exit 0' TERM INT
    while true; do
      sleep 60 &
      sleep_pid=$!
      wait "$sleep_pid"
      sleep_pid=''
      report_build_resources
    done
  ) &
  monitor_pid=$!
  trap 'kill "$monitor_pid" 2>/dev/null || true; wait "$monitor_pid" 2>/dev/null || true' EXIT
fi

python3 tools/run_bounded_zig_build.py --zig zig -- "${build_args[@]}"

chmod +x zig-out/bin/antfly

if [[ "$build_capi" == "true" ]]; then
  "$script_dir/zig-production-runtime-smoke.sh" zig-out/bin/antfly
fi
