#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
zig_dir="${repo_root}/zig"

data_source="${DATA_SOURCE:-${repo_root}/work-log/do8018/spaces}"
out_dir="${OUT_DIR:-${repo_root}/work-log/do8018/releasefast-baseline-$(date +%Y%m%d-%H%M%S)}"
port="${PORT:-18080}"
health_port="${HEALTH_PORT:-14200}"
table_prefix="${TABLE_PREFIX:-codex_sessions_releasefast}"
batch_size="${BATCH_SIZE:-200}"
batch_workers="${BATCH_WORKERS:-16}"
object_workers="${OBJECT_WORKERS:-8}"
wait_catchup="${WAIT_CATCHUP:-120s}"
git_commit="$(git -C "${repo_root}" rev-parse --short=12 HEAD)"

if [[ ! -d "${data_source}" ]]; then
  echo "missing DATA_SOURCE=${data_source}" >&2
  exit 1
fi

mkdir -p "${out_dir}"

echo "building ReleaseFast antfly"
(cd "${zig_dir}" && zig build install -Doptimize=ReleaseFast)

run_one() {
  local mode="$1"
  local metrics_enabled="$2"
  local data_dir="${out_dir}/data-${mode}"
  local log_file="${out_dir}/${mode}-server.log"
  local bench_log="${out_dir}/${mode}-bench.log"
  local summary_file="${out_dir}/${mode}-summary.json"
  local vmmap_file="${out_dir}/${mode}-vmmap-summary.txt"
  local table="${table_prefix}_${mode}"

  if [[ -e "${data_dir}" ]]; then
    echo "refusing to reuse existing data dir: ${data_dir}" >&2
    exit 1
  fi
  mkdir -p "${data_dir}"

  echo "starting antfly mode=${mode} data_dir=${data_dir}"
  (
    cd "${zig_dir}"
    if [[ "${metrics_enabled}" == "1" ]]; then
      export ANTFLY_BENCH_METRICS=1
    else
      unset ANTFLY_BENCH_METRICS
    fi
    exec ./zig-out/bin/antfly swarm \
      --host 127.0.0.1 \
      --port "${port}" \
      --health true \
      --health-port "${health_port}" \
      --data-dir "${data_dir}"
  ) >"${log_file}" 2>&1 &
  local server_pid="$!"

  cleanup() {
    if kill -0 "${server_pid}" 2>/dev/null; then
      kill "${server_pid}" 2>/dev/null || true
      wait "${server_pid}" 2>/dev/null || true
    fi
  }
  trap cleanup RETURN

  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:${health_port}/metrics" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  curl -fsS "http://127.0.0.1:${health_port}/metrics" >/dev/null

  echo "loading mode=${mode} table=${table}"
  (
    cd "${repo_root}/examples/docsaf"
    env GOWORK=off go run ./cmd/spaces-bench \
      --local-dir "${data_source}" \
      --contains "/codex/" \
      --table "${table}" \
      --antfly-url "http://127.0.0.1:${port}/api/v1" \
      --batch-size "${batch_size}" \
      --batch-workers "${batch_workers}" \
      --object-workers "${object_workers}" \
      --sample-pid "${server_pid}" \
      --sample-every 1s \
      --health-url "http://127.0.0.1:${health_port}/metrics" \
      --wait-catchup "${wait_catchup}" \
      --data-dir "${data_dir}" \
      --vmmap-out "${vmmap_file}" \
      --summary-out "${summary_file}" \
      --git-commit "${git_commit}" \
      --run-label "${mode}" \
      --metrics-enabled="$([[ "${metrics_enabled}" == "1" ]] && echo true || echo false)"
  ) | tee "${bench_log}"

  echo "mode=${mode} server_log=${log_file}"
  echo "mode=${mode} bench_log=${bench_log}"
  echo "mode=${mode} summary=${summary_file}"
  echo "mode=${mode} vmmap=${vmmap_file}"
}

run_one metrics_off 0
run_one metrics_on 1

echo "releasefast baseline output: ${out_dir}"
