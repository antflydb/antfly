#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 RUN_ROOT PORT HEALTH_PORT" >&2
  echo "" >&2
  echo "Environment:" >&2
  echo "  ANTFLY_BIN       Antfly binary (default: zig/zig-out/bin/antfly)" >&2
  echo "  VDBBENCH_ROOT    VectorDBBench checkout (default: ../VectorDBBench)" >&2
  echo "  VDBBENCH_CASE    Case name (default: Performance1536D50K)" >&2
  echo "  VDBBENCH_BATCH   Insert batch size (default: 100)" >&2
  echo "  VDBBENCH_WORKERS Load workers when supported by the checkout (default: 4)" >&2
  echo "  VDBBENCH_CONCURRENCY  Query concurrencies (default: 1,4,16)" >&2
  echo "  VDBBENCH_QUERY_SECONDS Seconds per concurrency (default: 10)" >&2
  exit 2
}

[[ $# -eq 3 ]] || usage

run_root=$1
port=$2
health_port=$3
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
common_git_dir=$(git -C "$repo_root" rev-parse --path-format=absolute --git-common-dir)
main_checkout=$(cd "$(dirname "$common_git_dir")" && pwd)
antfly_bin=${ANTFLY_BIN:-$repo_root/zig/zig-out/bin/antfly}
vdbbench_root=${VDBBENCH_ROOT:-$main_checkout/../VectorDBBench}
vdbbench_case=${VDBBENCH_CASE:-Performance1536D50K}
batch_size=${VDBBENCH_BATCH:-100}
load_workers=${VDBBENCH_WORKERS:-4}
query_concurrency=${VDBBENCH_CONCURRENCY:-1,4,16}
query_seconds=${VDBBENCH_QUERY_SECONDS:-10}
sampler=${FOOTPRINT_SAMPLER:-$main_checkout/../antfly-circus/benchmarks/CRAG-harness/footprint_sampler.py}
supports_load_concurrency=false
supports_serial_cooldown=false

if [[ -e "$run_root" ]]; then
  echo "run root already exists: $run_root" >&2
  exit 2
fi
for required in "$antfly_bin" "$vdbbench_root/.venv/bin/python" "$sampler"; do
  if [[ ! -e "$required" ]]; then
    echo "missing required path: $required" >&2
    exit 2
  fi
done

# VectorDBBench removed --load-concurrency from newer releases and currently
# runs the official load case through its SerialInsertRunner. Preserve support
# for older checkouts without making the qualification script fail against the
# current public CLI.
vdbbench_help=$(
  cd "$vdbbench_root"
  .venv/bin/python -m vectordb_bench.cli.vectordbbench antflyaknn --help
)
if [[ "$vdbbench_help" == *"--load-concurrency"* ]]; then
  supports_load_concurrency=true
else
  echo "VectorDBBench checkout has no --load-concurrency option; using its official serial load runner" >&2
fi
if [[ "$vdbbench_help" == *"--serial-cooldown"* ]]; then
  supports_serial_cooldown=true
fi

mkdir -p "$run_root/results"
python3 "$sampler" --capture-wired-baseline "$run_root/wired-baseline.json"

server_pid=
sampler_pid=

stop_server() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
    server_pid=
  fi
}

cleanup() {
  if [[ -n "$sampler_pid" ]]; then
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=
  fi
  stop_server
}
trap cleanup EXIT INT TERM

mark_phase() {
  python3 - "$run_root/phases.jsonl" "$1" <<'PY'
import json
import sys
import time

with open(sys.argv[1], "a", encoding="utf-8") as handle:
    handle.write(json.dumps({"phase": sys.argv[2], "monotonic_ns": time.monotonic_ns(), "wall_time": time.time()}) + "\n")
PY
}

start_server() {
  local log_name=$1
  "$antfly_bin" standalone \
    --host 127.0.0.1 \
    --port "$port" \
    --health-port "$health_port" \
    --auth false \
    --data-dir "$run_root/data" \
    >"$run_root/$log_name" 2>&1 &
  server_pid=$!
  printf '%s\n' "$server_pid" >"$run_root/antfly.pid"

  for _ in $(seq 1 300); do
    if curl -fsS "http://127.0.0.1:$health_port/healthz" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  curl -fsS "http://127.0.0.1:$health_port/healthz" >/dev/null
}

run_vdbbench() {
  local label=$1
  shift
  local cli_args=(
    antflyaknn
    --host 127.0.0.1
    --port "$port"
    --num-shards 1
    --case-type "$vdbbench_case"
    --db-label "$label"
    --num-concurrency "$query_concurrency"
    --concurrency-duration "$query_seconds"
  )
  if [[ "$supports_load_concurrency" == true ]]; then
    cli_args+=(--load-concurrency "$load_workers")
  fi
  if [[ "$supports_serial_cooldown" == true ]]; then
    cli_args+=(--serial-cooldown 2)
  fi
  (
    cd "$vdbbench_root"
    ANTFLY_VDBBENCH_KEEP_DEFAULT_FULL_TEXT=0 \
    ANTFLY_BENCH_STATUS=1 \
    ANTFLY_BENCH_STATUS_INTERVAL=5 \
    DATASET_LOCAL_DIR=${DATASET_LOCAL_DIR:-/private/tmp/vdbbench-dataset} \
    RESULTS_LOCAL_DIR="$run_root/results" \
    NUM_PER_BATCH="$batch_size" \
    IR_DATASETS_HOME=${IR_DATASETS_HOME:-/private/tmp/vdbbench-ir} \
    IR_DATASETS_TMP=${IR_DATASETS_TMP:-/private/tmp/vdbbench-ir-tmp} \
    PYTHONPATH=. \
    .venv/bin/python -m vectordb_bench.cli.vectordbbench "${cli_args[@]}" "$@"
  )
}

mark_phase server_start
start_server antfly-initial.log
python3 "$sampler" \
  --pid-file "$run_root/antfly.pid" \
  --wired-baseline-file "$run_root/wired-baseline.json" \
  --out "$run_root/footprint.json" \
  --timeline "$run_root/footprint.jsonl" \
  --interval 0.2 \
  >"$run_root/footprint.log" 2>&1 &
sampler_pid=$!

mark_phase live_load_and_query_start
run_vdbbench antfly-qualification-live --drop-old --load --search-concurrent --search-serial \
  >"$run_root/vdbbench-live.log" 2>&1
mark_phase live_load_and_query_end
curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-before-restart.txt"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench" >"$run_root/table-before-restart.json"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-before-restart.json"

mark_phase restart_begin
stop_server
mark_phase shutdown_complete
start_server antfly-reopened.log
mark_phase restart_ready

run_vdbbench antfly-qualification-reopened-cold --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
  >"$run_root/vdbbench-reopened-cold.log" 2>&1
mark_phase reopened_cold_query_end
run_vdbbench antfly-qualification-reopened-warm --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
  >"$run_root/vdbbench-reopened-warm.log" 2>&1
mark_phase reopened_warm_query_end
curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-after-restart.txt"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-after-restart.json"

kill "$sampler_pid" 2>/dev/null || true
wait "$sampler_pid" 2>/dev/null || true
sampler_pid=

python3 "$script_dir/summarize_vdbbench_qualification.py" "$run_root"
