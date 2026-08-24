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
  echo "  VDBBENCH_CONCURRENCY  Query concurrencies (default: 1,5,10,20,30)" >&2
  echo "  VDBBENCH_QUERY_SECONDS Seconds per concurrency (default: 30)" >&2
  echo "  VDBBENCH_PROCESS_MEMORY_BUDGET_MB Explicit Antfly process envelope (default: auto)" >&2
  echo "  VDBBENCH_PROFILE_COUNT Detailed public-API queries after the warm run (default: 1000; 0 disables)" >&2
  echo "  VDBBENCH_PROFILE_DATASET Dataset directory for detailed profiling (default: inferred from case)" >&2
  echo "  VDBBENCH_RESUME_AFTER_LIVE Restart/query an existing run root (default: 0)" >&2
  echo "  VDBBENCH_LABEL_SUFFIX Required unique suffix for a resume run (for example: -budget-1024)" >&2
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
query_concurrency=${VDBBENCH_CONCURRENCY:-1,5,10,20,30}
query_seconds=${VDBBENCH_QUERY_SECONDS:-30}
process_memory_budget_mb=${VDBBENCH_PROCESS_MEMORY_BUDGET_MB:-}
profile_count=${VDBBENCH_PROFILE_COUNT:-1000}
dataset_root=${DATASET_LOCAL_DIR:-/private/tmp/vdbbench-dataset}
profile_dataset=${VDBBENCH_PROFILE_DATASET:-}
sampler=${FOOTPRINT_SAMPLER:-$main_checkout/../antfly-circus/benchmarks/CRAG-harness/footprint_sampler.py}
supports_load_concurrency=false
supports_serial_cooldown=false
resume_after_live=${VDBBENCH_RESUME_AFTER_LIVE:-0}
label_suffix=${VDBBENCH_LABEL_SUFFIX:-}
live_label="antfly-qualification-online-live${label_suffix}"
cold_label="antfly-qualification-reopened-cold${label_suffix}"
warm_label="antfly-qualification-reopened-warm${label_suffix}"
case "$vdbbench_case" in
  *50K*)
    expected_docs=50000
    [[ -n "$profile_dataset" ]] || profile_dataset="$dataset_root/openai/openai_small_50k"
    ;;
  *1M*)
    expected_docs=1000000
    [[ -n "$profile_dataset" ]] || profile_dataset="$dataset_root/cohere/cohere_medium_1m"
    ;;
  *) expected_docs=1 ;;
esac

if [[ ! "$profile_count" =~ ^[0-9]+$ ]]; then
  echo "VDBBENCH_PROFILE_COUNT must be a non-negative integer" >&2
  exit 2
fi
if [[ -n "$process_memory_budget_mb" && ! "$process_memory_budget_mb" =~ ^[1-9][0-9]*$ ]]; then
  echo "VDBBENCH_PROCESS_MEMORY_BUDGET_MB must be a positive integer" >&2
  exit 2
fi

if [[ -e "$run_root" && "$resume_after_live" != "1" ]]; then
  echo "run root already exists: $run_root" >&2
  exit 2
fi
if [[ ! -e "$run_root" && "$resume_after_live" == "1" ]]; then
  echo "resume run root does not exist: $run_root" >&2
  exit 2
fi
if [[ "$resume_after_live" == "1" && -z "$label_suffix" ]]; then
  echo "VDBBENCH_LABEL_SUFFIX is required for a resume run to preserve existing evidence" >&2
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
if [[ "$resume_after_live" != "1" ]]; then
  python3 - "$run_root/run-config.json" "$repo_root" "$vdbbench_root" "$vdbbench_case" "$batch_size" "$load_workers" "$query_concurrency" "$query_seconds" "$process_memory_budget_mb" "$profile_count" "$profile_dataset" <<'PY'
import json
import subprocess
import sys

(
    out_path,
    repo_root,
    vdbbench_root,
    case_name,
    batch_size,
    workers,
    concurrency,
    query_seconds,
    memory_budget_mb,
    profile_count,
    profile_dataset,
) = sys.argv[1:]
antfly_git_head = subprocess.check_output(
    ["git", "-C", repo_root, "rev-parse", "HEAD"], text=True
).strip()
vdbbench_git_head = subprocess.check_output(
    ["git", "-C", vdbbench_root, "rev-parse", "HEAD"], text=True
).strip()
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "antfly_git_head": antfly_git_head,
            "vdbbench_git_head": vdbbench_git_head,
            "vdbbench_root": vdbbench_root,
            "case": case_name,
            "client_batch_size": int(batch_size),
            "load_workers_requested": int(workers),
            "query_concurrency": concurrency,
            "query_seconds": int(query_seconds),
            "process_memory_budget_mb": int(memory_budget_mb) if memory_budget_mb else None,
            "public_profile_count": int(profile_count),
            "public_profile_dataset": profile_dataset,
            "load_lifecycle": "public_api_online_incremental",
            "builder": "server_selected; do not assume recursive from client batch size",
        },
        handle,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")
PY
  python3 "$sampler" --capture-wired-baseline "$run_root/wired-baseline.json"
else
  python3 - "$run_root/resume-config${label_suffix}.json" "$repo_root" "$antfly_bin" "$process_memory_budget_mb" "$profile_count" "$profile_dataset" "$cold_label" "$warm_label" <<'PY'
import json
import subprocess
import sys

(
    out_path,
    repo_root,
    antfly_bin,
    memory_budget_mb,
    profile_count,
    profile_dataset,
    cold_label,
    warm_label,
) = sys.argv[1:]
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "antfly_git_head": subprocess.check_output(
                ["git", "-C", repo_root, "rev-parse", "HEAD"], text=True
            ).strip(),
            "antfly_bin": antfly_bin,
            "process_memory_budget_mb": (
                int(memory_budget_mb) if memory_budget_mb else None
            ),
            "public_profile_count": int(profile_count),
            "public_profile_dataset": profile_dataset,
            "cold_label": cold_label,
            "warm_label": warm_label,
        },
        handle,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")
PY
  python3 "$sampler" --capture-wired-baseline "$run_root/wired-baseline${label_suffix}.json"
fi

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
  local server_env=()
  if [[ -n "$process_memory_budget_mb" ]]; then
    server_env+=("ANTFLY_PROCESS_MEMORY_BUDGET_MB=$process_memory_budget_mb")
  fi
  env "${server_env[@]}" "$antfly_bin" standalone \
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

run_public_profile() {
  local suffix=$1
  if [[ "$profile_count" == "0" ]]; then
    return
  fi
  if [[ ! -f "$profile_dataset/test.parquet" || ! -f "$profile_dataset/neighbors.parquet" ]]; then
    echo "detailed profile dataset is incomplete: $profile_dataset" >&2
    exit 2
  fi
  mark_phase "public_profile${suffix}_start"
  "$vdbbench_root/.venv/bin/python" "$script_dir/profile_vdbbench_public_query.py" \
    --dataset "$profile_dataset" \
    --port "$port" \
    --count "$profile_count" \
    --output "$run_root/public-query-profile${suffix}.json" \
    >"$run_root/public-query-profile${suffix}.log" 2>&1
  mark_phase "public_profile${suffix}_end"
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

validate_vdbbench_result() {
  local db_label=$1
  local expected_load_count=$2
  local require_query_metrics=$3
  python3 - "$run_root/results" "$db_label" "$expected_load_count" "$require_query_metrics" <<'PY'
import json
import pathlib
import sys

results_root = pathlib.Path(sys.argv[1])
db_label = sys.argv[2]
expected_load_count = int(sys.argv[3])
require_query_metrics = sys.argv[4] == "1"
matches = []
for path in results_root.rglob("*.json"):
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    for result in payload.get("results", []):
        configured_label = result.get("task_config", {}).get("db_config", {}).get("db_label")
        if configured_label == db_label:
            matches.append((path, result))

if len(matches) != 1:
    raise SystemExit(
        f"expected exactly one VectorDBBench result for {db_label!r}, found {len(matches)}"
    )
path, result = matches[0]
if result.get("label") == "x":
    raise SystemExit(f"VectorDBBench stage {db_label!r} failed; see {path}")
metrics = result.get("metrics", {})
loaded = int(metrics.get("inserted_count", 0) or metrics.get("max_load_count", 0) or 0)
if loaded < expected_load_count:
    raise SystemExit(
        f"VectorDBBench stage {db_label!r} loaded {loaded}, expected at least {expected_load_count}; see {path}"
    )
if require_query_metrics:
    recall = float(metrics.get("recall", 0) or 0)
    latency = float(metrics.get("serial_latency_p95", 0) or 0)
    if recall <= 0 or latency <= 0:
        raise SystemExit(
            f"VectorDBBench stage {db_label!r} has no valid query metrics (recall={recall}, p95={latency}); see {path}"
        )
PY
}

if [[ "$resume_after_live" == "1" ]]; then
  mark_phase restart_begin
  start_server "antfly-reopened${label_suffix}.log"
  mark_phase restart_ready
  python3 "$sampler" \
    --pid-file "$run_root/antfly.pid" \
    --wired-baseline-file "$run_root/wired-baseline${label_suffix}.json" \
    --out "$run_root/footprint-restart${label_suffix}.json" \
    --timeline "$run_root/footprint-restart${label_suffix}.jsonl" \
    --interval 0.2 \
    >"$run_root/footprint-restart${label_suffix}.log" 2>&1 &
  sampler_pid=$!

  run_vdbbench "$cold_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
    >"$run_root/vdbbench-reopened-cold${label_suffix}.log" 2>&1
  validate_vdbbench_result "$cold_label" 0 1
  mark_phase reopened_cold_query_end
  run_vdbbench "$warm_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
    >"$run_root/vdbbench-reopened-warm${label_suffix}.log" 2>&1
  validate_vdbbench_result "$warm_label" 0 1
  mark_phase reopened_warm_query_end
  run_public_profile "$label_suffix"
  curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-after-restart${label_suffix}.txt"
  curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-after-restart${label_suffix}.json"

  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  sampler_pid=
  python3 "$script_dir/summarize_vdbbench_qualification.py" "$run_root"
  exit 0
fi

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
run_vdbbench "$live_label" --drop-old --load --search-concurrent --search-serial \
  >"$run_root/vdbbench-live.log" 2>&1
validate_vdbbench_result "$live_label" "$expected_docs" 1
mark_phase live_load_and_query_end
curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-before-restart.txt"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench" >"$run_root/table-before-restart.json"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-before-restart.json"

mark_phase restart_begin
stop_server
mark_phase shutdown_complete
start_server antfly-reopened.log
mark_phase restart_ready

run_vdbbench "$cold_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
  >"$run_root/vdbbench-reopened-cold.log" 2>&1
validate_vdbbench_result "$cold_label" 0 1
mark_phase reopened_cold_query_end
run_vdbbench "$warm_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
  >"$run_root/vdbbench-reopened-warm.log" 2>&1
validate_vdbbench_result "$warm_label" 0 1
mark_phase reopened_warm_query_end
run_public_profile ""
curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-after-restart.txt"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-after-restart.json"

kill "$sampler_pid" 2>/dev/null || true
wait "$sampler_pid" 2>/dev/null || true
sampler_pid=

python3 "$script_dir/summarize_vdbbench_qualification.py" "$run_root"
