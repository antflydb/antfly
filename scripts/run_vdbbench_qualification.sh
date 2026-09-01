#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 RUN_ROOT PORT HEALTH_PORT [OPTIONS]" >&2
  echo "" >&2
  echo "Options:" >&2
  echo "  --case NAME              VectorDBBench case" >&2
  echo "  --vdbbench-root PATH     VectorDBBench checkout" >&2
  echo "  --vdbbench-python PATH   Python from a prepared VectorDBBench virtualenv" >&2
  echo "  --batch N                Public insert batch size" >&2
  echo "  --workers N              Requested load workers when supported" >&2
  echo "  --query-concurrency CSV  Query concurrencies" >&2
  echo "  --query-seconds N        Seconds per query concurrency" >&2
  echo "  --memory-budget-mb N     Explicit Antfly process envelope" >&2
  echo "  --profile-count N        Detailed public-API query count" >&2
  echo "  --profile-dataset PATH   Dataset directory for detailed profiling" >&2
  echo "  --search-effort FLOAT    Public ANN effort in the inclusive range 0..1" >&2
  echo "  --native-hbc             Enable the HBC native posting WAL/segment store" >&2
  echo "  --vector-blocks          Build/use the shared mmap exact-vector projection" >&2
  echo "  --vector-block-encoding MODE  Exact projection encoding (float32 or float16)" >&2
  echo "  --recursive-topology-max-workspace-mb N" >&2
  echo "                           Enable bounded quiescent recursive topology rebuild" >&2
  echo "  --topology-rebuild-algorithm MODE  recursive, global_kmeans, or hierarchical_kmeans" >&2
  echo "  --centroid-directory MODE  HBC centroid routing mode (auto, hbc, flat_rabitq, or flat_exact)" >&2
  echo "  --flat-probe-count N     Initial flat centroid posting wave (0: search width)" >&2
  echo "  --posting-idle-max-postings N  Background dirty-posting batch per index" >&2
  echo "  --resume                 Restart/query an existing run root" >&2
  echo "  --resume-concurrent      Run the configured concurrent-search curve after warm resume" >&2
  echo "  --resume-concurrent-only Skip cold/warm serial passes and run only the concurrent curve" >&2
  echo "  --diagnostic-profile-only  Skip the official client lifecycle during a resume A/B" >&2
  echo "  --label-suffix SUFFIX    Unique suffix required by --resume" >&2
  echo "" >&2
  echo "Environment:" >&2
  echo "  ANTFLY_BIN       Antfly binary (default: zig/zig-out/bin/antfly)" >&2
  echo "  VDBBENCH_ROOT    VectorDBBench checkout (default: ../VectorDBBench)" >&2
  echo "  VDBBENCH_PYTHON  Python from a prepared VectorDBBench virtualenv" >&2
  echo "  VDBBENCH_CASE    Case name (default: Performance1536D50K)" >&2
  echo "  VDBBENCH_BATCH   Insert batch size (default: 100)" >&2
  echo "  VDBBENCH_WORKERS Load workers when supported by the checkout (default: 4)" >&2
  echo "  VDBBENCH_CONCURRENCY  Query concurrencies (default: 1,5,10,20,30)" >&2
  echo "  VDBBENCH_QUERY_SECONDS Seconds per concurrency (default: 30)" >&2
  echo "  VDBBENCH_PROCESS_MEMORY_BUDGET_MB Explicit Antfly process envelope (default: auto)" >&2
  echo "  VDBBENCH_PROFILE_COUNT Detailed public-API queries after the warm run (default: 1000; 0 disables)" >&2
  echo "  VDBBENCH_PROFILE_DATASET Dataset directory for detailed profiling (default: inferred from case)" >&2
  echo "  VDBBENCH_VECTOR_BLOCKS Build/use the shared mmap exact-vector projection (default: 0)" >&2
  echo "  VDBBENCH_VECTOR_BLOCK_ENCODING Exact projection encoding (default: float16)" >&2
  echo "  VDBBENCH_RECURSIVE_TOPOLOGY_MAX_WORKSPACE_MB Bounded recursive topology workspace (default: disabled)" >&2
  echo "  VDBBENCH_RESUME_AFTER_LIVE Restart/query an existing run root (default: 0)" >&2
  echo "  VDBBENCH_RESUME_CONCURRENT Run concurrent search after a warm resume (default: 0)" >&2
  echo "  VDBBENCH_LABEL_SUFFIX Required unique suffix for a resume run (for example: -budget-1024)" >&2
  exit 2
}

[[ $# -ge 3 ]] || usage

run_root=$1
port=$2
health_port=$3
shift 3
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
common_git_dir=$(git -C "$repo_root" rev-parse --path-format=absolute --git-common-dir)
main_checkout=$(cd "$(dirname "$common_git_dir")" && pwd)
antfly_bin=${ANTFLY_BIN:-$repo_root/zig/zig-out/bin/antfly}
vdbbench_root=${VDBBENCH_ROOT:-$main_checkout/../VectorDBBench}
vdbbench_python=${VDBBENCH_PYTHON:-}
vdbbench_case=${VDBBENCH_CASE:-Performance1536D50K}
batch_size=${VDBBENCH_BATCH:-100}
load_workers=${VDBBENCH_WORKERS:-4}
query_concurrency=${VDBBENCH_CONCURRENCY:-1,5,10,20,30}
query_seconds=${VDBBENCH_QUERY_SECONDS:-30}
process_memory_budget_mb=${VDBBENCH_PROCESS_MEMORY_BUDGET_MB:-}
profile_count=${VDBBENCH_PROFILE_COUNT:-1000}
dataset_root=${DATASET_LOCAL_DIR:-/private/tmp/vdbbench-dataset}
profile_dataset=${VDBBENCH_PROFILE_DATASET:-}
search_effort=${VDBBENCH_SEARCH_EFFORT:-}
sampler=${FOOTPRINT_SAMPLER:-$main_checkout/../antfly-circus/benchmarks/CRAG-harness/footprint_sampler.py}
supports_load_concurrency=false
supports_serial_cooldown=false
resume_after_live=${VDBBENCH_RESUME_AFTER_LIVE:-0}
resume_concurrent=${VDBBENCH_RESUME_CONCURRENT:-0}
resume_concurrent_only=0
label_suffix=${VDBBENCH_LABEL_SUFFIX:-}
native_hbc=0
vector_blocks=${VDBBENCH_VECTOR_BLOCKS:-0}
vector_block_encoding=${VDBBENCH_VECTOR_BLOCK_ENCODING:-float16}
recursive_topology_max_workspace_mb=${VDBBENCH_RECURSIVE_TOPOLOGY_MAX_WORKSPACE_MB:-}
topology_rebuild_algorithm=${VDBBENCH_TOPOLOGY_REBUILD_ALGORITHM:-global_kmeans}
diagnostic_profile_only=0
centroid_directory_mode=${VDBBENCH_HBC_CENTROID_DIRECTORY_MODE:-}
flat_probe_count=${VDBBENCH_HBC_FLAT_PROBE_COUNT:-}
posting_idle_max_postings=${ANTFLY_DENSE_POSTING_IDLE_MAX_POSTINGS_PER_INDEX:-}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --case) [[ $# -ge 2 ]] || usage; vdbbench_case=$2; shift 2 ;;
    --vdbbench-root) [[ $# -ge 2 ]] || usage; vdbbench_root=$2; shift 2 ;;
    --vdbbench-python) [[ $# -ge 2 ]] || usage; vdbbench_python=$2; shift 2 ;;
    --batch) [[ $# -ge 2 ]] || usage; batch_size=$2; shift 2 ;;
    --workers) [[ $# -ge 2 ]] || usage; load_workers=$2; shift 2 ;;
    --query-concurrency) [[ $# -ge 2 ]] || usage; query_concurrency=$2; shift 2 ;;
    --query-seconds) [[ $# -ge 2 ]] || usage; query_seconds=$2; shift 2 ;;
    --memory-budget-mb) [[ $# -ge 2 ]] || usage; process_memory_budget_mb=$2; shift 2 ;;
    --profile-count) [[ $# -ge 2 ]] || usage; profile_count=$2; shift 2 ;;
    --profile-dataset) [[ $# -ge 2 ]] || usage; profile_dataset=$2; shift 2 ;;
    --search-effort) [[ $# -ge 2 ]] || usage; search_effort=$2; shift 2 ;;
    --native-hbc) native_hbc=1; shift ;;
    --vector-blocks) vector_blocks=1; shift ;;
    --vector-block-encoding) [[ $# -ge 2 ]] || usage; vector_block_encoding=$2; shift 2 ;;
    --recursive-topology-max-workspace-mb) [[ $# -ge 2 ]] || usage; recursive_topology_max_workspace_mb=$2; shift 2 ;;
    --topology-rebuild-algorithm) [[ $# -ge 2 ]] || usage; topology_rebuild_algorithm=$2; shift 2 ;;
    --centroid-directory) [[ $# -ge 2 ]] || usage; centroid_directory_mode=$2; shift 2 ;;
    --flat-probe-count) [[ $# -ge 2 ]] || usage; flat_probe_count=$2; shift 2 ;;
    --posting-idle-max-postings) [[ $# -ge 2 ]] || usage; posting_idle_max_postings=$2; shift 2 ;;
    --resume) resume_after_live=1; shift ;;
    --resume-concurrent) resume_concurrent=1; shift ;;
    --resume-concurrent-only) resume_after_live=1; resume_concurrent=1; resume_concurrent_only=1; shift ;;
    --diagnostic-profile-only) diagnostic_profile_only=1; shift ;;
    --label-suffix) [[ $# -ge 2 ]] || usage; label_suffix=$2; shift 2 ;;
    *) usage ;;
  esac
done

if [[ -z "$vdbbench_python" ]]; then
  vdbbench_python=$vdbbench_root/.venv/bin/python
fi

# Production defaults to native storage. Qualification modes set every gate
# explicitly so legacy-vs-native A/B runs remain reproducible as defaults
# evolve and never accidentally measure a mixed storage authority.
export ANTFLY_HBC_POSTING_SIDECAR=$native_hbc
export ANTFLY_HBC_POSTING_WAL_STORE=$native_hbc
export ANTFLY_HBC_VECTOR_BLOCK_STORE=$vector_blocks
if [[ "$vector_block_encoding" != "float32" && "$vector_block_encoding" != "float16" && "$vector_block_encoding" != "f32" && "$vector_block_encoding" != "f16" ]]; then
  echo "vector block encoding must be float32 or float16" >&2
  exit 2
fi
export ANTFLY_HBC_VECTOR_BLOCK_ENCODING=$vector_block_encoding
if [[ -n "$recursive_topology_max_workspace_mb" ]]; then
  if [[ ! "$recursive_topology_max_workspace_mb" =~ ^[1-9][0-9]*$ ]]; then
    echo "recursive topology workspace must be a positive integer in MiB" >&2
    exit 2
  fi
  export ANTFLY_HBC_RECURSIVE_TOPOLOGY_REBUILD_MAX_BYTES=$((recursive_topology_max_workspace_mb * 1024 * 1024))
fi
if [[ "$topology_rebuild_algorithm" != "recursive" && "$topology_rebuild_algorithm" != "global_kmeans" && "$topology_rebuild_algorithm" != "hierarchical_kmeans" ]]; then
  echo "topology rebuild algorithm must be recursive, global_kmeans, or hierarchical_kmeans" >&2
  exit 2
fi
export ANTFLY_HBC_TOPOLOGY_REBUILD_ALGORITHM=$topology_rebuild_algorithm
if [[ -n "$centroid_directory_mode" ]]; then
  if [[ "$centroid_directory_mode" != "auto" && "$centroid_directory_mode" != "hbc" && "$centroid_directory_mode" != "flat_rabitq" && "$centroid_directory_mode" != "flat_exact" ]]; then
    echo "centroid directory mode must be auto, hbc, flat_rabitq, or flat_exact" >&2
    exit 2
  fi
  export ANTFLY_HBC_CENTROID_DIRECTORY_MODE=$centroid_directory_mode
fi
if [[ -n "$flat_probe_count" ]]; then
  if [[ ! "$flat_probe_count" =~ ^[0-9]+$ ]]; then
    echo "flat probe count must be a non-negative integer" >&2
    exit 2
  fi
  export ANTFLY_HBC_FLAT_CENTROID_PROBE_COUNT=$flat_probe_count
fi
if [[ -n "$posting_idle_max_postings" ]]; then
  if [[ ! "$posting_idle_max_postings" =~ ^[1-9][0-9]*$ ]]; then
    echo "posting idle max postings must be a positive integer" >&2
    exit 2
  fi
  export ANTFLY_DENSE_POSTING_IDLE_MAX_POSTINGS_PER_INDEX=$posting_idle_max_postings
fi

live_label="antfly-qualification-online-live${label_suffix}"
cold_label="antfly-qualification-reopened-cold${label_suffix}"
warm_label="antfly-qualification-reopened-warm${label_suffix}"
concurrent_label="antfly-qualification-reopened-concurrent${label_suffix}"
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
if [[ -n "$search_effort" ]] && ! python3 - "$search_effort" <<'PY'
import math
import sys

try:
    value = float(sys.argv[1])
except ValueError:
    raise SystemExit(1)
raise SystemExit(0 if math.isfinite(value) and 0 <= value <= 1 else 1)
PY
then
  echo "search effort must be a finite number in the inclusive range 0..1" >&2
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
if [[ "$diagnostic_profile_only" == "1" && "$resume_after_live" != "1" ]]; then
  echo "--diagnostic-profile-only requires --resume" >&2
  exit 2
fi
if [[ "$resume_concurrent" == "1" && "$resume_after_live" != "1" ]]; then
  echo "--resume-concurrent requires --resume" >&2
  exit 2
fi
if [[ "$resume_concurrent" == "1" && "$diagnostic_profile_only" == "1" ]]; then
  echo "--resume-concurrent is incompatible with --diagnostic-profile-only" >&2
  exit 2
fi
if [[ "$resume_concurrent_only" == "1" && "$profile_count" != "0" ]]; then
  echo "--resume-concurrent-only requires --profile-count 0 so the run remains concurrency-only" >&2
  exit 2
fi
for required in "$antfly_bin" "$vdbbench_python" "$sampler"; do
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
  LOG_FILE=/dev/null "$vdbbench_python" -m vectordb_bench.cli.vectordbbench antflyaknn --help
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
  python3 - "$run_root/run-config.json" "$repo_root" "$vdbbench_root" "$vdbbench_case" "$batch_size" "$load_workers" "$query_concurrency" "$query_seconds" "$process_memory_budget_mb" "$profile_count" "$profile_dataset" "$search_effort" "$native_hbc" "$vector_blocks" "$vector_block_encoding" "$recursive_topology_max_workspace_mb" "$topology_rebuild_algorithm" "$centroid_directory_mode" "$flat_probe_count" <<'PY'
import json
import os
import subprocess
import sys

experiment_environment_names = (
    "ANTFLY_DENSE_POSTING_IDLE_MAX_POSTINGS_PER_INDEX",
)

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
    search_effort,
    native_hbc,
    vector_blocks,
    vector_block_encoding,
    recursive_topology_max_workspace_mb,
    topology_rebuild_algorithm,
    centroid_directory_mode,
    flat_probe_count,
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
            "search_effort": float(search_effort) if search_effort else None,
            "native_hbc_posting_store": native_hbc == "1",
            "native_exact_vector_blocks": vector_blocks == "1",
            "vector_block_encoding": vector_block_encoding,
            "recursive_topology_max_workspace_mb": (
                int(recursive_topology_max_workspace_mb)
                if recursive_topology_max_workspace_mb
                else None
            ),
            "topology_rebuild_algorithm": topology_rebuild_algorithm,
            "centroid_directory_mode": centroid_directory_mode or None,
            "flat_centroid_probe_count": int(flat_probe_count) if flat_probe_count else None,
            "load_lifecycle": "public_api_online_incremental",
            "builder": "server_selected; do not assume recursive from client batch size",
            "experiment_environment": {
                name: os.environ[name]
                for name in experiment_environment_names
                if name in os.environ
            },
        },
        handle,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")
PY
  python3 "$sampler" --capture-wired-baseline "$run_root/wired-baseline.json"
else
  python3 - "$run_root/resume-config${label_suffix}.json" "$repo_root" "$antfly_bin" "$vdbbench_root" "$process_memory_budget_mb" "$profile_count" "$profile_dataset" "$search_effort" "$cold_label" "$warm_label" "$concurrent_label" "$native_hbc" "$vector_blocks" "$vector_block_encoding" "$diagnostic_profile_only" "$resume_concurrent" "$resume_concurrent_only" <<'PY'
import json
import os
import subprocess
import sys

experiment_environment_names = (
    "ANTFLY_DENSE_POSTING_IDLE_MAX_POSTINGS_PER_INDEX",
)

(
    out_path,
    repo_root,
    antfly_bin,
    vdbbench_root,
    memory_budget_mb,
    profile_count,
    profile_dataset,
    search_effort,
    cold_label,
    warm_label,
    concurrent_label,
    native_hbc,
    vector_blocks,
    vector_block_encoding,
    diagnostic_profile_only,
    resume_concurrent,
    resume_concurrent_only,
) = sys.argv[1:]
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "antfly_git_head": subprocess.check_output(
                ["git", "-C", repo_root, "rev-parse", "HEAD"], text=True
            ).strip(),
            "antfly_bin": antfly_bin,
            "vdbbench_root": vdbbench_root,
            "process_memory_budget_mb": (
                int(memory_budget_mb) if memory_budget_mb else None
            ),
            "public_profile_count": int(profile_count),
            "public_profile_dataset": profile_dataset,
            "search_effort": float(search_effort) if search_effort else None,
            "cold_label": cold_label,
            "warm_label": warm_label,
            "concurrent_label": concurrent_label if resume_concurrent == "1" else None,
            "native_hbc_posting_store": native_hbc == "1",
            "native_exact_vector_blocks": vector_blocks == "1",
            "vector_block_encoding": vector_block_encoding,
            "diagnostic_profile_only": diagnostic_profile_only == "1",
            "resume_concurrent": resume_concurrent == "1",
            "resume_concurrent_only": resume_concurrent_only == "1",
            "experiment_environment": {
                name: os.environ[name]
                for name in experiment_environment_names
                if name in os.environ
            },
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
rss_sampler_pid=
metrics_sampler_pid=

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
  if [[ -n "$rss_sampler_pid" ]]; then
    kill "$rss_sampler_pid" 2>/dev/null || true
    wait "$rss_sampler_pid" 2>/dev/null || true
    rss_sampler_pid=
  fi
  if [[ -n "$metrics_sampler_pid" ]]; then
    kill "$metrics_sampler_pid" 2>/dev/null || true
    wait "$metrics_sampler_pid" 2>/dev/null || true
    metrics_sampler_pid=
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
  local memory_args=()
  if [[ -n "$process_memory_budget_mb" ]]; then
    # Use the standalone contract rather than leaking the canonical process
    # budget environment variable into linked runtimes that also consume it.
    memory_args+=("--process-memory-budget-mb" "$process_memory_budget_mb")
  fi
  "$antfly_bin" standalone \
    --host 127.0.0.1 \
    --port "$port" \
    --health-port "$health_port" \
    --auth false \
    ${memory_args[@]+"${memory_args[@]}"} \
    --data-dir "$run_root/data" \
    >"$run_root/$log_name" 2>&1 &
  server_pid=$!
  printf '%s\n' "$server_pid" >"$run_root/antfly.pid"

  # Health alone proves only the operator listener. Require one harmless public
  # metadata read so the qualification's readiness boundary is end to end.
  for _ in $(seq 1 300); do
    if curl -fsS "http://127.0.0.1:$health_port/healthz" >/dev/null 2>&1 &&
      curl -fsS "http://127.0.0.1:$port/db/v1/tables" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  curl -fsS "http://127.0.0.1:$health_port/healthz" >/dev/null
  curl -fsS "http://127.0.0.1:$port/db/v1/tables" >/dev/null
}

wait_public_index_ready() {
  local suffix=$1
  local status_path="$run_root/index-readiness${suffix}.json"
  local attempts=0
  while [[ "$attempts" -lt 1800 ]]; do
    attempts=$((attempts + 1))
    if curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$status_path" 2>/dev/null &&
      python3 - "$status_path" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
status = payload.get("status", {})
ready = (
    status.get("readiness", {}).get("state") == "ready"
    and not status.get("backfill_active", True)
    and not status.get("dense_publish_pending", True)
    and not status.get("dense_vector_projection_pending", False)
    and status.get("hbc_posting", {}).get("dirty_postings", 0) == 0
)
raise SystemExit(0 if ready else 1)
PY
    then
      return
    fi
    sleep 1
  done
  echo "public vector index did not become query-ready within 30 minutes; see $status_path" >&2
  return 1
}

start_rss_sampler() {
  local samples_path=$1
  (
    printf 'wall_time_s\trss_kib\n'
    while kill -0 "$server_pid" 2>/dev/null; do
      local rss_kib
      rss_kib=$(ps -o rss= -p "$server_pid" 2>/dev/null | tr -d '[:space:]') || break
      if [[ "$rss_kib" =~ ^[0-9]+$ ]]; then
        printf '%s\t%s\n' "$(date +%s)" "$rss_kib"
      fi
      sleep 0.2
    done
  ) >"$samples_path" &
  rss_sampler_pid=$!
}

start_metrics_sampler() {
  local samples_path=$1
  (
    while kill -0 "$server_pid" 2>/dev/null; do
      printf '# snapshot_wall_time_s %s\n' "$(date +%s)"
      curl -fsS "http://127.0.0.1:$health_port/metrics" 2>/dev/null || true
      sleep 2
    done
  ) >"$samples_path" &
  metrics_sampler_pid=$!
}

stop_metrics_sampler() {
  if [[ -n "$metrics_sampler_pid" ]]; then
    kill "$metrics_sampler_pid" 2>/dev/null || true
    wait "$metrics_sampler_pid" 2>/dev/null || true
    metrics_sampler_pid=
  fi
}

stop_rss_sampler() {
  local samples_path=$1
  local summary_path=$2
  if [[ -n "$rss_sampler_pid" ]]; then
    kill "$rss_sampler_pid" 2>/dev/null || true
    wait "$rss_sampler_pid" 2>/dev/null || true
    rss_sampler_pid=
  fi
  python3 - "$samples_path" "$summary_path" <<'PY'
import json
import pathlib
import sys

samples_path = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
samples = []
if samples_path.exists():
    for line in samples_path.read_text(encoding="utf-8").splitlines()[1:]:
        fields = line.split("\t")
        if len(fields) != 2:
            continue
        samples.append((int(fields[0]), int(fields[1])))
with summary_path.open("w", encoding="utf-8") as handle:
    json.dump(
        {
            "samples": len(samples),
            "peak_rss_kib": max((rss for _, rss in samples), default=0),
            "peak_rss_bytes": max((rss for _, rss in samples), default=0) * 1024,
            "first_wall_time_s": samples[0][0] if samples else None,
            "last_wall_time_s": samples[-1][0] if samples else None,
            "interval_seconds": 0.2,
            "source": "ps_rss",
        },
        handle,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")
PY
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
  if [[ -n "$search_effort" ]]; then
    "$vdbbench_python" "$script_dir/profile_vdbbench_public_query.py" \
      --dataset "$profile_dataset" \
      --port "$port" \
      --count "$profile_count" \
      --output "$run_root/public-query-profile${suffix}.json" \
      --search-effort "$search_effort" \
      >"$run_root/public-query-profile${suffix}.log" 2>&1
  else
    "$vdbbench_python" "$script_dir/profile_vdbbench_public_query.py" \
      --dataset "$profile_dataset" \
      --port "$port" \
      --count "$profile_count" \
      --output "$run_root/public-query-profile${suffix}.json" \
      >"$run_root/public-query-profile${suffix}.log" 2>&1
  fi
  mark_phase "public_profile${suffix}_end"
}

capture_footprint_once() {
  local out=$1
  local timeline=$2
  local log=$3
  local baseline=$4
  local capture_out="${out}.capture.$$"
  local capture_timeline="${timeline}.capture.$$"
  local sampled=0

  # macOS vmmap can suspend or heavily contend with the target. The kernel's
  # phys_footprint ledger already retains the process high-water mark, so one
  # post-phase sample captures the timed phase without manufacturing periodic
  # latency or load stalls inside it.
  python3 "$sampler" \
    --pid-file "$run_root/antfly.pid" \
    --wired-baseline-file "$baseline" \
    --out "$capture_out" \
    --timeline "$capture_timeline" \
    --interval 0.2 \
    >"$log" 2>&1 &
  sampler_pid=$!
  for _ in $(seq 1 1200); do
    if python3 - "$capture_out" <<'PY' >/dev/null 2>&1
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    if int(json.load(handle).get("samples", 0)) < 1:
        raise SystemExit(1)
PY
    then
      sampled=1
      break
    fi
    sleep 0.05
  done
  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  sampler_pid=
  if [[ "$sampled" != "1" ]]; then
    echo "footprint sampler did not produce a valid post-phase sample: $log" >&2
    exit 1
  fi
  mv "$capture_out" "$out"
  if [[ -f "$capture_timeline" ]]; then
    mv "$capture_timeline" "$timeline"
  fi
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
  if [[ -n "$search_effort" ]]; then
    cli_args+=(--search-effort "$search_effort")
  fi
  (
    cd "$vdbbench_root"
    ANTFLY_VDBBENCH_KEEP_DEFAULT_FULL_TEXT=0 \
    ANTFLY_VDBBENCH_READ_ONLY_REUSE=${ANTFLY_VDBBENCH_READ_ONLY_REUSE:-0} \
    ANTFLY_BENCH_STATUS=1 \
    ANTFLY_BENCH_STATUS_INTERVAL=5 \
    DATASET_LOCAL_DIR=${DATASET_LOCAL_DIR:-/private/tmp/vdbbench-dataset} \
    RESULTS_LOCAL_DIR="$run_root/results" \
    NUM_PER_BATCH="$batch_size" \
    IR_DATASETS_HOME=${IR_DATASETS_HOME:-/private/tmp/vdbbench-ir} \
    IR_DATASETS_TMP=${IR_DATASETS_TMP:-/private/tmp/vdbbench-ir-tmp} \
    LOG_FILE="$run_root/vdbbench-framework.log" \
    PYTHONPATH=. \
    "$vdbbench_python" -m vectordb_bench.cli.vectordbbench "${cli_args[@]}" "$@"
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
  start_rss_sampler "$run_root/rss-restart${label_suffix}.tsv"
  start_metrics_sampler "$run_root/metrics-live-restart${label_suffix}.prom"
  mark_phase restart_ready
  wait_public_index_ready "$label_suffix"
  mark_phase restart_index_ready
  if [[ "$diagnostic_profile_only" != "1" ]]; then
    if [[ "$resume_concurrent_only" != "1" ]]; then
      ANTFLY_VDBBENCH_READ_ONLY_REUSE=1 run_vdbbench "$cold_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
        >"$run_root/vdbbench-reopened-cold${label_suffix}.log" 2>&1
      validate_vdbbench_result "$cold_label" 0 1
      mark_phase reopened_cold_query_end
      ANTFLY_VDBBENCH_READ_ONLY_REUSE=1 run_vdbbench "$warm_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
        >"$run_root/vdbbench-reopened-warm${label_suffix}.log" 2>&1
      validate_vdbbench_result "$warm_label" 0 1
      mark_phase reopened_warm_query_end
    fi
    if [[ "$resume_concurrent" == "1" ]]; then
      ANTFLY_VDBBENCH_READ_ONLY_REUSE=1 run_vdbbench "$concurrent_label" --skip-drop-old --skip-load --search-concurrent --skip-search-serial \
        >"$run_root/vdbbench-reopened-concurrent${label_suffix}.log" 2>&1
      validate_vdbbench_result "$concurrent_label" 0 0
      mark_phase reopened_concurrent_query_end
    fi
  fi
  run_public_profile "$label_suffix"
  curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-after-restart${label_suffix}.txt"
  curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-after-restart${label_suffix}.json"
  capture_footprint_once \
    "$run_root/footprint-restart${label_suffix}.json" \
    "$run_root/footprint-restart${label_suffix}.jsonl" \
    "$run_root/footprint-restart${label_suffix}.log" \
    "$run_root/wired-baseline${label_suffix}.json"
  stop_rss_sampler \
    "$run_root/rss-restart${label_suffix}.tsv" \
    "$run_root/rss-restart${label_suffix}.json"
  stop_metrics_sampler
  if [[ "$diagnostic_profile_only" != "1" ]]; then
    python3 "$script_dir/summarize_vdbbench_qualification.py" "$run_root"
  fi
  exit 0
fi

mark_phase server_start
start_server antfly-initial.log
start_rss_sampler "$run_root/rss-live.tsv"
start_metrics_sampler "$run_root/metrics-live.prom"
mark_phase live_load_and_query_start
run_vdbbench "$live_label" --drop-old --load --search-concurrent --search-serial \
  >"$run_root/vdbbench-live.log" 2>&1
validate_vdbbench_result "$live_label" "$expected_docs" 1
mark_phase live_load_and_query_end
curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-before-restart.txt"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench" >"$run_root/table-before-restart.json"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-before-restart.json"
capture_footprint_once \
  "$run_root/footprint.json" \
  "$run_root/footprint.jsonl" \
  "$run_root/footprint.log" \
  "$run_root/wired-baseline.json"
stop_rss_sampler "$run_root/rss-live.tsv" "$run_root/rss-live.json"
stop_metrics_sampler

mark_phase restart_begin
stop_server
mark_phase shutdown_complete
start_server antfly-reopened.log
start_rss_sampler "$run_root/rss-restart.tsv"
mark_phase restart_ready
wait_public_index_ready ""
mark_phase restart_index_ready

ANTFLY_VDBBENCH_READ_ONLY_REUSE=1 run_vdbbench "$cold_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
  >"$run_root/vdbbench-reopened-cold.log" 2>&1
validate_vdbbench_result "$cold_label" 0 1
mark_phase reopened_cold_query_end
ANTFLY_VDBBENCH_READ_ONLY_REUSE=1 run_vdbbench "$warm_label" --skip-drop-old --skip-load --skip-search-concurrent --search-serial \
  >"$run_root/vdbbench-reopened-warm.log" 2>&1
validate_vdbbench_result "$warm_label" 0 1
mark_phase reopened_warm_query_end
run_public_profile ""
curl -fsS "http://127.0.0.1:$health_port/metrics" >"$run_root/metrics-after-restart.txt"
curl -fsS "http://127.0.0.1:$port/db/v1/tables/vdbbench/indexes/vec" >"$run_root/index-after-restart.json"
capture_footprint_once \
  "$run_root/footprint-restart.json" \
  "$run_root/footprint-restart.jsonl" \
  "$run_root/footprint-restart.log" \
  "$run_root/wired-baseline.json"
stop_rss_sampler "$run_root/rss-restart.tsv" "$run_root/rss-restart.json"

python3 "$script_dir/summarize_vdbbench_qualification.py" "$run_root"
