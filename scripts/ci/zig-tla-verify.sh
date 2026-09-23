#!/usr/bin/env bash
set -euo pipefail

trace_root="${ANTFLY_TLA_TRACE_DIR:-${RUNNER_TEMP:-/tmp}/antfly-tla-traces}"

run_model_check="${ANTFLY_TLA_MODEL_CHECK:-false}"
run_trace_validate="${ANTFLY_TLA_TRACE_VALIDATE:-false}"

case "$run_model_check" in
  true|false) ;;
  *) echo "ANTFLY_TLA_MODEL_CHECK must be true or false, got: $run_model_check" >&2; exit 2 ;;
esac

case "$run_trace_validate" in
  true|false) ;;
  *) echo "ANTFLY_TLA_TRACE_VALIDATE must be true or false, got: $run_trace_validate" >&2; exit 2 ;;
esac

run_group() {
  local name="$1"
  shift

  echo "::group::$name"
  set +e
  (
    set -euo pipefail
    "$@"
  )
  local status="$?"
  set -e
  echo "::endgroup::"
  return "$status"
}

run_tlc() {
  make -C zig tla-check
}

download_tla_tools() {
  make -C zig tla-tools
}

extract_raft_trace() {
  (
    cd zig
    ANTFLY_TRACE_DIR="$trace_run/raft" python3 tools/run_bounded_zig_build.py --zig zig -- build -Dwith_tla=true antfly-raft-test
  )
  collect_traces raft
}

validate_raft_trace() {
  make -C zig tla-trace-raft TRACE_FILES="$trace_run/raft/*.ndjson"
}

extract_txn_trace() {
  (
    cd zig
    ANTFLY_TRACE_DIR="$trace_run/txn" python3 tools/run_bounded_zig_build.py --zig zig -- build -Dwith_tla=true antfly-storage-db-txn-test
  )
  collect_traces txn
}

validate_txn_trace() {
  make -C zig tla-trace-txn TRACE_FILES="$trace_run/txn/*.ndjson"
}

collect_traces() {
  local kind="$1"
  local files=("$trace_run/$kind/"*.ndjson)
  if [[ ! -s "${files[0]}" ]]; then
    echo "No $kind traces produced in $trace_run/$kind" >&2
    return 1
  fi
  wc -l "${files[@]}"
}

if [[ "$run_model_check" == "true" ]]; then
  run_group "Run TLC on all specs" run_tlc
fi

if [[ "$run_trace_validate" == "true" ]]; then
  mkdir -p "$trace_root"
  trace_root="$(cd "$trace_root" && pwd)"
  trace_run="$(mktemp -d "$trace_root/run.XXXXXX")"
  mkdir "$trace_run/raft" "$trace_run/txn"
  echo "Trace evidence: $trace_run"
  run_group "Download TLA+ tools" download_tla_tools
  run_group "Run raft tests and extract traces" extract_raft_trace
  run_group "Validate raft traces" validate_raft_trace
  run_group "Run transaction tests and extract traces" extract_txn_trace
  run_group "Validate transaction traces" validate_txn_trace
fi
