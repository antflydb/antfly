#!/usr/bin/env bash
set -euo pipefail

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
    ANTFLY_TRACE_FILE=/tmp/raft-trace.ndjson zig build -Dwith_tla=true raft-test
  )
  test -s /tmp/raft-trace.ndjson
  echo "Raft trace lines: $(wc -l < /tmp/raft-trace.ndjson)"
}

validate_raft_trace() {
  make -C zig tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson
}

extract_txn_trace() {
  (
    cd zig
    ANTFLY_TRACE_FILE=/tmp/txn-trace.ndjson zig build -Dwith_tla=true lib-db-txn-test
  )
  test -s /tmp/txn-trace.ndjson
  echo "Transaction trace lines: $(wc -l < /tmp/txn-trace.ndjson)"
}

validate_txn_trace() {
  make -C zig tla-trace-txn TRACE_FILES=/tmp/txn-trace.ndjson
}

if [[ "$run_model_check" == "true" ]]; then
  run_group "Run TLC on all specs" run_tlc
fi

if [[ "$run_trace_validate" == "true" ]]; then
  run_group "Download TLA+ tools" download_tla_tools
  run_group "Run raft tests and extract traces" extract_raft_trace
  run_group "Validate raft traces" validate_raft_trace
  run_group "Run transaction tests and extract traces" extract_txn_trace
  run_group "Validate transaction traces" validate_txn_trace
fi
