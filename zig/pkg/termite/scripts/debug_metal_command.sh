#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PKG_DIR="$ROOT_DIR/pkg/termite"
CALLER_DIR="$PWD"
DIAG_DIR="$HOME/Library/Logs/DiagnosticReports"
SYSTEM_DIAG_DIR="/Library/Logs/DiagnosticReports"

OUT_DIR="${OUT_DIR:-$PKG_DIR/.debug/metal-command-$(date +%Y%m%d-%H%M%S)}"
TIMEOUT_SECS="${TIMEOUT_SECS:-60}"
METAL_VALIDATE="${METAL_VALIDATE:-1}"
METAL_SHADER_VALIDATE_EXPLICIT="${METAL_SHADER_VALIDATE+x}"
METAL_SHADER_VALIDATE="${METAL_SHADER_VALIDATE:-$METAL_VALIDATE}"
SAMPLE_AFTER_SECS="${SAMPLE_AFTER_SECS:-5}"
SAMPLE_DURATION_SECS="${SAMPLE_DURATION_SECS:-5}"
SAMPLE_INTERVAL_MS="${SAMPLE_INTERVAL_MS:-5}"
WATCH_PATTERN="${WATCH_PATTERN:-}"
RUN_CWD="${RUN_CWD:-$CALLER_DIR}"
LABEL="${LABEL:-metal-command}"
ALLOW_BROAD_METAL_TEST="${TERMITE_ALLOW_BROAD_METAL_TEST:-0}"
ALLOW_BROAD_METAL_UNIT_CHUNK="${TERMITE_ALLOW_BROAD_METAL_UNIT_CHUNK:-0}"
SKIP_POSTCAPTURE="${TERMITE_METAL_SKIP_POSTCAPTURE:-0}"
PREBUILD_ONLY="${PREBUILD_ONLY:-0}"
LAUNCH_ONLY="${LAUNCH_ONLY:-0}"
LAUNCH_ONLY_FILTER="${LAUNCH_ONLY_FILTER:-__termite_no_matching_tests__}"
PREBUILT_TEST_BINARY="${PREBUILT_TEST_BINARY:-}"
SYNC_MARKERS="${TERMITE_METAL_SYNC_MARKERS:-1}"
REVERSE_TESTS="${REVERSE_TESTS:-0}"
TEST_OFFSET="${TEST_OFFSET:-0}"
TEST_LIMIT="${TEST_LIMIT:-0}"
RUNTIME_CURRENT_FILE="${TERMITE_TEST_CURRENT_FILE:-}"
RUNTIME_TRACE_FILE="${TERMITE_TEST_TRACE_FILE:-}"
RUNTIME_TEST_OFFSET="${TERMITE_TEST_RUNTIME_OFFSET:-}"
RUNTIME_TEST_LIMIT="${TERMITE_TEST_RUNTIME_LIMIT:-}"

usage() {
  cat >&2 <<EOF
Usage:
  bash pkg/termite/scripts/debug_metal_command.sh command [options] -- <command> [args...]
  bash pkg/termite/scripts/debug_metal_command.sh -- <command> [args...]
  bash pkg/termite/scripts/debug_metal_command.sh embed [options] -- <termite embed args...>
  bash pkg/termite/scripts/debug_metal_command.sh e2e [options] [pytest -k expression]
  bash pkg/termite/scripts/debug_metal_command.sh unit [options] [test-name-regex]

Common options:
  --label NAME          Bundle label
  --out-dir DIR         Output directory
  --timeout SECS        Timeout before killing command
  --watch PATTERN       pgrep -f pattern to sample instead of command pid
  --cwd DIR             Working directory for command
  --no-validate         Do not set MTL_DEBUG_LAYER / MTL_SHADER_VALIDATION
  --api-validate        Set MTL_DEBUG_LAYER only, without MTL_SHADER_VALIDATION

Unit mode environment:
  RUN_MODE=chunked|isolated
  CHUNK_SIZE=8
  USE_PREBUILT_UNIT=1   Build the Zig test binary once, then run filters directly
  METAL_SHADER_VALIDATE=1 Opt into shader validation for a narrowed unit repro
  LIST_ONLY=1           List candidate tests without launching Metal
  PREBUILD_ONLY=1       Build the Metal-enabled runtime-filtered test binary
                        and stop before launching it.
  LAUNCH_ONLY=1         Launch an existing prebuilt test binary with a filter
                        expected to match no tests, then stop.
  LAUNCH_ONLY_FILTER=... Override the no-match filter used by LAUNCH_ONLY.
  PREBUILT_TEST_BINARY=... Path to the test binary for LAUNCH_ONLY. If omitted,
                        unit mode reads prebuilt-test-binary.txt from --out-dir.
  REVERSE_TESTS=1       Run the matched test list in reverse order.
  TEST_OFFSET=N         Skip the first N matched tests after optional reversal.
  TEST_LIMIT=N          Run at most N matched tests after optional offset. Use
                        0 for no limit.
  TERMITE_TEST_RUNTIME_OFFSET=N
                        Skip the first N tests in the Zig runner's selected
                        builtin order, inside the test process.
  TERMITE_TEST_RUNTIME_LIMIT=N
                        Run at most N tests in the Zig runner's selected
                        builtin order, inside the test process.
  TERMITE_TEST_TRACE_FILE=PATH
                        Append and fsync each test start inside the Zig test
                        runner. Unit mode sets this automatically.
  RESUME=1              Resume an existing --out-dir, skipping PASS entries
  RESUME_SKIP_CURRENT=1 Also skip current_test.txt from an interrupted run
                        (default when RESUME=1)
  TERMITE_METAL_SKIP_POSTCAPTURE=1
                        Skip log-show and diagnostic report copying after a
                        command exits. Useful for watchdog bisection when the
                        per-test stdout/exit status and progress markers are
                        enough.
  TERMITE_METAL_SYNC_MARKERS=0
                        Do not call sync after updating debug breadcrumbs.
  TERMITE_ALLOW_BROAD_METAL_TEST=1
                        Override the safety guard that refuses unfiltered
                        "zig build test" in command mode.
  TERMITE_ALLOW_BROAD_METAL_UNIT_CHUNK=1
                        Override the safety guard that refuses running every
                        matched Metal unit test in one chunk/process.

Examples:
  bash pkg/termite/scripts/debug_metal_command.sh unit 'metal_compute|metal_runtime'
  LIST_ONLY=1 bash pkg/termite/scripts/debug_metal_command.sh unit 'MetalTensor|ReservedHiddenCarrier'
  bash pkg/termite/scripts/debug_metal_command.sh embed -- ~/.termite/models/antflydb/clipclap --text 'hello world'
  bash pkg/termite/scripts/debug_metal_command.sh e2e test_audio_embedding
  bash pkg/termite/scripts/debug_metal_command.sh command -- ./zig-out/bin/termite --help
EOF
}

quote_command() {
  local first=1
  for arg in "$@"; do
    if [[ "$first" -eq 0 ]]; then
      printf " "
    fi
    first=0
    printf "%q" "$arg"
  done
  printf "\n"
}

absolute_path_from_caller() {
  local path="$1"
  if [[ "$path" == /* ]]; then
    printf "%s\n" "$path"
  else
    printf "%s/%s\n" "$CALLER_DIR" "$path"
  fi
}

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

flush_debug_markers() {
  if is_truthy "$SYNC_MARKERS"; then
    sync >/dev/null 2>&1 || true
  fi
}

write_marker_file() {
  local path="$1"
  local value="$2"
  printf "%s\n" "$value" >"$path"
  flush_debug_markers
}

is_filtered_zig_test_command() {
  local arg
  for arg in "$@"; do
    if [[ "$arg" == "--test-filter" || "$arg" == --test-filter=* ]]; then
      return 0
    fi
  done
  return 1
}

is_broad_zig_build_test_command() {
  if [[ "$#" -lt 3 ]]; then
    return 1
  fi
  if [[ "$1" != "zig" || "$2" != "build" ]]; then
    return 1
  fi
  if [[ "$3" != "test" && "$3" != "termite-test" ]]; then
    return 1
  fi
  if is_filtered_zig_test_command "$@"; then
    return 1
  fi
  return 0
}

refuse_broad_zig_test_if_needed() {
  if ! is_broad_zig_build_test_command "$@"; then
    return 0
  fi
  if is_truthy "$ALLOW_BROAD_METAL_TEST"; then
    return 0
  fi
  cat >&2 <<EOF
Refusing unfiltered "zig build test" or "zig build termite-test" through the
Metal debug wrapper.

The broad test target can launch many Metal runtime tests in one process and
has previously produced a system watchdog reboot without preserving the failing
test name. Use the unit isolator instead:

  LIST_ONLY=1 bash pkg/termite/scripts/debug_metal_command.sh unit 'metal|Metal'
  RUN_MODE=isolated USE_PREBUILT_UNIT=1 bash pkg/termite/scripts/debug_metal_command.sh unit --api-validate 'metal|Metal'

To override intentionally, set TERMITE_ALLOW_BROAD_METAL_TEST=1.
EOF
  return 126
}

run_capture() {
  if [[ "$#" -eq 0 ]]; then
    usage
    return 2
  fi

  OUT_DIR="$(absolute_path_from_caller "$OUT_DIR")"
  mkdir -p "$OUT_DIR"

  local start_epoch start_iso start_local
  start_epoch="$(date +%s)"
  start_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  start_local="$(date +"%Y-%m-%d %H:%M:%S")"

  quote_command "$@" >"$OUT_DIR/command.txt"

  local guard_status=0
  refuse_broad_zig_test_if_needed "$@" || guard_status=$?
  if [[ "$guard_status" -ne 0 ]]; then
    local refused_status="$guard_status"
    cat >"$OUT_DIR/stdout.txt" <<EOF
Refused unsafe broad Metal test command.

Command:
$(cat "$OUT_DIR/command.txt")

Use unit mode so each test or chunk gets its own bundle and progress marker.
Override with TERMITE_ALLOW_BROAD_METAL_TEST=1 only when deliberately running
the full suite.
EOF
    echo "$refused_status" >"$OUT_DIR/exitcode.txt"
    cat >"$OUT_DIR/summary.txt" <<EOF
exit_code=$refused_status
label=$LABEL
refused=broad_zig_build_test

Next checks:
1. LIST_ONLY=1 bash pkg/termite/scripts/debug_metal_command.sh unit 'metal|Metal'
2. RUN_MODE=isolated USE_PREBUILT_UNIT=1 bash pkg/termite/scripts/debug_metal_command.sh unit --api-validate 'metal|Metal'
EOF
    echo "Refused broad zig build test. Bundle written to $OUT_DIR"
    return "$refused_status"
  fi

  cat >"$OUT_DIR/README.txt" <<EOF
Generic Metal debug bundle

Started:       $start_iso
Started local: $start_local
Repo:          $ROOT_DIR
Label:         $LABEL
Working dir:   $RUN_CWD
Timeout:       $TIMEOUT_SECS seconds
Validation:    $METAL_VALIDATE
Shader val.:   $METAL_SHADER_VALIDATE
Watch pattern: ${WATCH_PATTERN:-<command pid>}

Artifacts:
- command.txt: shell-escaped command
- stdout.txt: command stdout/stderr
- exitcode.txt: command exit status
- ps.txt: process snapshot, if captured
- sample.txt: sample of the command or watched process, if captured
- log-show.txt: filtered unified log output since start
- diagnostic-reports/: new DiagnosticReports files
- diagnostic-reports-before.txt: DiagnosticReports snapshot before launch
- summary.txt: exit status and next checks
EOF

  printf "%s\n" "$start_epoch" >"$OUT_DIR/started_epoch.txt"
  {
    find "$DIAG_DIR" "$SYSTEM_DIAG_DIR" -maxdepth 2 -type f \
      \( -name '*.ips' -o -name '*.crash' -o -name '*.panic' -o -name '*.diag' \) \
      -print 2>/dev/null || true
  } | sort >"$OUT_DIR/diagnostic-reports-before.txt"

  echo "Output directory: $OUT_DIR"
  echo "Label: $LABEL"
  echo "Timeout: $TIMEOUT_SECS"
  echo "Validation: $METAL_VALIDATE"
  echo "Shader validation: $METAL_SHADER_VALIDATE"
  echo "Watch pattern: ${WATCH_PATTERN:-<command pid>}"

  local command_pid="" watcher_pid=""
  cleanup_capture() {
    if [[ -n "$watcher_pid" ]]; then
      kill "$watcher_pid" >/dev/null 2>&1 || true
    fi
    if [[ -n "$command_pid" ]]; then
      kill "$command_pid" >/dev/null 2>&1 || true
    fi
  }

  capture_sample() {
    local pid="$1"
    ps -o pid,ppid,rss,vsz,%mem,etime,command -p "$pid" >"$OUT_DIR/ps.txt" 2>/dev/null || true
    sample "$pid" "$SAMPLE_DURATION_SECS" "$SAMPLE_INTERVAL_MS" -file "$OUT_DIR/sample.txt" >/dev/null 2>&1 || true
  }

  find_sample_pid() {
    if [[ -n "$WATCH_PATTERN" ]]; then
      pgrep -n -f "$WATCH_PATTERN" 2>/dev/null || true
      return 0
    fi
    printf "%s\n" "$command_pid"
  }

  watch_command() {
    local pid="$1"
    local elapsed=0
    local sampled=0
    while kill -0 "$pid" >/dev/null 2>&1; do
      if [[ "$sampled" -eq 0 && "$elapsed" -ge "$SAMPLE_AFTER_SECS" ]]; then
        local sample_pid
        sample_pid="$(find_sample_pid)"
        if [[ -n "$sample_pid" ]]; then
          capture_sample "$sample_pid"
          sampled=1
        fi
      fi
      if [[ "$elapsed" -ge "$TIMEOUT_SECS" ]]; then
        echo "timeout after ${TIMEOUT_SECS}s" >>"$OUT_DIR/stdout.txt"
        kill "$pid" >/dev/null 2>&1 || true
        return 124
      fi
      sleep 1
      elapsed=$((elapsed + 1))
    done
    if [[ "$sampled" -eq 0 ]]; then
      local sample_pid
      sample_pid="$(find_sample_pid)"
      if [[ -n "$sample_pid" ]]; then
        capture_sample "$sample_pid"
      fi
    fi
    return 0
  }

  pushd "$RUN_CWD" >/dev/null

  set +e
  if [[ "$METAL_VALIDATE" == "1" && "$METAL_SHADER_VALIDATE" == "1" ]]; then
    TERMITE_TEST_CURRENT_FILE="$RUNTIME_CURRENT_FILE" \
    TERMITE_TEST_TRACE_FILE="$RUNTIME_TRACE_FILE" \
    TERMITE_TEST_RUNTIME_OFFSET="$RUNTIME_TEST_OFFSET" \
    TERMITE_TEST_RUNTIME_LIMIT="$RUNTIME_TEST_LIMIT" \
    MTL_DEBUG_LAYER=1 \
    MTL_SHADER_VALIDATION=1 \
    "$@" >"$OUT_DIR/stdout.txt" 2>&1 &
  elif [[ "$METAL_VALIDATE" == "1" ]]; then
    TERMITE_TEST_CURRENT_FILE="$RUNTIME_CURRENT_FILE" \
    TERMITE_TEST_TRACE_FILE="$RUNTIME_TRACE_FILE" \
    TERMITE_TEST_RUNTIME_OFFSET="$RUNTIME_TEST_OFFSET" \
    TERMITE_TEST_RUNTIME_LIMIT="$RUNTIME_TEST_LIMIT" \
    MTL_DEBUG_LAYER=1 \
    env -u MTL_SHADER_VALIDATION \
    "$@" >"$OUT_DIR/stdout.txt" 2>&1 &
  else
    TERMITE_TEST_CURRENT_FILE="$RUNTIME_CURRENT_FILE" \
    TERMITE_TEST_TRACE_FILE="$RUNTIME_TRACE_FILE" \
    TERMITE_TEST_RUNTIME_OFFSET="$RUNTIME_TEST_OFFSET" \
    TERMITE_TEST_RUNTIME_LIMIT="$RUNTIME_TEST_LIMIT" \
    env -u MTL_DEBUG_LAYER -u MTL_SHADER_VALIDATION \
    "$@" >"$OUT_DIR/stdout.txt" 2>&1 &
  fi
  command_pid=$!
  watch_command "$command_pid" &
  watcher_pid=$!

  wait "$command_pid"
  local command_status=$?
  wait "$watcher_pid"
  local watch_status=$?
  set -e
  cleanup_capture

  popd >/dev/null

  if [[ "$command_status" -eq 0 && "$watch_status" -eq 124 ]]; then
    command_status=124
  fi

  echo "$command_status" >"$OUT_DIR/exitcode.txt"

  if is_truthy "$SKIP_POSTCAPTURE"; then
    cat >"$OUT_DIR/summary.txt" <<EOF
exit_code=$command_status
started_utc=$start_iso
label=$LABEL
timeout_secs=$TIMEOUT_SECS
validation=$METAL_VALIDATE
shader_validation=$METAL_SHADER_VALIDATE
watch_pattern=${WATCH_PATTERN:-}
postcapture_skipped=1

Next checks:
1. sed -n '1,220p' "$OUT_DIR/stdout.txt"
2. cat "$OUT_DIR/exitcode.txt"
3. If the machine rebooted, compare "$OUT_DIR/started_epoch.txt" with /Library/Logs/DiagnosticReports/Retired/panic-base-*.panic
EOF

    echo
    echo "Done. Bundle written to $OUT_DIR"
    echo "exit code: $command_status"
    echo "Post-capture skipped."
    echo "Open:"
    echo "  $OUT_DIR/stdout.txt"
    echo "  $OUT_DIR/exitcode.txt"
    return "$command_status"
  fi

  /usr/bin/log show \
    --style compact \
    --start "$start_local" \
    --predicate '
      process == "termite" OR
      process == "test" OR
      process == "WindowServer" OR
      senderImagePath CONTAINS[c] "AGX" OR
      eventMessage CONTAINS[c] "Metal" OR
      eventMessage CONTAINS[c] "GPU" OR
      eventMessage CONTAINS[c] "jetsam" OR
      eventMessage CONTAINS[c] "memorystatus" OR
      eventMessage CONTAINS[c] "out of memory"
    ' >"$OUT_DIR/log-show.txt" 2>&1 || true

  mkdir -p "$OUT_DIR/diagnostic-reports"
  find "$DIAG_DIR" "$SYSTEM_DIAG_DIR" -maxdepth 2 -type f \
    \( -name '*.ips' -o -name '*.crash' -o -name '*.panic' -o -name '*.diag' \) \
    -newermt "@$start_epoch" -print0 2>/dev/null | while IFS= read -r -d '' path; do
    cp "$path" "$OUT_DIR/diagnostic-reports/" || true
  done || true

  cat >"$OUT_DIR/summary.txt" <<EOF
exit_code=$command_status
started_utc=$start_iso
label=$LABEL
timeout_secs=$TIMEOUT_SECS
validation=$METAL_VALIDATE
shader_validation=$METAL_SHADER_VALIDATE
watch_pattern=${WATCH_PATTERN:-}

Next checks:
1. sed -n '1,220p' "$OUT_DIR/stdout.txt"
2. sed -n '1,220p' "$OUT_DIR/sample.txt"
3. grep -n "SIGABRT\\|panic\\|watchdog\\|jetsam\\|memorystatus\\|AGX\\|Metal\\|WindowServer" "$OUT_DIR/log-show.txt"
4. ls -1 "$OUT_DIR/diagnostic-reports"
5. If the machine rebooted, compare "$OUT_DIR/started_epoch.txt" with /Library/Logs/DiagnosticReports/Retired/panic-base-*.panic
EOF

  echo
  echo "Done. Bundle written to $OUT_DIR"
  echo "exit code: $command_status"
  echo "Open:"
  echo "  $OUT_DIR/stdout.txt"
  echo "  $OUT_DIR/sample.txt"
  echo "  $OUT_DIR/log-show.txt"
  echo "  $OUT_DIR/diagnostic-reports/"

  return "$command_status"
}

parse_options_until_command() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --)
        shift
        break
        ;;
      --label)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        LABEL="$2"
        shift 2
        ;;
      --out-dir)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        OUT_DIR="$2"
        shift 2
        ;;
      --timeout)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        TIMEOUT_SECS="$2"
        shift 2
        ;;
      --watch)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        WATCH_PATTERN="$2"
        shift 2
        ;;
      --cwd)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        RUN_CWD="$2"
        shift 2
        ;;
      --no-validate)
        METAL_VALIDATE=0
        METAL_SHADER_VALIDATE=0
        shift
        ;;
      --api-validate)
        METAL_VALIDATE=1
        METAL_SHADER_VALIDATE=0
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      -*)
        usage
        exit 2
        ;;
      *)
        break
        ;;
    esac
  done
  COMMAND_ARGS=("$@")
}

resolve_arg_path() {
  local value="$1"
  if [[ "$value" == "~/"* ]]; then
    printf "%s/%s" "$HOME" "${value#~/}"
    return 0
  fi
  if [[ "$value" == /* ]]; then
    printf "%s" "$value"
    return 0
  fi
  if [[ -e "$value" ]]; then
    printf "%s/%s" "$CALLER_DIR" "$value"
    return 0
  fi
  printf "%s" "$value"
}

run_command_mode() {
  COMMAND_ARGS=()
  parse_options_until_command "$@"
  run_capture "${COMMAND_ARGS[@]}"
}

run_embed_mode() {
  if [[ "$LABEL" == "metal-command" ]]; then
    LABEL="metal-embed"
  fi
  RUN_CWD="$PKG_DIR"
  COMMAND_ARGS=()
  parse_options_until_command "$@"
  if [[ "${#COMMAND_ARGS[@]}" -eq 0 ]]; then
    usage
    return 2
  fi

  local normalized_args=()
  local expect_path_value=0
  for arg in "${COMMAND_ARGS[@]}"; do
    if [[ "$expect_path_value" == "1" ]]; then
      normalized_args+=("$(resolve_arg_path "$arg")")
      expect_path_value=0
      continue
    fi
    case "$arg" in
      --audio|--image)
        normalized_args+=("$arg")
        expect_path_value=1
        ;;
      *)
        normalized_args+=("$arg")
        ;;
    esac
  done

  run_capture ./zig-out/bin/termite embed "${normalized_args[@]}" --backend metal
}

run_e2e_mode() {
  if [[ "$LABEL" == "metal-command" ]]; then
    LABEL="metal-e2e"
  fi
  RUN_CWD="$ROOT_DIR"
  WATCH_PATTERN="${WATCH_PATTERN:-termite run --host 127.0.0.1}"

  local test_expr="test_mixed_text_image_batch"
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --)
        shift
        break
        ;;
      --label)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        LABEL="$2"
        shift 2
        ;;
      --out-dir)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        OUT_DIR="$2"
        shift 2
        ;;
      --timeout)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        TIMEOUT_SECS="$2"
        shift 2
        ;;
      --watch)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        WATCH_PATTERN="$2"
        shift 2
        ;;
      --cwd)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        RUN_CWD="$2"
        shift 2
        ;;
      --no-validate)
        METAL_VALIDATE=0
        METAL_SHADER_VALIDATE=0
        shift
        ;;
      --api-validate)
        METAL_VALIDATE=1
        METAL_SHADER_VALIDATE=0
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      -*)
        usage
        exit 2
        ;;
      *)
        test_expr="$1"
        shift
        ;;
    esac
  done
  if [[ "$#" -gt 0 ]]; then
    test_expr="$*"
  fi

  run_capture env \
    TERMITE_PREFERRED_BACKEND=metal \
    TERMITE_BIN=./pkg/termite/zig-out/bin/termite \
    uv run --project e2e/termite \
    pytest -q -s -c e2e/termite/pyproject.toml e2e/termite/test_embed.py -k "$test_expr"
}

safe_name_for() {
  local raw="$1"
  raw="${raw//\//_}"
  raw="${raw// /_}"
  raw="${raw//:/_}"
  raw="${raw//\"/}"
  printf "%s" "$raw"
}

test_bundle_exit_code() {
  local unit_out_dir="$1"
  local label="$2"
  local file="$unit_out_dir/per-test/$(safe_name_for "$label")/exitcode.txt"
  if [[ -f "$file" ]]; then
    tr -d '[:space:]' <"$file"
  fi
}

escape_regex() {
  python3 -c 'import re, sys; print(re.escape(sys.argv[1]))' "$1"
}

run_unit_filter() {
  local unit_out_dir="$1"
  local label="$2"
  local test_binary="$3"
  shift 3
  local filters=("$@")
  local previous_runtime_current_file="$RUNTIME_CURRENT_FILE"
  local previous_runtime_trace_file="$RUNTIME_TRACE_FILE"
  RUNTIME_CURRENT_FILE="$unit_out_dir/current_runtime_test.txt"
  RUNTIME_TRACE_FILE="$unit_out_dir/current_runtime_trace.tsv"
  : >"$RUNTIME_TRACE_FILE"
  flush_debug_markers
  local status=0

  OUT_DIR="$unit_out_dir/per-test/$(safe_name_for "$label")"
  LABEL="$label"
  mkdir -p "$OUT_DIR"
  printf "enter run_unit_filter for:\n%s\n" "$label" >"$OUT_DIR/run-unit-filter.txt"
  flush_debug_markers
  RUN_CWD="$PKG_DIR"
  WATCH_PATTERN=""
  if [[ -n "$test_binary" ]]; then
    local args=()
    local filter
    for filter in "${filters[@]}"; do
      args+=(--test-filter "$filter")
    done
    run_capture "$test_binary" "${args[@]}" || status=$?
  else
    local filter_expr="" escaped
    for filter in "${filters[@]}"; do
      escaped="$(escape_regex "$filter")"
      if [[ -n "$filter_expr" ]]; then
        filter_expr+="|"
      fi
      filter_expr+="$escaped"
    done
    run_capture zig build test -Dmetal=true -Dmlx=false -- --test-filter "$filter_expr" || status=$?
  fi
  RUNTIME_CURRENT_FILE="$previous_runtime_current_file"
  RUNTIME_TRACE_FILE="$previous_runtime_trace_file"
  return "$status"
}

file_mtime() {
  local value
  value="$(stat -f %m "$1" 2>/dev/null || true)"
  if [[ "$value" =~ ^[0-9]+$ ]]; then
    printf "%s\n" "$value"
    return 0
  fi
  value="$(stat -c %Y "$1" 2>/dev/null || true)"
  if [[ "$value" =~ ^[0-9]+$ ]]; then
    printf "%s\n" "$value"
    return 0
  fi
  printf "0\n"
}

find_newest_test_binary() {
  local stamp_file="$1"
  local require_newer="${2:-1}"
  local best_path=""
  local best_mtime=0
  local candidate mtime
  local find_args=("$PKG_DIR/.zig-cache/o" -type f -name test)
  if [[ "$require_newer" == "1" ]]; then
    find_args+=(-newer "$stamp_file")
  fi

  while IFS= read -r candidate; do
    [[ -x "$candidate" ]] || continue
    mtime="$(file_mtime "$candidate")"
    if [[ "$mtime" -gt "$best_mtime" ]]; then
      best_mtime="$mtime"
      best_path="$candidate"
    fi
  done < <(find "${find_args[@]}" -print 2>/dev/null)

  if [[ -n "$best_path" ]]; then
    printf "%s\n" "$best_path"
  fi
}

prebuild_unit_test_binary() {
  local unit_out_dir="$1"
  local stamp_file="$unit_out_dir/prebuild.stamp"
  local binary_file="$unit_out_dir/prebuilt-test-binary.txt"

  : >"$stamp_file"
  OUT_DIR="$unit_out_dir/prebuild"
  LABEL="unit_prebuild"
  RUN_CWD="$PKG_DIR"
  WATCH_PATTERN=""
  local prebuild_status=0
  run_capture zig build test-bin -Dmetal=true -Dmlx=false -Druntime-test-filter=true || prebuild_status=$?
  if [[ "$prebuild_status" -ne 0 ]]; then
    return "$prebuild_status"
  fi

  if [[ -n "${current_file:-}" ]]; then
    write_marker_file "$current_file" "resolve prebuilt unit test binary"
  fi
  if type write_progress >/dev/null 2>&1; then
    write_progress "RUNNING" "resolve prebuilt unit test binary"
  fi

  local test_binary
  test_binary="$(find_newest_test_binary "$stamp_file")"
  if [[ -z "$test_binary" || ! -x "$test_binary" ]]; then
    test_binary="$PKG_DIR/zig-out/bin/termite-tests"
  fi
  if [[ -z "$test_binary" || ! -x "$test_binary" ]]; then
    test_binary="$(find_newest_test_binary "$stamp_file" 0)"
  fi
  if [[ -z "$test_binary" ]]; then
    echo "Unable to locate prebuilt Zig test binary under $PKG_DIR/.zig-cache/o" >&2
    if type write_progress >/dev/null 2>&1; then
      write_progress "FAIL(1)" "resolve prebuilt unit test binary"
    fi
    return 1
  fi
  printf "%s\n" "$test_binary" >"$binary_file"
  flush_debug_markers
  if type write_progress >/dev/null 2>&1; then
    write_progress "PASS" "resolve prebuilt unit test binary"
  fi
  echo "Prebuilt unit test binary: $test_binary"
}

refresh_unit_tests_from_prebuilt_binary() {
  local unit_out_dir="$1"
  local test_binary="$2"
  local test_name_regex="$3"
  local test_list_file="$4"
  local all_tests_file="$unit_out_dir/runtime-tests-all.txt"
  local refreshed_file="$unit_out_dir/runtime-tests-selected.txt"

  TERMITE_TEST_LIST_FILE="$all_tests_file" "$test_binary"
  rg "$test_name_regex" "$all_tests_file" >"$refreshed_file"
  mv "$refreshed_file" "$test_list_file"
  flush_debug_markers
}

run_unit_mode() {
  if [[ "$LABEL" == "metal-command" ]]; then
    LABEL="metal-unit"
  fi
  if [[ -z "$METAL_SHADER_VALIDATE_EXPLICIT" && "$METAL_VALIDATE" == "1" ]]; then
    METAL_SHADER_VALIDATE=0
  fi
  local test_name_regex="metal|Metal"
  local unit_out_dir="$PKG_DIR/.debug/metal-unit-$(date +%Y%m%d-%H%M%S)"
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --)
        shift
        break
        ;;
      --label)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        LABEL="$2"
        shift 2
        ;;
      --out-dir)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        unit_out_dir="$2"
        OUT_DIR="$2"
        shift 2
        ;;
      --timeout)
        [[ "$#" -ge 2 ]] || { usage; exit 2; }
        TIMEOUT_SECS="$2"
        shift 2
        ;;
      --no-validate)
        METAL_VALIDATE=0
        METAL_SHADER_VALIDATE=0
        shift
        ;;
      --api-validate)
        METAL_VALIDATE=1
        METAL_SHADER_VALIDATE=0
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      -*)
        usage
        exit 2
        ;;
      *)
        if [[ "$test_name_regex" == "metal|Metal" ]]; then
          test_name_regex="$1"
        else
          unit_out_dir="$1"
          OUT_DIR="$1"
        fi
        shift
        ;;
    esac
  done
  if [[ "$#" -gt 0 ]]; then
    test_name_regex="$*"
  fi
  local run_mode="${RUN_MODE:-chunked}"
  local chunk_size="${CHUNK_SIZE:-8}"
  local list_only="${LIST_ONLY:-0}"
  local use_prebuilt="${USE_PREBUILT_UNIT:-1}"
  local prebuild_only="${PREBUILD_ONLY:-0}"
  local launch_only="${LAUNCH_ONLY:-0}"
  local launch_only_filter="${LAUNCH_ONLY_FILTER:-__termite_no_matching_tests__}"
  local prebuilt_test_binary_env="${PREBUILT_TEST_BINARY:-}"
  local reverse_tests="${REVERSE_TESTS:-0}"
  local test_offset="${TEST_OFFSET:-0}"
  local test_limit="${TEST_LIMIT:-0}"
  local resume="${RESUME:-0}"
  local resume_skip_current="${RESUME_SKIP_CURRENT:-1}"
  local prebuilt_test_binary=""
  local progress_file="$unit_out_dir/progress.tsv"
  local current_file="$unit_out_dir/current_test.txt"
  local current_manifest_file="$unit_out_dir/current_manifest.txt"
  local test_list_file="$unit_out_dir/tests.txt"
  local interrupted_label=""

  unit_out_dir="$(absolute_path_from_caller "$unit_out_dir")"
  OUT_DIR="$(absolute_path_from_caller "$OUT_DIR")"
  progress_file="$unit_out_dir/progress.tsv"
  current_file="$unit_out_dir/current_test.txt"
  current_manifest_file="$unit_out_dir/current_manifest.txt"
  test_list_file="$unit_out_dir/tests.txt"

  mkdir -p "$unit_out_dir/per-test"

  rg -n '^test "' "$PKG_DIR/src" \
    | sed -E 's/^[^:]+:[0-9]+:test "(.*)" \{/\1/' \
    | rg "$test_name_regex" \
    >"$test_list_file"

  if [[ ! -s "$test_list_file" ]]; then
    echo "No tests matched regex: $test_name_regex" >&2
    return 1
  fi

  cat >"$unit_out_dir/README.txt" <<EOF
Metal unit test isolator

Repo:       $ROOT_DIR
Regex:      $test_name_regex
Run mode:   $run_mode
Chunk size: $chunk_size
Prebuilt:   $use_prebuilt
Validation: $METAL_VALIDATE
Shader val.: $METAL_SHADER_VALIDATE
List only:  $list_only
Prebuild only: $prebuild_only
Launch only: $launch_only
Launch filter: $launch_only_filter
Reverse tests: $reverse_tests
Test offset: $test_offset
Test limit: $test_limit
Resume:     $resume
Skip current on resume: $resume_skip_current

Files:
- tests.txt: candidate tests
- prebuilt-test-binary.txt: reused Zig test executable, when prebuild is enabled
- current_test.txt: last test or chunk started
- current_manifest.txt: tests in the current chunk or launch phase
- current_runtime_test.txt: last test entered inside the Zig test runner
- current_runtime_trace.tsv: fsynced sequence of tests entered inside the Zig test runner
- progress.tsv: append-only status log
- per-test/: generic command bundles
EOF

  if is_truthy "$resume" && [[ -f "$current_file" ]]; then
    interrupted_label="$(cat "$current_file" 2>/dev/null || true)"
  fi
  if is_truthy "$resume" && [[ -f "$progress_file" ]]; then
    printf "%s\tRESUME\t%s\n" "$(date +"%Y-%m-%d %H:%M:%S")" "${interrupted_label:-}" >>"$progress_file"
    flush_debug_markers
  else
    printf "timestamp\tstatus\ttest\n" >"$progress_file"
    flush_debug_markers
  fi

  echo "Output directory: $unit_out_dir"
  echo "Test regex: $test_name_regex"
  echo "Run mode: $run_mode"
  echo "Chunk size: $chunk_size"
  echo "Prebuilt: $use_prebuilt"
  echo "Validation: $METAL_VALIDATE"
  echo "Shader validation: $METAL_SHADER_VALIDATE"
  echo "List only: $list_only"
  echo "Prebuild only: $prebuild_only"
  echo "Launch only: $launch_only"
  echo "Launch filter: $launch_only_filter"
  echo "Reverse tests: $reverse_tests"
  echo "Test offset: $test_offset"
  echo "Test limit: $test_limit"
  if [[ -n "$RUNTIME_TEST_OFFSET" ]]; then
    echo "Runtime test offset: $RUNTIME_TEST_OFFSET"
  fi
  if [[ -n "$RUNTIME_TEST_LIMIT" ]]; then
    echo "Runtime test limit: $RUNTIME_TEST_LIMIT"
  fi
  echo "Resume: $resume"
  echo "Resume skip current: $resume_skip_current"
  if [[ -n "$interrupted_label" ]]; then
    echo "Interrupted/current label: $interrupted_label"
  fi
  local candidate_count
  candidate_count="$(wc -l <"$test_list_file" | tr -d ' ')"
  echo "Candidate tests: $candidate_count"

  if [[ "$list_only" == "1" ]]; then
    echo
    echo "LIST_ONLY=1: no Metal test process was launched."
    echo "Candidate list: $test_list_file"
    return 0
  fi

  if is_truthy "$prebuild_only" && is_truthy "$launch_only"; then
    echo "PREBUILD_ONLY=1 and LAUNCH_ONLY=1 are mutually exclusive" >&2
    return 2
  fi

  if ! [[ "$test_offset" =~ ^[0-9]+$ ]]; then
    echo "TEST_OFFSET must be a non-negative integer" >&2
    return 2
  fi
  if ! [[ "$test_limit" =~ ^[0-9]+$ ]]; then
    echo "TEST_LIMIT must be a non-negative integer" >&2
    return 2
  fi

  local tests=()
  while IFS= read -r test_name; do
    tests+=("$test_name")
  done <"$test_list_file"

  if is_truthy "$reverse_tests"; then
    local reversed_tests=()
    local reverse_index
    for ((reverse_index = ${#tests[@]} - 1; reverse_index >= 0; reverse_index--)); do
      reversed_tests+=("${tests[$reverse_index]}")
    done
    tests=("${reversed_tests[@]}")
  fi

  if [[ "$test_offset" -gt 0 || "$test_limit" -gt 0 ]]; then
    local sliced_tests=()
    local slice_index=0
    local selected_count=0
    for test_name in "${tests[@]}"; do
      if [[ "$slice_index" -lt "$test_offset" ]]; then
        slice_index=$((slice_index + 1))
        continue
      fi
      if [[ "$test_limit" -gt 0 && "$selected_count" -ge "$test_limit" ]]; then
        break
      fi
      sliced_tests+=("$test_name")
      selected_count=$((selected_count + 1))
      slice_index=$((slice_index + 1))
    done
    tests=("${sliced_tests[@]}")
  fi

  local run_count="${#tests[@]}"
  echo "Selected tests: $run_count"
  if [[ "$run_count" -eq 0 ]] && ! is_truthy "$launch_only"; then
    echo "No tests selected after TEST_OFFSET/TEST_LIMIT filtering" >&2
    return 1
  fi

  if ! is_truthy "$prebuild_only" && ! is_truthy "$launch_only" &&
    [[ "$run_mode" == "chunked" && "$chunk_size" -ge "$run_count" && "$run_count" -eq "$candidate_count" && "$run_count" -gt 1 ]] &&
    ! is_truthy "$ALLOW_BROAD_METAL_UNIT_CHUNK"; then
    cat >"$unit_out_dir/refused.txt" <<EOF
Refused unsafe broad Metal unit chunk.

Run mode: $run_mode
Chunk size: $chunk_size
Candidate tests: $candidate_count
Selected tests: $run_count

This would run every selected Metal unit test in one process, which can lose the
failing test name if the system watchdog resets. Use isolated mode or smaller
chunks, then resume from progress.tsv after a reboot.

Override intentionally with TERMITE_ALLOW_BROAD_METAL_UNIT_CHUNK=1.
EOF
    echo "Refusing unsafe broad Metal unit chunk. Details: $unit_out_dir/refused.txt" >&2
    flush_debug_markers
    return 126
  fi

  write_progress() {
    local status="$1"
    local label="$2"
    printf "%s\t%s\t%s\n" "$(date +"%Y-%m-%d %H:%M:%S")" "$status" "$label" >>"$progress_file"
    flush_debug_markers
  }

  write_current_manifest() {
    printf "%s\n" "$@" >"$current_manifest_file"
    flush_debug_markers
  }

  run_launch_only() {
    local label="launch-only no-match filter"
    local test_binary="$prebuilt_test_binary_env"
    if [[ -z "$test_binary" && -f "$unit_out_dir/prebuilt-test-binary.txt" ]]; then
      test_binary="$(cat "$unit_out_dir/prebuilt-test-binary.txt")"
    fi
    if [[ -z "$test_binary" || ! -x "$test_binary" ]]; then
      echo "LAUNCH_ONLY=1 requires PREBUILT_TEST_BINARY or $unit_out_dir/prebuilt-test-binary.txt" >&2
      return 1
    fi
    write_marker_file "$current_file" "$label"
    write_current_manifest "$launch_only_filter"
    write_progress "RUNNING" "$label"
    local status=0
    run_unit_filter "$unit_out_dir" "$label" "$test_binary" "$launch_only_filter" || status=$?
    if [[ "$status" -eq 0 ]]; then
      write_progress "PASS" "$label"
      return 0
    fi
    write_progress "FAIL($status)" "$label"
    return "$status"
  }

  if is_truthy "$launch_only"; then
    run_launch_only
    return $?
  fi

  if [[ "$use_prebuilt" == "1" ]]; then
    if [[ -n "$prebuilt_test_binary_env" ]]; then
      if [[ ! -x "$prebuilt_test_binary_env" ]]; then
        echo "PREBUILT_TEST_BINARY is not executable: $prebuilt_test_binary_env" >&2
        return 1
      fi
      local reuse_label="reuse prebuilt unit test binary"
      write_marker_file "$current_file" "$reuse_label"
      write_current_manifest "$prebuilt_test_binary_env"
      write_progress "RUNNING" "$reuse_label"
      printf "%s\n" "$prebuilt_test_binary_env" >"$unit_out_dir/prebuilt-test-binary.txt"
      flush_debug_markers
      write_progress "PASS" "$reuse_label"
    else
      local prebuild_label="prebuild unit test binary"
      write_marker_file "$current_file" "$prebuild_label"
      write_current_manifest "zig build test-bin -Dmetal=true -Dmlx=false -Druntime-test-filter=true"
      write_progress "RUNNING" "$prebuild_label"
      local prebuild_status=0
      prebuild_unit_test_binary "$unit_out_dir" || prebuild_status=$?
      if [[ "$prebuild_status" -eq 0 ]]; then
        write_progress "PASS" "$prebuild_label"
      else
        write_progress "FAIL($prebuild_status)" "$prebuild_label"
        return "$prebuild_status"
      fi
    fi
    if is_truthy "$prebuild_only"; then
      echo
      echo "PREBUILD_ONLY=1: stopping before launching the test binary."
      echo "Prebuilt unit test binary: $(cat "$unit_out_dir/prebuilt-test-binary.txt")"
      return 0
    fi
    prebuilt_test_binary="$(cat "$unit_out_dir/prebuilt-test-binary.txt")"
    refresh_unit_tests_from_prebuilt_binary "$unit_out_dir" "$prebuilt_test_binary" "$test_name_regex" "$test_list_file"
    tests=()
    while IFS= read -r test_name; do
      tests+=("$test_name")
    done <"$test_list_file"
    if is_truthy "$reverse_tests"; then
      local refreshed_reversed_tests=()
      local refreshed_reverse_index
      for ((refreshed_reverse_index = ${#tests[@]} - 1; refreshed_reverse_index >= 0; refreshed_reverse_index--)); do
        refreshed_reversed_tests+=("${tests[$refreshed_reverse_index]}")
      done
      tests=("${refreshed_reversed_tests[@]}")
    fi
    if [[ "$test_offset" -gt 0 || "$test_limit" -gt 0 ]]; then
      local refreshed_sliced_tests=()
      local refreshed_slice_index=0
      local refreshed_selected_count=0
      for test_name in "${tests[@]}"; do
        if [[ "$refreshed_slice_index" -lt "$test_offset" ]]; then
          refreshed_slice_index=$((refreshed_slice_index + 1))
          continue
        fi
        if [[ "$test_limit" -gt 0 && "$refreshed_selected_count" -ge "$test_limit" ]]; then
          break
        fi
        refreshed_sliced_tests+=("$test_name")
        refreshed_selected_count=$((refreshed_selected_count + 1))
        refreshed_slice_index=$((refreshed_slice_index + 1))
      done
      tests=("${refreshed_sliced_tests[@]}")
    fi
    candidate_count="${#tests[@]}"
    run_count="${#tests[@]}"
    echo "Runtime candidate tests: $candidate_count"
    if [[ "$run_count" -eq 0 ]] && ! is_truthy "$launch_only"; then
      echo "No runtime tests selected after prebuilt filtering" >&2
      return 1
    fi
  elif is_truthy "$prebuild_only"; then
    echo "PREBUILD_ONLY=1 requires USE_PREBUILT_UNIT=1" >&2
    return 2
  fi

  progress_has_pass() {
    local label="$1"
    [[ -f "$progress_file" ]] || return 1
    awk -F '\t' -v label="$label" '$2 == "PASS" && $3 == label { found = 1 } END { exit(found ? 0 : 1) }' "$progress_file"
  }

  bundle_has_success() {
    local label="$1"
    [[ "$(test_bundle_exit_code "$unit_out_dir" "$label")" == "0" ]]
  }

  should_skip_label() {
    local label="$1"
    is_truthy "$resume" || return 1
    if progress_has_pass "$label" || bundle_has_success "$label"; then
      return 0
    fi
    if is_truthy "$resume_skip_current" && [[ -n "$interrupted_label" && "$label" == "$interrupted_label" ]]; then
      return 0
    fi
    return 1
  }

  run_single_test() {
    local test_name="$1"
    if should_skip_label "$test_name"; then
      write_progress "SKIP" "$test_name"
      return 0
    fi
    write_marker_file "$current_file" "$test_name"
    write_current_manifest "$test_name"
    local pre_bundle_dir="$unit_out_dir/per-test/$(safe_name_for "$test_name")"
    mkdir -p "$pre_bundle_dir"
    printf "pending launch for test:\n%s\n" "$test_name" >"$pre_bundle_dir/prelaunch.txt"
    flush_debug_markers
    write_progress "RUNNING" "$test_name"
    local status=0
    run_unit_filter "$unit_out_dir" "$test_name" "$prebuilt_test_binary" "$test_name" || status=$?
    if [[ "$(test_bundle_exit_code "$unit_out_dir" "$test_name")" == "0" ]]; then
      write_progress "POSTCAPTURE" "$test_name"
    fi
    if [[ "$status" -eq 0 ]]; then
      write_progress "PASS" "$test_name"
      return 0
    else
      write_progress "FAIL($status)" "$test_name"
      return "$status"
    fi
  }

  run_chunk() {
    local chunk_index="$1"
    shift
    local chunk_tests=("$@")
    local chunk_label="chunk ${chunk_index} (${#chunk_tests[@]} tests)"
    if should_skip_label "$chunk_label"; then
      write_progress "SKIP" "$chunk_label"
      return 0
    fi
    write_marker_file "$current_file" "$chunk_label"
    write_current_manifest "${chunk_tests[@]}"
    local chunk_bundle_dir="$unit_out_dir/per-test/$(safe_name_for "chunk_${chunk_index}")"
    mkdir -p "$chunk_bundle_dir"
    printf "%s\n" "${chunk_tests[@]}" >"$chunk_bundle_dir/tests.txt"
    flush_debug_markers
    write_progress "RUNNING" "$chunk_label"
    local status=0
    run_unit_filter "$unit_out_dir" "chunk_${chunk_index}" "$prebuilt_test_binary" "${chunk_tests[@]}" || status=$?
    if [[ "$(test_bundle_exit_code "$unit_out_dir" "chunk_${chunk_index}")" == "0" ]]; then
      write_progress "POSTCAPTURE" "$chunk_label"
    fi
    if [[ "$status" -eq 0 ]]; then
      write_progress "PASS" "$chunk_label"
      return 0
    else
      write_progress "FAIL($status)" "$chunk_label"
      for test_name in "${chunk_tests[@]}"; do
        run_single_test "$test_name" || return $?
      done
      return "$status"
    fi
  }

  if [[ "$run_mode" == "isolated" ]]; then
    for test_name in "${tests[@]}"; do
      [[ -n "$test_name" ]] || continue
      run_single_test "$test_name" || return $?
    done
  elif [[ "$run_mode" == "chunked" ]]; then
    local chunk_index=1
    local chunk=()
    for test_name in "${tests[@]}"; do
      [[ -n "$test_name" ]] || continue
      chunk+=("$test_name")
      if [[ "${#chunk[@]}" -ge "$chunk_size" ]]; then
        run_chunk "$chunk_index" "${chunk[@]}" || return $?
        chunk_index=$((chunk_index + 1))
        chunk=()
      fi
    done
    if [[ "${#chunk[@]}" -gt 0 ]]; then
      run_chunk "$chunk_index" "${chunk[@]}" || return $?
    fi
  else
    echo "Unsupported RUN_MODE: $run_mode" >&2
    return 2
  fi

  rm -f "$current_file"
  echo
  echo "Finished all candidate tests successfully."
  echo "Progress log: $progress_file"
}

MODE="command"
if [[ "${1:-}" == "command" || "${1:-}" == "embed" || "${1:-}" == "e2e" || "${1:-}" == "unit" ]]; then
  MODE="$1"
  shift
elif [[ "${1:-}" == "--" ]]; then
  MODE="command"
fi

case "$MODE" in
  command)
    run_command_mode "$@"
    ;;
  embed)
    run_embed_mode "$@"
    ;;
  e2e)
    run_e2e_mode "$@"
    ;;
  unit)
    run_unit_mode "$@"
    ;;
  *)
    usage
    exit 2
    ;;
esac
