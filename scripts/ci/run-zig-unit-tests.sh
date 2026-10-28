#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

collect_descendants() {
  local roots="$*"
  local all=" ${roots} "
  local changed=1
  while [ "$changed" -eq 1 ]; do
    changed=0
    while read -r pid ppid; do
      if [[ " $all " == *" $ppid "* && " $all " != *" $pid "* ]]; then
        all="${all}${pid} "
        changed=1
      fi
    done < <(ps -eo pid=,ppid=)
  done
  echo "$all"
}

dump_stacks() {
  local root_pid="$1"
  for partition_log in "$ANTFLY_TEST_LOG_DIR"/*.log; do
    [ -f "$partition_log" ] || continue
    echo "::group::partition progress $partition_log"
    tail -n 80 "$partition_log" || true
    echo "::endgroup::"
  done
  echo "::group::antfly unit watchdog ptrace settings"
  if [ -r /proc/sys/kernel/yama/ptrace_scope ]; then
    cat /proc/sys/kernel/yama/ptrace_scope || true
  fi
  sysctl kernel.yama.ptrace_scope 2>/dev/null || true
  echo "::endgroup::"

  echo "::group::antfly unit watchdog process tree"
  ps -eo pid,ppid,stat,etime,cmd | awk 'NR == 1 || /zig build|zig-local|\/test|make zig-unit-test|make -C \.\/zig unit-test/' || true
  echo "::endgroup::"

  local pids
  pids="$(collect_descendants "$root_pid")"
  for pid in $pids; do
    if ! kill -0 "$pid" 2>/dev/null; then
      continue
    fi
    local cmdline
    cmdline="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
    case "$cmdline" in
      *"/test "*|*"/test"*|*"-tests "*|*"-tests"|*"zig build"*|*"zig test "*|*"zig test-obj "*|*" make "*)
        # Ephemeral runner caches disappear with the pod. Keep the
        # test bytes and compiler response files even without ptrace.
        python3 zig/tools/retain_test_process.py "$pid" "$ANTFLY_TEST_LOG_DIR/hung" || true
        echo "::group::proc snapshot pid=$pid cmd=$cmdline"
        if [ -r "/proc/$pid/status" ]; then
          grep -E "^(Name|State|Pid|PPid|Threads|VmRSS|VmHWM|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):" "/proc/$pid/status" || true
        fi
        if [ -r "/proc/$pid/wchan" ]; then
          printf "wchan="
          cat "/proc/$pid/wchan" || true
          printf "\n"
        fi
        if [ -r "/proc/$pid/stack" ]; then
          cat "/proc/$pid/stack" || true
        fi
        echo "::endgroup::"

        echo "::group::gdb thread dump pid=$pid cmd=$cmdline"
        timeout 45s gdb -batch -p "$pid" \
          -ex "set pagination off" \
          -ex "thread apply all bt" \
          -ex "detach" || \
          timeout 45s sudo gdb -batch -p "$pid" \
            -ex "set pagination off" \
            -ex "thread apply all bt" \
            -ex "detach" || true
        echo "::endgroup::"
        ;;
    esac
  done
}

terminate_tree() {
  local root_pid="$1"
  local pids
  pids="$(collect_descendants "$root_pid")"
  for pid in $pids; do
    if [ "$pid" != "$$" ]; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  sleep 5
  for pid in $pids; do
    if [ "$pid" != "$$" ]; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
}

if [ -r /proc/sys/kernel/yama/ptrace_scope ]; then
  echo "::group::configure ptrace for unit watchdog"
  echo "ptrace_scope before: $(cat /proc/sys/kernel/yama/ptrace_scope || true)"
  sudo sysctl -w kernel.yama.ptrace_scope=0 || true
  echo "ptrace_scope after: $(cat /proc/sys/kernel/yama/ptrace_scope || true)"
  echo "::endgroup::"
fi

unit_log="${RUNNER_TEMP:-/tmp}/antfly-zig-unit.log"
: > "$unit_log"
export ANTFLY_TEST_LOG_DIR="${RUNNER_TEMP:-/tmp}/antfly-zig-test-logs"
mkdir -p "$ANTFLY_TEST_LOG_DIR"

# Reject incomplete or overlapping shard inventories before launching unrelated
# compilation. Otherwise one cheap audit failure still burns the whole unit
# watchdog budget while the independent build branches continue.
(
  cd zig
  taskset -c "${1:?CPU affinity is required}" \
    python3 tools/run_bounded_zig_build.py --zig "${ZIG:-zig}" -- build \
    unit-storage-test-audit --maxrss "${2:?Scheduler memory budget is required}" --summary all
) 2>&1 | tee -a "$unit_log"

(
  set +e
  taskset -c "${1:?CPU affinity is required}" make zig-unit-test ZIG_BUILD_FLAGS="--maxrss ${2:?Scheduler memory budget is required} --summary all" 2>&1 | tee -a "$unit_log"
  exit "${PIPESTATUS[0]}"
) &
unit_pid="$!"
(
  # A cold Zig compile can legitimately remain silent for more than
  # 15 minutes on the x86_64 ARC runner. Keep the idle watchdog below
  # the independent hard limit without killing an active cold build.
  idle_limit_secs="${ANTFLY_UNIT_WATCHDOG_IDLE_SECS:-1800}"
  # PR #716's DB-core execution started 52 minutes into the combined
  # build/test step and was still progressing at the old 60m limit.
  # Allow 90m total, retaining the 30m idle and 120m outer job limits.
  hard_limit_secs="${ANTFLY_UNIT_WATCHDOG_HARD_SECS:-5400}"
  start_epoch="$(date +%s)"
  last_progress_epoch="$start_epoch"
  last_mtime=0
  while kill -0 "$unit_pid" 2>/dev/null; do
    sleep 15
    now_epoch="$(date +%s)"
    for progress_log in "$unit_log" "$ANTFLY_TEST_LOG_DIR"/*.log; do
      [ -f "$progress_log" ] || continue
      current_mtime="$(stat -c %Y "$progress_log" 2>/dev/null || echo 0)"
      if [ "$current_mtime" -gt "$last_mtime" ]; then
        last_mtime="$current_mtime"
        last_progress_epoch="$now_epoch"
      fi
    done

    elapsed_secs=$((now_epoch - start_epoch))
    idle_secs=$((now_epoch - last_progress_epoch))
    if [ "$hard_limit_secs" -gt 0 ] && [ "$elapsed_secs" -ge "$hard_limit_secs" ]; then
      echo "::error::zig-unit-test hard watchdog fired after ${elapsed_secs}s; dumping stacks before terminating live test processes"
      tail -n 200 "$unit_log" || true
      dump_stacks "$unit_pid"
      terminate_tree "$unit_pid"
      exit 0
    fi
    if [ "$idle_limit_secs" -gt 0 ] && [ "$idle_secs" -ge "$idle_limit_secs" ]; then
      echo "::error::zig-unit-test idle watchdog fired after ${idle_secs}s without unit log progress; dumping stacks before terminating live test processes"
      tail -n 200 "$unit_log" || true
      dump_stacks "$unit_pid"
      terminate_tree "$unit_pid"
      exit 0
    fi
  done
) &
watchdog_pid="$!"

status=0
wait "$unit_pid" || status="$?"
kill "$watchdog_pid" 2>/dev/null || true
wait "$watchdog_pid" 2>/dev/null || true
if [ "$status" -ne 0 ] && [ -d /sys/fs/cgroup ]; then
  echo "::group::unit failure cgroup memory"
  for metric in memory.current memory.peak memory.max memory.events; do
    if [ -r "/sys/fs/cgroup/$metric" ]; then
      echo "[$metric]"
      cat "/sys/fs/cgroup/$metric"
    fi
  done
  echo "::endgroup::"
fi
exit "$status"
