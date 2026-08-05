#!/usr/bin/env bash
set -euo pipefail

real_zig="${ANTFLY_GDB_REAL_ZIG:?missing ANTFLY_GDB_REAL_ZIG}"
gdb_log="${ANTFLY_GDB_LOG:?missing ANTFLY_GDB_LOG}"

is_antfly_build=false
if [ "${1:-}" = "build-exe" ]; then
  for ((i = 1; i < $#; i++)); do
    if [ "${!i}" = "--name" ]; then
      next=$((i + 1))
      if [ "${!next}" = "antfly" ]; then
        is_antfly_build=true
        break
      fi
    fi
  done
fi

if [ "$is_antfly_build" = false ]; then
  exec "$real_zig" "$@"
fi

# Redirect GDB's own output to the artifact log. The inferior retains the
# wrapper's stdin/stdout so Zig's --listen=- protocol continues to work.
exec gdb --quiet --batch \
  -ex "set logging file $gdb_log" \
  -ex 'set logging overwrite on' \
  -ex 'set logging redirect on' \
  -ex 'set logging enabled on' \
  -ex 'set pagination off' \
  -ex 'set confirm off' \
  -ex 'set print demangle on' \
  -ex 'set breakpoint pending on' \
  -ex 'break _ZSt17__throw_bad_allocv' \
  -ex 'break _ZN4llvm22report_bad_alloc_errorEPKcb' \
  -ex 'catch throw' \
  -ex run \
  -ex 'echo \n=== selected thread backtrace ===\n' \
  -ex 'bt 100' \
  -ex 'echo \n=== all thread backtraces ===\n' \
  -ex 'thread apply all bt 30' \
  -ex 'echo \n=== registers ===\n' \
  -ex 'info registers' \
  --args "$real_zig" "$@"
