#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
diagnostic_root="${RUNNER_TEMP:-/tmp}/zig-arm64-bad-alloc"
baseline_log="$diagnostic_root/baseline-build.log"
backtrace_log="$diagnostic_root/gdb-backtrace.log"
native_log="$diagnostic_root/no-llvm-build.log"
compiler_command_file="$diagnostic_root/compiler-command.txt"

mkdir -p "$diagnostic_root"

{
  echo "commit: $(git -C "$repo_root" rev-parse HEAD)"
  echo "zig: $(zig version)"
  echo "kernel: $(uname -a)"
  echo
  free -h
  echo
  ulimit -a
} > "$diagnostic_root/system-info.txt"

echo "Reproducing the release failure and retaining its generated modules..."
set +e
"$repo_root/scripts/packaging/build_zig_release_archive.sh" \
  --version 0.2.0-rc.26 \
  --target aarch64-linux-musl \
  --optimize ReleaseSmall \
  --jobs 1 \
  --archive-name antfly_0.2.0-rc.26_Linux_arm64.tar.gz \
  --out-dir "$diagnostic_root/archive" \
  --metal false \
  --system-blas false 2>&1 | tee "$baseline_log"
baseline_status=${PIPESTATUS[0]}
set -e
printf 'baseline exit status: %s\n' "$baseline_status" >> "$diagnostic_root/system-info.txt"

if [ "$baseline_status" -eq 0 ]; then
  echo "The baseline build unexpectedly succeeded; there is no failed compiler command to diagnose." >&2
  exit 1
fi

compiler_command="$(sed -n 's/^failed command: //p' "$baseline_log" | tail -1)"
if [ -z "$compiler_command" ]; then
  echo "Could not extract the failed Zig compiler command from the baseline log." >&2
  exit 1
fi

# --listen=- is the build-runner protocol. A direct invocation would wait for a
# protocol peer on stdin, so remove it before replaying the compiler process.
compiler_command="${compiler_command% --listen=-}"
printf '%s\n' "$compiler_command" > "$compiler_command_file"

# The generated command contains only workspace/cache paths without whitespace.
# Keep this validation explicit so a future path-layout change fails safely.
read -r -a compiler_argv <<< "$compiler_command"
if [ "${#compiler_argv[@]}" -lt 3 ] || [ "${compiler_argv[1]}" != "build-exe" ]; then
  echo "Extracted command is not the expected zig build-exe invocation." >&2
  exit 1
fi
for arg in "${compiler_argv[@]}"; do
  if [[ "$arg" == *[[:space:]]* ]]; then
    echo "Cannot safely replay a compiler argument containing whitespace: $arg" >&2
    exit 1
  fi
done

echo "Replaying the compiler under GDB until the bad allocation is raised..."
set +e
gdb --quiet --batch \
  -ex 'set pagination off' \
  -ex 'set confirm off' \
  -ex 'set print demangle on' \
  -ex 'break _ZSt17__throw_bad_allocv' \
  -ex 'break _ZN4llvm22report_bad_alloc_errorEPKcb' \
  -ex run \
  -ex 'echo \n=== selected thread backtrace ===\n' \
  -ex 'bt 100' \
  -ex 'echo \n=== all thread backtraces ===\n' \
  -ex 'thread apply all bt 30' \
  -ex 'echo \n=== registers ===\n' \
  -ex 'info registers' \
  --args "${compiler_argv[@]}" 2>&1 | tee "$backtrace_log"
gdb_status=${PIPESTATUS[0]}
set -e
printf 'gdb exit status: %s\n' "$gdb_status" >> "$diagnostic_root/system-info.txt"

echo "Replaying the same compiler command with the LLVM backend disabled..."
native_argv=("${compiler_argv[0]}" "${compiler_argv[1]}" -fno-llvm "${compiler_argv[@]:2}")
{
  printf 'native command:'
  printf ' %q' "${native_argv[@]}"
  printf '\n'
} > "$diagnostic_root/no-llvm-command.txt"

set +e
timeout --signal=TERM 45m "${native_argv[@]}" 2>&1 | tee "$native_log"
native_status=${PIPESTATUS[0]}
set -e
printf 'no-llvm exit status: %s\n' "$native_status" | tee -a "$native_log" "$diagnostic_root/system-info.txt"

echo "Diagnostics written to $diagnostic_root"

