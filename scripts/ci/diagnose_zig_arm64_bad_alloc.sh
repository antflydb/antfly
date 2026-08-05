#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
diagnostic_root="${RUNNER_TEMP:-/tmp}/zig-arm64-bad-alloc"
baseline_log="$diagnostic_root/baseline-build.log"
backtrace_log="$diagnostic_root/gdb-backtrace.log"
replay_log="$diagnostic_root/gdb-build-runner.log"
compiler_command_file="$diagnostic_root/compiler-command.txt"
build_runner_command_file="$diagnostic_root/build-runner-command.txt"

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

build_runner_command="$(awk '/^error: the following build command failed/{getline; print}' "$baseline_log" | tail -1)"
if [ -z "$build_runner_command" ]; then
  echo "Could not extract the failed Zig build-runner command from the baseline log." >&2
  exit 1
fi
printf '%s\n' "$build_runner_command" > "$build_runner_command_file"

read -r -a build_runner_argv <<< "$build_runner_command"
if [ "${#build_runner_argv[@]}" -lt 3 ] || [ ! -x "${build_runner_argv[0]}" ] || [ ! -x "${build_runner_argv[1]}" ]; then
  echo "Extracted command is not the expected Zig build-runner invocation." >&2
  exit 1
fi

# Re-run the cached build runner, replacing only its Zig executable argument
# with a wrapper. The wrapper starts GDB for the antfly build-exe child while
# preserving --listen=- and the build-runner protocol that were present in the
# original failure.
export ANTFLY_GDB_REAL_ZIG="${build_runner_argv[1]}"
export ANTFLY_GDB_LOG="$backtrace_log"
build_runner_argv[1]="$repo_root/scripts/ci/zig_gdb_wrapper.sh"

echo "Replaying the failing build-runner/compiler protocol under GDB..."
set +e
(cd "$repo_root/zig" && "${build_runner_argv[@]}") 2>&1 | tee "$replay_log"
replay_status=${PIPESTATUS[0]}
set -e
printf 'gdb build-runner exit status: %s\n' "$replay_status" >> "$diagnostic_root/system-info.txt"

echo "Diagnostics written to $diagnostic_root"

