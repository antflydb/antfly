#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
diagnostic_root="${RUNNER_TEMP:-/tmp}/zig-arm64-bad-alloc"
baseline_log="$diagnostic_root/baseline-build.log"
backtrace_log="$diagnostic_root/gdb-backtrace.log"
gdb_replay_log="$diagnostic_root/gdb-build-runner.log"
core_replay_log="$diagnostic_root/core-build-runner.log"
direct_time_log="$diagnostic_root/direct-compiler-time.log"
direct_gdb_log="$diagnostic_root/direct-compiler-gdb.log"
direct_strace_log="$diagnostic_root/direct-compiler-strace.log"
protocol_strace_log="$diagnostic_root/build-runner-protocol-strace.log"
compiler_command_file="$diagnostic_root/compiler-command.txt"
build_runner_command_file="$diagnostic_root/build-runner-command.txt"
core_marker="$diagnostic_root/core-marker"

mkdir -p "$diagnostic_root"

capture_memory_snapshot() {
  local label="$1"

  {
    echo
    echo "=== memory snapshot: $label ==="
    date -u '+utc: %Y-%m-%dT%H:%M:%SZ'
    echo
    free -h || true
    echo
    grep -E '^(MemTotal|MemAvailable|SwapTotal|SwapFree|CommitLimit|Committed_AS):' /proc/meminfo || true
    echo
    echo "cgroup memory files:"
    for path in \
      /sys/fs/cgroup/memory.max \
      /sys/fs/cgroup/memory.current \
      /sys/fs/cgroup/memory.peak \
      /sys/fs/cgroup/memory.high \
      /sys/fs/cgroup/memory.events \
      /sys/fs/cgroup/memory.events.local
    do
      if [ -e "$path" ]; then
        echo "--- $path"
        cat "$path" || true
      fi
    done
  } >> "$diagnostic_root/system-info.txt"
}

capture_overcommit_policy() {
  {
    echo
    echo "=== kernel overcommit policy ==="
    for path in \
      /proc/sys/vm/overcommit_memory \
      /proc/sys/vm/overcommit_ratio \
      /proc/sys/vm/overcommit_kbytes \
      /proc/sys/vm/admin_reserve_kbytes \
      /proc/sys/vm/user_reserve_kbytes
    do
      if [ -e "$path" ]; then
        echo "--- $path"
        cat "$path" || true
      fi
    done
  } >> "$diagnostic_root/system-info.txt"
}

sample_zig_memory() {
  local label="$1"
  local watched_pid="$2"
  local output="$3"

  while kill -0 "$watched_pid" 2>/dev/null; do
    {
      echo
      date -u "+utc: %Y-%m-%dT%H:%M:%SZ label: $label"
      for comm_path in /proc/[0-9]*/comm; do
        [ -r "$comm_path" ] || continue
        [ "$(cat "$comm_path" 2>/dev/null)" = zig ] || continue
        status_path="${comm_path%/comm}/status"
        [ -r "$status_path" ] || continue
        grep -E '^(Name|Pid|PPid|VmPeak|VmSize|VmHWM|VmRSS|RssAnon|RssFile|VmSwap|Threads):' "$status_path" || true
      done
      grep -E '^(CommitLimit|Committed_AS|MemAvailable):' /proc/meminfo || true
    } >> "$output"
    sleep 5
  done
}

{
  echo "commit: $(git -C "$repo_root" rev-parse HEAD)"
  echo "zig: $(zig version)"
  echo "kernel: $(uname -a)"
  echo
  free -h
  echo
  ulimit -a
} > "$diagnostic_root/system-info.txt"
capture_overcommit_policy
capture_memory_snapshot "initial"

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
capture_memory_snapshot "after baseline build"

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
if [ "${#build_runner_argv[@]}" -lt 6 ] || [ ! -x "${build_runner_argv[0]}" ] || [ ! -x "${build_runner_argv[1]}" ]; then
  echo "Extracted command is not the expected Zig build-runner invocation." >&2
  exit 1
fi
original_build_runner_argv=("${build_runner_argv[@]}")

if [ "${ANTFLY_CACHE_AB_ONLY:-false}" = true ]; then
  # Both replays start from byte-for-byte copies of the same local cache state
  # and empty private global caches. Neither replay can warm the other. The only
  # intentional compiler-mode difference is the build protocol's --listen=-.
  ab_root="${RUNNER_TEMP:-/tmp}/antfly-zig-cache-ab"
  seed_local_cache="$ab_root/seed-local"
  server_local_cache="$ab_root/server-local"
  server_global_cache="$ab_root/server-global"
  direct_local_cache="$ab_root/direct-local"
  direct_global_cache="$ab_root/direct-global"
  mkdir -p \
    "$seed_local_cache" \
    "$server_local_cache" \
    "$server_global_cache" \
    "$direct_local_cache" \
    "$direct_global_cache"
  cp -a "${build_runner_argv[4]}/." "$seed_local_cache/"
  cp -a "$seed_local_cache/." "$server_local_cache/"
  cp -a "$seed_local_cache/." "$direct_local_cache/"

  server_build_runner_argv=("${build_runner_argv[@]}")
  server_build_runner_argv[4]="$server_local_cache"
  server_build_runner_argv[5]="$server_global_cache"

  direct_compiler_argv=()
  for arg in "${compiler_argv[@]}"; do
    arg="${arg//${build_runner_argv[4]}/$direct_local_cache}"
    arg="${arg//${build_runner_argv[5]}/$direct_global_cache}"
    direct_compiler_argv+=("$arg")
  done

  printf '%q ' "${server_build_runner_argv[@]}" > "$diagnostic_root/cache-ab-server-command.txt"
  printf '\n' >> "$diagnostic_root/cache-ab-server-command.txt"
  printf '%q ' "${direct_compiler_argv[@]}" > "$diagnostic_root/cache-ab-direct-command.txt"
  printf '\n' >> "$diagnostic_root/cache-ab-direct-command.txt"

  echo "Running build-runner/server mode against isolated cache clone A..."
  set +e
  (cd "$repo_root/zig" && /usr/bin/time -v timeout 40m "${server_build_runner_argv[@]}") \
    > "$diagnostic_root/cache-ab-server.log" 2>&1 &
  server_pid=$!
  sample_zig_memory "server-listen" "$server_pid" "$diagnostic_root/cache-ab-server-memory.log" &
  server_sampler_pid=$!
  wait "$server_pid"
  server_status=$?
  wait "$server_sampler_pid" 2>/dev/null
  set -e
  printf 'cache A/B server exit status: %s\n' "$server_status" >> "$diagnostic_root/system-info.txt"
  capture_memory_snapshot "after isolated server cache replay"
  tail -n 40 "$diagnostic_root/cache-ab-server.log"

  echo "Running direct compiler mode against isolated cache clone B..."
  set +e
  (cd "$repo_root/zig" && /usr/bin/time -v timeout 40m "${direct_compiler_argv[@]}") \
    > "$diagnostic_root/cache-ab-direct.log" 2>&1 &
  direct_pid=$!
  sample_zig_memory "direct-no-listen" "$direct_pid" "$diagnostic_root/cache-ab-direct-memory.log" &
  direct_sampler_pid=$!
  wait "$direct_pid"
  direct_status=$?
  wait "$direct_sampler_pid" 2>/dev/null
  set -e
  printf 'cache A/B direct exit status: %s\n' "$direct_status" >> "$diagnostic_root/system-info.txt"
  capture_memory_snapshot "after isolated direct cache replay"
  tail -n 40 "$diagnostic_root/cache-ab-direct.log"

  if [ "$server_status" -eq 0 ] || [ "$direct_status" -ne 0 ]; then
    echo "Cache A/B did not reproduce the expected server-fails/direct-succeeds split." >&2
    exit 1
  fi

  echo "Cache A/B reproduced server failure and direct success from isolated identical cache snapshots."
  exit 0
fi

# The node's core pattern is /core.<exe>.<pid>.<time>. Re-run the cached build
# runner uninstrumented as root so the failing Zig child can write there, then
# inspect the core after the process has already failed. This preserves the
# original --listen=- compiler protocol without live-debugger interference.
touch "$core_marker"
core_local_cache="${RUNNER_TEMP:-/tmp}/antfly-zig-core-local-cache"
core_global_cache="${RUNNER_TEMP:-/tmp}/antfly-zig-core-global-cache"
mkdir -p "$core_local_cache" "$core_global_cache"
cp -a "${build_runner_argv[4]}/." "$core_local_cache/"
core_build_runner_argv=("${build_runner_argv[@]}")
core_build_runner_argv[4]="$core_local_cache"
core_build_runner_argv[5]="$core_global_cache"

echo "Replaying the failing build-runner/compiler protocol for a core dump..."
root_mode="$(stat -c '%a' /)"
restore_root_mode() {
  sudo chmod "$root_mode" /
}
trap restore_root_mode EXIT
sudo chmod 1777 /
set +e
(cd "$repo_root/zig" && timeout 40m "${core_build_runner_argv[@]}") 2>&1 | tee "$core_replay_log"
replay_status=${PIPESTATUS[0]}
set -e
restore_root_mode
trap - EXIT
printf 'core build-runner exit status: %s\n' "$replay_status" >> "$diagnostic_root/system-info.txt"
capture_memory_snapshot "after core build-runner replay"

core_path="$(sudo find / -maxdepth 1 -type f -name 'core.zig.*' -newer "$core_marker" -print | tail -1)"
if [ -n "$core_path" ]; then
  printf 'core path: %s\n' "$core_path" >> "$diagnostic_root/system-info.txt"
  set +e
  sudo gdb --quiet --batch \
    -ex 'set pagination off' \
    -ex 'set print demangle on' \
    -ex 'echo \n=== selected thread backtrace ===\n' \
    -ex 'bt 100' \
    -ex 'echo \n=== all thread backtraces ===\n' \
    -ex 'thread apply all bt 30' \
    -ex 'echo \n=== registers ===\n' \
    -ex 'info registers' \
    "${build_runner_argv[1]}" "$core_path" 2>&1 | tee "$backtrace_log"
  core_gdb_status=${PIPESTATUS[0]}
  set -e
  printf 'post-mortem gdb exit status: %s\n' "$core_gdb_status" >> "$diagnostic_root/system-info.txt"
  sudo rm -f -- "$core_path"
else
  echo "The uninstrumented compiler replay did not produce a Zig core dump." | tee "$backtrace_log"
fi

if [ "${ANTFLY_CORE_ONLY:-false}" = true ]; then
  echo "Core-only mode: skipping replay diagnostics already captured by the prior run."
  exit 0
fi

# Preserve the additional replay diagnostics added during investigation.
export ANTFLY_GDB_REAL_ZIG="${build_runner_argv[1]}"
export ANTFLY_GDB_LOG="$diagnostic_root/build-runner-gdb-backtrace.log"
gdb_build_runner_argv=("${build_runner_argv[@]}")
gdb_build_runner_argv[1]="$repo_root/scripts/ci/zig_gdb_wrapper.sh"

echo "Replaying the failing build-runner/compiler protocol under GDB..."
set +e
(cd "$repo_root/zig" && timeout 40m "${gdb_build_runner_argv[@]}") 2>&1 | tee "$gdb_replay_log"
gdb_replay_status=${PIPESTATUS[0]}
set -e
printf 'gdb build-runner exit status: %s\n' "$gdb_replay_status" >> "$diagnostic_root/system-info.txt"
capture_memory_snapshot "after build-runner gdb replay"

echo "Replaying the original build-runner/compiler protocol under strace..."
set +e
(
  cd "$repo_root/zig"
  timeout 30m strace -f \
    -e trace=brk,mmap,mmap2,munmap,mremap,execve,clone,clone3,fork,vfork,wait4 \
    -o "$protocol_strace_log" \
    "${original_build_runner_argv[@]}"
)
protocol_strace_status=$?
set -e
printf 'build-runner protocol strace exit status: %s\n' "$protocol_strace_status" >> "$diagnostic_root/system-info.txt"
capture_memory_snapshot "after build-runner protocol strace replay"

echo "Replaying the extracted compiler command directly under /usr/bin/time -v..."
set +e
(cd "$repo_root/zig" && timeout 40m /usr/bin/time -v "${compiler_argv[@]}") 2>&1 | tee "$direct_time_log"
direct_time_status=${PIPESTATUS[0]}
set -e
printf 'direct compiler time exit status: %s\n' "$direct_time_status" >> "$diagnostic_root/system-info.txt"
capture_memory_snapshot "after direct compiler time replay"

echo "Replaying the extracted compiler command directly under GDB..."
set +e
(
  cd "$repo_root/zig"
  timeout 40m gdb --quiet --batch \
    -ex 'set pagination off' \
    -ex 'set confirm off' \
    -ex 'set print demangle on' \
    -ex 'set breakpoint pending on' \
    -ex 'set follow-fork-mode child' \
    -ex 'set detach-on-fork off' \
    -ex 'catch signal SIGABRT' \
    -ex 'break _ZSt17__throw_bad_allocv' \
    -ex 'break _ZN4llvm22report_bad_alloc_errorEPKcb' \
    -ex 'catch throw' \
    -ex run \
    -ex 'echo \n=== selected thread backtrace ===\n' \
    -ex 'bt full' \
    -ex 'echo \n=== all thread backtraces ===\n' \
    -ex 'thread apply all bt full' \
    -ex 'echo \n=== registers ===\n' \
    -ex 'info registers' \
    --args "${compiler_argv[@]}"
) 2>&1 | tee "$direct_gdb_log"
direct_gdb_status=${PIPESTATUS[0]}
set -e
printf 'direct compiler gdb exit status: %s\n' "$direct_gdb_status" >> "$diagnostic_root/system-info.txt"
capture_memory_snapshot "after direct compiler gdb replay"

echo "Replaying the extracted compiler command under strace memory syscall tracing..."
set +e
(
  cd "$repo_root/zig"
  timeout 30m strace -f \
    -e trace=brk,mmap,mmap2,munmap,mremap,execve \
    -o "$direct_strace_log" \
    "${compiler_argv[@]}"
)
direct_strace_status=$?
set -e
printf 'direct compiler strace exit status: %s\n' "$direct_strace_status" >> "$diagnostic_root/system-info.txt"
capture_memory_snapshot "after direct compiler strace replay"

echo "Diagnostics written to $diagnostic_root"
