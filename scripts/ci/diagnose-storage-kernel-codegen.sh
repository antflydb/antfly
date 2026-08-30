#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 LABEL OPTIMIZE STORAGE_KERNEL_SECTIONS" >&2
  exit 2
fi

label="$1"
optimize="$2"
storage_kernel_sections="$3"

case "$optimize" in
  ReleaseFast|ReleaseSmall) ;;
  *) echo "unsupported optimize mode: $optimize" >&2; exit 2 ;;
esac
case "$storage_kernel_sections" in
  true|false) ;;
  *) echo "storage-kernel sections must be true or false" >&2; exit 2 ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
report_root="${STORAGE_KERNEL_REPORT_ROOT:?set STORAGE_KERNEL_REPORT_ROOT}"
probe_root="${RUNNER_TEMP:-/tmp}/storage-kernel-codegen/${label}"
local_cache="${probe_root}/local-cache"
global_cache="${probe_root}/global-cache"
build_log="${report_root}/${label}.build.log"
timeline="${report_root}/${label}.rss.tsv"
summary="${report_root}/${label}.summary.txt"
trace_prefix="${report_root}/${label}.alloc.trace"

mkdir -p "$report_root" "$local_cache" "$global_cache"
printf 'elapsed_s\tcgroup_current_bytes\tcompiler_pid\tcompiler_rss_kib\n' > "$timeline"

command=(
  python3 tools/run_bounded_zig_build.py
  --zig zig
  --max-rss-cap 21474836480
  --
  build
  runtime-unit-distributed
  -Dtarget=aarch64-linux-musl
  -Doptimize="$optimize"
  -Dstrip=true
  -Dcpu=baseline
  -Dantfly-version=0.2.1-rc2-memory-probe
  -Donnx=false
  -Dmetal=false
  -Dcuda=true
  -Dpjrt=true
  -Dsystem-blas=false
  -Dstorage-kernel-sections="$storage_kernel_sections"
  --cache-dir "$local_cache"
  --global-cache-dir "$global_cache"
)

wrapped=("${command[@]}")
if [ "${STORAGE_KERNEL_TRACE_ALLOCATIONS:-0}" = "1" ]; then
  wrapped=(
    strace -f -qq -tt -T
    -e "trace=brk,mmap,mremap,munmap"
    -o "$trace_prefix"
    "${command[@]}"
  )
fi

find_storage_compiler_pid() {
  pgrep -f 'zig build-lib.*--name antfly-storage-kernel' | head -1 || true
}

start_epoch="$(date +%s)"
max_compiler_rss_kib=0
max_cgroup_current_bytes=0

set +e
(
  cd "$repo_root/zig"
  ulimit -c unlimited
  "${wrapped[@]}"
) >"$build_log" 2>&1 &
build_pid="$!"
set -e

while kill -0 "$build_pid" 2>/dev/null; do
  now="$(date +%s)"
  elapsed="$((now - start_epoch))"
  cgroup_current=0
  if [ -r /sys/fs/cgroup/memory.current ]; then
    cgroup_current="$(cat /sys/fs/cgroup/memory.current)"
  fi
  compiler_pid="$(find_storage_compiler_pid)"
  compiler_rss_kib=0
  if [ -n "$compiler_pid" ]; then
    compiler_rss_kib="$(ps -o rss= -p "$compiler_pid" 2>/dev/null | tr -d ' ' || true)"
    compiler_rss_kib="${compiler_rss_kib:-0}"
  fi
  if [ "$compiler_rss_kib" -gt "$max_compiler_rss_kib" ]; then
    max_compiler_rss_kib="$compiler_rss_kib"
  fi
  if [ "$cgroup_current" -gt "$max_cgroup_current_bytes" ]; then
    max_cgroup_current_bytes="$cgroup_current"
  fi
  printf '%s\t%s\t%s\t%s\n' \
    "$elapsed" "$cgroup_current" "${compiler_pid:-}" "$compiler_rss_kib" >> "$timeline"
  sleep 1
done

set +e
wait "$build_pid"
status="$?"
set -e

elapsed="$(( $(date +%s) - start_epoch ))"
{
  echo "label=$label"
  echo "status=$status"
  echo "optimize=$optimize"
  echo "storage_kernel_sections=$storage_kernel_sections"
  echo "elapsed_s=$elapsed"
  echo "max_compiler_rss_kib=$max_compiler_rss_kib"
  echo "max_cgroup_current_bytes=$max_cgroup_current_bytes"
  if [ -r /sys/fs/cgroup/memory.peak ]; then
    echo "pod_cgroup_peak_bytes=$(cat /sys/fs/cgroup/memory.peak)"
  fi
  if [ -r /sys/fs/cgroup/memory.events ]; then
    while read -r event value; do
      echo "cgroup_${event}=$value"
    done < /sys/fs/cgroup/memory.events
  fi
} > "$summary"

if [ "${STORAGE_KERNEL_TRACE_ALLOCATIONS:-0}" = "1" ]; then
  grep -hE '= -1 (ENOMEM|EAGAIN)' "${trace_prefix}"* > "${report_root}/${label}.failed-allocations.txt" || true
  gzip -9 "${trace_prefix}"* || true
fi

core_index=0
while IFS= read -r core_file; do
  core_index="$((core_index + 1))"
  gdb -batch \
    -ex 'set pagination off' \
    -ex 'info registers' \
    -ex 'bt full 80' \
    "$(command -v zig)" "$core_file" \
    > "${report_root}/${label}.core-${core_index}.gdb.txt" 2>&1 || true
  rm -f "$core_file"
done < <(find "$repo_root/zig" "$probe_root" -maxdepth 2 -type f -name 'core*' -size +1M 2>/dev/null)

cat "$summary"
exit 0
