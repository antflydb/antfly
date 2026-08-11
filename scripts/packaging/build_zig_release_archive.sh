#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: build_zig_release_archive.sh --version VERSION --target TARGET --archive-name NAME --out-dir DIR [--metal true|false] [--system-blas true|false] [--optimize MODE] [--strip true|false] [--jobs N]

Builds the native Antfly Zig runtime and writes a release archive whose root
contains:
  antfly
  share/
  lib/
  include/
  README.md
  LICENSE
EOF
}

version=
target=
archive_name=
out_dir=
metal=false
system_blas=false
optimize=ReleaseFast
strip=true
jobs=

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      version="${2:?missing --version value}"
      shift 2
      ;;
    --target)
      target="${2:?missing --target value}"
      shift 2
      ;;
    --archive-name)
      archive_name="${2:?missing --archive-name value}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:?missing --out-dir value}"
      shift 2
      ;;
    --metal)
      metal="${2:?missing --metal value}"
      shift 2
      ;;
    --system-blas)
      system_blas="${2:?missing --system-blas value}"
      shift 2
      ;;
    --optimize)
      optimize="${2:?missing --optimize value}"
      shift 2
      ;;
    --strip)
      strip="${2:?missing --strip value}"
      shift 2
      ;;
    --jobs)
      jobs="${2:?missing --jobs value}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [ -z "$version" ] || [ -z "$target" ] || [ -z "$archive_name" ] || [ -z "$out_dir" ]; then
  usage
  exit 2
fi

if [ -n "$jobs" ] && ! [[ "$jobs" =~ ^[1-9][0-9]*$ ]]; then
  usage
  echo "--jobs must be a positive integer, got: $jobs" >&2
  exit 2
fi

case "$optimize" in
  Debug|ReleaseSafe|ReleaseFast|ReleaseSmall) ;;
  *)
    usage
    echo "--optimize must be one of Debug, ReleaseSafe, ReleaseFast, ReleaseSmall; got: $optimize" >&2
    exit 2
    ;;
esac

case "$strip" in
  true|false) ;;
  *)
    usage
    echo "--strip must be true or false, got: $strip" >&2
    exit 2
    ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/antfly-zig-release-${target}"
prefix="${work_root}/zig-out"
stage="${work_root}/stage"
local_cache="${work_root}/zig-cache"
cache_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/zig-cache"

if [ -d /mnt/cache ] && [ -w /mnt/cache ]; then
  cache_root=/mnt/cache/zig
fi

lite_library_name() {
  case "$1" in
    *macos*) echo "libantfly.dylib" ;;
    *windows*) echo "antfly.dll" ;;
    *) echo "libantfly.so" ;;
  esac
}

lite_library_archive_path() {
  case "$1" in
    *windows*) echo "./bin/$(lite_library_name "$1")" ;;
    *) echo "./lib/$(lite_library_name "$1")" ;;
  esac
}

lite_lib_name="$(lite_library_name "$target")"
lite_lib_archive_path="$(lite_library_archive_path "$target")"
lite_lib_prefix_path="$prefix/${lite_lib_archive_path#./}"

rm -rf "$work_root"
mkdir -p "$prefix" "$stage" "$local_cache" "$cache_root/global" "$out_dir"

zig_build_options=(
  -Dtarget="$target"
  -Doptimize="$optimize"
  -Dstrip="$strip"
  -Dcpu=baseline
  -Dedition=full
  -Dantfly-bin-name=antfly
  -Dantfly-version="$version"
  -Donnx=false
  -Dmetal="$metal"
  -Dsystem-blas="$system_blas"
)

zig_install_args=(
  --prefix "$prefix"
  --cache-dir "$local_cache"
  --global-cache-dir "$cache_root/global"
)

zig_lib_dir="$(zig env | sed -n 's/^[[:space:]]*\.lib_dir = "\(.*\)",$/\1/p')"
if [ -z "$zig_lib_dir" ] || [ ! -f "$zig_lib_dir/compiler/build_runner.zig" ]; then
  echo "unable to locate Zig 0.16 build_runner.zig from 'zig env'" >&2
  exit 1
fi
patched_build_runner="$work_root/zig-build-runner-maxrss.zig"
python3 "$repo_root/zig/tools/patch_zig_0_16_build_runner_maxrss.py" \
  "$zig_lib_dir/compiler/build_runner.zig" \
  "$patched_build_runner"
max_rss="$(python3 "$repo_root/zig/tools/run_bounded_zig_build.py" --print-max-rss)"
zig_install_args+=(
  --build-runner "$patched_build_runner"
  --maxrss "$max_rss"
)

run_zig_build_steps() {
  local -a command=(zig build)

  if [ -n "$jobs" ]; then
    command+=("-j$jobs")
  fi
  command+=("${zig_build_options[@]}" "$@" "${zig_install_args[@]}")
  "${command[@]}"
}

run_zig_build_steps_with_retry() {
  local label="$1"
  shift
  local first_attempt_log="$work_root/${label}-attempt-1.log"
  local retry_log="$work_root/${label}-attempt-2.log"
  local status

  set +e
  run_zig_build_steps "$@" 2>&1 | tee "$first_attempt_log"
  status=${PIPESTATUS[0]}
  set -e

  if [ "$status" -eq 0 ]; then
    return 0
  fi

  # Zig 0.16 can fail the first ARM64 musl ReleaseSmall compile in LLVM's
  # allocation path even though the runner has ample available memory. A replay
  # using the local cache populated by that failed compile has been observed to
  # complete at the same peak RSS. Limit the retry to that known failure mode so
  # unrelated compiler and source errors still fail immediately.
  if [ "$target" != aarch64-linux-musl ] || \
     [ "$optimize" != ReleaseSmall ] || \
     ! grep -Eq 'std::bad_alloc|LLVM ERROR: out of memory|Buffer allocation failed' "$first_attempt_log"
  then
    return "$status"
  fi

  echo "::warning::Zig ARM64 ReleaseSmall hit a compiler allocation failure; retrying $label once with the populated local cache"
  set +e
  run_zig_build_steps "$@" 2>&1 | tee "$retry_log"
  status=${PIPESTATUS[0]}
  set -e
  return "$status"
}

(
  cd "$repo_root/zig"
  # API and the shared PIC application/storage unit occupy the initial bounded
  # memory group. Inference starts after API, while the short remote CLI unit
  # starts after application/storage, preserving useful overlap deterministically.
  run_zig_build_steps_with_retry archive antfly capi
)

test -x "$prefix/bin/antfly"
test -f "$prefix/include/antfly.h"
if [ ! -f "$lite_lib_prefix_path" ]; then
  echo "missing Antfly C ABI library: $lite_lib_prefix_path" >&2
  find "$prefix" -maxdepth 3 -type f | sort >&2
  exit 1
fi
cp "$prefix/bin/antfly" "$stage/antfly"
if [ -d "$prefix/share" ]; then
  cp -R "$prefix/share" "$stage/share"
fi
if [ -d "$prefix/lib" ]; then
  cp -R "$prefix/lib" "$stage/lib"
fi
if [ -d "$prefix/include" ]; then
  cp -R "$prefix/include" "$stage/include"
fi
cp "$repo_root/README.md" "$stage/README.md"
cp "$repo_root/LICENSE" "$stage/LICENSE"

tar -C "$stage" -czf "$out_dir/$archive_name" .
tar -tzf "$out_dir/$archive_name" > "$work_root/archive-contents.txt"
grep -Fx "./include/antfly.h" "$work_root/archive-contents.txt" >/dev/null
grep -Fx "$lite_lib_archive_path" "$work_root/archive-contents.txt" >/dev/null
echo "wrote $out_dir/$archive_name"
