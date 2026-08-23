#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: build_zig_release_archive.sh --version VERSION --target TARGET --archive-name NAME --out-dir DIR [--metal true|false] [--system-blas true|false] [--optimize MODE] [--strip true|false] [--jobs N]

Builds the native Antfly Zig runtime and writes a release archive whose root
contains:
  antfly
  completions/
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
source_date_epoch="${SOURCE_DATE_EPOCH:-$(git -C "$repo_root" show -s --format=%ct HEAD)}"
if ! [[ "$source_date_epoch" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be a non-negative integer, got: $source_date_epoch" >&2
  exit 2
fi
export SOURCE_DATE_EPOCH="$source_date_epoch"

work_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/antfly-zig-release-${target}"
prefix="${work_root}/zig-out"
stage="${work_root}/stage"
local_cache="${work_root}/zig-cache"
cache_root="${ANTFLY_ZIG_CACHE_ROOT:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/zig-cache}"

if [ -z "${ANTFLY_ZIG_CACHE_ROOT:-}" ] && [ -d /mnt/cache ] && [ -w /mnt/cache ]; then
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

cuda=false
pjrt=false
case "$target" in
  *-linux-*)
    # Both backends load their driver/plugin at runtime, so the same Linux
    # artifact remains usable on CPU-only hosts.
    cuda=true
    pjrt=true
    ;;
esac

rm -rf "$work_root"
mkdir -p "$prefix" "$stage" "$local_cache" "$cache_root/global" "$out_dir"

zig_build_options=(
  -Dtarget="$target"
  -Doptimize="$optimize"
  -Dstrip="$strip"
  -Dcpu=baseline
  -Dlinked-runtime-libraries=true
  -Dstorage-kernel-experiment=true
  -Dantfly-bin-name=antfly
  -Dantfly-version="$version"
  -Donnx=false
  -Dmetal="$metal"
  -Dcuda="$cuda"
  -Dpjrt="$pjrt"
  -Dsystem-blas="$system_blas"
)

zig_install_args=(
  --prefix "$prefix"
  --cache-dir "$local_cache"
  --global-cache-dir "$cache_root/global"
)

run_zig_build_steps() {
  local -a command=(
    python3
    "$repo_root/zig/tools/run_bounded_zig_build.py"
    --zig
    zig
    --max-rss-cap
    21474836480
    --
    build
  )

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

  # Zig 0.16 can fail a first ARM64 release compile in LLVM's allocation path
  # even though the runner has ample available memory. This has occurred in the
  # historical Linux ReleaseSmall build and in the current Linux and macOS
  # ReleaseFast builds. A replay retains completed work in the local cache and
  # starts LLVM in a fresh process. Limit the retry to those observed production
  # combinations and allocation signatures so unrelated errors fail immediately.
  case "$target:$optimize" in
    aarch64-linux-musl:ReleaseSmall | \
    aarch64-linux-musl:ReleaseFast | \
    aarch64-macos:ReleaseFast) ;;
    *) return "$status" ;;
  esac
  if ! grep -Eq 'std::bad_alloc|LLVM ERROR: out of memory|Buffer allocation failed' "$first_attempt_log"; then
    return "$status"
  fi

  echo "::warning::Zig ARM64 release build hit a compiler allocation failure; retrying $label once with the populated local cache"
  set +e
  run_zig_build_steps "$@" 2>&1 | tee "$retry_log"
  status=${PIPESTATUS[0]}
  set -e
  return "$status"
}

(
  cd "$repo_root/zig"
  # Physical storage and distributed control form the initial bounded-memory
  # group. The storage-kernel experiment gives physical codegen its own archive
  # and an 18 GiB Darwin scheduler claim, so the 20 GiB release budget cannot
  # admit another compiler unit beside it. Downstream units retain overlap once
  # the storage claim is released.
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
"$repo_root/scripts/completions.sh" "$stage/completions"

python3 "$repo_root/scripts/packaging/create_reproducible_tar.py" \
  --source "$stage" \
  --output "$out_dir/$archive_name" \
  --mtime "$source_date_epoch"
tar -tzf "$out_dir/$archive_name" > "$work_root/archive-contents.txt"
grep -Fx "./include/antfly.h" "$work_root/archive-contents.txt" >/dev/null
grep -Fx "$lite_lib_archive_path" "$work_root/archive-contents.txt" >/dev/null
grep -Fx "./completions/antfly.bash" "$work_root/archive-contents.txt" >/dev/null
grep -Fx "./completions/antfly.zsh" "$work_root/archive-contents.txt" >/dev/null
grep -Fx "./completions/antfly.fish" "$work_root/archive-contents.txt" >/dev/null
echo "wrote $out_dir/$archive_name"
