#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: build_zig_release_archive.sh --version VERSION --target TARGET --archive-name NAME --out-dir DIR [--metal true|false] [--system-blas true|false] [--optimize MODE] [--jobs N]

Builds the native Antfly Zig runtime and writes a release archive whose root
contains:
  antfly
  share/
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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/antfly-zig-release-${target}"
prefix="${work_root}/zig-out"
stage="${work_root}/stage"
cache_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/zig-cache"

if [ -d /mnt/cache ] && [ -w /mnt/cache ]; then
  cache_root=/mnt/cache/zig
fi

rm -rf "$work_root"
mkdir -p "$prefix" "$stage" "$cache_root/global" "$out_dir"

zig_build_args=(
  -Dtarget="$target"
  -Doptimize="$optimize"
  -Dcpu=baseline
  -Dedition=full
  -Dantfly-bin-name=antfly
  -Dantfly-version="$version"
  -Donnx=false
  -Dmetal="$metal"
  -Dsystem-blas="$system_blas"
  install
  --prefix "$prefix"
  --global-cache-dir "$cache_root/global"
)

(
  cd "$repo_root/zig"
  if [ -n "$jobs" ]; then
    zig build "-j$jobs" "${zig_build_args[@]}"
  else
    zig build "${zig_build_args[@]}"
  fi
)

test -x "$prefix/bin/antfly"
cp "$prefix/bin/antfly" "$stage/antfly"
if [ -d "$prefix/share" ]; then
  cp -R "$prefix/share" "$stage/share"
fi
cp "$repo_root/README.md" "$stage/README.md"
cp "$repo_root/LICENSE" "$stage/LICENSE"

tar -C "$stage" -czf "$out_dir/$archive_name" .
echo "wrote $out_dir/$archive_name"
