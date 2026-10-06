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

usage() {
  cat >&2 <<'EOF'
usage: build_zig_release_archive.sh --version VERSION --target TARGET --archive-name NAME --out-dir DIR [--product server|embedded|lite|inference] [--apple-providers true|false] [--metal true|false] [--system-blas true|false] [--optimize MODE] [--strip true|false] [--jobs N]

Builds the native Antfly Zig runtime and writes a release archive whose root
contains:
  antfly (server), or antfly-lite, antfly-inference, and antfly-inference-worker (embedded)
  legacy Lite and inference products remain supported for schema 2 sources
  completions/ (server only)
  share/
  lib/ and include/ (server and embedded; legacy Lite too)
  README.md
  LICENSE
  THIRD_PARTY_NOTICES.md
  LICENSING.md (server only)
  scripts/{apache_engine_files.txt,source_license_roots.json,embedded_asset_licenses.json} (server and embedded; legacy Lite too)
  LICENSES/Apache-2.0.txt
  LICENSES/Elastic-2.0.txt (server only)
EOF
}

product=server
version=
target=
archive_name=
out_dir=
metal=false
apple_providers=false
system_blas=false
optimize=fast
strip=true
jobs=

while [ "$#" -gt 0 ]; do
  case "$1" in
    --product)
      product="${2:?missing --product value}"
      shift 2
      ;;
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
    --apple-providers)
      apple_providers="${2:?missing --apple-providers value}"
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

case "$product" in
  server|embedded|lite|inference) ;;
  *) echo "invalid --product: $product (expected server, embedded, lite, or inference)" >&2; exit 2 ;;
esac

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
  debug|safe|fast|small) ;;
  *)
    usage
    echo "--optimize must be one of debug, safe, fast, small; got: $optimize" >&2
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

case "$apple_providers" in
  true|false) ;;
  *) echo "--apple-providers must be true or false" >&2; exit 2 ;;
esac
if [ "$apple_providers" = true ]; then
  case "$target" in
    *macos*) ;;
    *) echo "--apple-providers requires a macOS target" >&2; exit 2 ;;
  esac
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_date_epoch="${SOURCE_DATE_EPOCH:-$(git -C "$repo_root" show -s --format=%ct HEAD)}"
if ! [[ "$source_date_epoch" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be a non-negative integer, got: $source_date_epoch" >&2
  exit 2
fi
export SOURCE_DATE_EPOCH="$source_date_epoch"

work_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/antfly-zig-release-${product}-${target}"
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
  *-linux-gnu | *-linux-gnu.*)
    # Driver and PJRT plugins use the host glibc ABI. Keep the portable musl
    # archives CPU-only and put runtime-loaded integrations in GNU archives.
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
  -Dantfly-version="$version"
  -Donnx=false
  -Dapple-providers="$apple_providers"
  -Dmetal="$metal"
  -Dcuda="$cuda"
  -Dpjrt="$pjrt"
  -Dsystem-blas="$system_blas"
)
if [ "$product" = server ]; then
  zig_build_options+=(-Dantfly-bin-name=antfly)
fi

zig_install_args=(
  --prefix "$prefix"
  --cache-dir "$local_cache"
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

  if [[ "$product" = lite || "$product" = embedded ]]; then
    command+=(--build-file embedded.build.zig)
  fi
  if [ -n "$jobs" ]; then
    command+=("-j$jobs")
  fi
  command+=("${zig_build_options[@]}" "$@" "${zig_install_args[@]}")
  ZIG_GLOBAL_CACHE_DIR="$cache_root/global" "${command[@]}"
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
  # historical Linux small build and in the current Linux and macOS
  # fast builds. A replay retains completed work in the local cache and
  # starts LLVM in a fresh process. Limit the retry to those observed production
  # combinations and allocation signatures so unrelated errors fail immediately.
  case "$target:$optimize" in
    aarch64-linux-musl:small | \
    aarch64-linux-musl:fast | \
    aarch64-macos:fast) ;;
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
  if [ "$product" = inference ]; then
    cd "$repo_root/zig/pkg/inference"
  else
    cd "$repo_root/zig"
  fi
  # Runtime archives carry measured max-RSS admission claims. Independent units
  # can compile concurrently when they fit the release memory budget; there are
  # no artificial ordering dependencies between those compilations.
  if [ "$product" = inference ]; then
    run_zig_build_steps_with_retry archive
  elif [[ "$product" = lite || "$product" = embedded ]]; then
    run_zig_build_steps_with_retry archive lite
  else
    run_zig_build_steps_with_retry archive antfly capi
  fi
)

if [ "$product" = embedded ]; then
  (
    cd "$repo_root/zig/pkg/inference"
    product=inference
    run_zig_build_steps_with_retry inference
  )
fi

if [[ "$product" = lite || "$product" = embedded ]]; then
  binary_name=antfly-lite
elif [ "$product" = inference ]; then
  binary_name=antfly-inference
else
  binary_name=antfly
fi
test -x "$prefix/bin/$binary_name"
if [ "$product" != inference ]; then
  test -f "$prefix/include/antfly.h"
  if [ ! -f "$lite_lib_prefix_path" ]; then
    echo "missing Antfly C ABI library: $lite_lib_prefix_path" >&2
    find "$prefix" -maxdepth 3 -type f | sort >&2
    exit 1
  fi
fi
cp "$prefix/bin/$binary_name" "$stage/$binary_name"
if [ "$product" = embedded ]; then
  test -x "$prefix/bin/antfly-inference"
  cp "$prefix/bin/antfly-inference" "$stage/antfly-inference"
fi
if [[ "$product" = lite || "$product" = embedded ]]; then
  # The Lite executable understands `inference _worker`. A second filename
  # gives embedded hosts a dedicated Apache worker candidate without another
  # native compile or a dependency on the ELv2 server executable.
  cp "$prefix/bin/antfly-lite" "$stage/antfly-inference-worker"
fi
if [ "$apple_providers" = true ] && [ ! -f "$prefix/lib/libantfly-apple.dylib" ]; then
  echo "missing native Apple bridge: $prefix/lib/libantfly-apple.dylib" >&2
  exit 1
fi
if [ -d "$prefix/share" ]; then
  cp -R "$prefix/share" "$stage/share"
fi
if [ "$product" != inference ]; then
  python3 "$repo_root/scripts/packaging/render_libantfly_pkgconfig.py" \
    --version "$version" --out "$prefix/lib/pkgconfig/libantfly.pc"
fi
if [ "$product" != inference ] && [ -d "$prefix/lib" ]; then
  cp -R "$prefix/lib" "$stage/lib"
fi
if [ "$product" != inference ] && [ -d "$prefix/include" ]; then
  cp -R "$prefix/include" "$stage/include"
fi
if [ "$product" = embedded ]; then
  cp "$repo_root/scripts/packaging/embedded-release-README.md" "$stage/README.md"
elif [ "$product" = lite ]; then
  cp "$repo_root/scripts/packaging/lite-release-README.md" "$stage/README.md"
elif [ "$product" = inference ]; then
  cp "$repo_root/scripts/packaging/inference-release-README.md" "$stage/README.md"
else
  cp "$repo_root/README.md" "$stage/README.md"
  cp "$repo_root/LICENSING.md" "$stage/LICENSING.md"
fi
if [ "$product" != inference ]; then
  mkdir -p "$stage/scripts"
  cp "$repo_root/scripts/apache_engine_files.txt" "$stage/scripts/apache_engine_files.txt"
  cp "$repo_root/scripts/source_license_roots.json" "$stage/scripts/source_license_roots.json"
  cp "$repo_root/scripts/embedded_asset_licenses.json" "$stage/scripts/embedded_asset_licenses.json"
fi
mkdir -p "$stage/LICENSES"
cp "$repo_root/LICENSES/Apache-2.0.txt" "$stage/LICENSES/Apache-2.0.txt"
if [ "$product" = server ]; then
  cp "$repo_root/LICENSES/Elastic-2.0.txt" "$stage/LICENSES/Elastic-2.0.txt"
fi
cp -R "$repo_root/LICENSES/third-party" "$stage/LICENSES/third-party"
if [ "$product" != server ]; then
  cp "$repo_root/LICENSES/Apache-2.0.txt" "$stage/LICENSE"
else
  cp "$repo_root/LICENSE" "$stage/LICENSE"
fi
cp "$repo_root/THIRD_PARTY_NOTICES.md" "$stage/THIRD_PARTY_NOTICES.md"
if [ "$product" = server ]; then
  "$repo_root/scripts/completions.sh" "$stage/completions"
fi

python3 "$repo_root/scripts/packaging/create_reproducible_tar.py" \
  --source "$stage" \
  --output "$out_dir/$archive_name" \
  --mtime "$source_date_epoch"
tar -tzf "$out_dir/$archive_name" > "$work_root/archive-contents.txt"
grep -Fx "./$binary_name" "$work_root/archive-contents.txt" >/dev/null
if [ "$product" = embedded ]; then
  grep -Fx "./antfly-inference" "$work_root/archive-contents.txt" >/dev/null
fi
if [[ "$product" = lite || "$product" = embedded ]]; then
  grep -Fx "./antfly-inference-worker" "$work_root/archive-contents.txt" >/dev/null
  if grep -Fx './antfly' "$work_root/archive-contents.txt" >/dev/null; then
    echo "Lite archive unexpectedly includes the server executable" >&2
    exit 1
  fi
fi
if [ "$product" = inference ]; then
  if grep -Eq '^\./(antfly|antfly-lite|antfly-inference-worker)$' "$work_root/archive-contents.txt"; then
    echo "Inference archive unexpectedly includes database or worker executables" >&2
    exit 1
  fi
else
  grep -Fx "./include/antfly.h" "$work_root/archive-contents.txt" >/dev/null
  grep -Fx "$lite_lib_archive_path" "$work_root/archive-contents.txt" >/dev/null
  grep -Fx "./scripts/apache_engine_files.txt" "$work_root/archive-contents.txt" >/dev/null
fi
grep -Fx "./THIRD_PARTY_NOTICES.md" "$work_root/archive-contents.txt" >/dev/null
if [ "$product" = server ]; then
  grep -Fx "./completions/antfly.bash" "$work_root/archive-contents.txt" >/dev/null
  grep -Fx "./completions/antfly.zsh" "$work_root/archive-contents.txt" >/dev/null
  grep -Fx "./completions/antfly.fish" "$work_root/archive-contents.txt" >/dev/null
fi
if [ "$product" = server ]; then
  grep -Fx "./LICENSES/Elastic-2.0.txt" "$work_root/archive-contents.txt" >/dev/null
fi
grep -Fx "./LICENSES/Apache-2.0.txt" "$work_root/archive-contents.txt" >/dev/null
if [ "$apple_providers" = true ]; then
  grep -Fx "./lib/libantfly-apple.dylib" "$work_root/archive-contents.txt" >/dev/null
fi
echo "wrote $out_dir/$archive_name"
