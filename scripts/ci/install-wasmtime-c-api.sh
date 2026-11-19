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
version=v45.0.2
case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    arch=x86_64
    sha256=c32ade1d96bbc5e2a835bd9016cb305c370c1a183c6ccaea5bf1c0b72d229eda
    ;;
  Linux:aarch64|Linux:arm64)
    arch=aarch64
    sha256=d0efd17b38850638e039dac3ffa91e22df0f2c00b9b71471df165e896a5125a8
    ;;
  *) echo "unsupported Wasmtime CI platform: $(uname -s):$(uname -m)" >&2; exit 1 ;;
esac
# Digests are the official release API asset digests for this pinned release:
# https://api.github.com/repos/bytecodealliance/wasmtime/releases/tags/v45.0.2
archive="wasmtime-${version}-${arch}-linux-c-api.tar.xz"
root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/antfly-wasmtime.XXXXXX")"
archive_path="$root/$archive"
trap 'rm -f "$archive_path"' EXIT
curl --fail --location --retry 3 --retry-delay 2 \
  "https://github.com/bytecodealliance/wasmtime/releases/download/${version}/${archive}" \
  --output "$archive_path"
printf '%s  %s\n' "$sha256" "$archive_path" | sha256sum --check --status
tar -xJf "$archive_path" -C "$root" --strip-components=1
lib="$root/lib/libwasmtime.so"
test -r "$lib"
if [ -n "${GITHUB_ENV:-}" ]; then
  printf 'ANTFLY_WASMTIME_LIB=%s\n' "$lib" >> "$GITHUB_ENV"
  printf 'LD_LIBRARY_PATH=%s/lib:%s\n' "$root" "${LD_LIBRARY_PATH:-}" >> "$GITHUB_ENV"
fi
printf 'Wasmtime %s full C API: %s (sha256 %s)\n' "$version" "$lib" "$sha256"
