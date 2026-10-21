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

# Test the installed C ABI with real SDK drivers; never build Zig here.
# Usage: test-embedded-sdk-native.sh INSTALL_ROOT [go|rust|python|typescript|all]
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
install_root="$(cd -- "${1:?libantfly installation root required}" && pwd)"
sdk="${2:-all}"
inference="${ANTFLY_SDK_NATIVE_INFERENCE:-0}"
case "$inference" in 0|1) ;; *) echo "ANTFLY_SDK_NATIVE_INFERENCE must be 0 or 1" >&2; exit 2 ;; esac
case "$sdk" in go|rust|python|typescript|all) ;; *) echo "unknown SDK: $sdk" >&2; exit 2 ;; esac
case "$(uname -s)" in
  Darwin) filename=libantfly.dylib ;;
  Linux) filename=libantfly.so ;;
  *) echo "native SDK CI supports Linux and macOS" >&2; exit 2 ;;
esac
for required in "$install_root/lib/$filename" "$install_root/include/antfly.h" "$install_root/lib/pkgconfig/libantfly.pc"; do
  if [[ ! -s "$required" ]]; then
    echo "missing required libantfly installation file: $required" >&2
    exit 1
  fi
done
export ANTFLY_LIB_DIR="$install_root/lib"
export ANTFLY_LIBRARY="$ANTFLY_LIB_DIR/$filename"
export ANTFLY_LITE_REQUIRE_LIBRARY=1
export PKG_CONFIG_PATH="$ANTFLY_LIB_DIR/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export LD_LIBRARY_PATH="$ANTFLY_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export DYLD_LIBRARY_PATH="$ANTFLY_LIB_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
export CGO_ENABLED=1
export GOWORK=off
pkg-config --exists libantfly
if [[ -n "${ANTFLY_INFERENCE_MODELS_DIR:-}" ]]; then mkdir -p "$ANTFLY_INFERENCE_MODELS_DIR"; fi
if [[ "$sdk" == go || "$sdk" == all ]]; then
  go_args=()
  if [[ "$inference" == 0 ]]; then go_args+=(-skip '^TestInference'); fi
  (cd "$repo_root/go/pkg/embedded" && go test -tags libantfly -count=1 ${go_args[@]+"${go_args[@]}"} ./...)
fi
if [[ "$sdk" == rust || "$sdk" == all ]]; then
  rust_args=()
  if [[ "$inference" == 0 ]]; then
    rust_args+=(--lib)
    for target in "$repo_root/rs/crates/embedded/tests/"*.rs; do
      name="$(basename "$target" .rs)"
      if [[ "$name" != inference ]]; then rust_args+=(--test "$name"); fi
    done
  fi
  cargo test --locked --manifest-path "$repo_root/rs/Cargo.toml" \
    --package antfly-embedded --package antfly-embedded-sys \
    --features antfly-embedded/libantfly,antfly-embedded/sqlx ${rust_args[@]+"${rust_args[@]}"}
fi
if [[ "$sdk" == python || "$sdk" == all ]]; then
  python_args=()
  if [[ "$inference" == 0 ]]; then python_args+=(--ignore=tests/test_inference.py); fi
  (cd "$repo_root/py/packages/embedded" && uv run --locked pytest -q ${python_args[@]+"${python_args[@]}"})
fi
if [[ "$sdk" == typescript || "$sdk" == all ]]; then
  ts_args=()
  if [[ "$inference" == 0 ]]; then ts_args+=(--exclude test/inference.test.ts); fi
  (cd "$repo_root/ts" && \
    node scripts/run-pinned-toolchain.mjs pnpm install --frozen-lockfile --filter @antfly/embedded... && \
    node scripts/run-pinned-toolchain.mjs pnpm --filter @antfly/embedded test ${ts_args[@]+"${ts_args[@]}"})
fi
