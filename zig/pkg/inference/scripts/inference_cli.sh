#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
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

ANTFLY_INFERENCE_ZIG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ANTFLY_INFERENCE_PKG_DIR="$ANTFLY_INFERENCE_ZIG_ROOT/pkg/inference"

resolve_antfly_inference_bin() {
  if [[ -n "${ANTFLY_BIN:-}" ]]; then
    printf '%s\n' "$ANTFLY_BIN"
    return
  fi
  if [[ -x "$ANTFLY_INFERENCE_PKG_DIR/zig-out/bin/antfly-inference" ]]; then
    printf '%s\n' "$ANTFLY_INFERENCE_PKG_DIR/zig-out/bin/antfly-inference"
    return
  fi
  if [[ -x "$ANTFLY_INFERENCE_PKG_DIR/zig-out/bin/antfly" ]]; then
    printf '%s\n' "$ANTFLY_INFERENCE_PKG_DIR/zig-out/bin/antfly"
    return
  fi
  printf '%s\n' "$ANTFLY_INFERENCE_PKG_DIR/zig-out/bin/antfly-inference"
}

run_antfly_inference() {
  local bin
  bin="$(resolve_antfly_inference_bin)"
  if [[ "$(basename "$bin")" == "antfly" ]]; then
    "$bin" inference "$@"
  else
    "$bin" "$@"
  fi
}
