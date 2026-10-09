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

# Run a CI validation phase with a private Cargo target. Dependency downloads
# remain reusable; compiled outputs and package verification scratch expire once
# all phase consumers have exited, before Actions starts saving other caches.
with_disposable_cargo_target() (
  set -euo pipefail
  cargo_phase_target=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/antfly-sdk-cargo.XXXXXXXX")
  readonly cargo_phase_target
  trap 'rm -rf -- "$cargo_phase_target"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  export CARGO_TARGET_DIR="$cargo_phase_target"
  "$@"
)
