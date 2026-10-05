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

# Qualify the checked-out revision, including PR revisions whose workflow is
# dispatched from main. Keep target selection here rather than in workflow YAML.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 audit|runtime" >&2
  exit 2
fi

: "${VOPR_LOCAL_CACHE_DIR:?}"
: "${VOPR_GLOBAL_CACHE_DIR:?}"

case "$1" in
  audit)
    exec python3 tools/run_bounded_zig_build.py --max-rss-cap 23622320128 -- build \
      vopr-determinism-audit -Doptimize=safe --summary all \
      --cache-dir "$VOPR_LOCAL_CACHE_DIR" --global-cache-dir "$VOPR_GLOBAL_CACHE_DIR"
    ;;
  runtime)
    : "${GITHUB_WORKSPACE:?}"
    : "${RUNNER_TEMP:?}"
    exec python3 "$GITHUB_WORKSPACE/scripts/ci/measure_disk_usage.py" \
      --output "$RUNNER_TEMP/vopr-qualify-disk.json" \
      --path /mnt/cache --path "$GITHUB_WORKSPACE" -- \
      python3 tools/run_bounded_zig_build.py --max-rss-cap 23622320128 -- build \
      antfly-raft-transport-test standby-vopr-test vopr-runtime-test \
      restore-admission-vopr-test secrets-vopr-test \
      -Doptimize=safe --summary all \
      --cache-dir "$VOPR_LOCAL_CACHE_DIR" --global-cache-dir "$VOPR_GLOBAL_CACHE_DIR"
    ;;
  *)
    echo "usage: $0 audit|runtime" >&2
    exit 2
    ;;
esac
