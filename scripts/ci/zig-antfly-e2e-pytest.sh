#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
# Lightweight checks can use four workers while the scheduler separately caps
# concurrent Antfly processes and clusters.
default_workers=4
detected_workers="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
if [[ "$detected_workers" =~ ^[1-9][0-9]*$ ]] && (( detected_workers < default_workers )); then
  default_workers="$detected_workers"
fi
workers="${ANTFLY_E2E_WORKERS:-$default_workers}"
process_slots="${ANTFLY_E2E_PROCESS_SLOTS:-2}"

if [[ ! "$workers" =~ ^(0|[1-9][0-9]*)$ ]]; then
  echo "ANTFLY_E2E_WORKERS must be a non-negative integer; got: $workers" >&2
  exit 2
fi
if [[ ! "$process_slots" =~ ^[1-9][0-9]*$ ]]; then
  echo "ANTFLY_E2E_PROCESS_SLOTS must be a positive integer; got: $process_slots" >&2
  exit 2
fi

cd "$repo_root/zig"
if (( workers > 1 )); then
  # Isolation groups preserve shared fixture lifecycles; independent tests are
  # scheduled longest-first without exceeding the Antfly process budget.
  exec uv run --project e2e/antfly pytest -q --continue-on-collection-errors \
    -n "$workers" --dist=loadgroup --e2e-process-slots "$process_slots" "$@"
fi

exec uv run --project e2e/antfly pytest -q --continue-on-collection-errors "$@"
