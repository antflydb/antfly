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

if [[ "$#" -eq 0 || "$1" == --help || "$1" == -h ]]; then
  printf 'usage: %s COMMAND [ARGS...]\n' "$0"
  printf 'Run a conservative A100 CUDA baseline without inherited experimental tuning.\n'
  if [[ "$#" -eq 0 ]]; then exit 2; fi
  exit 0
fi

# A shell used for GB10/SM89 experiments may retain required architecture
# profiles. Remove project tuning only, in this child shell; retain NVIDIA
# device visibility, driver/library paths, and caller memory/admission limits.
for antfly_a100_name in \
  "${!ANTFLY_INFERENCE_CUDA_@}" "${!ANTFLY_CUDA_@}" \
  "${!TERMITE_CUDA_@}" "${!antfly_cuda_@}" "${!TERMITE_GEMMA4_@}"; do
  case "$antfly_a100_name" in
    # Memory ceilings remain caller-owned even when they have a CUDA prefix.
    *_BUDGET_*|*_MAX_MB|*_CACHE_MB|*_MAX_BUFFERS|*_WORKSPACE_MB) continue ;;
  esac
  if [[ -n "$antfly_a100_name" ]]; then unset "$antfly_a100_name"; fi
done
unset TERMITE_DISABLE_PAGED_KV

antfly_a100_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$antfly_a100_script_dir/gemma4_e4b_a100.env"
exec "$@"
